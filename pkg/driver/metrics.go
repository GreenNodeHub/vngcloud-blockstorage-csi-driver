package driver

import (
	lctx "context"
	lstr "strings"
	ltime "time"

	lgrpc "google.golang.org/grpc"

	lscloud "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud"
	lsmetrics "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/metrics"
)

// metricsInterceptor records one in-flight gauge movement and one duration
// observation per CSI RPC.
//
// The in-flight gauge is the point of this: every stuck-operation incident this
// driver has had looked the same from outside - a handler that never returns,
// holding its inFlight entry, with nothing to see but logs. A gauge that stays
// flat and non-zero for minutes says that directly.
//
// It wraps rather than replaces the existing error log so both survive; order
// matters only in that the gauge must be decremented on every exit, hence the
// defer.
func metricsInterceptor(
	pctx lctx.Context, preq any, pinfo *lgrpc.UnaryServerInfo, phandler lgrpc.UnaryHandler,
) (any, error) {
	op := rpcOperationLabel(pinfo.FullMethod)
	labels := map[string]string{"op": op}

	lsmetrics.Recorder().AddGauge(lsmetrics.OperationsInFlight, lsmetrics.OperationsInFlightHelp, 1, labels)
	defer lsmetrics.Recorder().AddGauge(lsmetrics.OperationsInFlight, lsmetrics.OperationsInFlightHelp, -1, labels)

	start := ltime.Now()
	resp, err := phandler(pctx, preq)

	outcome := lsmetrics.OutcomeOK
	if err != nil {
		outcome = lsmetrics.OutcomeError
	}

	lsmetrics.Recorder().ObserveHistogram(
		lsmetrics.OperationDuration, lsmetrics.OperationDurationHelp,
		ltime.Since(start).Seconds(),
		map[string]string{"op": op, "outcome": outcome},
		lsmetrics.OperationDurationBuckets,
	)

	return resp, err
}

// rpcOperationLabel reduces "/csi.v1.Controller/CreateVolume" to
// "CreateVolume". The service prefix is constant per RPC and would only widen
// the label without adding information.
func rpcOperationLabel(pfullMethod string) string {
	if i := lstr.LastIndex(pfullMethod, "/"); i >= 0 && i+1 < len(pfullMethod) {
		return pfullMethod[i+1:]
	}

	return "unknown"
}

// seriesKind says which recorder Initialize* call creates a spec.
type seriesKind int

const (
	seriesCounter seriesKind = iota
	seriesGauge
	seriesHistogram
)

// seriesSpec is one series to create at zero on startup.
type seriesSpec struct {
	Kind    seriesKind
	Name    string
	Help    string
	Labels  map[string]string
	Buckets []float64
}

// startupSeries lists the series to pre-create for a given driver mode.
//
// Split out from InitializeStartupMetrics as a pure function so the mode gate
// can be asserted directly. It cannot be tested through the recorder: that is
// a process-wide singleton created with sync.Once, so once one test has
// registered a controller series, no later test in the same binary can observe
// its absence.
func startupSeries(pmode Mode) []seriesSpec {
	var specs []seriesSpec

	// Every process that talks to vServer, in any mode, makes API calls.
	for _, rm := range lscloud.KnownAPIRoutes() {
		labels := map[string]string{"route": rm.Route, "method": rm.Method}

		specs = append(specs,
			seriesSpec{seriesHistogram, lsmetrics.APIRequestDuration, lsmetrics.APIRequestDurationHelp,
				labels, lsmetrics.APIRequestDurationBuckets},
			seriesSpec{Kind: seriesCounter, Name: lsmetrics.APIRequestThrottles,
				Help: lsmetrics.APIRequestThrottlesHelp, Labels: labels},
			seriesSpec{Kind: seriesCounter, Name: lsmetrics.APIRequestsShed,
				Help: lsmetrics.APIRequestsShedHelp, Labels: labels},
		)

		for _, outcome := range []string{
			lsmetrics.OutcomeOK, lsmetrics.OutcomeError,
			lsmetrics.OutcomeThrottled, lsmetrics.OutcomeShed,
		} {
			specs = append(specs, seriesSpec{
				Kind: seriesCounter, Name: lsmetrics.APIRequests, Help: lsmetrics.APIRequestsHelp,
				Labels: map[string]string{"route": rm.Route, "method": rm.Method, "outcome": outcome},
			})
		}
	}

	if pmode == NodeMode {
		return specs // everything below is controller-only state
	}

	for _, reason := range lscloud.AllErrorReasons() {
		specs = append(specs, seriesSpec{
			Kind: seriesCounter, Name: lsmetrics.DetachBreakerTrips,
			Help: lsmetrics.DetachBreakerTripsHelp, Labels: map[string]string{"reason": reason},
		})

		for _, op := range []string{"create", "attach", "detach"} {
			specs = append(specs, seriesSpec{
				Kind: seriesCounter, Name: lsmetrics.IaaSErrors, Help: lsmetrics.IaaSErrorsHelp,
				Labels: map[string]string{"op": op, "reason": reason},
			})
		}
	}

	specs = append(specs,
		seriesSpec{Kind: seriesGauge, Name: lsmetrics.CreateGateWaiting, Help: lsmetrics.CreateGateWaitingHelp},
		seriesSpec{Kind: seriesHistogram, Name: lsmetrics.CreateGateWaitSeconds,
			Help: lsmetrics.CreateGateWaitSecondsHelp, Buckets: lsmetrics.CreateGateWaitBuckets},
	)

	return specs
}

// InitializeStartupMetrics creates, at zero, the series whose absence would
// otherwise be indistinguishable from "nothing has gone wrong yet".
//
// Without this, an error counter does not exist until the first error, so a
// dashboard shows "no data" instead of 0 and an alert rule evaluates against a
// missing series. aws-ebs-csi-driver does the same thing for its 16 known API
// operations (InitializeAPIMetrics).
//
// Only RARE series are pre-created. CSI RPC series are left lazy on purpose:
// Probe and GetPluginInfo arrive within seconds of startup on every cluster,
// so those series exist almost immediately anyway, and enumerating RPCs here
// would add a second list to keep in sync with the gRPC registration above.
//
// api_request_errors_total is also left lazy, for a different reason: its
// third label is the SDK's error code, which has no enumerable set worth
// pinning here.
//
// detach_pending_seconds is deliberately NOT pre-created. Its absence is
// meaningful - it means no (volume, node) pair is stuck - and pre-creating it
// would need placeholder label values that match no real volume.
func InitializeStartupMetrics(pmode Mode) {
	rec := lsmetrics.Recorder()
	if rec == nil {
		return // metrics endpoint not enabled
	}

	for _, spec := range startupSeries(pmode) {
		switch spec.Kind {
		case seriesCounter:
			rec.InitializeCounter(spec.Name, spec.Help, spec.Labels)
		case seriesGauge:
			rec.InitializeGauge(spec.Name, spec.Help, spec.Labels)
		case seriesHistogram:
			rec.InitializeHistogram(spec.Name, spec.Help, spec.Labels, spec.Buckets)
		}
	}
}
