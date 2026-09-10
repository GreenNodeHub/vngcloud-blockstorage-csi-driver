package driver

import (
	lctx "context"
	lerrors "errors"
	ltesting "testing"
	ltime "time"

	ldto "github.com/prometheus/client_model/go"
	lgrpc "google.golang.org/grpc"

	lscloud "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud"
	lsmetrics "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/metrics"
)

func TestRPCOperationLabel(t *ltesting.T) {
	for _, tc := range []struct{ in, want string }{
		{"/csi.v1.Controller/CreateVolume", "CreateVolume"},
		{"/csi.v1.Node/NodeStageVolume", "NodeStageVolume"},
		{"/csi.v1.Identity/Probe", "Probe"},
		// Not a valid gRPC FullMethod; the label says so rather than
		// inventing an operation name from a malformed string.
		{"NoSlashes", "unknown"},
		{"", "unknown"},
		{"/", "unknown"},
		{"trailing/", "unknown"},
	} {
		if got := rpcOperationLabel(tc.in); got != tc.want {
			t.Errorf("rpcOperationLabel(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// The in-flight gauge is the whole reason this interceptor exists, so the
// assertion is on the value DURING the handler, not before or after it. A
// gauge that is only correct at the edges would report 0 throughout every
// wedged operation - exactly the case it was added for.
func TestMetricsInterceptorCountsInFlightDuringTheHandler(t *ltesting.T) {
	lsmetrics.InitializeRecorder()

	info := &lgrpc.UnaryServerInfo{FullMethod: "/csi.v1.Controller/ControllerPublishVolume"}
	labels := map[string]string{"op": "ControllerPublishVolume"}

	before := gaugeValueWithLabels(t, lsmetrics.OperationsInFlight, labels)

	var during float64
	handler := func(lctx.Context, any) (any, error) {
		during = gaugeValueWithLabels(t, lsmetrics.OperationsInFlight, labels)

		return "ok", nil
	}

	if _, err := metricsInterceptor(lctx.Background(), nil, info, handler); err != nil {
		t.Fatalf("interceptor error = %v", err)
	}

	if during != before+1 {
		t.Errorf("in-flight during the handler = %v, want %v", during, before+1)
	}
	if after := gaugeValueWithLabels(t, lsmetrics.OperationsInFlight, labels); after != before {
		t.Errorf("in-flight after the handler = %v, want it back at %v", after, before)
	}
}

// A handler that fails must still release the gauge, or one error permanently
// inflates the "stuck operations" reading and the metric stops being usable.
func TestMetricsInterceptorReleasesInFlightOnError(t *ltesting.T) {
	lsmetrics.InitializeRecorder()

	info := &lgrpc.UnaryServerInfo{FullMethod: "/csi.v1.Controller/DeleteVolume"}
	labels := map[string]string{"op": "DeleteVolume"}

	before := gaugeValueWithLabels(t, lsmetrics.OperationsInFlight, labels)

	boom := lerrors.New("boom")
	handler := func(lctx.Context, any) (any, error) { return nil, boom }

	if _, err := metricsInterceptor(lctx.Background(), nil, info, handler); !lerrors.Is(err, boom) {
		t.Fatalf("interceptor error = %v, want the handler's error unchanged", err)
	}

	if after := gaugeValueWithLabels(t, lsmetrics.OperationsInFlight, labels); after != before {
		t.Errorf("in-flight after a failed handler = %v, want %v", after, before)
	}

	if got := counterFromHistogram(t, lsmetrics.OperationDuration,
		map[string]string{"op": "DeleteVolume", "outcome": lsmetrics.OutcomeError}); got == 0 {
		t.Error("a failed RPC recorded no duration under outcome=error")
	}
}

// The duration must be the handler's real duration. Asserted by value because
// a histogram's sample count also rises when the observed value is zero.
func TestMetricsInterceptorRecordsRealDuration(t *ltesting.T) {
	lsmetrics.InitializeRecorder()

	const delay = 25 * ltime.Millisecond
	info := &lgrpc.UnaryServerInfo{FullMethod: "/csi.v1.Controller/CreateVolume"}
	labels := map[string]string{"op": "CreateVolume", "outcome": lsmetrics.OutcomeOK}

	before := sumFromHistogram(t, lsmetrics.OperationDuration, labels)

	handler := func(lctx.Context, any) (any, error) {
		ltime.Sleep(delay)

		return "ok", nil
	}

	if _, err := metricsInterceptor(lctx.Background(), nil, info, handler); err != nil {
		t.Fatalf("interceptor error = %v", err)
	}

	if observed := sumFromHistogram(t, lsmetrics.OperationDuration, labels) - before; observed < delay.Seconds() {
		t.Errorf("observed duration %vs is below the %v the handler took", observed, delay)
	}
}

// Concurrent RPCs must each be counted. This is why the recorder needed
// AddGauge: Set would have required the caller to track the count itself.
func TestMetricsInterceptorCountsConcurrentCalls(t *ltesting.T) {
	lsmetrics.InitializeRecorder()

	info := &lgrpc.UnaryServerInfo{FullMethod: "/csi.v1.Controller/ValidateVolumeCapabilities"}
	labels := map[string]string{"op": "ValidateVolumeCapabilities"}

	before := gaugeValueWithLabels(t, lsmetrics.OperationsInFlight, labels)

	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})

	handler := func(lctx.Context, any) (any, error) {
		entered <- struct{}{}
		<-release

		return "ok", nil
	}

	const n = 3
	for i := 0; i < n; i++ {
		go func() {
			_, _ = metricsInterceptor(lctx.Background(), nil, info, handler)
			done <- struct{}{}
		}()
	}
	for i := 0; i < n; i++ {
		<-entered
	}

	if got := gaugeValueWithLabels(t, lsmetrics.OperationsInFlight, labels); got != before+n {
		t.Errorf("in-flight with %d concurrent handlers = %v, want %v", n, got, before+n)
	}

	close(release)
	for i := 0; i < n; i++ {
		<-done
	}

	if got := gaugeValueWithLabels(t, lsmetrics.OperationsInFlight, labels); got != before {
		t.Errorf("in-flight after all handlers returned = %v, want %v", got, before)
	}
}

// InitializeStartupMetrics exists so a dashboard shows 0 rather than "no data"
// and an alert rule has a series to evaluate. The assertion is therefore
// "exists AND is zero" - existence alone would also be satisfied by a metric
// that had already counted something.
func TestInitializeStartupMetricsCreatesZeroValuedSeries(t *ltesting.T) {
	lsmetrics.InitializeRecorder()
	InitializeStartupMetrics(ControllerMode)

	labels := map[string]string{"op": "detach", "reason": "IaaSUnreachable"}
	m := findSample(t, lsmetrics.IaaSErrors, labels)
	if m == nil {
		t.Fatalf("%s%v was not created at startup", lsmetrics.IaaSErrors, labels)
	}
	if got := m.GetCounter().GetValue(); got != 0 {
		t.Errorf("pre-created counter = %v, want 0", got)
	}

	// A histogram pre-created by observing would carry a fake sample; it must
	// exist with no observations at all.
	h := findSample(t, lsmetrics.APIRequestDuration,
		map[string]string{"route": "volumes/{id}/servers/{id}/detach", "method": "PUT"})
	if h == nil {
		t.Fatal("api_request_duration was not pre-created for the detach route")
	}
	if got := h.GetHistogram().GetSampleCount(); got != 0 {
		t.Errorf("pre-created histogram has %d samples, want 0", got)
	}
}

// Node mode must not publish controller-only series: a DaemonSet pod holds no
// breaker state, so a zeroed breaker series on 200 nodes would be 200 series
// that can never move.
//
// Asserted against startupSeries rather than the registry, because the
// recorder is a sync.Once singleton - once any earlier test has registered a
// controller series in this binary, absence is unobservable there.
func TestStartupSeriesSkipsControllerSeriesInNodeMode(t *ltesting.T) {
	node := startupSeries(NodeMode)

	for _, spec := range node {
		switch spec.Name {
		case lsmetrics.DetachBreakerTrips, lsmetrics.IaaSErrors,
			lsmetrics.CreateGateWaiting, lsmetrics.CreateGateWaitSeconds:
			t.Errorf("node mode would pre-create controller-only series %q", spec.Name)
		}
	}

	// The API series are shared: a node plugin builds its own SDK client and
	// talks to vServer too.
	if !hasSpec(node, lsmetrics.APIRequests, map[string]string{
		"route": "volumes/{id}", "method": "GET", "outcome": lsmetrics.OutcomeOK,
	}) {
		t.Error("node mode did not pre-create the shared API series")
	}
}

// Controller mode is the superset, and the counts are pinned so that dropping
// a whole group silently is a test failure rather than a quieter dashboard.
func TestStartupSeriesControllerModeCoversEveryRouteAndReason(t *ltesting.T) {
	specs := startupSeries(ControllerMode)

	routes := lscloud.KnownAPIRoutes()
	reasons := lscloud.AllErrorReasons()

	// Per route: 1 duration histogram + throttles + shed + 4 outcomes = 7.
	// Per reason: 1 breaker trip + 3 iaas_errors ops = 4. Plus the 2 gate series.
	want := len(routes)*7 + len(reasons)*4 + 2
	if len(specs) != want {
		t.Errorf("startupSeries(ControllerMode) has %d specs, want %d", len(specs), want)
	}

	for _, reason := range reasons {
		if !hasSpec(specs, lsmetrics.IaaSErrors, map[string]string{"op": "detach", "reason": reason}) {
			t.Errorf("no pre-created iaas_errors series for reason %q", reason)
		}
	}

	for _, rm := range routes {
		if !hasSpec(specs, lsmetrics.APIRequestDuration,
			map[string]string{"route": rm.Route, "method": rm.Method}) {
			t.Errorf("no pre-created duration series for %q %s", rm.Route, rm.Method)
		}
	}
}

// detach_pending_seconds must NOT be pre-created: its absence is the signal
// that nothing is stuck, and a placeholder series would need label values
// matching no real volume.
func TestStartupSeriesDoesNotPreCreateTheDetachGauge(t *ltesting.T) {
	for _, spec := range startupSeries(ControllerMode) {
		if spec.Name == lsmetrics.DetachPendingSeconds {
			t.Fatal("detach_pending_seconds must stay absent until a pair is actually stuck")
		}
	}
}

func hasSpec(pspecs []seriesSpec, pname string, plabels map[string]string) bool {
	for _, spec := range pspecs {
		if spec.Name != pname || len(spec.Labels) != len(plabels) {
			continue
		}
		match := true
		for k, v := range plabels {
			if spec.Labels[k] != v {
				match = false

				break
			}
		}
		if match {
			return true
		}
	}

	return false
}

func findSample(t *ltesting.T, pname string, plabels map[string]string) *ldto.Metric {
	t.Helper()

	families, err := lsmetrics.Recorder().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}

	for _, f := range families {
		if f.GetName() != pname {
			continue
		}
		for _, m := range f.GetMetric() {
			if len(m.GetLabel()) != len(plabels) {
				continue
			}
			match := true
			for _, l := range m.GetLabel() {
				if want, ok := plabels[l.GetName()]; !ok || want != l.GetValue() {
					match = false

					break
				}
			}
			if match {
				return m
			}
		}
	}

	return nil
}

func gaugeValueWithLabels(t *ltesting.T, pname string, plabels map[string]string) float64 {
	t.Helper()

	if m := findSample(t, pname, plabels); m != nil {
		return m.GetGauge().GetValue()
	}

	return 0
}

func counterFromHistogram(t *ltesting.T, pname string, plabels map[string]string) uint64 {
	t.Helper()

	if m := findSample(t, pname, plabels); m != nil {
		return m.GetHistogram().GetSampleCount()
	}

	return 0
}

func sumFromHistogram(t *ltesting.T, pname string, plabels map[string]string) float64 {
	t.Helper()

	if m := findSample(t, pname, plabels); m != nil {
		return m.GetHistogram().GetSampleSum()
	}

	return 0
}
