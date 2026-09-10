package metrics

// Every metric this driver publishes, and the help string an operator reads
// beside it in a dashboard.
//
// They live here rather than in pkg/driver/constants.go because pkg/cloud
// publishes some of them too, and pkg/cloud cannot import pkg/driver. Names
// and help strings sitting together also makes it hard to add one without the
// other - the recorder has no default help, by design.
//
// Prefix: vks_csi_. The three metrics that shipped first used vcontainer_csi_;
// they were renamed while no dashboard, alert or recording rule referenced
// them, since controller.enableMetrics is false on every production cluster
// and no tenant runs a scraper yet.
const (
	// Gauge: seconds a (volume, node) pair has been failing to detach,
	// measured from its first failure. This is the alertable one - "stuck >
	// 30m" - and the direct equivalent of aws-ebs-csi-driver's
	// ec2_detach_pending_seconds_total. Only the leader emits it, because only
	// the leader holds breaker state.
	DetachPendingSeconds     = "vks_csi_volume_detach_pending_seconds"
	DetachPendingSecondsHelp = "seconds a (volume, node) pair has been failing to detach, measured from its first failure"

	// Counter: breaker trips and backoff-step increases, by event reason.
	DetachBreakerTrips     = "vks_csi_detach_breaker_trips_total"
	DetachBreakerTripsHelp = "detach circuit-breaker trips and backoff-step increases, by error reason"

	// Counter: every classified IaaS error, by CSI operation and reason. This
	// is the CSI-operation view; the api_request_* family below is the
	// per-HTTP-call view. They answer different questions and neither replaces
	// the other.
	IaaSErrors     = "vks_csi_iaas_errors_total"
	IaaSErrorsHelp = "IaaS errors returned to the driver, by operation and classified reason"

	// The api_request_* family: one observation per HTTP call to vServer,
	// recorded in the single place every such call passes through. Labelled by
	// a NORMALISED route, never a raw URL - see pkg/cloud/apiroute.go.
	//
	// aws-ebs-csi-driver gets its equivalent from the AWS SDK's middleware
	// stack, which carries an operation name. The vngcloud SDK's IRequest
	// exposes only the HTTP method, hence the route normaliser.
	APIRequestDuration     = "vks_csi_api_request_duration_seconds"
	APIRequestDurationHelp = "latency of a vServer API call, by normalised route and method"

	APIRequests     = "vks_csi_api_requests_total"
	APIRequestsHelp = "vServer API calls, by normalised route, method and outcome"

	APIRequestErrors     = "vks_csi_api_request_errors_total"
	APIRequestErrorsHelp = "vServer API calls that failed, by normalised route, method, SDK error code and HTTP status"

	APIRequestThrottles     = "vks_csi_api_request_throttles_total"
	APIRequestThrottlesHelp = "vServer API calls rejected with HTTP 429, by normalised route and method"

	// Counter: requests this driver's own rate limiter dropped before they
	// reached the network. Without it a cluster being squeezed by the limiter
	// looks exactly like an idle one.
	APIRequestsShed     = "vks_csi_api_requests_shed_total"
	APIRequestsShedHelp = "vServer API calls shed by the driver's own rate limiter before leaving the process"

	// Gauge: the adaptive limiter's current rate. aws-ebs-csi-driver has no
	// equivalent because it does not rate-limit itself.
	RateLimiterQPS     = "vks_csi_rate_limiter_qps"
	RateLimiterQPSHelp = "current requests-per-second ceiling of the driver's adaptive rate limiter"

	// Gauge + histogram over CSI RPCs. The gauge is the one that answers "is a
	// handler wedged right now" without reading logs: a flat non-zero series
	// means some handler is not returning, which is the shape of every
	// stuck-inflight incident this driver has had.
	OperationsInFlight     = "vks_csi_operations_inflight"
	OperationsInFlightHelp = "CSI RPCs currently being served, by operation"

	OperationDuration     = "vks_csi_operation_duration_seconds"
	OperationDurationHelp = "duration of a CSI RPC, by operation and outcome"

	// The CreateVolume concurrency gate. Its cap is a compile-time constant
	// validated by one experiment; these two series are what would let anyone
	// re-tune it without re-running the whole bulk-provisioning suite.
	CreateGateWaiting     = "vks_csi_volume_create_gate_waiting"
	CreateGateWaitingHelp = "CreateVolume calls waiting for a slot in the concurrency gate"

	CreateGateWaitSeconds     = "vks_csi_volume_create_gate_wait_seconds"
	CreateGateWaitSecondsHelp = "seconds a CreateVolume call waited for a slot in the concurrency gate"
)

// Label KEYS, shared by every package that publishes these metrics.
//
// Constants rather than literals because a mistyped key does not fail: it
// silently registers a second series under a name no dashboard queries, and
// the original then looks like it stopped moving. That is the exact class of
// silent-wrong-metric failure this whole surface exists to remove, so the keys
// are declared once.
const (
	LabelRoute    = "route"
	LabelMethod   = "method"
	LabelOutcome  = "outcome"
	LabelCode     = "code"
	LabelStatus   = "status"
	LabelOp       = "op"
	LabelReason   = "reason"
	LabelVolumeID = "volume_id"
	LabelNodeID   = "node_id"
)

// Label values for outcome, shared so a dashboard query cannot drift from what
// the code emits.
const (
	OutcomeOK        = "ok"
	OutcomeError     = "error"
	OutcomeThrottled = "throttled"
	OutcomeShed      = "shed"
)

// CSI operations the iaas_errors_total counter reports against.
const (
	OpCreate = "create"
	OpAttach = "attach"
	OpDetach = "detach"
)

// StatusNone is the status label for a call that got no HTTP response at all -
// a refused connection, a DNS failure, the client timeout. Deliberately not
// "0": on a dashboard that reads as an HTTP status, and there is no such code.
const StatusNone = "none"

// ValueUnknown labels something the driver could not identify: a URL matching
// no known route, or a gRPC method name that is not a valid FullMethod.
const ValueUnknown = "unknown"

// APIRequestDurationBuckets tops out at 120s because that is exactly the SDK's
// request timeout (v2.21.0) and retry is disabled, so no single call can
// exceed it. A bucket above the hard ceiling would never fill.
var APIRequestDurationBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120,
}

// OperationDurationBuckets covers a whole CSI RPC, which may contain many API
// calls plus poll waits. 600s is past every sidecar timeout in the chart, so
// anything landing in +Inf is a handler that outlived its caller.
var OperationDurationBuckets = []float64{
	0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600,
}

// CreateGateWaitBuckets: the gate's wait ends with the request context, so the
// interesting range is "did it wait at all" up to the provisioner's 60s.
var CreateGateWaitBuckets = []float64{
	0.01, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120,
}
