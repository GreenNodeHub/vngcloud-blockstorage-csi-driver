package cloud

import (
	lhttp "net/http"
	ltesting "testing"
	ltime "time"

	lreq "github.com/imroc/req/v3"
	ldto "github.com/prometheus/client_model/go"
	lsdkClient "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/client"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
	lrate "golang.org/x/time/rate"

	lsmetrics "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/metrics"
)

const testVolumeURL = "https://vserver/vserver-gateway/v2/proj-1/volumes/vol-9"

// slowHTTPClient takes a measurable amount of time so that a duration
// assertion can check the VALUE, not merely that an observation happened. A
// histogram's sample count increases even when the observed value is 0, so
// "count > 0" alone would pass against code that timed nothing.
type slowHTTPClient struct {
	fakeHTTPClient
	delay ltime.Duration
}

func (s *slowHTTPClient) DoRequest(purl string, preq lsdkClient.IRequest) (*lreq.Response, lsdkErrs.IError) {
	ltime.Sleep(s.delay)

	return s.fakeHTTPClient.DoRequest(purl, preq)
}

func TestDoRequestRecordsRealDuration(t *ltesting.T) {
	lsmetrics.InitializeRecorder()

	const delay = 25 * ltime.Millisecond
	client := &throttledHTTPClient{
		inner:   &slowHTTPClient{delay: delay},
		limiter: newAdaptiveRateLimiter(),
	}

	labels := map[string]string{lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet}
	sumBefore := histogramSum(t, lsmetrics.APIRequestDuration, labels)

	if _, err := client.DoRequest(testVolumeURL, fakeRequest{}); err != nil {
		t.Fatalf("DoRequest error = %v", err)
	}

	observed := histogramSum(t, lsmetrics.APIRequestDuration, labels) - sumBefore
	if observed < delay.Seconds() {
		t.Errorf("observed duration %vs is below the %v the call actually took", observed, delay)
	}
}

// A successful call must land in the ok bucket and produce no error series.
func TestDoRequestRecordsSuccess(t *ltesting.T) {
	lsmetrics.InitializeRecorder()
	client := &throttledHTTPClient{inner: &fakeHTTPClient{}, limiter: newAdaptiveRateLimiter()}

	before := counterValue(t, lsmetrics.APIRequests,
		map[string]string{lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet, lsmetrics.LabelOutcome: lsmetrics.OutcomeOK})
	errsBefore := familySampleCount(t, lsmetrics.APIRequestErrors)

	if _, err := client.DoRequest(testVolumeURL, fakeRequest{}); err != nil {
		t.Fatalf("DoRequest error = %v, want nil", err)
	}

	after := counterValue(t, lsmetrics.APIRequests,
		map[string]string{lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet, lsmetrics.LabelOutcome: lsmetrics.OutcomeOK})
	if after != before+1 {
		t.Errorf("ok counter = %v, want %v", after, before+1)
	}

	if got := histogramCount(t, lsmetrics.APIRequestDuration,
		map[string]string{lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet}); got == 0 {
		t.Error("duration histogram recorded no observation for a successful call")
	}

	if got := familySampleCount(t, lsmetrics.APIRequestErrors); got != errsBefore {
		t.Errorf("error counter moved on a successful call: %v -> %v", errsBefore, got)
	}
}

// A 429 must be counted as throttled AND as an error-code series, because both
// questions get asked during an incident: "are we being throttled" and "which
// call is failing".
func TestDoRequestRecordsThrottle(t *ltesting.T) {
	lsmetrics.InitializeRecorder()
	client := &throttledHTTPClient{
		inner:   &fakeHTTPClient{err: sdkErrWithStatus(lhttp.StatusTooManyRequests, lsdkErrs.EcPermissionDenied)},
		limiter: newAdaptiveRateLimiter(),
	}

	labels := map[string]string{lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet}
	before := counterValue(t, lsmetrics.APIRequestThrottles, labels)

	if _, err := client.DoRequest(testVolumeURL, fakeRequest{}); err == nil {
		t.Fatal("the inner error must be returned")
	}

	if after := counterValue(t, lsmetrics.APIRequestThrottles, labels); after != before+1 {
		t.Errorf("throttle counter = %v, want %v", after, before+1)
	}

	if got := counterValue(t, lsmetrics.APIRequests, map[string]string{
		lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet, lsmetrics.LabelOutcome: lsmetrics.OutcomeThrottled,
	}); got < 1 {
		t.Errorf("throttled outcome counter = %v, want >= 1", got)
	}
}

// The branch this test exists for: a genuine 403 and a 404 take the limiter's
// "default" arm, the same arm a success takes. Deciding the outcome from the
// limiter branch instead of from the error would report every such failure as
// a success - and a 403 is exactly what vServer returns when a project's
// permissions are wrong, so it must not read as healthy traffic.
func TestDoRequestCountsNonThrottleFailuresAsErrors(t *ltesting.T) {
	lsmetrics.InitializeRecorder()
	client := &throttledHTTPClient{
		inner:   &fakeHTTPClient{err: sdkErrWithStatus(lhttp.StatusForbidden, lsdkErrs.EcPermissionDenied)},
		limiter: newAdaptiveRateLimiter(),
	}

	okLabels := map[string]string{
		lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet, lsmetrics.LabelOutcome: lsmetrics.OutcomeOK,
	}
	errLabels := map[string]string{
		lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet, lsmetrics.LabelOutcome: lsmetrics.OutcomeError,
	}
	okBefore := counterValue(t, lsmetrics.APIRequests, okLabels)
	errBefore := counterValue(t, lsmetrics.APIRequests, errLabels)

	if _, err := client.DoRequest(testVolumeURL, fakeRequest{}); err == nil {
		t.Fatal("the inner error must be returned")
	}

	if got := counterValue(t, lsmetrics.APIRequests, okLabels); got != okBefore {
		t.Errorf("a 403 was counted as a success: ok %v -> %v", okBefore, got)
	}
	if got := counterValue(t, lsmetrics.APIRequests, errLabels); got != errBefore+1 {
		t.Errorf("error outcome counter = %v, want %v", got, errBefore+1)
	}

	if got := counterValue(t, lsmetrics.APIRequestErrors, map[string]string{
		lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet,
		lsmetrics.LabelCode: string(lsdkErrs.EcPermissionDenied), lsmetrics.LabelStatus: "403",
	}); got < 1 {
		t.Errorf("error counter for the SDK code = %v, want >= 1", got)
	}
}

// The status label exists because the SDK code alone is often UnknownError -
// measured live, all three attach retries against a volume in IN-PROCESS
// state reported that one code. Without the status, a 429, a 409 and a 500
// collapse into one series.
func TestDoRequestRecordsTheHTTPStatusAlongsideTheCode(t *ltesting.T) {
	lsmetrics.InitializeRecorder()

	for _, tc := range []struct {
		name       string
		status     int
		wantStatus string
	}{
		{"a 409 keeps its status", lhttp.StatusConflict, "409"},
		{"a 500 keeps its status", lhttp.StatusInternalServerError, "500"},
	} {
		t.Run(tc.name, func(t *ltesting.T) {
			client := &throttledHTTPClient{
				inner:   &fakeHTTPClient{err: sdkErrWithStatus(tc.status, lsdkErrs.EcUnexpectedError)},
				limiter: newAdaptiveRateLimiter(),
			}

			labels := map[string]string{
				lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet,
				lsmetrics.LabelCode: string(lsdkErrs.EcUnexpectedError), lsmetrics.LabelStatus: tc.wantStatus,
			}
			before := counterValue(t, lsmetrics.APIRequestErrors, labels)

			if _, err := client.DoRequest(testVolumeURL, fakeRequest{}); err == nil {
				t.Fatal("the inner error must be returned")
			}

			if got := counterValue(t, lsmetrics.APIRequestErrors, labels); got != before+1 {
				t.Errorf("counter for status %s = %v, want %v", tc.wantStatus, got, before+1)
			}
		})
	}
}

// A transport-level failure never gets a status, and "none" has to be
// distinguishable from a real code - it is how a refused connection, a DNS
// failure and the 120s client timeout all present.
func TestDoRequestLabelsAMissingStatusAsNone(t *ltesting.T) {
	lsmetrics.InitializeRecorder()

	for _, tc := range []struct {
		name string
		err  lsdkErrs.IError
	}{
		{
			// The shape the SDK actually produces for a transport-level
			// failure: the parameter IS present and it is zero. Verified
			// against the SDK - WithKVparameters stores the zero rather than
			// dropping it - so this, not an absent parameter, is the case that
			// matters: a refused connection, a DNS failure and the 120s client
			// timeout all land here.
			name: "statusCode present and zero",
			err:  sdkErrWithStatus(0, lsdkErrs.EcUnexpectedError),
		},
		{
			name: "statusCode absent entirely",
			err:  new(lsdkErrs.SdkError).WithErrorCode(lsdkErrs.EcUnexpectedError),
		},
	} {
		t.Run(tc.name, func(t *ltesting.T) {
			client := &throttledHTTPClient{
				inner:   &fakeHTTPClient{err: tc.err},
				limiter: newAdaptiveRateLimiter(),
			}

			none := map[string]string{
				lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet,
				lsmetrics.LabelCode: string(lsdkErrs.EcUnexpectedError), lsmetrics.LabelStatus: lsmetrics.StatusNone,
			}
			zero := map[string]string{
				lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet,
				lsmetrics.LabelCode: string(lsdkErrs.EcUnexpectedError), lsmetrics.LabelStatus: "0",
			}

			before := counterValue(t, lsmetrics.APIRequestErrors, none)
			zeroBefore := counterValue(t, lsmetrics.APIRequestErrors, zero)

			if _, err := client.DoRequest(testVolumeURL, fakeRequest{}); err == nil {
				t.Fatal("the inner error must be returned")
			}

			if got := counterValue(t, lsmetrics.APIRequestErrors, none); got != before+1 {
				t.Errorf("counter for a status-less error = %v, want %v", got, before+1)
			}

			// "0" must never appear as a status: on a dashboard it reads as an
			// HTTP code, and there is no such code.
			if got := counterValue(t, lsmetrics.APIRequestErrors, zero); got != zeroBefore {
				t.Errorf("a status=\"0\" series was written (%v); 0 is not a status", got)
			}
		})
	}
}

// respHTTPClient returns a response ALONGSIDE an error, which is the shape of
// the SDK's generic error path - `return resp, ErrorHandler(resp.Err)` - and
// the shape the status label depends on.
type respHTTPClient struct {
	fakeHTTPClient
	status int
}

func (s *respHTTPClient) DoRequest(purl string, preq lsdkClient.IRequest) (*lreq.Response, lsdkErrs.IError) {
	_, err := s.fakeHTTPClient.DoRequest(purl, preq)

	return &lreq.Response{Response: &lhttp.Response{StatusCode: s.status}}, err
}

// The case live testing exposed: the SDK's generic error path attaches NO
// statusCode to the error, only to the response. Reading the status from the
// error alone labelled the driver's most frequent API error - an attach
// against a volume in IN-PROCESS state - as "none", the same as a transport
// failure.
func TestDoRequestTakesTheStatusFromTheResponseWhenTheErrorHasNone(t *ltesting.T) {
	lsmetrics.InitializeRecorder()

	client := &throttledHTTPClient{
		// Error with no statusCode parameter at all, response with 400.
		inner: &respHTTPClient{
			fakeHTTPClient: fakeHTTPClient{
				err: new(lsdkErrs.SdkError).WithErrorCode(lsdkErrs.EcUnexpectedError),
			},
			status: lhttp.StatusBadRequest,
		},
		limiter: newAdaptiveRateLimiter(),
	}

	want := map[string]string{
		lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet,
		lsmetrics.LabelCode: string(lsdkErrs.EcUnexpectedError), lsmetrics.LabelStatus: "400",
	}
	none := map[string]string{
		lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet,
		lsmetrics.LabelCode: string(lsdkErrs.EcUnexpectedError), lsmetrics.LabelStatus: lsmetrics.StatusNone,
	}
	before := counterValue(t, lsmetrics.APIRequestErrors, want)
	noneBefore := counterValue(t, lsmetrics.APIRequestErrors, none)

	if _, err := client.DoRequest(testVolumeURL, fakeRequest{}); err == nil {
		t.Fatal("the inner error must be returned")
	}

	if got := counterValue(t, lsmetrics.APIRequestErrors, want); got != before+1 {
		t.Errorf("status=400 counter = %v, want %v", got, before+1)
	}
	if got := counterValue(t, lsmetrics.APIRequestErrors, none); got != noneBefore {
		t.Errorf("the error was also counted as status=none (%v); the response carried 400", got)
	}
}

// A response object with no embedded HTTP response must not be dereferenced,
// and must fall through to the error. The SDK's own nil-guard is
// `resp != nil && resp.Response != nil`, so this shape occurs.
func TestDoRequestSurvivesAResponseWithNoHTTPResponse(t *ltesting.T) {
	lsmetrics.InitializeRecorder()

	client := &throttledHTTPClient{
		inner:   &emptyRespHTTPClient{err: sdkErrWithStatus(lhttp.StatusForbidden, lsdkErrs.EcPermissionDenied)},
		limiter: newAdaptiveRateLimiter(),
	}

	labels := map[string]string{
		lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet,
		lsmetrics.LabelCode: string(lsdkErrs.EcPermissionDenied), lsmetrics.LabelStatus: "403",
	}
	before := counterValue(t, lsmetrics.APIRequestErrors, labels)

	if _, err := client.DoRequest(testVolumeURL, fakeRequest{}); err == nil {
		t.Fatal("the inner error must be returned")
	}

	if got := counterValue(t, lsmetrics.APIRequestErrors, labels); got != before+1 {
		t.Errorf("status fell back to the error = %v, want %v", got, before+1)
	}
}

type emptyRespHTTPClient struct {
	fakeHTTPClient
	err lsdkErrs.IError
}

func (s *emptyRespHTTPClient) DoRequest(string, lsdkClient.IRequest) (*lreq.Response, lsdkErrs.IError) {
	return &lreq.Response{}, s.err
}

// A shed request never left the process, so it must not be timed: a 0s
// observation would pull the latency histogram DOWN at the exact moment the
// driver is most degraded, which is the opposite of what the graph should show.
func TestDoRequestShedIsCountedButNotTimed(t *ltesting.T) {
	lsmetrics.InitializeRecorder()

	inner := &fakeHTTPClient{}
	// 0.1 QPS with burst 1: the first token is free, the second needs a 10s
	// wait, which is past rateLimitMaxWait - so wait() sheds. Same construction
	// as TestAdaptiveRateLimiterShedsInsteadOfSleeping.
	client := &throttledHTTPClient{
		inner:   inner,
		limiter: &adaptiveRateLimiter{limiter: lrate.NewLimiter(0.1, 1), qps: 0.1},
	}
	if !client.limiter.wait() {
		t.Fatal("setup: the first token must be available")
	}

	labels := map[string]string{lsmetrics.LabelRoute: routeVolumeByID, lsmetrics.LabelMethod: lhttp.MethodGet}
	shedBefore := counterValue(t, lsmetrics.APIRequestsShed, labels)
	durBefore := histogramCount(t, lsmetrics.APIRequestDuration, labels)

	if _, err := client.DoRequest(testVolumeURL, fakeRequest{}); err == nil {
		t.Fatal("a shed request must return an error")
	}

	if inner.calls != 0 {
		t.Fatalf("inner called %d times; a shed request must not reach the network", inner.calls)
	}
	if got := counterValue(t, lsmetrics.APIRequestsShed, labels); got != shedBefore+1 {
		t.Errorf("shed counter = %v, want %v", got, shedBefore+1)
	}
	if got := histogramCount(t, lsmetrics.APIRequestDuration, labels); got != durBefore {
		t.Errorf("shed request was timed: histogram count %v -> %v", durBefore, got)
	}
}

func TestDoRequestPublishesLimiterQPS(t *ltesting.T) {
	lsmetrics.InitializeRecorder()
	client := &throttledHTTPClient{inner: &fakeHTTPClient{}, limiter: newAdaptiveRateLimiter()}

	if _, err := client.DoRequest(testVolumeURL, fakeRequest{}); err != nil {
		t.Fatalf("DoRequest error = %v", err)
	}

	got, ok := gaugeValue(t, lsmetrics.RateLimiterQPS)
	if !ok {
		t.Fatal("rate limiter gauge was never published")
	}
	if want := client.limiter.currentQPS(); got != want {
		t.Errorf("gauge = %v, want the limiter's current QPS %v", got, want)
	}
}

// Assertion helpers. They read back through the recorder's public Gather so a
// test proves what a scrape would actually show, not what the code intended.

func metricFamily(t *ltesting.T, pname string) *ldto.MetricFamily {
	t.Helper()

	families, err := lsmetrics.Recorder().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, f := range families {
		if f.GetName() == pname {
			return f
		}
	}

	return nil
}

func sampleWithLabels(pmf *ldto.MetricFamily, plabels map[string]string) *ldto.Metric {
	if pmf == nil {
		return nil
	}
	for _, m := range pmf.GetMetric() {
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

	return nil
}

// counterValue returns 0 for a series that does not exist yet, which is what
// makes before/after comparisons work on the first run of a test.
func counterValue(t *ltesting.T, pname string, plabels map[string]string) float64 {
	t.Helper()

	m := sampleWithLabels(metricFamily(t, pname), plabels)
	if m == nil {
		return 0
	}

	return m.GetCounter().GetValue()
}

func gaugeValue(t *ltesting.T, pname string) (float64, bool) {
	t.Helper()

	mf := metricFamily(t, pname)
	if mf == nil || len(mf.GetMetric()) == 0 {
		return 0, false
	}

	return mf.GetMetric()[0].GetGauge().GetValue(), true
}

func histogramSum(t *ltesting.T, pname string, plabels map[string]string) float64 {
	t.Helper()

	m := sampleWithLabels(metricFamily(t, pname), plabels)
	if m == nil {
		return 0
	}

	return m.GetHistogram().GetSampleSum()
}

func histogramCount(t *ltesting.T, pname string, plabels map[string]string) uint64 {
	t.Helper()

	m := sampleWithLabels(metricFamily(t, pname), plabels)
	if m == nil {
		return 0
	}

	return m.GetHistogram().GetSampleCount()
}

// familySampleCount sums every series in a counter family, for assertions of
// the form "this family did not move at all".
func familySampleCount(t *ltesting.T, pname string) float64 {
	t.Helper()

	mf := metricFamily(t, pname)
	if mf == nil {
		return 0
	}

	total := 0.0
	for _, m := range mf.GetMetric() {
		total += m.GetCounter().GetValue()
	}

	return total
}
