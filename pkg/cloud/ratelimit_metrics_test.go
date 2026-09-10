package cloud

import (
	lhttp "net/http"
	ltesting "testing"

	lreq "github.com/imroc/req/v3"
	lsdkClient "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/client"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
	lrate "golang.org/x/time/rate"
	ltime "time"

	ldto "github.com/prometheus/client_model/go"

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

	labels := map[string]string{"route": "volumes/{id}", "method": "GET"}
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
		map[string]string{"route": "volumes/{id}", "method": "GET", "outcome": lsmetrics.OutcomeOK})
	errsBefore := familySampleCount(t, lsmetrics.APIRequestErrors)

	if _, err := client.DoRequest(testVolumeURL, fakeRequest{}); err != nil {
		t.Fatalf("DoRequest error = %v, want nil", err)
	}

	after := counterValue(t, lsmetrics.APIRequests,
		map[string]string{"route": "volumes/{id}", "method": "GET", "outcome": lsmetrics.OutcomeOK})
	if after != before+1 {
		t.Errorf("ok counter = %v, want %v", after, before+1)
	}

	if got := histogramCount(t, lsmetrics.APIRequestDuration,
		map[string]string{"route": "volumes/{id}", "method": "GET"}); got == 0 {
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

	labels := map[string]string{"route": "volumes/{id}", "method": "GET"}
	before := counterValue(t, lsmetrics.APIRequestThrottles, labels)

	if _, err := client.DoRequest(testVolumeURL, fakeRequest{}); err == nil {
		t.Fatal("the inner error must be returned")
	}

	if after := counterValue(t, lsmetrics.APIRequestThrottles, labels); after != before+1 {
		t.Errorf("throttle counter = %v, want %v", after, before+1)
	}

	if got := counterValue(t, lsmetrics.APIRequests, map[string]string{
		"route": "volumes/{id}", "method": "GET", "outcome": lsmetrics.OutcomeThrottled,
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
		"route": "volumes/{id}", "method": "GET", "outcome": lsmetrics.OutcomeOK,
	}
	errLabels := map[string]string{
		"route": "volumes/{id}", "method": "GET", "outcome": lsmetrics.OutcomeError,
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
		"route": "volumes/{id}", "method": "GET", "code": string(lsdkErrs.EcPermissionDenied),
	}); got < 1 {
		t.Errorf("error counter for the SDK code = %v, want >= 1", got)
	}
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

	labels := map[string]string{"route": "volumes/{id}", "method": "GET"}
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
