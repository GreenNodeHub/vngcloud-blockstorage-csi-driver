package cloud

import (
	ltesting "testing"
	ltime "time"

	lreq "github.com/imroc/req/v3"
	lsdkClient "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/client"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
	lrate "golang.org/x/time/rate"
)

// sdkErrWithStatus reproduces exactly how the SDK packages an HTTP error:
// defaultErrorResponse stores statusCode in the parameters, then
// SdkErrorHandler forces the error code to PermissionDenied and returns that
// SAME object.
func sdkErrWithStatus(pstatus int, pcode lsdkErrs.ErrorCode) lsdkErrs.IError {
	return new(lsdkErrs.SdkError).
		WithErrorCode(pcode).
		WithKVparameters("statusCode", pstatus, "url", "https://vserver/volumes")
}

// TestIsThrottled: a 429 must be distinguishable from a 403 even though the
// SDK flattens both into PermissionDenied. That very ambiguity once
// misdirected the diagnosis of an lb-controller incident ("permission denied"
// when the truth was quota exhaustion).
func TestIsThrottled(t *ltesting.T) {
	tcs := []struct {
		name string
		err  lsdkErrs.IError
		want bool
	}{
		{"429 despite PermissionDenied error code", sdkErrWithStatus(429, lsdkErrs.EcPermissionDenied), true},
		{"a real 403 is not throttling", sdkErrWithStatus(403, lsdkErrs.EcPermissionDenied), false},
		{"500 is not throttling", sdkErrWithStatus(500, lsdkErrs.EcUnknownError), false},
		{"error without statusCode", new(lsdkErrs.SdkError).WithErrorCode(lsdkErrs.EcUnknownError), false},
		{"no error", nil, false},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *ltesting.T) {
			if got := isThrottled(tc.err); got != tc.want {
				t.Fatalf("isThrottled() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAdaptiveRateLimiterDecreasesOnThrottle(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	now := ltime.Unix(0, 0)

	if got := rl.currentQPS(); got != rateLimitMaxQPS {
		t.Fatalf("initial QPS = %v, want %v", got, rateLimitMaxQPS)
	}

	// Each iteration is a SEPARATE throttle event, spaced past the cooldown.
	// A burst of 429s arriving together is one event, and is covered by
	// TestAdaptiveRateLimiterCollapsesAConcurrentThrottleBurst.
	want := rateLimitMaxQPS
	for i := 0; i < 10; i++ {
		now = now.Add(rateLimitDecreaseCooldown)
		rl.onThrottled(now)
		want *= rateLimitDecreaseFactor
		if want < rateLimitMinQPS {
			want = rateLimitMinQPS
		}

		if got := rl.currentQPS(); got != want {
			t.Fatalf("after %d throttles: QPS = %v, want %v", i+1, got, want)
		}
	}

	if got := rl.currentQPS(); got != rateLimitMinQPS {
		t.Fatalf("QPS must stop at the floor %v, got %v", rateLimitMinQPS, got)
	}
}

func TestAdaptiveRateLimiterRecoveryIsGatedAndCapped(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	now := ltime.Unix(0, 0)

	rl.onThrottled(now)
	dropped := rl.currentQPS()

	// Immediately after a throttle there must be no recovery yet.
	rl.onSuccess(now.Add(rateLimitRecoverEvery - ltime.Millisecond))
	if got := rl.currentQPS(); got != dropped {
		t.Fatalf("recovered too early: QPS = %v, want to hold %v", got, dropped)
	}

	// Past the quiet window it climbs gradually, and never exceeds the ceiling.
	at := now
	for i := 0; i < 100; i++ {
		at = at.Add(rateLimitRecoverEvery)
		rl.onSuccess(at)
		if got := rl.currentQPS(); got > rateLimitMaxQPS {
			t.Fatalf("QPS = %v exceeds the ceiling %v", got, rateLimitMaxQPS)
		}
	}

	if got := rl.currentQPS(); got != rateLimitMaxQPS {
		t.Fatalf("after a long quiet spell, QPS = %v, want back at %v", got, rateLimitMaxQPS)
	}
}

// TestAdaptiveRateLimiterShedsInsteadOfSleeping pins the most important
// property: never sleep for long inside the handler, because the handler is
// holding the inflight lock.
func TestAdaptiveRateLimiterShedsInsteadOfSleeping(t *ltesting.T) {
	// 0.1 QPS, burst 1 => the second token needs a 10s wait, beyond rateLimitMaxWait.
	rl := &adaptiveRateLimiter{limiter: lrate.NewLimiter(0.1, 1), qps: 0.1}

	if !rl.wait() {
		t.Fatal("the first token must be available immediately")
	}

	start := ltime.Now()
	if rl.wait() {
		t.Fatal("the second token must be refused instead of waiting 10s")
	}

	if elapsed := ltime.Since(start); elapsed > ltime.Second {
		t.Fatalf("wait() slept %v before refusing; it must return immediately", elapsed)
	}
}

// fakeHTTPClient stands in for lsdkClient.IHttpClient to test the decorator.
type fakeHTTPClient struct {
	calls int
	err   lsdkErrs.IError
}

func (s *fakeHTTPClient) DoRequest(_ string, _ lsdkClient.IRequest) (*lreq.Response, lsdkErrs.IError) {
	s.calls++

	return nil, s.err
}

func (s *fakeHTTPClient) WithRetryCount(int) lsdkClient.IHttpClient             { return s }
func (s *fakeHTTPClient) WithTimeout(ltime.Duration) lsdkClient.IHttpClient     { return s }
func (s *fakeHTTPClient) WithSleep(ltime.Duration) lsdkClient.IHttpClient       { return s }
func (s *fakeHTTPClient) WithKvDefaultHeaders(...string) lsdkClient.IHttpClient { return s }
func (s *fakeHTTPClient) WithReauthFunc(
	lsdkClient.AuthOpts,
	func() (lsdkClient.ISdkAuthentication, lsdkErrs.IError),
) lsdkClient.IHttpClient {
	return s
}

type fakeRequest struct{ lsdkClient.IRequest }

func (fakeRequest) GetRequestMethod() string { return "GET" }

func TestThrottledHTTPClientReactsTo429(t *ltesting.T) {
	inner := &fakeHTTPClient{err: sdkErrWithStatus(429, lsdkErrs.EcPermissionDenied)}
	client := &throttledHTTPClient{inner: inner, limiter: newAdaptiveRateLimiter()}

	before := client.limiter.currentQPS()
	if _, err := client.DoRequest("https://vserver/volumes", fakeRequest{}); err == nil {
		t.Fatal("the inner client error must be returned unchanged")
	}

	if inner.calls != 1 {
		t.Fatalf("inner called %d times, want 1", inner.calls)
	}

	if after := client.limiter.currentQPS(); after >= before {
		t.Fatalf("QPS did not drop on a 429: before %v, after %v", before, after)
	}
}

func TestThrottledHTTPClientIgnoresNonThrottleErrors(t *ltesting.T) {
	inner := &fakeHTTPClient{err: sdkErrWithStatus(403, lsdkErrs.EcPermissionDenied)}
	client := &throttledHTTPClient{inner: inner, limiter: newAdaptiveRateLimiter()}

	before := client.limiter.currentQPS()
	if _, err := client.DoRequest("https://vserver/volumes", fakeRequest{}); err == nil {
		t.Fatal("the inner client error must be returned unchanged")
	}

	if after := client.limiter.currentQPS(); after != before {
		t.Fatalf("a genuine 403 must not reduce QPS: before %v, after %v", before, after)
	}
}

// TestErrClientRateLimitedCarriesAnError pins the worst bug of the first
// draft: an IError without WithErrors() has a nil GetError(), and every caller
// written as `return sdkErr.GetError()` turns a shed request into a SILENT
// SUCCESS. On the DetachVolume path that makes external-attacher delete the
// VolumeAttachment while the disk is still attached to the node.
func TestErrClientRateLimitedCarriesAnError(t *ltesting.T) {
	err := errClientRateLimited("https://vserver/volumes/vol-1")

	if err.GetError() == nil {
		t.Fatal("GetError() = nil; a caller doing `return sdkErr.GetError()` reports false success")
	}

	if err.GetErrorCode() != ecCsiClientRateLimited {
		t.Fatalf("error code = %v, want %v", err.GetErrorCode(), ecCsiClientRateLimited)
	}
}

// TestAdaptiveRateLimiterShrinksBurst: lowering only the rate while keeping
// the burst means a scale-up still gets the whole bucket admitted at once -
// the very burst that caused the 429s.
func TestAdaptiveRateLimiterShrinksBurst(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	now := ltime.Unix(0, 0)

	before := rl.limiter.Burst()
	for i := 0; i < 5; i++ {
		rl.onThrottled(now)
	}

	after := rl.limiter.Burst()
	if after >= before {
		t.Fatalf("burst did not shrink with the rate: before %d, after %d", before, after)
	}
	if after < 1 {
		t.Fatalf("burst = %d, must not go below 1", after)
	}
}

// TestThrottledHTTPClientRecoversAfterNonThrottleErrors: if recovery only ran
// on sdkErr == nil, a sustained 5xx spell would pin the limiter at the 1 QPS
// floor indefinitely.
func TestThrottledHTTPClientRecoversAfterNonThrottleErrors(t *ltesting.T) {
	inner := &fakeHTTPClient{err: sdkErrWithStatus(429, lsdkErrs.EcPermissionDenied)}
	client := &throttledHTTPClient{inner: inner, limiter: newAdaptiveRateLimiter()}

	if _, err := client.DoRequest("https://vserver/volumes", fakeRequest{}); err == nil {
		t.Fatal("expected an error from the inner client")
	}
	dropped := client.limiter.currentQPS()

	// Move past the recovery window, then return a 404 - neither a 429 nor a
	// 5xx. A 500 used to stand here, but it now carries its own meaning
	// (overload, hold the rate); see TestSuccessesDoNotClimbWhileServerErrorsAreRecent.
	client.limiter.lastAdj = ltime.Now().Add(-2 * rateLimitRecoverEvery)
	inner.err = sdkErrWithStatus(404, lsdkErrs.EcUnknownError)

	if _, err := client.DoRequest("https://vserver/volumes", fakeRequest{}); err == nil {
		t.Fatal("expected an error from the inner client")
	}

	if got := client.limiter.currentQPS(); got <= dropped {
		t.Fatalf("a 404 is neither throttling nor overload, recovery must proceed: before %v, after %v", dropped, got)
	}
}

// TestAdaptiveRateLimiterCollapsesAConcurrentThrottleBurst pins the behaviour
// that the TS-B load test found missing on 29/08/2026: with worker-threads=100
// a single throttle event arrives as a BURST of 429s, one per in-flight
// request. Halving once per 429 collapsed the limiter 20 -> 1 QPS in ~100ms,
// after which recovery took (20-1)/1 * 5s = 95s. One throttle event must cost
// exactly one halving.
func TestAdaptiveRateLimiterCollapsesAConcurrentThrottleBurst(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	now := ltime.Unix(0, 0)

	// Eight concurrent requests all bounce off the same throttle event.
	for i := 0; i < 8; i++ {
		rl.onThrottled(now.Add(ltime.Duration(i) * ltime.Millisecond))
	}

	want := rateLimitMaxQPS * rateLimitDecreaseFactor
	if got := rl.currentQPS(); got != want {
		t.Fatalf("a burst of 8 concurrent 429s dropped QPS to %v, want a single halving to %v", got, want)
	}
}

// TestAdaptiveRateLimiterKeepsBackingOffOnSustainedThrottling: the cooldown
// must not blunt the response to throttling that genuinely persists.
func TestAdaptiveRateLimiterKeepsBackingOffOnSustainedThrottling(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	at := ltime.Unix(0, 0)

	rl.onThrottled(at)
	first := rl.currentQPS()

	at = at.Add(rateLimitDecreaseCooldown)
	rl.onThrottled(at)

	want := first * rateLimitDecreaseFactor
	if got := rl.currentQPS(); got != want {
		t.Fatalf("a throttle past the cooldown left QPS at %v, want a further halving to %v", got, want)
	}
}

// A throttle spell that genuinely persists must still walk the rate down to the
// floor - the cooldown spaces the decreases out, it must not cap them.
func TestAdaptiveRateLimiterStillReachesFloorUnderSustainedThrottling(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	at := ltime.Unix(0, 0)

	// 60s of unbroken throttling, requests arriving every 100ms.
	for i := 0; i < 600; i++ {
		at = at.Add(100 * ltime.Millisecond)
		rl.onThrottled(at)
	}

	if got := rl.currentQPS(); got != rateLimitMinQPS {
		t.Fatalf("after a sustained throttle spell QPS = %v, want the floor %v", got, rateLimitMinQPS)
	}
}

// The TS-B rerun on 30/08/2026 measured vServer 500s tracking load exactly: none
// at N<=25, 16 at N=50, 98 at N=100. A 5xx under that pattern is the service
// saying it is overloaded, so the limiter must stop climbing. It must NOT back
// off either - a 5xx spell unrelated to quota would then starve the driver at
// the floor for a reason that has nothing to do with it.

func TestServerErrorsDoNotReduceTheRate(t *ltesting.T) {
	inner := &fakeHTTPClient{err: sdkErrWithStatus(500, lsdkErrs.EcUnknownError)}
	client := &throttledHTTPClient{inner: inner, limiter: newAdaptiveRateLimiter()}

	before := client.limiter.currentQPS()
	if _, err := client.DoRequest("https://vserver/volumes", fakeRequest{}); err == nil {
		t.Fatal("the inner client error must be returned unchanged")
	}

	if after := client.limiter.currentQPS(); after != before {
		t.Fatalf("a 500 changed QPS from %v to %v; it must hold steady", before, after)
	}
}

// The subtle one. During the N=100 run the 98 failures were mixed in with ~370
// requests, most of which succeeded. If each success is still allowed to climb,
// the freeze is cancelled out by its own neighbours and the driver keeps
// accelerating into an overloaded service.
func TestSuccessesDoNotClimbWhileServerErrorsAreRecent(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	at := ltime.Unix(0, 0)

	// Drop below the ceiling so there is headroom to climb into.
	rl.onThrottled(at)
	dropped := rl.currentQPS()

	// The service is failing intermittently: 5xx keep arriving, interleaved
	// with successes, for a minute. That is the shape the N=100 run had - 98
	// failures scattered through ~370 requests.
	for i := 0; i < 10; i++ {
		at = at.Add(ltime.Second)
		rl.onServerError(at)

		at = at.Add(rateLimitRecoverEvery)
		rl.onSuccess(at)

		if got := rl.currentQPS(); got != dropped {
			t.Fatalf("QPS climbed to %v while 5xx were still arriving; want to hold %v", got, dropped)
		}
	}
}

func TestRecoveryResumesOnceServerErrorsStop(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	at := ltime.Unix(0, 0)

	rl.onThrottled(at)
	dropped := rl.currentQPS()

	at = at.Add(ltime.Second)
	rl.onServerError(at)

	// Quiet for longer than the 5xx window, then a success.
	at = at.Add(rateLimitServerErrorQuiet + rateLimitRecoverEvery)
	rl.onSuccess(at)

	if got := rl.currentQPS(); got <= dropped {
		t.Fatalf("QPS = %v after the 5xx spell ended; want it climbing above %v", got, dropped)
	}
}
