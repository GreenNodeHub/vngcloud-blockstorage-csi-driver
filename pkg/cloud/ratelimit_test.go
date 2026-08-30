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

// TestThrottledHTTPClientRecoversAfterNonThrottleErrors: an error that is
// neither a 429 nor a 5xx (a 404, a genuine 403) is not a quota or overload
// signal, so it must not block recovery - otherwise any persistent non-quota
// failure would pin the limiter low for reasons unrelated to it.
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

// The 5xx policy. The evidence and the reasoning behind it live in one place:
// the comment block on rateLimitServerErrorDecreaseFactor in ratelimit.go.
// In short: 5xx tracked load in the TS-B reruns, so it is backpressure - back
// off mildly (even at the ceiling), do not climb while it continues, but never
// let it pin the limiter down forever (rateLimitServerErrorHoldMax).

// sdkServerError builds an IError exactly as SDK v2.21.0 does for a 500/503:
// WithErrorInternalServerError/WithErrorServiceMaintenance assign a dedicated
// error code (unlike 429, which is flattened into EcPermissionDenied).
func sdkServerError(pstatus int) lsdkErrs.IError {
	code := lsdkErrs.EcInternalServerError
	if pstatus == 503 {
		code = lsdkErrs.EcServiceMaintenance
	}

	return sdkErrWithStatus(pstatus, code)
}

// TestIsServerError pins the classification to the SDK's error codes. The
// statusCode parameter is NOT usable here: the SDK only stashes it for
// 401/429/500/503/403, so a `statusCode >= 500` check silently misses
// everything a gateway mints (502, 504).
func TestIsServerError(t *ltesting.T) {
	tcs := []struct {
		name string
		err  lsdkErrs.IError
		want bool
	}{
		{"500 carries EcInternalServerError", sdkServerError(500), true},
		{"503 carries EcServiceMaintenance", sdkServerError(503), true},
		// The SDK's ErrorHandler also assigns these codes by message pattern on
		// paths that never stash a statusCode - classification must not depend
		// on the parameter map.
		{"5xx code without a statusCode parameter",
			new(lsdkErrs.SdkError).WithErrorCode(lsdkErrs.EcInternalServerError), true},
		{"a 429 is throttling, not overload", sdkErrWithStatus(429, lsdkErrs.EcPermissionDenied), false},
		{"a genuine 403", sdkErrWithStatus(403, lsdkErrs.EcPermissionDenied), false},
		{"unclassified error", new(lsdkErrs.SdkError).WithErrorCode(lsdkErrs.EcUnknownError), false},
		{"no error", nil, false},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *ltesting.T) {
			if got := isServerError(tc.err); got != tc.want {
				t.Fatalf("isServerError() = %v, want %v", got, tc.want)
			}
		})
	}
}

// End-to-end through DoRequest, at the ceiling - the exact state the motivating
// incident was in (the N=100 rerun had zero 429s, so qps sat at max the whole
// time). A freeze would be a no-op here; real backpressure must reduce.
// Because the assertion expects a CHANGE, this test also fails if the switch
// misroutes the 500 into the onSuccess branch - unlike a hold-steady assertion,
// which is vacuously true at the ceiling.
func TestServerErrorBacksOffMildlyThroughDoRequest(t *ltesting.T) {
	inner := &fakeHTTPClient{err: sdkServerError(500)}
	client := &throttledHTTPClient{inner: inner, limiter: newAdaptiveRateLimiter()}

	if _, err := client.DoRequest("https://vserver/volumes", fakeRequest{}); err == nil {
		t.Fatal("the inner client error must be returned unchanged")
	}

	want := rateLimitMaxQPS * rateLimitServerErrorDecreaseFactor
	if got := client.limiter.currentQPS(); got != want {
		t.Fatalf("one 500 through DoRequest left QPS at %v, want a mild decrease to %v", got, want)
	}
}

// One 5xx event arrives as a burst, one per in-flight request - same shape as
// the 429 case, same rule: one event, one decrease.
func TestServerErrorBurstCountsOnce(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	now := ltime.Unix(0, 0)

	for i := 0; i < 8; i++ {
		rl.onServerError(now.Add(ltime.Duration(i) * ltime.Millisecond))
	}

	want := rateLimitMaxQPS * rateLimitServerErrorDecreaseFactor
	if got := rl.currentQPS(); got != want {
		t.Fatalf("a burst of 8 concurrent 500s dropped QPS to %v, want a single step to %v", got, want)
	}
}

// Interleaved failures and successes (the N=100 shape: 98 failures scattered
// through ~370 requests) must not let the successes climb the rate back up
// while the spell lasts.
func TestSuccessesDoNotClimbWhileServerErrorsAreRecent(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	at := ltime.Unix(0, 0)

	// The timeline below assumes each success lands inside the quiet window of
	// the 5xx before it, and consecutive 5xx keep the spell alive.
	errToSuccess := rateLimitRecoverEvery // past the recovery gate, so only the 5xx gate can stop the climb
	if errToSuccess+ltime.Second >= rateLimitServerErrorQuiet {
		t.Fatalf("timeline broken by retuning: %v + 1s must stay below %v", rateLimitRecoverEvery, rateLimitServerErrorQuiet)
	}

	rl.onServerError(at)
	prev := rl.currentQPS()

	// Stay well inside rateLimitServerErrorHoldMax.
	for i := 0; i < 5; i++ {
		at = at.Add(ltime.Second)
		rl.onServerError(at)

		at = at.Add(errToSuccess)
		rl.onSuccess(at)

		got := rl.currentQPS()
		if got > prev {
			t.Fatalf("QPS climbed %v -> %v while 5xx were still arriving", prev, got)
		}
		prev = got
	}
}

// A sustained pure-5xx storm must keep walking the rate down to the floor,
// exactly as a 429 storm does - the mild factor only changes the pace.
func TestSustainedServerErrorsReachTheFloor(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	at := ltime.Unix(0, 0)

	for i := 0; i < 60; i++ {
		at = at.Add(rateLimitDecreaseCooldown)
		rl.onServerError(at)
	}

	if got := rl.currentQPS(); got != rateLimitMinQPS {
		t.Fatalf("after a sustained 5xx storm QPS = %v, want the floor %v", got, rateLimitMinQPS)
	}
}

// The anti-starvation cap. A single poisoned resource whose GET returns 500
// forever, polled every few seconds, re-arms the quiet window indefinitely.
// Without a cap that would pin the process-wide limiter down for the lifetime
// of the poisoned resource - the exact self-starvation the pre-5xx code
// existed to prevent. Past rateLimitServerErrorHoldMax the successes must be
// allowed to climb again, 5xx or not.
func TestClimbResumesOncePoisonedResourceOutlivesTheHoldCap(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	at := ltime.Unix(0, 0)

	rl.onServerError(at) // the spell begins

	// The poisoned resource keeps failing every 6s - inside the quiet window,
	// so the spell never ends - until well past the hold cap.
	for at.Sub(ltime.Unix(0, 0)) < rateLimitServerErrorHoldMax+rateLimitServerErrorQuiet {
		at = at.Add(6 * ltime.Second)
		rl.onServerError(at)
	}

	// Healthy traffic alongside it: successes past the recovery gate.
	before := rl.currentQPS()
	at = at.Add(rateLimitRecoverEvery)
	rl.onSuccess(at)

	if got := rl.currentQPS(); got <= before {
		t.Fatalf("QPS = %v after the hold cap expired; want it climbing above %v", got, before)
	}
}

func TestRecoveryResumesOnceServerErrorsStop(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	at := ltime.Unix(0, 0)

	rl.onThrottled(at)

	at = at.Add(ltime.Second)
	rl.onServerError(at)
	held := rl.currentQPS()

	// Quiet for longer than the 5xx window, then a success.
	at = at.Add(rateLimitServerErrorQuiet + rateLimitRecoverEvery)
	rl.onSuccess(at)

	if got := rl.currentQPS(); got <= held {
		t.Fatalf("QPS = %v after the 5xx spell ended; want it climbing above %v", got, held)
	}
}
