package cloud

import (
	lctx "context"
	lfmt "fmt"
	lhttp "net/http"
	lsync "sync"
	ltime "time"

	lreq "github.com/imroc/req/v3"
	lsdkClient "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/client"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
	lrate "golang.org/x/time/rate"
	llog "k8s.io/klog/v2"
)

// Client-side adaptive throttling for every request leaving for vServer.
//
// Compared against aws-ebs-csi-driver pkg/cloud/retry_manager.go: AWS attaches
// a SEPARATE retry.NewAdaptiveMode() to each mutating API, because "the AWS SDK
// throttles on a retryer object level, not by API name" and AWS accounts can
// raise limits per API independently.
//
// That split is deliberately NOT copied here. vServer's quota is ONE bucket
// shared per PROJECT (~1000 requests / 60s), and that bucket is further shared
// with capv, lb-controller, cluster-autoscaler... in the same project.
// Splitting the limiter per API would let total throughput exceed the real
// bucket. Hence a SINGLE limiter shared by every request of the process.
//
// Why it lives at the HTTP layer rather than in pkg/cloud: the SDK converts 429
// into PermissionDenied before returning (vngcloud/client/http.go, case
// lhttp.StatusTooManyRequests -> lserr.WithErrorPermissionDenied()). Layers
// above can no longer tell "out of quota" from "not allowed" - the exact
// ambiguity that once misdirected the diagnosis of an lb-controller incident.
// Only here is the raw statusCode still readable, because
// defaultErrorResponse() stores it in the IError parameters and
// SdkErrorHandler returns that very object.
const (
	// Throughput ceiling. Deliberately set ABOVE the project bucket: in a
	// healthy system the limiter barely intervenes, and it only squeezes after
	// seeing a real 429. Same philosophy as AdaptiveMode ("restricts attempts
	// of API calls that recently hit throttle errors"), not a guessed static
	// quota.
	rateLimitMaxQPS = 20.0

	// Floor. Below this the driver is effectively stalled, which is never useful.
	rateLimitMinQPS = 1.0

	// Multiplicative decrease on a 429.
	rateLimitDecreaseFactor = 0.5

	// Additive increase every rateLimitRecoverEvery while no longer throttled.
	rateLimitIncreaseQPS  = 1.0
	rateLimitRecoverEvery = 5 * ltime.Second

	// Maximum time to wait for a token. Beyond this, fail fast so the CO
	// retries, instead of sleeping inside the handler - the handler is holding
	// the inflight lock.
	rateLimitMaxWait = 5 * ltime.Second

	ecCsiClientRateLimited = lsdkErrs.ErrorCode("CsiClientRateLimited")
)

// adaptiveRateLimiter is a token bucket whose rate follows AIMD: back off
// sharply when throttled, climb back gradually while quiet.
type adaptiveRateLimiter struct {
	mu      lsync.Mutex
	limiter *lrate.Limiter
	qps     float64
	lastAdj ltime.Time
}

func newAdaptiveRateLimiter() *adaptiveRateLimiter {
	return &adaptiveRateLimiter{
		limiter: lrate.NewLimiter(lrate.Limit(rateLimitMaxQPS), int(rateLimitMaxQPS)),
		qps:     rateLimitMaxQPS,
	}
}

// onThrottled: saw a 429 -> back the rate off sharply.
func (s *adaptiveRateLimiter) onThrottled(pnow ltime.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := s.qps * rateLimitDecreaseFactor
	if next < rateLimitMinQPS {
		next = rateLimitMinQPS
	}

	s.lastAdj = pnow
	if next == s.qps {
		return
	}

	llog.InfoS("[WARN] - rateLimiter: vServer returned 429, backing off",
		"fromQPS", s.qps, "toQPS", next)
	s.setRateLocked(next)
}

// onSuccess: after rateLimitRecoverEvery of quiet, climb back gradually.
func (s *adaptiveRateLimiter) onSuccess(pnow ltime.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.qps >= rateLimitMaxQPS {
		return
	}

	if pnow.Sub(s.lastAdj) < rateLimitRecoverEvery {
		return
	}

	next := s.qps + rateLimitIncreaseQPS
	if next > rateLimitMaxQPS {
		next = rateLimitMaxQPS
	}

	llog.V(2).InfoS("[DEBUG] - rateLimiter: recovering", "fromQPS", s.qps, "toQPS", next)
	s.lastAdj = pnow
	s.setRateLocked(next)
}

// setRateLocked adjusts BOTH the rate AND the burst. Changing only the rate is
// not enough: burst is the bucket size, so leaving it at 20 means that after
// four 429s (rate down to 1.25 QPS) a scale-up burst still gets 20 requests
// admitted at once as soon as the bucket refills - exactly the burst that
// caused the 429 storm.
func (s *adaptiveRateLimiter) setRateLocked(pqps float64) {
	burst := int(pqps)
	if burst < 1 {
		burst = 1
	}

	s.qps = pqps
	s.limiter.SetLimit(lrate.Limit(pqps))
	s.limiter.SetBurst(burst)
}

// currentQPS exists for observability and for tests.
func (s *adaptiveRateLimiter) currentQPS() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.qps
}

// wait acquires one token. Returns false when the wait would exceed
// rateLimitMaxWait - the caller should then fail fast for the CO to retry
// rather than sleep while holding the inflight lock.
//
// Implemented with Limiter.Wait over an internal deadline instead of a
// hand-rolled Reserve/Delay/Cancel/Sleep: Wait returns tokens correctly when
// the deadline preempts it, whereas Reserve+Cancel under-returns once later
// reservations exist, so a burst of shed attempts would permanently burn
// bucket capacity and push genuine waiters over the threshold. (DoRequest has
// no caller context to thread in - the SDK interface does not carry one - so
// the deadline here is the best available bound.)
func (s *adaptiveRateLimiter) wait() bool {
	ctx, cancel := lctx.WithTimeout(lctx.Background(), rateLimitMaxWait)
	defer cancel()

	return s.limiter.Wait(ctx) == nil
}

// isThrottled reads the raw statusCode the SDK stashes in the IError
// parameters before flattening the error into PermissionDenied.
func isThrottled(perr lsdkErrs.IError) bool {
	if perr == nil {
		return false
	}

	code, ok := perr.GetParameters()["statusCode"]
	if !ok {
		return false
	}

	status, ok := code.(int)

	return ok && status == lhttp.StatusTooManyRequests
}

// throttledHTTPClient wraps lsdkClient.IHttpClient so that every request goes
// through the shared limiter.
type throttledHTTPClient struct {
	inner   lsdkClient.IHttpClient
	limiter *adaptiveRateLimiter
}

// NewThrottledHTTPClient builds the throttled http client for ONE process.
// Controller and node plugin are separate processes, so each gets its own
// limiter; that is the honest limit of this approach - the bucket cannot be
// shared across processes.
//
// Exported because the node plugin may build its own SDK client too; if that
// client does not come through here, the entire DaemonSet's traffic goes
// uncounted - and that fan-out is precisely what drains the project bucket
// fastest during a node-group scale-up.
func NewThrottledHTTPClient(pctx lctx.Context) lsdkClient.IHttpClient {
	inner := lsdkClient.NewHttpClient(pctx)

	// The timeout is NOT set here: the SDK's 120s default (v2.21.0) is correct,
	// and the reasoning for that number lives in the SDK - where it applies to
	// every consumer. Restating it here would only create a second place to
	// keep in sync.
	//
	// Retry, however, is turned off. SDK v2.21.0's retry is far healthier
	// (backoff with jitter, only GET/HEAD/OPTIONS/TRACE are retried), but for
	// this driver it remains both redundant and harmful:
	//
	//   - Redundant: every read this driver makes already sits inside a poll
	//     loop that retries, with its own backoff, bounded by the ctx. A second
	//     retry layer only multiplies the request count.
	//   - Harmful: one DoRequest call could become 4 x 120s plus backoff.
	//     ExponentialBackoffWithContext only tests ctx.Done() BETWEEN condition
	//     calls, so it cannot interrupt a request already in flight - the
	//     handler would outlive the sidecar deadline while still holding the
	//     inflight lock.
	//
	// Dropping retry bounds the worst case of a single call at exactly 120s.
	inner.WithRetryCount(0)

	return &throttledHTTPClient{
		inner:   inner,
		limiter: newAdaptiveRateLimiter(),
	}
}

func (s *throttledHTTPClient) DoRequest(purl string, preq lsdkClient.IRequest) (*lreq.Response, lsdkErrs.IError) {
	if !s.limiter.wait() {
		llog.InfoS("[WARN] - rateLimiter: shedding request, token wait exceeds budget",
			"url", purl, "method", preq.GetRequestMethod(), "qps", s.limiter.currentQPS())

		return nil, errClientRateLimited(purl)
	}

	resp, sdkErr := s.inner.DoRequest(purl, preq)
	if isThrottled(sdkErr) {
		s.limiter.onThrottled(ltime.Now())
		// Record the truth the SDK is about to mask: this is a 429, not a 403.
		llog.InfoS("[WARN] - rateLimiter: request throttled by vServer (HTTP 429, reported as PermissionDenied)",
			"url", purl, "method", preq.GetRequestMethod())
	} else {
		// Any outcome that is NOT a 429 is evidence we are no longer being
		// quota-squeezed - including a 500 or a 404. If recovery only ran on
		// sdkErr == nil, a sustained 5xx spell would pin the limiter at the
		// 1 QPS floor indefinitely, leaving the driver self-starved for a
		// reason unrelated to quota once the service comes back.
		s.limiter.onSuccess(ltime.Now())
	}

	return resp, sdkErr
}

func (s *throttledHTTPClient) WithRetryCount(pretryCount int) lsdkClient.IHttpClient {
	s.inner.WithRetryCount(pretryCount)

	return s
}

func (s *throttledHTTPClient) WithTimeout(ptimeout ltime.Duration) lsdkClient.IHttpClient {
	s.inner.WithTimeout(ptimeout)

	return s
}

func (s *throttledHTTPClient) WithSleep(psleep ltime.Duration) lsdkClient.IHttpClient {
	s.inner.WithSleep(psleep)

	return s
}

func (s *throttledHTTPClient) WithKvDefaultHeaders(pargs ...string) lsdkClient.IHttpClient {
	s.inner.WithKvDefaultHeaders(pargs...)

	return s
}

func (s *throttledHTTPClient) WithReauthFunc(
	pauthOpt lsdkClient.AuthOpts,
	preauthFunc func() (lsdkClient.ISdkAuthentication, lsdkErrs.IError),
) lsdkClient.IHttpClient {
	s.inner.WithReauthFunc(pauthOpt, preauthFunc)

	return s
}

// errClientRateLimited MUST carry a real error.
//
// IError.GetError() returns whatever WithErrors() loaded; without it,
// GetError() is nil and every caller written as `return sdkErr.GetError()`
// turns a shed request into a SILENT SUCCESS. On the DetachVolume path that
// would make external-attacher delete the VolumeAttachment while the disk is
// still attached to the node - the exact class of damage this branch exists
// to fix.
func errClientRateLimited(purl string) lsdkErrs.IError {
	return new(lsdkErrs.SdkError).
		WithErrorCode(ecCsiClientRateLimited).
		WithMessage("request shed by the CSI driver client-side rate limiter").
		WithErrors(lfmt.Errorf("request to %s shed by the client-side rate limiter", purl)).
		WithKVparameters("url", purl)
}
