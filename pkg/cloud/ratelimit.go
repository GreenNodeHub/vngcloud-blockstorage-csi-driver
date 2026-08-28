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

// Client-side adaptive throttling cho moi request di ra vServer.
//
// Doi chieu voi aws-ebs-csi-driver pkg/cloud/retry_manager.go: AWS gan mot
// retry.NewAdaptiveMode() RIENG cho tung mutating API, vi "the AWS SDK throttles
// on a retryer object level, not by API name" va tai khoan AWS co the nang han
// muc cho tung API doc lap.
//
// KHONG sao chep cach chia do sang day. Han muc cua vServer la MOT bucket dung
// chung theo PROJECT (~1000 request / 60s), va bucket do con bi chia se voi capv,
// lb-controller, cluster-autoscaler... trong cung project. Chia limiter theo API
// se cho phep tong throughput vuot bucket that. Vi vay o day dung MOT limiter
// dung chung cho moi request cua process.
//
// Ly do phai nam o tang HTTP chu khong phai tang cloud: SDK bien 429 thanh
// PermissionDenied truoc khi tra len (vngcloud/client/http.go, case
// lhttp.StatusTooManyRequests -> lserr.WithErrorPermissionDenied()). Tang tren
// khong con phan biet duoc "het quota" voi "khong co quyen" - dung loi da tung
// lam sai chan doan mot su co cua lb-controller. Chi o day moi con doc duoc
// statusCode goc, vi defaultErrorResponse() gan no vao parameters cua IError va
// SdkErrorHandler tra ve dung object do.
const (
	// Tran throughput. Dat CAO hon bucket cua project mot cach co y: o trang thai
	// khoe manh limiter gan nhu khong can thiep, no chi that su siet lai sau khi
	// nhin thay 429 that. Day cung la triet ly cua AdaptiveMode ("restricts
	// attempts of API calls that recently hit throttle errors"), khong phai mot
	// han muc tinh doan mo.
	rateLimitMaxQPS = 20.0

	// San. Duoi muc nay thi driver coi nhu dung han, khong con hop ly.
	rateLimitMinQPS = 1.0

	// Multiplicative decrease khi dinh 429.
	rateLimitDecreaseFactor = 0.5

	// Additive increase moi rateLimitRecoverEvery neu khong con bi throttle.
	rateLimitIncreaseQPS  = 1.0
	rateLimitRecoverEvery = 5 * ltime.Second

	// Cho toi da bao lau de lay duoc token. Vuot qua thi tra loi ngay cho CO
	// retry, thay vi ngu trong handler - handler dang giu inflight lock.
	rateLimitMaxWait = 5 * ltime.Second

	ecCsiClientRateLimited = lsdkErrs.ErrorCode("CsiClientRateLimited")
)

// adaptiveRateLimiter la mot token bucket co toc do dieu chinh theo AIMD:
// giam nhan khi bi throttle, tang dan tro lai khi yen.
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

// onThrottled: nhin thay 429 -> giam nhan toc do.
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

// onSuccess: yen duoc rateLimitRecoverEvery thi tang dan tro lai.
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

// setRateLocked chinh CA rate VA burst. Chi doi rate la khong du: burst la kich
// thuoc bucket, nen neu giu nguyen 20 thi sau bon lan 429 (rate con 1.25 QPS)
// mot dot scale-up van duoc nap 20 request cung luc ngay khi bucket day lai -
// dung cai burst da tao ra con bao 429.
func (s *adaptiveRateLimiter) setRateLocked(pqps float64) {
	burst := int(pqps)
	if burst < 1 {
		burst = 1
	}

	s.qps = pqps
	s.limiter.SetLimit(lrate.Limit(pqps))
	s.limiter.SetBurst(burst)
}

// currentQPS chi de quan sat va de test doc duoc.
func (s *adaptiveRateLimiter) currentQPS() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.qps
}

// wait lay mot token. Tra ve false neu phai cho lau hon rateLimitMaxWait -
// khi do caller nen tra loi ngay cho CO thay vi ngu giu inflight lock.
func (s *adaptiveRateLimiter) wait() bool {
	res := s.limiter.Reserve()
	if !res.OK() {
		return false
	}

	delay := res.Delay()
	if delay > rateLimitMaxWait {
		res.Cancel()
		return false
	}

	if delay > 0 {
		ltime.Sleep(delay)
	}

	return true
}

// isThrottled doc statusCode goc ma SDK gan vao parameters cua IError truoc khi
// no bi ep thanh PermissionDenied.
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

// throttledHTTPClient boc lsdkClient.IHttpClient de moi request deu di qua
// limiter dung chung.
type throttledHTTPClient struct {
	inner   lsdkClient.IHttpClient
	limiter *adaptiveRateLimiter
}

// NewThrottledHTTPClient tao http client co throttle cho MOT process. Controller
// va node plugin la hai process khac nhau nen moi ben co limiter rieng; day la
// gioi han that su cua cach lam nay, khong the chia se bucket qua process.
//
// Duoc export vi node plugin cung co the tu dung mot SDK client
// rieng; neu no khong di qua day thi toan bo traffic cua DaemonSet se khong duoc
// dem, ma do lai chinh la nguon fan-out de lam can bucket cua project nhat khi
// scale-up node group.
func NewThrottledHTTPClient(pctx lctx.Context) lsdkClient.IHttpClient {
	inner := lsdkClient.NewHttpClient(pctx)

	// Timeout khong dat o day: mac dinh 120s cua SDK (v2.21.0) da dung, va ly do
	// chon con so do nam trong SDK - noi no ap dung cho moi consumer. Dat lai o
	// day chi tao them mot cho phai dong bo.
	//
	// Retry thi tat. SDK v2.21.0 da lanh hon nhieu (backoff co jitter, chi thu
	// lai GET/HEAD/OPTIONS/TRACE), nhung voi driver nay no van thua va co hai:
	//
	//   - Thua: moi request doc cua driver deu nam trong mot vong poll da tu
	//     retry san, voi backoff rieng va bi ctx cat. Them mot tang retry nua chi
	//     nhan so request len.
	//   - Co hai: mot lan goi DoRequest co the thanh 4 lan x 120s cong backoff.
	//     ExponentialBackoffWithContext chi kiem tra ctx.Done() GIUA cac lan goi
	//     condition nen no khong cat duoc mot request dang treo - handler se song
	//     qua han cua sidecar va tiep tuc giu inflight lock.
	//
	// Bo retry keo truong hop xau nhat cua mot lan goi ve dung 120s.
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
		// Ghi lai su that ma SDK sap che mat: day la 429, khong phai 403.
		llog.InfoS("[WARN] - rateLimiter: request throttled by vServer (HTTP 429, reported as PermissionDenied)",
			"url", purl, "method", preq.GetRequestMethod())
	} else {
		// Bat ky ket qua nao KHONG phai 429 deu la bang chung rang ta khong con bi
		// siet quota, ke ca 500 hay 404. Neu chi hoi phuc khi sdkErr == nil thi mot
		// dot loi 5xx keo dai se ghim limiter o san 1 QPS vo thoi han, va khi dich
		// vu song lai driver van tu bo doi minh vi mot ly do khong lien quan.
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

// errClientRateLimited PHAI mang mot error that su.
//
// IError.GetError() tra ve error da duoc WithErrors() nap vao; neu bo qua no thi
// GetError() la nil, va moi caller kieu `return sdkErr.GetError()` bien mot
// request bi shed thanh THANH CONG IM LANG. Tren duong DetachVolume dieu do se
// khien external-attacher xoa VolumeAttachment trong khi dia con dinh vao node -
// dung loai hong ma nhanh nay sinh ra de sua.
func errClientRateLimited(purl string) lsdkErrs.IError {
	return new(lsdkErrs.SdkError).
		WithErrorCode(ecCsiClientRateLimited).
		WithMessage("request shed by the CSI driver client-side rate limiter").
		WithErrors(lfmt.Errorf("request to %s shed by the client-side rate limiter", purl)).
		WithKVparameters("url", purl)
}
