package cloud

import (
	ltesting "testing"
	ltime "time"

	lreq "github.com/imroc/req/v3"
	lsdkClient "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/client"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
	lrate "golang.org/x/time/rate"
)

// sdkErrWithStatus dung lai dung cach SDK dong goi loi HTTP: defaultErrorResponse
// gan statusCode vao parameters, roi SdkErrorHandler ep error code thanh
// PermissionDenied va tra ve CHINH object do.
func sdkErrWithStatus(pstatus int, pcode lsdkErrs.ErrorCode) lsdkErrs.IError {
	return new(lsdkErrs.SdkError).
		WithErrorCode(pcode).
		WithKVparameters("statusCode", pstatus, "url", "https://vserver/volumes")
}

// TestIsThrottled: 429 phai phan biet duoc voi 403 du SDK bien ca hai thanh
// PermissionDenied. Chinh su nhap nhang nay tung lam chan doan sai mot su co
// cua lb-controller ("permission denied" trong khi that ra la het quota).
func TestIsThrottled(t *ltesting.T) {
	tcs := []struct {
		name string
		err  lsdkErrs.IError
		want bool
	}{
		{"429 du error code la PermissionDenied", sdkErrWithStatus(429, lsdkErrs.EcPermissionDenied), true},
		{"403 that su khong phai throttle", sdkErrWithStatus(403, lsdkErrs.EcPermissionDenied), false},
		{"500 khong phai throttle", sdkErrWithStatus(500, lsdkErrs.EcUnknownError), false},
		{"loi khong co statusCode", new(lsdkErrs.SdkError).WithErrorCode(lsdkErrs.EcUnknownError), false},
		{"khong co loi", nil, false},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *ltesting.T) {
			if got := isThrottled(tc.err); got != tc.want {
				t.Fatalf("isThrottled() = %v, muon %v", got, tc.want)
			}
		})
	}
}

func TestAdaptiveRateLimiterDecreasesOnThrottle(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	now := ltime.Unix(0, 0)

	if got := rl.currentQPS(); got != rateLimitMaxQPS {
		t.Fatalf("QPS ban dau = %v, muon %v", got, rateLimitMaxQPS)
	}

	want := rateLimitMaxQPS
	for i := 0; i < 10; i++ {
		rl.onThrottled(now)
		want *= rateLimitDecreaseFactor
		if want < rateLimitMinQPS {
			want = rateLimitMinQPS
		}

		if got := rl.currentQPS(); got != want {
			t.Fatalf("sau %d lan throttle: QPS = %v, muon %v", i+1, got, want)
		}
	}

	if got := rl.currentQPS(); got != rateLimitMinQPS {
		t.Fatalf("QPS phai dung o san %v, thuc te %v", rateLimitMinQPS, got)
	}
}

func TestAdaptiveRateLimiterRecoveryIsGatedAndCapped(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	now := ltime.Unix(0, 0)

	rl.onThrottled(now)
	dropped := rl.currentQPS()

	// Ngay sau khi bi throttle thi khong duoc tang lai.
	rl.onSuccess(now.Add(rateLimitRecoverEvery - ltime.Millisecond))
	if got := rl.currentQPS(); got != dropped {
		t.Fatalf("hoi phuc qua som: QPS = %v, muon giu %v", got, dropped)
	}

	// Qua cua so yen thi tang dan, va khong bao gio vuot tran.
	at := now
	for i := 0; i < 100; i++ {
		at = at.Add(rateLimitRecoverEvery)
		rl.onSuccess(at)
		if got := rl.currentQPS(); got > rateLimitMaxQPS {
			t.Fatalf("QPS = %v vuot tran %v", got, rateLimitMaxQPS)
		}
	}

	if got := rl.currentQPS(); got != rateLimitMaxQPS {
		t.Fatalf("sau khi yen lau, QPS = %v, muon tro lai %v", got, rateLimitMaxQPS)
	}
}

// TestAdaptiveRateLimiterShedsInsteadOfSleeping khoa tinh chat quan trong nhat:
// khong bao gio ngu lau trong handler, vi handler dang giu inflight lock.
func TestAdaptiveRateLimiterShedsInsteadOfSleeping(t *ltesting.T) {
	// 0.1 QPS, burst 1 => token thu hai phai cho 10 giay, vuot rateLimitMaxWait.
	rl := &adaptiveRateLimiter{limiter: lrate.NewLimiter(0.1, 1), qps: 0.1}

	if !rl.wait() {
		t.Fatal("token dau tien phai lay duoc ngay")
	}

	start := ltime.Now()
	if rl.wait() {
		t.Fatal("token thu hai phai bi tu choi thay vi cho 10 giay")
	}

	if elapsed := ltime.Since(start); elapsed > ltime.Second {
		t.Fatalf("wait() ngu mat %v truoc khi tu choi; phai tra ve ngay", elapsed)
	}
}

// fakeHTTPClient thay cho lsdkClient.IHttpClient de kiem tra decorator.
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
		t.Fatal("loi cua inner client phai duoc tra nguyen ve tren")
	}

	if inner.calls != 1 {
		t.Fatalf("inner duoc goi %d lan, muon 1", inner.calls)
	}

	if after := client.limiter.currentQPS(); after >= before {
		t.Fatalf("gap 429 ma QPS khong giam: truoc %v, sau %v", before, after)
	}
}

func TestThrottledHTTPClientIgnoresNonThrottleErrors(t *ltesting.T) {
	inner := &fakeHTTPClient{err: sdkErrWithStatus(403, lsdkErrs.EcPermissionDenied)}
	client := &throttledHTTPClient{inner: inner, limiter: newAdaptiveRateLimiter()}

	before := client.limiter.currentQPS()
	if _, err := client.DoRequest("https://vserver/volumes", fakeRequest{}); err == nil {
		t.Fatal("loi cua inner client phai duoc tra nguyen ve tren")
	}

	if after := client.limiter.currentQPS(); after != before {
		t.Fatalf("403 that su khong duoc lam giam QPS: truoc %v, sau %v", before, after)
	}
}

// TestErrClientRateLimitedCarriesAnError khoa lai bug nang nhat cua ban dau:
// IError khong co WithErrors() thi GetError() tra ve nil, va moi caller kieu
// `return sdkErr.GetError()` bien mot request bi shed thanh THANH CONG IM LANG.
// Tren duong DetachVolume dieu do lam external-attacher xoa VolumeAttachment
// trong khi dia con dinh vao node.
func TestErrClientRateLimitedCarriesAnError(t *ltesting.T) {
	err := errClientRateLimited("https://vserver/volumes/vol-1")

	if err.GetError() == nil {
		t.Fatal("GetError() = nil; caller lam `return sdkErr.GetError()` se bao thanh cong gia")
	}

	if err.GetErrorCode() != ecCsiClientRateLimited {
		t.Fatalf("error code = %v, muon %v", err.GetErrorCode(), ecCsiClientRateLimited)
	}
}

// TestAdaptiveRateLimiterShrinksBurst: chi ha rate ma giu nguyen burst thi mot
// dot scale-up van duoc nap ca bucket cung luc - dung cai burst gay ra 429.
func TestAdaptiveRateLimiterShrinksBurst(t *ltesting.T) {
	rl := newAdaptiveRateLimiter()
	now := ltime.Unix(0, 0)

	before := rl.limiter.Burst()
	for i := 0; i < 5; i++ {
		rl.onThrottled(now)
	}

	after := rl.limiter.Burst()
	if after >= before {
		t.Fatalf("burst khong giam theo rate: truoc %d, sau %d", before, after)
	}
	if after < 1 {
		t.Fatalf("burst = %d, khong duoc xuong duoi 1", after)
	}
}

// TestThrottledHTTPClientRecoversAfterNonThrottleErrors: neu chi hoi phuc khi
// sdkErr == nil thi mot dot 5xx keo dai se ghim limiter o san 1 QPS vo thoi han.
func TestThrottledHTTPClientRecoversAfterNonThrottleErrors(t *ltesting.T) {
	inner := &fakeHTTPClient{err: sdkErrWithStatus(429, lsdkErrs.EcPermissionDenied)}
	client := &throttledHTTPClient{inner: inner, limiter: newAdaptiveRateLimiter()}

	if _, err := client.DoRequest("https://vserver/volumes", fakeRequest{}); err == nil {
		t.Fatal("mong doi loi tu inner client")
	}
	dropped := client.limiter.currentQPS()

	// Doi qua cua so hoi phuc roi tra ve 500 - khong phai 429.
	client.limiter.lastAdj = ltime.Now().Add(-2 * rateLimitRecoverEvery)
	inner.err = sdkErrWithStatus(500, lsdkErrs.EcUnknownError)

	if _, err := client.DoRequest("https://vserver/volumes", fakeRequest{}); err == nil {
		t.Fatal("mong doi loi tu inner client")
	}

	if got := client.limiter.currentQPS(); got <= dropped {
		t.Fatalf("500 khong phai throttle nen phai cho hoi phuc: truoc %v, sau %v", dropped, got)
	}
}
