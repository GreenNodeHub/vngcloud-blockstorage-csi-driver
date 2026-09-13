package cloud

import (
	ltesting "testing"

	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"

	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
)

// sdkWrapped builds the driver-level IError the way DetachVolume does: an
// lserr.IError wrapping an SDK error, so Classify must dig through the wrapper.
func sdkWrapped(pcode lsdkErrs.ErrorCode) lserr.IError {
	return lserr.NewError(new(lsdkErrs.SdkError).WithErrorCode(pcode))
}

func sdkWrappedStatus(pstatus int, pcode lsdkErrs.ErrorCode) lserr.IError {
	return lserr.NewError(new(lsdkErrs.SdkError).
		WithErrorCode(pcode).
		WithKVparameters("statusCode", pstatus, "url", "https://vserver/volumes"))
}

func TestClassify(t *ltesting.T) {
	tcs := []struct {
		name         string
		err          lserr.IError
		wantTerminal bool
		wantReason   string
	}{
		{"volume count quota", sdkWrapped(lsdkErrs.EcVServerVolumeExceedQuota), true, ReasonVolumeQuotaExceeded},
		{"volume size quota", sdkWrapped(lsdkErrs.EcVServerVolumeSizeExceedGlobalQuota), true, ReasonVolumeSizeQuotaExceeded},
		{"attach quota per server", sdkWrapped(lsdkErrs.EcVServerServerVolumeAttachQuotaExceeded), true, ReasonVolumeAttachQuotaExceeded},
		// 429 and 403 both arrive as EcPermissionDenied; only statusCode separates them.
		{"real 403 is terminal", sdkWrappedStatus(403, lsdkErrs.EcPermissionDenied), true, ReasonIaaSPermissionDenied},
		{"429 is transient", sdkWrappedStatus(429, lsdkErrs.EcPermissionDenied), false, ReasonIaaSThrottled},
		{"500 is transient", sdkWrapped(lsdkErrs.EcInternalServerError), false, ReasonIaaSServerError},
		{"503 is transient", sdkWrapped(lsdkErrs.EcServiceMaintenance), false, ReasonIaaSServerError},
		{"in-process is transient", sdkWrapped(lsdkErrs.EcVServerVolumeInProcess), false, ReasonIaaSOperationStalled},
		{"unknown is transient", sdkWrapped(lsdkErrs.EcUnknownError), false, ReasonIaaSUnknownError},
		// Both codes are mapped by the SDK for AttachBlockVolume, and neither
		// had a reason here - a missing volume reported as IaaSUnknownError.
		{"volume not found", sdkWrapped(lsdkErrs.EcVServerVolumeNotFound), false, ReasonIaaSResourceNotFound},
		{"server not found", sdkWrapped(lsdkErrs.EcVServerServerNotFound), false, ReasonIaaSResourceNotFound},
		// The case actually measured on the dev cluster on 10/09/2026: the
		// SDK's mapping did not match the response body, so a 404 arrived as
		// the catch-all. The status is what identifies it.
		{"404 arriving as the catch-all", sdkWrappedStatus(404, lsdkErrs.EcUnexpectedError), false, ReasonIaaSResourceNotFound},
		// And the discriminations around it must survive: a 5xx and a
		// no-response still take their own branches, not this one.
		{"500 as the catch-all is a server error", sdkWrappedStatus(500, lsdkErrs.EcUnexpectedError), false, ReasonIaaSServerError},
		{"catch-all with no response at all", sdkWrapped(lsdkErrs.EcUnexpectedError), false, ReasonIaaSUnreachable},
		{"catch-all with an unmodelled 4xx", sdkWrappedStatus(409, lsdkErrs.EcUnexpectedError), false, ReasonIaaSUnknownError},
		// 401 arrives in two shapes depending on whether the SDK's mapping
		// matched, so it is recognised by status, not by code. Terminal
		// because the SDK has already retried through its reauth hook by the
		// time the error gets here.
		{"401 as the catch-all", sdkWrappedStatus(401, lsdkErrs.EcUnexpectedError), true, ReasonIaaSAuthFailed},
		{"401 mapped to permission denied", sdkWrappedStatus(401, lsdkErrs.EcPermissionDenied), true, ReasonIaaSAuthFailed},
		// And 401 must not swallow the neighbouring statuses.
		{"403 stays permission denied", sdkWrappedStatus(403, lsdkErrs.EcUnexpectedError), true, ReasonIaaSPermissionDenied},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *ltesting.T) {
			got := Classify(tc.err)
			if got.Terminal != tc.wantTerminal {
				t.Fatalf("Terminal = %v, want %v", got.Terminal, tc.wantTerminal)
			}
			if got.Reason != tc.wantReason {
				t.Fatalf("Reason = %q, want %q", got.Reason, tc.wantReason)
			}
		})
	}
}

// A nil error must not panic and must not look like a failure worth reporting.
func TestClassifyNilIsEmpty(t *ltesting.T) {
	got := Classify(nil)
	if got.Terminal || got.Reason != "" {
		t.Fatalf("Classify(nil) = %+v, want zero value", got)
	}
}

// The driver's own error for an ERROR-state volume has no SDK code behind it.
func TestClassifyDriverErrorState(t *ltesting.T) {
	got := Classify(lserr.ErrVolumeIsInErrorState("vol-x"))
	if !got.Terminal || got.Reason != ReasonVolumeInErrorState {
		t.Fatalf("Classify(ErrVolumeIsInErrorState) = %+v, want terminal %q", got, ReasonVolumeInErrorState)
	}
}

// rawSdkError is what the vServer SDK hands DetachVolume: an lsdkErrs.IError,
// before the driver wraps it.
func rawSdkError(pcode lsdkErrs.ErrorCode) lsdkErrs.IError {
	return new(lsdkErrs.SdkError).WithErrorCode(pcode)
}

func rawSdkErrorStatus(pstatus int, pcode lsdkErrs.ErrorCode) lsdkErrs.IError {
	return new(lsdkErrs.SdkError).
		WithErrorCode(pcode).
		WithKVparameters("statusCode", pstatus, "url", "https://vserver/volumes")
}

// Item 2: every case above is built the way the CREATE path builds errors -
// one SdkError carrying the code Classify reads. The DETACH path does not look
// like that: lserr.ErrVolumeFailedToDetach stamps its own wrapper code over the
// SDK's and merges only the inner error and the parameters, so the SDK code is
// the one thing that has to survive on purpose. Build these through the real
// wrapper, never by hand, or the test stops describing the production path.
func TestClassifyThroughDriverWrappers(t *ltesting.T) {
	tcs := []struct {
		name         string
		err          lserr.IError
		wantTerminal bool
		wantReason   string
	}{
		{
			"detach rejected, volume in process",
			lserr.ErrVolumeFailedToDetach("ins-1", "vol-a", rawSdkError(lsdkErrs.EcVServerVolumeInProcess)),
			false, ReasonIaaSOperationStalled,
		},
		{
			"detach hit a 500",
			lserr.ErrVolumeFailedToDetach("ins-1", "vol-a", rawSdkError(lsdkErrs.EcInternalServerError)),
			false, ReasonIaaSServerError,
		},
		{
			"detach hit a volume quota",
			lserr.ErrVolumeFailedToDetach("ins-1", "vol-a", rawSdkError(lsdkErrs.EcVServerVolumeExceedQuota)),
			true, ReasonVolumeQuotaExceeded,
		},
		{
			"detach throttled with 429",
			lserr.ErrVolumeFailedToDetach("ins-1", "vol-a", rawSdkErrorStatus(429, lsdkErrs.EcPermissionDenied)),
			false, ReasonIaaSThrottled,
		},
		{
			"detach denied with 403",
			lserr.ErrVolumeFailedToDetach("ins-1", "vol-a", rawSdkErrorStatus(403, lsdkErrs.EcPermissionDenied)),
			true, ReasonIaaSPermissionDenied,
		},
		{
			// The wait helpers pass psdkErr == nil: the detach command was
			// accepted and the volume simply never finished. This is the
			// 23-hour incident's own error, and the reason the feature exists.
			"detach accepted but never completed",
			lserr.ErrVolumeFailedToDetach("ins-1", "vol-a", nil),
			false, ReasonIaaSOperationStalled,
		},
		{
			// Deliberately unknown: a failed READ says nothing about what
			// state the volume is actually in.
			"failed read stays unknown",
			lserr.ErrVolumeFailedToGet("vol-a", nil),
			false, ReasonIaaSUnknownError,
		},
		{
			"failed attach carries the SDK code through too",
			lserr.ErrVolumeFailedToAttach("ins-1", "vol-a", rawSdkError(lsdkErrs.EcVServerServerVolumeAttachQuotaExceeded)),
			true, ReasonVolumeAttachQuotaExceeded,
		},
		{
			"failed delete carries the SDK code through too",
			lserr.ErrVolumeFailedToDelete("vol-a", rawSdkError(lsdkErrs.EcInternalServerError)),
			false, ReasonIaaSServerError,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *ltesting.T) {
			got := Classify(tc.err)
			if got.Terminal != tc.wantTerminal {
				t.Fatalf("Terminal = %v, want %v", got.Terminal, tc.wantTerminal)
			}
			if got.Reason != tc.wantReason {
				t.Fatalf("Reason = %q, want %q", got.Reason, tc.wantReason)
			}
		})
	}
}

// TestClassifyTransportFailures covers the gap TS-G found on the dev cluster on
// 07/09/2026. With vServer egress dropped, every event and both metrics reported
// reason="IaaSUnknownError" - for op=detach AND op=create - because the SDK
// answers every transport-level failure with one catch-all code that Classify
// did not map.
//
// The shapes below are the real ones. Observed in the driver log:
//
//	sdkErrorCode="VngCloudApiUnexpectedError"
//	err="Get \"https://.../volumes/vol-...\": net/http: request canceled while
//	     waiting for connection (Client.Timeout exceeded while awaiting headers)"
//
// The SDK builds that in sdk_error/common.go:164 and attaches statusCode, which
// is 0 when no response ever arrived. statusCode is what separates "could not
// reach vServer at all" from "vServer answered with something unrecognised".
func TestClassifyTransportFailures(t *ltesting.T) {
	tcs := []struct {
		name         string
		err          lserr.IError
		wantTerminal bool
		wantReason   string
	}{
		{
			"detach: HTTP timeout, no response, statusCode 0",
			lserr.ErrVolumeFailedToGet("vol-a", rawSdkErrorStatus(0, lsdkErrs.EcUnexpectedError)),
			false, ReasonIaaSUnreachable,
		},
		{
			"detach: transport failure with no statusCode recorded at all",
			lserr.ErrVolumeFailedToDetach("ins-1", "vol-a", rawSdkError(lsdkErrs.EcUnexpectedError)),
			false, ReasonIaaSUnreachable,
		},
		{
			"create: same catch-all code on the create path",
			sdkWrappedStatus(0, lsdkErrs.EcUnexpectedError),
			false, ReasonIaaSUnreachable,
		},
		{
			// A response DID arrive, so this is not unreachable - the server
			// answered and failed. Report it as the server error it is.
			"unrecognised 502 from vServer",
			lserr.ErrVolumeFailedToDetach("ins-1", "vol-a", rawSdkErrorStatus(502, lsdkErrs.EcUnexpectedError)),
			false, ReasonIaaSServerError,
		},
		{
			"unrecognised 503 from vServer",
			sdkWrappedStatus(503, lsdkErrs.EcUnexpectedError),
			false, ReasonIaaSServerError,
		},
		{
			// 429 and 403 must keep winning: they are discriminated by
			// statusCode before the code is consulted at all, and a transport
			// code must not shadow them.
			"429 still reads as throttling even under the catch-all code",
			lserr.ErrVolumeFailedToDetach("ins-1", "vol-a", rawSdkErrorStatus(429, lsdkErrs.EcUnexpectedError)),
			false, ReasonIaaSThrottled,
		},
		{
			// A 4xx that is not 403/429 is the caller's fault, not the
			// network's, and retrying it unchanged will not help. Still
			// non-terminal, because misclassifying a transient as terminal is
			// the more expensive mistake.
			"unrecognised 400 is neither unreachable nor a server error",
			sdkWrappedStatus(400, lsdkErrs.EcUnexpectedError),
			false, ReasonIaaSUnknownError,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *ltesting.T) {
			got := Classify(tc.err)
			if got.Reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", got.Reason, tc.wantReason)
			}
			if got.Terminal != tc.wantTerminal {
				t.Errorf("terminal = %v, want %v", got.Terminal, tc.wantTerminal)
			}
		})
	}
}

// AllErrorReasons drives the pre-created metric series. A reason that Classify
// can emit but the list omits gets no zero-valued series, so its panel reads
// "no data" until the first occurrence - the exact problem pre-creation
// exists to solve. Asserted by driving Classify rather than by re-listing the
// constants, which would just be the same list twice.
func TestAllErrorReasonsCoversEveryReasonClassifyEmits(t *ltesting.T) {
	known := make(map[string]bool, len(AllErrorReasons()))
	for _, r := range AllErrorReasons() {
		known[r] = true
	}

	for _, err := range []lserr.IError{
		sdkWrapped(lsdkErrs.EcVServerVolumeExceedQuota),
		sdkWrapped(lsdkErrs.EcVServerVolumeSizeExceedGlobalQuota),
		sdkWrapped(lsdkErrs.EcVServerServerVolumeAttachQuotaExceeded),
		sdkWrappedStatus(403, lsdkErrs.EcPermissionDenied),
		sdkWrappedStatus(429, lsdkErrs.EcPermissionDenied),
		sdkWrapped(lsdkErrs.EcInternalServerError),
		sdkWrapped(lsdkErrs.EcVServerVolumeInProcess),
		sdkWrapped(lsdkErrs.EcVServerVolumeNotFound),
		sdkWrapped(lsdkErrs.EcVServerServerNotFound),
		sdkWrappedStatus(404, lsdkErrs.EcUnexpectedError),
		sdkWrappedStatus(401, lsdkErrs.EcUnexpectedError),
		sdkWrapped(lsdkErrs.EcUnexpectedError),
		sdkWrapped(lsdkErrs.EcUnknownError),
	} {
		reason := Classify(err).Reason
		if !known[reason] {
			t.Errorf("Classify emits %q, which AllErrorReasons does not list", reason)
		}
	}
}

// TestClassifyDriverConstructedErrors drives Classify with the error
// CONSTRUCTORS the driver actually calls, not with synthetic codes.
//
// This is the test that would have caught the miss. The first attempt at a
// not-found reason classified only the SDK's codes, and every table entry
// built its error with sdkWrapped(...) - so the table proved the set worked
// on codes that path never produces. The path that fires is
// getVolumeForAttach re-stamping the SDK's not-found with the driver's own
// constant, whose VALUE differs ("VServerVolumeNotFound" against
// "VngCloudVServerVolumeNotFound") while its Go name does not.
//
// A live probe with a bogus volume handle still reported IaaSUnknownError
// after that change shipped. Constructors, not codes.
func TestClassifyDriverConstructedErrors(t *ltesting.T) {
	for _, tc := range []struct {
		name       string
		err        lserr.IError
		wantReason string
	}{
		{
			name:       "the read path's not-found, as getVolumeForAttach builds it",
			err:        lserr.ErrVolumeNotFound("vol-x"),
			wantReason: ReasonIaaSResourceNotFound,
		},
		{
			name:       "an ERROR-state volume, which has no SDK code at all",
			err:        lserr.ErrVolumeIsInErrorState("vol-x"),
			wantReason: ReasonVolumeInErrorState,
		},
		{
			name:       "a detach that the IaaS accepted and never finished",
			err:        lserr.ErrVolumeFailedToDetach("ins-x", "vol-x", nil),
			wantReason: ReasonIaaSOperationStalled,
		},
	} {
		t.Run(tc.name, func(t *ltesting.T) {
			if got := Classify(tc.err).Reason; got != tc.wantReason {
				t.Errorf("Classify = %q, want %q", got, tc.wantReason)
			}
		})
	}
}
