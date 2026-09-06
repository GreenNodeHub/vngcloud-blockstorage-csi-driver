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
