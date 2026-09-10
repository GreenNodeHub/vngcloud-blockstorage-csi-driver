package cloud

import (
	lhttp "net/http"

	lset "github.com/cuongpiger/joat/data-structure/set"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"

	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
)

// Event reasons. Stable CamelCase strings: they end up as Kubernetes event
// reasons and in metric labels, so operators grep and alert on them.
const (
	ReasonVolumeQuotaExceeded       = "VolumeQuotaExceeded"
	ReasonVolumeSizeQuotaExceeded   = "VolumeSizeQuotaExceeded"
	ReasonVolumeAttachQuotaExceeded = "VolumeAttachQuotaExceeded"
	ReasonIaaSPermissionDenied      = "IaaSPermissionDenied"
	ReasonVolumeInErrorState        = "VolumeInErrorState"
	ReasonIaaSThrottled             = "IaaSThrottled"
	ReasonIaaSServerError           = "IaaSServerError"
	ReasonIaaSUnreachable           = "IaaSUnreachable"
	ReasonIaaSOperationStalled      = "IaaSOperationStalled"
	ReasonIaaSUnknownError          = "IaaSUnknownError"
)

// AllErrorReasons is every value Classify can put in a Reason, so those
// metric series can be created at zero on startup rather than appearing only
// once the corresponding failure happens for the first time.
//
// Keeping it beside the constants is deliberate: a reason added above without
// being added here still works, it just loses its pre-created series - and the
// test in errclass_test.go asserts the two stay in step.
func AllErrorReasons() []string {
	return []string{
		ReasonVolumeQuotaExceeded,
		ReasonVolumeSizeQuotaExceeded,
		ReasonVolumeAttachQuotaExceeded,
		ReasonIaaSPermissionDenied,
		ReasonVolumeInErrorState,
		ReasonIaaSThrottled,
		ReasonIaaSServerError,
		ReasonIaaSUnreachable,
		ReasonIaaSOperationStalled,
		ReasonIaaSUnknownError,
	}
}

// Class says what an IaaS error means for retry policy.
//
// Terminal means retrying the same call will never succeed on its own - a
// human has to raise a quota or fix a permission. Everything else is treated
// as transient, deliberately including the unknown case: labelling a
// transient error terminal is the more expensive mistake, because it stops
// the driver from recovering by itself.
type Class struct {
	Terminal bool
	Reason   string
}

var (
	// Quota exhaustion: the request is well-formed, the account is simply full.
	errSetQuotaExceeded = lset.NewSet[lsdkErrs.ErrorCode](
		lsdkErrs.EcVServerVolumeExceedQuota,
	)
	errSetSizeQuotaExceeded = lset.NewSet[lsdkErrs.ErrorCode](
		lsdkErrs.EcVServerVolumeSizeExceedGlobalQuota,
	)
	errSetAttachQuotaExceeded = lset.NewSet[lsdkErrs.ErrorCode](
		lsdkErrs.EcVServerServerVolumeAttachQuotaExceeded,
	)

	// The IaaS is mid-operation on this volume - it will clear on its own.
	errSetOperationStalled = lset.NewSet[lsdkErrs.ErrorCode](
		lsdkErrs.EcVServerVolumeInProcess,
		lsdkErrs.EcVServerVolumeIsMigrating,
	)
)

// Classify maps a driver-level error onto a retry class and an event reason.
//
// The input is lserr.IError - what DetachVolume and the create path actually
// return - not the raw SDK error, so both driver-defined conditions (an
// ERROR-state volume) and wrapped SDK codes are visible here.
func Classify(perr lserr.IError) Class {
	if perr == nil {
		return Class{}
	}

	code := effectiveCode(perr)

	switch {
	case errSetQuotaExceeded.ContainsOne(code):
		return Class{Terminal: true, Reason: ReasonVolumeQuotaExceeded}
	case errSetSizeQuotaExceeded.ContainsOne(code):
		return Class{Terminal: true, Reason: ReasonVolumeSizeQuotaExceeded}
	case errSetAttachQuotaExceeded.ContainsOne(code):
		return Class{Terminal: true, Reason: ReasonVolumeAttachQuotaExceeded}
	case code == lserr.EcVServerVolumeIsInErrorState:
		return Class{Terminal: true, Reason: ReasonVolumeInErrorState}
	}

	// The SDK flattens both 429 and 403 into EcPermissionDenied. Matching on
	// that error code alone would turn throttling into a permanent "permission
	// denied" - the exact misreading that once misdirected an lb-controller
	// incident diagnosis. We must check the raw statusCode instead.
	if isThrottledStatus(perr, lhttp.StatusTooManyRequests) {
		return Class{Reason: ReasonIaaSThrottled}
	}
	if isThrottledStatus(perr, lhttp.StatusForbidden) {
		return Class{Terminal: true, Reason: ReasonIaaSPermissionDenied}
	}

	switch {
	case code == lsdkErrs.EcInternalServerError || code == lsdkErrs.EcServiceMaintenance:
		return Class{Reason: ReasonIaaSServerError}
	case errSetOperationStalled.ContainsOne(code):
		return Class{Reason: ReasonIaaSOperationStalled}
	case code == ecCsiClientRateLimited:
		return Class{Reason: ReasonIaaSThrottled}
	case code == lserr.EcVServerVolumeFailedToDetach:
		// No SDK code at all: the wait helpers build this one with
		// psdkErr == nil, meaning the IaaS ACCEPTED the detach and the volume
		// then never finished. That is exactly the 23-hour incident's error.
		return Class{Reason: ReasonIaaSOperationStalled}
	case code == lsdkErrs.EcUnexpectedError:
		// The SDK's catch-all, and by volume the most common IaaS failure there
		// is: sdk_error/common.go:164 stamps it on every transport-level
		// problem - connection refused, DNS failure, the 120s client timeout -
		// as well as on any response whose status it does not recognise.
		//
		// TS-G on 07/09/2026 dropped vServer egress and watched every event and
		// both metrics come back reason="IaaSUnknownError", for op=create as
		// well as op=detach, because this code was unmapped. The classifier
		// looked like it worked and reported nothing useful about the failure
		// operators are most likely to hit.
		//
		// statusCode is the discriminator the SDK leaves behind: it is 0 when no
		// response ever arrived. Nothing about the volume's state is knowable in
		// that case, so this is NOT ReasonIaaSOperationStalled, which the spec
		// suggested - "stalled" claims the IaaS accepted the operation and is
		// working on it, and here the request never landed.
		if hasServerErrorStatus(perr) {
			return Class{Reason: ReasonIaaSServerError}
		}
		if !hasResponseStatus(perr) {
			return Class{Reason: ReasonIaaSUnreachable}
		}
		// A response arrived with a status that is neither 5xx nor one of the
		// codes handled above - a 4xx we do not model. Retrying it unchanged
		// will not help, but it stays non-terminal: calling a transient error
		// terminal is the more expensive mistake.
		return Class{Reason: ReasonIaaSUnknownError}
	}

	// EcVServerVolumeFailedToGet is deliberately left to fall through to
	// unknown: a failed READ says nothing about what state the volume is in.
	return Class{Reason: ReasonIaaSUnknownError}
}

// effectiveCode returns the SDK's own error code when the driver's wrappers
// preserved one, and the wrapper's code otherwise.
//
// lserr.ErrVolumeFailedToDetach and its siblings stamp a single driver code
// over whatever the SDK reported, so reading GetErrorCode() alone flattens
// every detach failure - quota, 500, busy volume - into one indistinguishable
// value, and Classify answered ReasonIaaSUnknownError for all of them. That is
// the same trap one level up from the SDK's own flattening of 429 and 403 into
// EcPermissionDenied - see the comment on the isThrottledStatus checks in
// Classify: a code that has already lost a distinction cannot be used to make
// it.
func effectiveCode(perr lserr.IError) lsdkErrs.ErrorCode {
	if raw, ok := perr.GetParameters()["sdkErrorCode"]; ok {
		if s, ok := raw.(string); ok && s != "" {
			return lsdkErrs.ErrorCode(s)
		}
	}

	return perr.GetErrorCode()
}

// hasResponseStatus reports whether a response actually came back. The SDK
// records statusCode 0 when the request never reached a server, so a missing or
// zero value means "no round trip completed", not "status unknown".
func hasResponseStatus(perr lserr.IError) bool {
	raw, ok := perr.GetParameters()["statusCode"]
	if !ok {
		return false
	}

	status, ok := raw.(int)

	return ok && status != 0
}

// hasServerErrorStatus reports whether the response that did arrive was a 5xx.
func hasServerErrorStatus(perr lserr.IError) bool {
	raw, ok := perr.GetParameters()["statusCode"]
	if !ok {
		return false
	}

	status, ok := raw.(int)

	return ok && status >= lhttp.StatusInternalServerError
}

// isThrottledStatus reads the raw statusCode the SDK stashes in the error
// parameters. Same access path isThrottled() uses in ratelimit.go.
func isThrottledStatus(perr lserr.IError, pstatus int) bool {
	raw, ok := perr.GetParameters()["statusCode"]
	if !ok {
		return false
	}

	status, ok := raw.(int)

	return ok && status == pstatus
}
