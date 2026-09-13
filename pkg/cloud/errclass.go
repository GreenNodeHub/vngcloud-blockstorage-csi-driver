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
	ReasonIaaSResourceNotFound      = "IaaSResourceNotFound"
	ReasonIaaSAuthFailed            = "IaaSAuthFailed"
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
		ReasonIaaSResourceNotFound,
		ReasonIaaSAuthFailed,
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

	// The volume or the node VM is gone as far as the IaaS is concerned.
	//
	// THREE codes, not two, and the third is the one that actually fires.
	//
	// The SDK's two are mapped for AttachBlockVolume
	// (services/compute/v2/server.go). But the path that reaches Classify in
	// practice is getVolumeForAttach's READ: it recognises the SDK's
	// not-found and re-stamps it with lserr.ErrVolumeNotFound, which carries
	// the DRIVER's own constant. The two constants share a Go name and differ
	// in value - lsdkErrs.EcVServerVolumeNotFound is
	// "VngCloudVServerVolumeNotFound", lserr.EcVServerVolumeNotFound is
	// "VServerVolumeNotFound" - so a set holding only the SDK's silently
	// matches nothing on that path.
	//
	// Found by probing a bogus volume handle on the dev cluster on
	// 11/09/2026: the event still read IaaSUnknownError while its own message
	// said "Volume ... not found". ErrVolumeNotFound takes no sdkErr either,
	// so there is no sdkErrorCode parameter for effectiveCode to recover.
	errSetResourceNotFound = lset.NewSet[lsdkErrs.ErrorCode](
		lsdkErrs.EcVServerVolumeNotFound,
		lsdkErrs.EcVServerServerNotFound,
		lserr.EcVServerVolumeNotFound,
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
	if hasResponseStatusCode(perr, lhttp.StatusTooManyRequests) {
		return Class{Reason: ReasonIaaSThrottled}
	}
	if hasResponseStatusCode(perr, lhttp.StatusForbidden) {
		return Class{Terminal: true, Reason: ReasonIaaSPermissionDenied}
	}
	// 401 is the most actionable failure this driver can report and it was
	// arriving as IaaSUnknownError. Measured on the dev cluster on
	// 11/09/2026: IAM began rejecting the driver's clientId, the project
	// lookup failed 15 times with status 401, every CreateVolume failed, and
	// the reason on the PVC said "unknown".
	//
	// Checked by STATUS, beside the 429/403 pair, because the two shapes this
	// arrives in carry different codes: the SDK's catch-all when its own
	// mapping misses, or EcPermissionDenied when it matches. Only the status
	// is common to both.
	//
	// Terminal. The SDK already retries once through its reauth hook
	// (client/http.go), so a 401 that reaches here means re-authentication
	// itself failed - the credential is wrong or revoked, and no amount of
	// retrying fixes that. On the detach path terminal trips the breaker on
	// the first failure, which is exactly right: stop hammering an endpoint
	// that is rejecting this identity.
	if hasResponseStatusCode(perr, lhttp.StatusUnauthorized) {
		return Class{Terminal: true, Reason: ReasonIaaSAuthFailed}
	}

	switch {
	case code == lsdkErrs.EcInternalServerError || code == lsdkErrs.EcServiceMaintenance:
		return Class{Reason: ReasonIaaSServerError}
	case errSetOperationStalled.ContainsOne(code):
		return Class{Reason: ReasonIaaSOperationStalled}
	case errSetResourceNotFound.ContainsOne(code):
		// NOT terminal. A 404 moments after a create is eventual consistency,
		// and this file's standing rule is that calling a transient error
		// terminal costs more than the reverse: a terminal class trips the
		// detach breaker on the first failure. Only the label changes here;
		// retry behaviour is exactly as before.
		return Class{Reason: ReasonIaaSResourceNotFound}
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
		// A 404 that arrived as the SDK's catch-all rather than as
		// VolumeNotFound/ServerNotFound. Measured on the dev cluster on
		// 10/09/2026: one attach failed with code UnknownError and status 404,
		// so the SDK's own mapping did not match the response body. The status
		// says what the code could not, and the meaning is the same as the
		// mapped codes above.
		if hasResponseStatusCode(perr, lhttp.StatusNotFound) {
			return Class{Reason: ReasonIaaSResourceNotFound}
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
// EcPermissionDenied - see the comment on the hasResponseStatusCode checks in
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

// hasResponseStatusCode reads the raw statusCode the SDK stashes in the error
// parameters and compares it. Same access path isThrottled() uses in
// ratelimit.go.
//
// Used for three statuses now - 429, 403 and 404 - which is why it is not
// named after any one of them. It was isThrottledStatus while it served only
// the 429/403 split, and that name had already stopped being true.
func hasResponseStatusCode(perr lserr.IError, pstatus int) bool {
	raw, ok := perr.GetParameters()["statusCode"]
	if !ok {
		return false
	}

	status, ok := raw.(int)

	return ok && status == pstatus
}
