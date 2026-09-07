package cloud

import (
	lerrors "errors"
	lfmt "fmt"

	lsdkClientV2 "github.com/vngcloud/vngcloud-go-sdk/v2/client"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
	lsdkPortalSvcV1 "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/services/portal/v1"
	llog "k8s.io/klog/v2"

	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
)

// PortalLookupFunc maps the project id from instance metadata onto the portal
// `pro-...` project id that every vServer URL path embeds. It is the only
// vServer call the driver needs before it can issue any other.
type PortalLookupFunc func(pclient lsdkClientV2.IClient, punderProjectId string) (string, lserr.IError)

// DefaultPortalLookup is the real call. A package-level var, like
// DefaultEventRecorder, so the resolver can be driven in tests with no network
// and without changing NewCloud's signature.
var DefaultPortalLookup PortalLookupFunc = func(pclient lsdkClientV2.IClient, punderProjectId string) (string, lserr.IError) {
	portal, sdkErr := pclient.VServerGateway().V1().PortalService().
		GetPortalInfo(lsdkPortalSvcV1.NewGetPortalInfoRequest(punderProjectId))
	if sdkErr != nil {
		return "", lserr.NewError(sdkErr)
	}

	return portal.ProjectID, nil
}

// projectClient returns the project-scoped SDK client, running the portal
// lookup on first use.
//
// This used to happen in NewCloud, whose error newControllerService turns into
// panic(err): on 07/09/2026, with vServer egress blocked, the csi-controller
// went CrashLoopBackOff instead of degraded, so no sidecar could reach the
// socket and not one ControllerUnpublishVolume ran - the detach
// circuit-breaker and the classified error events exist for exactly that
// outage and could not execute.
//
// The client is returned rather than assigned back to a field: at
// worker-threads=100 a field written by whichever goroutine resolved first
// while the others read it is a data race.
//
// A resolved id never expires - see newPermanentMetaCache - and a failure is
// never cached, so the first call after vServer comes back resolves.
func (s *cloud) projectClient() (lsdkClientV2.IClient, lserr.IError) {
	return s.projectClientCache.get(func() (lsdkClientV2.IClient, lserr.IError) {
		underProjectId := s.metadataService.GetProjectID()

		projectId, ierr := s.portalLookup(s.baseClient, underProjectId)
		if ierr != nil {
			ierr = errProjectUnresolved(underProjectId, ierr)
			llog.ErrorS(ierr.GetError(), "[ERROR] - projectClient: Failed to resolve the project against vServer",
				ierr.GetListParameters()...)

			return nil, ierr
		}

		llog.InfoS("[INFO] - projectClient: Resolved the project against vServer",
			"underProjectId", underProjectId, "projectId", projectId)

		// WithProjectId mutates the client and returns it (SDK
		// client/client.go:106). baseClient is handed out nowhere else - the
		// portal lookup above is its only other user, and it runs inside this
		// same single flight.
		return s.baseClient.WithProjectId(projectId), nil
	})
}

// ecCsiProjectUnresolved marks a call that never reached vServer because the
// driver still has no portal project id to address it with.
const ecCsiProjectUnresolved = lsdkErrs.ErrorCode("CsiProjectUnresolved")

// errProjectUnresolvedText leads the message of every such failure. An
// operator reading a CreateVolume or DetachVolume error has to be able to tell
// that the cause is vServer being unreachable and not anything about the
// volume.
const errProjectUnresolvedText = "could not resolve the driver's project against vServer"

// errProjectUnresolved MUST carry a real error.
//
// Cloud methods that return plain `error` reach it through ierr.GetError(),
// which returns whatever WithErrors() loaded; a nil there is read as success -
// the same trap documented on errClientRateLimited.
func errProjectUnresolved(punderProjectId string, psdkErr lserr.IError) lserr.IError {
	msg := lfmt.Sprintf("%s (project id %s from instance metadata); the IaaS is unreachable or rejected the portal lookup",
		errProjectUnresolvedText, punderProjectId)

	e := new(lsdkErrs.SdkError).
		WithErrorCode(ecCsiProjectUnresolved).
		WithMessage(msg).
		WithKVparameters("underProjectId", punderProjectId)

	if psdkErr == nil {
		return lserr.NewError(e.WithErrors(lerrors.New(msg)))
	}

	cause := psdkErr.GetError()
	if cause == nil {
		cause = lerrors.New(psdkErr.GetMessage())
	}

	// WithErrorCode above overwrote the SDK's own code with this wrapper's, so
	// stash the code and the SDK's parameters (statusCode among them) where
	// Classify can still read them - same handling as lserr.ErrVolumeFailedToGet.
	return lserr.NewError(e.
		WithErrors(lfmt.Errorf("%s: %w", msg, cause)).
		WithParameters(psdkErr.GetParameters()).
		WithKVparameters("sdkErrorCode", string(psdkErr.GetErrorCode())))
}
