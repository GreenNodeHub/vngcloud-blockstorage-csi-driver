package cloud

import (
	lctx "context"
	lfmt "fmt"
	"strings"

	ljmath "github.com/cuongpiger/joat/math"
	lsdkClientV2 "github.com/vngcloud/vngcloud-go-sdk/v2/client"
	lsdkEntity "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/entity"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
	lsdkComputeV2 "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/services/compute/v2"
	lsdkVolumeV1 "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/services/volume/v1"
	lsdkVolumeV2 "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/services/volume/v2"
	llog "k8s.io/klog/v2"

	lsentity "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/entity"
	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
	lsutil "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/util"
)

// NewCloud builds the cloud client. It makes no vServer call and keeps its
// (Cloud, error) signature for its callers: the one thing it needs from vServer
// - the portal project id every request URL embeds - is resolved on first use
// instead, so an unreachable IaaS no longer reaches the panic(err) in
// newControllerService. See projectClient.
func NewCloud(iamURL, vserverUrl, clientID, clientSecret string, metadataSvc MetadataService) (Cloud, error) {
	clientCfg := lsdkClientV2.NewSdkConfigure().
		WithClientId(clientID).
		WithClientSecret(clientSecret).
		WithIamEndpoint(iamURL).
		WithVServerEndpoint(vserverUrl)

	// WithHttpClient must be called BEFORE Configure: Configure only creates an
	// http client of its own when the field is still nil (client/client.go).
	cloudClient := lsdkClientV2.NewClient(lctx.TODO()).
		WithHttpClient(NewThrottledHTTPClient(lctx.TODO())).
		Configure(clientCfg)

	llog.V(5).InfoS("[DEBUG] - NewCloud: Built the cloud client",
		"iamURL", iamURL, "vserverUrl", vserverUrl, "clientID", clientID)

	return &cloud{
		metadataService:    metadataSvc,
		baseClient:         cloudClient,
		portalLookup:       DefaultPortalLookup,
		projectClientCache: newPermanentMetaCache[lsdkClientV2.IClient](),
		zonesCache:         newMetaCache[*lsentity.ListZones](metaCacheTTL),
		volumeTypeCache:    newKeyedMetaCache[string](metaCacheTTL),
	}, nil
}

type (
	cloud struct {
		metadataService MetadataService

		// baseClient carries no project id. Only the portal lookup may use it
		// directly; everything else goes through projectClient, which is the
		// only thing that knows the project id. See project_scope.go.
		baseClient         lsdkClientV2.IClient
		portalLookup       PortalLookupFunc
		projectClientCache *metaCache[lsdkClientV2.IClient]

		// Catalog lookups on the CreateVolume path. See metacache.go.
		zonesCache      *metaCache[*lsentity.ListZones]
		volumeTypeCache *keyedMetaCache[string]
	}

	// ModifyDiskOptions represents parameters to modify a volume
	ModifyDiskOptions struct {
		VolumeType string
	}
)

func (s *cloud) EitherCreateResizeVolume(preq lsdkVolumeV2.ICreateBlockVolumeRequest) (*lsentity.Volume, lserr.IError) {
	var (
		vol, tmpVol *lsdkEntity.Volume
		serr        lserr.IError
		sdkErr      lsdkErrs.IError
	)

	client, ierr := s.projectClient()
	if ierr != nil {
		return nil, ierr
	}

	// Get the volume depend on the volume name
	if preq.GetVolumeName() != "" {
		llog.InfoS("[INFO] - EitherCreateResizeVolume: Get the volume by name", "volumeName", preq.GetVolumeName())
		vol, serr = s.getVolumeByName(preq.GetVolumeName())
		if serr != nil {
			if !serr.IsError(lsdkErrs.EcVServerVolumeNotFound) {
				llog.ErrorS(serr.GetError(), "[ERROR] - EitherCreateResizeVolume: Failed to get the volume by name", serr.GetListParameters()...)
				return nil, serr
			}
		}
	}

	if vol != nil {
		newSize := ljmath.MaxNumeric(vol.Size, uint64(preq.GetSize()))
		newVolumeType := preq.GetVolumeType()
		if vol.Size != newSize || vol.VolumeTypeID != newVolumeType {
			llog.InfoS("[INFO] - EitherCreateResizeVolume: Resize the volume", "volumeID", vol.Id, "newSize", newSize, "newVolumeType", newVolumeType)
			opt := lsdkVolumeV2.NewResizeBlockVolumeByIdRequest(newVolumeType, vol.Id, int(newSize))
			tmpVol, sdkErr = client.VServerGateway().V2().VolumeService().ResizeBlockVolumeById(opt)
			if sdkErr != nil {
				if sdkErr.IsError(lsdkErrs.EcVServerVolumeUnchanged) {
					return &lsentity.Volume{Volume: tmpVol}, nil
				}

				llog.ErrorS(sdkErr.GetError(), "[ERROR] - EitherCreateResizeVolume: Failed to resize the volume", sdkErr.GetListParameters()...)
				return nil, lserr.NewError(sdkErr)
			}

			vol = tmpVol
		}

		return &lsentity.Volume{Volume: vol}, nil
	}

	llog.InfoS("[INFO] - EitherCreateResizeVolume: Create the volume", preq.GetListParameters()...)
	vol, sdkErr = client.VServerGateway().V2().VolumeService().CreateBlockVolume(preq)
	if sdkErr != nil {
		llog.ErrorS(sdkErr.GetError(), "[ERROR] - EitherCreateResizeVolume: Failed to create the volume", sdkErr.GetListParameters()...)
		return nil, lserr.NewError(sdkErr)
	}

	llog.InfoS("[INFO] - EitherCreateResizeVolume: Created the volume successfully", "volumeID", vol.Id, "zoneId", vol.ZoneId)
	return &lsentity.Volume{
		Volume: vol,
	}, nil
}

func (s *cloud) GetVolumeByName(pvolName string) (*lsentity.Volume, lserr.IError) {
	vol, serr := s.getVolumeByName(pvolName)
	if serr != nil {
		return nil, serr
	}

	return &lsentity.Volume{
		Volume: vol,
	}, nil
}

func (s *cloud) GetVolume(volumeID string) (*lsentity.Volume, lserr.IError) {
	vol, serr := s.getVolumeById(volumeID)
	if serr != nil {
		return nil, serr
	}

	return &lsentity.Volume{
		Volume: vol,
	}, nil
}

// DeleteVolume deletes the volume, waiting until it reaches a deletable state
// before issuing the delete.
//
// The previous implementation reached its verdict through a sentinel variable
// after `_ = ljwait.ExponentialBackoff(...)`: if the volume never became
// CanDelete, then after 10 minutes `ierr` was still nil and the function
// reported SUCCESS. external-provisioner would then remove the PV while the
// volume lived on at the IaaS forever - orphaned, still billed, with no
// Kubernetes object pointing at it.
func (s *cloud) DeleteVolume(pctx lctx.Context, volID string) lserr.IError {
	llog.InfoS("[INFO] - DeleteVolume: Start deleting the volume", "volumeId", volID)

	client, ierr := s.projectClient()
	if ierr != nil {
		return ierr
	}

	vol, sdkErr := s.getVolumeById(volID)
	if sdkErr != nil {
		if sdkErr.IsError(lsdkErrs.EcVServerVolumeNotFound) {
			llog.InfoS("[INFO] - DeleteVolume: The volume was deleted before", "volumeId", volID)
			return nil
		}

		return lserr.ErrVolumeFailedToGet(volID, sdkErr)
	}

	// Wait for the volume to become deletable. The ctx is the deadline.
	if !vol.CanDelete() {
		if err := s.waitVolumeDeletable(pctx, volID); err != nil {
			return err
		}
	}

	if sdkErr := client.VServerGateway().V2().VolumeService().
		DeleteBlockVolumeById(lsdkVolumeV2.NewDeleteBlockVolumeByIdRequest(volID)); sdkErr != nil {
		if sdkErr.IsError(lsdkErrs.EcVServerVolumeNotFound) {
			llog.InfoS("[INFO] - DeleteVolume: The volume was deleted before", "volumeId", volID)
			return nil
		}

		ierr := lserr.ErrVolumeFailedToDelete(volID, sdkErr)
		llog.ErrorS(ierr.GetError(), "[ERROR] - DeleteVolume: Failed to delete the volume", ierr.GetListParameters()...)

		return ierr
	}

	llog.InfoS("[INFO] - DeleteVolume: Deleted the volume successfully", "volumeId", volID)

	return nil
}

// AttachVolume attaches the volume to the instance and waits until vServer
// reports the attachment complete.
//
// Shape: read state -> issue the command EXACTLY ONCE -> read-only poll with
// the ctx as the deadline. Mirrors DetachVolume below and aws-ebs-csi-driver's
// AttachDisk.
func (s *cloud) AttachVolume(pctx lctx.Context, pinstanceId, pvolumeId string) (*lsentity.Volume, lserr.IError) {
	client, ierr := s.projectClient()
	if ierr != nil {
		return nil, ierr
	}

	vol, ierr := s.getVolumeForAttach(pvolumeId)
	if ierr != nil {
		return nil, ierr
	}

	if vol.AttachedTheInstance(pinstanceId) {
		// Fully attached only when the status is IN-USE too. A retry landing
		// while the volume is still transitional (VmId set, status ATTACHING/
		// PROCESSING) must NOT report success yet - ControllerPublishVolume
		// would hand out a devicePath for a block device that does not exist on
		// the VM. The attach is already in flight, so skip the mutate and just
		// wait; the same predicate waitDiskAttached uses.
		if vol.Status == VolumeInUseStatus {
			llog.InfoS("[INFO] - AttachVolume: The volume is already attached", "volumeId", pvolumeId, "instanceId", pinstanceId)
			return lsentity.NewVolume(vol), nil
		}

		llog.InfoS("[INFO] - AttachVolume: Attach already in flight, waiting",
			"volumeId", pvolumeId, "instanceId", pinstanceId, "status", vol.Status)

		return s.waitDiskAttached(pctx, pinstanceId, pvolumeId)
	}

	// Issue the attach exactly once. If the ctx expires during the poll, the CO
	// retries the whole RPC and that retry takes the "already attached" fast
	// path above.
	llog.InfoS("[INFO] - AttachVolume: Attaching the volume", "volumeId", pvolumeId, "instanceId", pinstanceId)
	if sdkErr := client.VServerGateway().V2().ComputeService().
		AttachBlockVolume(lsdkComputeV2.NewAttachBlockVolumeRequest(pinstanceId, pvolumeId)); sdkErr != nil {
		switch sdkErr.GetErrorCode() {
		case lsdkErrs.EcVServerVolumeAlreadyAttachedThisServer:
			// Goal already reached - fall through to the wait to confirm IN-USE.
		case lsdkErrs.EcVServerVolumeInProcess:
			// The IaaS REJECTED the attach because another operation owns the
			// volume, so nothing was queued and there is no attachment to wait
			// for. Returning straight away - what this did before - is correct
			// but expensive: it hands the whole delay to the CO's retry
			// backoff, which starts at 1s and doubles, so four rejections cost
			// ~15s of pure sleeping on top of four round trips. Measured on the
			// dev cluster, a pod's first attach took 39-40s that way, of which
			// only a few seconds were IaaS work.
			//
			// So wait for the LOCK to clear rather than for an attach to
			// finish, on a short bounded budget, then issue the attach once
			// more. This is the one place those two differ: a freshly created
			// volume clears in ~15-20s, and collapsing four CO rounds into one
			// RPC is where the time comes back.
			//
			// If it does not clear in time the old behaviour resumes exactly -
			// return, release the inflight entry, let the CO retry - because a
			// volume held by something genuinely stuck must not keep a handler
			// parked on it.
			llog.InfoS("[INFO] - AttachVolume: The volume is busy, waiting for it to clear",
				"volumeId", pvolumeId, "errorCode", sdkErr.GetStringErrorCode())

			cleared, werr := s.waitVolumeAttachable(pctx, pinstanceId, pvolumeId)
			if werr != nil {
				llog.InfoS("[INFO] - AttachVolume: Still busy, returning for the CO to retry",
					"volumeId", pvolumeId, "errorCode", sdkErr.GetStringErrorCode())

				return nil, lserr.ErrVolumeFailedToAttach(pinstanceId, pvolumeId, sdkErr)
			}

			// The wait's predicate also accepts "already attached to us", which
			// happens when the operation holding the volume WAS our own attach
			// from an earlier RPC. Nothing left to issue.
			if cleared.AttachedTheInstance(pinstanceId) {
				llog.InfoS("[INFO] - AttachVolume: The volume attached while waiting",
					"volumeId", pvolumeId, "instanceId", pinstanceId)

				return lsentity.NewVolume(cleared), nil
			}

			// Exactly one more attempt. A second rejection means the volume is
			// contended beyond what a single RPC should absorb, so it goes back
			// to the CO rather than looping here.
			llog.InfoS("[INFO] - AttachVolume: Re-issuing the attach after the volume cleared",
				"volumeId", pvolumeId, "instanceId", pinstanceId)

			if retryErr := client.VServerGateway().V2().ComputeService().
				AttachBlockVolume(lsdkComputeV2.NewAttachBlockVolumeRequest(pinstanceId, pvolumeId)); retryErr != nil &&
				retryErr.GetErrorCode() != lsdkErrs.EcVServerVolumeAlreadyAttachedThisServer {
				ierr = lserr.ErrVolumeFailedToAttach(pinstanceId, pvolumeId, retryErr)
				llog.ErrorS(ierr.GetError(),
					"[ERROR] - AttachVolume: The re-issued attach failed", ierr.GetListParameters()...)

				return nil, ierr
			}
		default:
			ierr = lserr.ErrVolumeFailedToAttach(pinstanceId, pvolumeId, sdkErr)
			llog.ErrorS(ierr.GetError(), "[ERROR] - AttachVolume: Failed to attach the volume", ierr.GetListParameters()...)

			return nil, ierr
		}
	}

	return s.waitDiskAttached(pctx, pinstanceId, pvolumeId)
}

// getVolumeForAttach reads the volume state before attaching.
//
// Read errors MUST be told apart: previously every error from
// GetBlockVolumeById was reported as ErrVolumeNotFound, because the guard
// `sdkErr.IsError(NotFound) || vol == nil` was always true - the SDK returns
// vol == nil on EVERY error. A 429 or a 500 therefore surfaced as "volume does
// not exist", both misdirecting diagnosis and making the caller give up
// instead of retrying.
func (s *cloud) getVolumeForAttach(pvolumeId string) (*lsdkEntity.Volume, lserr.IError) {
	vol, sdkErr := s.getVolumeById(pvolumeId)
	if sdkErr != nil {
		if sdkErr.IsError(lsdkErrs.EcVServerVolumeNotFound) {
			ierr := lserr.ErrVolumeNotFound(pvolumeId)
			llog.ErrorS(ierr.GetError(), "[ERROR] - AttachVolume: Volume not found", ierr.GetListParameters()...)

			return nil, ierr
		}

		ierr := lserr.ErrVolumeFailedToGet(pvolumeId, sdkErr)
		llog.ErrorS(ierr.GetError(), "[ERROR] - AttachVolume: Failed to get the volume", ierr.GetListParameters()...)

		return nil, ierr
	}

	if vol.IsError() {
		ierr := lserr.ErrVolumeIsInErrorState(pvolumeId)
		llog.ErrorS(ierr.GetError(), "[ERROR] - AttachVolume: The volume is in error state", ierr.GetListParameters()...)

		return nil, ierr
	}

	return vol, nil
}

// DetachVolume detaches the volume from the instance and waits until vServer
// reports it fully released.
//
// Contract: idempotent (CSI spec 5.4). Calling it again on an already-detached
// volume must return nil, never an error - otherwise external-attacher never
// gets to remove the VolumeAttachment finalizer.
func (s *cloud) DetachVolume(pctx lctx.Context, pinstanceId, pvolumeId string) lserr.IError {
	client, resolveErr := s.projectClient()
	if resolveErr != nil {
		return resolveErr
	}

	vol, sdkErr := s.getVolumeById(pvolumeId)
	if sdkErr != nil {
		if sdkErr.IsError(lsdkErrs.EcVServerVolumeNotFound) {
			llog.InfoS("[INFO] - DetachVolume: The volume no longer exists, treat as detached", "volumeId", pvolumeId)
			return nil
		}

		ierr := lserr.ErrVolumeFailedToGet(pvolumeId, sdkErr)
		llog.ErrorS(ierr.GetError(), "[ERROR] - DetachVolume: Failed to get the volume", ierr.GetListParameters()...)

		return ierr
	}

	if isDetachedFrom(vol, pinstanceId) {
		llog.InfoS("[INFO] - DetachVolume: The volume is already detached from the instance",
			"volumeId", pvolumeId, "instanceId", pinstanceId, "status", vol.Status)

		return nil
	}

	if vol.IsError() {
		llog.InfoS("[INFO] - DetachVolume: The volume is in error state", "volumeId", pvolumeId)
		return lserr.ErrVolumeIsInErrorState(pvolumeId)
	}

	// Issue the detach exactly once. Previously this command sat inside the
	// poll loop and was re-issued every 10 seconds for up to 10 minutes.
	llog.InfoS("[INFO] - DetachVolume: Detaching the volume", "volumeId", pvolumeId, "instanceId", pinstanceId)
	if sdkErr = client.VServerGateway().V2().ComputeService().
		DetachBlockVolume(lsdkComputeV2.NewDetachBlockVolumeRequest(pinstanceId, pvolumeId)); sdkErr != nil {
		switch {
		case errSetDetachDone.ContainsOne(sdkErr.GetErrorCode()):
			llog.InfoS("[INFO] - DetachVolume: Nothing left to detach", "volumeId", pvolumeId,
				"instanceId", pinstanceId, "errorCode", sdkErr.GetStringErrorCode())

			return nil
		case errSetDetachRetryable.ContainsOne(sdkErr.GetErrorCode()):
			// The IaaS REJECTED the detach because another operation owns the
			// volume - nothing was queued. Falling into the read-only wait here
			// would poll for a detach nobody is performing: if the busy
			// operation is not itself a detach, the volume settles back to
			// attached and the poll burns the whole sidecar budget while
			// holding the inflight lock. Return instead; the CO retries and
			// the next RPC re-reads state and re-issues.
			llog.InfoS("[INFO] - DetachVolume: The volume is busy, returning for the CO to retry",
				"volumeId", pvolumeId, "errorCode", sdkErr.GetStringErrorCode())

			return lserr.ErrVolumeFailedToDetach(pinstanceId, pvolumeId, sdkErr)
		default:
			ierr := lserr.ErrVolumeFailedToDetach(pinstanceId, pvolumeId, sdkErr)
			llog.ErrorS(ierr.GetError(), "[ERROR] - DetachVolume: Failed to detach the volume", ierr.GetListParameters()...)

			return ierr
		}
	}

	return s.waitVolumeDetached(pctx, pinstanceId, pvolumeId)
}

// IsDetachedFrom is read-only: no DetachBlockVolume, one GetVolume.
//
// It reports detached in two cases: the volume no longer exists, or the read
// shows it AVAILABLE or not attached to this instance. A missing volume counts
// as detached because there is nothing left to detach, and reporting an error
// would keep external-attacher from removing the finalizer.
//
// This is NOT parity with errSetDetachDone, which is a set of codes returned by
// the detach COMMAND: EcVServerVolumeAvailable is only covered here
// incidentally, by isDetachedFrom's IsAvailable() check, and
// EcVServerServerNotFound has no analogue at all because GetVolume cannot
// return it.
func (s *cloud) IsDetachedFrom(pctx lctx.Context, pinstanceId, pvolumeId string) (bool, lserr.IError) {
	vol, ierr := s.getVolumeById(pvolumeId)
	if ierr != nil {
		if ierr.IsError(lsdkErrs.EcVServerVolumeNotFound) {
			return true, nil
		}

		return false, ierr
	}

	return isDetachedFrom(vol, pinstanceId), nil
}

func (s *cloud) ResizeOrModifyDisk(pctx lctx.Context, volumeID string, newSizeBytes int64, options *ModifyDiskOptions) (newSize int64, err error) {
	client, ierr := s.projectClient()
	if ierr != nil {
		return 0, ierr.GetError()
	}

	newSizeGiB := uint64(lsutil.RoundUpGiB(newSizeBytes))
	volume, sdkErr := s.GetVolume(volumeID)
	if sdkErr != nil {
		return 0, sdkErr.GetError()
	}

	if newSizeGiB < volume.Size {
		newSizeGiB = volume.Size
	}

	if options.VolumeType == "" {
		options.VolumeType = volume.VolumeTypeID
	}

	// Check that we need to modify this volume`
	needsModification, volumeSize, err := s.validateModifyVolume(pctx, volume, volumeID, newSizeGiB, options)
	if err != nil || !needsModification {
		return volumeSize, err
	}

	// The volume types are different => so please check the zone a same
	same, sdkErr2 := s.checkSameZone(options.VolumeType, volume.VolumeTypeID)
	if sdkErr2 != nil {
		return 0, sdkErr2.GetError()
	} else if !same && !volume.IsAttched() {
		// The target volume type lives in another zone => migrate before resizing.
		if ierr := s.migrateVolumeToType(pctx, volume, volumeID, options.VolumeType); ierr != nil {
			return 0, ierr.GetError()
		}
	}

	opt := lsdkVolumeV2.NewResizeBlockVolumeByIdRequest(volumeID, options.VolumeType, int(newSizeGiB))
	_, sdkErr = client.VServerGateway().V2().VolumeService().ResizeBlockVolumeById(opt)
	if sdkErr != nil && !sdkErr.IsError(lsdkErrs.EcVServerVolumeUnchanged) {
		return 0, sdkErr.GetError()
	}

	settled, err := s.waitVolumeAchieveStatus(pctx, volumeID, volumeArchivedStatus)
	if err != nil {
		return 0, err
	}

	// Perform one final check on the volume the wait just observed - no extra GET.
	return checkDesiredState(settled, volumeID, newSizeGiB, options)
}

// migrateVolumeToType moves the volume to a volume type in another zone and
// waits for completion.
//
// Same shape as DetachVolume: read state -> issue the command EXACTLY ONCE
// (and only when no migration is already running) -> read-only poll with the
// ctx as the deadline.
//
// Previously the migrate command sat inside the poll loop, and that loop used
// NewBackOff(10, 10, true, 30m): with Revert=true the FIRST sleep is
// floor((2^9-1)/2) = 255 seconds, while csi-resizer only waits 60.
// The caller passes the volume it already holds - this used to be the third
// sequential GET of the same object on the modify path.
func (s *cloud) migrateVolumeToType(pctx lctx.Context, vol *lsentity.Volume, pvolumeId, ptargetType string) lserr.IError {
	client, ierr := s.projectClient()
	if ierr != nil {
		return ierr
	}

	if isMigratedToType(vol, ptargetType) {
		llog.InfoS("[INFO] - migrateVolumeToType: The volume is already on the target type",
			"volumeId", pvolumeId, "volumeType", ptargetType)

		return nil
	}

	if !vol.IsMigration() && !vol.IsCreating() {
		llog.InfoS("[INFO] - migrateVolumeToType: Migrating the volume",
			"volumeId", pvolumeId, "volumeType", ptargetType)

		if migErr := client.VServerGateway().V2().VolumeService().
			MigrateBlockVolumeById(lsdkVolumeV2.NewMigrateBlockVolumeByIdRequest(pvolumeId, ptargetType).
				WithConfirm(true)); migErr != nil {
			if !errSetMigrateInProgress.ContainsOne(migErr.GetErrorCode()) {
				llog.ErrorS(migErr.GetError(), "[ERROR] - migrateVolumeToType: Failed to migrate the volume", migErr.GetListParameters()...)
				return lserr.NewError(migErr)
			}

			llog.InfoS("[INFO] - migrateVolumeToType: Migration is already in progress",
				"volumeId", pvolumeId, "errorCode", migErr.GetStringErrorCode())
		}
	}

	return s.waitVolumeMigrated(pctx, pvolumeId, ptargetType)
}

func (s *cloud) ModifyVolumeType(pctx lctx.Context, pvolumeId, pvolumeType string, psize int) lserr.IError {
	llog.InfoS("[INFO] - ModifyVolumeType: Modify the volume type", "volumeId", pvolumeId, "volumeType", pvolumeType, "size", psize)

	client, ierr := s.projectClient()
	if ierr != nil {
		return ierr
	}

	opts := lsdkVolumeV2.NewResizeBlockVolumeByIdRequest(pvolumeId, pvolumeType, psize)

	if _, sdkErr := client.VServerGateway().V2().VolumeService().ResizeBlockVolumeById(opts); sdkErr != nil {
		if !sdkErr.IsError(lsdkErrs.EcVServerVolumeUnchanged) {
			llog.ErrorS(sdkErr.GetError(), "[ERROR] - ModifyVolumeType: Failed to modify the volume type", sdkErr.GetListParameters()...)
			return lserr.NewError(sdkErr)
		}
	}

	llog.InfoS("[INFO] - ModifyVolumeType: Request accepted, waiting for the volume to settle",
		"volumeId", pvolumeId, "volumeType", pvolumeType, "size", psize)

	// This wait used to be `_ = ljwait.ExponentialBackoff(...)` with a sentinel
	// `ierr` verdict: if the volume never reached the target type within 20
	// minutes, ierr stayed nil and the function reported SUCCESS. Now a timeout
	// is returned as an error.
	return s.waitVolumeMigrated(pctx, pvolumeId, pvolumeType)
}

func (s *cloud) ExpandVolume(pctx lctx.Context, volumeID, volumeTypeID string, newSize uint64) error {
	_, err := s.ResizeOrModifyDisk(pctx, volumeID, lsutil.GiBToBytes(int64(newSize)), &ModifyDiskOptions{
		VolumeType: volumeTypeID,
	})
	return err
}

func (s *cloud) GetDeviceDiskID(pvolID string) (string, error) {
	client, ierr := s.projectClient()
	if ierr != nil {
		return "", ierr.GetError()
	}

	opts := lsdkVolumeV2.NewGetBlockVolumeByIdRequest(pvolID)
	vol, err := client.VServerGateway().V2().VolumeService().GetUnderBlockVolumeId(opts)
	if err != nil {
		llog.ErrorS(err.GetError(), "[ERROR] - GetDeviceDiskID: Failed to get the device disk ID", err.GetListParameters()...)
		return "", err.GetError()
	}

	return vol.UnderId, nil
}

// GetVolumeSnapshotByName is the idempotency check on the create path, so a
// snapshot it fails to find is a snapshot the driver creates a second time.
// It used to ask for page 1 of ten and ignore the rest, which on a volume with
// more than ten snapshots reported ErrSnapshotNotFound for a snapshot that
// exists - once every retry, each one a duplicate against a shared project
// quota.
//
// The walk ends on the first match, at the page count the server reports, at a
// short or empty page, or at snapshotListMaxPages, whichever comes first. It
// ends on pctx too: this runs inside waitSnapshotActive, whose caller gives up
// after 15 seconds.
func (s *cloud) GetVolumeSnapshotByName(pctx lctx.Context, pvolID, psnapshotName string) (*lsentity.Snapshot, error) {
	client, ierr := s.projectClient()
	if ierr != nil {
		return nil, ierr.GetError()
	}

	volumeService := client.VServerGateway().V2().VolumeService()
	for page := 1; page <= snapshotListMaxPages; page++ {
		if err := pctx.Err(); err != nil {
			return nil, err
		}

		opt := lsdkVolumeV2.NewListSnapshotsByBlockVolumeIdRequest(page, snapshotListPageSize, pvolID)
		res, err := volumeService.ListSnapshotsByBlockVolumeId(opt)
		if err != nil {
			return nil, err.GetError()
		}
		if res == nil {
			break
		}

		for _, snap := range res.Items {
			if snap.VolumeId == pvolID && snap.Name == psnapshotName {
				return &lsentity.Snapshot{Snapshot: snap}, nil
			}
		}

		// Two stop conditions, and which one applies depends on whether the
		// server populated TotalPages - they must not be OR'd together.
		//
		// When TotalPages is usable it is the authority. Falling back to "this
		// page was shorter than I asked for" in that case would mis-stop if the
		// server ever caps the page size below the requested 100: every page
		// would look short, the walk would end after page one, and this lookup
		// would silently be the single-page version again. That is not a
		// cosmetic regression - a missed lookup makes the create path produce a
		// duplicate snapshot on every retry, which is the whole reason this
		// walks pages at all.
		//
		// When TotalPages is 0 or negative the server told us nothing, and
		// trusting it would stop at page one for the opposite reason
		// (1 >= 0). Only then is the short-page heuristic the best signal
		// available.
		if res.TotalPages > 0 {
			if page >= res.TotalPages {
				break
			}

			continue
		}
		if len(res.Items) < snapshotListPageSize {
			break
		}
	}

	return nil, ErrSnapshotNotFound
}

func (s *cloud) CreateSnapshotFromVolume(pctx lctx.Context, pclusterId, pvolId, psnapshotName string) (*lsentity.Snapshot, error) {
	client, ierr := s.projectClient()
	if ierr != nil {
		return nil, ierr.GetError()
	}

	opt := lsdkVolumeV2.NewCreateSnapshotByBlockVolumeIdRequest(psnapshotName, pvolId).
		WithPermanently(true).
		WithDescription(lfmt.Sprintf(patternSnapshotDescription, pvolId, pclusterId))

	snapshot, sdkErr := client.VServerGateway().V2().VolumeService().CreateSnapshotByBlockVolumeId(opt)
	if sdkErr != nil {
		return nil, sdkErr.GetError()
	}

	err := s.waitSnapshotActive(pctx, pvolId, snapshot.Name)
	return &lsentity.Snapshot{Snapshot: snapshot}, err
}

func (s *cloud) DeleteSnapshot(psnapshotID string) error {
	client, ierr := s.projectClient()
	if ierr != nil {
		return ierr.GetError()
	}

	opt := lsdkVolumeV2.NewDeleteSnapshotByIdRequest(psnapshotID)
	sdkErr := client.VServerGateway().V2().VolumeService().DeleteSnapshotById(opt)
	if sdkErr != nil {
		if !sdkErr.IsError(lsdkErrs.EcVServerSnapshotNotFound) {
			return sdkErr.GetError()
		}
	}
	return nil
}

func (s *cloud) ListSnapshots(pvolID string, ppage int, ppageSize int) (*lsentity.ListSnapshots, lserr.IError) {
	client, ierr := s.projectClient()
	if ierr != nil {
		return nil, ierr
	}

	opt := lsdkVolumeV2.NewListSnapshotsByBlockVolumeIdRequest(ppage, ppageSize, pvolID)
	res, sdkErr := client.VServerGateway().V2().VolumeService().ListSnapshotsByBlockVolumeId(opt)
	if sdkErr != nil {
		return nil, lserr.NewError(sdkErr)
	}

	return &lsentity.ListSnapshots{ListSnapshots: res}, nil
}

func (s *cloud) GetVolumeTypeById(pvolTypeId string) (*lsentity.VolumeType, lserr.IError) {
	client, ierr := s.projectClient()
	if ierr != nil {
		return nil, ierr
	}

	opt := lsdkVolumeV1.NewGetVolumeTypeByIdRequest(pvolTypeId)
	volType, err := client.VServerGateway().V1().VolumeService().GetVolumeTypeById(opt)
	if err != nil {
		return nil, lserr.NewError(err)
	}

	return &lsentity.VolumeType{VolumeType: volType}, nil
}

func (s *cloud) GetDefaultVolumeType() (*lsentity.VolumeType, lserr.IError) {
	client, ierr := s.projectClient()
	if ierr != nil {
		return nil, ierr
	}

	volType, err := client.VServerGateway().V1().VolumeService().GetDefaultVolumeType()
	if err != nil {
		return nil, lserr.NewError(err)
	}

	return &lsentity.VolumeType{VolumeType: volType}, nil
}

func (s *cloud) GetVolumeTypeIdByName(zoneId, volumeName string) (string, lserr.IError) {
	parts := strings.Split(volumeName, "-")
	if len(parts) != 2 {
		return volumeName, nil
	}
	volTypeName := strings.ToUpper(parts[0])
	iopsName := strings.TrimPrefix(parts[1], "iops")

	return s.volumeTypeCache.get(zoneId+"/"+volumeName, func() (string, lserr.IError) {
		return s.lookupVolumeTypeId(zoneId, volumeName, volTypeName, iopsName)
	})
}

func (s *cloud) lookupVolumeTypeId(zoneId, volumeName, volTypeName, iopsName string) (string, lserr.IError) {
	client, ierr := s.projectClient()
	if ierr != nil {
		return "", ierr
	}

	req := lsdkVolumeV1.NewGetVolumeTypeZonesRequest(zoneId)
	res, sdkErr := client.VServerGateway().V1().VolumeService().GetVolumeTypeZones(req)
	if sdkErr != nil {
		return "", lserr.NewError(sdkErr)
	}

	for _, vtZone := range res.VolumeTypeZones {
		if vtZone == nil || vtZone.Name != volTypeName {
			continue
		}

		listReq := lsdkVolumeV1.NewListVolumeTypeRequest(vtZone.Id)
		listRes, sdkErr := client.VServerGateway().V1().VolumeService().GetListVolumeTypes(listReq)
		if sdkErr != nil {
			return "", lserr.NewError(sdkErr)
		}

		for _, vt := range listRes.VolumeTypes {
			if vt != nil && vt.Name == iopsName {
				llog.InfoS("[INFO] - GetVolumeTypeIdByName: Found volume type ID",
					"volumeTypeId", vt.Id, "zoneId", zoneId, "volumeName", volumeName)
				return vt.Id, nil
			}
		}
		break
	}
	llog.InfoS("[INFO] - GetVolumeTypeIdByName: Volume type ID response", "zoneId", zoneId, "volumeName", volumeName)
	return volumeName, nil
}

func (s *cloud) GetListZones() (*lsentity.ListZones, lserr.IError) {
	return s.zonesCache.get(func() (*lsentity.ListZones, lserr.IError) {
		client, ierr := s.projectClient()
		if ierr != nil {
			return nil, ierr
		}

		res, sdkErr := client.VServerGateway().V1().PortalService().ListZones()
		if sdkErr != nil {
			return nil, lserr.NewError(sdkErr)
		}

		return &lsentity.ListZones{ListZones: res}, nil
	})
}
