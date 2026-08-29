package cloud

import (
	lctx "context"
	lfmt "fmt"
	lstr "strings"
	ltime "time"

	ljset "github.com/cuongpiger/joat/data-structure/set"
	lset "github.com/cuongpiger/joat/data-structure/set"
	ljwait "github.com/cuongpiger/joat/utils/exponential-backoff"
	lsdkEntity "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/entity"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
	lsdkVolume "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/services/volume/v2"
	lerrgroup "golang.org/x/sync/errgroup"
	lwait "k8s.io/apimachinery/pkg/util/wait"
	llog "k8s.io/klog/v2"

	lsentity "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/entity"
	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
)

func (s *cloud) getVolumeByName(pvolName string) (*lsdkEntity.Volume, lserr.IError) {
	// The volume name is empty
	if pvolName == "" {
		return nil, new(lsdkErrs.SdkError).WithErrorCode(lsdkErrs.EcVServerVolumeNotFound)
	}

	// Get the volume depends on name
	opts := lsdkVolume.NewListBlockVolumesRequest(1, 10).WithName(pvolName)
	vols, sdkErr := s.client.VServerGateway().V2().VolumeService().ListBlockVolumes(opts)
	if sdkErr != nil {
		return nil, lserr.NewError(sdkErr)
	}

	// Get volume by name with greater than 1 vol
	if vols.Len() != 1 {
		return nil, new(lsdkErrs.SdkError).WithErrorCode(lsdkErrs.EcVServerVolumeNotFound)
	}

	// If all valid, return the first item
	return vols.Items[0], nil
}

func (s *cloud) getVolumeById(pvolId string) (*lsdkEntity.Volume, lserr.IError) {
	// Get the volume depends on id
	opts := lsdkVolume.NewGetBlockVolumeByIdRequest(pvolId)
	vol, sdkErr := s.client.VServerGateway().V2().VolumeService().GetBlockVolumeById(opts)
	if sdkErr != nil {
		return nil, lserr.NewError(sdkErr)
	}

	return vol, nil
}

func (s *cloud) waitSnapshotActive(pvolID, psnapshotName string) error {
	return ljwait.ExponentialBackoff(ljwait.NewBackOff(waitSnapshotActiveSteps, waitSnapshotActiveDelay, true, waitSnapshotActiveTimeout), func() (bool, error) {
		vol, err := s.GetVolumeSnapshotByName(pvolID, psnapshotName)
		if err != nil {
			return false, err
		}

		if vol.Status == SnapshotActiveStatus {
			return true, nil
		}

		return false, nil
	})
}

// isDetachedFrom reports whether the volume can be considered detached from
// the instance.
//
// This is the correct completion condition for ControllerUnpublishVolume under
// the CSI spec: the RPC must be idempotent, so every state that is "no longer
// attached to this particular instance" counts as success - including the
// volume being attached to a DIFFERENT instance.
func isDetachedFrom(pvol *lsdkEntity.Volume, pinstanceId string) bool {
	if pvol == nil {
		return false
	}

	return pvol.IsAvailable() || !pvol.AttachedTheInstance(pinstanceId)
}

// waitDiskAttached polls until the volume is attached to the right instance.
// Read-only - the attach command was already issued once in AttachVolume.
func (s *cloud) waitDiskAttached(pctx lctx.Context, pinstanceId, pvolumeId string) (*lsentity.Volume, lserr.IError) {
	var res *lsentity.Volume

	start := ltime.Now()
	waitErr := lwait.ExponentialBackoffWithContext(pctx, volumeOperationBackoff, func(_ lctx.Context) (bool, error) {
		vol, err := s.getVolumeById(pvolumeId)
		if err != nil {
			// A read error does NOT invalidate the attach command already sent.
			// Giving up here would fail the RPC over a single 500 or a DNS blip.
			// aws-ebs-csi-driver does exactly this in WaitForAttachmentState:
			// "Ignoring error from describe volume, will retry". The ctx still
			// bounds this loop.
			llog.InfoS("[WARN] - waitDiskAttached: Ignoring error while polling, will retry",
				"volumeId", pvolumeId, "err", err.GetMessage())

			return false, nil
		}

		if vol.IsError() {
			return false, lserr.ErrVolumeIsInErrorState(pvolumeId).GetError()
		}

		if vol.AttachedTheInstance(pinstanceId) && vol.Status == VolumeInUseStatus {
			llog.V(4).InfoS("[DEBUG] - waitDiskAttached: The volume is attached",
				"volumeId", pvolumeId, "instanceId", pinstanceId, "elapsed", ltime.Since(start).String())
			res = lsentity.NewVolume(vol)

			return true, nil
		}

		return false, nil
	})

	if waitErr != nil {
		ierr := lserr.ErrVolumeFailedToAttach(pinstanceId, pvolumeId, nil).WithErrors(waitErr)
		llog.ErrorS(waitErr, "[ERROR] - waitDiskAttached: Gave up waiting for the volume to attach",
			"volumeId", pvolumeId, "instanceId", pinstanceId, "elapsed", ltime.Since(start).String())

		return nil, ierr
	}

	return res, nil
}

// waitVolumeDetached polls until the volume is no longer attached to the
// instance. Read-only - the detach command was already issued once in
// DetachVolume.
func (s *cloud) waitVolumeDetached(pctx lctx.Context, pinstanceId, pvolumeId string) lserr.IError {
	start := ltime.Now()
	waitErr := lwait.ExponentialBackoffWithContext(pctx, volumeOperationBackoff, func(_ lctx.Context) (bool, error) {
		vol, err := s.getVolumeById(pvolumeId)
		if err != nil {
			if err.IsError(lsdkErrs.EcVServerVolumeNotFound) {
				llog.InfoS("[INFO] - waitVolumeDetached: The volume no longer exists, treat as detached", "volumeId", pvolumeId)
				return true, nil
			}

			llog.InfoS("[WARN] - waitVolumeDetached: Ignoring error while polling, will retry",
				"volumeId", pvolumeId, "err", err.GetMessage())

			return false, nil
		}

		if isDetachedFrom(vol, pinstanceId) {
			llog.V(4).InfoS("[DEBUG] - waitVolumeDetached: The volume is detached",
				"volumeId", pvolumeId, "instanceId", pinstanceId, "elapsed", ltime.Since(start).String())

			return true, nil
		}

		if vol.IsError() {
			return false, lserr.ErrVolumeIsInErrorState(pvolumeId).GetError()
		}

		return false, nil
	})

	if waitErr != nil {
		ierr := lserr.ErrVolumeFailedToDetach(pinstanceId, pvolumeId, nil).WithErrors(waitErr)
		llog.ErrorS(waitErr, "[ERROR] - waitVolumeDetached: Gave up waiting for the volume to detach",
			"volumeId", pvolumeId, "instanceId", pinstanceId, "elapsed", ltime.Since(start).String())

		return ierr
	}

	return nil
}

// waitVolumeDeletable waits for the volume to reach a deletable state.
// A volume that disappears mid-wait counts as deleted (idempotency).
func (s *cloud) waitVolumeDeletable(pctx lctx.Context, pvolumeId string) lserr.IError {
	start := ltime.Now()
	waitErr := lwait.ExponentialBackoffWithContext(pctx, volumeOperationBackoff, func(_ lctx.Context) (bool, error) {
		vol, err := s.getVolumeById(pvolumeId)
		if err != nil {
			if err.IsError(lsdkErrs.EcVServerVolumeNotFound) {
				return true, nil
			}

			llog.InfoS("[WARN] - waitVolumeDeletable: Ignoring error while polling, will retry",
				"volumeId", pvolumeId, "err", err.GetMessage())

			return false, nil
		}

		if vol.CanDelete() {
			llog.V(4).InfoS("[DEBUG] - waitVolumeDeletable: The volume can be deleted now",
				"volumeId", pvolumeId, "elapsed", ltime.Since(start).String())

			return true, nil
		}

		return false, nil
	})

	if waitErr != nil {
		llog.ErrorS(waitErr, "[ERROR] - waitVolumeDeletable: Gave up waiting for the volume to become deletable",
			"volumeId", pvolumeId, "elapsed", ltime.Since(start).String())

		return lserr.ErrVolumeFailedToDelete(pvolumeId, nil).WithErrors(waitErr)
	}

	return nil
}

// isMigratedToType reports whether the volume has settled on the target
// volume type. BOTH conditions are required: a settled status AND the right
// type.
func isMigratedToType(pvol *lsentity.Volume, ptargetType string) bool {
	if pvol == nil {
		return false
	}

	return volumeArchivedStatus.ContainsOne(pvol.Status) && pvol.VolumeTypeID == ptargetType
}

// waitVolumeMigrated polls until the volume reaches the target volume type.
// Read-only - the migrate command was already issued once in
// migrateVolumeToType.
func (s *cloud) waitVolumeMigrated(pctx lctx.Context, pvolumeId, ptargetType string) lserr.IError {
	start := ltime.Now()
	waitErr := lwait.ExponentialBackoffWithContext(pctx, volumeMigrationBackoff, func(_ lctx.Context) (bool, error) {
		raw, err := s.getVolumeById(pvolumeId)
		if err != nil {
			llog.InfoS("[WARN] - waitVolumeMigrated: Ignoring error while polling, will retry",
				"volumeId", pvolumeId, "err", err.GetMessage())

			return false, nil
		}

		vol := lsentity.NewVolume(raw)
		if isMigratedToType(vol, ptargetType) {
			llog.V(4).InfoS("[DEBUG] - waitVolumeMigrated: The volume reached the target type",
				"volumeId", pvolumeId, "volumeType", ptargetType, "elapsed", ltime.Since(start).String())

			return true, nil
		}

		if vol.IsError() {
			return false, lserr.ErrVolumeIsInErrorState(pvolumeId).GetError()
		}

		return false, nil
	})

	if waitErr != nil {
		llog.ErrorS(waitErr, "[ERROR] - waitVolumeMigrated: Gave up waiting for the migration",
			"volumeId", pvolumeId, "volumeType", ptargetType, "elapsed", ltime.Since(start).String())

		return lserr.ErrVolumeFailedToGet(pvolumeId, nil).WithErrors(waitErr)
	}

	return nil
}

func (s *cloud) validateModifyVolume(pctx lctx.Context, volumeID string, newSizeGiB uint64, options *ModifyDiskOptions) (bool, int64, error) {
	volume, err := s.GetVolume(volumeID)
	if err != nil {
		return true, 0, err.GetError()
	}

	// At this point, we know we are starting a new volume modification
	// If we're asked to modify a volume to its current state, ignore the request and immediately return a success
	if !needsVolumeModification(volume, newSizeGiB, options) {
		// Wait for any existing modifications to prevent race conditions where DescribeVolume(s) returns the new
		// state before the volume is actually finished modifying
		_, err2 := s.waitVolumeAchieveStatus(pctx, volumeID, volumeArchivedStatus)
		if err != nil {
			return true, int64(volume.Size), err2
		}

		returnGiB, returnErr := s.checkDesiredState(volumeID, newSizeGiB, options)
		return false, returnGiB, returnErr
	}

	return true, 0, nil
}

func (s *cloud) checkDesiredState(volumeID string, desiredSizeGiB uint64, options *ModifyDiskOptions) (int64, error) {
	volume, err := s.GetVolume(volumeID)
	if err != nil {
		return 0, err.GetError()
	}

	// AWS resizes in chunks of GiB (not GB)
	realSizeGiB := int64(volume.Size)

	// Check if there is a mismatch between the requested modification and the current volume
	// If there is, the volume is still modifying and we should not return a success
	if uint64(realSizeGiB) < desiredSizeGiB {
		return realSizeGiB, lfmt.Errorf("volume %q is still being expanded to %d size", volumeID, desiredSizeGiB)
	} else if options.VolumeType != "" && !lstr.EqualFold(volume.VolumeTypeID, options.VolumeType) {
		return realSizeGiB, lfmt.Errorf("volume %q is still being modified to type %q", volumeID, options.VolumeType)
	}

	return realSizeGiB, nil
}

func needsVolumeModification(volume *lsentity.Volume, newSizeGiB uint64, options *ModifyDiskOptions) bool {
	oldSizeGiB := volume.Size
	needsModification := false

	if oldSizeGiB < newSizeGiB {
		needsModification = true
	}

	if options.VolumeType != "" && !lstr.EqualFold(volume.VolumeTypeID, options.VolumeType) {
		needsModification = true
	}

	return needsModification
}

func (s *cloud) waitVolumeAchieveStatus(pctx lctx.Context, pvolID string, pdesiredStatus lset.Set[string]) (*lsentity.Volume, error) {
	var resVolume *lsentity.Volume
	err := lwait.ExponentialBackoffWithContext(pctx, volumeOperationBackoff, func(_ lctx.Context) (bool, error) {
		vol, err := s.getVolumeById(pvolID)
		if err != nil {
			return false, err.GetError()
		}

		if pdesiredStatus.ContainsOne(vol.Status) {
			resVolume = &lsentity.Volume{Volume: vol}
			return true, nil
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return resVolume, nil
}

func (s *cloud) checkSameZone(pvolTypeA, pvolTypeB string) (bool, lsdkErrs.IError) {
	if pvolTypeA == pvolTypeB {
		return true, nil
	}

	var (
		zoneSet = ljset.NewSet[string]()
		sdkErr  lsdkErrs.IError
	)

	group, _ := lerrgroup.WithContext(lctx.TODO())
	for _, volType := range []string{pvolTypeA, pvolTypeB} {
		tmpVolType := volType
		group.Go(func() error {
			vol, err := s.GetVolumeTypeById(tmpVolType)
			if err != nil {
				sdkErr = err
				return err.GetError()
			}

			zoneSet.Add(vol.ZoneId)
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return false, sdkErr
	}

	if zoneSet.IsEmpty() || zoneSet.Cardinality() != 1 {
		return false, nil
	}

	return true, nil
}
