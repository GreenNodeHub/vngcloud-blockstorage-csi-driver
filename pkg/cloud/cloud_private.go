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

// notFoundPolicy states what a wait loop does when the volume vanishes
// mid-poll. It must be explicit per operation: for a detach or a delete a
// missing volume IS the goal, while for an attach or a migrate the goal has
// become unreachable and polling on is guaranteed futile.
type notFoundPolicy int

const (
	notFoundIsDone notFoundPolicy = iota
	notFoundFails
)

// volumeWaitSpec parameterizes waitVolume. Everything the five wait loops used
// to copy from one another now lives in one place, so a fix to the polling
// skeleton applies everywhere at once.
type volumeWaitSpec struct {
	opName     string
	backoff    lwait.Backoff
	onNotFound notFoundPolicy
	// failOnErrorState aborts as soon as the volume reports ERROR. Waits whose
	// done-predicate already accepts ERROR (CanDelete does) leave this false.
	failOnErrorState bool
	done             func(pvol *lsdkEntity.Volume) bool
	wrap             func(psdkErr lsdkErrs.IError) lserr.IError
}

// waitVolume is the single polling skeleton behind every volume wait.
//
// Transient read errors do not abort the wait: the mutating command was
// already issued, and giving up over a single 500 or a shed request would fail
// the RPC for nothing. aws-ebs-csi-driver does the same in
// WaitForAttachmentState ("Ignoring error from describe volume, will retry").
// The ctx still bounds the loop. VolumeNotFound is the one read error handled
// by policy instead of tolerated, because no amount of waiting changes it.
func (s *cloud) waitVolume(pctx lctx.Context, pvolumeId string, pspec volumeWaitSpec) (*lsdkEntity.Volume, lserr.IError) {
	var res *lsdkEntity.Volume

	start := ltime.Now()
	waitErr := lwait.ExponentialBackoffWithContext(pctx, pspec.backoff, func(_ lctx.Context) (bool, error) {
		vol, err := s.getVolumeById(pvolumeId)
		if err != nil {
			if err.IsError(lsdkErrs.EcVServerVolumeNotFound) {
				if pspec.onNotFound == notFoundIsDone {
					llog.InfoS("[INFO] - "+pspec.opName+": The volume no longer exists, goal reached", "volumeId", pvolumeId)
					return true, nil
				}

				return false, lserr.ErrVolumeNotFound(pvolumeId).GetError()
			}

			llog.InfoS("[WARN] - "+pspec.opName+": Ignoring error while polling, will retry",
				"volumeId", pvolumeId, "err", err.GetMessage())

			return false, nil
		}

		if pspec.done(vol) {
			llog.V(4).InfoS("[DEBUG] - "+pspec.opName+": goal reached",
				"volumeId", pvolumeId, "elapsed", ltime.Since(start).String())
			res = vol

			return true, nil
		}

		if pspec.failOnErrorState && vol.IsError() {
			return false, lserr.ErrVolumeIsInErrorState(pvolumeId).GetError()
		}

		return false, nil
	})

	if waitErr != nil {
		llog.ErrorS(waitErr, "[ERROR] - "+pspec.opName+": Gave up waiting",
			"volumeId", pvolumeId, "elapsed", ltime.Since(start).String())

		return nil, pspec.wrap(nil).WithErrors(waitErr)
	}

	return res, nil
}

// waitDiskAttached polls until the volume is attached to the right instance.
// Read-only - the attach command was already issued once in AttachVolume.
func (s *cloud) waitDiskAttached(pctx lctx.Context, pinstanceId, pvolumeId string) (*lsentity.Volume, lserr.IError) {
	vol, ierr := s.waitVolume(pctx, pvolumeId, volumeWaitSpec{
		opName:  "waitDiskAttached",
		backoff: volumeOperationBackoff,
		// A volume that vanished can never become attached.
		onNotFound:       notFoundFails,
		failOnErrorState: true,
		done: func(pvol *lsdkEntity.Volume) bool {
			return pvol.AttachedTheInstance(pinstanceId) && pvol.Status == VolumeInUseStatus
		},
		wrap: func(psdkErr lsdkErrs.IError) lserr.IError {
			return lserr.ErrVolumeFailedToAttach(pinstanceId, pvolumeId, psdkErr)
		},
	})
	if ierr != nil {
		return nil, ierr
	}

	return lsentity.NewVolume(vol), nil
}

// waitVolumeDetached polls until the volume is no longer attached to the
// instance. Read-only - the detach command was already issued once in
// DetachVolume.
func (s *cloud) waitVolumeDetached(pctx lctx.Context, pinstanceId, pvolumeId string) lserr.IError {
	_, ierr := s.waitVolume(pctx, pvolumeId, volumeWaitSpec{
		opName:           "waitVolumeDetached",
		backoff:          volumeOperationBackoff,
		onNotFound:       notFoundIsDone,
		failOnErrorState: true,
		done: func(pvol *lsdkEntity.Volume) bool {
			return isDetachedFrom(pvol, pinstanceId)
		},
		wrap: func(psdkErr lsdkErrs.IError) lserr.IError {
			return lserr.ErrVolumeFailedToDetach(pinstanceId, pvolumeId, psdkErr)
		},
	})

	return ierr
}

// waitVolumeDeletable waits for the volume to reach a deletable state.
// A volume that disappears mid-wait counts as deleted (idempotency). No
// failOnErrorState: CanDelete already accepts ERROR as deletable.
func (s *cloud) waitVolumeDeletable(pctx lctx.Context, pvolumeId string) lserr.IError {
	_, ierr := s.waitVolume(pctx, pvolumeId, volumeWaitSpec{
		opName:     "waitVolumeDeletable",
		backoff:    volumeOperationBackoff,
		onNotFound: notFoundIsDone,
		done: func(pvol *lsdkEntity.Volume) bool {
			return pvol.CanDelete()
		},
		wrap: func(psdkErr lsdkErrs.IError) lserr.IError {
			return lserr.ErrVolumeFailedToDelete(pvolumeId, psdkErr)
		},
	})

	return ierr
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
	_, ierr := s.waitVolume(pctx, pvolumeId, volumeWaitSpec{
		opName:  "waitVolumeMigrated",
		backoff: volumeMigrationBackoff,
		// A volume that vanished can never reach the target type.
		onNotFound:       notFoundFails,
		failOnErrorState: true,
		done: func(pvol *lsdkEntity.Volume) bool {
			return isMigratedToType(lsentity.NewVolume(pvol), ptargetType)
		},
		wrap: func(psdkErr lsdkErrs.IError) lserr.IError {
			return lserr.ErrVolumeFailedToGet(pvolumeId, psdkErr)
		},
	})

	return ierr
}

// validateModifyVolume takes the already-fetched volume from its caller
// instead of re-reading it - the modify path used to GET the same object up to
// five times per RPC.
func (s *cloud) validateModifyVolume(pctx lctx.Context, volume *lsentity.Volume, volumeID string, newSizeGiB uint64, options *ModifyDiskOptions) (bool, int64, error) {
	// If we're asked to modify a volume to its current state, ignore the
	// request and immediately return a success.
	if !needsVolumeModification(volume, newSizeGiB, options) {
		// Wait for any existing modifications to prevent race conditions where
		// DescribeVolume(s) returns the new state before the volume is actually
		// finished modifying. This guard used to test the wrong variable (`err`
		// from an earlier, already-returned read instead of `err2`), silently
		// swallowing a timed-out wait.
		settled, err2 := s.waitVolumeAchieveStatus(pctx, volumeID, volumeArchivedStatus)
		if err2 != nil {
			return true, int64(volume.Size), err2
		}

		returnGiB, returnErr := checkDesiredState(settled, volumeID, newSizeGiB, options)
		return false, returnGiB, returnErr
	}

	return true, 0, nil
}

// checkDesiredState verifies the supplied volume against the requested
// modification; the caller passes the freshly-observed volume so no extra GET
// is needed.
func checkDesiredState(volume *lsentity.Volume, volumeID string, desiredSizeGiB uint64, options *ModifyDiskOptions) (int64, error) {
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
	vol, ierr := s.waitVolume(pctx, pvolID, volumeWaitSpec{
		opName:  "waitVolumeAchieveStatus",
		backoff: volumeOperationBackoff,
		// A vanished volume will never reach the desired status.
		onNotFound: notFoundFails,
		done: func(pvol *lsdkEntity.Volume) bool {
			return pdesiredStatus.ContainsOne(pvol.Status)
		},
		wrap: func(psdkErr lsdkErrs.IError) lserr.IError {
			return lserr.ErrVolumeFailedToGet(pvolID, psdkErr)
		},
	})
	if ierr != nil {
		return nil, ierr.GetError()
	}

	return lsentity.NewVolume(vol), nil
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
