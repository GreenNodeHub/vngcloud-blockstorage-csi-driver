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
	lsdkPortalSvcV1 "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/services/portal/v1"
	lsdkVolumeV1 "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/services/volume/v1"
	lsdkVolumeV2 "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/services/volume/v2"
	llog "k8s.io/klog/v2"

	lsentity "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/entity"
	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
	lsutil "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/util"
)

func NewCloud(iamURL, vserverUrl, clientID, clientSecret string, metadataSvc MetadataService) (Cloud, error) {
	projectID := metadataSvc.GetProjectID()
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

	llog.V(5).InfoS("[DEBUG] - NodeGetInfo: Get the portal info and quota",
		"underProjectId", projectID, "iamURL", iamURL, "vserverUrl", vserverUrl, "clientID", clientID)
	portal, sdkErr := cloudClient.VServerGateway().V1().PortalService().
		GetPortalInfo(lsdkPortalSvcV1.NewGetPortalInfoRequest(projectID))

	if sdkErr != nil {
		llog.ErrorS(sdkErr.GetError(), "[ERROR] - NodeGetInfo; failed to get portal info", "errMsg", sdkErr.GetErrorMessages())
		return nil, sdkErr.GetError()
	}

	llog.InfoS("[INFO] - NodeGetInfo: Received the portal info successfully", "portal", portal)
	cloudClient = cloudClient.WithProjectId(portal.ProjectID)

	return &cloud{
		metadataService: metadataSvc,
		client:          cloudClient,
	}, nil
}

type (
	cloud struct {
		metadataService MetadataService
		client          lsdkClientV2.IClient
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
			tmpVol, sdkErr = s.client.VServerGateway().V2().VolumeService().ResizeBlockVolumeById(opt)
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
	vol, sdkErr = s.client.VServerGateway().V2().VolumeService().CreateBlockVolume(preq)
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

	if sdkErr := s.client.VServerGateway().V2().VolumeService().
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
	vol, ierr := s.getVolumeForAttach(pvolumeId)
	if ierr != nil {
		return nil, ierr
	}

	if vol.AttachedTheInstance(pinstanceId) {
		llog.InfoS("[INFO] - AttachVolume: The volume is already attached", "volumeId", pvolumeId, "instanceId", pinstanceId)
		return lsentity.NewVolume(vol), nil
	}

	// Issue the attach exactly once. If the ctx expires during the poll, the CO
	// retries the whole RPC and that retry takes the "already attached" fast
	// path above.
	llog.InfoS("[INFO] - AttachVolume: Attaching the volume", "volumeId", pvolumeId, "instanceId", pinstanceId)
	if sdkErr := s.client.VServerGateway().V2().ComputeService().
		AttachBlockVolume(lsdkComputeV2.NewAttachBlockVolumeRequest(pinstanceId, pvolumeId)); sdkErr != nil {
		switch sdkErr.GetErrorCode() {
		case lsdkErrs.EcVServerVolumeAlreadyAttachedThisServer:
			// Goal already reached.
		case lsdkErrs.EcVServerVolumeInProcess:
			llog.InfoS("[INFO] - AttachVolume: The volume is in process, waiting", "volumeId", pvolumeId)
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
	vol, sdkErr := s.client.VServerGateway().V2().VolumeService().
		GetBlockVolumeById(lsdkVolumeV2.NewGetBlockVolumeByIdRequest(pvolumeId))
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
	vol, sdkErr := s.client.VServerGateway().V2().VolumeService().
		GetBlockVolumeById(lsdkVolumeV2.NewGetBlockVolumeByIdRequest(pvolumeId))
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
	if sdkErr = s.client.VServerGateway().V2().ComputeService().
		DetachBlockVolume(lsdkComputeV2.NewDetachBlockVolumeRequest(pinstanceId, pvolumeId)); sdkErr != nil {
		switch {
		case errSetDetachDone.ContainsOne(sdkErr.GetErrorCode()):
			llog.InfoS("[INFO] - DetachVolume: Nothing left to detach", "volumeId", pvolumeId,
				"instanceId", pinstanceId, "errorCode", sdkErr.GetStringErrorCode())

			return nil
		case errSetDetachRetryable.ContainsOne(sdkErr.GetErrorCode()):
			llog.InfoS("[INFO] - DetachVolume: The volume is busy, waiting", "volumeId", pvolumeId,
				"errorCode", sdkErr.GetStringErrorCode())
		default:
			ierr := lserr.ErrVolumeFailedToDetach(pinstanceId, pvolumeId, sdkErr)
			llog.ErrorS(ierr.GetError(), "[ERROR] - DetachVolume: Failed to detach the volume", ierr.GetListParameters()...)

			return ierr
		}
	}

	return s.waitVolumeDetached(pctx, pinstanceId, pvolumeId)
}

func (s *cloud) ResizeOrModifyDisk(pctx lctx.Context, volumeID string, newSizeBytes int64, options *ModifyDiskOptions) (newSize int64, err error) {
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
	needsModification, volumeSize, err := s.validateModifyVolume(pctx, volumeID, newSizeGiB, options)
	if err != nil || !needsModification {
		return volumeSize, err
	}

	// The volume types are different => so please check the zone a same
	same, sdkErr2 := s.checkSameZone(options.VolumeType, volume.VolumeTypeID)
	if sdkErr2 != nil {
		return 0, sdkErr2.GetError()
	} else if !same && !volume.IsAttched() {
		// The target volume type lives in another zone => migrate before resizing.
		if ierr := s.migrateVolumeToType(pctx, volumeID, options.VolumeType); ierr != nil {
			return 0, ierr.GetError()
		}
	}

	opt := lsdkVolumeV2.NewResizeBlockVolumeByIdRequest(volumeID, options.VolumeType, int(newSizeGiB))
	_, sdkErr = s.client.VServerGateway().V2().VolumeService().ResizeBlockVolumeById(opt)
	if sdkErr != nil && !sdkErr.IsError(lsdkErrs.EcVServerVolumeUnchanged) {
		return 0, sdkErr.GetError()
	}

	_, err = s.waitVolumeAchieveStatus(pctx, volumeID, volumeArchivedStatus)
	if err != nil {
		return 0, err
	}

	// Perform one final check on the volume
	return s.checkDesiredState(volumeID, newSizeGiB, options)
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
func (s *cloud) migrateVolumeToType(pctx lctx.Context, pvolumeId, ptargetType string) lserr.IError {
	raw, sdkErr := s.getVolumeById(pvolumeId)
	if sdkErr != nil {
		return lserr.ErrVolumeFailedToGet(pvolumeId, sdkErr)
	}

	vol := lsentity.NewVolume(raw)
	if isMigratedToType(vol, ptargetType) {
		llog.InfoS("[INFO] - migrateVolumeToType: The volume is already on the target type",
			"volumeId", pvolumeId, "volumeType", ptargetType)

		return nil
	}

	if !vol.IsMigration() && !vol.IsCreating() {
		llog.InfoS("[INFO] - migrateVolumeToType: Migrating the volume",
			"volumeId", pvolumeId, "volumeType", ptargetType)

		if migErr := s.client.VServerGateway().V2().VolumeService().
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
	opts := lsdkVolumeV2.NewResizeBlockVolumeByIdRequest(pvolumeId, pvolumeType, psize)

	if _, sdkErr := s.client.VServerGateway().V2().VolumeService().ResizeBlockVolumeById(opts); sdkErr != nil {
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
	opts := lsdkVolumeV2.NewGetBlockVolumeByIdRequest(pvolID)
	vol, err := s.client.VServerGateway().V2().VolumeService().GetUnderBlockVolumeId(opts)
	if err != nil {
		llog.ErrorS(err.GetError(), "[ERROR] - GetDeviceDiskID: Failed to get the device disk ID", err.GetListParameters()...)
		return "", err.GetError()
	}

	return vol.UnderId, nil
}

func (s *cloud) GetVolumeSnapshotByName(pvolID, psnapshotName string) (*lsentity.Snapshot, error) {
	opt := lsdkVolumeV2.NewListSnapshotsByBlockVolumeIdRequest(1, 10, pvolID)
	res, err := s.client.VServerGateway().V2().VolumeService().ListSnapshotsByBlockVolumeId(opt)
	if err != nil {
		return nil, err.GetError()
	}

	for _, snap := range res.Items {
		if snap.VolumeId == pvolID && snap.Name == psnapshotName {
			return &lsentity.Snapshot{Snapshot: snap}, nil
		}
	}

	return nil, ErrSnapshotNotFound
}

func (s *cloud) CreateSnapshotFromVolume(pclusterId, pvolId, psnapshotName string) (*lsentity.Snapshot, error) {
	opt := lsdkVolumeV2.NewCreateSnapshotByBlockVolumeIdRequest(psnapshotName, pvolId).
		WithPermanently(true).
		WithDescription(lfmt.Sprintf(patternSnapshotDescription, pvolId, pclusterId))

	snapshot, sdkErr := s.client.VServerGateway().V2().VolumeService().CreateSnapshotByBlockVolumeId(opt)
	if sdkErr != nil {
		return nil, sdkErr.GetError()
	}

	err := s.waitSnapshotActive(pvolId, snapshot.Name)
	return &lsentity.Snapshot{Snapshot: snapshot}, err
}

func (s *cloud) DeleteSnapshot(psnapshotID string) error {
	opt := lsdkVolumeV2.NewDeleteSnapshotByIdRequest(psnapshotID)
	sdkErr := s.client.VServerGateway().V2().VolumeService().DeleteSnapshotById(opt)
	if sdkErr != nil {
		if !sdkErr.IsError(lsdkErrs.EcVServerSnapshotNotFound) {
			return sdkErr.GetError()
		}
	}
	return nil
}

func (s *cloud) ListSnapshots(pvolID string, ppage int, ppageSize int) (*lsentity.ListSnapshots, lserr.IError) {
	opt := lsdkVolumeV2.NewListSnapshotsByBlockVolumeIdRequest(ppage, ppageSize, pvolID)
	res, sdkErr := s.client.VServerGateway().V2().VolumeService().ListSnapshotsByBlockVolumeId(opt)
	if sdkErr != nil {
		return nil, lserr.NewError(sdkErr)
	}

	return &lsentity.ListSnapshots{ListSnapshots: res}, nil
}

func (s *cloud) GetVolumeTypeById(pvolTypeId string) (*lsentity.VolumeType, lserr.IError) {
	opt := lsdkVolumeV1.NewGetVolumeTypeByIdRequest(pvolTypeId)
	volType, err := s.client.VServerGateway().V1().VolumeService().GetVolumeTypeById(opt)
	if err != nil {
		return nil, lserr.NewError(err)
	}

	return &lsentity.VolumeType{VolumeType: volType}, nil
}

func (s *cloud) GetDefaultVolumeType() (*lsentity.VolumeType, lserr.IError) {
	volType, err := s.client.VServerGateway().V1().VolumeService().GetDefaultVolumeType()
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

	req := lsdkVolumeV1.NewGetVolumeTypeZonesRequest(zoneId)
	res, sdkErr := s.client.VServerGateway().V1().VolumeService().GetVolumeTypeZones(req)
	if sdkErr != nil {
		return "", lserr.NewError(sdkErr)
	}

	for _, vtZone := range res.VolumeTypeZones {
		if vtZone == nil || vtZone.Name != volTypeName {
			continue
		}

		listReq := lsdkVolumeV1.NewListVolumeTypeRequest(vtZone.Id)
		listRes, sdkErr := s.client.VServerGateway().V1().VolumeService().GetListVolumeTypes(listReq)
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
	res, sdkErr := s.client.VServerGateway().V1().PortalService().ListZones()
	if sdkErr != nil {
		return nil, lserr.NewError(sdkErr)
	}

	return &lsentity.ListZones{ListZones: res}, nil
}
