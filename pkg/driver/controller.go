package driver

import (
	lctx "context"
	lerr "errors"
	lfmt "fmt"
	lstrconv "strconv"
	lstr "strings"
	ltime "time"

	lcsi "github.com/container-storage-interface/spec/lib/go/csi"
	ljoat "github.com/cuongpiger/joat/parser"
	lvmrpc "github.com/vngcloud/vngcloud-csi-volume-modifier/pkg/rpc"
	lsdkEntity "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/entity"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
	lts "google.golang.org/protobuf/types/known/timestamppb"
	lcoreV1 "k8s.io/api/core/v1"
	lk8srecord "k8s.io/client-go/tools/record"
	llog "k8s.io/klog/v2"

	lscloud "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud"
	lsentity "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/entity"
	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
	lsinternal "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/driver/internal"
	lsk8s "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/k8s"
	lsmetrics "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/metrics"
	lsutil "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/util"
)

type controllerService struct {
	cloud               lscloud.Cloud
	inFlight            *lsinternal.InFlight
	createGate          *lsinternal.Semaphore
	detachBreaker       *lsinternal.Breaker
	modifyVolumeManager *modifyVolumeManager
	driverOptions       *DriverOptions
	k8sClient           lsk8s.IKubernetes
	broadcaster         lk8srecord.EventBroadcaster

	lvmrpc.UnimplementedModifyServer
}

// newControllerService creates a new controller service it panics if failed to create the service
func newControllerService(pdriOpts *DriverOptions) controllerService {
	metadata, err := NewMetadataFunc(lscloud.DefaultVServerMetadataClient)
	if err != nil {
		llog.ErrorS(err, "[ERROR] - newControllerService: Could not determine the metadata information for the driver")
		panic(err)
	}

	cloudSrv, err := NewCloudFunc(pdriOpts.identityURL, pdriOpts.vServerURL, pdriOpts.clientID, pdriOpts.clientSecret, metadata)
	if err != nil {
		panic(err)
	}

	// Create Kubernetes client
	k8sClient, err := lscloud.DefaultKubernetesAPIClient()
	if err != nil {
		llog.ErrorS(err, "[ERROR] - newControllerService: Failed to create Kubernetes client")
		panic(err)
	}

	// Create event braodcaster and recorder
	broadcaster, recorder, err := lscloud.DefaultEventRecorder(k8sClient)
	if err != nil {
		llog.ErrorS(err, "[ERROR] - newControllerService: Failed to create event recorder")
		panic(err)
	}

	return controllerService{
		cloud:               cloudSrv,
		inFlight:            lsinternal.NewInFlight(),
		createGate:          lsinternal.NewSemaphore(pdriOpts.maxConcurrentVolumeCreates),
		detachBreaker:       lsinternal.NewBreaker(),
		driverOptions:       pdriOpts,
		modifyVolumeManager: newModifyVolumeManager(),
		k8sClient:           lsk8s.NewKubernetes(k8sClient, recorder),
		broadcaster:         broadcaster,
	}
}

func (s *controllerService) CreateVolume(pctx lctx.Context, preq *lcsi.CreateVolumeRequest) (*lcsi.CreateVolumeResponse, error) {
	var (
		serr lserr.IError
	)

	llog.V(5).InfoS("[INFO] - CreateVolume: Called", "request", *preq)

	// Validate the create volume request
	if err := validateCreateVolumeRequest(preq); err != nil {
		llog.ErrorS(err, "[ERROR] - CreateVolume: Invalid request", "request", *preq)
		ns, name := getCreateVolumeRequestNamespacedName(preq)
		s.k8sClient.PersistentVolumeClaimEventWarning(pctx, ns, name, "CsiCreateVolumeInvalidRequest", err.Error())
		return nil, err
	}

	volName := preq.GetName()              // get the name of the volume, always in the format of pvc-<random-uuid>
	volCap := preq.GetVolumeCapabilities() // get volume capabilities
	multiAttach := isMultiAttach(volCap)   // check if the volume is multi-attach, true if multi-attach, false otherwise
	listZones, serr := s.cloud.GetListZones()
	if serr != nil {
		llog.ErrorS(serr.GetError(), "[ERROR] - CreateVolume: Failed to list availability zones")
		s.reportCreateIaaSError(pctx, preq, serr)
		return nil, serr.GetError()
	}
	availabilityZone := pickAvailabilityZone(preq.GetAccessibilityRequirements())
	portalZone := availabilityZone
	for _, az := range listZones.Items {
		if az.OpenstackZone == availabilityZone {
			portalZone = az.Uuid
			break
		}
	}

	llog.V(5).InfoS("[INFO] - CreateVolume: availability zone", "portalZone", portalZone)

	// Validate volume size, if volume size is less than the default volume size of cloud provider, set it to the default volume size
	volumeTypeId, volSizeBytes, err := s.getVolSizeBytes(pctx, portalZone, preq)
	if err != nil {
		llog.ErrorS(err, "[ERROR] - CreateVolume: Failed to get volume size")
		return nil, ErrFailedToValidateVolumeSize(preq.GetName(), err)
	}

	// check if a request is already in-flight
	if ok := s.inFlight.Insert(volName); !ok {
		llog.InfoS("[INFO] - CreateVolume: Operation is already in-flight", "volumeName", volName, "inflightKey", volName)
		return nil, ErrVolumeIsCreating(volName)
	}

	llog.InfoS("[INFO] - CreateVolume: Insert this action to inflight cache", "volumeName", volName, "inflightKey", volName)
	defer func() {
		llog.InfoS("[INFO] - CreateVolume: Operation completed", "volumeName", volName, "inflightKey", volName)
		s.inFlight.Delete(volName)
	}()

	// Cap concurrent creates against vServer (see internal.Semaphore for the
	// measurements). Waiting is deliberate - a parked goroutine beats retry
	// churn - and ends with pctx: the sidecar's own timeout cancels it, the CO
	// retries, and the duplicate is cheaply rejected by the inflight cache
	// above while this handler still holds the name.
	//
	// The two gate series exist because the cap is a compile-time constant
	// backed by a single experiment: without them there is no way to tell a
	// cap that is throttling real work from one that is never reached.
	lsmetrics.Recorder().AddGauge(lsmetrics.CreateGateWaiting, lsmetrics.CreateGateWaitingHelp, 1, nil)
	gateStart := ltime.Now()

	gateErr := s.createGate.Acquire(pctx)

	lsmetrics.Recorder().AddGauge(lsmetrics.CreateGateWaiting, lsmetrics.CreateGateWaitingHelp, -1, nil)
	// Observed on both paths: a wait that ended in cancellation is exactly the
	// case worth seeing, and dropping it would bias the histogram towards the
	// waits that succeeded.
	lsmetrics.Recorder().ObserveHistogram(
		lsmetrics.CreateGateWaitSeconds, lsmetrics.CreateGateWaitSecondsHelp,
		ltime.Since(gateStart).Seconds(), nil, lsmetrics.CreateGateWaitBuckets,
	)

	if gateErr != nil {
		llog.InfoS("[INFO] - CreateVolume: Context ended while waiting for a create slot", "volumeName", volName)
		return nil, ErrWaitingForCreateSlot(volName)
	}
	defer s.createGate.Release()

	if _, serr = s.cloud.GetVolumeByName(volName); serr != nil {
		// EcVServerVolumeNotFound is the answer this call is looking for, not
		// a failure, so only the other codes are reported.
		if !serr.IsError(lsdkErrs.EcVServerVolumeNotFound) {
			llog.ErrorS(serr.GetError(), "[ERROR] - CreateVolume: Failed to get volume", "volumeName", volName)
			s.reportCreateIaaSError(pctx, preq, serr)
			return nil, ErrFailedToListVolumeByName(volName)
		}
	}

	cvr := NewCreateVolumeRequest().WithDriverOptions(s.driverOptions).WithZone(portalZone).WithVolumeTypeID(volumeTypeId)
	parser, _ := ljoat.GetParser()
	for pk, pv := range preq.GetParameters() {
		llog.InfoS("[INFO] - CreateVolume: Parsing request parameters", "key", pk, "value", pv)
		switch lstr.ToLower(pk) {
		case EncryptedKey:
			cvr = cvr.WithEncrypted(pv)
		case PVCNameKey:
			cvr = cvr.WithPvcNameTag(pv)
		case PVCNamespaceKey:
			cvr = cvr.WithPvcNamespaceTag(pv)
		case BlockSizeKey:
			if isAlphanumeric := parser.StringIsAlphanumeric(pv); !isAlphanumeric {
				return nil, ErrCanNotParseRequestArguments(BlockSizeKey, pv)
			}
			cvr = cvr.WithBlockSize(pv)
		case InodeSizeKey:
			if isAlphanumeric := parser.StringIsAlphanumeric(pv); !isAlphanumeric {
				return nil, ErrCanNotParseRequestArguments(InodeSizeKey, pv)
			}
			cvr = cvr.WithInodeSize(pv)
		case BytesPerInodeKey:
			if isAlphanumeric := parser.StringIsAlphanumeric(pv); !isAlphanumeric {
				return nil, ErrCanNotParseRequestArguments(BytesPerInodeKey, pv)
			}
			cvr = cvr.WithBytesPerInode(pv)
		case NumberOfInodesKey:
			if isAlphanumeric := parser.StringIsAlphanumeric(pv); !isAlphanumeric {
				return nil, ErrCanNotParseRequestArguments(NumberOfInodesKey, pv)
			}
			cvr = cvr.WithNumberOfInodes(pv)
		case Ext4ClusterSizeKey:
			if isAlphanumeric := parser.StringIsAlphanumeric(pv); !isAlphanumeric {
				return nil, ErrCanNotParseRequestArguments(Ext4ClusterSizeKey, pv)
			}
			cvr = cvr.WithExt4ClusterSize(pv)
		case Ext4BigAllocKey:
			cvr = cvr.WithExt4BigAlloc(pv == "true")
		case IsPoc:
			cvr = cvr.WithPoc(pv == "true")
		}
	}

	modifyOpts, _ := parseModifyVolumeParameters(preq.GetMutableParameters())
	volumeSource := preq.GetVolumeContentSource()
	if volumeSource != nil {
		if _, ok := volumeSource.GetType().(*lcsi.VolumeContentSource_Snapshot); !ok {
			llog.ErrorS(nil, "[ERROR] - CreateVolume: VolumeContentSource not supported", "volumeID", volName)
			s.k8sClient.PersistentVolumeClaimEventWarning(pctx, cvr.PvcNamespaceTag, cvr.PvcNameTag,
				"CsiVolumeContentSourceNotSupported", "VolumeContentSource_Snapshot not supported")
			return nil, ErrVolumeContentSourceNotSupported
		}
		sourceSnapshot := volumeSource.GetSnapshot()
		if sourceSnapshot == nil {
			llog.ErrorS(nil, "[ERROR] - CreateVolume: Snapshot is nil within volumeContentSource", "volumeID", volName)
			s.k8sClient.PersistentVolumeClaimEventWarning(pctx, cvr.PvcNamespaceTag, cvr.PvcNameTag,
				"CsiSnapshotNotFound", "Snapshot is nil within volumeContentSource")
			return nil, ErrSnapshotIsNil
		}
		cvr = cvr.WithSnapshotID(sourceSnapshot.GetSnapshotId())
	}

	respCtx, err := cvr.ToResponseContext(volCap)
	if err != nil {
		llog.ErrorS(err, "[ERROR] - CreateVolume: Failed to parse response context", "volumeID", volName)
		s.k8sClient.PersistentVolumeClaimEventWarning(pctx, cvr.PvcNamespaceTag, cvr.PvcNameTag,
			"CsiCreateVolumeRequestInvalid", err.Error())
		return nil, err
	}

	cvr = cvr.WithVolumeName(volName).
		WithMultiAttach(multiAttach).
		WithVolumeSize(uint64(lsutil.RoundUpSize(volSizeBytes, 1024*1024*1024))).
		WithVolumeTypeID(modifyOpts.VolumeType).
		WithClusterID(s.getClusterID())

	// Get the proper PVC from the API server
	pvc, ierr := s.k8sClient.GetPersistentVolumeClaimByName(pctx, cvr.PvcNamespaceTag, cvr.PvcNameTag)
	if ierr != nil {
		llog.ErrorS(ierr.GetError(), "[ERROR] - CreateVolume: Failed to get PVC", "pvcName", cvr.PvcNameTag, "pvcNamespace", cvr.PvcNamespaceTag)
		s.k8sClient.PersistentVolumeClaimEventWarning(pctx, cvr.PvcNamespaceTag, cvr.PvcNameTag,
			"CsiGetPersistentVolumeClaimFailure", ierr.GetMessage())
		return nil, ierr.GetError()
	}

	cvr = cvr.WithVolumeTypeID(pvc.GetCsiVolumeTypeAnnotation())

	// Check if the PVC annotations include the encrypted key
	if pvc.GetCsiEncryptedAnnotation() != "" {
		cvr = cvr.WithEncrypted(pvc.GetCsiEncryptedAnnotation())
	}

	newVol, sdkErr := s.cloud.EitherCreateResizeVolume(cvr.ToSdkCreateVolumeRequest())
	if sdkErr != nil {
		llog.ErrorS(sdkErr.GetError(), "[ERROR] - CreateVolume: failed to create volume", "errMsg", sdkErr.GetErrorMessages())
		s.reportCreateIaaSError(pctx, preq, sdkErr)
		return nil, sdkErr.GetError()
	}

	s.k8sClient.PersistentVolumeClaimEventNormal(pctx, cvr.PvcNamespaceTag, cvr.PvcNameTag,
		"CsiCreateVolumeSuccess", lfmt.Sprintf("Volume created successfully with ID %s for PersistentVolume %s", newVol.Id, newVol.Name))
	return newCreateVolumeResponse(newVol, availabilityZone, cvr, respCtx), nil
}

// reportCreateIaaSError publishes the two operator-facing halves of one IaaS
// failure on the create path: the op="create" series of lsmetrics.IaaSErrors and a
// Warning on the PVC named after the classified reason.
//
// A specific reason is what makes the event actionable: "quota exhausted"
// needs a human, "throttled" resolves itself.
//
// The PVC coordinates come from the request parameters, not from cvr, because
// most of the create path's IaaS calls happen before cvr is built. Both carry
// the same two values - cvr copies them out of these parameters.
//
// Observability never fails the caller: this returns nothing, and the event
// and metric sinks each swallow their own errors.
func (s *controllerService) reportCreateIaaSError(pctx lctx.Context, preq *lcsi.CreateVolumeRequest, pierr lserr.IError) {
	if pierr == nil {
		return
	}

	cls := lscloud.Classify(pierr)
	lsmetrics.Recorder().IncreaseCount(lsmetrics.IaaSErrors, lsmetrics.IaaSErrorsHelp, map[string]string{
		"op": "create", "reason": cls.Reason,
	})

	ns, name := getCreateVolumeRequestNamespacedName(preq)
	s.k8sClient.PersistentVolumeClaimEventWarning(pctx, ns, name, cls.Reason, pierr.GetMessage())
}

// pickAvailabilityZone selects 1 zone given topology requirement.
// if not found, empty string is returned.
func pickAvailabilityZone(requirement *lcsi.TopologyRequirement) string {
	if requirement == nil {
		return ""
	}
	for _, topology := range requirement.GetPreferred() {
		zone, exists := topology.GetSegments()[WellKnownZoneTopologyKey]
		if exists {
			return zone
		}

		zone, exists = topology.GetSegments()[ZoneTopologyKey]
		if exists {
			return zone
		}
	}
	for _, topology := range requirement.GetRequisite() {
		zone, exists := topology.GetSegments()[WellKnownZoneTopologyKey]
		if exists {
			return zone
		}
		zone, exists = topology.GetSegments()[ZoneTopologyKey]
		if exists {
			return zone
		}
	}
	return ""
}

func (s *controllerService) DeleteVolume(pctx lctx.Context, preq *lcsi.DeleteVolumeRequest) (*lcsi.DeleteVolumeResponse, error) {
	llog.InfoS("[INFO] - DeleteVolume: called", "request", *preq)

	if err := validateDeleteVolumeRequest(preq); err != nil {
		llog.ErrorS(err, "[ERROR] - DeleteVolume: Invalid request", "request", *preq)
		return nil, err
	}

	volumeID := preq.GetVolumeId()
	// check if a request is already in-flight
	if ok := s.inFlight.Insert(volumeID); !ok {
		llog.InfoS("[INFO] - DeleteVolume: Operation is already in-flight", "volumeID", volumeID)
		return nil, ErrOperationAlreadyExists(volumeID)
	}

	llog.InfoS("[INFO] - DeleteVolume: Insert this action to inflight cache", "volumeID", volumeID, "inflightKey", volumeID)
	defer func() {
		llog.InfoS("[INFO] - DeleteVolume: Operation completed", "volumeID", volumeID, "inflightKey", volumeID)
		s.inFlight.Delete(volumeID)
	}()

	// So the volume MUST NOT truly be deleted if it has at least one snapshot
	lstSnapshots, ierr := s.cloud.ListSnapshots(volumeID, 1, 10)
	if ierr != nil {
		llog.ErrorS(ierr.GetError(), "[ERROR] - DeleteVolume: Failed to list snapshots", "volumeId", volumeID)
		return nil, ErrFailedToListSnapshot(volumeID)
	}

	if !lstSnapshots.IsEmpty() {
		llog.ErrorS(nil, "[ERROR] - DeleteVolume: CANNOT delete this volume because of having snapshots", "volumeId", volumeID)
		return nil, ErrDeleteVolumeHavingSnapshots(volumeID)
	}

	if err := s.cloud.DeleteVolume(pctx, volumeID); err != nil {
		if err != nil {
			llog.ErrorS(err.GetError(), "[ERROR] - DeleteVolume: Failed to delete volume", "volumeID", volumeID)
			return nil, ErrFailedToDeleteVolume(volumeID)
		}
	}

	return &lcsi.DeleteVolumeResponse{}, nil
}

func (s *controllerService) ControllerPublishVolume(pctx lctx.Context, preq *lcsi.ControllerPublishVolumeRequest) (result *lcsi.ControllerPublishVolumeResponse, err error) {
	llog.V(5).InfoS("[INFO] - ControllerPublishVolume: Called", "request", *preq)

	if err = validateControllerPublishVolumeRequest(preq); err != nil {
		llog.ErrorS(err, "[ERROR] - ControllerPublishVolume: Invalid request")
		return nil, err
	}

	volumeID := preq.GetVolumeId() // get the cloud volume ID
	nodeID := preq.GetNodeId()     // get the cloud node ID
	key := volumeID + nodeID

	// Make sure there are no 2 operations on the same volume and node at the same time
	if !s.inFlight.Insert(volumeID + nodeID) {
		llog.InfoS("[INFO] - ControllerPublishVolume: Operation is already in-flight", "volumeID", volumeID, "nodeID", nodeID, "inflightKey", key)
		return nil, ErrOperationAlreadyExists(volumeID)
	}

	llog.V(5).InfoS("[INFO] - ControllerPublishVolume: Insert this action to inflight cache", "volumeID", volumeID, "nodeID", nodeID, "inflightKey", key)
	defer func() {
		llog.InfoS("[INFO] - ControllerPublishVolume: Operation completed", "volumeID", volumeID, "nodeID", nodeID, "inflightKey", key)
		s.inFlight.Delete(volumeID + nodeID)
	}()

	llog.InfoS("[INFO] - ControllerPublishVolume: attaching volume into the instance", "volumeID", volumeID, "nodeID", nodeID)

	// Attach the volume and wait for it to be attached
	_, ierr := s.cloud.AttachVolume(pctx, nodeID, volumeID)
	if ierr != nil {
		llog.ErrorS(ierr.GetError(), "[ERROR] - ControllerPublishVolume; failed to attach volume to instance", "volumeID", volumeID, "nodeID", nodeID)
		s.reportAttachIaaSError(pctx, volumeID, nodeID, ierr)

		return nil, ErrAttachVolume(volumeID, nodeID)
	}

	devicePath, err := s.cloud.GetDeviceDiskID(volumeID)
	if err != nil {
		llog.ErrorS(err, "[ERROR] - ControllerPublishVolume; failed to get device path for volume", "volumeID", volumeID)
		return nil, ErrFailedToGetDevicePath(volumeID, nodeID)
	}

	llog.V(5).InfoS("[INFO] - ControllerPublishVolume; volume attached to instance successfully", "volumeID", volumeID, "nodeID", nodeID)
	s.clearDetachStateOnAttach(lsinternal.BreakerKey{VolumeID: volumeID, NodeID: nodeID}, ltime.Now())

	return newControllerPublishVolumeResponse(devicePath), nil
}

func (s *controllerService) ControllerUnpublishVolume(pctx lctx.Context, preq *lcsi.ControllerUnpublishVolumeRequest) (*lcsi.ControllerUnpublishVolumeResponse, error) {
	llog.InfoS("[INFO] - ControllerUnpublishVolume: Called", "request", *preq)

	if err := validateControllerUnpublishVolumeRequest(preq); err != nil {
		llog.ErrorS(err, "[ERROR] - ControllerUnpublishVolume: Invalid request", "request", *preq)
		return nil, err
	}

	volumeID := preq.GetVolumeId()
	nodeID := preq.GetNodeId()
	key := volumeID + nodeID
	bkey := lsinternal.BreakerKey{VolumeID: volumeID, NodeID: nodeID}

	if !s.inFlight.Insert(key) {
		llog.InfoS("[INFO] - ControllerUnpublishVolume: Operation is already in-flight", "volumeID", volumeID, "nodeID", nodeID, "inflightKey", key)
		return nil, ErrOperationAlreadyExists(volumeID)
	}

	llog.V(5).InfoS("[INFO] - ControllerUnpublishVolume: Insert this action to inflight cache", "volumeID", volumeID, "nodeID", nodeID, "inflightKey", key)
	defer func() {
		llog.InfoS("[INFO] - ControllerUnpublishVolume: Operation completed", "volumeID", volumeID, "nodeID", nodeID, "inflightKey", key)
		s.inFlight.Delete(volumeID + nodeID)
	}()

	now := ltime.Now()
	s.evictStaleDetachState(now)

	// While the breaker is open we stop commanding the IaaS and only read
	// state. A stuck detach used to cost ~13 vServer calls every 6 minutes for
	// as long as it stayed stuck (23 hours, ~3,000 calls, in the incident this
	// guards against), all from a quota bucket shared across the project.
	if s.detachBreaker.Allow(bkey, now) == lsinternal.ProbeOnly {
		detached, ierr := s.cloud.IsDetachedFrom(pctx, nodeID, volumeID)
		if ierr == nil && detached {
			s.onDetachSucceeded(pctx, volumeID, nodeID, bkey, now)

			return &lcsi.ControllerUnpublishVolumeResponse{}, nil
		}

		// A failed probe says nothing about whether the IaaS accepts commands,
		// so it must NOT advance the backoff. But a probe error and a genuine
		// "still attached" read must not look the same to an operator: the
		// former means the driver has lost the ability to observe this pair,
		// which is itself worth knowing.
		if ierr != nil {
			llog.InfoS("[INFO] - ControllerUnpublishVolume: detach paused by breaker, could not confirm state",
				"volumeID", volumeID, "nodeID", nodeID, "error", ierr.GetError())
		} else {
			llog.InfoS("[INFO] - ControllerUnpublishVolume: detach paused by breaker, still attached",
				"volumeID", volumeID, "nodeID", nodeID)
		}

		return nil, ErrDetachVolumePaused(volumeID, nodeID)
	}

	if ierr := s.cloud.DetachVolume(pctx, nodeID, volumeID); ierr != nil {
		llog.ErrorS(ierr.GetError(), "[ERROR] - ControllerUnpublishVolume: Failed to detach volume from instance", "volumeID", volumeID, "nodeID", nodeID)
		s.onDetachFailed(pctx, volumeID, nodeID, bkey, now, ierr)

		return nil, ErrDetachVolume(volumeID, nodeID)
	}

	s.onDetachSucceeded(pctx, volumeID, nodeID, bkey, now)
	llog.InfoS("[INFO] - ControllerUnpublishVolume: Volume detached from instance successfully", "volumeID", volumeID, "nodeID", nodeID)

	return &lcsi.ControllerUnpublishVolumeResponse{}, nil
}

// evictStaleDetachState drops every breaker entry no traffic has touched for
// twice the capped step, and clears the gauge series that went with them.
//
// A pair whose backoff has gone untouched that long is either gone or was
// unstuck by something other than a successful ControllerUnpublishVolume (a
// force-deleted VolumeAttachment, a garbage-collected Machine, a deleted
// volume). Left behind, its gauge series would sit frozen at its last value
// forever and the "stuck > 30m" alert this feature exists to raise would fire
// permanently on a healthy cluster - the alert silencing itself.
func (s *controllerService) evictStaleDetachState(pnow ltime.Time) {
	for _, stale := range s.detachBreaker.EvictStale(pnow) {
		lsmetrics.Recorder().DeleteGauge(lsmetrics.DetachPendingSeconds, map[string]string{
			"volume_id": stale.VolumeID, "node_id": stale.NodeID,
		})
	}
}

// clearDetachStateOnAttach voids this pair's detach state after a successful
// attach, and sweeps everything else that has gone stale while it was at it.
//
// A fresh successful attach proves any prior detach state void: whatever the
// breaker still believed about this pair, the volume is demonstrably attached
// now, so the next detach must get a real attempt rather than starting inside
// a leftover pause of up to two hours - a pause its probes could never clear,
// because the volume genuinely IS attached again.
//
// Running the sweep here too matters because ControllerUnpublishVolume was the
// only thing driving it, which makes the leak's own trigger condition (this
// pair stops receiving unpublish calls) also the thing that stops the sweep.
//
// This is NOT an attach-path breaker: the spec forbids gating attach, and
// nothing here can block or delay one. It only clears state. Both handlers
// take the same volumeID+nodeID inflight key, so an attach and a detach for
// one pair cannot interleave and this cannot race the detach bookkeeping.
func (s *controllerService) clearDetachStateOnAttach(pkey lsinternal.BreakerKey, pnow ltime.Time) {
	s.evictStaleDetachState(pnow)

	s.detachBreaker.Success(pkey)
	lsmetrics.Recorder().DeleteGauge(lsmetrics.DetachPendingSeconds, map[string]string{
		"volume_id": pkey.VolumeID, "node_id": pkey.NodeID,
	})
}

// onDetachFailed records a real failed attempt, and reports it once per
// escalation rather than once per retry - the incident this guards against
// would have produced 5 events instead of ~230.
func (s *controllerService) onDetachFailed(
	pctx lctx.Context, pvolumeID, pnodeID string, pkey lsinternal.BreakerKey, pnow ltime.Time, pierr lserr.IError,
) {
	cls := lscloud.Classify(pierr)
	tripped, stepped := s.detachBreaker.Failure(pkey, cls.Terminal, pnow)
	stuck, _, isTripped := s.detachBreaker.Since(pkey, pnow)

	// Unconditional: iaas_errors_total is the series that covers failures the
	// breaker has not opened on yet.
	lsmetrics.Recorder().IncreaseCount(lsmetrics.IaaSErrors, lsmetrics.IaaSErrorsHelp, map[string]string{
		"op": "detach", "reason": cls.Reason,
	})

	// The gauge is only meaningful for a pair the breaker has actually opened
	// on. Publishing a series born at 0s for every transient failure is churn
	// with no signal, and every such series then has to be evicted again.
	// isTripped covers all three cases at once - this failure tripped it, this
	// failure stepped it, or it was already open - because Failure has already
	// updated the entry by the time Since reads it.
	if isTripped {
		lsmetrics.Recorder().SetGauge(lsmetrics.DetachPendingSeconds, lsmetrics.DetachPendingSecondsHelp, stuck.Seconds(), map[string]string{
			"volume_id": pvolumeID, "node_id": pnodeID,
		})
	}

	if !tripped && !stepped {
		return
	}

	lsmetrics.Recorder().IncreaseCount(lsmetrics.DetachBreakerTrips, lsmetrics.DetachBreakerTripsHelp, map[string]string{
		"reason": cls.Reason,
	})

	msg := lfmt.Sprintf(
		"Detach %s from %s keeps failing (%s, stuck for %s). Pausing IaaS detach calls; state will still be probed on each retry.",
		pvolumeID, pnodeID, cls.Reason, stuck.Round(ltime.Second),
	)
	s.emitVolumeEvent(pctx, pvolumeID, lcoreV1.EventTypeWarning, "VolumeDetachStalled", msg)
}

// onDetachSucceeded clears the breaker, drops the gauge series so nothing
// keeps alerting, and says so out loud if the pair had been stuck.
func (s *controllerService) onDetachSucceeded(
	pctx lctx.Context, pvolumeID, pnodeID string, pkey lsinternal.BreakerKey, pnow ltime.Time,
) {
	stuck, _, wasTripped := s.detachBreaker.Since(pkey, pnow)
	s.detachBreaker.Success(pkey)

	// Unconditional: deleting an absent series is a cheap no-op, and it is the
	// one call that must not be skipped by mistake.
	lsmetrics.Recorder().DeleteGauge(lsmetrics.DetachPendingSeconds, map[string]string{
		"volume_id": pvolumeID, "node_id": pnodeID,
	})

	// Only a pair that actually tripped gets a recovery event. A pair that
	// merely had one transient failure was never reported as stuck, so there
	// is nothing to report as recovered - and each event costs a full
	// unpaginated PersistentVolumes().List() (see
	// FindPersistentVolumeByHandle), which must stay a per-stuck-volume cost,
	// never a per-detach one.
	if !wasTripped {
		return
	}

	msg := lfmt.Sprintf("Detach %s from %s succeeded after being stuck for %s.",
		pvolumeID, pnodeID, stuck.Round(ltime.Second))
	s.emitVolumeEvent(pctx, pvolumeID, lcoreV1.EventTypeNormal, "VolumeDetachRecovered", msg)
}

// reportAttachIaaSError gives a failed attach a reason an operator can act on.
//
// F9, measured on the dev cluster: the driver reported CSINode
// allocatable.count = 10, the scheduler saw 8 in use and placed another pod,
// and the IaaS refused the attach because an orphaned volume already held the
// last slot. The pod sat Pending forever retrying FailedAttachVolume while the
// scheduler never reported "no capacity" - and the driver said nothing that
// distinguished a full node from any other attach failure.
//
// Classify already mapped EcVServerServerVolumeAttachQuotaExceeded to
// ReasonVolumeAttachQuotaExceeded; nothing on this path had ever called it.
//
// This deliberately does NOT pre-check capacity or change the gRPC code.
// Upstream aws-ebs-csi-driver does no pre-check either and relies on reporting
// the condition distinctly; and the code this path returns decides what
// external-attacher does next, which is not provable from source here because
// the attacher is not vendored. Reporting is additive and safe. Whether
// codes.ResourceExhausted is the better answer is a separate question that
// needs a live experiment, exactly like the Internal-vs-Aborted question on
// the detach path.
func (s *controllerService) reportAttachIaaSError(pctx lctx.Context, pvolumeID, pnodeID string, pierr lserr.IError) {
	if pierr == nil {
		return
	}

	cls := lscloud.Classify(pierr)
	lsmetrics.Recorder().IncreaseCount(lsmetrics.IaaSErrors, lsmetrics.IaaSErrorsHelp, map[string]string{
		"op": "attach", "reason": cls.Reason,
	})

	// The reason field already carries the classification. Repeating it in the
	// body made a Contains-based test pass even with a generic reason field.
	msg := lfmt.Sprintf("Attach %s to %s failed: %s", pvolumeID, pnodeID, pierr.GetMessage())
	s.emitVolumeEvent(pctx, pvolumeID, lcoreV1.EventTypeWarning, cls.Reason, msg)
}

// emitVolumeEvent resolves the IaaS volume ID to its PV and emits there (and on
// the PVC if it still exists). Every failure is swallowed: reporting a problem
// must never create one.
func (s *controllerService) emitVolumeEvent(pctx lctx.Context, pvolumeID, peventType, preason, pmessage string) {
	pv, ierr := s.k8sClient.FindPersistentVolumeByHandle(pctx, pvolumeID)
	if ierr != nil || pv == nil || pv.PersistentVolume == nil {
		llog.V(2).InfoS("[DEBUG] - emitVolumeEvent: no PV for this volume, skipping event",
			"volumeID", pvolumeID, "reason", preason)

		return
	}

	if peventType == lcoreV1.EventTypeNormal {
		s.k8sClient.VolumeEventNormal(pctx, pv.PersistentVolume.Name, preason, pmessage)

		return
	}
	s.k8sClient.VolumeEventWarning(pctx, pv.PersistentVolume.Name, preason, pmessage)
}

func (s *controllerService) CreateSnapshot(pctx lctx.Context, preq *lcsi.CreateSnapshotRequest) (*lcsi.CreateSnapshotResponse, error) {
	llog.V(4).InfoS("[INFO] - CreateSnapshot: called", "preq", *preq)
	if err := validateCreateSnapshotRequest(preq); err != nil {
		llog.ErrorS(err, "CreateSnapshot: invalid request")
		return nil, err
	}

	snapshotName := preq.GetName()
	volumeID := preq.GetSourceVolumeId()

	// check if a request is already in-flight
	if ok := s.inFlight.Insert(snapshotName); !ok {
		return nil, ErrOperationAlreadyExists(volumeID)
	}
	defer s.inFlight.Delete(snapshotName)

	snapshot, err := s.cloud.GetVolumeSnapshotByName(pctx, volumeID, snapshotName)
	if err != nil {
		if !lerr.Is(err, lscloud.ErrSnapshotNotFound) {
			llog.ErrorS(err, "Error looking for the snapshot", "snapshotName", snapshotName)
			return nil, err
		}
	}

	if snapshot != nil {
		return newCreateSnapshotResponse(snapshot)
	}

	snapshot, err = s.cloud.CreateSnapshotFromVolume(pctx, s.getClusterID(), volumeID, snapshotName)
	if err != nil {
		llog.ErrorS(err, "CreateSnapshot: Error creating snapshot", "snapshotName", snapshotName, "volumeID", volumeID)
		return nil, err
	}

	return newCreateSnapshotResponse(snapshot)
}

func (s *controllerService) DeleteSnapshot(_ lctx.Context, preq *lcsi.DeleteSnapshotRequest) (*lcsi.DeleteSnapshotResponse, error) {
	llog.V(4).InfoS("DeleteSnapshot: called", "preq", *preq)

	if err := validateDeleteSnapshotRequest(preq); err != nil {
		llog.ErrorS(err, "DeleteSnapshot: invalid request")
		return nil, err
	}

	snapshotID := preq.GetSnapshotId()

	// check if a request is already in-flight
	if ok := s.inFlight.Insert(snapshotID); !ok {
		return nil, ErrSnapshotIsDeleting(snapshotID)
	}
	defer s.inFlight.Delete(snapshotID)

	if err := s.cloud.DeleteSnapshot(snapshotID); err != nil {
		llog.ErrorS(err, "DeleteSnapshot: Error deleting snapshot", "snapshotID", snapshotID)
		return nil, ErrFailedToDeleteSnapshot(snapshotID)
	}

	llog.V(5).InfoS("DeleteSnapshot: snapshot deleted successfully", "snapshotID", snapshotID)
	return &lcsi.DeleteSnapshotResponse{}, nil
}

func (s *controllerService) ListSnapshots(_ lctx.Context, preq *lcsi.ListSnapshotsRequest) (*lcsi.ListSnapshotsResponse, error) {
	llog.V(4).InfoS("ListSnapshots: called", "preq", *preq)

	snapshotID := preq.GetSnapshotId()
	if snapshotID != "" {
		llog.InfoS("Seems some volumes need to use snapshot, ignoring...", "snapshotID", snapshotID)
		return newGetSnapshotsResponse(snapshotID), nil
	}

	volumeID := preq.GetSourceVolumeId()
	nextToken := parsePage(preq.GetStartingToken())
	maxEntries := int(preq.GetMaxEntries())

	cloudSnapshots, ierr := s.cloud.ListSnapshots(volumeID, nextToken, maxEntries)
	if ierr != nil {
		llog.ErrorS(ierr.GetError(), "ListSnapshots: Error listing snapshots", "volumeID", volumeID, "nextToken", nextToken, "maxEntries", maxEntries)
		return nil, ErrFailedToListSnapshot(volumeID)
	}

	response := newListSnapshotsResponse(cloudSnapshots)
	return response, nil
}

func (s *controllerService) ValidateVolumeCapabilities(pctx lctx.Context, preq *lcsi.ValidateVolumeCapabilitiesRequest) (*lcsi.ValidateVolumeCapabilitiesResponse, error) {
	llog.V(4).InfoS("ValidateVolumeCapabilities: called", "preq", *preq)

	volumeID := preq.GetVolumeId()
	if volumeID == "" {
		return nil, ErrVolumeIDNotProvided
	}

	volCaps := preq.GetVolumeCapabilities()
	if len(volCaps) < 1 {
		return nil, ErrVolumeCapabilitiesNotProvided
	}

	if _, err := s.cloud.GetVolume(volumeID); err != nil {
		if err.IsError(lsdkErrs.EcVServerVolumeNotFound) {
			return nil, ErrVolumeNotFound(volumeID)
		}

		return nil, ErrFailedToGetVolume(volumeID)
	}

	var confirmed *lcsi.ValidateVolumeCapabilitiesResponse_Confirmed
	if isValidVolumeCapabilities(volCaps) {
		confirmed = &lcsi.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: volCaps}
	}
	return &lcsi.ValidateVolumeCapabilitiesResponse{
		Confirmed: confirmed,
	}, nil
}

func (s *controllerService) ControllerGetCapabilities(ctx lctx.Context, req *lcsi.ControllerGetCapabilitiesRequest) (*lcsi.ControllerGetCapabilitiesResponse, error) {
	llog.V(4).InfoS("[INFO] - ControllerGetCapabilities: Called", "request", *req)
	var caps []*lcsi.ControllerServiceCapability
	for _, capa := range controllerCaps {
		c := &lcsi.ControllerServiceCapability{
			Type: &lcsi.ControllerServiceCapability_Rpc{
				Rpc: &lcsi.ControllerServiceCapability_RPC{
					Type: capa,
				},
			},
		}
		caps = append(caps, c)
	}
	return &lcsi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

func (s *controllerService) ControllerExpandVolume(pctx lctx.Context, preq *lcsi.ControllerExpandVolumeRequest) (*lcsi.ControllerExpandVolumeResponse, error) {
	llog.V(4).InfoS("[INFO] - ControllerExpandVolume: Called", "request", *preq)

	volumeID := preq.GetVolumeId()
	if volumeID == "" {
		return nil, ErrVolumeIDNotProvided
	}

	// check if a request is already in-flight
	if ok := s.inFlight.Insert(volumeID); !ok {
		llog.InfoS("[INFO] - ControllerExpandVolume: Operation is already in-flight", "volumeID", volumeID)
		return nil, ErrOperationAlreadyExists(volumeID)
	}
	defer func() {
		llog.InfoS("[INFO] - ControllerExpandVolume: Operation completed", "volumeID", volumeID)
		s.inFlight.Delete(volumeID)
	}()

	capRange := preq.GetCapacityRange()
	if capRange == nil {
		llog.Errorf("ControllerExpandVolume: Capacity range is required")
		return nil, ErrCapacityRangeNotProvided
	}

	volSizeBytes := preq.GetCapacityRange().GetRequiredBytes()
	volSizeGB := uint64(lsutil.RoundUpSize(volSizeBytes, 1024*1024*1024))
	maxVolSize := capRange.GetLimitBytes()

	if maxVolSize > 0 && volSizeBytes > maxVolSize {
		llog.Errorf("ControllerExpandVolume: Requested size %d exceeds limit %d", volSizeBytes, maxVolSize)
		return nil, ErrRequestExceedLimit(volSizeBytes, maxVolSize)
	}

	volume, err := s.cloud.GetVolume(volumeID)
	if err != nil {
		llog.ErrorS(err.GetError(), "ControllerExpandVolume: failed to get volume", "volumeID", volumeID)
		return nil, ErrFailedToGetVolume(volumeID)
	}

	if volume == nil {
		llog.Errorf("ControllerExpandVolume: volume %s not found", volumeID)
		return nil, ErrVolumeNotFound(volumeID)
	}

	if volume.Size >= volSizeGB {
		llog.V(2).Infof("ControllerExpandVolume; volume %s already has size %d GiB", volumeID, volume.Size)
		return &lcsi.ControllerExpandVolumeResponse{
			CapacityBytes:         lsutil.GiBToBytes(int64(volume.Size)),
			NodeExpansionRequired: true,
		}, nil
	}

	llog.V(5).InfoS("ControllerExpandVolume: expanding volume", "volumeID", volumeID, "newSize", volSizeGB)
	// Expand the volume
	err1 := s.cloud.ExpandVolume(pctx, volumeID, volume.VolumeTypeID, volSizeGB)
	if err1 != nil {
		llog.ErrorS(err1, "ControllerExpandVolume: failed to expand volume", "volumeID", volumeID)
		return nil, ErrFailedToExpandVolume(volumeID, int64(volSizeGB))
	}

	llog.V(4).InfoS("ControllerExpandVolume: volume expanded successfully", "volumeID", volumeID, "newSize", volSizeGB)
	return &lcsi.ControllerExpandVolumeResponse{
		CapacityBytes:         volSizeBytes,
		NodeExpansionRequired: true,
	}, nil
}

func (s *controllerService) ControllerModifyVolume(ctx lctx.Context, preq *lcsi.ControllerModifyVolumeRequest) (*lcsi.ControllerModifyVolumeResponse, error) {
	llog.V(4).InfoS("ControllerModifyVolume: called", "preq", *preq)

	volumeID := preq.GetVolumeId()
	if volumeID == "" {
		return nil, ErrVolumeIDNotProvided
	}

	options, err := parseModifyVolumeParameters(preq.GetMutableParameters())
	if err != nil {
		llog.ErrorS(err, "ControllerModifyVolume: invalid request")
		return nil, err
	}

	err = s.modifyVolumeWithCoalescing(ctx, volumeID, options)
	if err != nil {
		llog.ErrorS(err, "ControllerModifyVolume: failed to modify volume", "volumeID", volumeID)
		return nil, err
	}

	return &lcsi.ControllerModifyVolumeResponse{}, nil
}

func (s *controllerService) ModifyVolumeProperties(pctx lctx.Context, preq *lvmrpc.ModifyVolumePropertiesRequest) (*lvmrpc.ModifyVolumePropertiesResponse, error) {
	llog.V(5).InfoS("[INFO] - ModifyVolumeProperties: Called", "request", preq)

	if err := validateModifyVolumePropertiesRequest(preq); err != nil {
		llog.ErrorS(err, "[ERROR] - ModifyVolumeProperties: Invalid request because of volume ID is empty", "request", preq)
		return nil, err
	}

	options, _ := parseModifyVolumeParameters(preq.GetParameters())
	volumeID := preq.GetName()

	// check if a request is already in-flight
	if ok := s.inFlight.Insert(volumeID); !ok {
		return nil, ErrOperationAlreadyExists(volumeID)
	}
	defer s.inFlight.Delete(volumeID)

	volume, errSdk := s.cloud.GetVolume(volumeID)
	if errSdk != nil {
		llog.ErrorS(errSdk.GetError(), "[ERROR] - ModifyVolumeProperties: Failed to get volume", "volumeID", volumeID)
		return nil, ErrFailedToGetVolume(volumeID)
	}

	volumeTypeId, sdkErr := s.cloud.GetVolumeTypeIdByName(volume.ZoneId, options.VolumeType)
	if sdkErr != nil {
		llog.ErrorS(sdkErr.GetError(), "[ERROR] - ModifyVolumeProperties: Failed to get the volume type ID by name", sdkErr.GetListParameters()...)
		return nil, ErrFailedToGetVolume(volumeID)
	}

	if volume.VolumeTypeID == options.VolumeType || volumeTypeId == volume.VolumeTypeID {
		llog.V(2).Infof("[INFO] - ModifyVolumeProperties: Volume %s already has volume type %s", volumeID, options.VolumeType)
		return &lvmrpc.ModifyVolumePropertiesResponse{}, nil
	}

	llog.InfoS("[INFO] - ModifyVolumeProperties: Modifying volume", "volumeID", volumeID, "newVolumeType", options.VolumeType, "oldVolumeType", volume.VolumeTypeID, "newSize", volume.Size)
	ierr := s.cloud.ModifyVolumeType(pctx, volumeID, volumeTypeId, int(volume.Size))
	if ierr != nil {
		llog.ErrorS(ierr.GetError(), "ModifyVolumeProperties: failed to modify volume", "volumeID", volumeID)
		return nil, ierr.GetError()
	}

	return &lvmrpc.ModifyVolumePropertiesResponse{}, nil
}

func (s *controllerService) GetCSIDriverModificationCapability(_ lctx.Context, _ *lvmrpc.GetCSIDriverModificationCapabilityRequest) (*lvmrpc.GetCSIDriverModificationCapabilityResponse, error) {
	return &lvmrpc.GetCSIDriverModificationCapabilityResponse{}, nil
}

func (s *controllerService) ControllerGetVolume(_ lctx.Context, preq *lcsi.ControllerGetVolumeRequest) (*lcsi.ControllerGetVolumeResponse, error) {
	llog.V(4).InfoS("ControllerGetVolume: called", "preq", *preq)
	return nil, ErrNotImplemented("ControllerGetVolume")
}

func (s *controllerService) ListVolumes(ctx lctx.Context, req *lcsi.ListVolumesRequest) (*lcsi.ListVolumesResponse, error) {
	llog.V(4).InfoS("ListVolumes: called", "args", *req)
	return nil, ErrNotImplemented("ListVolumes")
}

func (s *controllerService) GetCapacity(ctx lctx.Context, req *lcsi.GetCapacityRequest) (*lcsi.GetCapacityResponse, error) {
	return nil, ErrNotImplemented("GetCapacity")
}

func (s *controllerService) getClusterID() string {
	return s.driverOptions.clusterID
}

// getVolSizeBytes takes pctx only so the three IaaS lookups below can report
// their own failures - each of them used to discard the lserr.IError with
// GetError(), which threw away the only thing the classifier can read.
func (s *controllerService) getVolSizeBytes(pctx lctx.Context, zoneID string, preq *lcsi.CreateVolumeRequest) (volumeTypeId string, volSizeBytes int64, err error) {
	// get the volume size that user provided
	if preq.GetCapacityRange() != nil {
		volSizeBytes = preq.GetCapacityRange().GetRequiredBytes()
	}

	// Get the volume type that user specified in the StorageClass
	volType, ok := preq.GetParameters()[VolumeTypeKey]
	if !ok {
		// If the user forget to specify the volume type, get the default volume type
		tmpVolType, sdkErr := s.cloud.GetDefaultVolumeType()
		if sdkErr != nil {
			s.reportCreateIaaSError(pctx, preq, sdkErr)
			return "", 0, sdkErr.GetError()
		}

		volType = tmpVolType.Id
	}
	volumeTypeId, sdkErr := s.cloud.GetVolumeTypeIdByName(zoneID, volType)
	if sdkErr != nil {
		s.reportCreateIaaSError(pctx, preq, sdkErr)
		return "", 0, sdkErr.GetError()
	}

	// Get the minimum volume size allowed by the volume type
	volTypeEntity, sdkErr := s.cloud.GetVolumeTypeById(volumeTypeId)
	if sdkErr != nil {
		s.reportCreateIaaSError(pctx, preq, sdkErr)
		return "", 0, sdkErr.GetError()
	}

	// Calculate the bytes that cloud provider allowing to create the volume
	cvs := lsutil.GiBToBytes(int64(volTypeEntity.MinSize))
	if volSizeBytes < cvs {
		// Deliberately NOT reported through reportCreateIaaSError: the IaaS
		// answered fine, the request asked for too little. Counting it as an
		// IaaS error would charge a user mistake to the operator's IaaS error
		// budget and point the reason label at the wrong subsystem.
		return volumeTypeId, 0, ErrVolumeSizeTooSmall(preq.GetName(), volSizeBytes)
	}

	return volumeTypeId, volSizeBytes, nil
}

func newCreateVolumeResponse(disk *lsentity.Volume, availabilityZone string, pcvr *CreateVolumeRequest, prespCtx map[string]string) *lcsi.CreateVolumeResponse {
	var vcs *lcsi.VolumeContentSource
	if pcvr.SnapshotID != "" {
		vcs = &lcsi.VolumeContentSource{
			Type: &lcsi.VolumeContentSource_Snapshot{
				Snapshot: &lcsi.VolumeContentSource_SnapshotSource{
					SnapshotId: pcvr.SnapshotID,
				},
			},
		}
	}
	segments := map[string]string{ZoneTopologyKey: availabilityZone}
	return &lcsi.CreateVolumeResponse{
		Volume: &lcsi.Volume{
			VolumeId:      disk.Id,
			CapacityBytes: int64(disk.Size * 1024 * 1024 * 1024),
			VolumeContext: prespCtx,
			AccessibleTopology: []*lcsi.Topology{
				{
					Segments: segments,
				},
			},
			ContentSource: vcs,
		},
	}
}

func newCreateSnapshotResponse(snapshot *lsentity.Snapshot) (*lcsi.CreateSnapshotResponse, error) {
	creationTime, err := ltime.Parse("2006-01-02T15:04:05.000-07:00", snapshot.CreatedAt)
	if err != nil {
		creationTime = ltime.Now()
	}

	return &lcsi.CreateSnapshotResponse{
		Snapshot: &lcsi.Snapshot{
			SnapshotId:     snapshot.Id,
			SourceVolumeId: snapshot.VolumeId,
			SizeBytes:      snapshot.VolumeSize * lsutil.GiB,
			CreationTime:   lts.New(creationTime),
			ReadyToUse:     true,
		},
	}, nil
}

func newListSnapshotsResponse(psnapshotList *lsentity.ListSnapshots) *lcsi.ListSnapshotsResponse {
	var entries []*lcsi.ListSnapshotsResponse_Entry
	for _, snapshot := range psnapshotList.Items {
		snapshotResponseEntry := newListSnapshotsResponseEntry(snapshot)
		entries = append(entries, snapshotResponseEntry)
	}

	nextToken := ""
	if psnapshotList.Page < psnapshotList.TotalPages {
		nextToken = lstrconv.Itoa(psnapshotList.Page + 1)
	}

	return &lcsi.ListSnapshotsResponse{
		Entries:   entries,
		NextToken: nextToken,
	}
}

func newGetSnapshotsResponse(psnapshotID string) *lcsi.ListSnapshotsResponse {
	return &lcsi.ListSnapshotsResponse{
		Entries: []*lcsi.ListSnapshotsResponse_Entry{
			{
				Snapshot: &lcsi.Snapshot{
					SnapshotId:     psnapshotID,
					SourceVolumeId: "undefined",
					SizeBytes:      0,
					CreationTime:   lts.Now(),
					ReadyToUse:     true,
				},
			},
		},
		NextToken: "",
	}
}

func newListSnapshotsResponseEntry(snapshot *lsdkEntity.Snapshot) *lcsi.ListSnapshotsResponse_Entry {
	creationTime, err := ltime.Parse("2006-01-02T15:04:05.000-07:00", snapshot.CreatedAt)
	if err != nil {
		creationTime = ltime.Now()
	}

	return &lcsi.ListSnapshotsResponse_Entry{
		Snapshot: &lcsi.Snapshot{
			SnapshotId:     snapshot.Id,
			SourceVolumeId: snapshot.VolumeId,
			SizeBytes:      snapshot.Size * lsutil.GiB,
			CreationTime:   lts.New(creationTime),
			ReadyToUse:     snapshot.Status == lscloud.SnapshotActiveStatus,
		},
	}
}

func newControllerPublishVolumeResponse(pdevicePath string) *lcsi.ControllerPublishVolumeResponse {
	return &lcsi.ControllerPublishVolumeResponse{
		PublishContext: map[string]string{
			DevicePathKey: pdevicePath,
		},
	}
}

func parsePage(nextToken string) int {
	if nextToken == "" {
		return 1
	}

	page, err := lstrconv.Atoi(nextToken)
	if err != nil {
		return 1
	}

	return page
}

func getCreateVolumeRequestNamespacedName(preq *lcsi.CreateVolumeRequest) (string, string) {
	params := preq.GetParameters()
	if params != nil {
		return params[PVCNamespaceKey], params[PVCNameKey]
	}
	return "", ""
}
