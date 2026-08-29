package cloud

import (
	ltime "time"

	lset "github.com/cuongpiger/joat/data-structure/set"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
	lwait "k8s.io/apimachinery/pkg/util/wait"

	lsutil "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/util"
)

const (
	VksClusterIdTagKey      = "vng.vks.cluster.id"
	VksBillingProductTagKey = "vng.billing.product"
)

const (
	// DefaultVolumeSize represents the default volume size.
	DefaultVolumeSize int64 = 5 * lsutil.GiB
)

const (
	waitSnapshotActiveTimeout = 5 * ltime.Minute
	waitSnapshotActiveDelay   = 10
	waitSnapshotActiveSteps   = 5
)

// Backoff for volume operations against the IaaS. Identical to what is already
// deployed in vngcloud-manage-csi-driver, and modelled on aws-ebs-csi-driver
// (pkg/cloud/cloud.go, volumeWaitParameters):
//
//  1. ASCENDING, starting short. Attach/detach on vServer completes within
//     seconds to a few tens of seconds, so the first poll must happen almost
//     immediately.
//
//  2. The deadline is the gRPC context, NOT an internal clock. csi-attacher
//     here runs --timeout=6m, csi-resizer/volumemodifier 60s; the handler must
//     die with its client to release the inflight lock, otherwise every retry
//     gets "Aborted: operation already exists". Steps is only an upper bound.
//
//  3. Never re-issue a mutating call inside the poll loop.
//
// Cap is deliberately NOT set. In apimachinery, Cap is not a ceiling but a full
// stop: delay() sets steps = 0 as soon as the next interval would exceed Cap,
// and ExponentialBackoffWithContext loops on `for backoff.Steps > 0` - setting
// Cap silently makes Steps meaningless.
var (
	// 1s, 1.6s, 2.56s, 4.1s, 6.55s, 10.49s, ...; 7m47s in total, 8 polls within
	// the first 60 seconds.
	volumeOperationBackoff = lwait.Backoff{
		Duration: 1 * ltime.Second,
		Factor:   1.6,
		Steps:    13,
	}

	// Migrating a volume across zones is a minutes-scale operation, so start at 2s.
	volumeMigrationBackoff = lwait.Backoff{
		Duration: 2 * ltime.Second,
		Factor:   1.6,
		Steps:    12,
	}
)

const (
	VolumeAvailableStatus = "AVAILABLE"
	VolumeInUseStatus     = "IN-USE"
	VolumeCreatingStatus  = "CREATING"
	VolumeErrorStatus     = "ERROR"

	SnapshotActiveStatus = "ACTIVE"
)

// Error classification for DetachBlockVolume. The set of codes this API can
// return is finite and declared by the SDK itself
// (services/compute/v2/server.go): VolumeNotFound, VolumeInProcess,
// ServerNotFound, VolumeIsMigrating, VolumeAvailable. Any unclassified code
// falls into the default branch and fails the RPC.
var (
	// Goal reached - nothing is attached to this instance any more.
	// ServerNotFound belongs here: once the node VM is gone there can be no
	// attachment left, and returning an error would mean external-attacher
	// never gets to remove the VolumeAttachment finalizer.
	errSetDetachDone = lset.NewSet[lsdkErrs.ErrorCode](
		lsdkErrs.EcVServerVolumeNotFound,
		lsdkErrs.EcVServerVolumeAvailable,
		lsdkErrs.EcVServerServerNotFound,
	)

	// The IaaS is busy with another operation on this volume - fall through to
	// the poll instead of returning an error.
	errSetDetachRetryable = lset.NewSet[lsdkErrs.ErrorCode](
		lsdkErrs.EcVServerVolumeInProcess,
		lsdkErrs.EcVServerVolumeIsMigrating,
	)

	// Codes vServer returns when a migration has already started or is running -
	// re-issuing the command is harmless, merely redundant.
	//
	// EcVServerVolumeMigrateInSameZone is DELIBERATELY absent: it means the
	// request was REJECTED because source and target share a zone - nothing is
	// running and nothing ever will be. Swallowing it and falling through to
	// the poll would burn the whole budget and then fail with ErrWaitTimeout,
	// hiding the real reason.
	errSetMigrateInProgress = lset.NewSet[lsdkErrs.ErrorCode](
		lsdkErrs.EcVServerVolumeMigrateBeingProcess,
		lsdkErrs.EcVServerVolumeMigrateProcessingConfirm,
		lsdkErrs.EcVServerVolumeMigrateBeingMigrating,
		lsdkErrs.EcVServerVolumeMigrateBeingFinish,
	)
)

var (
	volumeArchivedStatus  = lset.NewSet[string](VolumeAvailableStatus, VolumeInUseStatus)
	volumeAvailableStatus = lset.NewSet[string](VolumeAvailableStatus)
	availableDeleteStatus = lset.NewSet[string](VolumeAvailableStatus, VolumeErrorStatus)
)

const (
	patternSnapshotDescription = "Snapshot of PersistentVolume %s for vKS cluster %s"
)
