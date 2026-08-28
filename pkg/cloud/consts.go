package cloud

import (
	ltime "time"

	lwait "k8s.io/apimachinery/pkg/util/wait"

	lset "github.com/cuongpiger/joat/data-structure/set"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"

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

// Backoff cho cac thao tac volume phia IaaS. Giong het ban da ap dung cho
// vngcloud-manage-csi-driver, va hoc tu aws-ebs-csi-driver
// (pkg/cloud/cloud.go, volumeWaitParameters):
//
//  1. TANG DAN, bat dau ngan. Attach/detach tren vServer hoan tat trong khoang
//     giay den vai chuc giay nen lan poll dau tien phai xay ra gan nhu ngay.
//
//  2. Deadline la ctx cua gRPC, KHONG phai dong ho noi bo. csi-attacher o day
//     chay --timeout=6m, csi-resizer/volumemodifier 60s; handler phai chet cung
//     client de nha inflight lock, neu khong moi retry deu an
//     "Aborted: operation already exists". Steps chi la chan tren.
//
//  3. Khong phat lai lenh mutate trong vong poll.
//
// KHONG dat truong Cap. Trong apimachinery, Cap khong phai tran ma la dau cham
// het: delay() gan steps = 0 ngay khi buoc ke tiep vuot Cap, va vong lap cua
// ExponentialBackoffWithContext la `for backoff.Steps > 0` - dat Cap se lam
// Steps tro nen vo nghia.
var (
	// 1s, 1.6s, 2.56s, 4.1s, 6.55s, 10.49s, ... ; tong 7m47s, 8 lan poll trong
	// 60 giay dau.
	volumeOperationBackoff = lwait.Backoff{
		Duration: 1 * ltime.Second,
		Factor:   1.6,
		Steps:    13,
	}

	// Migrate volume sang zone khac la thao tac cap phut nen bat dau tu 2s.
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

// Phan loai loi cua DetachBlockVolume. Danh sach ma API nay tra ve la huu han
// va do chinh SDK khai bao (services/compute/v2/server.go): VolumeNotFound,
// VolumeInProcess, ServerNotFound, VolumeIsMigrating, VolumeAvailable. Ma nao
// khong duoc phan loai se roi vao nhanh default va lam RPC that bai.
var (
	// Muc tieu da dat - khong con gi dinh vao instance nay nua.
	// ServerNotFound nam o day: node VM da bien mat thi khong the con
	// attachment, va neu tra loi thi external-attacher se khong bao gio go duoc
	// finalizer cua VolumeAttachment.
	errSetDetachDone = lset.NewSet[lsdkErrs.ErrorCode](
		lsdkErrs.EcVServerVolumeNotFound,
		lsdkErrs.EcVServerVolumeAvailable,
		lsdkErrs.EcVServerServerNotFound,
	)

	// IaaS dang ban voi mot thao tac khac tren volume nay - xuong poll, dung tra loi.
	errSetDetachRetryable = lset.NewSet[lsdkErrs.ErrorCode](
		lsdkErrs.EcVServerVolumeInProcess,
		lsdkErrs.EcVServerVolumeIsMigrating,
	)

	// Cac ma vServer tra ve khi migration da hoac dang chay - phat lenh lan nua
	// khong sai gi, chi la thua.
	//
	// EcVServerVolumeMigrateInSameZone CO Y KHONG nam trong danh sach nay: no
	// nghia la yeu cau bi TU CHOI vi nguon va dich cung zone, khong co migration
	// nao chay va se khong bao gio co. Nuot no roi xuong poll se dot het ngan
	// sach roi that bai bang ErrWaitTimeout, giau mat ly do that.
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
