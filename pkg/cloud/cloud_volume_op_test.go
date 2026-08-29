package cloud

import (
	ltesting "testing"
	ltime "time"

	lsdkEntity "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/entity"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
	lwait "k8s.io/apimachinery/pkg/util/wait"

	lsentity "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/entity"
)

const (
	testInstanceA  = "ins-aaaaaaaa-0000-0000-0000-000000000000"
	testInstanceB  = "ins-bbbbbbbb-0000-0000-0000-000000000000"
	testTypeSource = "vtype-11111111-0000-0000-0000-000000000000"
	testTypeTarget = "vtype-22222222-0000-0000-0000-000000000000"
)

func rawVolume(pstatus, pvmId string, pattached ...string) *lsdkEntity.Volume {
	return &lsdkEntity.Volume{
		Id:              "vol-8a1f7e28-b36b-4d72-b3c5-de4a7b944654",
		Status:          pstatus,
		VmId:            pvmId,
		AttachedMachine: pattached,
	}
}

// TestIsDetachedFrom khoa hop dong idempotent cua ControllerUnpublishVolume:
// moi trang thai "khong con dinh vao dung instance nay" deu la thanh cong.
func TestIsDetachedFrom(t *ltesting.T) {
	tcs := []struct {
		name     string
		vol      *lsdkEntity.Volume
		instance string
		want     bool
	}{
		{"AVAILABLE thi luon la da detach", rawVolume(VolumeAvailableStatus, ""), testInstanceA, true},
		{"IN-USE tren dung instance thi chua detach", rawVolume(VolumeInUseStatus, testInstanceA), testInstanceA, false},
		{"IN-USE tren instance khac thi coi nhu da detach", rawVolume(VolumeInUseStatus, testInstanceB), testInstanceA, true},
		{"dinh qua AttachedMachine cung tinh la chua detach", rawVolume(VolumeInUseStatus, "", testInstanceA), testInstanceA, false},
		{"AVAILABLE nhung con so AttachedMachine cu van la da detach", rawVolume(VolumeAvailableStatus, "", testInstanceA), testInstanceA, true},
		{"vol nil thi khong duoc ket luan da detach", nil, testInstanceA, false},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *ltesting.T) {
			if got := isDetachedFrom(tc.vol, tc.instance); got != tc.want {
				t.Fatalf("isDetachedFrom() = %v, muon %v", got, tc.want)
			}
		})
	}
}

// TestIsMigratedToType: migration chi xong khi volume vua ve trang thai on dinh
// VUA dung volume type dich.
func TestIsMigratedToType(t *ltesting.T) {
	typed := func(pstatus, ptype string) *lsentity.Volume {
		return lsentity.NewVolume(&lsdkEntity.Volume{Status: pstatus, VolumeTypeID: ptype})
	}

	tcs := []struct {
		name string
		vol  *lsentity.Volume
		want bool
	}{
		{"AVAILABLE + dung type = xong", typed(VolumeAvailableStatus, testTypeTarget), true},
		{"IN-USE + dung type = xong", typed(VolumeInUseStatus, testTypeTarget), true},
		{"AVAILABLE nhung con type cu = chua xong", typed(VolumeAvailableStatus, testTypeSource), false},
		{"MIGRATING du da mang type dich = chua xong", typed("MIGRATING", testTypeTarget), false},
		{"CREATING = chua xong", typed(VolumeCreatingStatus, testTypeTarget), false},
		{"ERROR = chua xong", typed(VolumeErrorStatus, testTypeTarget), false},
		{"vol nil = chua xong", nil, false},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *ltesting.T) {
			if got := isMigratedToType(tc.vol, testTypeTarget); got != tc.want {
				t.Fatalf("isMigratedToType() = %v, muon %v", got, tc.want)
			}
		})
	}
}

// TestDetachErrorSetsCoverEverySdkCode: DetachBlockVolume chi tra ve dung nam ma
// loi, do chinh SDK khai bao (services/compute/v2/server.go). Ma nao khong duoc
// phan loai se roi vao nhanh default va lam RPC that bai.
func TestDetachErrorSetsCoverEverySdkCode(t *ltesting.T) {
	type detachClass string

	const (
		classDone         detachClass = "done"
		classRetryable    detachClass = "retryable"
		classUnclassified detachClass = "unclassified"
	)

	classify := func(pcode lsdkErrs.ErrorCode) detachClass {
		switch {
		case errSetDetachDone.ContainsOne(pcode):
			return classDone
		case errSetDetachRetryable.ContainsOne(pcode):
			return classRetryable
		default:
			return classUnclassified
		}
	}

	for code, want := range map[lsdkErrs.ErrorCode]detachClass{
		lsdkErrs.EcVServerVolumeNotFound:    classDone,
		lsdkErrs.EcVServerVolumeAvailable:   classDone,
		lsdkErrs.EcVServerServerNotFound:    classDone,
		lsdkErrs.EcVServerVolumeInProcess:   classRetryable,
		lsdkErrs.EcVServerVolumeIsMigrating: classRetryable,
	} {
		if got := classify(code); got != want {
			t.Errorf("%s duoc phan loai la %q, muon %q", code, got, want)
		}
	}
}

// TestMigrateInSameZoneIsNotTreatedAsInProgress: ma nay nghia la yeu cau bi TU
// CHOI (nguon va dich cung zone), khong co migration nao chay. Nuot no roi
// xuong poll se dot het ngan sach roi that bai bang ErrWaitTimeout.
func TestMigrateInSameZoneIsNotTreatedAsInProgress(t *ltesting.T) {
	if errSetMigrateInProgress.ContainsOne(lsdkErrs.EcVServerVolumeMigrateInSameZone) {
		t.Fatal("MigrateInSameZone la tu choi vinh vien, khong phai 'dang chay'")
	}

	for _, code := range []lsdkErrs.ErrorCode{
		lsdkErrs.EcVServerVolumeMigrateBeingProcess,
		lsdkErrs.EcVServerVolumeMigrateProcessingConfirm,
		lsdkErrs.EcVServerVolumeMigrateBeingMigrating,
		lsdkErrs.EcVServerVolumeMigrateBeingFinish,
	} {
		if !errSetMigrateInProgress.ContainsOne(code) {
			t.Errorf("%s phai duoc coi la migration dang chay", code)
		}
	}
}

// walkBackoff mo phong dung vong lap cua wait.ExponentialBackoffWithContext.
func walkBackoff(pbo lwait.Backoff) (polls int, total ltime.Duration, pollsIn60s int) {
	b := pbo
	for b.Steps > 0 {
		polls++
		if total < 60*ltime.Second {
			pollsIn60s++
		}
		if b.Steps == 1 {
			break
		}
		total += b.Step()
	}

	return polls, total, pollsIn60s
}

// TestBackoffsStartFastAndDeclareNoCap khoa hai thu:
//   - lan cho dau tien phai ngan (backoff joat cu voi Revert=true ngu 4m15s
//     truoc lan kiem tra dau tien khi steps=10)
//   - khong duoc dat Cap: trong apimachinery, Cap khong phai tran ma la dau cham
//     het - delay() gan steps = 0 ngay khi buoc ke tiep vuot Cap.
func TestBackoffsStartFastAndDeclareNoCap(t *ltesting.T) {
	for name, bo := range map[string]lwait.Backoff{
		"volumeOperationBackoff": volumeOperationBackoff,
		"volumeMigrationBackoff": volumeMigrationBackoff,
	} {
		if bo.Cap != 0 {
			t.Errorf("%s dat Cap = %v; Cap se zero hoa Steps va cat ngan ngan sach cho", name, bo.Cap)
		}

		probe := bo
		first := probe.Step()
		if first > 5*ltime.Second {
			t.Errorf("%s: lan sleep dau tien = %v, qua lau", name, first)
		}

		polls, total, in60s := walkBackoff(bo)
		if in60s < 5 {
			t.Errorf("%s: chi poll duoc %d lan trong 60s dau", name, in60s)
		}
		t.Logf("%s: %d lan poll, tong %v, %d lan trong 60s dau", name, polls, total.Round(ltime.Millisecond), in60s)
	}
}
