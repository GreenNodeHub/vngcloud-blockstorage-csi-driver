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

// TestIsDetachedFrom pins the idempotency contract of ControllerUnpublishVolume:
// every state that is "no longer attached to this particular instance" counts
// as success.
func TestIsDetachedFrom(t *ltesting.T) {
	tcs := []struct {
		name     string
		vol      *lsdkEntity.Volume
		instance string
		want     bool
	}{
		{"AVAILABLE always counts as detached", rawVolume(VolumeAvailableStatus, ""), testInstanceA, true},
		{"IN-USE on this instance is not detached", rawVolume(VolumeInUseStatus, testInstanceA), testInstanceA, false},
		{"IN-USE on another instance counts as detached", rawVolume(VolumeInUseStatus, testInstanceB), testInstanceA, true},
		{"attachment via AttachedMachine is not detached", rawVolume(VolumeInUseStatus, "", testInstanceA), testInstanceA, false},
		{"AVAILABLE with a stale AttachedMachine entry is detached", rawVolume(VolumeAvailableStatus, "", testInstanceA), testInstanceA, true},
		{"nil volume must not be concluded detached", nil, testInstanceA, false},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *ltesting.T) {
			if got := isDetachedFrom(tc.vol, tc.instance); got != tc.want {
				t.Fatalf("isDetachedFrom() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestIsMigratedToType: a migration is complete only when the volume has BOTH
// a settled status AND the target volume type.
func TestIsMigratedToType(t *ltesting.T) {
	typed := func(pstatus, ptype string) *lsentity.Volume {
		return lsentity.NewVolume(&lsdkEntity.Volume{Status: pstatus, VolumeTypeID: ptype})
	}

	tcs := []struct {
		name string
		vol  *lsentity.Volume
		want bool
	}{
		{"AVAILABLE + target type = done", typed(VolumeAvailableStatus, testTypeTarget), true},
		{"IN-USE + target type = done", typed(VolumeInUseStatus, testTypeTarget), true},
		{"AVAILABLE but still the old type = not done", typed(VolumeAvailableStatus, testTypeSource), false},
		{"MIGRATING even with the target type = not done", typed("MIGRATING", testTypeTarget), false},
		{"CREATING = not done", typed(VolumeCreatingStatus, testTypeTarget), false},
		{"ERROR = not done", typed(VolumeErrorStatus, testTypeTarget), false},
		{"nil volume = not done", nil, false},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *ltesting.T) {
			if got := isMigratedToType(tc.vol, testTypeTarget); got != tc.want {
				t.Fatalf("isMigratedToType() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestDetachErrorSetsCoverEverySdkCode: DetachBlockVolume returns exactly five
// error codes, declared by the SDK itself (services/compute/v2/server.go). Any
// unclassified code falls into the default branch and fails the RPC.
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
			t.Errorf("%s classified as %q, want %q", code, got, want)
		}
	}
}

// TestMigrateInSameZoneIsNotTreatedAsInProgress: this code means the request
// was REJECTED (source and target share a zone) - no migration is running.
// Swallowing it and falling through to the poll would burn the whole budget
// and then fail with ErrWaitTimeout.
func TestMigrateInSameZoneIsNotTreatedAsInProgress(t *ltesting.T) {
	if errSetMigrateInProgress.ContainsOne(lsdkErrs.EcVServerVolumeMigrateInSameZone) {
		t.Fatal("MigrateInSameZone is a permanent rejection, not 'in progress'")
	}

	for _, code := range []lsdkErrs.ErrorCode{
		lsdkErrs.EcVServerVolumeMigrateBeingProcess,
		lsdkErrs.EcVServerVolumeMigrateProcessingConfirm,
		lsdkErrs.EcVServerVolumeMigrateBeingMigrating,
		lsdkErrs.EcVServerVolumeMigrateBeingFinish,
	} {
		if !errSetMigrateInProgress.ContainsOne(code) {
			t.Errorf("%s must be treated as a migration in progress", code)
		}
	}
}

// walkBackoff simulates the exact loop of wait.ExponentialBackoffWithContext.
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

// TestBackoffsStartFastAndDeclareNoCap pins two properties:
//   - the first wait must be short (the old joat backoff with Revert=true
//     slept 4m15s before the first check when steps=10)
//   - Cap must not be set: in apimachinery, Cap is not a ceiling but a full
//     stop - delay() zeroes steps as soon as the next interval would exceed it.
func TestBackoffsStartFastAndDeclareNoCap(t *ltesting.T) {
	for name, bo := range map[string]lwait.Backoff{
		"volumeOperationBackoff": volumeOperationBackoff,
		"volumeMigrationBackoff": volumeMigrationBackoff,
	} {
		if bo.Cap != 0 {
			t.Errorf("%s sets Cap = %v; Cap zeroes Steps and truncates the wait budget", name, bo.Cap)
		}

		probe := bo
		first := probe.Step()
		if first > 5*ltime.Second {
			t.Errorf("%s: first sleep = %v, too long", name, first)
		}

		polls, total, in60s := walkBackoff(bo)
		if in60s < 5 {
			t.Errorf("%s: only %d polls within the first 60s", name, in60s)
		}
		t.Logf("%s: %d polls, %v total, %d within the first 60s", name, polls, total.Round(ltime.Millisecond), in60s)
	}
}
