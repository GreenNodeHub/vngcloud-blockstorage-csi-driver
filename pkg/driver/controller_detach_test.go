package driver

import (
	lctx "context"
	lstr "strings"
	ltesting "testing"
	ltime "time"

	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
	lcoreV1 "k8s.io/api/core/v1"
	lmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	lfake "k8s.io/client-go/kubernetes/fake"
	lk8srecord "k8s.io/client-go/tools/record"

	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
	lsinternal "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/driver/internal"
	lsk8s "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/k8s"
)

// These three helpers hold the only genuinely new policy in Task 5: the
// "event once per escalation, not per retry" dedup rule, the gauge lifecycle,
// and the Since-before-Success ordering. None of them touch s.cloud, so they
// are testable today with no Cloud mock - unlike ControllerUnpublishVolume
// itself, which the spec accepted would stay untested until one exists.

func pvWithHandle(pname, phandle string) *lcoreV1.PersistentVolume {
	return &lcoreV1.PersistentVolume{
		ObjectMeta: lmetav1.ObjectMeta{Name: pname},
		Spec: lcoreV1.PersistentVolumeSpec{
			PersistentVolumeSource: lcoreV1.PersistentVolumeSource{
				CSI: &lcoreV1.CSIPersistentVolumeSource{
					Driver:       "bs.csi.vngcloud.vn",
					VolumeHandle: phandle,
				},
			},
		},
	}
}

// nonTerminalDetachError classifies as ReasonIaaSUnknownError, non-terminal -
// so repeated failures walk the breaker through its steps instead of tripping
// on the first call.
func nonTerminalDetachError() lserr.IError {
	return lserr.NewError(new(lsdkErrs.SdkError).WithErrorCode(lsdkErrs.EcUnknownError))
}

func newDetachTestService(pvolumeID string) (*controllerService, chan string) {
	rec := lk8srecord.NewFakeRecorder(20)
	client := lfake.NewSimpleClientset(pvWithHandle("pv-a", pvolumeID))

	svc := &controllerService{
		detachBreaker: lsinternal.NewBreaker(),
		k8sClient:     lsk8s.NewKubernetes(client, rec),
	}

	return svc, rec.Events
}

// Finding 3 / test 1: repeated failures produce an event on the trip and on
// each step increase, but not on the retries in between (including the
// retries that pile up after the backoff has held at the cap).
func TestOnDetachFailedEventsOnlyOnTripAndStepIncrease(t *ltesting.T) {
	const volumeID, nodeID = "vol-a", "ins-1"
	svc, events := newDetachTestService(volumeID)
	key := lsinternal.BreakerKey{VolumeID: volumeID, NodeID: nodeID}
	ierr := nonTerminalDetachError()

	// Six minutes apart: the real cadence of the incident, and far enough to
	// clear lsinternal.BreakerTripMinElapsed - the breaker no longer trips on
	// a burst of failures seconds apart.
	now := ltime.Unix(0, 0)
	gotEvents := 0
	var messages []string
	for i := 0; i < BreakerFailuresToWalkAllStepsAndPastCap; i++ {
		svc.onDetachFailed(lctx.Background(), volumeID, nodeID, key, now, ierr)
		now = now.Add(6 * ltime.Minute)

		select {
		case msg := <-events:
			gotEvents++
			messages = append(messages, msg)
		default:
		}
	}

	// 3 failures to trip (1 event) + 3 step increases (1 event each) = 4.
	// BreakerSteps has 4 entries, so only 3 transitions (0->1, 1->2, 2->3)
	// exist before the backoff holds at the cap; further failures at the cap
	// produce no more events.
	if gotEvents != 4 {
		t.Fatalf("got %d events, want 4 (1 trip + 3 step increases); messages=%v", gotEvents, messages)
	}

	foundReasonAndDuration := false
	for _, msg := range messages {
		if lstr.Contains(msg, "VolumeDetachStalled") && lstr.Contains(msg, "IaaSUnknownError") && lstr.Contains(msg, "stuck for") {
			foundReasonAndDuration = true
		}
	}
	if !foundReasonAndDuration {
		t.Fatalf("no event named both the reason and the elapsed time; messages=%v", messages)
	}
}

// BreakerFailuresToWalkAllStepsAndPastCap: BreakerTripAfter(3) to trip, then
// one failure per remaining step transition (len(BreakerSteps)-1 = 3) to walk
// step 0 -> 1 -> 2 -> 3, then a few more once held at the cap to prove those
// stay silent.
const BreakerFailuresToWalkAllStepsAndPastCap = 3 + 3 + 5

// Finding 3 / test 2: a pair that was never tracked (no prior Failure call)
// produces no event on success - guards the wasTracked branch in
// onDetachSucceeded.
func TestOnDetachSucceededNoEventWhenNeverTracked(t *ltesting.T) {
	const volumeID, nodeID = "vol-untracked", "ins-1"
	svc, events := newDetachTestService(volumeID)
	key := lsinternal.BreakerKey{VolumeID: volumeID, NodeID: nodeID}

	svc.onDetachSucceeded(lctx.Background(), volumeID, nodeID, key, ltime.Unix(0, 0))

	select {
	case msg := <-events:
		t.Fatalf("event emitted for a pair that was never tracked as failing: %q", msg)
	default:
	}
}

// Finding 3 / test 3: a pair that had tripped produces exactly one Normal /
// VolumeDetachRecovered event on success. This is the test that would fail if
// Since (which reads whether the pair was tracked) and Success (which clears
// it) were ever swapped in onDetachSucceeded - swap them and wasTracked is
// always false, silently losing every recovery event with no other symptom.
func TestOnDetachSucceededEmitsRecoveredEventWhenPreviouslyStuck(t *ltesting.T) {
	const volumeID, nodeID = "vol-b", "ins-1"
	svc, events := newDetachTestService(volumeID)
	key := lsinternal.BreakerKey{VolumeID: volumeID, NodeID: nodeID}
	ierr := nonTerminalDetachError()

	// Spaced past lsinternal.BreakerTripMinElapsed so the pair actually trips;
	// a fast burst would not, and after Item 3 an untripped pair emits nothing.
	now := ltime.Unix(0, 0)
	for i := 0; i < 3; i++ {
		svc.onDetachFailed(lctx.Background(), volumeID, nodeID, key, now, ierr)
		now = now.Add(6 * ltime.Minute)
	}
	// Drain the trip event so it does not get mistaken for the recovery one.
	select {
	case <-events:
	default:
		t.Fatal("expected a trip event before recovery, got none")
	}

	now = now.Add(30 * ltime.Minute)
	svc.onDetachSucceeded(lctx.Background(), volumeID, nodeID, key, now)

	var got []string
	drain := true
	for drain {
		select {
		case msg := <-events:
			got = append(got, msg)
		default:
			drain = false
		}
	}

	if len(got) != 1 {
		t.Fatalf("got %d events on recovery, want exactly 1; messages=%v", len(got), got)
	}
	if !lstr.Contains(got[0], "Normal") || !lstr.Contains(got[0], "VolumeDetachRecovered") {
		t.Fatalf("event = %q, want a Normal VolumeDetachRecovered event", got[0])
	}
}

// Item 3: a transient failure that never trips the breaker must stay silent on
// recovery.
//
// The busy-volume rejection is a normal occurrence on this driver, so
// "one failure then success" is the common case, not the interesting one. Each
// VolumeDetachRecovered event costs a FindPersistentVolumeByHandle - a full
// unpaginated PersistentVolumes().List() plus a PV GET plus a PVC GET - and
// pkg/k8s/k8s.go promises that only ever happens at a handful of moments per
// STUCK volume. An unpaginated LIST once per detach is the load shape that has
// OOMed tenant apiservers on this fleet.
func TestOnDetachSucceededNoEventWhenNeverTripped(t *ltesting.T) {
	const volumeID, nodeID = "vol-transient", "ins-1"
	svc, events := newDetachTestService(volumeID)
	key := lsinternal.BreakerKey{VolumeID: volumeID, NodeID: nodeID}

	now := ltime.Unix(0, 0)
	svc.onDetachFailed(lctx.Background(), volumeID, nodeID, key, now, nonTerminalDetachError())

	select {
	case msg := <-events:
		t.Fatalf("a single untripped failure emitted an event: %q", msg)
	default:
	}

	svc.onDetachSucceeded(lctx.Background(), volumeID, nodeID, key, now.Add(2*ltime.Second))

	select {
	case msg := <-events:
		t.Fatalf("recovery event emitted for a pair that never tripped: %q", msg)
	default:
	}
}
