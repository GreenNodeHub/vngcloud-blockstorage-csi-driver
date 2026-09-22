package driver

import (
	lctx "context"
	lstrings "strings"
	ltesting "testing"
	ltime "time"

	lcsi "github.com/container-storage-interface/spec/lib/go/csi"
	lsdkEntity "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/entity"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"

	lscloud "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud"
	lsentity "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/entity"
	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
	lsinternal "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/driver/internal"
)

// deleteSpyCloud counts what the handler actually asked the IaaS to do. The
// counts are the point of every test here: the breaker's whole purpose on this
// path is to stop DeleteVolume from being entered at all, and an assertion on
// the returned error alone passes just as happily when it is entered.
type deleteSpyCloud struct {
	lscloud.Cloud

	getVol    *lsdkEntity.Volume
	getErr    lserr.IError
	deleteErr lserr.IError
	listErr   lserr.IError

	gets, lists, deletes int
}

func (s *deleteSpyCloud) GetVolume(_ string) (*lsentity.Volume, lserr.IError) {
	s.gets++
	if s.getErr != nil {
		return nil, s.getErr
	}

	return &lsentity.Volume{Volume: s.getVol}, nil
}

func (s *deleteSpyCloud) ListSnapshots(_ string, _, _ int) (*lsentity.ListSnapshots, lserr.IError) {
	s.lists++
	if s.listErr != nil {
		return nil, s.listErr
	}

	return &lsentity.ListSnapshots{ListSnapshots: &lsdkEntity.ListSnapshots{}}, nil
}

func (s *deleteSpyCloud) DeleteVolume(_ lctx.Context, _ string) lserr.IError {
	s.deletes++

	return s.deleteErr
}

// stuckVolume is the shape QC hit: vServer still lists the machine, so
// CanDelete stays false and waitVolumeDeletable polls until the sidecar's
// context expires.
func stuckVolume(pinstanceID string) *lsdkEntity.Volume {
	return &lsdkEntity.Volume{VmId: pinstanceID, AttachedMachine: []string{pinstanceID}, Status: "DETACHING"}
}

func freeVolume() *lsdkEntity.Volume {
	return &lsdkEntity.Volume{Status: "AVAILABLE"}
}

// tripDeleteBreaker opens the breaker the way a terminal failure does, which
// needs no clock injection: BreakerSteps[0] is 10 minutes, so the next real
// attempt is comfortably in the future for the handler's own time.Now().
func tripDeleteBreaker(psvc *controllerService, pvolumeID string) {
	psvc.deleteBreaker.Failure(lsinternal.BreakerKey{VolumeID: pvolumeID}, true, ltime.Now())
}

// The reason this PR exists. Before it, every one of the 37 csi-provisioner
// retries entered cloud.DeleteVolume, which polls waitVolumeDeletable until
// the 60s sidecar context expires - a full worker slot per retry, spent on a
// volume vServer was never going to release on its own.
func TestDeleteVolumePausedIssuesNoDeleteAndOnlyOneRead(t *ltesting.T) {
	const volumeID = "vol-a"
	svc, _ := newDetachTestService(volumeID)
	svc.inFlight = lsinternal.NewInFlight()
	spy := &deleteSpyCloud{getVol: stuckVolume("ins-1")}
	svc.cloud = spy

	tripDeleteBreaker(svc, volumeID)

	_, err := svc.DeleteVolume(lctx.Background(), &lcsi.DeleteVolumeRequest{VolumeId: volumeID})
	if err == nil {
		t.Fatal("a paused delete must still fail, so csi-provisioner keeps the PV and retries")
	}
	if !lstrings.Contains(err.Error(), "PAUSED") {
		t.Errorf("error = %q, want it to say the call was paused", err)
	}

	if spy.deletes != 0 {
		t.Errorf("cloud.DeleteVolume entered %d times while paused, want 0 - the wait is the cost being removed", spy.deletes)
	}
	if spy.lists != 0 {
		t.Errorf("ListSnapshots called %d times while paused, want 0", spy.lists)
	}
	if spy.gets != 1 {
		t.Errorf("GetVolume called %d times, want exactly 1 (the probe)", spy.gets)
	}
}

// The breaker must not outlive the condition it was opened on. A volume that
// became deletable gets its real attempt on the very next retry rather than
// sitting out the rest of a backoff step that can be two hours long.
func TestDeleteVolumePausedFallsThroughOnceDeletable(t *ltesting.T) {
	const volumeID = "vol-a"
	svc, _ := newDetachTestService(volumeID)
	svc.inFlight = lsinternal.NewInFlight()
	spy := &deleteSpyCloud{getVol: freeVolume()}
	svc.cloud = spy

	tripDeleteBreaker(svc, volumeID)

	if _, err := svc.DeleteVolume(lctx.Background(), &lcsi.DeleteVolumeRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("delete = %v, want success once the probe says the volume is deletable", err)
	}

	if spy.deletes != 1 {
		t.Errorf("cloud.DeleteVolume entered %d times, want 1", spy.deletes)
	}
}

// A volume that is GONE must not be held by the breaker: DeleteVolume is
// required to be idempotent, and the PV is only released once this returns OK.
func TestDeleteVolumePausedTreatsMissingVolumeAsDeletable(t *ltesting.T) {
	const volumeID = "vol-a"
	svc, _ := newDetachTestService(volumeID)
	svc.inFlight = lsinternal.NewInFlight()
	spy := &deleteSpyCloud{
		getErr: lserr.NewError(new(lsdkErrs.SdkError).WithErrorCode(lsdkErrs.EcVServerVolumeNotFound)),
	}
	svc.cloud = spy

	tripDeleteBreaker(svc, volumeID)

	if _, err := svc.DeleteVolume(lctx.Background(), &lcsi.DeleteVolumeRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("delete = %v, want success - a volume that no longer exists is deleted", err)
	}
	if spy.deletes != 1 {
		t.Errorf("cloud.DeleteVolume entered %d times, want 1", spy.deletes)
	}
}

// A probe that cannot read the volume says nothing about whether a delete
// would work, so it must keep the pause rather than spending the call.
func TestDeleteVolumePausedStaysPausedWhenProbeFails(t *ltesting.T) {
	const volumeID = "vol-a"
	svc, _ := newDetachTestService(volumeID)
	svc.inFlight = lsinternal.NewInFlight()
	spy := &deleteSpyCloud{
		getErr: lserr.NewError(new(lsdkErrs.SdkError).WithErrorCode(lsdkErrs.EcUnknownError)),
	}
	svc.cloud = spy

	tripDeleteBreaker(svc, volumeID)

	if _, err := svc.DeleteVolume(lctx.Background(), &lcsi.DeleteVolumeRequest{VolumeId: volumeID}); err == nil {
		t.Fatal("expected the delete to stay paused when the probe cannot read the volume")
	}
	if spy.deletes != 0 {
		t.Errorf("cloud.DeleteVolume entered %d times, want 0", spy.deletes)
	}
}

// A failed probe must NOT advance the backoff: it sent no command, so it is no
// evidence about whether the IaaS accepts them. Without this the breaker would
// walk 10m -> 30m -> 1h -> 2h on nothing but read failures.
func TestDeleteProbeFailureDoesNotAdvanceBackoff(t *ltesting.T) {
	const volumeID = "vol-a"
	svc, _ := newDetachTestService(volumeID)
	svc.inFlight = lsinternal.NewInFlight()
	svc.cloud = &deleteSpyCloud{
		getErr: lserr.NewError(new(lsdkErrs.SdkError).WithErrorCode(lsdkErrs.EcUnknownError)),
	}

	tripDeleteBreaker(svc, volumeID)
	key := lsinternal.BreakerKey{VolumeID: volumeID}
	base := ltime.Now()

	// The step is readable only through Allow: at base+15m the first step (10m)
	// has expired, so a breaker still on step 0 grants a real attempt. One that
	// had been stepped to 30m would not.
	for range 5 {
		if _, err := svc.DeleteVolume(lctx.Background(), &lcsi.DeleteVolumeRequest{VolumeId: volumeID}); err == nil {
			t.Fatal("expected the probe to fail")
		}
	}

	if got := svc.deleteBreaker.Allow(key, base.Add(15*ltime.Minute)); got != lsinternal.Full {
		t.Errorf("Allow after 15m = %v, want Full - five failed probes advanced the backoff", got)
	}
}

// The two breakers are separate instances on purpose. Sharing one would mean a
// volume the IaaS cannot delete also stops being detached - a coupling nothing
// in either incident calls for, and one that would be very hard to see.
func TestDeleteBreakerDoesNotPauseDetach(t *ltesting.T) {
	const volumeID, nodeID = "vol-a", "ins-1"
	svc, _ := newDetachTestService(volumeID)

	tripDeleteBreaker(svc, volumeID)

	got := svc.detachBreaker.Allow(lsinternal.BreakerKey{VolumeID: volumeID, NodeID: nodeID}, ltime.Now())
	if got != lsinternal.Full {
		t.Errorf("detach Allow = %v, want Full - a stuck delete must not pause detach", got)
	}
}

// A volume held by its own snapshots is not a stalled IaaS. The delete can
// never succeed until the user removes the snapshot, but backing off and
// reporting "vServer is stuck" would be a lie about where the problem is.
func TestDeleteVolumeWithSnapshotsDoesNotTripTheBreaker(t *ltesting.T) {
	const volumeID = "vol-a"
	svc, _ := newDetachTestService(volumeID)
	svc.inFlight = lsinternal.NewInFlight()
	svc.cloud = &snapshotHoldingCloud{}

	for range 5 {
		if _, err := svc.DeleteVolume(lctx.Background(), &lcsi.DeleteVolumeRequest{VolumeId: volumeID}); err == nil {
			t.Fatal("expected the delete to be refused while a snapshot holds the volume")
		}
	}

	if _, tracked, _ := svc.deleteBreaker.Since(lsinternal.BreakerKey{VolumeID: volumeID}, ltime.Now()); tracked {
		t.Error("the delete breaker tracked a volume held by its own snapshots")
	}
}

type snapshotHoldingCloud struct {
	lscloud.Cloud
}

func (s *snapshotHoldingCloud) ListSnapshots(_ string, _, _ int) (*lsentity.ListSnapshots, lserr.IError) {
	return &lsentity.ListSnapshots{
		ListSnapshots: &lsdkEntity.ListSnapshots{Items: []*lsdkEntity.Snapshot{{Id: "snap-1"}}},
	}, nil
}

// Events: the first failure reports its reason immediately (what v1.5.1 gave),
// the retries in between report nothing, and the escalation says the driver
// has stopped trying. 37 retries used to mean 37 unpaginated PV List calls.
func TestOnDeleteFailedEventsOnFirstFailureAndEscalationsOnly(t *ltesting.T) {
	const volumeID = "vol-a"
	svc, events := newDetachTestService(volumeID)
	key := lsinternal.BreakerKey{VolumeID: volumeID}
	ierr := lserr.NewError(new(lsdkErrs.SdkError).WithErrorCode(lsdkErrs.EcUnknownError))

	// Twelve failures six minutes apart - the incident's own cadence, and well
	// past the point where the backoff reaches its cap.
	now := ltime.Unix(0, 0)
	var got []string
	for range 12 {
		svc.onDeleteFailed(lctx.Background(), volumeID, key, now, ierr)
		now = now.Add(6 * ltime.Minute)

		select {
		case e := <-events:
			got = append(got, e)
		default:
		}
	}

	// Failure 1 reports its reason. Failure 2 is silent (still inside
	// BreakerTripMinElapsed). Failure 3 trips. Then one event per step
	// increase - three of them, 30m, 1h, 2h - and silence once the backoff
	// holds at the cap. Five in total, no matter how long the volume stays
	// stuck: that last part is the property worth pinning, because the cap is
	// what stops a volume stuck for a day from emitting events all day.
	const wantEvents = 5
	if len(got) != wantEvents {
		t.Fatalf("got %d events for 12 failures, want %d: %v", len(got), wantEvents, got)
	}
	if !lstrings.Contains(got[0], lscloud.ReasonIaaSUnknownError) {
		t.Errorf("first event = %q, want the classified reason", got[0])
	}
	for _, e := range got[1:] {
		if !lstrings.Contains(e, "VolumeDeleteStalled") {
			t.Errorf("escalation event = %q, want VolumeDeleteStalled", e)
		}
	}
}

// A recovery event is only worth the PV List it costs when the volume had
// actually been reported stuck.
func TestOnDeleteSucceededOnlyReportsRecoveryAfterATrip(t *ltesting.T) {
	const volumeID = "vol-a"
	svc, events := newDetachTestService(volumeID)
	key := lsinternal.BreakerKey{VolumeID: volumeID}
	now := ltime.Unix(0, 0)

	svc.onDeleteSucceeded(lctx.Background(), volumeID, key, now)
	select {
	case e := <-events:
		t.Fatalf("untracked volume produced a recovery event: %q", e)
	default:
	}

	svc.deleteBreaker.Failure(key, true, now)
	svc.onDeleteSucceeded(lctx.Background(), volumeID, key, now.Add(ltime.Hour))

	select {
	case e := <-events:
		if !lstrings.Contains(e, "VolumeDeleteRecovered") {
			t.Errorf("event = %q, want VolumeDeleteRecovered", e)
		}
	default:
		t.Error("a volume that had tripped produced no recovery event")
	}
}
