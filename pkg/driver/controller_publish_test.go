package driver

import (
	lctx "context"
	ltesting "testing"
	ltime "time"

	lcsi "github.com/container-storage-interface/spec/lib/go/csi"

	lscloud "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud"
	lsentity "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/entity"
	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
	lsinternal "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/driver/internal"
)

// attachOnlyCloud implements just the two calls ControllerPublishVolume makes.
// The embedded nil interface is deliberate: any other cloud call this handler
// might grow would panic here rather than silently pass.
type attachOnlyCloud struct {
	lscloud.Cloud
}

func (s attachOnlyCloud) AttachVolume(_ lctx.Context, _, _ string) (*lsentity.Volume, lserr.IError) {
	return &lsentity.Volume{}, nil
}

func (s attachOnlyCloud) GetDeviceDiskID(_ string) (string, error) {
	return "/dev/vdb", nil
}

func publishRequest(pvolumeID, pnodeID string) *lcsi.ControllerPublishVolumeRequest {
	return &lcsi.ControllerPublishVolumeRequest{
		VolumeId: pvolumeID,
		NodeId:   pnodeID,
		VolumeCapability: &lcsi.VolumeCapability{
			AccessType: &lcsi.VolumeCapability_Mount{Mount: &lcsi.VolumeCapability_MountVolume{}},
			AccessMode: &lcsi.VolumeCapability_AccessMode{Mode: SingleNodeWriter},
		},
	}
}

// Item 4: a successful attach must void any leftover detach state for the pair.
//
// EvictStale only runs from ControllerUnpublishVolume, so the leak's own
// trigger - this pair stops receiving unpublish calls - is also what stops the
// sweep. After a human force-deletes the VolumeAttachment (the actual remedy
// used in the 23-hour incident) nothing ever calls Success. The breaker entry
// survives keyed only on (volumeID, nodeID); re-attach that volume to the same
// node and the next detach starts inside a stale pause, ProbeOnly for up to two
// hours, and the probe cannot clear it because the volume genuinely IS attached
// again.
func TestControllerPublishVolumeClearsATrippedDetachBreaker(t *ltesting.T) {
	const volumeID, nodeID = "vol-a", "ins-1"
	svc, _ := newDetachTestService(volumeID)
	svc.cloud = attachOnlyCloud{}
	svc.inFlight = lsinternal.NewInFlight()

	key := lsinternal.BreakerKey{VolumeID: volumeID, NodeID: nodeID}
	now := ltime.Unix(0, 0)
	for i := 0; i < 3; i++ {
		svc.onDetachFailed(lctx.Background(), volumeID, nodeID, key, now, nonTerminalDetachError())
		now = now.Add(6 * ltime.Minute)
	}
	if got := svc.detachBreaker.Allow(key, now); got != lsinternal.ProbeOnly {
		t.Fatalf("setup: Allow() = %v, want ProbeOnly (the pair must be tripped)", got)
	}

	if _, err := svc.ControllerPublishVolume(lctx.Background(), publishRequest(volumeID, nodeID)); err != nil {
		t.Fatalf("ControllerPublishVolume() error = %v", err)
	}

	if got := svc.detachBreaker.Allow(key, now); got != lsinternal.Full {
		t.Fatalf("Allow() after a successful attach = %v, want Full", got)
	}
	if _, tracked, _ := svc.detachBreaker.Since(key, now); tracked {
		t.Fatal("the breaker still tracks a pair that has since attached successfully")
	}
}

// The sweep must run from the attach path too, and not only for the pair being
// attached: attach traffic is the only traffic a pair whose unpublish calls
// have stopped will ever see again. Driven through the helper so the eviction
// clock is controlled rather than wall-clock.
func TestClearDetachStateOnAttachEvictsStaleEntriesForOtherPairs(t *ltesting.T) {
	svc, _ := newDetachTestService("vol-a")

	// A different, long-abandoned pair - the frozen gauge series case.
	abandoned := lsinternal.BreakerKey{VolumeID: "vol-abandoned", NodeID: "ins-9"}
	at := ltime.Unix(0, 0)
	for i := 0; i < 3; i++ {
		svc.detachBreaker.Failure(abandoned, false, at)
		at = at.Add(6 * ltime.Minute)
	}
	if _, tracked, _ := svc.detachBreaker.Since(abandoned, at); !tracked {
		t.Fatal("setup: the abandoned pair should be tracked")
	}

	// Long past 2x the capped step, an attach lands on an unrelated pair.
	capStep := lsinternal.BreakerSteps[len(lsinternal.BreakerSteps)-1]
	later := at.Add(5 * capStep)
	svc.clearDetachStateOnAttach(lsinternal.BreakerKey{VolumeID: "vol-a", NodeID: "ins-1"}, later)

	if _, tracked, _ := svc.detachBreaker.Since(abandoned, later); tracked {
		t.Fatal("a stale entry for an unrelated pair survived the attach-path sweep")
	}
}
