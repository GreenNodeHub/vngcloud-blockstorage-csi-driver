package k8s

import (
	lctx "context"
	ltesting "testing"

	lcoreV1 "k8s.io/api/core/v1"
	lmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	lfake "k8s.io/client-go/kubernetes/fake"
	lk8srecord "k8s.io/client-go/tools/record"
)

func pvWithHandle(pname, phandle string, pclaim *lcoreV1.ObjectReference) *lcoreV1.PersistentVolume {
	return &lcoreV1.PersistentVolume{
		ObjectMeta: lmetav1.ObjectMeta{Name: pname},
		Spec: lcoreV1.PersistentVolumeSpec{
			ClaimRef: pclaim,
			PersistentVolumeSource: lcoreV1.PersistentVolumeSource{
				CSI: &lcoreV1.CSIPersistentVolumeSource{
					Driver:       "bs.csi.vngcloud.vn",
					VolumeHandle: phandle,
				},
			},
		},
	}
}

func TestFindPersistentVolumeByHandle(t *ltesting.T) {
	client := lfake.NewSimpleClientset(
		pvWithHandle("pv-a", "vol-aaa", nil),
		pvWithHandle("pv-b", "vol-bbb", nil),
	)
	k := NewKubernetes(client, lk8srecord.NewFakeRecorder(10))

	pv, ierr := k.FindPersistentVolumeByHandle(lctx.Background(), "vol-bbb")
	if ierr != nil {
		t.Fatalf("FindPersistentVolumeByHandle() error = %v", ierr)
	}
	if pv == nil || pv.PersistentVolume.Name != "pv-b" {
		t.Fatalf("found %+v, want pv-b", pv)
	}
}

func TestFindPersistentVolumeByHandleMissingIsNotFatal(t *ltesting.T) {
	client := lfake.NewSimpleClientset(pvWithHandle("pv-a", "vol-aaa", nil))
	k := NewKubernetes(client, lk8srecord.NewFakeRecorder(10))

	pv, ierr := k.FindPersistentVolumeByHandle(lctx.Background(), "vol-zzz")
	if pv != nil {
		t.Fatalf("found %+v for an unknown handle, want nil", pv)
	}
	if ierr == nil {
		t.Fatal("want a not-found error so the caller can tell it apart from a hit")
	}
}

// The volume that stayed stuck for 23 hours had already lost its PVC: the PV
// was Released. So the PV is the reliable anchor and must always get the event.
func TestVolumeEventWarningOnPVOnlyWhenClaimGone(t *ltesting.T) {
	client := lfake.NewSimpleClientset(pvWithHandle("pv-a", "vol-aaa", nil))
	rec := lk8srecord.NewFakeRecorder(10)
	k := NewKubernetes(client, rec)

	k.VolumeEventWarning(lctx.Background(), "pv-a", "VolumeDetachStalled", "detach paused")

	select {
	case ev := <-rec.Events:
		if !containsAll(ev, "Warning", "VolumeDetachStalled", "detach paused") {
			t.Fatalf("event = %q, missing expected parts", ev)
		}
	default:
		t.Fatal("no event recorded for the PV")
	}

	select {
	case ev := <-rec.Events:
		t.Fatalf("a second event was recorded with no PVC to attach it to: %q", ev)
	default:
	}
}

func TestVolumeEventWarningAlsoOnPVCWhenPresent(t *ltesting.T) {
	claim := &lcoreV1.ObjectReference{Namespace: "app", Name: "data"}
	client := lfake.NewSimpleClientset(
		pvWithHandle("pv-a", "vol-aaa", claim),
		&lcoreV1.PersistentVolumeClaim{ObjectMeta: lmetav1.ObjectMeta{Namespace: "app", Name: "data"}},
	)
	rec := lk8srecord.NewFakeRecorder(10)
	k := NewKubernetes(client, rec)

	k.VolumeEventWarning(lctx.Background(), "pv-a", "VolumeDetachStalled", "detach paused")

	got := 0
	for i := 0; i < 2; i++ {
		select {
		case <-rec.Events:
			got++
		default:
		}
	}
	if got != 2 {
		t.Fatalf("recorded %d events, want 2 (PV and PVC)", got)
	}
}

// An event must never be able to break the operation that triggered it.
func TestVolumeEventWarningOnUnknownPVIsSilent(t *ltesting.T) {
	client := lfake.NewSimpleClientset()
	rec := lk8srecord.NewFakeRecorder(10)
	k := NewKubernetes(client, rec)

	k.VolumeEventWarning(lctx.Background(), "pv-missing", "VolumeDetachStalled", "detach paused")

	select {
	case ev := <-rec.Events:
		t.Fatalf("event recorded for a PV that does not exist: %q", ev)
	default:
	}
}

func containsAll(phaystack string, pneedles ...string) bool {
	for _, n := range pneedles {
		found := false
		for i := 0; i+len(n) <= len(phaystack); i++ {
			if phaystack[i:i+len(n)] == n {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}
