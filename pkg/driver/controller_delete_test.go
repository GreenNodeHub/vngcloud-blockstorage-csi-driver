package driver

import (
	lctx "context"
	lstrings "strings"
	ltesting "testing"

	lcsi "github.com/container-storage-interface/spec/lib/go/csi"
	lsdkEntity "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/entity"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"

	lscloud "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud"
	lsentity "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/entity"
	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
	lsinternal "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/driver/internal"
	lsmetrics "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/metrics"
)

// deleteFailingCloud lets DeleteVolume fail with a caller-supplied error while
// ListSnapshots answers empty, which is what the handler checks first.
type deleteFailingCloud struct {
	lscloud.Cloud
	err     lserr.IError
	listErr lserr.IError
}

func (s deleteFailingCloud) ListSnapshots(_ string, _, _ int) (*lsentity.ListSnapshots, lserr.IError) {
	if s.listErr != nil {
		return nil, s.listErr
	}

	// The embedded pointer must be non-nil: IsEmpty reads s.Items through it.
	return &lsentity.ListSnapshots{ListSnapshots: &lsdkEntity.ListSnapshots{}}, nil
}

func (s deleteFailingCloud) DeleteVolume(_ lctx.Context, _ string) lserr.IError {
	return s.err
}

// QC, 15/09/2026: a volume stuck DETACHING at vServer made every DeleteVolume
// time out. csi-provisioner retried 37 times, the PV sat in Released, and there
// was nothing to look at - no event on the PV, and no iaas_errors_total series
// for op="delete" at all, because the delete path never called Classify.
//
// The FR-1 behaviour underneath is correct and must stay: the driver refuses to
// report success, so the PV is NOT deleted while the IaaS still holds the
// volume. What was missing was any way to see WHY.
func TestDeleteVolumeReportsStalledIaaSWithItsOwnReason(t *ltesting.T) {
	const volumeID = "vol-a"
	svc, events := newDetachTestService(volumeID)
	svc.inFlight = lsinternal.NewInFlight()
	svc.cloud = deleteFailingCloud{
		// The shape the incident produced: the wait gave up, so the driver
		// builds this error with no SDK code behind it.
		err: lserr.ErrVolumeFailedToDelete(volumeID, nil),
	}

	_, err := svc.DeleteVolume(lctx.Background(), &lcsi.DeleteVolumeRequest{VolumeId: volumeID})
	if err == nil {
		t.Fatal("expected the delete to fail; reporting success here is the v1.4.0 bug")
	}

	msg := drainOneEvent(t, events)
	want := "Warning " + lscloud.ReasonIaaSOperationStalled + " "
	if !lstrings.HasPrefix(msg, want) {
		t.Errorf("event = %q, want it to start with %q", msg, want)
	}
}

// A delete that fails for a DIFFERENT reason must say so. One generic string on
// every delete failure would leave the operator exactly where the incident left
// them.
func TestDeleteVolumeDistinguishesReasons(t *ltesting.T) {
	const volumeID = "vol-a"
	svc, events := newDetachTestService(volumeID)
	svc.inFlight = lsinternal.NewInFlight()
	svc.cloud = deleteFailingCloud{
		err: lserr.NewError(new(lsdkErrs.SdkError).
			WithErrorCode(lsdkErrs.EcUnexpectedError).
			WithKVparameters("statusCode", 500).
			WithMessage("boom")),
	}

	if _, err := svc.DeleteVolume(lctx.Background(), &lcsi.DeleteVolumeRequest{VolumeId: volumeID}); err == nil {
		t.Fatal("expected the delete to fail")
	}

	msg := drainOneEvent(t, events)
	want := "Warning " + lscloud.ReasonIaaSServerError + " "
	if !lstrings.HasPrefix(msg, want) {
		t.Errorf("event = %q, want it to start with %q", msg, want)
	}
}

// The inflight entry must be released even when the delete fails. If it were
// not, the 37 provisioner retries would each return "already in-flight" and the
// only recovery would be a controller restart - the exact wedge this programme
// removed from the other paths.
func TestDeleteVolumeReleasesInflightOnFailure(t *ltesting.T) {
	const volumeID = "vol-a"
	svc, _ := newDetachTestService(volumeID)
	svc.inFlight = lsinternal.NewInFlight()
	svc.cloud = deleteFailingCloud{err: lserr.ErrVolumeFailedToDelete(volumeID, nil)}

	req := &lcsi.DeleteVolumeRequest{VolumeId: volumeID}
	if _, err := svc.DeleteVolume(lctx.Background(), req); err == nil {
		t.Fatal("expected the delete to fail")
	}

	// A second call must get as far as the cloud again, not be refused by the
	// inflight map.
	if _, err := svc.DeleteVolume(lctx.Background(), req); err == nil {
		t.Fatal("expected the second delete to fail too")
	} else if lstrings.Contains(err.Error(), "already exists") {
		t.Errorf("second call was refused by the inflight map: %v", err)
	}
}

// The QC report asked for iaas_errors_total{op="delete"} by name, so assert the
// LABEL, not just the event. Checking the event alone let a mutation that
// stamped op="detach" pass unnoticed.
func TestDeleteVolumeCountsUnderItsOwnOpLabel(t *ltesting.T) {
	const volumeID = "vol-a"
	lsmetrics.InitializeRecorder()

	svc, _ := newDetachTestService(volumeID)
	svc.inFlight = lsinternal.NewInFlight()
	svc.cloud = deleteFailingCloud{err: lserr.ErrVolumeFailedToDelete(volumeID, nil)}

	labels := map[string]string{
		lsmetrics.LabelOp:     lsmetrics.OpDelete,
		lsmetrics.LabelReason: lscloud.ReasonIaaSOperationStalled,
	}
	before := 0.0
	if m := findSample(t, lsmetrics.IaaSErrors, labels); m != nil {
		before = m.GetCounter().GetValue()
	}

	if _, err := svc.DeleteVolume(lctx.Background(), &lcsi.DeleteVolumeRequest{VolumeId: volumeID}); err == nil {
		t.Fatal("expected the delete to fail")
	}

	m := findSample(t, lsmetrics.IaaSErrors, labels)
	if m == nil {
		t.Fatalf("no %s series for op=delete reason=%s", lsmetrics.IaaSErrors, lscloud.ReasonIaaSOperationStalled)
	}
	if got := m.GetCounter().GetValue(); got != before+1 {
		t.Errorf("counter = %v, want %v", got, before+1)
	}
}

// ListSnapshots is the FIRST IaaS call DeleteVolume makes, so during an IaaS
// outage it is the one that fails - the delete below is never reached. Measured
// on the dev cluster on 16/09/2026: with vServer blocked, every DeleteVolume
// died here, and the first version of this reporting left that branch silent,
// reproducing the very blind spot it was meant to close.
func TestDeleteVolumeReportsAFailedSnapshotListing(t *ltesting.T) {
	const volumeID = "vol-a"
	lsmetrics.InitializeRecorder()

	svc, events := newDetachTestService(volumeID)
	svc.inFlight = lsinternal.NewInFlight()
	svc.cloud = deleteFailingCloud{
		listErr: lserr.NewError(new(lsdkErrs.SdkError).
			WithErrorCode(lsdkErrs.EcUnexpectedError).
			WithMessage("dial tcp: i/o timeout")),
	}

	labels := map[string]string{
		lsmetrics.LabelOp:     lsmetrics.OpDelete,
		lsmetrics.LabelReason: lscloud.ReasonIaaSUnreachable,
	}
	before := 0.0
	if m := findSample(t, lsmetrics.IaaSErrors, labels); m != nil {
		before = m.GetCounter().GetValue()
	}

	if _, err := svc.DeleteVolume(lctx.Background(), &lcsi.DeleteVolumeRequest{VolumeId: volumeID}); err == nil {
		t.Fatal("expected the delete to fail at the snapshot listing")
	}

	msg := drainOneEvent(t, events)
	want := "Warning " + lscloud.ReasonIaaSUnreachable + " "
	if !lstrings.HasPrefix(msg, want) {
		t.Errorf("event = %q, want it to start with %q", msg, want)
	}

	m := findSample(t, lsmetrics.IaaSErrors, labels)
	if m == nil || m.GetCounter().GetValue() != before+1 {
		t.Errorf("iaas_errors_total{op=delete,reason=IaaSUnreachable} did not move")
	}
}
