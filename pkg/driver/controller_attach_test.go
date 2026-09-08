package driver

import (
	lctx "context"
	lstrings "strings"
	ltesting "testing"

	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"

	lscloud "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud"
	lsentity "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/entity"
	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
	lsinternal "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/driver/internal"
)

// attachFailingCloud fails AttachVolume with a caller-supplied error. The
// embedded nil interface is deliberate: any other cloud call this handler might
// grow panics here rather than silently passing.
type attachFailingCloud struct {
	lscloud.Cloud
	err lserr.IError
}

func (s attachFailingCloud) AttachVolume(_ lctx.Context, _, _ string) (*lsentity.Volume, lserr.IError) {
	return nil, s.err
}

// F9, measured on the dev cluster: the driver reported CSINode
// allocatable.count = 10, the scheduler saw 8 in use and placed another pod, and
// the IaaS refused the attach because an orphaned volume already held the last
// slot. The pod then sat Pending forever retrying FailedAttachVolume while the
// scheduler never said "no capacity".
//
// The reason the driver could not help diagnose that is this: Classify already
// maps EcVServerServerVolumeAttachQuotaExceeded to ReasonVolumeAttachQuotaExceeded,
// but ControllerPublishVolume returned a bare ErrAttachVolume and classified
// nothing - so "this node is full" was indistinguishable from any other attach
// failure, in the events and in the metrics alike.
//
// Upstream aws-ebs-csi-driver deliberately does no capacity pre-check either; it
// relies on reporting the condition distinctly. This test pins the reporting.
func TestControllerPublishVolumeReportsAttachQuotaExceeded(t *ltesting.T) {
	const volumeID, nodeID = "vol-a", "ins-1"
	svc, events := newDetachTestService(volumeID)
	svc.inFlight = lsinternal.NewInFlight()
	svc.cloud = attachFailingCloud{
		err: lserr.NewError(new(lsdkErrs.SdkError).
			WithErrorCode(lsdkErrs.EcVServerServerVolumeAttachQuotaExceeded).
			WithMessage("the server has reached its volume attach limit")),
	}

	_, err := svc.ControllerPublishVolume(lctx.Background(), publishRequest(volumeID, nodeID))
	if err == nil {
		t.Fatal("expected the attach to fail")
	}

	msg := drainOneEvent(t, events)
	want := "Warning " + lscloud.ReasonVolumeAttachQuotaExceeded + " "
	if !lstrings.HasPrefix(msg, want) {
		t.Errorf("event = %q, want it to start with %q", msg, want)
	}
}

// A transient attach failure must still be reported, and with ITS OWN reason -
// otherwise the operator cannot tell a full node from an unreachable IaaS, which
// is the whole point of classifying rather than emitting one generic string.
func TestControllerPublishVolumeReportsUnreachableIaaSDistinctly(t *ltesting.T) {
	const volumeID, nodeID = "vol-a", "ins-1"
	svc, events := newDetachTestService(volumeID)
	svc.inFlight = lsinternal.NewInFlight()
	svc.cloud = attachFailingCloud{
		err: lserr.NewError(new(lsdkErrs.SdkError).
			WithErrorCode(lsdkErrs.EcUnexpectedError).
			WithMessage("timeout").
			WithKVparameters("statusCode", 0)),
	}

	if _, err := svc.ControllerPublishVolume(lctx.Background(), publishRequest(volumeID, nodeID)); err == nil {
		t.Fatal("expected the attach to fail")
	}

	msg := drainOneEvent(t, events)
	want := "Warning " + lscloud.ReasonIaaSUnreachable + " "
	if !lstrings.HasPrefix(msg, want) {
		t.Errorf("event = %q, want it to start with %q", msg, want)
	}
	if lstrings.Contains(msg, lscloud.ReasonVolumeAttachQuotaExceeded) {
		t.Errorf("event = %q, must not claim the node is full", msg)
	}
}

// drainOneEvent returns the one event the fake recorder holds. FakeRecorder
// formats an event as "<Type> <Reason> <Message>", so asserting on a PREFIX is
// what pins the reason FIELD. A plain Contains check does not: the first
// version of these tests passed even with a hard-coded generic reason, because
// the message body happened to repeat the classification too.
func drainOneEvent(t *ltesting.T, pevents chan string) string {
	t.Helper()

	select {
	case msg := <-pevents:
		return msg
	default:
		t.Fatal("no event emitted for a failed attach")
	}

	return ""
}
