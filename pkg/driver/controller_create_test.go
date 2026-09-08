package driver

import (
	lctx "context"
	lerr "errors"
	lstr "strings"
	ltesting "testing"

	lcsi "github.com/container-storage-interface/spec/lib/go/csi"
	lsdkEntity "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/entity"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
	lsdkVolumeV2 "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/services/volume/v2"
	lcoreV1 "k8s.io/api/core/v1"
	lmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	lfake "k8s.io/client-go/kubernetes/fake"
	lk8srecord "k8s.io/client-go/tools/record"

	lscloud "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud"
	lsentity "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/entity"
	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
	lsinternal "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/driver/internal"
	lsk8s "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/k8s"
)

// CreateVolume makes four calls that can fail at the IaaS, and each of these
// tests fails exactly one of them. What is asserted is the Warning that lands
// on the PVC and, specifically, that its reason is the CLASSIFIED reason -
// operators alert on that string, and a generic one would tell them nothing
// about whether a human has to raise a quota.
//
// The metric half of the same report is deliberately not asserted from this
// package: lsmetrics.Recorder() is nil unless InitializeRecorder has run, and
// the registry it would hold is unexported, so there is nothing here to read a
// counter back from. The event is the assertable half.

const (
	createTestPvcNamespace = "default"
	createTestPvcName      = "pvc-classify"
	createTestVolumeName   = "pvc-11111111-2222-3333-4444-555555555555"
)

// unreachableIaaSError is the shape a blocked vServer egress produces: the
// SDK's catch-all code plus statusCode 0, meaning no response ever arrived.
func unreachableIaaSError() lserr.IError {
	return lserr.NewError(new(lsdkErrs.SdkError).
		WithErrorCode(lsdkErrs.EcUnexpectedError).
		WithMessage("could not reach vServer").
		WithErrors(lerr.New("context deadline exceeded")).
		WithKVparameters("statusCode", 0))
}

// volumeQuotaError is the other shape under test - a terminal one, so the two
// assertions together are about classification rather than about one constant.
func volumeQuotaError() lserr.IError {
	return lserr.NewError(new(lsdkErrs.SdkError).
		WithErrorCode(lsdkErrs.EcVServerVolumeExceedQuota).
		WithMessage("volume quota exhausted").
		WithErrors(lerr.New("quota exceeded")))
}

// createHappyCloud answers every IaaS call CreateVolume makes BEFORE the
// create itself. Each test embeds it and overrides the one call it wants to
// fail. The embedded nil lscloud.Cloud is deliberate: any other cloud call -
// EitherCreateResizeVolume included, which only the test that fails it
// defines - panics here instead of silently passing.
type createHappyCloud struct {
	lscloud.Cloud
}

func (createHappyCloud) GetListZones() (*lsentity.ListZones, lserr.IError) {
	return &lsentity.ListZones{ListZones: &lsdkEntity.ListZones{}}, nil
}

func (createHappyCloud) GetDefaultVolumeType() (*lsentity.VolumeType, lserr.IError) {
	return volumeTypeEntity(), nil
}

func (createHappyCloud) GetVolumeTypeIdByName(_, _ string) (string, lserr.IError) {
	return "vtype-1", nil
}

func (createHappyCloud) GetVolumeTypeById(_ string) (*lsentity.VolumeType, lserr.IError) {
	return volumeTypeEntity(), nil
}

// The create path treats "not found" as the normal answer here - it is how it
// learns the volume still has to be created.
func (createHappyCloud) GetVolumeByName(_ string) (*lsentity.Volume, lserr.IError) {
	return nil, lserr.NewError(new(lsdkErrs.SdkError).
		WithErrorCode(lsdkErrs.EcVServerVolumeNotFound).
		WithErrors(lerr.New("volume not found")))
}

func newCreateTestService(pcloud lscloud.Cloud) (*controllerService, chan string) {
	rec := lk8srecord.NewFakeRecorder(20)
	// PersistentVolumeClaimEventWarning resolves the PVC through the API
	// before recording, so an absent PVC would swallow every event under test.
	client := lfake.NewSimpleClientset(&lcoreV1.PersistentVolumeClaim{
		ObjectMeta: lmetav1.ObjectMeta{Namespace: createTestPvcNamespace, Name: createTestPvcName},
	})

	svc := &controllerService{
		cloud:         pcloud,
		inFlight:      lsinternal.NewInFlight(),
		createGate:    lsinternal.NewSemaphore(1),
		driverOptions: &DriverOptions{},
		k8sClient:     lsk8s.NewKubernetes(client, rec),
	}

	return svc, rec.Events
}

func volumeTypeEntity() *lsentity.VolumeType {
	return &lsentity.VolumeType{VolumeType: &lsdkEntity.VolumeType{Id: "vtype-1", MinSize: 20}}
}

// createRequest builds a request the validator accepts, carrying the PVC
// coordinates the report reads. prequiredBytes is a parameter because the
// too-small case is the one test that needs it below the fake type's MinSize.
func createRequest(prequiredBytes int64) *lcsi.CreateVolumeRequest {
	return &lcsi.CreateVolumeRequest{
		Name: createTestVolumeName,
		CapacityRange: &lcsi.CapacityRange{
			RequiredBytes: prequiredBytes,
		},
		VolumeCapabilities: []*lcsi.VolumeCapability{
			{
				AccessType: &lcsi.VolumeCapability_Mount{Mount: &lcsi.VolumeCapability_MountVolume{}},
				AccessMode: &lcsi.VolumeCapability_AccessMode{Mode: SingleNodeWriter},
			},
		},
		Parameters: map[string]string{
			PVCNamespaceKey: createTestPvcNamespace,
			PVCNameKey:      createTestPvcName,
		},
	}
}

const createTestGiB = int64(1024 * 1024 * 1024)

// drainEvents collects everything the fake recorder holds. The reason is the
// second field of the string the recorder formats, but Contains is enough
// here and keeps the assertion readable.
func drainEvents(pevents chan string) []string {
	var got []string
	for {
		select {
		case msg := <-pevents:
			got = append(got, msg)
		default:
			return got
		}
	}
}

func assertWarningWithReason(pt *ltesting.T, pevents chan string, preason string) {
	pt.Helper()

	got := drainEvents(pevents)
	for _, msg := range got {
		if lstr.Contains(msg, "Warning") && lstr.Contains(msg, preason) {
			return
		}
	}

	pt.Fatalf("no Warning event named the classified reason %q; events=%v", preason, got)
}

type createFailCloud struct {
	createHappyCloud
}

func (createFailCloud) EitherCreateResizeVolume(_ lsdkVolumeV2.ICreateBlockVolumeRequest) (*lsentity.Volume, lserr.IError) {
	return nil, volumeQuotaError()
}

// The one path that already reported before this change. It is here so the
// helper extraction is held to producing the same event it did inline.
func TestCreateVolumeReportsClassifiedReasonWhenCreateFails(t *ltesting.T) {
	if cls := lscloud.Classify(volumeQuotaError()); !cls.Terminal {
		t.Fatalf("fixture drifted: a volume quota error must classify terminal, got %+v", cls)
	}

	svc, events := newCreateTestService(createFailCloud{})

	if _, err := svc.CreateVolume(lctx.Background(), createRequest(20*createTestGiB)); err == nil {
		t.Fatal("CreateVolume() = nil error, want the quota failure")
	}

	assertWarningWithReason(t, events, lscloud.ReasonVolumeQuotaExceeded)
}

type listZonesFailCloud struct {
	createHappyCloud
}

func (listZonesFailCloud) GetListZones() (*lsentity.ListZones, lserr.IError) {
	return nil, unreachableIaaSError()
}

// The failure the dev cluster actually showed on 08/09/2026 once vServer
// egress was blocked: this is the first IaaS call on the create path.
func TestCreateVolumeReportsClassifiedReasonWhenListZonesFails(t *ltesting.T) {
	svc, events := newCreateTestService(listZonesFailCloud{})

	if _, err := svc.CreateVolume(lctx.Background(), createRequest(20*createTestGiB)); err == nil {
		t.Fatal("CreateVolume() = nil error, want the list-zones failure")
	}

	assertWarningWithReason(t, events, lscloud.ReasonIaaSUnreachable)
}

type volumeTypeFailCloud struct {
	createHappyCloud
}

func (volumeTypeFailCloud) GetVolumeTypeIdByName(_, _ string) (string, lserr.IError) {
	return "", unreachableIaaSError()
}

// getVolSizeBytes used to discard the IError with GetError(), which threw away
// the only thing the classifier can read.
func TestCreateVolumeReportsClassifiedReasonWhenVolumeTypeLookupFails(t *ltesting.T) {
	svc, events := newCreateTestService(volumeTypeFailCloud{})

	if _, err := svc.CreateVolume(lctx.Background(), createRequest(20*createTestGiB)); err == nil {
		t.Fatal("CreateVolume() = nil error, want the volume-type lookup failure")
	}

	assertWarningWithReason(t, events, lscloud.ReasonIaaSUnreachable)
}

type getVolumeByNameFailCloud struct {
	createHappyCloud
}

func (getVolumeByNameFailCloud) GetVolumeByName(_ string) (*lsentity.Volume, lserr.IError) {
	return nil, unreachableIaaSError()
}

func TestCreateVolumeReportsClassifiedReasonWhenGetVolumeByNameFails(t *ltesting.T) {
	svc, events := newCreateTestService(getVolumeByNameFailCloud{})

	if _, err := svc.CreateVolume(lctx.Background(), createRequest(20*createTestGiB)); err == nil {
		t.Fatal("CreateVolume() = nil error, want the get-by-name failure")
	}

	assertWarningWithReason(t, events, lscloud.ReasonIaaSUnreachable)
}

// A requested size below the volume type's minimum is a request-validation
// failure, not an IaaS one. Reporting it as an IaaS error would charge a user
// mistake to the operator's IaaS error budget and point the reason label at
// the wrong subsystem, so getVolSizeBytes must return that one unreported.
func TestCreateVolumeDoesNotReportTooSmallSizeAsAnIaaSError(t *ltesting.T) {
	svc, events := newCreateTestService(createHappyCloud{})

	if _, err := svc.CreateVolume(lctx.Background(), createRequest(1*createTestGiB)); err == nil {
		t.Fatal("CreateVolume() = nil error, want the too-small size failure")
	}

	for _, msg := range drainEvents(events) {
		for _, reason := range []string{
			lscloud.ReasonIaaSUnreachable,
			lscloud.ReasonIaaSUnknownError,
			lscloud.ReasonVolumeQuotaExceeded,
			lscloud.ReasonVolumeSizeQuotaExceeded,
		} {
			if lstr.Contains(msg, reason) {
				t.Fatalf("a too-small requested size produced an IaaS-classified event: %q", msg)
			}
		}
	}
}
