package cloud

import (
	lctx "context"
	lerrors "errors"
	lstr "strings"
	lsync "sync"
	lsync_atomic "sync/atomic"
	ltesting "testing"
	ltime "time"

	lsdkClientV2 "github.com/vngcloud/vngcloud-go-sdk/v2/client"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
	lsdkVolumeV2 "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/services/volume/v2"

	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
)

// The controller used to resolve its project against vServer inside NewCloud
// and panic on failure. On 07/09/2026 vServer egress was blocked on the dev
// cluster and the csi-controller went CrashLoopBackOff: no sidecar could reach
// the socket, not one ControllerUnpublishVolume ran, and the detach
// circuit-breaker and error events built for exactly that outage never got to
// execute. These tests pin the resolution to first use instead of startup.

const (
	// The instance-metadata project id, and the portal `pro-...` id it maps to.
	// They are deliberately different: that mapping is the only product of the
	// portal call.
	testUnderProjectId  = "9d5c9a3e-a1d1-4f21-b58a-4e6a6bd12a2f"
	testPortalProjectId = "pro-65e2d7ca-6a4f-4a92-9c25-9a3b58d0f7c1"

	// A transport failure carries no HTTP status and says nothing about a
	// project - so an assertion that the driver's own wording survives cannot
	// pass by accident.
	testDialError = "dial tcp 10.166.12.196:443: connect: connection refused"

	testSnapshotId = "snap-7c3a1d90-0000-0000-0000-000000000000"
	testVolumeId   = "vol-8a1f7e28-b36b-4d72-b3c5-de4a7b944654"
	testVolumeType = "vtype-33333333-0000-0000-0000-000000000000"
	testZoneId     = "HCM03-1B"

	// Any endpoint at all: a test that reaches the network is a test that has
	// already failed.
	testUnreachableURL = "http://vserver.invalid"
)

type fakeMetadataService struct{}

func (s fakeMetadataService) GetInstanceID() string {
	return "ins-11111111-0000-0000-0000-000000000000"
}
func (s fakeMetadataService) GetProjectID() string        { return testUnderProjectId }
func (s fakeMetadataService) GetAvailabilityZone() string { return testZoneId }

// portalLookupStub stands in for the portal call: it counts its invocations,
// can fail the first pfailFor of them, and can be held open to line concurrent
// callers up on a cold resolver.
type portalLookupStub struct {
	calls   lsync_atomic.Int32
	failFor int32
	release chan struct{}

	gotUnderProjectId lsync_atomic.Value
}

func (s *portalLookupStub) lookup(_ lsdkClientV2.IClient, punderProjectId string) (string, lserr.IError) {
	n := s.calls.Add(1)
	s.gotUnderProjectId.Store(punderProjectId)

	if s.release != nil {
		<-s.release
	}

	if n <= s.failFor {
		return "", lserr.NewError(new(lsdkErrs.SdkError).
			WithErrorCode(lsdkErrs.EcUnexpectedError).
			WithMessage(testDialError).
			WithErrors(lerrors.New(testDialError)))
	}

	return testPortalProjectId, nil
}

// spyClient records the project id the resolver stamps on the SDK client.
// Only WithProjectId is implemented; nothing else is called on this path.
type spyClient struct {
	lsdkClientV2.IClient
	gotProjectId string
}

func (s *spyClient) WithProjectId(pprojectId string) lsdkClientV2.IClient {
	s.gotProjectId = pprojectId
	return s
}

// newStubbedCloud builds a Cloud the way the driver does, with the portal
// lookup redirected at the stub.
func newStubbedCloud(t *ltesting.T, pstub *portalLookupStub) *cloud {
	t.Helper()

	prev := DefaultPortalLookup
	DefaultPortalLookup = pstub.lookup
	t.Cleanup(func() { DefaultPortalLookup = prev })

	c, err := NewCloud(testUnreachableURL, testUnreachableURL, "client-id", "client-secret", fakeMetadataService{})
	if err != nil {
		t.Fatalf("NewCloud() returned error %v, want nil - an unreachable IaaS must not stop the driver starting", err)
	}
	if c == nil {
		t.Fatal("NewCloud() returned a nil Cloud")
	}

	return c.(*cloud)
}

// ierrOf converts an lserr.IError the way the driver's own `error`-returning
// methods do. A wrapper that forgot WithErrors() has a nil GetError(), and
// every call site written as `return ierr.GetError()` then reports SUCCESS.
func ierrOf(pierr lserr.IError) error {
	if pierr == nil {
		return nil
	}

	return pierr.GetError()
}

func assertUnresolvedProject(t *ltesting.T, perr error) {
	t.Helper()

	if perr == nil {
		t.Fatal("returned nil error - a project that could not be resolved must never read as success")
	}
	if !lstr.Contains(perr.Error(), errProjectUnresolvedText) {
		t.Fatalf("error %q does not say the project could not be resolved; an operator cannot tell the IaaS is unreachable", perr)
	}
}

// TestNewCloudStartsWhenTheIaaSIsUnreachable is the defect itself: NewCloud
// must not touch vServer, and the first call afterwards must fail with a clear
// error rather than the process never starting.
func TestNewCloudStartsWhenTheIaaSIsUnreachable(t *ltesting.T) {
	stub := &portalLookupStub{failFor: 1 << 30}
	c := newStubbedCloud(t, stub)

	if got := stub.calls.Load(); got != 0 {
		t.Fatalf("NewCloud ran the portal lookup %d times, want 0 - startup must not depend on vServer", got)
	}

	assertUnresolvedProject(t, c.DeleteSnapshot(testSnapshotId))

	if got := stub.calls.Load(); got != 1 {
		t.Fatalf("the first call ran the portal lookup %d times, want 1", got)
	}
	if got, _ := stub.gotUnderProjectId.Load().(string); got != testUnderProjectId {
		t.Fatalf("the portal lookup got under project id %q, want the instance-metadata id %q", got, testUnderProjectId)
	}
}

// TestEveryCloudMethodSurfacesAnUnresolvedProject covers the whole Cloud
// surface: each method needs the project-scoped client, so each one must report
// the resolver's failure in its own return type - including the ones returning
// plain `error` through GetError(), where a wrapper without WithErrors() would
// turn the outage into a silent success.
func TestEveryCloudMethodSurfacesAnUnresolvedProject(t *ltesting.T) {
	ctx := lctx.TODO()

	tcs := []struct {
		name string
		call func(pc Cloud) error
	}{
		{"EitherCreateResizeVolume", func(pc Cloud) error {
			_, ierr := pc.EitherCreateResizeVolume(lsdkVolumeV2.NewCreateBlockVolumeRequest("pvc-1", testVolumeType, 20))
			return ierrOf(ierr)
		}},
		{"GetVolumeByName", func(pc Cloud) error {
			_, ierr := pc.GetVolumeByName("pvc-1")
			return ierrOf(ierr)
		}},
		{"GetVolume", func(pc Cloud) error {
			_, ierr := pc.GetVolume(testVolumeId)
			return ierrOf(ierr)
		}},
		{"DeleteVolume", func(pc Cloud) error { return ierrOf(pc.DeleteVolume(ctx, testVolumeId)) }},
		{"AttachVolume", func(pc Cloud) error {
			_, ierr := pc.AttachVolume(ctx, testInstanceA, testVolumeId)
			return ierrOf(ierr)
		}},
		{"DetachVolume", func(pc Cloud) error {
			return ierrOf(pc.DetachVolume(ctx, testInstanceA, testVolumeId))
		}},
		{"IsDetachedFrom", func(pc Cloud) error {
			_, ierr := pc.IsDetachedFrom(ctx, testInstanceA, testVolumeId)
			return ierrOf(ierr)
		}},
		{"ModifyVolumeType", func(pc Cloud) error {
			return ierrOf(pc.ModifyVolumeType(ctx, testVolumeId, testVolumeType, 20))
		}},
		{"ResizeOrModifyDisk", func(pc Cloud) error {
			_, err := pc.ResizeOrModifyDisk(ctx, testVolumeId, 20<<30, &ModifyDiskOptions{VolumeType: testVolumeType})
			return err
		}},
		{"ExpandVolume", func(pc Cloud) error { return pc.ExpandVolume(ctx, testVolumeId, testVolumeType, 20) }},
		{"GetDeviceDiskID", func(pc Cloud) error {
			_, err := pc.GetDeviceDiskID(testVolumeId)
			return err
		}},
		{"GetVolumeSnapshotByName", func(pc Cloud) error {
			_, err := pc.GetVolumeSnapshotByName(ctx, testVolumeId, "snapshot-1")
			return err
		}},
		{"CreateSnapshotFromVolume", func(pc Cloud) error {
			_, err := pc.CreateSnapshotFromVolume(ctx, "k8s-test", testVolumeId, "snapshot-1")
			return err
		}},
		{"DeleteSnapshot", func(pc Cloud) error { return pc.DeleteSnapshot(testSnapshotId) }},
		{"ListSnapshots", func(pc Cloud) error {
			_, ierr := pc.ListSnapshots(testVolumeId, 1, 10)
			return ierrOf(ierr)
		}},
		{"GetVolumeTypeById", func(pc Cloud) error {
			_, ierr := pc.GetVolumeTypeById(testVolumeType)
			return ierrOf(ierr)
		}},
		{"GetDefaultVolumeType", func(pc Cloud) error {
			_, ierr := pc.GetDefaultVolumeType()
			return ierrOf(ierr)
		}},
		{"GetVolumeTypeIdByName", func(pc Cloud) error {
			_, ierr := pc.GetVolumeTypeIdByName(testZoneId, "nvme-iops5000")
			return ierrOf(ierr)
		}},
		{"GetListZones", func(pc Cloud) error {
			_, ierr := pc.GetListZones()
			return ierrOf(ierr)
		}},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *ltesting.T) {
			stub := &portalLookupStub{failFor: 1 << 30}
			c := newStubbedCloud(t, stub)

			assertUnresolvedProject(t, tc.call(c))

			if got := stub.calls.Load(); got < 1 {
				t.Fatalf("%s never tried to resolve the project", tc.name)
			}
		})
	}
}

// A cached failure would outlive the outage: the resolver is consulted by every
// call, so one unlucky lookup must not wedge the controller for its lifetime.
func TestProjectScopeRecoversAfterAFailedLookup(t *ltesting.T) {
	stub := &portalLookupStub{failFor: 1}
	c := newStubbedCloud(t, stub)

	if _, ierr := c.projectClient(); ierr == nil {
		t.Fatal("projectClient() returned nil error while the portal lookup was failing")
	}

	client, ierr := c.projectClient()
	if ierr != nil {
		t.Fatalf("projectClient() = %v after the portal lookup recovered, want nil", ierr)
	}
	if client == nil {
		t.Fatal("projectClient() returned a nil client and a nil error")
	}
	if got := stub.calls.Load(); got != 2 {
		t.Fatalf("the portal lookup ran %d times, want 2 - once failing, once recovered", got)
	}
}

// At worker-threads=100 the resolver is on every RPC. Resolving once is the
// difference between one portal call per process and one per volume operation.
func TestProjectScopeResolvesOnceAcrossSequentialCalls(t *ltesting.T) {
	stub := &portalLookupStub{}
	c := newStubbedCloud(t, stub)

	var first lsdkClientV2.IClient
	for i := 0; i < 20; i++ {
		client, ierr := c.projectClient()
		if ierr != nil {
			t.Fatalf("projectClient() call %d returned %v", i, ierr)
		}
		if i == 0 {
			first = client
		} else if client != first {
			t.Fatalf("projectClient() call %d returned a different client", i)
		}
	}

	if got := stub.calls.Load(); got != 1 {
		t.Fatalf("20 calls ran the portal lookup %d times, want 1", got)
	}
}

// The production shape of first use: a cold resolver hit by every worker at
// once, right after a restart. Run under -race.
func TestProjectScopeResolvesOnceUnderConcurrentFirstUse(t *ltesting.T) {
	stub := &portalLookupStub{release: make(chan struct{})}
	c := newStubbedCloud(t, stub)

	var wg lsync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if client, ierr := c.projectClient(); ierr != nil || client == nil {
				t.Errorf("projectClient() = %v, %v; want a client and no error", client, ierr)
			}
		}()
	}

	// Let the goroutines pile up on the cold resolver before it answers.
	ltime.Sleep(50 * ltime.Millisecond)
	close(stub.release)
	wg.Wait()

	if got := stub.calls.Load(); got != 1 {
		t.Fatalf("100 concurrent first uses ran the portal lookup %d times, want 1", got)
	}
}

// A resolved project id must never expire. get() re-runs the loader once the
// expiry has passed and returns its error, so a lapsing TTL would fail every
// call during an outage even though the id was already known - and a cluster's
// project id does not change, so there is nothing to refresh.
func TestProjectScopeNeverReresolvesAfterAnExpiryWouldHaveLapsed(t *ltesting.T) {
	stub := &portalLookupStub{}
	c := newStubbedCloud(t, stub)

	if _, ierr := c.projectClient(); ierr != nil {
		t.Fatalf("projectClient() = %v, want nil", ierr)
	}

	// Wind the clock past any expiry by construction instead of sleeping.
	c.projectClientCache.expires = ltime.Now().Add(-ltime.Hour)

	if _, ierr := c.projectClient(); ierr != nil {
		t.Fatalf("projectClient() after the expiry lapsed = %v, want nil", ierr)
	}
	if got := stub.calls.Load(); got != 1 {
		t.Fatalf("the portal lookup ran %d times, want 1 - a resolved project id must never expire", got)
	}
}

// The portal call's only product is the `pro-...` id that every vServer URL
// path embeds, so the resolved client has to carry it.
func TestProjectClientAppliesTheResolvedProjectId(t *ltesting.T) {
	stub := &portalLookupStub{}
	c := newStubbedCloud(t, stub)

	spy := &spyClient{IClient: c.baseClient}
	c.baseClient = spy

	client, ierr := c.projectClient()
	if ierr != nil {
		t.Fatalf("projectClient() = %v, want nil", ierr)
	}
	if spy.gotProjectId != testPortalProjectId {
		t.Fatalf("the client was scoped to %q, want the resolved portal id %q", spy.gotProjectId, testPortalProjectId)
	}
	if client != lsdkClientV2.IClient(spy) {
		t.Fatal("projectClient() returned a client other than the scoped one")
	}
}

// During the incident every event and both metrics read reason=IaaSUnknownError.
// The resolver failure is the one error an operator sees first in an outage, so
// it must classify as unreachable and stay non-terminal.
func TestProjectUnresolvedErrorClassifiesAsIaaSUnreachable(t *ltesting.T) {
	sdkErr := lserr.NewError(new(lsdkErrs.SdkError).
		WithErrorCode(lsdkErrs.EcUnexpectedError).
		WithMessage(testDialError).
		WithErrors(lerrors.New(testDialError)))

	got := Classify(errProjectUnresolved(testUnderProjectId, sdkErr))
	if got.Reason != ReasonIaaSUnreachable {
		t.Fatalf("Classify() reason = %q, want %q", got.Reason, ReasonIaaSUnreachable)
	}
	if got.Terminal {
		t.Fatal("Classify() reported terminal; an unreachable IaaS recovers on its own")
	}
}
