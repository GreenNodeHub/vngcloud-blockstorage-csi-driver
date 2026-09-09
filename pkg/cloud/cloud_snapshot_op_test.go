package cloud

import (
	lctx "context"
	lerrors "errors"
	lfmt "fmt"
	ltesting "testing"
	ltime "time"

	lsdkClientV2 "github.com/vngcloud/vngcloud-go-sdk/v2/client"
	lsdkEntity "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/entity"
	lsdkGateway "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/gateway"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"
	lsdkVolumeSvc "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/services/volume"
	lsdkVolumeV2 "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/services/volume/v2"
)

// The snapshot create path was the last one still discarding its context: the
// csi-snapshotter sidecar is deployed without --timeout, so it gives up after
// its 15s default while waitSnapshotActive kept polling for up to five
// minutes - holding the inFlight key for snapshotName the whole time, so every
// retry got "Aborted: operation already exists" until the controller was
// restarted.

const testSnapshotName = "snapshot-3b1f0c42"

// snapshotServiceStub stands in for the SDK's volume service, faked the same
// way project_scope_test.go fakes the client: embed the SDK interface and
// implement only what the path under test calls, so anything else panics
// instead of quietly passing.
//
// It serves `items` back in whatever page and size the request asks for, and
// records every request, which is the only way to assert how many API calls a
// lookup costs without a live API.
type snapshotServiceStub struct {
	lsdkVolumeSvc.IVolumeServiceV2

	// items is the whole server-side list for the volume.
	items []*lsdkEntity.Snapshot

	// totalPages, when non-zero, is reported instead of the honest page count -
	// the "the server lies" case the iteration cap exists for. alwaysFull keeps
	// every page full so a short page never contradicts that lie.
	totalPages int
	alwaysFull bool

	// err, when set, fails every list call.
	err lsdkErrs.IError

	calls    int
	gotPages []int
	gotSizes []int
}

func (s *snapshotServiceStub) ListSnapshotsByBlockVolumeId(popts lsdkVolumeV2.IListSnapshotsByBlockVolumeIdRequest) (*lsdkEntity.ListSnapshots, lsdkErrs.IError) {
	// The request interface hides page and size; the concrete SDK type carries
	// them, and asserting on it is what makes "which pages did it ask for?"
	// observable.
	req, ok := popts.(*lsdkVolumeV2.ListSnapshotsByBlockVolumeIdRequest)
	if !ok {
		panic(lfmt.Sprintf("unexpected list-snapshots request type %T", popts))
	}

	s.calls++
	s.gotPages = append(s.gotPages, req.Page)
	s.gotSizes = append(s.gotSizes, req.Size)

	if s.err != nil {
		return nil, s.err
	}

	size := req.Size
	if size < 1 {
		size = 1
	}

	res := &lsdkEntity.ListSnapshots{
		Page:       req.Page,
		PageSize:   size,
		TotalItems: len(s.items),
		TotalPages: (len(s.items) + size - 1) / size,
	}

	switch {
	case s.alwaysFull:
		for i := 0; i < size; i++ {
			res.Items = append(res.Items, rawSnapshot(lfmt.Sprintf("filler-%d-%d", req.Page, i), SnapshotActiveStatus))
		}
	default:
		start := (req.Page - 1) * size
		if start < len(s.items) {
			end := start + size
			if end > len(s.items) {
				end = len(s.items)
			}
			res.Items = s.items[start:end]
		}
	}

	if s.totalPages > 0 {
		res.TotalPages = s.totalPages
	}

	return res, nil
}

func rawSnapshot(pname, pstatus string) *lsdkEntity.Snapshot {
	return &lsdkEntity.Snapshot{
		Id:       testSnapshotId,
		Name:     pname,
		Status:   pstatus,
		VolumeId: testVolumeId,
	}
}

// The rest of the chain: gateway -> V2 -> volume service. Each level embeds its
// SDK interface and overrides the single accessor the driver walks through.
type (
	fakeVServerGatewayV2 struct {
		lsdkGateway.IVServerGatewayV2
		volumeService lsdkVolumeSvc.IVolumeServiceV2
	}

	fakeVServerGateway struct {
		lsdkGateway.IVServerGateway
		v2 lsdkGateway.IVServerGatewayV2
	}

	fakeSdkClient struct {
		lsdkClientV2.IClient
		gateway lsdkGateway.IVServerGateway
	}
)

func (s *fakeVServerGatewayV2) VolumeService() lsdkVolumeSvc.IVolumeServiceV2 { return s.volumeService }
func (s *fakeVServerGateway) V2() lsdkGateway.IVServerGatewayV2               { return s.v2 }
func (s *fakeSdkClient) VServerGateway() lsdkGateway.IVServerGateway          { return s.gateway }

// projectClient scopes the client by calling WithProjectId, which the real SDK
// implements by mutating and returning itself.
func (s *fakeSdkClient) WithProjectId(_ string) lsdkClientV2.IClient { return s }

// newSnapshotStubbedCloud builds a Cloud whose project resolves without a
// network call and whose volume service is the stub.
func newSnapshotStubbedCloud(t *ltesting.T, pservice *snapshotServiceStub) *cloud {
	t.Helper()

	c := newStubbedCloud(t, &portalLookupStub{})
	c.baseClient = &fakeSdkClient{
		IClient: c.baseClient,
		gateway: &fakeVServerGateway{
			v2: &fakeVServerGatewayV2{volumeService: pservice},
		},
	}

	return c
}

// TestWaitSnapshotActiveHonoursACancelledContext is the defect itself. The
// wait must die with its caller: the snapshotter has already given up, and
// every extra second is a second the inFlight key for this snapshot name stays
// held.
//
// The stub fails every list call, so a wait that ignored the context would
// abort on the first poll rather than sleep - the assertion is that it never
// polls at all.
func TestWaitSnapshotActiveHonoursACancelledContext(t *ltesting.T) {
	svc := &snapshotServiceStub{
		err: new(lsdkErrs.SdkError).
			WithErrorCode(lsdkErrs.EcUnexpectedError).
			WithMessage(testDialError).
			WithErrors(lerrors.New(testDialError)),
	}
	c := newSnapshotStubbedCloud(t, svc)

	ctx, cancel := lctx.WithCancel(lctx.Background())
	cancel()

	start := ltime.Now()
	err := c.waitSnapshotActive(ctx, testVolumeId, testSnapshotName)
	elapsed := ltime.Since(start)

	if !lerrors.Is(err, lctx.Canceled) {
		t.Fatalf("waitSnapshotActive() = %v, want context.Canceled - the caller's cancellation is the only deadline this wait has", err)
	}
	if svc.calls != 0 {
		t.Fatalf("waitSnapshotActive() made %d list calls on an already-cancelled context, want 0", svc.calls)
	}
	// The old joat backoff (Revert=true) slept its ceiling FIRST and ran to a
	// 5-minute timeout; anything on that scale means the context is not bounding
	// the loop.
	if elapsed > ltime.Second {
		t.Fatalf("waitSnapshotActive() took %v on a cancelled context, want well under the old 5-minute bound", elapsed)
	}
}

// The happy path, so the assertions above cannot be satisfied by a wait that
// simply never polls.
func TestWaitSnapshotActiveReturnsAsSoonAsTheSnapshotIsActive(t *ltesting.T) {
	svc := &snapshotServiceStub{items: []*lsdkEntity.Snapshot{rawSnapshot(testSnapshotName, SnapshotActiveStatus)}}
	c := newSnapshotStubbedCloud(t, svc)

	if err := c.waitSnapshotActive(lctx.Background(), testVolumeId, testSnapshotName); err != nil {
		t.Fatalf("waitSnapshotActive() = %v, want nil for an ACTIVE snapshot", err)
	}
	if svc.calls != 1 {
		t.Fatalf("waitSnapshotActive() made %d list calls for an already-ACTIVE snapshot, want 1", svc.calls)
	}
}
