package cloud

import (
	lctx "context"
	lerrors "errors"
	lfmt "fmt"
	ltesting "testing"
	ltime "time"

	lslices "slices"

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

	// onCall runs after each call is recorded, so a test can cancel the
	// context between two pages.
	onCall func(pcall int)

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

	if s.onCall != nil {
		s.onCall(s.calls)
	}

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

// snapshotListing builds a server-side list of pcount snapshots with the one
// under test at ptargetIdx (-1 for a list that does not contain it).
func snapshotListing(pcount, ptargetIdx int) []*lsdkEntity.Snapshot {
	items := make([]*lsdkEntity.Snapshot, pcount)
	for i := range items {
		items[i] = rawSnapshot(lfmt.Sprintf("other-%d", i), SnapshotActiveStatus)
	}

	if ptargetIdx >= 0 {
		items[ptargetIdx] = rawSnapshot(testSnapshotName, SnapshotActiveStatus)
	}

	return items
}

// TestGetVolumeSnapshotByNamePaginatesToFindASnapshotOnALaterPage is the latent
// bug Part 1 would otherwise have set off. This lookup asked for page 1 of ten
// and ignored the rest, so on a volume with more than ten snapshots it reported
// "not found" for a snapshot that exists - and it is the idempotency check on
// the create path, so every retry created ANOTHER snapshot against a shared
// project quota.
func TestGetVolumeSnapshotByNamePaginatesToFindASnapshotOnALaterPage(t *ltesting.T) {
	// Three pages' worth, with the snapshot on the third.
	svc := &snapshotServiceStub{items: snapshotListing(2*snapshotListPageSize+1, 2*snapshotListPageSize)}
	c := newSnapshotStubbedCloud(t, svc)

	snap, err := c.GetVolumeSnapshotByName(lctx.Background(), testVolumeId, testSnapshotName)
	if err != nil {
		t.Fatalf("GetVolumeSnapshotByName() = %v, want the snapshot that sits on page 3", err)
	}
	if snap == nil || snap.Name != testSnapshotName {
		t.Fatalf("GetVolumeSnapshotByName() returned %+v, want the snapshot named %q", snap, testSnapshotName)
	}

	if want := []int{1, 2, 3}; !lslices.Equal(svc.gotPages, want) {
		t.Fatalf("asked for pages %v, want %v", svc.gotPages, want)
	}

	// The per-project API quota bucket is shared with every other consumer and
	// this driver has already been the thing that drained it, so the common case
	// (a volume with a handful of snapshots) has to cost ONE call.
	for i, size := range svc.gotSizes {
		if size <= 10 {
			t.Fatalf("page %d asked for size %d; a page of ten costs a call per ten snapshots", i+1, size)
		}
	}
}

// Two stops that both have to hold, or the loop either misses pages or burns
// calls it does not need.
func TestGetVolumeSnapshotByNameStopsAtTotalPagesAndStopsWhenFound(t *ltesting.T) {
	// Every page full, so the reported page count is the ONLY thing that can
	// end the walk.
	t.Run("stops at the page count the server reports", func(t *ltesting.T) {
		svc := &snapshotServiceStub{totalPages: 3, alwaysFull: true}
		c := newSnapshotStubbedCloud(t, svc)

		_, err := c.GetVolumeSnapshotByName(lctx.Background(), testVolumeId, testSnapshotName)
		if !lerrors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("GetVolumeSnapshotByName() = %v, want ErrSnapshotNotFound", err)
		}
		if svc.calls != 3 {
			t.Fatalf("made %d list calls for a listing the server says has 3 pages, want exactly 3", svc.calls)
		}
	})

	// The mirror case: an honest listing of 3 pages behind a TotalPages of 5.
	// A page shorter than asked for is the end of the data whatever the count
	// says, so the walk must not spend two calls on pages that do not exist.
	t.Run("stops at a short page even when the server claims more", func(t *ltesting.T) {
		svc := &snapshotServiceStub{items: snapshotListing(2*snapshotListPageSize+1, -1), totalPages: 5}
		c := newSnapshotStubbedCloud(t, svc)

		_, err := c.GetVolumeSnapshotByName(lctx.Background(), testVolumeId, testSnapshotName)
		if !lerrors.Is(err, ErrSnapshotNotFound) {
			t.Fatalf("GetVolumeSnapshotByName() = %v, want ErrSnapshotNotFound", err)
		}
		if svc.calls != 3 {
			t.Fatalf("made %d list calls for 3 pages of data, want exactly 3", svc.calls)
		}
	})

	t.Run("a snapshot on page 1 costs one call", func(t *ltesting.T) {
		svc := &snapshotServiceStub{items: snapshotListing(2*snapshotListPageSize+1, 0)}
		c := newSnapshotStubbedCloud(t, svc)

		if _, err := c.GetVolumeSnapshotByName(lctx.Background(), testVolumeId, testSnapshotName); err != nil {
			t.Fatalf("GetVolumeSnapshotByName() = %v, want the snapshot on page 1", err)
		}
		if svc.calls != 1 {
			t.Fatalf("made %d list calls for a snapshot on page 1, want 1 - the walk must stop on a match", svc.calls)
		}
	})
}

// TotalPages comes from the server, so it cannot be the only thing that ends
// the loop: a wrong or absurd value would otherwise page forever, on the same
// quota bucket this change exists to protect.
func TestGetVolumeSnapshotByNameCapsIterationsWhenTheServerReportsAbsurdTotalPages(t *ltesting.T) {
	svc := &snapshotServiceStub{
		items:      snapshotListing(snapshotListPageSize, -1),
		totalPages: 1 << 20,
		alwaysFull: true,
	}
	c := newSnapshotStubbedCloud(t, svc)

	_, err := c.GetVolumeSnapshotByName(lctx.Background(), testVolumeId, testSnapshotName)
	if !lerrors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("GetVolumeSnapshotByName() = %v, want ErrSnapshotNotFound", err)
	}
	if svc.calls != snapshotListMaxPages {
		t.Fatalf("made %d list calls against a server claiming %d pages, want the cap of %d",
			svc.calls, svc.totalPages, snapshotListMaxPages)
	}
	// A literal bound as well as the constant: the assertion above moves with
	// snapshotListMaxPages, so on its own it says nothing about how big the cap
	// is. 64 calls is already far more than any real volume's snapshot count
	// needs, on a quota bucket shared with the whole project.
	if svc.calls > 64 {
		t.Fatalf("the cap let the walk make %d list calls; that is not a bound worth having", svc.calls)
	}
}

// The walk is on the create path, whose caller gives up after 15s. Part 1
// bounds the wait by the context; the pages this lookup fetches inside that
// wait have to be bounded by it too, or the RPC still outlives its client.
func TestGetVolumeSnapshotByNameStopsPagingWhenTheContextIsCancelled(t *ltesting.T) {
	ctx, cancel := lctx.WithCancel(lctx.Background())
	defer cancel()

	svc := &snapshotServiceStub{items: snapshotListing(2*snapshotListPageSize+1, 2*snapshotListPageSize)}
	// The caller gives up while page 1 is in flight.
	svc.onCall = func(pcall int) {
		if pcall == 1 {
			cancel()
		}
	}
	c := newSnapshotStubbedCloud(t, svc)

	_, err := c.GetVolumeSnapshotByName(ctx, testVolumeId, testSnapshotName)
	if !lerrors.Is(err, lctx.Canceled) {
		t.Fatalf("GetVolumeSnapshotByName() = %v, want context.Canceled", err)
	}
	if svc.calls != 1 {
		t.Fatalf("made %d list calls after the context was cancelled, want 1", svc.calls)
	}
}
