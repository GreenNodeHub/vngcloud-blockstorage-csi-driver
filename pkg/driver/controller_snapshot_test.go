package driver

import (
	lctx "context"
	lerrors "errors"
	lstr "strings"
	lsync "sync"
	ltesting "testing"
	ltime "time"

	lcsi "github.com/container-storage-interface/spec/lib/go/csi"

	lscloud "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud"
	lsentity "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/entity"
	lsinternal "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/driver/internal"
)

const (
	testSnapshotName = "snapshot-3b1f0c42"
	testSnapshotVol  = "vol-8a1f7e28-b36b-4d72-b3c5-de4a7b944654"

	// How long the stub waits for a cancellation it should have seen at once.
	// Only reached when the handler discards its context, and short enough that
	// such a failure is reported rather than waited out.
	snapshotStubGiveUp = 2 * ltime.Second
)

// errSnapshotWaitNotCancelled is what the stub returns when nothing ever
// observed the cancellation - the production symptom, where the snapshotter
// has long given up and the wait polls on holding the inFlight key.
var errSnapshotWaitNotCancelled = lerrors.New("the create call ran on after its caller was cancelled")

// blockingSnapshotCloud implements only the two calls CreateSnapshot makes.
// The embedded nil interface is deliberate: any other cloud call this handler
// might grow would panic here rather than silently pass.
type blockingSnapshotCloud struct {
	lscloud.Cloud

	// entered closes once the handler has reached the create-and-wait call, so
	// the test cancels a request that is genuinely in flight.
	entered   chan struct{}
	enterOnce lsync.Once
}

func newBlockingSnapshotCloud() *blockingSnapshotCloud {
	return &blockingSnapshotCloud{entered: make(chan struct{})}
}

// The idempotency lookup is not what this test drives: report "no such
// snapshot" so the handler goes on to create and wait.
func (s *blockingSnapshotCloud) GetVolumeSnapshotByName(_ lctx.Context, _, _ string) (*lsentity.Snapshot, error) {
	return nil, lscloud.ErrSnapshotNotFound
}

// Stands in for CreateSnapshotFromVolume's waitSnapshotActive: minutes-scale,
// and finished only by its context.
func (s *blockingSnapshotCloud) CreateSnapshotFromVolume(pctx lctx.Context, _, _, _ string) (*lsentity.Snapshot, error) {
	s.enterOnce.Do(func() { close(s.entered) })

	select {
	case <-pctx.Done():
		return nil, pctx.Err()
	case <-ltime.After(snapshotStubGiveUp):
		return nil, errSnapshotWaitNotCancelled
	}
}

func newSnapshotTestService(pcloud lscloud.Cloud) *controllerService {
	return &controllerService{
		cloud:         pcloud,
		inFlight:      lsinternal.NewInFlight(),
		driverOptions: &DriverOptions{clusterID: "k8s-test"},
	}
}

func snapshotRequest() *lcsi.CreateSnapshotRequest {
	return &lcsi.CreateSnapshotRequest{Name: testSnapshotName, SourceVolumeId: testSnapshotVol}
}

// TestCreateSnapshotReleasesTheInFlightKeyWhenItsContextIsCancelled is the
// whole point of threading the context through: CreateSnapshot holds the
// inFlight key for the snapshot NAME until it returns, and csi-snapshotter -
// deployed with no --timeout, so 15s - retries the same name. A handler that
// outlives its RPC refuses every one of those retries with "operation already
// exists" until the controller is restarted.
func TestCreateSnapshotReleasesTheInFlightKeyWhenItsContextIsCancelled(t *ltesting.T) {
	cloudStub := newBlockingSnapshotCloud()
	svc := newSnapshotTestService(cloudStub)

	ctx, cancel := lctx.WithCancel(lctx.Background())
	defer cancel()

	first := make(chan error, 1)
	go func() {
		_, err := svc.CreateSnapshot(ctx, snapshotRequest())
		first <- err
	}()

	// Cancel a request that is genuinely waiting, the way the snapshotter's
	// 15s deadline does.
	<-cloudStub.entered
	cancel()

	select {
	case err := <-first:
		if lerrors.Is(err, errSnapshotWaitNotCancelled) {
			t.Fatalf("CreateSnapshot() = %v; the cancellation never reached the wait", err)
		}
		if !lerrors.Is(err, lctx.Canceled) {
			t.Fatalf("CreateSnapshot() = %v, want context.Canceled", err)
		}
	case <-ltime.After(snapshotStubGiveUp + ltime.Second):
		t.Fatal("CreateSnapshot() never returned after its context was cancelled - the inFlight key is held until the controller restarts")
	}

	// The retry. It must reach the cloud rather than be refused at the gate;
	// its own context is already cancelled, so context.Canceled coming back is
	// proof it got all the way down.
	retryCtx, cancelRetry := lctx.WithCancel(lctx.Background())
	cancelRetry()

	_, err := svc.CreateSnapshot(retryCtx, snapshotRequest())
	if err != nil && lstr.Contains(err.Error(), "already exists") {
		t.Fatalf("the retry was refused with %v - the in-flight key for %q was never released", err, testSnapshotName)
	}
	if !lerrors.Is(err, lctx.Canceled) {
		t.Fatalf("the retry returned %v, want context.Canceled - it did not reach the cloud", err)
	}
}
