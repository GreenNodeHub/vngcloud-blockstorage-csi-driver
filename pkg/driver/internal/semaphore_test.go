package internal

import (
	lctx "context"
	lsync "sync"
	lsync_atomic "sync/atomic"
	ltesting "testing"
	ltime "time"
)

// The TS-B experiment of 30/08/2026 (same 100-PVC storm, csi-provisioner
// worker-threads 100 vs 10) showed vServer chokes on CONCURRENT CreateVolume
// calls, not on request rate: 10 workers finished 31% faster with 92% fewer
// vServer 500s. The gate caps in-flight creates inside the driver, so the
// protection no longer depends on how the CO happens to be configured.

func TestSemaphoreCapsConcurrency(t *ltesting.T) {
	const cap, workers = 3, 20
	sem := NewSemaphore(cap)

	var cur, max lsync_atomic.Int32
	var wg lsync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sem.Acquire(lctx.Background()); err != nil {
				t.Errorf("Acquire() = %v, want nil", err)
				return
			}
			defer sem.Release()

			n := cur.Add(1)
			for {
				m := max.Load()
				if n <= m || max.CompareAndSwap(m, n) {
					break
				}
			}
			ltime.Sleep(10 * ltime.Millisecond)
			cur.Add(-1)
		}()
	}
	wg.Wait()

	if got := max.Load(); got > cap {
		t.Fatalf("observed %d concurrent holders, cap is %d", got, cap)
	}
	if got := max.Load(); got != cap {
		t.Fatalf("observed only %d concurrent holders with %d workers; the gate is narrower than its cap %d", got, workers, cap)
	}
}

// The handler must not wait forever: the sidecar cancels the request context
// on its own timeout, and the wait has to end there so the CO can retry.
func TestSemaphoreAcquireEndsWithTheContext(t *ltesting.T) {
	sem := NewSemaphore(1)
	if err := sem.Acquire(lctx.Background()); err != nil {
		t.Fatalf("first Acquire() = %v, want nil", err)
	}

	ctx, cancel := lctx.WithCancel(lctx.Background())
	done := make(chan error, 1)
	go func() { done <- sem.Acquire(ctx) }()

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Acquire() = nil on a cancelled context; the slot was full")
		}
	case <-ltime.After(2 * ltime.Second):
		t.Fatal("Acquire() still blocked 2s after its context was cancelled")
	}
}

func TestSemaphoreReleaseFreesASlot(t *ltesting.T) {
	sem := NewSemaphore(1)
	if err := sem.Acquire(lctx.Background()); err != nil {
		t.Fatalf("first Acquire() = %v, want nil", err)
	}
	sem.Release()

	ctx, cancel := lctx.WithTimeout(lctx.Background(), 2*ltime.Second)
	defer cancel()
	if err := sem.Acquire(ctx); err != nil {
		t.Fatalf("Acquire() after Release() = %v, want nil", err)
	}
}
