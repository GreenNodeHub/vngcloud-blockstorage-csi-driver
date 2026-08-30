package cloud

import (
	lsync "sync"
	lsync_atomic "sync/atomic"
	ltesting "testing"
	ltime "time"

	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
)

// The catalog lookups on the CreateVolume path (ListZones, GetVolumeTypeZones,
// GetListVolumeTypes) describe a topology that changes on the timescale of a
// region rollout, yet the TS-B load test on 29/08/2026 measured them being
// re-fetched on EVERY call: 709 ListZones for 190 PVCs. That traffic is what
// pushed the driver past the vServer quota and pinned its own rate limiter at
// the 1 QPS floor.

func TestMetaCacheServesRepeatCallsWithoutRefetching(t *ltesting.T) {
	var calls lsync_atomic.Int32
	c := newMetaCache[int](ltime.Minute)
	load := func() (int, lserr.IError) {
		calls.Add(1)
		return 42, nil
	}

	for i := 0; i < 5; i++ {
		got, err := c.get(load)
		if err != nil {
			t.Fatalf("get() returned error: %v", err)
		}
		if got != 42 {
			t.Fatalf("get() = %v, want 42", got)
		}
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("5 calls triggered %d fetches, want 1", got)
	}
}

func TestMetaCacheRefetchesAfterTTL(t *ltesting.T) {
	var calls lsync_atomic.Int32
	c := newMetaCache[int](50 * ltime.Millisecond)
	load := func() (int, lserr.IError) {
		calls.Add(1)
		return 42, nil
	}

	if _, err := c.get(load); err != nil {
		t.Fatalf("first get() returned error: %v", err)
	}
	ltime.Sleep(80 * ltime.Millisecond)
	if _, err := c.get(load); err != nil {
		t.Fatalf("second get() returned error: %v", err)
	}

	if got := calls.Load(); got != 2 {
		t.Fatalf("after the TTL expired there were %d fetches, want 2", got)
	}
}

// A cached failure would turn one unlucky lookup into a lasting outage.
func TestMetaCacheDoesNotCacheFailures(t *ltesting.T) {
	var calls lsync_atomic.Int32
	c := newMetaCache[int](ltime.Minute)
	load := func() (int, lserr.IError) {
		if calls.Add(1) == 1 {
			return 0, errClientRateLimited("https://vserver/zones")
		}
		return 42, nil
	}

	if _, err := c.get(load); err == nil {
		t.Fatal("first get() should surface the loader's error")
	}

	got, err := c.get(load)
	if err != nil {
		t.Fatalf("second get() returned error: %v", err)
	}
	if got != 42 {
		t.Fatalf("get() = %v, want 42 once the loader recovers", got)
	}
}

// The case that matters in production: with worker-threads=100 a cold cache is
// hit by 100 CreateVolume calls at once. Without single-flight each one issues
// its own request and the stampede is exactly the load we are trying to remove.
func TestMetaCacheCollapsesAConcurrentStampede(t *ltesting.T) {
	var calls lsync_atomic.Int32
	release := make(chan struct{})
	c := newMetaCache[int](ltime.Minute)
	load := func() (int, lserr.IError) {
		calls.Add(1)
		<-release
		return 42, nil
	}

	var wg lsync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, err := c.get(load); err != nil || got != 42 {
				t.Errorf("get() = %v, %v; want 42, nil", got, err)
			}
		}()
	}

	// Let the goroutines pile up on the cold cache before the loader returns.
	ltime.Sleep(50 * ltime.Millisecond)
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("100 concurrent calls triggered %d fetches, want 1", got)
	}
}

// GetVolumeTypeIdByName is looked up per (zone, volume type name), so its cache
// has to be keyed - one shared entry would hand a zone the other zone's answer.
func TestKeyedMetaCacheSeparatesKeys(t *ltesting.T) {
	var calls lsync_atomic.Int32
	c := newKeyedMetaCache[string](ltime.Minute)
	load := func(pwant string) func() (string, lserr.IError) {
		return func() (string, lserr.IError) {
			calls.Add(1)
			return pwant, nil
		}
	}

	got, err := c.get("HCM03-1B/nvme-iops5000", load("vtype-nvme"))
	if err != nil || got != "vtype-nvme" {
		t.Fatalf("get() = %v, %v; want vtype-nvme, nil", got, err)
	}

	got, err = c.get("HCM03-1A/ssd-iops3000", load("vtype-ssd"))
	if err != nil || got != "vtype-ssd" {
		t.Fatalf("a different key returned %v, %v; want vtype-ssd, nil", got, err)
	}

	// Both keys are warm now.
	if got, _ := c.get("HCM03-1B/nvme-iops5000", load("unused")); got != "vtype-nvme" {
		t.Fatalf("repeat get() = %v, want the cached vtype-nvme", got)
	}
	if want := int32(2); calls.Load() != want {
		t.Fatalf("3 calls over 2 keys triggered %d fetches, want %d", calls.Load(), want)
	}
}
