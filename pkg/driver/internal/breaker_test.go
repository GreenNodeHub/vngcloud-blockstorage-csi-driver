package internal

import (
	lsync "sync"
	ltesting "testing"
	ltime "time"
)

var bkKey = BreakerKey{VolumeID: "vol-1", NodeID: "ins-1"}

func TestBreakerAllowsFullUntilTripAfterFailures(t *ltesting.T) {
	b := NewBreaker()
	at := ltime.Unix(0, 0)

	// Below the threshold the driver keeps issuing real detach commands.
	for i := 0; i < BreakerTripAfter-1; i++ {
		if got := b.Allow(bkKey, at); got != Full {
			t.Fatalf("failure %d: Allow() = %v, want Full", i, got)
		}
		tripped, _ := b.Failure(bkKey, false, at)
		if tripped {
			t.Fatalf("tripped after only %d failures, want %d", i+1, BreakerTripAfter)
		}
		at = at.Add(6 * ltime.Minute)
	}

	// The threshold failure trips it.
	if got := b.Allow(bkKey, at); got != Full {
		t.Fatalf("Allow() before the tripping failure = %v, want Full", got)
	}
	tripped, _ := b.Failure(bkKey, false, at)
	if !tripped {
		t.Fatalf("did not trip on failure %d", BreakerTripAfter)
	}
	if got := b.Allow(bkKey, at); got != ProbeOnly {
		t.Fatalf("Allow() after trip = %v, want ProbeOnly", got)
	}
}

// A terminal error - a raised quota is the only cure - must not wait for three
// rounds before anyone is told.
func TestBreakerTripsImmediatelyOnTerminal(t *ltesting.T) {
	b := NewBreaker()
	at := ltime.Unix(0, 0)

	tripped, _ := b.Failure(bkKey, true, at)
	if !tripped {
		t.Fatal("a terminal failure must trip on the first occurrence")
	}
	if got := b.Allow(bkKey, at); got != ProbeOnly {
		t.Fatalf("Allow() = %v, want ProbeOnly", got)
	}
}

func TestBreakerWalksStepsAndHoldsAtCap(t *ltesting.T) {
	b := NewBreaker()
	at := ltime.Unix(0, 0)

	for i := 0; i < BreakerTripAfter; i++ {
		b.Failure(bkKey, false, at)
	}

	// Each expiry grants exactly one real attempt; failing it advances a step.
	for i, want := range BreakerSteps {
		at = at.Add(want)
		if got := b.Allow(bkKey, at); got != Full {
			t.Fatalf("step %d: Allow() at expiry = %v, want Full", i, got)
		}
		if got := b.Allow(bkKey, at); got != Full {
			t.Fatalf("step %d: Allow() must stay Full until an outcome is reported, got %v", i, got)
		}
		_, stepped := b.Failure(bkKey, false, at)
		if i < len(BreakerSteps)-1 && !stepped {
			t.Fatalf("step %d: expected the step to advance", i)
		}
		if got := b.Allow(bkKey, at); got != ProbeOnly {
			t.Fatalf("step %d: Allow() right after failing = %v, want ProbeOnly", i, got)
		}
	}

	// At the cap the wait must stop growing, not keep doubling.
	cap := BreakerSteps[len(BreakerSteps)-1]
	at = at.Add(cap)
	if got := b.Allow(bkKey, at); got != Full {
		t.Fatalf("at cap: Allow() after the capped wait = %v, want Full", got)
	}
	_, stepped := b.Failure(bkKey, false, at)
	if stepped {
		t.Fatal("stepped past the last step; the backoff must hold at the cap")
	}
}

func TestBreakerSuccessClearsState(t *ltesting.T) {
	b := NewBreaker()
	at := ltime.Unix(0, 0)

	for i := 0; i < BreakerTripAfter; i++ {
		b.Failure(bkKey, false, at)
	}
	if got := b.Allow(bkKey, at); got != ProbeOnly {
		t.Fatalf("Allow() = %v, want ProbeOnly", got)
	}

	b.Success(bkKey)
	if got := b.Allow(bkKey, at); got != Full {
		t.Fatalf("Allow() after Success = %v, want Full", got)
	}
	if _, ok := b.Since(bkKey, at); ok {
		t.Fatal("Since() still reports a stuck pair after Success")
	}
}

// Since feeds the detach_pending_seconds gauge: it measures from the FIRST
// failure, not from the trip, so the gauge shows the whole outage.
func TestBreakerSinceMeasuresFromFirstFailure(t *ltesting.T) {
	b := NewBreaker()
	start := ltime.Unix(0, 0)

	b.Failure(bkKey, false, start)
	b.Failure(bkKey, false, start.Add(6*ltime.Minute))

	got, ok := b.Since(bkKey, start.Add(20*ltime.Minute))
	if !ok {
		t.Fatal("Since() = not tracked, want tracked")
	}
	if got != 20*ltime.Minute {
		t.Fatalf("Since() = %v, want 20m", got)
	}
}

func TestBreakerKeysAreIndependent(t *ltesting.T) {
	b := NewBreaker()
	at := ltime.Unix(0, 0)
	keyA := BreakerKey{VolumeID: "vol-1", NodeID: "ins-1"}
	keyB := BreakerKey{VolumeID: "vol-1", NodeID: "ins-2"}

	for i := 0; i < BreakerTripAfter; i++ {
		b.Failure(keyA, false, at)
	}

	if got := b.Allow(keyB, at); got != Full {
		t.Fatalf("a different pair was affected: Allow() = %v, want Full", got)
	}
}

func TestBreakerIsConcurrencySafe(t *ltesting.T) {
	b := NewBreaker()
	at := ltime.Unix(0, 0)

	var wg lsync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Allow(bkKey, at)
			b.Failure(bkKey, false, at)
			b.Since(bkKey, at)
			b.Success(bkKey)
		}()
	}
	wg.Wait()
}

// A pair unstuck by something other than a successful ControllerUnpublishVolume
// (an operator force-deleting the VolumeAttachment, a garbage-collected
// Machine, ...) never calls Success. Left behind, its entry - and the gauge
// series keyed off it - would sit frozen forever and the "stuck > 30m" alert
// this feature exists to raise would fire permanently on a healthy cluster.
// EvictStale is the lazy, no-goroutine way out: anything idle past twice the
// capped step, measured from its nextAttempt due date, is dropped and its key
// handed back so the caller can clear the gauge too.
func TestBreakerEvictStaleDropsIdleEntryAndReturnsItsKey(t *ltesting.T) {
	b := NewBreaker()
	start := ltime.Unix(0, 0)

	for i := 0; i < BreakerTripAfter; i++ {
		b.Failure(bkKey, false, start)
	}
	if got := b.Allow(bkKey, start); got != ProbeOnly {
		t.Fatalf("Allow() = %v, want ProbeOnly", got)
	}

	// Tripping on the last failure set nextAttempt = start + BreakerSteps[0];
	// nothing touches the entry again, so staleness is measured from there.
	dueAt := start.Add(BreakerSteps[0])
	capStep := BreakerSteps[len(BreakerSteps)-1]

	// Not yet stale: still inside 2x the capped step past the due date.
	notStaleAt := dueAt.Add(2*capStep - ltime.Second)
	if evicted := b.EvictStale(notStaleAt); len(evicted) != 0 {
		t.Fatalf("EvictStale() = %v before the idle window elapsed, want none evicted", evicted)
	}
	if _, ok := b.Since(bkKey, notStaleAt); !ok {
		t.Fatal("entry evicted too early")
	}

	// Past the idle window: the entry (and only this one) must go.
	staleAt := dueAt.Add(2*capStep + ltime.Second)
	evicted := b.EvictStale(staleAt)
	if len(evicted) != 1 || evicted[0] != bkKey {
		t.Fatalf("EvictStale() = %v, want exactly [%v]", evicted, bkKey)
	}
	if _, ok := b.Since(bkKey, staleAt); ok {
		t.Fatal("Since() still reports the evicted pair")
	}
	// And it is fully forgotten, not merely marked: a fresh Allow starts at Full.
	if got := b.Allow(bkKey, staleAt); got != Full {
		t.Fatalf("Allow() after eviction = %v, want Full", got)
	}
}

func TestBreakerEvictStaleLeavesActiveEntriesAlone(t *ltesting.T) {
	b := NewBreaker()
	at := ltime.Unix(0, 0)
	stale := BreakerKey{VolumeID: "vol-stale", NodeID: "ins-1"}
	active := BreakerKey{VolumeID: "vol-active", NodeID: "ins-1"}

	for i := 0; i < BreakerTripAfter; i++ {
		b.Failure(stale, false, at)
		b.Failure(active, false, at)
	}

	dueAt := at.Add(BreakerSteps[0])
	capStep := BreakerSteps[len(BreakerSteps)-1]
	staleAt := dueAt.Add(2*capStep + ltime.Second)

	// The active pair gets touched right before the eviction check (its own
	// due date lands long after staleAt), the stale one does not.
	b.Failure(active, false, staleAt.Add(-ltime.Second))

	evicted := b.EvictStale(staleAt)
	if len(evicted) != 1 || evicted[0] != stale {
		t.Fatalf("EvictStale() = %v, want exactly [%v]", evicted, stale)
	}
	if _, ok := b.Since(active, staleAt); !ok {
		t.Fatal("EvictStale() dropped a pair that was still being touched")
	}
}
