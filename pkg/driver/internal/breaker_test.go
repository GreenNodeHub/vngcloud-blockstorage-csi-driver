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
	at := tripBreaker(t, b, bkKey, ltime.Unix(0, 0))

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
	at := tripBreaker(t, b, bkKey, ltime.Unix(0, 0))

	if got := b.Allow(bkKey, at); got != ProbeOnly {
		t.Fatalf("Allow() = %v, want ProbeOnly", got)
	}

	b.Success(bkKey)
	if got := b.Allow(bkKey, at); got != Full {
		t.Fatalf("Allow() after Success = %v, want Full", got)
	}
	if _, ok, _ := b.Since(bkKey, at); ok {
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

	got, ok, _ := b.Since(bkKey, start.Add(20*ltime.Minute))
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

	at = tripBreaker(t, b, keyA, at)
	if got := b.Allow(keyA, at); got != ProbeOnly {
		t.Fatalf("the pair that failed was not tripped: Allow() = %v, want ProbeOnly", got)
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
	trippedAt := tripBreaker(t, b, bkKey, ltime.Unix(0, 0))

	if got := b.Allow(bkKey, trippedAt); got != ProbeOnly {
		t.Fatalf("Allow() = %v, want ProbeOnly", got)
	}

	// Tripping on the last failure set nextAttempt = trippedAt +
	// BreakerSteps[0]; nothing touches the entry again, so staleness is
	// measured from there.
	dueAt := trippedAt.Add(BreakerSteps[0])
	capStep := BreakerSteps[len(BreakerSteps)-1]

	// Not yet stale: still inside 2x the capped step past the due date.
	notStaleAt := dueAt.Add(2*capStep - ltime.Second)
	if evicted := b.EvictStale(notStaleAt); len(evicted) != 0 {
		t.Fatalf("EvictStale() = %v before the idle window elapsed, want none evicted", evicted)
	}
	if _, ok, _ := b.Since(bkKey, notStaleAt); !ok {
		t.Fatal("entry evicted too early")
	}

	// Past the idle window: the entry (and only this one) must go.
	staleAt := dueAt.Add(2*capStep + ltime.Second)
	evicted := b.EvictStale(staleAt)
	if len(evicted) != 1 || evicted[0] != bkKey {
		t.Fatalf("EvictStale() = %v, want exactly [%v]", evicted, bkKey)
	}
	if _, ok, _ := b.Since(bkKey, staleAt); ok {
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

	trippedAt := tripBreaker(t, b, stale, at)
	tripBreaker(t, b, active, at)

	dueAt := trippedAt.Add(BreakerSteps[0])
	capStep := BreakerSteps[len(BreakerSteps)-1]
	staleAt := dueAt.Add(2*capStep + ltime.Second)

	// The active pair gets touched right before the eviction check (its own
	// due date lands long after staleAt), the stale one does not.
	b.Failure(active, false, staleAt.Add(-ltime.Second))

	evicted := b.EvictStale(staleAt)
	if len(evicted) != 1 || evicted[0] != stale {
		t.Fatalf("EvictStale() = %v, want exactly [%v]", evicted, stale)
	}
	if _, ok, _ := b.Since(active, staleAt); !ok {
		t.Fatal("EvictStale() dropped a pair that was still being touched")
	}
}

// tripBreaker drives pkey through BreakerTripAfter non-terminal failures
// spaced far enough apart to clear BreakerTripMinElapsed, and returns the
// instant of the tripping failure - which is what nextAttempt is measured
// from. Tests that need a tripped pair must go through this: a burst of
// same-instant failures no longer trips anything.
func tripBreaker(t *ltesting.T, pb *Breaker, pkey BreakerKey, pstart ltime.Time) ltime.Time {
	t.Helper()

	at := pstart
	var tripped bool
	for i := 0; i < BreakerTripAfter; i++ {
		tripped, _ = pb.Failure(pkey, false, at)
		if i < BreakerTripAfter-1 {
			at = at.Add(BreakerTripMinElapsed)
		}
	}
	if !tripped {
		t.Fatalf("tripBreaker: %d failures spanning %v did not trip", BreakerTripAfter,
			BreakerTripMinElapsed*ltime.Duration(BreakerTripAfter-1))
	}

	return at
}

// Item 1: the trip needs an elapsed-time floor, not just a failure count.
//
// DetachVolume has two paths that fail immediately without queuing anything at
// the IaaS, and external-attacher retries from --retry-interval-start=1s,
// doubling. Three such failures land ~1s/2s/4s apart. Counting alone would
// open the breaker in about three seconds and then give the pair no real
// detach command for ten minutes.
func TestBreakerDoesNotTripOnAFastFailureBurst(t *ltesting.T) {
	b := NewBreaker()
	start := ltime.Unix(0, 0)

	for i, delay := range []ltime.Duration{0, ltime.Second, 3 * ltime.Second} {
		at := start.Add(delay)
		tripped, stepped := b.Failure(bkKey, false, at)
		if tripped || stepped {
			t.Fatalf("failure %d at +%v: tripped=%v stepped=%v, want both false inside the %v floor",
				i+1, delay, tripped, stepped, BreakerTripMinElapsed)
		}
		if got := b.Allow(bkKey, at); got != Full {
			t.Fatalf("failure %d at +%v: Allow() = %v, want Full", i+1, delay, got)
		}
	}
}

// The failures the breaker exists for are slow ones: each burns the attacher's
// 6-minute budget, so three of them span ~18 minutes and must trip.
func TestBreakerTripsWhenFailuresSpanTheFloor(t *ltesting.T) {
	b := NewBreaker()
	start := ltime.Unix(0, 0)

	b.Failure(bkKey, false, start)
	b.Failure(bkKey, false, start.Add(6*ltime.Minute))

	at := start.Add(12 * ltime.Minute)
	tripped, _ := b.Failure(bkKey, false, at)
	if !tripped {
		t.Fatalf("%d failures spanning 12m did not trip", BreakerTripAfter)
	}
	if got := b.Allow(bkKey, at); got != ProbeOnly {
		t.Fatalf("Allow() before nextAttempt = %v, want ProbeOnly", got)
	}
}

// The floor delays the trip; it must not reset the count. A burst of fast
// rejections followed by one more failure once the floor has elapsed trips on
// that later failure.
func TestBreakerTripsOnTheFirstFailurePastTheFloor(t *ltesting.T) {
	b := NewBreaker()
	start := ltime.Unix(0, 0)

	for _, delay := range []ltime.Duration{0, ltime.Second, 3 * ltime.Second, 4 * ltime.Second} {
		if tripped, _ := b.Failure(bkKey, false, start.Add(delay)); tripped {
			t.Fatalf("tripped at +%v, inside the %v floor", delay, BreakerTripMinElapsed)
		}
	}

	late := start.Add(BreakerTripMinElapsed + ltime.Second)
	tripped, stepped := b.Failure(bkKey, false, late)
	if !tripped {
		t.Fatalf("did not trip on the first failure past the floor (at +%v)", BreakerTripMinElapsed+ltime.Second)
	}
	if stepped {
		t.Fatal("reported a step increase on the tripping failure")
	}
	if got := b.Allow(bkKey, late); got != ProbeOnly {
		t.Fatalf("Allow() after trip = %v, want ProbeOnly", got)
	}
}

// A terminal error bypasses the floor deliberately: a quota that needs raising
// or a volume in ERROR state will not fix itself inside five minutes, so
// waiting the floor out would only delay telling someone. Asserted against a
// non-terminal failure at the same instant so the test measures the flag, not
// the clock.
func TestBreakerTerminalTripsInsideTheFloor(t *ltesting.T) {
	at := ltime.Unix(0, 0)

	nonTerminal := NewBreaker()
	if tripped, _ := nonTerminal.Failure(bkKey, false, at); tripped {
		t.Fatal("a non-terminal first failure must not trip inside the floor")
	}

	b := NewBreaker()
	tripped, _ := b.Failure(bkKey, true, at)
	if !tripped {
		t.Fatal("a terminal failure must trip on its first occurrence, floor notwithstanding")
	}
	if got := b.Allow(bkKey, at); got != ProbeOnly {
		t.Fatalf("Allow() after a terminal trip = %v, want ProbeOnly", got)
	}
}

// Since must separate "tracked" from "tripped". An entry exists from failure
// #1, but the handler may only report a pair as stuck once the breaker has
// actually opened on it - reporting on merely tracked pairs means one
// unpaginated PV LIST per transient detach failure.
func TestBreakerSinceSeparatesTrackedFromTripped(t *ltesting.T) {
	b := NewBreaker()
	start := ltime.Unix(0, 0)

	if _, tracked, tripped := b.Since(bkKey, start); tracked || tripped {
		t.Fatalf("untouched pair: tracked=%v tripped=%v, want both false", tracked, tripped)
	}

	b.Failure(bkKey, false, start)
	_, tracked, tripped := b.Since(bkKey, start.Add(ltime.Second))
	if !tracked {
		t.Fatal("after one failure the pair must be tracked")
	}
	if tripped {
		t.Fatal("one failure must not report the pair as tripped")
	}

	// A pair of its own, so the helper starts from a clean entry.
	trippedKey := BreakerKey{VolumeID: "vol-tripped", NodeID: "ins-1"}
	at := tripBreaker(t, b, trippedKey, start)
	if _, tracked, tripped = b.Since(trippedKey, at); !tracked || !tripped {
		t.Fatalf("after tripping: tracked=%v tripped=%v, want both true", tracked, tripped)
	}
}
