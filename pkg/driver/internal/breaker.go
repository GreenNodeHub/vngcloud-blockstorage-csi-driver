package internal

import (
	lsync "sync"
	ltime "time"
)

// Breaker caps what a stuck detach costs.
//
// Measured on the dev farm 04-05/09/2026: one volume whose detach never
// completed made the controller re-issue DetachBlockVolume every 6 minutes for
// 23 hours - ~230 rounds, ~13 vServer calls each, ~3,000 calls drawn from a
// quota bucket shared by the whole project. The 6-minute cadence was not
// backoff: it was external-attacher's --timeout=6m cancelling a request the
// driver held open while polling, so the attacher's own exponential backoff
// never engaged.
//
// After BreakerTripAfter real failures spanning at least BreakerTripMinElapsed
// for one (volume, node) pair - or one terminal failure, which bypasses that
// floor - the handler stops sending commands and only probes state, spacing
// real attempts out along BreakerSteps. It never gives up: every step expiry
// grants one more real attempt, so the pair still recovers on its own once the
// IaaS does.
//
// State is in-memory and dies with the process. That is deliberate - in the
// incident above, a controller restart was what finally cleared the volume, so
// a fresh leader must be allowed to try for real immediately.
const BreakerTripAfter = 3

// BreakerTripMinElapsed is the elapsed-time floor on the trip: three failures
// must also span at least this long before the breaker opens.
//
// The spec justified BreakerTripAfter = 3 as "~18 minutes at the current
// cadence", but that arithmetic only holds when each failure burns the
// attacher's whole 6-minute timeout. Two DetachVolume paths fail immediately
// without queuing anything at the IaaS - the busy-volume rejection
// (EcVServerVolumeInProcess / EcVServerVolumeIsMigrating) and a failed
// getVolumeById - and external-attacher retries those from
// --retry-interval-start=1s, doubling. Three of them therefore stack about
// three seconds apart, which without this floor would open the breaker in ~3s
// and then withhold every real detach command for ten minutes. A volume that
// is IN-PROCESS for a few seconds is routine here, so that would turn a
// 10-40s detach into a 10-minute one.
const BreakerTripMinElapsed = 5 * ltime.Minute

// BreakerSteps is the wait before each successive real attempt once tripped.
// The last entry is the cap: the wait stops growing there rather than doubling
// towards infinity, because the pair must stay recoverable.
var BreakerSteps = []ltime.Duration{
	10 * ltime.Minute,
	30 * ltime.Minute,
	60 * ltime.Minute,
	120 * ltime.Minute,
}

// Decision says what the handler may do on this call.
type Decision int

const (
	// Full - issue the real detach command.
	Full Decision = iota
	// ProbeOnly - read state only; do not command the IaaS.
	ProbeOnly
)

// BreakerKey identifies a (volume, node) pair. String() is the same
// concatenation the handler already uses for its inflight key, so the two
// stay identical in value - but keeping the fields typed lets the breaker
// report what it drops (see EvictStale), which a bare string cannot.
type BreakerKey struct {
	VolumeID, NodeID string
}

func (s BreakerKey) String() string {
	return s.VolumeID + s.NodeID
}

type breakerEntry struct {
	key         BreakerKey
	failures    int
	firstFailAt ltime.Time
	step        int
	nextAttempt ltime.Time
	tripped     bool
}

type Breaker struct {
	mu      lsync.Mutex
	entries map[string]*breakerEntry
}

func NewBreaker() *Breaker {
	return &Breaker{entries: make(map[string]*breakerEntry)}
}

// Allow reports whether the caller may issue a real command for pkey.
func (s *Breaker) Allow(pkey BreakerKey, pnow ltime.Time) Decision {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[pkey.String()]
	if !ok || !e.tripped {
		return Full
	}

	if pnow.Before(e.nextAttempt) {
		return ProbeOnly
	}

	// The step has expired: this call gets a real attempt. Allow stays Full
	// until an outcome is reported, so a retry that races in before
	// Failure/Success is not silently downgraded to a probe.
	return Full
}

// Failure records a REAL attempt that failed. Never call it for a probe: a
// probe sends no command, so it says nothing about whether the IaaS is
// accepting them, and must not advance the backoff.
//
// Returns tripped when this failure trips the breaker, and stepped when it
// advances to a longer wait. The handler emits an event on either.
func (s *Breaker) Failure(pkey BreakerKey, pterminal bool, pnow ltime.Time) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[pkey.String()]
	if !ok {
		e = &breakerEntry{key: pkey, firstFailAt: pnow}
		s.entries[pkey.String()] = e
	}
	e.failures++
	// Last-touched marker for EvictStale. Once tripped this gets overwritten
	// below by the real next-attempt due date; until then it just says "this
	// pair was still failing as of pnow", so an untripped entry that stops
	// being touched goes stale on the same clock as a tripped one.
	e.nextAttempt = pnow

	if !e.tripped {
		// A terminal error will not fix itself with more of the same call, so
		// it trips at once instead of burning the whole threshold - and it
		// bypasses BreakerTripMinElapsed deliberately: a quota that needs
		// raising, or a volume the IaaS has parked in ERROR state, will not
		// come right inside five minutes either.
		//
		// Everything else must clear both the count AND the floor, because a
		// count on its own is satisfied by three fast rejections seconds apart
		// (see BreakerTripMinElapsed).
		if pterminal || (e.failures >= BreakerTripAfter && pnow.Sub(e.firstFailAt) >= BreakerTripMinElapsed) {
			e.tripped = true
			e.step = 0
			e.nextAttempt = pnow.Add(BreakerSteps[0])

			return true, false
		}

		return false, false
	}

	if e.step < len(BreakerSteps)-1 {
		e.step++
		e.nextAttempt = pnow.Add(BreakerSteps[e.step])

		return false, true
	}

	e.nextAttempt = pnow.Add(BreakerSteps[len(BreakerSteps)-1])

	return false, false
}

// Success clears the pair: the volume detached, nothing left to track.
func (s *Breaker) Success(pkey BreakerKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.entries, pkey.String())
}

// Since reports how long pkey has been failing, measured from its first
// failure - that is the number the detach_pending_seconds gauge publishes.
func (s *Breaker) Since(pkey BreakerKey, pnow ltime.Time) (ltime.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[pkey.String()]
	if !ok {
		return 0, false
	}

	return pnow.Sub(e.firstFailAt), true
}

// EvictStale drops every entry that has gone untouched for twice the capped
// step, and returns the keys it dropped.
//
// Every Failure call stamps nextAttempt - either to pnow itself, before the
// pair has tripped, or to the next due date once it has - so "untouched past
// 2 * cap" means no ControllerUnpublishVolume call has landed on this pair
// for a long time after it should have. That is either a volume that is
// gone, or one unstuck by something other than a successful
// ControllerUnpublishVolume (a force-deleted VolumeAttachment, a
// garbage-collected Machine, ...): either way nothing should still be
// probing it or reporting it stuck.
//
// Lazy: no background goroutine. The caller is expected to invoke this
// explicitly (never from inside Allow, which already holds this lock for one
// key) and clear any gauge series for the keys returned.
func (s *Breaker) EvictStale(pnow ltime.Time) []BreakerKey {
	s.mu.Lock()
	defer s.mu.Unlock()

	capStep := BreakerSteps[len(BreakerSteps)-1]
	staleBefore := pnow.Add(-2 * capStep)

	var evicted []BreakerKey
	for k, e := range s.entries {
		if e.nextAttempt.Before(staleBefore) {
			evicted = append(evicted, e.key)
			delete(s.entries, k)
		}
	}

	return evicted
}
