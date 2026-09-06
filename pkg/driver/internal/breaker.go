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
// After BreakerTripAfter real failures for one (volume, node) pair the handler
// stops sending commands and only probes state, spacing real attempts out along
// BreakerSteps. It never gives up: every step expiry grants one more real
// attempt, so the pair still recovers on its own once the IaaS does.
//
// State is in-memory and dies with the process. That is deliberate - in the
// incident above, a controller restart was what finally cleared the volume, so
// a fresh leader must be allowed to try for real immediately.
const BreakerTripAfter = 3

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

type breakerEntry struct {
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
func (s *Breaker) Allow(pkey string, pnow ltime.Time) Decision {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[pkey]
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
func (s *Breaker) Failure(pkey string, pterminal bool, pnow ltime.Time) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[pkey]
	if !ok {
		e = &breakerEntry{firstFailAt: pnow}
		s.entries[pkey] = e
	}
	e.failures++

	if !e.tripped {
		// A terminal error will not fix itself with more of the same call, so
		// it trips at once instead of burning the whole threshold.
		if pterminal || e.failures >= BreakerTripAfter {
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
func (s *Breaker) Success(pkey string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.entries, pkey)
}

// Since reports how long pkey has been failing, measured from its first
// failure - that is the number the detach_pending_seconds gauge publishes.
func (s *Breaker) Since(pkey string, pnow ltime.Time) (ltime.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[pkey]
	if !ok {
		return 0, false
	}

	return pnow.Sub(e.firstFailAt), true
}
