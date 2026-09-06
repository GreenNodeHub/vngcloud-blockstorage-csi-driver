# Detach Circuit-Breaker and Classified Error Events Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop a stuck detach from re-issuing `DetachBlockVolume` forever, and surface it to operators through a Kubernetes event and an alertable metric.

**Architecture:** Three independent units — an error classifier in `pkg/cloud`, a keyed circuit-breaker in `pkg/driver/internal`, and PV/PVC event helpers in `pkg/k8s` — composed by `ControllerUnpublishVolume`. After N real failures for a (volume, node) pair the handler stops sending detach commands and only issues a read-only state probe until the next scheduled attempt, so the volume still self-heals when the IaaS recovers.

**Tech Stack:** Go 1.25 (use `/home/tytv2/sdk/go1.25/bin/go`; system go 1.18 cannot parse go.mod), `k8s.io/component-base/metrics`, `k8s.io/client-go` fake clientset + `record.FakeRecorder` for tests, `github.com/cuongpiger/joat/data-structure/set` for code sets.

**Spec:** `docs/superpowers/specs/2026-09-06-detach-breaker-and-error-events-design.md`

## Global Constraints

- Go toolchain: `/home/tytv2/sdk/go1.25/bin/go` — system `go` is 1.18 and fails on `go.mod`.
- Branch: `tytv2/detach-breaker-error-events` (already exists, spec committed as `e9ba661`).
- Import aliasing: every import gets an `l`-prefixed alias, matching the whole repo (`lctx "context"`, `ltime "time"`, `lsync "sync"`, `lserr ".../pkg/cloud/errors"`).
- Receiver naming: methods use `func (s *T)`; parameters are `p`-prefixed (`pctx`, `pkey`, `pnow`).
- Observability must never fail the primary path: event/metric errors are logged at `V(2)` and swallowed.
- Breaker state is in-memory only. No persistence, no cleanup goroutine.
- Every task ends green with `/home/tytv2/sdk/go1.25/bin/go test ./pkg/... -race`.
- Metric names use the existing `vcontainer_` prefix.
- PR-A = Tasks 1–5. PR-B = Task 6.

---

### Task 1: Error classifier

**Files:**
- Create: `pkg/cloud/errclass.go`
- Create: `pkg/cloud/errclass_test.go`

**Interfaces:**
- Consumes: `lserr.IError` (from `pkg/cloud/errors`), existing helpers `isThrottled(lsdkErrs.IError) bool` and `isServerError(lsdkErrs.IError) bool` in `pkg/cloud/ratelimit.go`.
- Produces: `type Class struct { Terminal bool; Reason string }` and `func Classify(perr lserr.IError) Class`. Reason constants exported: `ReasonVolumeQuotaExceeded`, `ReasonVolumeSizeQuotaExceeded`, `ReasonVolumeAttachQuotaExceeded`, `ReasonIaaSPermissionDenied`, `ReasonVolumeInErrorState`, `ReasonIaaSThrottled`, `ReasonIaaSServerError`, `ReasonIaaSOperationStalled`, `ReasonIaaSUnknownError`.

- [ ] **Step 1: Write the failing test**

Create `pkg/cloud/errclass_test.go`:

```go
package cloud

import (
	ltesting "testing"

	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"

	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
)

// sdkWrapped builds the driver-level IError the way DetachVolume does: an
// lserr.IError wrapping an SDK error, so Classify must dig through the wrapper.
func sdkWrapped(pcode lsdkErrs.ErrorCode) lserr.IError {
	return lserr.NewError(new(lsdkErrs.SdkError).WithErrorCode(pcode))
}

func sdkWrappedStatus(pstatus int, pcode lsdkErrs.ErrorCode) lserr.IError {
	return lserr.NewError(new(lsdkErrs.SdkError).
		WithErrorCode(pcode).
		WithKVparameters("statusCode", pstatus, "url", "https://vserver/volumes"))
}

func TestClassify(t *ltesting.T) {
	tcs := []struct {
		name         string
		err          lserr.IError
		wantTerminal bool
		wantReason   string
	}{
		{"volume count quota", sdkWrapped(lsdkErrs.EcVServerVolumeExceedQuota), true, ReasonVolumeQuotaExceeded},
		{"volume size quota", sdkWrapped(lsdkErrs.EcVServerVolumeSizeExceedGlobalQuota), true, ReasonVolumeSizeQuotaExceeded},
		{"attach quota per server", sdkWrapped(lsdkErrs.EcVServerServerVolumeAttachQuotaExceeded), true, ReasonVolumeAttachQuotaExceeded},
		// 429 and 403 both arrive as EcPermissionDenied; only statusCode separates them.
		{"real 403 is terminal", sdkWrappedStatus(403, lsdkErrs.EcPermissionDenied), true, ReasonIaaSPermissionDenied},
		{"429 is transient", sdkWrappedStatus(429, lsdkErrs.EcPermissionDenied), false, ReasonIaaSThrottled},
		{"500 is transient", sdkWrapped(lsdkErrs.EcInternalServerError), false, ReasonIaaSServerError},
		{"503 is transient", sdkWrapped(lsdkErrs.EcServiceMaintenance), false, ReasonIaaSServerError},
		{"in-process is transient", sdkWrapped(lsdkErrs.EcVServerVolumeInProcess), false, ReasonIaaSOperationStalled},
		{"unknown is transient", sdkWrapped(lsdkErrs.EcUnknownError), false, ReasonIaaSUnknownError},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *ltesting.T) {
			got := Classify(tc.err)
			if got.Terminal != tc.wantTerminal {
				t.Fatalf("Terminal = %v, want %v", got.Terminal, tc.wantTerminal)
			}
			if got.Reason != tc.wantReason {
				t.Fatalf("Reason = %q, want %q", got.Reason, tc.wantReason)
			}
		})
	}
}

// A nil error must not panic and must not look like a failure worth reporting.
func TestClassifyNilIsEmpty(t *ltesting.T) {
	got := Classify(nil)
	if got.Terminal || got.Reason != "" {
		t.Fatalf("Classify(nil) = %+v, want zero value", got)
	}
}

// The driver's own error for an ERROR-state volume has no SDK code behind it.
func TestClassifyDriverErrorState(t *ltesting.T) {
	got := Classify(lserr.ErrVolumeIsInErrorState("vol-x"))
	if !got.Terminal || got.Reason != ReasonVolumeInErrorState {
		t.Fatalf("Classify(ErrVolumeIsInErrorState) = %+v, want terminal %q", got, ReasonVolumeInErrorState)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/home/tytv2/sdk/go1.25/bin/go test ./pkg/cloud/ -run TestClassify`
Expected: FAIL — build error `undefined: Classify`, `undefined: ReasonVolumeQuotaExceeded`.

- [ ] **Step 3: Write minimal implementation**

Create `pkg/cloud/errclass.go`:

```go
package cloud

import (
	lhttp "net/http"

	lset "github.com/cuongpiger/joat/data-structure/set"
	lsdkErrs "github.com/vngcloud/vngcloud-go-sdk/v2/vngcloud/sdk_error"

	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
)

// Event reasons. Stable CamelCase strings: they end up as Kubernetes event
// reasons and in metric labels, so operators grep and alert on them.
const (
	ReasonVolumeQuotaExceeded       = "VolumeQuotaExceeded"
	ReasonVolumeSizeQuotaExceeded   = "VolumeSizeQuotaExceeded"
	ReasonVolumeAttachQuotaExceeded = "VolumeAttachQuotaExceeded"
	ReasonIaaSPermissionDenied      = "IaaSPermissionDenied"
	ReasonVolumeInErrorState        = "VolumeInErrorState"
	ReasonIaaSThrottled             = "IaaSThrottled"
	ReasonIaaSServerError           = "IaaSServerError"
	ReasonIaaSOperationStalled      = "IaaSOperationStalled"
	ReasonIaaSUnknownError          = "IaaSUnknownError"
)

// Class says what an IaaS error means for retry policy.
//
// Terminal means retrying the same call will never succeed on its own - a
// human has to raise a quota or fix a permission. Everything else is treated
// as transient, deliberately including the unknown case: labelling a
// transient error terminal is the more expensive mistake, because it stops
// the driver from recovering by itself.
type Class struct {
	Terminal bool
	Reason   string
}

var (
	// Quota exhaustion: the request is well-formed, the account is simply full.
	errSetQuotaExceeded = lset.NewSet[lsdkErrs.ErrorCode](
		lsdkErrs.EcVServerVolumeExceedQuota,
	)
	errSetSizeQuotaExceeded = lset.NewSet[lsdkErrs.ErrorCode](
		lsdkErrs.EcVServerVolumeSizeExceedGlobalQuota,
	)
	errSetAttachQuotaExceeded = lset.NewSet[lsdkErrs.ErrorCode](
		lsdkErrs.EcVServerServerVolumeAttachQuotaExceeded,
	)

	// The IaaS is mid-operation on this volume - it will clear on its own.
	errSetOperationStalled = lset.NewSet[lsdkErrs.ErrorCode](
		lsdkErrs.EcVServerVolumeInProcess,
		lsdkErrs.EcVServerVolumeIsMigrating,
	)
)

// Classify maps a driver-level error onto a retry class and an event reason.
//
// The input is lserr.IError - what DetachVolume and the create path actually
// return - not the raw SDK error, so both driver-defined conditions (an
// ERROR-state volume) and wrapped SDK codes are visible here.
func Classify(perr lserr.IError) Class {
	if perr == nil {
		return Class{}
	}

	code := perr.GetErrorCode()

	switch {
	case errSetQuotaExceeded.ContainsOne(code):
		return Class{Terminal: true, Reason: ReasonVolumeQuotaExceeded}
	case errSetSizeQuotaExceeded.ContainsOne(code):
		return Class{Terminal: true, Reason: ReasonVolumeSizeQuotaExceeded}
	case errSetAttachQuotaExceeded.ContainsOne(code):
		return Class{Terminal: true, Reason: ReasonVolumeAttachQuotaExceeded}
	case code == lserr.EcVServerVolumeIsInErrorState:
		return Class{Terminal: true, Reason: ReasonVolumeInErrorState}
	}

	// 429 must be checked BEFORE 403: the SDK flattens both into
	// EcPermissionDenied, and only the raw statusCode tells them apart. Getting
	// this order wrong turns quota exhaustion into a permanent "permission
	// denied" - the exact misreading that once misdirected an lb-controller
	// incident diagnosis.
	if isThrottledStatus(perr, lhttp.StatusTooManyRequests) {
		return Class{Reason: ReasonIaaSThrottled}
	}
	if isThrottledStatus(perr, lhttp.StatusForbidden) {
		return Class{Terminal: true, Reason: ReasonIaaSPermissionDenied}
	}

	switch {
	case code == lsdkErrs.EcInternalServerError || code == lsdkErrs.EcServiceMaintenance:
		return Class{Reason: ReasonIaaSServerError}
	case errSetOperationStalled.ContainsOne(code):
		return Class{Reason: ReasonIaaSOperationStalled}
	case code == ecCsiClientRateLimited:
		return Class{Reason: ReasonIaaSThrottled}
	}

	return Class{Reason: ReasonIaaSUnknownError}
}

// isThrottledStatus reads the raw statusCode the SDK stashes in the error
// parameters. Same access path isThrottled() uses in ratelimit.go.
func isThrottledStatus(perr lserr.IError, pstatus int) bool {
	raw, ok := perr.GetParameters()["statusCode"]
	if !ok {
		return false
	}

	status, ok := raw.(int)

	return ok && status == pstatus
}
```

- [ ] **Step 4: Confirm the three borrowed names (already verified, 06/09/2026)**

The implementation above leans on three existing names. They were checked when this plan was written; re-run to be sure nothing moved:

```bash
cd /home/tytv2/sources/vngcloud-blockstorage-csi-driver
grep -n 'EcVServerVolumeIsInErrorState' pkg/cloud/errors/*.go   # the code ErrVolumeIsInErrorState sets
grep -n 'func NewError' pkg/cloud/errors/errors.go              # used by the test helpers
sed -n '1,10p' pkg/cloud/errors/ierrors.go                      # IError embeds lsdkErrs.IError
```

Expected: `EcVServerVolumeIsInErrorState` exists in `pkg/cloud/errors`; `NewError(psdkErr lsdkErrs.IError) IError` exists; `lserr.IError` is `interface { lsdkErrs.IError }`, so it inherits `GetErrorCode()` and `GetParameters()` from the SDK interface — that is why `Classify` can read both the driver's own codes and the wrapped SDK ones through a single parameter. If any of these has moved, adjust the code and do not invent a replacement name.

- [ ] **Step 5: Run test to verify it passes**

Run: `/home/tytv2/sdk/go1.25/bin/go test ./pkg/cloud/ -run TestClassify -v`
Expected: PASS, all subtests.

- [ ] **Step 6: Verify the 429-before-403 ordering is load-bearing**

Swap the two `isThrottledStatus` blocks so 403 is checked first, re-run, confirm the "429 is transient" subtest FAILS, then swap back and confirm PASS. A test that passes either way is not protecting the ordering.

Run: `/home/tytv2/sdk/go1.25/bin/go test ./pkg/cloud/ -run TestClassify -v`

- [ ] **Step 7: Commit**

```bash
git add pkg/cloud/errclass.go pkg/cloud/errclass_test.go
git commit -m "feat(cloud): classify IaaS errors into retry class and event reason"
```

---

### Task 2: Keyed circuit-breaker

**Files:**
- Create: `pkg/driver/internal/breaker.go`
- Create: `pkg/driver/internal/breaker_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks (pure logic, stdlib only).
- Produces: `type Decision int` with constants `Full` and `ProbeOnly`; `func NewBreaker() *Breaker`; methods `Allow(pkey string, pnow ltime.Time) Decision`, `Failure(pkey string, pterminal bool, pnow ltime.Time) (tripped bool, stepped bool)`, `Success(pkey string)`, `Since(pkey string, pnow ltime.Time) (ltime.Duration, bool)`. Exported constants `BreakerTripAfter` (int) and `BreakerSteps` (`[]ltime.Duration`).

- [ ] **Step 1: Write the failing test**

Create `pkg/driver/internal/breaker_test.go`:

```go
package internal

import (
	lsync "sync"
	ltesting "testing"
	ltime "time"
)

const bkKey = "vol-1ins-1"

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

	for i := 0; i < BreakerTripAfter; i++ {
		b.Failure("vol-1ins-1", false, at)
	}

	if got := b.Allow("vol-1ins-2", at); got != Full {
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/home/tytv2/sdk/go1.25/bin/go test ./pkg/driver/internal/ -run TestBreaker`
Expected: FAIL — build error `undefined: NewBreaker`, `undefined: BreakerTripAfter`.

- [ ] **Step 3: Write minimal implementation**

Create `pkg/driver/internal/breaker.go`:

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/home/tytv2/sdk/go1.25/bin/go test ./pkg/driver/internal/ -run TestBreaker -race -count=3 -v`
Expected: PASS, all tests, no race.

- [ ] **Step 5: Verify the cap test is load-bearing**

Temporarily change the last branch of `Failure` to `e.step++` unconditionally (removing the cap), re-run `TestBreakerWalksStepsAndHoldsAtCap`, confirm it FAILS with "stepped past the last step", then restore.

Run: `/home/tytv2/sdk/go1.25/bin/go test ./pkg/driver/internal/ -run TestBreakerWalksStepsAndHoldsAtCap -v`

- [ ] **Step 6: Commit**

```bash
git add pkg/driver/internal/breaker.go pkg/driver/internal/breaker_test.go
git commit -m "feat(driver): add per-volume-node detach circuit-breaker"
```

---

### Task 3: PV/PVC event helpers

**Files:**
- Modify: `pkg/k8s/ik8s.go` (add three methods to the `IKubernetes` interface)
- Modify: `pkg/k8s/k8s.go` (implement them)
- Create: `pkg/k8s/k8s_event_test.go`

**Interfaces:**
- Consumes: existing `lsentity.NewPersistentVolume`, `lserr.ErrK8sPvNotFound`, `lserr.ErrK8sPvFailedToGet`, the embedded `lk8s.Interface` and `lk8srecord.EventRecorder` on the `kubernetes` struct.
- Produces: on `IKubernetes` — `FindPersistentVolumeByHandle(pctx lctx.Context, phandle string) (*lsentity.PersistentVolume, lserr.IError)`, `VolumeEventWarning(pctx lctx.Context, ppvName, preason, pmessage string)`, `VolumeEventNormal(pctx lctx.Context, ppvName, preason, pmessage string)`.

- [ ] **Step 1: Write the failing test**

Create `pkg/k8s/k8s_event_test.go`:

```go
package k8s

import (
	lctx "context"
	ltesting "testing"

	lcoreV1 "k8s.io/api/core/v1"
	lmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	lfake "k8s.io/client-go/kubernetes/fake"
	lk8srecord "k8s.io/client-go/tools/record"
)

func pvWithHandle(pname, phandle string, pclaim *lcoreV1.ObjectReference) *lcoreV1.PersistentVolume {
	return &lcoreV1.PersistentVolume{
		ObjectMeta: lmetav1.ObjectMeta{Name: pname},
		Spec: lcoreV1.PersistentVolumeSpec{
			ClaimRef: pclaim,
			PersistentVolumeSource: lcoreV1.PersistentVolumeSource{
				CSI: &lcoreV1.CSIPersistentVolumeSource{
					Driver:       "bs.csi.vngcloud.vn",
					VolumeHandle: phandle,
				},
			},
		},
	}
}

func TestFindPersistentVolumeByHandle(t *ltesting.T) {
	client := lfake.NewSimpleClientset(
		pvWithHandle("pv-a", "vol-aaa", nil),
		pvWithHandle("pv-b", "vol-bbb", nil),
	)
	k := NewKubernetes(client, lk8srecord.NewFakeRecorder(10))

	pv, ierr := k.FindPersistentVolumeByHandle(lctx.Background(), "vol-bbb")
	if ierr != nil {
		t.Fatalf("FindPersistentVolumeByHandle() error = %v", ierr)
	}
	if pv == nil || pv.PersistentVolume.Name != "pv-b" {
		t.Fatalf("found %+v, want pv-b", pv)
	}
}

func TestFindPersistentVolumeByHandleMissingIsNotFatal(t *ltesting.T) {
	client := lfake.NewSimpleClientset(pvWithHandle("pv-a", "vol-aaa", nil))
	k := NewKubernetes(client, lk8srecord.NewFakeRecorder(10))

	pv, ierr := k.FindPersistentVolumeByHandle(lctx.Background(), "vol-zzz")
	if pv != nil {
		t.Fatalf("found %+v for an unknown handle, want nil", pv)
	}
	if ierr == nil {
		t.Fatal("want a not-found error so the caller can tell it apart from a hit")
	}
}

// The volume that stayed stuck for 23 hours had already lost its PVC: the PV
// was Released. So the PV is the reliable anchor and must always get the event.
func TestVolumeEventWarningOnPVOnlyWhenClaimGone(t *ltesting.T) {
	client := lfake.NewSimpleClientset(pvWithHandle("pv-a", "vol-aaa", nil))
	rec := lk8srecord.NewFakeRecorder(10)
	k := NewKubernetes(client, rec)

	k.VolumeEventWarning(lctx.Background(), "pv-a", "VolumeDetachStalled", "detach paused")

	select {
	case ev := <-rec.Events:
		if !containsAll(ev, "Warning", "VolumeDetachStalled", "detach paused") {
			t.Fatalf("event = %q, missing expected parts", ev)
		}
	default:
		t.Fatal("no event recorded for the PV")
	}

	select {
	case ev := <-rec.Events:
		t.Fatalf("a second event was recorded with no PVC to attach it to: %q", ev)
	default:
	}
}

func TestVolumeEventWarningAlsoOnPVCWhenPresent(t *ltesting.T) {
	claim := &lcoreV1.ObjectReference{Namespace: "app", Name: "data"}
	client := lfake.NewSimpleClientset(
		pvWithHandle("pv-a", "vol-aaa", claim),
		&lcoreV1.PersistentVolumeClaim{ObjectMeta: lmetav1.ObjectMeta{Namespace: "app", Name: "data"}},
	)
	rec := lk8srecord.NewFakeRecorder(10)
	k := NewKubernetes(client, rec)

	k.VolumeEventWarning(lctx.Background(), "pv-a", "VolumeDetachStalled", "detach paused")

	got := 0
	for i := 0; i < 2; i++ {
		select {
		case <-rec.Events:
			got++
		default:
		}
	}
	if got != 2 {
		t.Fatalf("recorded %d events, want 2 (PV and PVC)", got)
	}
}

// An event must never be able to break the operation that triggered it.
func TestVolumeEventWarningOnUnknownPVIsSilent(t *ltesting.T) {
	client := lfake.NewSimpleClientset()
	rec := lk8srecord.NewFakeRecorder(10)
	k := NewKubernetes(client, rec)

	k.VolumeEventWarning(lctx.Background(), "pv-missing", "VolumeDetachStalled", "detach paused")

	select {
	case ev := <-rec.Events:
		t.Fatalf("event recorded for a PV that does not exist: %q", ev)
	default:
	}
}

func containsAll(phaystack string, pneedles ...string) bool {
	for _, n := range pneedles {
		found := false
		for i := 0; i+len(n) <= len(phaystack); i++ {
			if phaystack[i:i+len(n)] == n {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/home/tytv2/sdk/go1.25/bin/go test ./pkg/k8s/ -run 'TestFindPersistentVolumeByHandle|TestVolumeEvent'`
Expected: FAIL — build error: `k.FindPersistentVolumeByHandle undefined`, `k.VolumeEventWarning undefined`.

- [ ] **Step 3: Add the methods to the interface**

In `pkg/k8s/ik8s.go`, inside `type IKubernetes interface`, after the existing `PersistentVolumeEventNormal` line:

```go
	// Volume-centric events: emit on the PV (always present while the volume
	// exists) and additionally on the PVC when it is still around. A stuck
	// detach usually outlives its PVC, so the PV is the anchor.
	FindPersistentVolumeByHandle(pctx lctx.Context, phandle string) (*lsentity.PersistentVolume, lserr.IError)
	VolumeEventWarning(pctx lctx.Context, ppvName, preason, pmessage string)
	VolumeEventNormal(pctx lctx.Context, ppvName, preason, pmessage string)
```

- [ ] **Step 4: Implement them**

Append to `pkg/k8s/k8s.go`:

```go
// FindPersistentVolumeByHandle locates the PV backing an IaaS volume ID.
//
// This is a full LIST of PersistentVolumes. That is affordable only because
// callers use it at a handful of moments per stuck volume (breaker trip, each
// backoff step, recovery) - never once per retry. Keep it that way.
func (s *kubernetes) FindPersistentVolumeByHandle(pctx lctx.Context, phandle string) (*lsentity.PersistentVolume, lserr.IError) {
	if phandle == "" {
		return nil, lserr.ErrK8sPvNotFound(phandle)
	}

	pvs, err := s.CoreV1().PersistentVolumes().List(pctx, lmetav1.ListOptions{})
	if err != nil {
		return nil, lserr.ErrK8sPvFailedToGet(phandle, err)
	}

	for i := range pvs.Items {
		csi := pvs.Items[i].Spec.CSI
		if csi != nil && csi.VolumeHandle == phandle {
			return lsentity.NewPersistentVolume(&pvs.Items[i]), nil
		}
	}

	return nil, lserr.ErrK8sPvNotFound(phandle)
}

// VolumeEventWarning emits a Warning on the PV, and on its PVC when that still
// exists. Failures are logged and swallowed: telling someone about a problem
// must never become a second problem.
func (s *kubernetes) VolumeEventWarning(pctx lctx.Context, ppvName, preason, pmessage string) {
	s.volumeEvent(pctx, ppvName, lcoreV1.EventTypeWarning, preason, pmessage)
}

// VolumeEventNormal emits a Normal event the same way - used to report that a
// pair which had been stuck finally detached.
func (s *kubernetes) VolumeEventNormal(pctx lctx.Context, ppvName, preason, pmessage string) {
	s.volumeEvent(pctx, ppvName, lcoreV1.EventTypeNormal, preason, pmessage)
}

func (s *kubernetes) volumeEvent(pctx lctx.Context, ppvName, peventType, preason, pmessage string) {
	if ppvName == "" {
		return
	}

	pv, ierr := s.GetPersistentVolume(pctx, ppvName)
	if ierr != nil || pv == nil || pv.PersistentVolume == nil {
		llog.V(2).InfoS("[DEBUG] - volumeEvent: PV not available, skipping event",
			"pv", ppvName, "reason", preason)

		return
	}
	s.EventRecorder.Event(pv.PersistentVolume, peventType, preason, pmessage)

	claim := pv.PersistentVolume.Spec.ClaimRef
	if claim == nil || claim.Name == "" {
		return
	}

	pvc, ierr := s.GetPersistentVolumeClaimByName(pctx, claim.Namespace, claim.Name)
	if ierr != nil || pvc == nil || pvc.PersistentVolumeClaim == nil {
		llog.V(2).InfoS("[DEBUG] - volumeEvent: PVC gone, event emitted on PV only",
			"pv", ppvName, "namespace", claim.Namespace, "name", claim.Name)

		return
	}
	s.EventRecorder.Event(pvc.PersistentVolumeClaim, peventType, preason, pmessage)
}
```

Add `llog "k8s.io/klog/v2"` to the import block of `pkg/k8s/k8s.go` if it is not already there.

- [ ] **Step 5: Check the entity field names this task assumes**

`volumeEvent` reads `pv.PersistentVolume.Spec.ClaimRef` and `pvc.PersistentVolumeClaim`.

Run:
```bash
cd /home/tytv2/sources/vngcloud-blockstorage-csi-driver
sed -n '1,20p' pkg/cloud/entity/persistentvolume.go
sed -n '1,25p' pkg/cloud/entity/persistentvolumeclaim.go
```

If the embedded field is named differently, adjust the accesses. Do not guess.

- [ ] **Step 6: Run test to verify it passes**

Run: `/home/tytv2/sdk/go1.25/bin/go test ./pkg/k8s/ -run 'TestFindPersistentVolumeByHandle|TestVolumeEvent' -v`
Expected: PASS, all five tests.

- [ ] **Step 7: Commit**

```bash
git add pkg/k8s/ik8s.go pkg/k8s/k8s.go pkg/k8s/k8s_event_test.go
git commit -m "feat(k8s): emit volume events on the PV and on the PVC when it exists"
```

---

### Task 4: Gauge support in the metric recorder

**Files:**
- Modify: `pkg/metrics/metrics.go`
- Create: `pkg/metrics/metrics_gauge_test.go`

**Interfaces:**
- Consumes: existing private helpers `registerCounterVec`, `createCounterVec`, the `m.metrics` map and `m.registry`.
- Produces: on `*metricRecorder` — `SetGauge(pname string, pvalue float64, plabels map[string]string)` and `DeleteGauge(pname string, plabels map[string]string)`.

- [ ] **Step 1: Write the failing test**

Create `pkg/metrics/metrics_gauge_test.go`:

```go
package metrics

import (
	ltesting "testing"
)

func TestSetGaugeThenDelete(t *ltesting.T) {
	r := InitializeRecorder()
	labels := map[string]string{"volume_id": "vol-1", "node_id": "ins-1"}

	r.SetGauge("vcontainer_csi_test_pending_seconds", 42, labels)
	if got := gaugeValue(t, r, "vcontainer_csi_test_pending_seconds", labels); got != 42 {
		t.Fatalf("gauge = %v, want 42", got)
	}

	// Overwriting the same series must replace, not accumulate.
	r.SetGauge("vcontainer_csi_test_pending_seconds", 100, labels)
	if got := gaugeValue(t, r, "vcontainer_csi_test_pending_seconds", labels); got != 100 {
		t.Fatalf("gauge after re-set = %v, want 100", got)
	}

	// Deleting must remove the series, otherwise a recovered volume keeps
	// reporting a stale "stuck for N seconds" forever.
	r.DeleteGauge("vcontainer_csi_test_pending_seconds", labels)
	if gaugeExists(t, r, "vcontainer_csi_test_pending_seconds", labels) {
		t.Fatal("series still present after DeleteGauge")
	}
}

// SetGauge on an unknown metric name must register it once and not panic when
// called again.
func TestSetGaugeIsIdempotentOnRegistration(t *ltesting.T) {
	r := InitializeRecorder()
	labels := map[string]string{"volume_id": "vol-1", "node_id": "ins-1"}

	r.SetGauge("vcontainer_csi_test_twice", 1, labels)
	r.SetGauge("vcontainer_csi_test_twice", 2, labels)

	if got := gaugeValue(t, r, "vcontainer_csi_test_twice", labels); got != 2 {
		t.Fatalf("gauge = %v, want 2", got)
	}
}
```

- [ ] **Step 2: Write the test helpers**

Append to `pkg/metrics/metrics_gauge_test.go`. These read the registry so the test asserts on published output, not on internal bookkeeping:

```go
import (
	ldto "github.com/prometheus/client_model/go"
)

func gaugeValue(t *ltesting.T, r *metricRecorder, pname string, plabels map[string]string) float64 {
	t.Helper()

	mf := gatherFamily(t, r, pname)
	if mf == nil {
		t.Fatalf("metric family %q not published", pname)
	}
	for _, m := range mf.GetMetric() {
		if labelsMatch(m, plabels) {
			return m.GetGauge().GetValue()
		}
	}
	t.Fatalf("no series in %q matching %v", pname, plabels)

	return 0
}

func gaugeExists(t *ltesting.T, r *metricRecorder, pname string, plabels map[string]string) bool {
	t.Helper()

	mf := gatherFamily(t, r, pname)
	if mf == nil {
		return false
	}
	for _, m := range mf.GetMetric() {
		if labelsMatch(m, plabels) {
			return true
		}
	}

	return false
}

func gatherFamily(t *ltesting.T, r *metricRecorder, pname string) *ldto.MetricFamily {
	t.Helper()

	families, err := r.registry.Gather()
	if err != nil {
		t.Fatalf("registry.Gather() error = %v", err)
	}
	for _, f := range families {
		if f.GetName() == pname {
			return f
		}
	}

	return nil
}

func labelsMatch(pm *ldto.Metric, plabels map[string]string) bool {
	if len(pm.GetLabel()) != len(plabels) {
		return false
	}
	for _, l := range pm.GetLabel() {
		if plabels[l.GetName()] != l.GetValue() {
			return false
		}
	}

	return true
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `/home/tytv2/sdk/go1.25/bin/go test ./pkg/metrics/ -run TestSetGauge`
Expected: FAIL — `r.SetGauge undefined`.

If `registry.Gather()` is not available on the registry type used by `metrics.NewKubeRegistry()`, inspect what it does expose (`go doc k8s.io/component-base/metrics.KubeRegistry`) and adapt the helpers; keep asserting on published output rather than on `m.metrics`.

- [ ] **Step 4: Write minimal implementation**

Add to `pkg/metrics/metrics.go`, next to `IncreaseCount`/`ObserveHistogram`:

```go
// SetGauge publishes an absolute value for one label set, registering the
// metric on first use. Unlike the counters here, a gauge must be able to go
// down and to disappear - see DeleteGauge.
func (m *metricRecorder) SetGauge(pname string, pvalue float64, plabels map[string]string) {
	if m == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			klog.V(2).InfoS("[DEBUG] - SetGauge: recovered", "metric", pname, "err", r)
		}
	}()

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, exists := m.metrics[pname]; !exists {
		m.registerGaugeVec(pname, "vngcloud blockstorage csi metric", getLabelNames(plabels))
	}

	gauge, ok := m.metrics[pname].(*metrics.GaugeVec)
	if !ok {
		return
	}
	gauge.With(plabels).Set(pvalue)
}

// DeleteGauge drops one series. Needed because a recovered volume must stop
// reporting "stuck for N seconds" - a stale series would alert forever.
func (m *metricRecorder) DeleteGauge(pname string, plabels map[string]string) {
	if m == nil {
		return
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	gauge, ok := m.metrics[pname].(*metrics.GaugeVec)
	if !ok {
		return
	}
	gauge.Delete(plabels)
}

func (m *metricRecorder) registerGaugeVec(name, help string, labels []string) {
	if _, exists := m.metrics[name]; exists {
		return
	}
	gauge := createGaugeVec(name, help, labels)
	m.metrics[name] = gauge
	m.registry.MustRegister(gauge)
}

func createGaugeVec(name, help string, labels []string) *metrics.GaugeVec {
	return metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Name:           name,
			Help:           help,
			StabilityLevel: metrics.ALPHA,
		},
		labels,
	)
}
```

- [ ] **Step 5: Match the surrounding style**

Read `IncreaseCount` and `ObserveHistogram` (`pkg/metrics/metrics.go:21-70`) and make `SetGauge` consistent with them: same mutex field name (it may not be `m.mutex`), same panic-recovery pattern or lack of it, same `klog` alias. If those methods do not lock, do not add locking only here — follow the file.

Run: `sed -n '18,75p' pkg/metrics/metrics.go`

- [ ] **Step 6: Run test to verify it passes**

Run: `/home/tytv2/sdk/go1.25/bin/go test ./pkg/metrics/ -run TestSetGauge -v`
Expected: PASS both tests.

- [ ] **Step 7: Commit**

```bash
git add pkg/metrics/metrics.go pkg/metrics/metrics_gauge_test.go
git commit -m "feat(metrics): add gauge support to the recorder"
```

---

### Task 5: Wire the breaker into ControllerUnpublishVolume

**Files:**
- Modify: `pkg/cloud/icloud.go` (add `IsDetachedFrom` to the `Cloud` interface)
- Modify: `pkg/cloud/cloud.go` (implement it)
- Modify: `pkg/driver/controller.go` (`controllerService` struct, `newControllerService`, `ControllerUnpublishVolume`)
- Modify: `pkg/driver/constants.go` (metric name constants)

**Interfaces:**
- Consumes: `Classify`/`Reason*` (Task 1), `NewBreaker`/`Allow`/`Failure`/`Success`/`Since`/`Full`/`ProbeOnly` (Task 2), `VolumeEventWarning`/`VolumeEventNormal`/`FindPersistentVolumeByHandle` (Task 3), `SetGauge`/`DeleteGauge` (Task 4).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add `IsDetachedFrom` to the cloud layer**

In `pkg/cloud/icloud.go`, add to the `Cloud` interface next to `DetachVolume`:

```go
	// IsDetachedFrom reads whether the volume is already off this instance,
	// without commanding anything. Used while a detach is paused by the
	// circuit-breaker, so recovery is still noticed promptly.
	IsDetachedFrom(pctx lctx.Context, pinstanceId, pvolumeId string) (bool, lserr.IError)
```

In `pkg/cloud/cloud.go`, add:

```go
// IsDetachedFrom is read-only: no DetachBlockVolume, one GetVolume.
//
// A volume that no longer exists counts as detached, matching errSetDetachDone
// - once the volume is gone there is nothing left to detach, and reporting an
// error would keep external-attacher from removing the finalizer.
func (s *cloud) IsDetachedFrom(pctx lctx.Context, pinstanceId, pvolumeId string) (bool, lserr.IError) {
	vol, ierr := s.getVolumeById(pvolumeId)
	if ierr != nil {
		if ierr.IsError(lsdkErrs.EcVServerVolumeNotFound) {
			return true, nil
		}

		return false, ierr
	}

	return isDetachedFrom(vol, pinstanceId), nil
}
```

- [ ] **Step 2: Add metric name constants**

In `pkg/driver/constants.go`, inside the existing `const` block:

```go
	// Gauge: seconds a (volume, node) pair has been failing to detach, measured
	// from its first failure. This is the alertable one - "stuck > 30m" - and
	// the direct equivalent of aws-ebs-csi-driver's
	// ec2_detach_pending_seconds_total. Only the leader emits it, because only
	// the leader holds breaker state.
	MetricDetachPendingSeconds = "vcontainer_csi_volume_detach_pending_seconds"

	// Counter: breaker trips and backoff-step increases, by event reason.
	MetricDetachBreakerTrips = "vcontainer_csi_detach_breaker_trips_total"

	// Counter: every classified IaaS error, by operation and reason.
	MetricIaaSErrors = "vcontainer_csi_iaas_errors_total"
```

- [ ] **Step 3: Add the breaker to the service**

In `pkg/driver/controller.go`, add a field to `controllerService` after `createGate`:

```go
	detachBreaker       *lsinternal.Breaker
```

And in `newControllerService`'s returned struct, after the `createGate` line:

```go
		detachBreaker:       lsinternal.NewBreaker(),
```

- [ ] **Step 4: Replace the detach call in `ControllerUnpublishVolume`**

Replace this block:

```go
	if ierr := s.cloud.DetachVolume(pctx, nodeID, volumeID); ierr != nil {
		llog.ErrorS(ierr.GetError(), "[ERROR] - ControllerUnpublishVolume: Failed to detach volume from instance", "volumeID", volumeID, "nodeID", nodeID)
		return nil, ErrDetachVolume(volumeID, nodeID)
	}

	llog.InfoS("[INFO] - ControllerUnpublishVolume: Volume detached from instance successfully", "volumeID", volumeID, "nodeID", nodeID)
	return &lcsi.ControllerUnpublishVolumeResponse{}, nil
```

with:

```go
	now := ltime.Now()

	// While the breaker is open we stop commanding the IaaS and only read
	// state. A stuck detach used to cost ~13 vServer calls every 6 minutes for
	// as long as it stayed stuck (23 hours, ~3,000 calls, in the incident this
	// guards against), all from a quota bucket shared across the project.
	if s.detachBreaker.Allow(key, now) == lsinternal.ProbeOnly {
		detached, ierr := s.cloud.IsDetachedFrom(pctx, nodeID, volumeID)
		if ierr == nil && detached {
			s.onDetachSucceeded(pctx, volumeID, nodeID, key, now)

			return &lcsi.ControllerUnpublishVolumeResponse{}, nil
		}

		// A failed probe says nothing about whether the IaaS accepts commands,
		// so it must NOT advance the backoff.
		llog.InfoS("[INFO] - ControllerUnpublishVolume: detach paused by breaker, still attached",
			"volumeID", volumeID, "nodeID", nodeID)

		return nil, ErrDetachVolume(volumeID, nodeID)
	}

	if ierr := s.cloud.DetachVolume(pctx, nodeID, volumeID); ierr != nil {
		llog.ErrorS(ierr.GetError(), "[ERROR] - ControllerUnpublishVolume: Failed to detach volume from instance", "volumeID", volumeID, "nodeID", nodeID)
		s.onDetachFailed(pctx, volumeID, nodeID, key, now, ierr)

		return nil, ErrDetachVolume(volumeID, nodeID)
	}

	s.onDetachSucceeded(pctx, volumeID, nodeID, key, now)
	llog.InfoS("[INFO] - ControllerUnpublishVolume: Volume detached from instance successfully", "volumeID", volumeID, "nodeID", nodeID)

	return &lcsi.ControllerUnpublishVolumeResponse{}, nil
```

- [ ] **Step 5: Add the two bookkeeping helpers**

Append to `pkg/driver/controller.go`:

```go
// onDetachFailed records a real failed attempt, and reports it once per
// escalation rather than once per retry - the incident this guards against
// would have produced 5 events instead of ~230.
func (s *controllerService) onDetachFailed(
	pctx lctx.Context, pvolumeID, pnodeID, pkey string, pnow ltime.Time, pierr lserr.IError,
) {
	cls := lscloud.Classify(pierr)
	tripped, stepped := s.detachBreaker.Failure(pkey, cls.Terminal, pnow)

	lsmetrics.Recorder().IncreaseCount(MetricIaaSErrors, map[string]string{
		"op": "detach", "reason": cls.Reason,
	})

	if stuck, ok := s.detachBreaker.Since(pkey, pnow); ok {
		lsmetrics.Recorder().SetGauge(MetricDetachPendingSeconds, stuck.Seconds(), map[string]string{
			"volume_id": pvolumeID, "node_id": pnodeID,
		})
	}

	if !tripped && !stepped {
		return
	}

	lsmetrics.Recorder().IncreaseCount(MetricDetachBreakerTrips, map[string]string{
		"reason": cls.Reason,
	})

	stuck, _ := s.detachBreaker.Since(pkey, pnow)
	msg := lfmt.Sprintf(
		"Detach %s from %s keeps failing (%s, stuck for %s). Pausing IaaS detach calls; state will still be probed on each retry.",
		pvolumeID, pnodeID, cls.Reason, stuck.Round(ltime.Second),
	)
	s.emitVolumeEvent(pctx, pvolumeID, lcoreV1.EventTypeWarning, "VolumeDetachStalled", msg)
}

// onDetachSucceeded clears the breaker, drops the gauge series so nothing
// keeps alerting, and says so out loud if the pair had been stuck.
func (s *controllerService) onDetachSucceeded(
	pctx lctx.Context, pvolumeID, pnodeID, pkey string, pnow ltime.Time,
) {
	stuck, wasTracked := s.detachBreaker.Since(pkey, pnow)
	s.detachBreaker.Success(pkey)

	lsmetrics.Recorder().DeleteGauge(MetricDetachPendingSeconds, map[string]string{
		"volume_id": pvolumeID, "node_id": pnodeID,
	})

	if !wasTracked {
		return
	}

	msg := lfmt.Sprintf("Detach %s from %s succeeded after being stuck for %s.",
		pvolumeID, pnodeID, stuck.Round(ltime.Second))
	s.emitVolumeEvent(pctx, pvolumeID, lcoreV1.EventTypeNormal, "VolumeDetachRecovered", msg)
}

// emitVolumeEvent resolves the IaaS volume ID to its PV and emits there (and on
// the PVC if it still exists). Every failure is swallowed: reporting a problem
// must never create one.
func (s *controllerService) emitVolumeEvent(pctx lctx.Context, pvolumeID, peventType, preason, pmessage string) {
	pv, ierr := s.k8sClient.FindPersistentVolumeByHandle(pctx, pvolumeID)
	if ierr != nil || pv == nil || pv.PersistentVolume == nil {
		llog.V(2).InfoS("[DEBUG] - emitVolumeEvent: no PV for this volume, skipping event",
			"volumeID", pvolumeID, "reason", preason)

		return
	}

	if peventType == lcoreV1.EventTypeNormal {
		s.k8sClient.VolumeEventNormal(pctx, pv.PersistentVolume.Name, preason, pmessage)

		return
	}
	s.k8sClient.VolumeEventWarning(pctx, pv.PersistentVolume.Name, preason, pmessage)
}
```

Add the imports these helpers need to `pkg/driver/controller.go` if absent: `lcoreV1 "k8s.io/api/core/v1"`, `lsmetrics ".../pkg/metrics"`. `lfmt`, `ltime`, `llog`, `lscloud`, `lserr`, `lsinternal` are already imported there.

- [ ] **Step 6: Build and run the whole suite**

Run:
```bash
/home/tytv2/sdk/go1.25/bin/go build ./... && \
/home/tytv2/sdk/go1.25/bin/go vet ./... && \
/home/tytv2/sdk/go1.25/bin/go test ./pkg/... -race
```
Expected: build OK, vet silent, all packages `ok`.

If `go build` fails on the `Cloud` interface, some other implementation (a fake in a test, `pkg/driver/sanity` if present) needs `IsDetachedFrom` too — add it there returning `(false, nil)`.

- [ ] **Step 7: Run the exact CI steps**

Run:
```bash
GO=/home/tytv2/sdk/go1.25/bin/go
$GO test -tags=unit $($GO list ./... | sed -e '/sanity/ { N; d; }' | sed -e '/tests/ {N; d;}')
```
Expected: `ok` for every package with tests.

- [ ] **Step 8: Commit**

```bash
git add pkg/cloud/icloud.go pkg/cloud/cloud.go pkg/driver/controller.go pkg/driver/constants.go
git commit -m "feat(driver): pause IaaS detach calls behind a breaker, report via event and metric"
```

---

### Task 6: Classified reason on the create-path event (PR-B)

**Files:**
- Modify: `pkg/driver/controller.go` (the create-failure event at roughly line 243-245)

**Interfaces:**
- Consumes: `Classify`, `Reason*` (Task 1), `MetricIaaSErrors` (Task 5).
- Produces: nothing.

- [ ] **Step 1: Read the current call site**

Run:
```bash
cd /home/tytv2/sources/vngcloud-blockstorage-csi-driver
grep -n 'CsiCreateVolumeFailure' -B4 -A2 pkg/driver/controller.go
```

Expect an existing `PersistentVolumeClaimEventWarning(..., "CsiCreateVolumeFailure", sdkErr.GetMessage())`. The driver already emits here — this task only sharpens the reason, it does not add a new emission path.

- [ ] **Step 2: Replace the literal reason with the classified one**

Change the event call so the reason comes from `Classify` and the error is also counted:

```go
		cls := lscloud.Classify(sdkErr)
		lsmetrics.Recorder().IncreaseCount(MetricIaaSErrors, map[string]string{
			"op": "create", "reason": cls.Reason,
		})
		// A specific reason is what makes this event actionable: "quota
		// exhausted" needs a human, "throttled" resolves itself.
		s.k8sClient.PersistentVolumeClaimEventWarning(pctx, cvr.PvcNamespaceTag, cvr.PvcNameTag,
			cls.Reason, sdkErr.GetMessage())
```

Keep the surrounding `llog.ErrorS` line as it is.

- [ ] **Step 3: Check the variable's type at that site**

`Classify` takes `lserr.IError`. If the variable there (`sdkErr`) is an `lsdkErrs.IError` rather than `lserr.IError`, wrap it: `lscloud.Classify(lserr.NewError(sdkErr))`. Confirm which it is before editing:

Run: `sed -n '230,250p' pkg/driver/controller.go`

- [ ] **Step 4: Build and test**

Run:
```bash
/home/tytv2/sdk/go1.25/bin/go build ./... && \
/home/tytv2/sdk/go1.25/bin/go vet ./... && \
/home/tytv2/sdk/go1.25/bin/go test ./pkg/... -race
```
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add pkg/driver/controller.go
git commit -m "feat(driver): use the classified reason for create-failure events"
```

---

### Task 7: Verify on the test cluster

**Files:** none (verification only). Runners already exist under `/home/tytv2/csi-perf/runner/`.

**Interfaces:**
- Consumes: the whole feature.
- Produces: evidence for the PR description.

- [ ] **Step 1: Push and open the PR**

```bash
cd /home/tytv2/sources/vngcloud-blockstorage-csi-driver
git push -u origin tytv2/detach-breaker-error-events
```

Then open a PR to `dev`. Title: `feat(driver): cap stuck detach retries and report them`. Body must cite the incident numbers (23h06m, ~230 rounds, ~13 calls per 6 minutes, ~3,000 calls) and state that the breaker never gives up.

- [ ] **Step 2: Wait for the dev image**

`build_dev.yml` builds on merge into `dev` (its trigger was fixed in PR #5/#9). The tag is `v0.0.0-dev-<short sha>`. Confirm:

```bash
gh run list --workflow=build_dev.yml --branch dev --limit 3 \
  --json status,conclusion,displayTitle \
  --jq '.[] | "\(.status)/\(.conclusion) \(.displayTitle[0:50])"'
```

- [ ] **Step 3: Deploy to the test cluster only**

Patch the image tag on the HCP that owns exactly one HRP. Verify the scope first — both HCPs share the same `clusterSelector`, so scope comes from the HRP's `helmchartproxy-name` label and ownerRef, never from the selector:

```bash
kubectl --kubeconfig /home/tytv2/kubeconfig/dev.yaml get hrp -A -o json | python3 -c "
import json,sys,collections
d=json.load(sys.stdin); c=collections.Counter()
for h in d['items']:
    for o in h['metadata'].get('ownerReferences',[]) or []:
        if o['kind']=='HelmChartProxy': c[o['name']]+=1
for k,v in sorted(c.items()):
    if 'block-store' in k: print(v, k)"
```

Expect `block-store-csi-helm-chart-117` to own 1. Then patch its `spec.valuesTemplate` image tag (pattern files live in `/home/tytv2/csi-perf/deploy/`) and wait for the rollout.

- [ ] **Step 4: Reproduce a stuck detach**

Replay the case that produced the incident — 10 volumes onto one node, which sits exactly at `VOLUME_PER_SERVER=10`:

```bash
cd /home/tytv2/csi-perf/runner
WL=/tmp/claude-1003/-home-tytv2/*/scratchpad/wl.yaml
./ts-b-rest.sh $WL B2 10 /home/tytv2/csi-perf/dev/$(date +%F)
```

Then delete the pod and watch. A stuck pair is not guaranteed — it happened once in ten volumes. If nothing sticks, the fallback is to confirm the negative case instead: a healthy detach must produce **no** event, **no** trip counter, and no lingering gauge series.

- [ ] **Step 5: Check what a stuck pair now produces**

```bash
K() { kubectl --kubeconfig $WL "$@"; }
# events on the PV (and PVC if it still exists)
K get events -A --field-selector reason=VolumeDetachStalled
K get events -A --field-selector reason=VolumeDetachRecovered
# the alertable gauge
POD=$(K get pods -n kube-system -o name | grep csi-controller | head -1)
K exec -n kube-system ${POD#pod/} -c vngcloud-plugin -- \
  wget -qO- http://localhost:8080/metrics 2>/dev/null | grep -E 'detach_pending_seconds|breaker_trips|iaas_errors'
```

Expected once a pair trips: one `VolumeDetachStalled` event, `detach_breaker_trips_total` at 1, `detach_pending_seconds` climbing, and — the point of the whole change — `DetachVolume: Detaching the volume` appearing in the driver log at 10-minute then 30-minute spacing instead of every 6 minutes.

Confirm the metrics port and path from the deployment's `--http-endpoint` arg before running the exec.

- [ ] **Step 6: Count the saving**

```bash
K logs -n kube-system ${POD#pod/} -c vngcloud-plugin --since=60m | \
  grep -c 'DetachVolume: Detaching the volume'
```

Over an hour a stuck pair should show ~4 real attempts (10m, 30m, 1h steps), against 10 before the change.

- [ ] **Step 7: Clean up and record**

Delete the test PVCs, confirm `PV=0 VA=0`, and add the numbers to the PR description plus `csi-perf/testsuite-design.md`.
