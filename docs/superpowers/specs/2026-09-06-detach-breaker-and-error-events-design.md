# Detach circuit-breaker and classified error events

Date: 2026-09-06 · Status: approved design, pending implementation plan
Scope: `vngcloud-blockstorage-csi-driver` controller, PR-A (detach breaker) and PR-B (create-path reasons)

## 1. Problem

Load testing on the dev farm (cluster `k8s-b08ab805`, 04–05/09/2026) exposed a volume
whose detach never completed. The controller re-issued `DetachBlockVolume` every 6 minutes
for **23 hours** (~230 rounds) before a controller restart cleared it:

```
14:59:13  ControllerUnpublishVolume -> DetachVolume: Detaching the volume
15:05:13  waitVolumeDetached: Gave up waiting  elapsed=5m59.9s
          GRPC error: CANNOT Detach volume ...
15:05:13  (CO retry immediately) -> Detaching the volume ...
```

Three consequences, in increasing order of cost:

1. The VolumeAttachment keeps its `deletionTimestamp` while `attached=true`; the PV stays
   `Released` behind the `external-attacher` finalizer; the volume lives on at the IaaS,
   still billed, and occupies one attach slot on the node.
2. Nobody was told. There is no event, no metric; the only trace is the driver log.
3. Every round costs ~13 vServer API calls (1 GetVolume + 1 DetachBlockVolume +
   ~11 polls from `volumeOperationBackoff`), ~3,000 calls over the incident, drawn from
   the **project-wide** quota bucket (1000 req/60s) shared with every other consumer.

The 6-minute cadence is not backoff. It is the external-attacher `--timeout=6m` cancelling
a request that the driver holds open while polling in `waitVolumeDetached`. Every retry is
therefore a fresh request, and the attacher's own exponential backoff never engages.

A second observation from the same incident: after ~230 failures with a controller whose
uptime was 20h+, the **first** attempt after a restart succeeded. n=1, but consistent with
client-side state (connection, token, SDK client) that a restart resets.

## 2. Goals

- Cap the cost of a stuck detach: stop sending `DetachBlockVolume` after repeated failures,
  fall back to a cheap read-only probe, and lengthen the interval between real attempts.
- Never stop for good. A stuck volume must still self-heal when the IaaS recovers, and a
  controller restart must reset the state (it is the observed remedy).
- Tell people. Emit a Kubernetes event when a detach trips the breaker, at each escalation,
  and on recovery; expose a metric that a dashboard can alert on ("stuck > 30m").
- Classify IaaS errors once, so the existing create-path event carries a specific reason
  (quota exhausted, throttled, server error) instead of the generic `CsiCreateVolumeFailure`.

## 3. Non-goals

- Attach-path breaker. Attach after provision *normally* takes 1–3 IN-PROCESS retries
  (measured p50 39s vs 9.5s real IaaS latency), so a naive trip threshold would fire on
  healthy traffic. Separate work with its own threshold; see backlog.
- Persisting breaker state. In-memory only; reset on restart is a feature.
- New event emission on the create path. The driver already emits a PVC event there
  (`controller.go:244`); PR-B only sharpens its reason.
- Runtime-aware `CSINode.allocatable`. Separate backlog item.

## 4. Design

### 4.1 Components

```
                 ControllerUnpublishVolume (modified)
                              │
        ┌─────────────────────┼────────────────────────┐
        ▼                     ▼                        ▼
 internal.Breaker      cloud.Classify()        k8s.VolumeEventWarning()
 when may we call      what is this error,     PV always, PVC if it
 the IaaS, when        which tier, which       still exists
 only probe            event reason
```

Each unit has one job and no knowledge of the others; only the handler composes them.

#### `pkg/cloud/errclass.go` — `Classify(err lserr.IError) Class`

Input is the driver's `lserr.IError` — the type `DetachVolume`/`EitherCreateResizeVolume`
return. It wraps the SDK error, so SDK codes are read through `IsErrorAny`/`GetErrorCode`
and the raw `statusCode` through the same parameter path `isThrottled` already uses. Driver-
defined conditions (volume in ERROR state) are matched by the driver's own error code, not an
SDK code.

```go
type Class struct {
    Terminal bool   // retrying will never help; trip immediately
    Reason   string // CamelCase, stable, used as the Kubernetes event reason
}
```

Built with the `lset.NewSet[lsdkErrs.ErrorCode]` pattern already used by `errSetDetachDone`
and `errSetDetachRetryable` in `consts.go`.

| Source | Reason | Terminal |
|---|---|---|
| `EcVServerVolumeExceedQuota` | `VolumeQuotaExceeded` | yes |
| `EcVServerVolumeSizeExceedGlobalQuota` | `VolumeSizeQuotaExceeded` | yes |
| `EcVServerServerVolumeAttachQuotaExceeded` | `VolumeAttachQuotaExceeded` | yes |
| statusCode 403 and **not** `isThrottled` | `IaaSPermissionDenied` | yes |
| driver code of `lserr.ErrVolumeIsInErrorState` | `VolumeInErrorState` | yes |
| `isThrottled` (429) or `ecCsiClientRateLimited` | `IaaSThrottled` | no |
| `isServerError` (5xx by SDK code, per PR #6) | `IaaSServerError` | no |
| `EcVServerVolumeInProcess`, `EcVServerVolumeIsMigrating`, `context.DeadlineExceeded` | `IaaSOperationStalled` | no |
| anything else | `IaaSUnknownError` | no |

Unknown is deliberately non-terminal: mislabelling a transient error as terminal is the
more expensive mistake. `Classify(nil)` returns the zero value; it never panics.

#### `pkg/driver/internal/breaker.go` — `Breaker`

Lives beside `InFlight` and `Semaphore`, same style. Keyed by `volumeID + nodeID`, the
same key the inflight cache uses.

```go
type Decision int
const (
    Full      Decision = iota // may issue DetachBlockVolume
    ProbeOnly                 // may only GetVolume to check state
)

func (b *Breaker) Allow(key string, now time.Time) Decision
func (b *Breaker) Failure(key string, terminal bool, now time.Time) (tripped, stepped bool)
func (b *Breaker) Success(key string)
func (b *Breaker) Since(key string, now time.Time) (time.Duration, bool) // for the gauge
```

Constants, with the measurements that justify them in comments:

- `tripAfter = 3` consecutive real failures (~18 minutes at the current cadence). A terminal
  class trips on the first failure.
- Backoff steps after tripping: `10m → 30m → 1h → 2h`, capped at the last value.
- A step ends when `now >= nextAttempt`; `Allow` then returns `Full` exactly once. Another
  failure advances the step; success deletes the entry.
- `Failure` is only called after a **real** attempt. A probe result never touches the breaker.

State is a `map[string]entry` under a mutex, in-memory, lost on restart — intentionally.

#### `pkg/k8s` — two additions to `IKubernetes`

```go
FindPersistentVolumeByHandle(ctx, handle string) (*lsentity.PersistentVolume, lserr.IError)
VolumeEventWarning(ctx, pvName, reason, message string)
VolumeEventNormal (ctx, pvName, reason, message string)
```

`FindPersistentVolumeByHandle` lists PVs and matches `spec.csi.volumeHandle`. It is a full
LIST; acceptable **only because** the breaker emits at three moments per stuck volume, not
per retry. `VolumeEvent*` emits on the PV, then reads `claimRef` and also emits on the PVC
if it still exists. Errors are logged at V(2) and swallowed — an event must never fail the
operation that triggered it. This matches how `PersistentVolumeEventWarning` already behaves.

#### `pkg/metrics` — gauge support

The recorder has counters and histograms only. Add `registerGaugeVec`, `SetGauge`,
`DeleteGauge` in the same style as the existing pair.

### 4.2 Flow in `ControllerUnpublishVolume`

```
validate → inflight.Insert(key)                      unchanged
d := breaker.Allow(key, now)
  Full      → cloud.DetachVolume(ctx, node, vol)      unchanged path
  ProbeOnly → cloud.IsDetachedFrom(ctx, node, vol)    one GetVolume, no command
outcome:
  success                     → breaker.Success(key); DeleteGauge; if it had been tripped:
                                 VolumeEventNormal(VolumeDetachRecovered); return OK
  ProbeOnly, still attached   → return ErrDetachVolume with a message naming the pause;
                                 breaker untouched
  Full, error                 → cls := Classify(err)
                                 tripped, stepped := breaker.Failure(key, cls.Terminal, now)
                                 if tripped || stepped:
                                     VolumeEventWarning(VolumeDetachStalled, ...)
                                     IncreaseCount(detach_breaker_trips_total{reason})
                                 SetGauge(detach_pending_seconds, Since(key))
                                 return ErrDetachVolume
```

`cloud.IsDetachedFrom` is a thin wrapper: `getVolumeById` + the existing `isDetachedFrom`;
`EcVServerVolumeNotFound` counts as detached, matching `errSetDetachDone`.

Edge cases considered:

- Same volume attached to another node while (vol, nodeA) is paused: different key, unaffected.
- Volume deleted at the IaaS during a pause: probe sees NotFound → done → Success. Self-heals.
- Probe runs in ~1s, well inside the attacher's 6m context. The 6-minute hold is gone.
- Leader change mid-pause: the new leader's breaker is empty; its first call is `Full`.
  Correct — the new leader deserves one real attempt (this is what fixed the incident).
- Wide IaaS outage: one entry per pair, no global breaker. The rate limiter and 5xx
  backpressure (PR #6) own the process-wide reaction; the breaker owns the specific pair.

### 4.3 Events

Reasons are stable CamelCase strings from `Classify`, plus two breaker-specific ones:
`VolumeDetachStalled` (Warning) and `VolumeDetachRecovered` (Normal).

Message = stable head + short variable tail, so Kubernetes aggregation still works within a
step while a human reading `describe` gets the numbers:

```
Detach vol-8a588643 from ins-1869d42a has failed 3 times over 18m (last: context deadline exceeded).
Pausing IaaS detach calls for 10m; will probe state on each retry.
```

Emission points for PR-A: first trip, each step increase, recovery. The 23-hour incident
would have produced 5 events instead of ~230.

PR-B: at `controller.go:244`, replace the literal reason `CsiCreateVolumeFailure` with
`Classify(sdkErr).Reason`. No new emission path.

### 4.4 Metrics

Prefix follows the existing `vcontainer_*` names.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `vcontainer_csi_volume_detach_pending_seconds` | gauge | `volume_id`, `node_id` | seconds since the first real failure of a pair still stuck; series deleted on success. The alertable one ("stuck > 30m"). Equivalent of EBS `ec2_detach_pending_seconds_total`. |
| `vcontainer_csi_detach_breaker_trips_total` | counter | `reason` | trips and step increases |
| `vcontainer_csi_iaas_errors_total` | counter | `op`, `reason` | every classified IaaS error; `op` ∈ create/attach/detach. PR-B uses it for create. |

Only the leader holds breaker state, so only the leader emits — same caveat EBS documents.

Cardinality of the gauge is bounded by the number of pairs *currently stuck*, which is
expected to be zero almost always and a handful during an incident; series are deleted on
success, so it cannot grow without bound.

## 5. Error handling

Observability must never damage the primary path:

- LIST PV or event emission fails → V(2) log, continue, return the detach result unchanged.
- `Classify` on unknown input → `{false, "IaaSUnknownError"}`; never panics.
- Probe fails with anything other than NotFound → return error to attacher, breaker untouched.
- No breaker map cleanup routine: entries leave on success or via the NotFound→done branch.
- Tunables (`tripAfter`, steps, cap) are constants with measurement-citing comments. No flag
  until operations asks for one.

## 6. Testing

TDD: every test watched failing before the code exists.

- `internal/breaker_test.go` (pure, no mocks): new key → `Full`; `tripAfter` failures →
  `ProbeOnly`; steps follow 10m/30m/1h/2h and hold at the cap; `Full` exactly once when due;
  `Success` clears; terminal trips on first failure; keys independent; `-race` under
  concurrent callers.
- `cloud/errclass_test.go` (table, reusing `sdkErrWithStatus`): each code → expected class;
  429 ≠ 403; 5xx by SDK code not statusCode; nil and unknown safe.
- `k8s/k8s_test.go` (`fake.NewSimpleClientset` + `record.NewFakeRecorder`): find by handle;
  not found → nil, no error; event on PV; also on PVC when `claimRef` resolves; **not** on PVC
  when it is gone (the orphan case); LIST failure swallowed.
- `metrics`: gauge set/delete hits the right series.
- Handler: no `Cloud` mock exists in the repo; keep the handler diff to composing the three
  tested units, as with the semaphore. End-to-end via the existing perf runner: replay B2
  (10 volumes on one node), then inspect events, gauge, and API call counts.
- Mutation check before trusting a test: remove the gate, confirm the test fails where expected.

## 7. Rollout

PR-A: breaker + `Classify` + k8s event helpers + gauge support + handler change.
PR-B: create-path reason from `Classify`, `iaas_errors_total{op=create}`.
Both to `dev` first, image built by `build_dev.yml`, verified on the test cluster with the
runner, then ported to `main`.

The new metrics only exist if the deployment passes `--http-endpoint`, which defaults empty
(`cmd/vngcloud-blockstorage-csi-driver/main.go:44` only initialises the recorder when it is
set). The Helm chart lives outside this repo, so until the chart sets that flag and a scrape
target exists, the metric half of the "tell people" goal ships dark - the events are the only
signal that reaches anyone.

## 8. Comparison with aws-ebs-csi-driver (commit 36d7fd88)

- Same detach shape (`DetachVolume` then `WaitForAttachmentState`, backoff 1s×1.8^n×13),
  **no per-volume breaker either** — but it exposes `ec2_detach_pending_seconds_total`
  per (volume, instance), which this design adopts.
- EBS emits no Kubernetes events itself; it maps errors to gRPC codes
  (`codes.ResourceExhausted` for attach limits) and lets sidecars emit. This driver already
  emits its own PVC event on create failures, so PR-B sharpens that rather than switching model.
- EBS self-heals attachments stuck in `attaching` after 90s by detaching and retrying —
  backlog for this driver.
- EBS computes `allocatable` from instance type minus root/ENI, with
  `--reserved-volume-attachments` — backlog for this driver.

## 9. Backlog spawned by this design

- Attach-path breaker with its own threshold (must exceed the normal 1–3 IN-PROCESS retries).
- Stuck-attaching self-heal (EBS: 90s).
- `--reserved-volume-attachments` and runtime-aware `NodeGetInfo` allocatable.
- Investigate whether recreating the SDK client after N consecutive same-class failures
  reproduces the restart remedy without a restart.
