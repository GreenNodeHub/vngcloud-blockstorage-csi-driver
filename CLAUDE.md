# vngcloud-blockstorage-csi-driver

Kubernetes CSI driver for VNGCloud block storage (vServer volumes and snapshots). One binary runs in two
modes: the controller service (CreateVolume, DeleteVolume, ControllerPublish/Unpublish, snapshots, volume
modify; runs next to the CSI sidecars) and the node service (stage, publish, expand; runs on every node, plus
a PreStop hook). It forked from cloud-provider-openstack and talks to the cloud through `vngcloud-go-sdk`.
Older sibling of `vngcloud-manage-csi-driver`.

Operational context (incidents, invariants, farms, the Redmine task workflow) lives in the **vks-harness**
repo (`knowledge/`, `AGENTS.md`). Management farms are read-only for agents.

## Commands
- `make build`: build the driver binary (`cmd/vngcloud-blockstorage-csi-driver`).
- `make unit`: unit tests (`-tags=unit`, same as CI); `make test` runs `unit` then a no-op `functional`.
- `make lint`: golangci-lint on **new code only** (diff vs `LINT_BASE`, default `origin/main`). The linter is
  pinned (same version as CI) and installed into `./bin` on first use; config is `.golangci.yml`.
- `make check`: full-repo golangci-lint (also run through the old `fmt`/`vet` aliases). Old code has findings.
- `make verify-fast`: `lint` + `go vet ./...` + `go test -short -tags=unit` on the unit packages. Takes about
  25s warm; the first run also downloads and builds the linter (a few minutes). Needs no cluster or cloud access.
- Use the Go version from `go.mod` (the Makefile keeps GOPATH in `./.go`, which is git-ignored).
- No code generation: there are no CRDs, no generated clients and no manifests in this repo.

## Layout
- `cmd/vngcloud-blockstorage-csi-driver/`: entrypoint, flags, mode selection. `cmd/hooks/`: PreStop hook that
  drains this node's VolumeAttachments.
- `pkg/driver/`: gRPC services. `controller.go` (controller RPCs), `node.go` + `mount.go` (node RPCs),
  `controller_modify_volume.go`, `validation.go` (request checks), `errors.go` (gRPC code mapping and events),
  `internal/` (`breaker.go` detach circuit breaker, `inflight.go`, `semaphore.go`).
- `pkg/cloud/`: vServer API client behind the `ICloud` interface (`icloud.go`, `cloud.go`), `errclass.go` error
  classification, `ratelimit.go`, `metacache.go`, `project_scope.go`, `metadata*.go`; `entity/` holds API models.
- `pkg/k8s/`: Kubernetes client and event recorder. `pkg/metrics/`: metric names and help strings.
  `pkg/mounter/`, `pkg/util/`: small helpers.
- `.github/workflows/ci.yml`: the PR gate (build, vet, `make unit`, lint on new issues).

## Sensitive areas (review carefully, cover with tests)
- Detach and delete paths: `pkg/cloud/cloud.go` (`DetachVolume`, `DeleteVolume`, `DeleteSnapshot`),
  `pkg/cloud/consts.go` (detach error sets), `pkg/driver/controller.go` (`ControllerUnpublishVolume`,
  `DeleteVolume`) and `pkg/driver/internal/breaker.go`. Returning success too early makes external-attacher
  remove the VolumeAttachment finalizer while the disk is still attached; returning an error forever blocks
  node and PVC deletion. These paths issue real cloud deletes.
- `pkg/cloud/ratelimit.go`, `errclass.go`, `project_scope.go`: the API quota bucket is shared per project, so
  extra retries or calls can starve every other tenant workload.
- `pkg/metrics/names.go`: published metric names (`vks_csi_*`) are consumed by dashboards and alerts; do not
  rename them casually.
- `cmd/hooks/prestop.go` and `Dockerfile`/`.github/workflows/`: node drain and the image/release pipeline.
- The Helm chart, RBAC and sidecar versions are not in this repo; a flag or RPC change here may need a matching
  change in the chart.

## Conventions
- English only for code, comments, docs, commit and PR text.
- Run `make verify-fast` before finishing a change or opening a PR.
- Never run tests or tools that call real cloud APIs or a cluster; unit tests use in-package fakes.
