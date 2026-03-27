# Change Logs

## 2026-03-09

### Summary
- Refactored discovery-backed layer reader logic out of `cmd/afs_mount` into reusable package `pkg/layerreader`.
- Added registry mirror support to Go registry client and wired it into `afs_layerstore_grpcd`.
- Added `ReadLayerStream` (server-streaming) RPC and client-side stream-first fallback logic.
- Increased default `afs_mount` gRPC read chunk size from `1MiB` to `4MiB`.
- Added/expanded integration benchmark for `DiscoveryBackedReaderAt`:
  - in-process discovery + layerstore boot
  - image pull + concurrent multi-file reads
  - MD5 correctness checks
  - remote vs local baseline throughput + overhead output

### Key Code Changes
- API / protobuf:
  - `api/layerstore/v1/layerstore.proto`: added `ReadLayerStream`.
  - regenerated `pkg/layerstorepb/layerstore.pb.go` and `pkg/layerstorepb/layerstore_grpc.pb.go`.
- Mount side:
  - `cmd/afs_mount/main.go`: uses `pkg/layerreader`, default `-grpc-max-chunk=4<<20`.
  - `pkg/layerreader/discovery_reader.go`: concurrency optimization (remove full-read serialization), stream-first read path with unary fallback.
- Layerstore side:
  - `pkg/layerstore/service.go`: implemented `ReadLayerStream`; mirror config plumbing (`SetRegistryMirrors`).
  - `cmd/afs_layerstore_grpcd/main.go`: added `-registry-mirror` flag and parsing.
- Registry client:
  - `pkg/registry/client.go`: per-registry mirror config + mirror/origin fallback behavior.
  - `pkg/registry/client_test.go`: mirror fallback unit test.
  - `cmd/afs_layerstore_grpcd/main_test.go`: mirror flag parse tests.
- Docs:
  - `docs/components/afs_layerstored_proxy.zh-CN.md`: design draft.
  - `docs/README.zh-CN.md`: doc index update.

### Performance Results (Integration, local run)
Command used:
- `env -u HTTPS_PROXY -u HTTP_PROXY -u ALL_PROXY -u https_proxy -u http_proxy -u all_proxy go test -tags integration ./pkg/layerreader -run TestIntegrationDiscoveryBackedReaderAtConcurrentReadPerfAndMD5 -v`

Result snapshot (`ubuntu:latest`, workers=8, iterations=8):
- Remote (`DiscoveryBackedReaderAt`): `112.95 MiB/s`
- Local baseline (direct local `.afslyr` read): `449.49 MiB/s`
- Overhead (`remote_vs_local`): `74.87%`
- MD5 checks: all passed.

### Notes
- `docker pull` may succeed while Go client fails when shell proxy env forces blocked CONNECT tunnel. Running tests without proxy env made the integration run stable.
- The E2E integration test now auto-detects local Docker auth and Docker daemon mirrors for better parity with local Docker behavior.

### Update: Overhead Root Cause + Optimization
- Discovery API now supports layer-targeted lookup:
  - `FindProvider.layer_digest` is used for layer-targeted lookup, while `FindImageProvider.image_key` is used for complete-image provider lookup.
  - Discovery uses index-based selection (`byLayer`) plus image-layer intersection to avoid full scans.
  - Mount reader now queries discovery with `LayerDigest` directly via `FindProvider`.
- Added detailed `ReadAt` timing/counter instrumentation in integration perf test to identify overhead location.
- Confirmed bottleneck is remote `ReadAt` call granularity (many tiny reads), not `CopyFile` itself.

Optimization applied:
- In `pkg/layerformat/format.go`, wrapped `io.SectionReader` with `bufio.NewReaderSize(..., 1<<20)` before `gzip.NewReader(...)`.
- This increases gzip source-read buffering from default `4KiB` to `1MiB`, reducing remote `ReadAt` call count.

Latest integration result (same command and dataset):
- Remote throughput: `399.28 MiB/s`
- Local baseline throughput: `454.76 MiB/s`
- Overhead: `12.20%` (was ~`74.87%`)
- `ReadAt` stats:
  - remote: `calls=80`, `got_bytes=69854240`, `total_readat_time=258.565ms`, avg request `~873178 bytes`
  - local: `calls=80`, `got_bytes=69854240`, `total_readat_time=7.522ms`
- MD5 checks: all passed.

### Update: `afs_mount` Cold First-Read Optimization (`/bin/true` E2E)
- Added `afs_mount` flag:
  - `-no-spill-cache` to disable on-disk spill-file reuse for decompressed payloads.
- Optimized layer FUSE read path:
  - spill write path now uses `bufio.NewWriterSize(..., 1<<20)` for smoother large writes.
  - `no-spill-cache` mode keeps fd-backed temp storage and removes temp path, so cache files do not accumulate.
- Tuned layer FUSE mount options in `cmd/afs_mount/main.go`:
  - `MaxWrite=1<<20`, `MaxReadAhead=1<<20`
  - `EntryTimeout=30s`, `AttrTimeout=30s`, `NegativeTimeout=5s`

#### Validation Snapshot
- Cold first-run `afs_runc -- /bin/true` on image:
  - `mirrors.tencent.com/josephyu/ioi_docker:ioi_it_1772788528_aplusb_2023_builder`
- Before (same environment): `runc_wait` around `~1.2s`.
- After current optimization:
  - `default`: `runc_total=344ms`, `runc_wait=338ms`
  - `--no-spill-cache`: `runc_total=329ms`, `runc_wait=322ms`

#### E2E (current branch)
- One cold + five warm rounds (`afs_mount + afs_runc /bin/true`):
  - cold: `mount_ready=63001ms`, `runc_total=319ms`, `e2e=63327ms`
  - warm `runc_total`: `p50=348ms`, `p95=363ms`
  - warm `e2e`: `p50=1062ms`, `p95=1073ms`

## 2026-03-10

### Summary
- Added `afs_mount` reusable package (`pkg/afsmount`) and switched `afslet` to support in-process mount execution path.
- Added shared gRPC client cache (`pkg/grpcclientcache`) to reuse connections with bounded cache size.
- Added mount/readiness timing instrumentation across `afsmount` and `afslet` for phase-level benchmark analysis.
- Replaced in-process mount readiness polling with event-driven ready notification (`OnReady` callback), removing fixed `200ms` polling delay on the hot path.
- Added layer-provider discovery cache and optimistic provider reuse/failover enhancements in discovery-backed layer reader.
- Added Helm / compose knobs for `mount-in-process` rollout.

### Key Code Changes
- New package:
  - `pkg/afsmount/runner.go`: extracted mount flow from CLI style into reusable in-process runner.
  - `pkg/grpcclientcache/cache.go`: shared gRPC connection cache for discovery/layerstore clients.
- `afslet`:
  - `pkg/afslet/service.go`: in-process mount path, mount lifecycle timing logs, and in-process ready event wait (`waitForInProcessMountReady`).
  - `pkg/afslet/service_test.go`: added unit tests for in-process ready signal and early mount-exit behavior.
  - `cmd/afslet/main.go`: added/propagated `-mount-in-process`.
- reader/cache:
  - `pkg/layerreader/discovery_reader.go`: optimistic provider strategy, discovery failover hooks, known layer size propagation, close lifecycle handling.
- deployment config:
  - `docker-compose.yaml`: `AFSLET_MOUNT_IN_PROCESS` env wiring.
  - `helm/afs/values.yaml`: `afslet.mountInProcess` value.
  - `helm/afs/templates/afslet-deployment.yaml`: pass `-mount-in-process` arg.

### Performance Validation

#### A) Local bare processes (host, root-run afslet/layerstore/discovery)
- Command pattern:
  - start `afs_discovery_grpcd` / `afs_layerstore_grpcd` / `afslet` directly, then run `afs_cli` 10 times.
- Result (`alpine`, warm):
  - `wait_mount_ready`: `avg=12.00ms` (max `65ms`)
  - `mount_lifetime`: `avg=84.40ms`
  - `runc_total`: `avg=65.30ms`

#### B) Docker Compose
- Config:
  - `AFSLET_MOUNT_IN_PROCESS=true`, `AFSLET_LAYER_MOUNT_CONCURRENCY=1`
- Warm benchmark (`alpine`, 20 runs):
  - `wait_mount_ready`: `avg=6.05ms` (max `8ms`)
  - `mount_lifetime`: `avg=75.35ms`
  - `run_image_mode_total` observed mostly `60-84ms`
- Before this ready-event change (same setup, prior measurement):
  - `wait_mount_ready` was about `200ms` due polling step.
  - New change removes that fixed wait from hot path.

#### C) Kubernetes Helm
- Deployed with:
  - `afslet.mountInProcess=true` (Helm revision 19+)
- Warm benchmark (`mirrors.tencent.com/josephyu/afs:<deployed-tag>`, 10 runs):
  - `wait_mount_ready`: `avg=36.80ms` (max `84ms`)
  - `mount_lifetime`: `avg=160.60ms`
  - `runc_total`: `avg=115.50ms`
- Cold run on first request included remote image resolution/pull cost (`wait_mount_ready` seconds-level), then warm runs dropped to sub-100ms ready wait.

### Notes
- Helm environment image source constraints were significant:
  - `docker.io/library/alpine` often timed out from cluster nodes.
  - `mirrors.tencent.com/library/alpine:latest` returned manifest unknown.
- For stable Helm verification, benchmark used an existing deploy image (`mirrors.tencent.com/josephyu/afs:<tag>`), which is resolvable in-cluster.
