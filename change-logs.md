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
  - `FindImageRequest.layer_digest` added, and discovery service uses index-based selection (`byLayer`/`byImageKey`) to avoid full scans.
  - Mount reader now queries discovery with `LayerDigest` directly.
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
