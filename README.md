# AFS (Agent File System)

AFS stands for **Agent File System**.

It is a filesystem-oriented solution for RL workloads (for example, SWE-Agent) to quickly deploy and reuse container image file data.

## Background

In RL clusters, task volume is high, image diversity is high, and pulls are frequent.
If every node repeatedly pulls the same image from the same registry, it causes:

- Excessive registry load
- Bandwidth waste
- Higher startup latency

The goal of AFS is to expand image data reuse from “single node” to “entire cluster”.

## Core Idea

Unlike Docker’s strictly local layer dependency, AFS does not require all layers to exist on the current machine.

As long as a layer (or the corresponding image) is available on some node in the cluster, another node can read it over the network and mount the filesystem.

This means:

- A single node’s disk is no longer the hard limit for image capacity
- The aggregate disk capacity of the cluster can hold a much larger image set
- Repeated downloads of the same image from the registry are reduced

## Components

- `afs_discovery_grpcd`
  - Service discovery and node liveness/status management (heartbeat)
  - Returns endpoints of nodes that contain a target image

- `afs_layerstore_grpcd`
  - Manages layer cache
  - Provides `PullImage / HasImage / HasLayer / StatLayer / ReadLayer / PruneCache`
  - Periodically reports to one or more discovery services after startup
  - Supports cache limit and LRU eviction
    - Default limit: `min(1TB, 60% of free space of cache filesystem)`
    - Configurable via `-cache-max-bytes`
    - `PullImage` reserves cache space before download (concurrency-safe)
    - If image required size exceeds cache limit, fails fast
    - If over limit, evicts old layers (LRU by access time) and triggers heartbeat

- `afs_mount`
  - Connects only to discovery
  - Finds nodes that contain the image, then mounts
  - Automatically retries by switching providers on read failures
  - Re-resolves layer providers from discovery during reads, so topology updates can take effect quickly

- `afslet`
  - Streaming execution service based on `afs_mount + afs_runc`
  - Supports resource admission (`cpu_cores`, `memory_mb`) and exposes runtime resource status

- `afs_proxy`
  - Proxies `Afslet.Execute`
  - Schedules by `cpu_cores/memory_mb` using DNS-discovered afslet backends (A records re-resolved per dispatch)
  - Uses power-of-two-choices (P2C) among feasible backends
  - Supports dispatch retry (`proxy_dispatch_max_retries`, `proxy_dispatch_backoff_ms`)
  - Provides HTTP `/dispatching` for local/cluster dispatching queue count

## Quick Flow

1. Start `afs_discovery_grpcd`
2. Start `afs_layerstore_grpcd` on nodes and register to discovery
3. Run `afs_mount` from workload side with target image and mountpoint

For local all-in-one startup, use `docker-compose.yaml` (discovery + layerstore + afslet + afs_proxy).

## Key Commands

- Build all binaries:
  - `make build-local`

- Run layerstore with explicit cache limit:
  - `./dist/linux_amd64/afs_layerstore_grpcd -listen 127.0.0.1:50051 -cache-dir .cache/layerstore -node-id node-1 -discovery-endpoint 127.0.0.1:60051 -cache-max-bytes 53687091200`

- Prune ~30% cache via gRPC (from your own client):
  - call `LayerStore.PruneCache` with `percent=30`

- Integration test (layer read recovery after deleting one provider's layer file):
  - `sudo go test -tags integration ./cmd/afs_mount -run TestIntegrationMountReadRecoversAfterLayerFileDeleted -v -count=1`
  - Optional DockerHub auth env to avoid anonymous rate limit:
    - `AFS_INTEGRATION_DOCKER_AUTH_B64=<base64(username:password_or_token)>`

## Positioning

AFS currently focuses on image file reuse and acceleration for large-scale concurrent RL jobs.

It is not a replacement for Docker; it is a cross-node image file access layer for agent-centric workloads.
