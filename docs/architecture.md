# Architecture

## High-Level Goal

AFS turns container image filesystem data into a cluster-level reusable cache instead of a per-node local-only cache.

## Core Services

- `afs_discovery_grpcd`
  - Registry of live layerstore nodes, their advertised layer coverage, and resolved image metadata.
- `afs_layerstore_grpcd`
  - Ensures requested layers, stores `.afslyr` files, serves random-access reads.
- `afs_mount`
  - Resolves image from discovery, ensures missing layers on a selected layerstore, mounts each layer via FUSE, and builds final rootfs with union mount.
- `afs_runc`
  - Runs command in prepared rootfs with cgroup CPU/memory limits and timeout control.
- `afslet`
  - gRPC streaming runtime that receives extra files, calls `afs_mount` + `afs_runc`, and returns writable diff as tar.gz.
- `afs_proxy`
  - gRPC proxy for `Afslet.Execute` with resource-aware dispatch and retries.

## Typical Execute Path

1. Client (`afs_cli` / Python SDK) opens `Afslet.Execute` on `afs_proxy`.
2. `afs_proxy` resolves afslet backends from DNS, fetches runtime status, picks backend, forwards stream.
3. Backend `afslet` receives request, reserves CPU/memory, builds extra-dir from uploaded file frames.
4. `afslet` launches `afs_mount`.
5. `afs_mount` calls discovery `ResolveImage` and receives resolved reference plus ordered layers.
6. `afs_mount` asks discovery for complete image providers; if needed, it selects a layerstore and calls `EnsureLayers`.
7. `afs_mount` mounts each `.afslyr` via FUSE and creates union mount rootfs.
8. `afslet` launches `afs_runc` with CPU/memory/timeout limits.
9. `afslet` returns logs, result, and tar.gz stream of writable layer changes.

## Service Discovery and Cache Visibility

- Layerstore sends heartbeat to discovery with:
  - endpoint, node id, layer digests,
  - layer size stats,
  - cache max bytes.
- Discovery stores:
  - resolved image records (`image_key -> resolved ref + ordered layers[]`),
  - per-layer provider mapping derived from layer heartbeats,
  - complete image providers derived from layer coverage.
- Proxy status API streams:
  - all discovered layerstore instances,
  - all resolved afslet instances and runtime status,
  - summary counters.

## Deployment Modes

- `docker-compose.yaml`
  - Local all-in-one deployment.
- `helm/afs`
  - Kubernetes deployment with discovery, layerstore daemonset, afslet, afs_proxy.
