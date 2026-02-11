# Architecture

## High-Level Goal

AFS turns container image filesystem data into a cluster-level reusable cache instead of a per-node local-only cache.

## Core Services

- `afs_discovery_grpcd`
  - Registry of live layerstore nodes and their advertised cache state.
- `afs_layerstore_grpcd`
  - Pulls image layers, stores `.afslyr` files, serves random-access reads.
- `afs_mount`
  - Resolves image from discovery/layerstore, mounts each layer via FUSE, and builds final rootfs with union mount.
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
5. `afs_mount` queries discovery for image providers and pulls metadata/layers via layerstore gRPC.
6. `afs_mount` mounts each `.afslyr` via FUSE and creates union mount rootfs.
7. `afslet` launches `afs_runc` with CPU/memory/timeout limits.
8. `afslet` returns logs, result, and tar.gz stream of writable layer changes.

## Service Discovery and Cache Visibility

- Layerstore sends heartbeat to discovery with:
  - endpoint, node id, cached image keys, layer digests,
  - layer size stats,
  - cache max bytes.
- Proxy status API streams:
  - all discovered layerstore instances,
  - all resolved afslet instances and runtime status,
  - summary counters.

## Deployment Modes

- `docker-compose.yaml`
  - Local all-in-one deployment.
- `helm/afs`
  - Kubernetes deployment with discovery, layerstore daemonset, afslet, afs_proxy.
