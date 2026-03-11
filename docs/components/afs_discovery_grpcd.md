# Component: afs_discovery_grpcd

## Purpose

Maintains a live in-memory view of available layerstore nodes and their cache state.

## Provides

- gRPC service: `discovery.v1.ServiceDiscovery`
  - `Heartbeat`
  - `FindImage`
  - `ResolveImage`
  - `FindProvider`
  - `FindImageProvider`

## Receives From

- `afs_layerstore_grpcd` heartbeats.

## Served To

- `afs_mount` for image resolution and provider discovery (`ResolveImage`, `FindImageProvider`, `FindProvider`).
- `afs_proxy` for replica coordination and status aggregation.

## Key Data Tracked

- Node endpoint and node id.
- Cached image keys.
- Layer digests and per-layer size.
- Node-level `cache_max_bytes`.
- Last-seen timestamp.

## Operational Notes

- Discovery itself stores no image/layer bytes.
- It is a control-plane registry, not a data-plane service.
- If heartbeat stops, node state eventually becomes stale and should not be relied on.
