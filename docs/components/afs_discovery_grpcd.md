# Component: afs_discovery_grpcd

## Purpose

Maintains a live in-memory view of available layerstore nodes, their layer coverage, and resolved image metadata cache.

## Provides

- gRPC service: `discovery.v1.ServiceDiscovery`
  - `Heartbeat`
  - `FindImage`
  - `ResolveImage`

## Receives From

- `afs_layerstore_grpcd` heartbeats.

## Served To

- `afs_mount` for image resolution and provider discovery (`ResolveImage`, `FindImage`).
- `afs_proxy` for replica coordination and status aggregation.

## Key Data Tracked

- Node endpoint and node id.
- Layer digests and per-layer size.
- Image records: image key, resolved registry/repository/reference, ordered layers.
- Derived provider sets for image key and layer digest queries.
- Node-level `cache_max_bytes`.
- Last-seen timestamp.

## Operational Notes

- Discovery stores image metadata cache and provider indexes, but no layer bytes.
- It is a control-plane registry, not a data-plane service.
- Image availability is derived from layer presence; it is not reported as a separate cached-image list.
- If heartbeat stops, node state eventually becomes stale and should not be relied on.
