# Component: afs_layerstore_grpcd

## Purpose

Maintains local layer cache and serves random-access layer reads over gRPC.

## Provides

- gRPC service: `layerstore.v1.LayerStore`
  - `EnsureLayers`
  - `HasLayer`
  - `StatLayer`
  - `ReadLayer`
  - `PruneCache`

## Talks To

- Docker/OCI registry endpoints to fetch blobs.
- Discovery service for periodic heartbeat updates.

## Main Responsibilities

- Ensure requested layers into local cache after discovery resolves the image.
- Store layers in `.afslyr` format.
- Serve offset-based reads for mounted layer access.
- Own layer state only; image metadata lives in discovery.
- Enforce cache budget:
  - configured by `-cache-max-bytes`, or
  - default `min(1TB, 60% of free space)`.
- Reserve disk budget before ensure for concurrency safety.
- LRU eviction when over budget.

## Heartbeat Content

Reports to discovery:

- endpoint / node id,
- layer digests,
- layer stats (`digest`, `afs_size`),
- `cache_max_bytes`.

## Important Flags

- `-listen`
- `-cache-dir`
- `-node-id`
- `-discovery-endpoint` (repeatable)
- `-cache-max-bytes`
- registry auth / mirror flags (`-auth-registry-token`, `-auth-registry-basic`, `-registry-mirror`, etc.)
