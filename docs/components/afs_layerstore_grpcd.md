# Component: afs_layerstore_grpcd

## Purpose

Maintains local layer cache and serves random-access layer reads over gRPC.

## Provides

- gRPC service: `layerstore.v1.LayerStore`
  - `PullImage`
  - `HasImage`
  - `HasLayer`
  - `StatLayer`
  - `ReadLayer`
  - `PruneCache`

## Talks To

- Docker/OCI registry endpoints to fetch manifests/blobs.
- Discovery service for periodic heartbeat updates.

## Main Responsibilities

- Pull image metadata and layers into local cache.
- Store layers in `.afslyr` format.
- Serve offset-based reads for mounted layer access.
- Enforce cache budget:
  - configured by `-cache-max-bytes`, or
  - default `min(1TB, 60% of free space)`.
- Reserve disk budget before pull for concurrency safety.
- LRU eviction when over budget.

## Heartbeat Content

Reports to discovery:

- endpoint / node id,
- cached image keys,
- layer digests,
- layer stats (`digest`, `afs_size`),
- `cache_max_bytes`.

## Important Flags

- `-listen`
- `-cache-dir`
- `-node-id`
- `-discovery-endpoint` (repeatable)
- `-cache-max-bytes`
- registry auth flags (`-auth-registry-token`, `-auth-registry-basic`, etc.)
