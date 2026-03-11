# Component: afs_mount

## Purpose

Builds a runnable rootfs mountpoint for an image by combining:

- read-only layer mounts,
- optional extra read-only directory,
- writable upper layer.

## Talks To

- Discovery (`ResolveImage`, `FindImage`) for image resolution and provider selection.
- Layerstore (`EnsureLayers`, `ReadLayer`, etc.) for layer materialization and bytes.

## Local Mount Pipeline

1. Resolve image in discovery and get ordered layer metadata.
2. Ask discovery for complete image providers.
3. If no suitable provider exists, select a layerstore and call `EnsureLayers`.
4. Mount each image layer with FUSE reader.
5. Compose union mount (`fuse-overlayfs`) as final rootfs.
6. Optionally mount `/proc` and `/dev` into rootfs.

## Reliability Behavior

- Supports provider failover during read path.
- Re-resolves providers from discovery to adapt to topology changes.
- Stops waiting when mount subprocess exits with failure.

## Important Flags

- `-mountpoint`
- `-image`, `-tag`
- `-discovery-addr`
- `-extra-dir`
- `-mount-proc-dev`
- `-work-dir`
- `-force-local-fetch`
- platform flags (`-platform-os`, `-platform-arch`, `-platform-variant`)
