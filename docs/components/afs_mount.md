# Component: afs_mount

## Purpose

Builds a runnable rootfs mountpoint for an image by combining:

- read-only layer mounts,
- optional extra read-only directory,
- writable upper layer.

## Talks To

- Discovery (`FindImage`) for provider selection.
- Layerstore (`PullImage`, `ReadLayer`, etc.) for image metadata and layer bytes.

## Local Mount Pipeline

1. Resolve candidate layerstore services from discovery.
2. Pull image metadata from a chosen provider.
3. Mount each image layer with FUSE reader.
4. Compose union mount (`fuse-overlayfs`) as final rootfs.
5. Optionally mount `/proc` and `/dev` into rootfs.

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
