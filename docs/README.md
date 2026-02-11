# AFS Documentation

This directory contains architecture and component design documents for AFS.

## Document Map

- `docs/architecture.md`
  - End-to-end runtime flow and component interaction map.
- `docs/components/afs_discovery_grpcd.md`
  - Discovery service design.
- `docs/components/afs_layerstore_grpcd.md`
  - Layer cache service design.
- `docs/components/afs_mount.md`
  - Read-only layer mount and union mount design.
- `docs/components/afs_runc.md`
  - OCI runner design and resource isolation behavior.
- `docs/components/afslet.md`
  - Streaming execution service design.
- `docs/components/afs_proxy.md`
  - Execute proxy and scheduling design.
- `docs/components/clients.md`
  - `afs_cli` and Python SDK behavior.

## Scope

These docs focus on:

- What each component does.
- Which components it talks to.
- What APIs and protocols it uses.
- Operational behavior that affects production usage.
