# Component: afslet

## Purpose

Streaming execution service that wraps `afs_mount` and `afs_runc`.

## Provides

- gRPC service: `afsletpb.Afslet`
  - `Execute` (bi-directional stream)
  - `GetRuntimeStatus` (unary)

## Talks To

- Executes local binaries: `afs_mount` and `afs_runc`.
- Through `afs_mount`, indirectly depends on discovery and layerstore.

## Execute Protocol

- First frame must be `StartRequest`.
- Additional frames upload extra-dir entries in order:
  - directory,
  - symlink,
  - file begin/chunks/end.
- Response stream includes:
  - accepted marker,
  - process logs,
  - command result,
  - tar.gz output stream of writable-upper.

## Resource Admission

- Request must include `cpu_cores` and `memory_mb` (>0).
- Service enforces global limits configured at startup.
- If requested resources exceed limit or current available budget, request is rejected.
- Runtime status returns used/available counters.

## Important Flags

- `-listen`
- `-mount-binary`
- `-runc-binary`
- `-discovery-addr`
- `-temp-dir`
- `-limit-cpu`
- `-limit-memory-mb`
- `-graceful-timeout`
