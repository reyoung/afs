# Component: Clients (afs_cli and Python SDK)

## afs_cli

### Purpose

CLI client for execute and status workflows.

### Talks To

- `Afslet.Execute` for execution.
- `Afslet.GetRuntimeStatus` for runtime counters.
- `AfsProxy.Status` for proxy cluster status.

### Key Capabilities

- Upload local directory as ordered file stream.
- Send execute request with image/command/resources.
- Print streaming logs and results.
- Save writable-upper tar.gz output.
- Query proxy status stream (`-proxy-status`).

## Python SDK (`python/afs_sdk`)

### Purpose

Asyncio + grpclib client library for integration in Python applications.

### API Layers

- Low level:
  - `raw_execute()` uses protobuf frames directly.
- High level:
  - `execute(ExecuteInput)` with typed request models and typed event stream.
  - `status()` returns typed proxy status events.

### Streaming Model

- Upload side supports bytes and async byte iterators for file content.
- Download side parses tar.gz stream incrementally and yields typed tar entries/file parts.
