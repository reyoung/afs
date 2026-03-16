# Python Async SDK (grpclib)

This SDK talks to `afs_proxy` / `afslet` using `asyncio + grpclib`.
Generated protobuf modules are inside `afs_sdk/api/...`.

## Version

When imported from a git checkout, `afs_sdk.__version__` is resolved from:

```bash
git describe --tags --long --always --dirty --match 'v*'
```

Example output:

```python
>>> import afs_sdk
>>> afs_sdk.__version__
'v0.2.0-1-gabc1234-dirty'
>>> afs_sdk.get_version_info()
VersionInfo(version='v0.2.0-1-gabc1234-dirty', tag='v0.2.0', commit_sha='abc1234', distance=1, dirty=True)
```

When installed from a built distribution without `.git` metadata, it falls back
to the package metadata version. PyPI release versions are derived from git tags
via `setuptools_scm`.

## Setup (uv)

```bash
cd python
~/.local/bin/uv sync
```

## API

- Low-level: `AfsClient.raw_execute(requests: AsyncIterator[ExecuteRequest])`
- High-level: `AfsClient.execute(request: ExecuteInput)`
  - request is a single typed object (`image`, optional `command`, resources, and extra file entries)
  - set `force_local_fetch=True` to force local layer fetch on selected layerstore
  - response is `AsyncIterator[ExecuteEvent]`
- Proxy status stream: `AfsClient.status(include_layerstores=True, include_afslets=True)`
  - returns `AsyncIterator[ProxyStatusEvent]`
- Reconcile image replica: `AfsClient.reconcile_image(request: ReconcileImageInput)`
  - request: image/tag/platform/replica
  - response: `ReconcileImageResult(image_key, current_replica, requested_replica, ensured)`
- Execute tar.gz output is parsed in streaming form into:
  - `TarDirectory`
  - `TarSymlink`
  - `TarFilePart` (chunked file data; no full-file buffering)
- CPU-heavy tar.gz decode/untar is offloaded via executor:
  - pass `tar_executor=...` to `AfsClient`
  - default is an internal `ThreadPoolExecutor(max_workers=1)`

## Extra file input types

`ExtraFile.content` supports:
- `bytes`
- `AsyncIterator[bytes]`

## Example

```python
import asyncio
from afs_sdk import AfsClient, ExecuteInput, ExtraFile

async def run():
    req = ExecuteInput(
        image="alpine",
        tag="latest",
        command=["/bin/sh", "-c", "echo ok >/tmp/ok.txt"],
        cpu_cores=1,
        memory_mb=256,
        timeout_ms=2000,
        extra_entries=[
            ExtraFile(path="hello.txt", content=b"hello from sdk\n"),
        ],
    )

    async with AfsClient("127.0.0.1", 62051) as client:
        async for event in client.execute(req):
            print(type(event).__name__)

asyncio.run(run())
```

CLI-style demo:

```bash
PYTHONPATH=python ~/.local/bin/uv run python python/examples/execute_stream.py \
  --addr 127.0.0.1:62051 \
  --dir ./extra-dir \
  --image alpine --tag latest \
  -- /bin/sh -c 'echo ok >/tmp/ok.txt'
```

If `command` is omitted, AFS uses the image `Entrypoint`/`Cmd` defaults returned by discovery.

Reconcile image replica demo:

```bash
PYTHONPATH=python ~/.local/bin/uv run python python/examples/reconcile_image.py \
  --addr 127.0.0.1:62051 \
  --image alpine --tag latest --replica 0
```
