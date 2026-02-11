# Python Async SDK (grpclib)

This SDK talks to `afs_proxy` / `afslet` using `asyncio + grpclib`.
Generated protobuf modules are inside `afs_sdk/api/...`.

## Setup (uv)

```bash
cd python
~/.local/bin/uv sync
```

## API

- Low-level: `AfsClient.raw_execute(requests: AsyncIterator[ExecuteRequest])`
- High-level: `AfsClient.execute(request: ExecuteInput)`
  - request is a single typed object (`image`, `command`, resources, and extra file entries)
  - response is `AsyncIterator[ExecuteEvent]`
  - tar.gz is parsed in streaming form into:
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
