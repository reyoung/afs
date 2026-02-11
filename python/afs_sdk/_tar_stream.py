from __future__ import annotations

import asyncio
import gzip
import io
import queue
import tarfile
from concurrent.futures import Executor
from multiprocessing import Manager
from multiprocessing.managers import SyncManager
from typing import Any, AsyncIterator, Optional

from .models import TarDirectory, TarFilePart, TarSymlink


_SENTINEL = None


def _tar_worker(in_q: Any, out_q: Any, part_size: int) -> None:
    class ChunkReader(io.RawIOBase):
        def __init__(self, q: Any) -> None:
            self._q = q
            self._buf = b""
            self._eof = False

        def readable(self) -> bool:
            return True

        def readinto(self, b: bytearray) -> int:
            if self._eof and not self._buf:
                return 0
            view = memoryview(b)
            written = 0
            while written < len(view):
                if self._buf:
                    n = min(len(self._buf), len(view) - written)
                    view[written : written + n] = self._buf[:n]
                    self._buf = self._buf[n:]
                    written += n
                    if written > 0:
                        return written
                if self._eof:
                    return written
                chunk = self._q.get()
                if chunk is _SENTINEL:
                    self._eof = True
                elif isinstance(chunk, (bytes, bytearray)):
                    self._buf = bytes(chunk)
                else:
                    raise TypeError(f"unexpected chunk type: {type(chunk)!r}")
            return written

    def put_event(kind: str, payload: dict[str, Any]) -> None:
        out_q.put((kind, payload))

    try:
        raw = ChunkReader(in_q)
        buffered = io.BufferedReader(raw, buffer_size=part_size)
        gz = gzip.GzipFile(fileobj=buffered, mode="rb")
        tf = tarfile.open(fileobj=gz, mode="r|")
        for member in tf:
            base = {
                "path": member.name,
                "mode": int(member.mode),
                "uid": int(member.uid),
                "gid": int(member.gid),
                "uname": member.uname or "",
                "gname": member.gname or "",
                "mtime_unix": int(member.mtime),
            }
            if member.isdir():
                put_event("tar_dir", base)
                continue
            if member.issym():
                payload = dict(base)
                payload["target"] = member.linkname or ""
                put_event("tar_symlink", payload)
                continue
            if member.isreg():
                f = tf.extractfile(member)
                if f is None:
                    payload = dict(base)
                    payload.update({"size": int(member.size), "offset": 0, "data": b"", "eof": True})
                    put_event("tar_file_part", payload)
                    continue
                offset = 0
                size = int(member.size)
                while True:
                    data = f.read(part_size)
                    if not data:
                        break
                    payload = dict(base)
                    payload.update(
                        {
                            "size": size,
                            "offset": offset,
                            "data": data,
                            "eof": False,
                        }
                    )
                    put_event("tar_file_part", payload)
                    offset += len(data)
                payload = dict(base)
                payload.update({"size": size, "offset": offset, "data": b"", "eof": True})
                put_event("tar_file_part", payload)
        out_q.put(("eof", {}))
    except Exception as err:
        out_q.put(("error", {"message": str(err)}))


class TarGzParser:
    def __init__(self, loop: asyncio.AbstractEventLoop, executor: Executor, part_size: int = 256 * 1024) -> None:
        self._loop = loop
        self._executor = executor
        self._part_size = part_size
        self._events: asyncio.Queue[object] = asyncio.Queue()
        self._poller_task: Optional[asyncio.Task[None]] = None
        self._worker_fut: Optional[asyncio.Future[None]] = None
        self._manager: Optional[SyncManager] = None

        if executor.__class__.__name__ == "ProcessPoolExecutor":
            self._manager = Manager()
            self._in_q = self._manager.Queue()
            self._out_q = self._manager.Queue()
        else:
            self._in_q = queue.Queue()
            self._out_q = queue.Queue()

    def start(self) -> None:
        if self._worker_fut is not None:
            return
        self._worker_fut = self._loop.run_in_executor(self._executor, _tar_worker, self._in_q, self._out_q, self._part_size)
        self._poller_task = self._loop.create_task(self._poll_out_queue())

    async def feed(self, chunk: bytes) -> None:
        await asyncio.to_thread(self._in_q.put, chunk)

    async def finish_input(self) -> None:
        await asyncio.to_thread(self._in_q.put, _SENTINEL)

    async def drain_available(self) -> list[object]:
        out: list[object] = []
        while True:
            try:
                out.append(self._events.get_nowait())
            except asyncio.QueueEmpty:
                return out

    async def iter_until_end(self) -> AsyncIterator[object]:
        while True:
            item = await self._events.get()
            if item is _SENTINEL:
                return
            if isinstance(item, Exception):
                raise item
            yield item

    async def wait_closed(self) -> None:
        if self._poller_task is not None:
            await self._poller_task
        if self._worker_fut is not None:
            await self._worker_fut
        if self._manager is not None:
            self._manager.shutdown()

    async def _poll_out_queue(self) -> None:
        while True:
            kind, payload = await asyncio.to_thread(self._out_q.get)
            if kind == "eof":
                await self._events.put(_SENTINEL)
                return
            if kind == "error":
                await self._events.put(RuntimeError(payload.get("message", "tar parse failed")))
                await self._events.put(_SENTINEL)
                return
            if kind == "tar_dir":
                await self._events.put(TarDirectory(**payload))
                continue
            if kind == "tar_symlink":
                await self._events.put(TarSymlink(**payload))
                continue
            if kind == "tar_file_part":
                await self._events.put(TarFilePart(**payload))
                continue
            await self._events.put(RuntimeError(f"unknown parser event kind: {kind}"))
            await self._events.put(_SENTINEL)
            return
