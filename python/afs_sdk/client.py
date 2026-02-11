from __future__ import annotations

import asyncio
import sys
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import AsyncIterator, Optional

from grpclib.client import Channel

from ._tar_stream import TarGzParser
from .api.afslet.v1.afslet_grpc import AfsletStub
from .api.afslet.v1 import afslet_pb2 as pb
from .api.afsproxy.v1.afsproxy_grpc import AfsProxyStub
from .api.afsproxy.v1 import afsproxy_pb2 as proxypb
from .models import (
    AcceptedEvent,
    DoneEvent,
    ExecuteEvent,
    ExecuteInput,
    ExtraDirectory,
    ExtraFile,
    ExtraSymlink,
    LogEvent,
    ResultEvent,
    RuntimeStatus,
    TarArchiveStart,
    ProxyStatusEvent,
    ProxyStatusSummary,
    ProxyLayerInfo,
    ProxyLayerstoreInstance,
    ProxyAfsletInstance,
    ProxyStatusError,
)


class AfsClient:
    """Asyncio + grpclib client for afs_proxy/afslet."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        ssl: Optional[bool] = None,
        tar_executor: Optional[Executor] = None,
    ) -> None:
        self._channel = Channel(host=host, port=port, ssl=ssl)
        self._stub = AfsletStub(self._channel)
        self._proxy_stub = AfsProxyStub(self._channel)
        self._owned_executor: Optional[ThreadPoolExecutor] = None
        if tar_executor is None:
            self._owned_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="afs-sdk-tar")
            self._tar_executor: Executor = self._owned_executor
        else:
            self._tar_executor = tar_executor

    async def close(self) -> None:
        self._channel.close()
        if self._owned_executor is not None:
            self._owned_executor.shutdown(wait=True, cancel_futures=False)
            self._owned_executor = None

    async def __aenter__(self) -> "AfsClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def get_runtime_status(self) -> RuntimeStatus:
        resp = await self._stub.GetRuntimeStatus(pb.GetRuntimeStatusRequest())
        return RuntimeStatus(
            running_containers=resp.running_containers,
            limit_cpu_cores=resp.limit_cpu_cores,
            limit_memory_mb=resp.limit_memory_mb,
            used_cpu_cores=resp.used_cpu_cores,
            used_memory_mb=resp.used_memory_mb,
            available_cpu_cores=resp.available_cpu_cores,
            available_memory_mb=resp.available_memory_mb,
        )

    async def iter_runtime_status(self) -> AsyncIterator[RuntimeStatus]:
        yield await self.get_runtime_status()

    async def status(
        self,
        *,
        include_layerstores: bool = True,
        include_afslets: bool = True,
    ) -> AsyncIterator[ProxyStatusEvent]:
        req = proxypb.StatusRequest(
            include_layerstores=include_layerstores,
            include_afslets=include_afslets,
        )
        call = self._proxy_stub.Status(req)
        if hasattr(call, "__aiter__"):
            async_iter = call
            sync_iter = None
        else:
            result = await call
            if hasattr(result, "__aiter__"):
                async_iter = result
                sync_iter = None
            else:
                async_iter = None
                sync_iter = result

        if async_iter is not None:
            async for resp in async_iter:
                kind = resp.WhichOneof("payload")
                if kind == "summary":
                    p = resp.summary
                    yield ProxyStatusSummary(
                        layerstore_instances=p.layerstore_instances,
                        afslet_instances=p.afslet_instances,
                        afslet_reachable=p.afslet_reachable,
                        total_layers=p.total_layers,
                    )
                elif kind == "layerstore":
                    p = resp.layerstore
                    layers = [ProxyLayerInfo(digest=x.digest, afs_size=x.afs_size) for x in p.layers]
                    yield ProxyLayerstoreInstance(
                        node_id=p.node_id,
                        endpoint=p.endpoint,
                        last_seen_unix=p.last_seen_unix,
                        cache_max_bytes=p.cache_max_bytes,
                        layers=layers,
                        cached_images=list(p.cached_images),
                    )
                elif kind == "afslet":
                    p = resp.afslet
                    yield ProxyAfsletInstance(
                        endpoint=p.endpoint,
                        reachable=p.reachable,
                        error=p.error,
                        running_containers=p.running_containers,
                        limit_cpu_cores=p.limit_cpu_cores,
                        limit_memory_mb=p.limit_memory_mb,
                        used_cpu_cores=p.used_cpu_cores,
                        used_memory_mb=p.used_memory_mb,
                        available_cpu_cores=p.available_cpu_cores,
                        available_memory_mb=p.available_memory_mb,
                    )
                elif kind == "error":
                    p = resp.error
                    yield ProxyStatusError(source=p.source, message=p.message)
            return

        for resp in sync_iter:
            kind = resp.WhichOneof("payload")
            if kind == "summary":
                p = resp.summary
                yield ProxyStatusSummary(
                    layerstore_instances=p.layerstore_instances,
                    afslet_instances=p.afslet_instances,
                    afslet_reachable=p.afslet_reachable,
                    total_layers=p.total_layers,
                )
            elif kind == "layerstore":
                p = resp.layerstore
                layers = [ProxyLayerInfo(digest=x.digest, afs_size=x.afs_size) for x in p.layers]
                yield ProxyLayerstoreInstance(
                    node_id=p.node_id,
                    endpoint=p.endpoint,
                    last_seen_unix=p.last_seen_unix,
                    cache_max_bytes=p.cache_max_bytes,
                    layers=layers,
                    cached_images=list(p.cached_images),
                )
            elif kind == "afslet":
                p = resp.afslet
                yield ProxyAfsletInstance(
                    endpoint=p.endpoint,
                    reachable=p.reachable,
                    error=p.error,
                    running_containers=p.running_containers,
                    limit_cpu_cores=p.limit_cpu_cores,
                    limit_memory_mb=p.limit_memory_mb,
                    used_cpu_cores=p.used_cpu_cores,
                    used_memory_mb=p.used_memory_mb,
                    available_cpu_cores=p.available_cpu_cores,
                    available_memory_mb=p.available_memory_mb,
                )
            elif kind == "error":
                p = resp.error
                yield ProxyStatusError(source=p.source, message=p.message)

    async def raw_execute(self, requests: AsyncIterator[pb.ExecuteRequest]) -> AsyncIterator[pb.ExecuteResponse]:
        """Low-level Execute API using raw protobuf frames."""
        async with self._stub.Execute.open() as stream:
            await stream.send_request()

            async def _sender() -> None:
                async for req in requests:
                    await stream.send_message(req)
                await stream.end()

            sender_task = asyncio.create_task(_sender())
            try:
                while True:
                    msg = await stream.recv_message()
                    if msg is None:
                        break
                    yield msg
            finally:
                active_exc = sys.exc_info()[1]
                if not sender_task.done():
                    sender_task.cancel()
                try:
                    await sender_task
                except asyncio.CancelledError:
                    pass
                except BaseException:
                    if active_exc is None:
                        raise

    async def execute(self, request: ExecuteInput) -> AsyncIterator[ExecuteEvent]:
        """High-level Execute API with typed request/event models."""
        req_stream = self._iter_raw_requests(request)
        parser: Optional[TarGzParser] = None
        async for resp in self.raw_execute(req_stream):
            kind = resp.WhichOneof("payload")
            if kind == "accepted":
                yield AcceptedEvent(accepted=resp.accepted.accepted)
                continue
            if kind == "log":
                yield LogEvent(source=resp.log.source, message=resp.log.message)
                continue
            if kind == "result":
                yield ResultEvent(success=resp.result.success, exit_code=resp.result.exit_code, error=resp.result.error)
                continue
            if kind == "tar_header":
                yield TarArchiveStart(name=resp.tar_header.name)
                if parser is None:
                    parser = TarGzParser(asyncio.get_running_loop(), self._tar_executor)
                    parser.start()
                continue
            if kind == "tar_chunk":
                if parser is None:
                    parser = TarGzParser(asyncio.get_running_loop(), self._tar_executor)
                    parser.start()
                await parser.feed(resp.tar_chunk.data)
                for event in await parser.drain_available():
                    yield event
                continue
            if kind == "done":
                if parser is not None:
                    await parser.finish_input()
                    async for event in parser.iter_until_end():
                        yield event
                    await parser.wait_closed()
                yield DoneEvent()
                return

    async def _iter_raw_requests(self, req: ExecuteInput) -> AsyncIterator[pb.ExecuteRequest]:
        if req.cpu_cores <= 0:
            raise ValueError("cpu_cores must be > 0")
        if req.memory_mb <= 0:
            raise ValueError("memory_mb must be > 0")
        if req.timeout_ms <= 0:
            raise ValueError("timeout_ms must be > 0")
        if req.upload_chunk_size <= 0:
            raise ValueError("upload_chunk_size must be > 0")
        if not req.image.strip():
            raise ValueError("image is required")
        if not req.command:
            raise ValueError("command is required")

        yield pb.ExecuteRequest(
            start=pb.StartRequest(
                image=req.image,
                tag=req.tag,
                command=list(req.command),
                cpu_cores=req.cpu_cores,
                memory_mb=req.memory_mb,
                timeout_ms=req.timeout_ms,
                discovery_addr=req.discovery_addr,
                force_local_fetch=req.force_local_fetch,
                node_id=req.node_id,
                platform_os=req.platform_os,
                platform_arch=req.platform_arch,
                platform_variant=req.platform_variant,
                proxy_dispatch_max_retries=req.proxy_dispatch_max_retries,
                proxy_dispatch_backoff_ms=req.proxy_dispatch_backoff_ms,
            )
        )

        for entry in req.extra_entries:
            if isinstance(entry, ExtraDirectory):
                yield pb.ExecuteRequest(
                    file_begin=pb.FileEntryBegin(
                        path=entry.path,
                        type=pb.FILE_TYPE_DIR,
                        mode=entry.mode,
                        uid=entry.uid,
                        gid=entry.gid,
                        mtime_unix=entry.mtime_unix,
                        symlink_target="",
                    )
                )
                yield pb.ExecuteRequest(file_end=pb.FileEntryEnd())
                continue

            if isinstance(entry, ExtraSymlink):
                yield pb.ExecuteRequest(
                    file_begin=pb.FileEntryBegin(
                        path=entry.path,
                        type=pb.FILE_TYPE_SYMLINK,
                        mode=entry.mode,
                        uid=entry.uid,
                        gid=entry.gid,
                        mtime_unix=entry.mtime_unix,
                        symlink_target=entry.target,
                    )
                )
                yield pb.ExecuteRequest(file_end=pb.FileEntryEnd())
                continue

            if isinstance(entry, ExtraFile):
                yield pb.ExecuteRequest(
                    file_begin=pb.FileEntryBegin(
                        path=entry.path,
                        type=pb.FILE_TYPE_FILE,
                        mode=entry.mode,
                        uid=entry.uid,
                        gid=entry.gid,
                        mtime_unix=entry.mtime_unix,
                        symlink_target="",
                    )
                )
                if isinstance(entry.content, (bytes, bytearray)):
                    data = bytes(entry.content)
                    step = req.upload_chunk_size
                    for i in range(0, len(data), step):
                        yield pb.ExecuteRequest(file_chunk=pb.FileChunk(data=data[i : i + step]))
                else:
                    async for chunk in entry.content:
                        if not isinstance(chunk, (bytes, bytearray)):
                            raise TypeError(f"file content chunk must be bytes, got {type(chunk)!r}")
                        if chunk:
                            yield pb.ExecuteRequest(file_chunk=pb.FileChunk(data=bytes(chunk)))
                yield pb.ExecuteRequest(file_end=pb.FileEntryEnd())
                continue

            raise TypeError(f"unsupported extra entry type: {type(entry)!r}")
