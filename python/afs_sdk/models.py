from __future__ import annotations

from dataclasses import dataclass, field
from typing import AsyncIterator, Sequence, Union


ByteChunks = AsyncIterator[bytes]


@dataclass(slots=True)
class ExtraDirectory:
    path: str
    mode: int = 0o755
    uid: int = 0
    gid: int = 0
    mtime_unix: int = 0


@dataclass(slots=True)
class ExtraSymlink:
    path: str
    target: str
    mode: int = 0o777
    uid: int = 0
    gid: int = 0
    mtime_unix: int = 0


@dataclass(slots=True)
class ExtraFile:
    path: str
    content: Union[bytes, ByteChunks]
    mode: int = 0o644
    uid: int = 0
    gid: int = 0
    mtime_unix: int = 0


ExtraEntry = Union[ExtraDirectory, ExtraSymlink, ExtraFile]


@dataclass(slots=True)
class ExecuteInput:
    image: str
    command: Sequence[str]
    tag: str = ""
    cpu_cores: int = 1
    memory_mb: int = 256
    timeout_ms: int = 1000
    discovery_addr: str = ""
    force_pull: bool = False
    node_id: str = ""
    platform_os: str = "linux"
    platform_arch: str = "amd64"
    platform_variant: str = ""
    proxy_dispatch_max_retries: int = 0
    proxy_dispatch_backoff_ms: int = 50
    extra_entries: Sequence[ExtraEntry] = field(default_factory=tuple)
    upload_chunk_size: int = 256 * 1024


@dataclass(slots=True)
class RuntimeStatus:
    running_containers: int
    limit_cpu_cores: int
    limit_memory_mb: int
    used_cpu_cores: int
    used_memory_mb: int
    available_cpu_cores: int
    available_memory_mb: int


@dataclass(slots=True)
class AcceptedEvent:
    accepted: bool


@dataclass(slots=True)
class LogEvent:
    source: str
    message: str


@dataclass(slots=True)
class ResultEvent:
    success: bool
    exit_code: int
    error: str


@dataclass(slots=True)
class TarArchiveStart:
    name: str


@dataclass(slots=True)
class TarDirectory:
    path: str
    mode: int
    uid: int
    gid: int
    uname: str
    gname: str
    mtime_unix: int


@dataclass(slots=True)
class TarSymlink:
    path: str
    target: str
    mode: int
    uid: int
    gid: int
    uname: str
    gname: str
    mtime_unix: int


@dataclass(slots=True)
class TarFilePart:
    path: str
    mode: int
    uid: int
    gid: int
    uname: str
    gname: str
    mtime_unix: int
    size: int
    offset: int
    data: bytes
    eof: bool


@dataclass(slots=True)
class DoneEvent:
    pass


ExecuteEvent = Union[
    AcceptedEvent,
    LogEvent,
    ResultEvent,
    TarArchiveStart,
    TarDirectory,
    TarSymlink,
    TarFilePart,
    DoneEvent,
]
