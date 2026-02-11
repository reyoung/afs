from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import stat
from typing import AsyncIterator, List

from afs_sdk import (
    AfsClient,
    DoneEvent,
    ExecuteInput,
    ExtraDirectory,
    ExtraFile,
    ExtraSymlink,
    LogEvent,
    ResultEvent,
    TarDirectory,
    TarFilePart,
    TarSymlink,
)


async def file_chunks(path: pathlib.Path, chunk_size: int) -> AsyncIterator[bytes]:
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk


def build_entries(root_dir: str, chunk_size: int):
    if not root_dir:
        return []
    root = pathlib.Path(root_dir).resolve()
    out = []
    for p in sorted(root.rglob("*")):
        rel = p.relative_to(root).as_posix()
        st = p.lstat()
        mode = stat.S_IMODE(st.st_mode)
        if p.is_dir():
            out.append(ExtraDirectory(path=rel, mode=mode, uid=st.st_uid, gid=st.st_gid, mtime_unix=int(st.st_mtime)))
        elif p.is_symlink():
            out.append(
                ExtraSymlink(
                    path=rel,
                    target=os.readlink(p),
                    mode=mode,
                    uid=st.st_uid,
                    gid=st.st_gid,
                    mtime_unix=int(st.st_mtime),
                )
            )
        else:
            out.append(
                ExtraFile(
                    path=rel,
                    content=file_chunks(p, chunk_size),
                    mode=mode,
                    uid=st.st_uid,
                    gid=st.st_gid,
                    mtime_unix=int(st.st_mtime),
                )
            )
    return out


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", default="127.0.0.1:62051", help="afs_proxy/afslet address")
    parser.add_argument("--dir", default="", help="optional extra-dir to upload")
    parser.add_argument("--chunk-size", type=int, default=256 * 1024)
    parser.add_argument("--image", required=True)
    parser.add_argument("--tag", default="")
    parser.add_argument("--cpu", type=int, default=1)
    parser.add_argument("--memory-mb", type=int, default=256)
    parser.add_argument("--timeout-ms", type=int, default=1000)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        raise SystemExit("command is required, e.g. -- /bin/sh -c 'echo ok'")

    host, port_str = args.addr.rsplit(":", 1)
    port = int(port_str)

    req = ExecuteInput(
        image=args.image,
        tag=args.tag,
        command=args.command,
        cpu_cores=args.cpu,
        memory_mb=args.memory_mb,
        timeout_ms=args.timeout_ms,
        extra_entries=build_entries(args.dir, args.chunk_size),
        upload_chunk_size=args.chunk_size,
    )

    async with AfsClient(host, port) as client:
        async for ev in client.execute(req):
            if isinstance(ev, LogEvent):
                print(f"[log:{ev.source}] {ev.message}")
            elif isinstance(ev, ResultEvent):
                print(f"[result] success={ev.success} exit={ev.exit_code} err={ev.error!r}")
            elif isinstance(ev, TarDirectory):
                print(f"[tar:dir] {ev.path}")
            elif isinstance(ev, TarSymlink):
                print(f"[tar:symlink] {ev.path} -> {ev.target}")
            elif isinstance(ev, TarFilePart):
                print(f"[tar:file] {ev.path} offset={ev.offset} bytes={len(ev.data)} eof={ev.eof}")
            elif isinstance(ev, DoneEvent):
                print("[done]")


if __name__ == "__main__":
    asyncio.run(main())
