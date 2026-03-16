from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version as package_version
from pathlib import Path
import re
import subprocess

_DESCRIBE_PATTERN = re.compile(
    r"^(?P<tag>.+)-(?P<distance>\d+)-g(?P<commit>[0-9a-f]+)(?P<dirty>-dirty)?$"
)
_COMMIT_PATTERN = re.compile(r"^(?P<commit>[0-9a-f]{7,})(?P<dirty>-dirty)?$")


@dataclass(frozen=True, slots=True)
class VersionInfo:
    version: str
    tag: str | None
    commit_sha: str | None
    distance: int | None
    dirty: bool


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _describe_from_git() -> str | None:
    try:
        completed = subprocess.run(
            [
                "git",
                "describe",
                "--tags",
                "--long",
                "--always",
                "--dirty",
                "--match",
                "v*",
            ],
            cwd=_repo_root(),
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, OSError, subprocess.CalledProcessError):
        return None

    describe = completed.stdout.strip()
    return describe or None


def _fallback_version() -> str:
    try:
        return package_version("afs-sdk")
    except PackageNotFoundError:
        return "0.0.0+unknown"


def _parse_version(describe: str) -> VersionInfo:
    describe_match = _DESCRIBE_PATTERN.fullmatch(describe)
    if describe_match is not None:
        return VersionInfo(
            version=describe,
            tag=describe_match.group("tag"),
            commit_sha=describe_match.group("commit"),
            distance=int(describe_match.group("distance")),
            dirty=describe_match.group("dirty") is not None,
        )

    commit_match = _COMMIT_PATTERN.fullmatch(describe)
    if commit_match is not None:
        return VersionInfo(
            version=describe,
            tag=None,
            commit_sha=commit_match.group("commit"),
            distance=None,
            dirty=commit_match.group("dirty") is not None,
        )

    return VersionInfo(
        version=describe,
        tag=describe,
        commit_sha=None,
        distance=None,
        dirty=describe.endswith("-dirty"),
    )


@lru_cache(maxsize=1)
def get_version_info() -> VersionInfo:
    describe = _describe_from_git()
    if describe is not None:
        return _parse_version(describe)

    version = _fallback_version()
    return VersionInfo(
        version=version,
        tag=version,
        commit_sha=None,
        distance=None,
        dirty=False,
    )


def get_version() -> str:
    return get_version_info().version


__version__ = get_version()

