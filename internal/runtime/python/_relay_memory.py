"""cgroup memory discovery and read-only sampling.

Embedded beside the bootstrap and written to ``/relay/_relay_memory.py`` in the
runtime image.

The per-invocation report includes the container's current cgroup memory usage
and its lifetime peak. cgroup v2 is preferred (``memory.current``/
``memory.peak``) and v1 is the fallback (``memory.usage_in_bytes``/
``memory.max_usage_in_bytes``). Membership and mount layout come from
``/proc/self/cgroup`` and ``/proc/self/mountinfo``; no fixed ``/sys/fs/cgroup``
layout is assumed.

Discovery is read-only, happens once per process, and caches only the resulting
candidate file paths. The bootstrap initializes it after INIT_START, before
serving, so the first invocation's measurement never includes proc/mountinfo
parsing. Sampling reads the resolved counters without polling or writing. Every
read is best-effort: an unavailable value is omitted, never fabricated, and a
read failure never changes an invocation's result.
"""

import os
from typing import NamedTuple

PROC_SELF_CGROUP = "/proc/self/cgroup"
PROC_SELF_MOUNTINFO = "/proc/self/mountinfo"


class MemoryPaths(NamedTuple):
    """Resolved current and lifetime-peak counter files for one cgroup mount."""

    current: str
    peak: str


class MemoryUsage(NamedTuple):
    """One sample; a field is None when its counter is unavailable."""

    current_bytes: int | None
    peak_bytes: int | None


_UNSET = object()
_memory_paths = _UNSET


def _unescape_mountinfo(field):
    """Decode the octal escapes mountinfo uses for spaces and control bytes."""
    if "\\" not in field:
        return field
    out = []
    i = 0
    while i < len(field):
        if (
            field[i] == "\\"
            and i + 3 < len(field)
            and all(ch in "01234567" for ch in field[i + 1 : i + 4])
        ):
            out.append(chr(int(field[i + 1 : i + 4], 8)))
            i += 4
            continue
        out.append(field[i])
        i += 1
    return "".join(out)


class _CgroupMount(NamedTuple):
    """One cgroup/cgroup2 mount record parsed from mountinfo."""

    root: str
    mount_point: str
    fstype: str
    super_options: list[str]


def _parse_mountinfo(text):
    """Return CgroupMount records for cgroup/cgroup2 mounts."""
    mounts = []
    for line in text.splitlines():
        fields = line.split()
        try:
            separator = fields.index("-", 6)
        except ValueError:
            continue
        if separator + 1 >= len(fields):
            continue
        fstype = fields[separator + 1]
        if fstype not in ("cgroup", "cgroup2"):
            continue
        super_options = fields[separator + 3] if separator + 3 < len(fields) else ""
        mounts.append(
            _CgroupMount(
                root=_unescape_mountinfo(fields[3]),
                mount_point=_unescape_mountinfo(fields[4]),
                fstype=fstype,
                super_options=super_options.split(","),
            )
        )
    return mounts


def _cgroup_v2_path(text):
    """Return the unified hierarchy path (``0::<path>``) or None."""
    for line in text.splitlines():
        hierarchy, _, path = line.partition(":")
        if hierarchy != "0":
            continue
        controllers, _, path = path.partition(":")
        if controllers == "" and path:
            return path
    return None


def _cgroup_v1_memory_path(text):
    """Return the memory controller's hierarchy path or None."""
    for line in text.splitlines():
        fields = line.split(":", 2)
        if len(fields) != 3:
            continue
        _, controllers, path = fields
        if not controllers or not path:
            continue
        if "memory" in controllers.split(","):
            return path
    return None


def _mount_directory(mount_root, mount_point, cgroup_path):
    """Resolve a mount's directory for the process, rejecting escaped paths."""
    if (
        not mount_point.startswith("/")
        or not cgroup_path.startswith("/")
        or "\0" in mount_point
        or "\0" in cgroup_path
    ):
        return None

    if mount_root and mount_root != "/":
        if not mount_root.startswith("/"):
            return None
        if cgroup_path == mount_root:
            relative = "/"
        elif cgroup_path.startswith(mount_root + "/"):
            relative = cgroup_path[len(mount_root) :]
        else:
            # The membership is outside this mount's root, so appending it
            # under mount_point would address an unrelated cgroup.
            return None
    else:
        relative = cgroup_path

    point = os.path.normpath(mount_point)
    directory = os.path.normpath(os.path.join(point, relative.lstrip("/")))
    try:
        if os.path.commonpath([point, directory]) != point:
            return None
    except ValueError:
        return None
    return directory


def resolve_memory_paths(cgroup_text, mountinfo_text):
    """Resolve cgroup memory candidates from fixture-able process text.

    Pure: production passes the contents of ``/proc/self/cgroup`` and
    ``/proc/self/mountinfo``; tests pass literal layouts. Returns a tuple of
    MemoryPaths in preference order (cgroup v2 then v1), empty when no mount
    resolves safely.
    """
    mounts = _parse_mountinfo(mountinfo_text)
    paths = []

    v2_path = _cgroup_v2_path(cgroup_text)
    if v2_path is not None:
        for mount in mounts:
            if mount.fstype != "cgroup2":
                continue
            directory = _mount_directory(mount.root, mount.mount_point, v2_path)
            if directory is not None:
                paths.append(
                    MemoryPaths(
                        os.path.join(directory, "memory.current"),
                        os.path.join(directory, "memory.peak"),
                    )
                )

    v1_path = _cgroup_v1_memory_path(cgroup_text)
    if v1_path is not None:
        for mount in mounts:
            if mount.fstype != "cgroup" or "memory" not in mount.super_options:
                continue
            directory = _mount_directory(mount.root, mount.mount_point, v1_path)
            if directory is not None:
                paths.append(
                    MemoryPaths(
                        os.path.join(directory, "memory.usage_in_bytes"),
                        os.path.join(directory, "memory.max_usage_in_bytes"),
                    )
                )

    return tuple(paths)


def initialize():
    """Resolve and cache the process's cgroup memory paths once at startup.

    Called by the bootstrap immediately after INIT_START, before serving, so
    the first invocation's measurement never includes proc/mountinfo parsing.
    """
    global _memory_paths
    try:
        _discover_memory_paths()
    except Exception:
        _memory_paths = ()


def _read_proc_text(path):
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            return handle.read()
    except OSError:
        return None


def _discover_memory_paths():
    """Return the cached resolved memory paths, discovering at most once."""
    global _memory_paths
    if _memory_paths is not _UNSET:
        return _memory_paths
    cgroup_text = _read_proc_text(PROC_SELF_CGROUP)
    mountinfo_text = _read_proc_text(PROC_SELF_MOUNTINFO)
    _memory_paths = (
        resolve_memory_paths(cgroup_text, mountinfo_text)
        if cgroup_text is not None and mountinfo_text is not None
        else ()
    )
    return _memory_paths


def read_memory_value(path):
    """Read one cgroup byte value, returning None when unavailable/malformed."""
    if path is None:
        return None
    try:
        with open(path, encoding="ascii") as handle:
            text = handle.read().strip()
    except (OSError, UnicodeDecodeError):
        return None
    if not text or not all(ch in "0123456789" for ch in text):
        return None
    return int(text)


def _memory_candidates(paths):
    """Expand an injectable single candidate or collection into candidates."""
    if paths is None:
        return _discover_memory_paths()
    if isinstance(paths, MemoryPaths):
        return (paths,)
    return paths


def sample_memory(paths=None):
    """Read current and lifetime peak memory from the same cgroup candidate.

    Candidates are tried in preference order. For each one both counters are
    read, and the first candidate that yields either value decides the sample:
    the other field is reported unavailable rather than taken from a different
    cgroup, so the two numbers always describe the same cgroup. ``paths`` is
    injectable so tests can point at temp files; production omits it and uses
    the process's once-resolved candidates.
    """
    try:
        for candidate in _memory_candidates(paths):
            current = read_memory_value(candidate.current)
            peak = read_memory_value(candidate.peak)
            if current is not None or peak is not None:
                return MemoryUsage(current, peak)
    except Exception:
        # A resource read is diagnostic only; it must never fail an invocation.
        pass
    return MemoryUsage(None, None)
