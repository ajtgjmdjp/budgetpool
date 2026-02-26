"""Memory monitoring utilities."""

from __future__ import annotations

import os
from dataclasses import dataclass

import psutil


@dataclass(frozen=True, slots=True)
class MemoryInfo:
    """Snapshot of system memory state."""

    total_gb: float
    available_gb: float
    used_gb: float
    percent: float

    @property
    def free_for_workers_gb(self) -> float:
        """Estimate of memory available for worker processes.

        Reserves 20% of total or 2GB (whichever is smaller) for OS and
        other processes.
        """
        reserve = min(self.total_gb * 0.2, 2.0)
        return max(0.0, self.available_gb - reserve)


def get_memory_info() -> MemoryInfo:
    """Get current system memory information.

    On Linux, respects cgroup memory limits (e.g. inside Docker containers).
    """
    cgroup_limit = _read_cgroup_limit()
    vm = psutil.virtual_memory()

    if cgroup_limit is not None and cgroup_limit < vm.total:
        # Inside a cgroup-limited container
        total_gb = cgroup_limit / (1024**3)
        available_gb = (cgroup_limit - vm.used) / (1024**3)
        available_gb = max(0.0, available_gb)
        used_gb = vm.used / (1024**3)
        percent = (vm.used / cgroup_limit) * 100
    else:
        total_gb = vm.total / (1024**3)
        available_gb = vm.available / (1024**3)
        used_gb = vm.used / (1024**3)
        percent = vm.percent

    return MemoryInfo(
        total_gb=round(total_gb, 2),
        available_gb=round(available_gb, 2),
        used_gb=round(used_gb, 2),
        percent=round(percent, 1),
    )


def safe_worker_count(
    memory_per_worker_gb: float = 1.0,
    max_workers: int | None = None,
) -> int:
    """Calculate safe number of workers based on available memory.

    Args:
        memory_per_worker_gb: Estimated peak memory per worker process in GB.
        max_workers: Optional upper bound (defaults to CPU count).

    Returns:
        Safe number of workers (always >= 1).
    """
    if memory_per_worker_gb <= 0:
        raise ValueError("memory_per_worker_gb must be positive")

    info = get_memory_info()
    max_by_memory = int(info.free_for_workers_gb / memory_per_worker_gb)
    max_by_cpu = max_workers if max_workers is not None else (os.cpu_count() or 4)

    workers = max(1, min(max_by_memory, max_by_cpu))
    return workers


def _read_cgroup_limit() -> int | None:
    """Read cgroup v2/v1 memory limit, if available."""
    # cgroup v2
    try:
        with open("/sys/fs/cgroup/memory.max") as f:
            val = f.read().strip()
            if val != "max":
                return int(val)
    except (FileNotFoundError, PermissionError, ValueError):
        pass

    # cgroup v1
    try:
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
            val = int(f.read().strip())
            # Very large values mean "no limit"
            if val < 2**62:
                return val
    except (FileNotFoundError, PermissionError, ValueError):
        pass

    return None
