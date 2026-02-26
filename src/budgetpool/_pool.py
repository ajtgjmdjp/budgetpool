"""BudgetPool - Memory-budget-aware process pool executor."""

from __future__ import annotations

import logging
import os
import threading
import time
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Iterator

import psutil

from budgetpool._monitor import MemoryInfo, get_memory_info, safe_worker_count

logger = logging.getLogger("budgetpool")


@dataclass
class PoolStats:
    """Cumulative statistics for a BudgetPool instance."""

    tasks_submitted: int = 0
    tasks_completed: int = 0
    tasks_failed: int = 0
    memory_warnings: int = 0
    peak_memory_percent: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


class MemoryBudgetExceeded(RuntimeError):
    """Raised when memory usage exceeds the configured budget."""

    def __init__(self, budget_gb: float, current_gb: float) -> None:
        self.budget_gb = budget_gb
        self.current_gb = current_gb
        super().__init__(
            f"Memory budget exceeded: {current_gb:.1f}GB used, "
            f"budget is {budget_gb:.1f}GB"
        )


class BudgetPool:
    """A memory-budget-aware drop-in replacement for ProcessPoolExecutor.

    Calculates a safe number of workers at startup based on available
    memory and the estimated per-worker memory usage. Provides
    backpressure by limiting the number of concurrently pending tasks.

    Example::

        with BudgetPool(memory_budget_gb=12.0, memory_per_worker_gb=2.0) as pool:
            results = list(pool.map(heavy_func, items))

    Args:
        memory_budget_gb: Total memory budget for all workers combined.
            If None, uses available memory minus OS reserve.
        memory_per_worker_gb: Estimated peak memory per worker process.
        max_workers: Hard upper bound on worker count (regardless of
            memory). Defaults to CPU count.
        max_pending: Maximum number of tasks queued beyond active workers.
            Provides backpressure to prevent memory spikes from queued
            data. Defaults to 2x worker count.
        warn_at_percent: Log a warning when system memory usage exceeds
            this percentage. Set to None to disable.
        fail_at_percent: Raise MemoryBudgetExceeded when system memory
            exceeds this percentage on task submission. Set to None to
            disable.
        mp_context: Multiprocessing context (e.g. ``"spawn"``).
            Defaults to None (system default).
    """

    def __init__(
        self,
        memory_budget_gb: float | None = None,
        memory_per_worker_gb: float = 1.0,
        max_workers: int | None = None,
        max_pending: int | None = None,
        warn_at_percent: float | None = 85.0,
        fail_at_percent: float | None = 95.0,
        mp_context: Any = None,
        on_task_complete: Callable[[Future[Any]], None] | None = None,
    ) -> None:
        if memory_per_worker_gb <= 0:
            raise ValueError("memory_per_worker_gb must be positive")

        self._memory_per_worker_gb = memory_per_worker_gb
        self._warn_at_percent = warn_at_percent
        self._fail_at_percent = fail_at_percent

        # Determine memory budget
        info = get_memory_info()
        if memory_budget_gb is not None:
            self._budget_gb = memory_budget_gb
        else:
            self._budget_gb = info.free_for_workers_gb

        # Calculate worker count from budget
        budget_workers = max(1, int(self._budget_gb / memory_per_worker_gb))
        cpu_limit = max_workers if max_workers is not None else (os.cpu_count() or 4)
        self._num_workers = min(budget_workers, cpu_limit)

        # Backpressure: limit pending futures
        self._max_pending = (
            max_pending if max_pending is not None else self._num_workers * 2
        )

        logger.info(
            "BudgetPool: budget=%.1fGB, per_worker=%.1fGB, workers=%d "
            "(cpu_limit=%d, budget_limit=%d), system=%.1f/%.1fGB (%.0f%%)",
            self._budget_gb,
            memory_per_worker_gb,
            self._num_workers,
            cpu_limit,
            budget_workers,
            info.used_gb,
            info.total_gb,
            info.percent,
        )

        self._executor = ProcessPoolExecutor(
            max_workers=self._num_workers,
            mp_context=mp_context,
        )
        self._semaphore = threading.Semaphore(self._max_pending)
        self._closed = False
        self._stats = PoolStats()
        self._on_task_complete = on_task_complete

    @property
    def num_workers(self) -> int:
        """Number of worker processes."""
        return self._num_workers

    @property
    def memory_budget_gb(self) -> float:
        """Configured memory budget in GB."""
        return self._budget_gb

    @property
    def memory_info(self) -> MemoryInfo:
        """Current system memory snapshot."""
        return get_memory_info()

    @property
    def stats(self) -> PoolStats:
        """Cumulative pool statistics."""
        return self._stats

    def submit(
        self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any
    ) -> Future[Any]:
        """Submit a task, with backpressure and memory checks.

        Blocks if the number of pending futures exceeds ``max_pending``.
        """
        if self._closed:
            raise RuntimeError("BudgetPool is shut down")

        self._check_memory()
        self._semaphore.acquire()

        future = self._executor.submit(fn, *args, **kwargs)
        with self._stats._lock:
            self._stats.tasks_submitted += 1
        future.add_done_callback(self._on_future_done)
        return future

    def map(
        self,
        fn: Callable[..., Any],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[Any]:
        """Memory-aware version of ProcessPoolExecutor.map.

        Submits tasks with backpressure to avoid overwhelming memory.
        """
        if self._closed:
            raise RuntimeError("BudgetPool is shut down")

        futures: list[Future[Any]] = []
        for args in zip(*iterables):
            future = self.submit(fn, *args)
            futures.append(future)

        # Yield results in submission order
        for future in futures:
            result = future.result(timeout=timeout)
            yield result

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """Shut down the pool."""
        self._closed = True
        self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)

    def __enter__(self) -> BudgetPool:
        return self

    def __exit__(self, *args: Any) -> None:
        self.shutdown(wait=True)

    def __repr__(self) -> str:
        info = get_memory_info()
        return (
            f"BudgetPool(workers={self._num_workers}, "
            f"budget={self._budget_gb:.1f}GB, "
            f"system={info.available_gb:.1f}GB free)"
        )

    def _check_memory(self) -> None:
        """Check memory and warn/fail if thresholds exceeded."""
        info = get_memory_info()

        with self._stats._lock:
            if info.percent > self._stats.peak_memory_percent:
                self._stats.peak_memory_percent = info.percent

        if self._fail_at_percent is not None and info.percent >= self._fail_at_percent:
            raise MemoryBudgetExceeded(
                budget_gb=self._budget_gb,
                current_gb=info.used_gb,
            )

        if self._warn_at_percent is not None and info.percent >= self._warn_at_percent:
            with self._stats._lock:
                self._stats.memory_warnings += 1
            logger.warning(
                "BudgetPool: memory at %.1f%% (%s/%sGB). "
                "Consider reducing workload.",
                info.percent,
                info.used_gb,
                info.total_gb,
            )

    def _on_future_done(self, future: Future[Any]) -> None:
        """Release semaphore slot and update stats when a task completes."""
        with self._stats._lock:
            if future.exception() is not None:
                self._stats.tasks_failed += 1
            else:
                self._stats.tasks_completed += 1
        self._semaphore.release()
        if self._on_task_complete is not None:
            self._on_task_complete(future)
