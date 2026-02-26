"""Tests for BudgetPool."""

from __future__ import annotations

import time

import pytest

from budgetpool._pool import BudgetPool, MemoryBudgetExceeded, PoolStats


def _identity(x: int) -> int:
    return x


def _slow_identity(x: int) -> int:
    time.sleep(0.01)
    return x


def _allocate_memory(mb: int) -> int:
    """Allocate approximately `mb` megabytes and return the size."""
    data = bytearray(mb * 1024 * 1024)
    return len(data)


class TestBudgetPoolBasic:
    def test_context_manager(self) -> None:
        with BudgetPool(memory_per_worker_gb=0.1) as pool:
            assert pool.num_workers >= 1

    def test_submit(self) -> None:
        with BudgetPool(memory_per_worker_gb=0.1) as pool:
            future = pool.submit(_identity, 42)
            assert future.result(timeout=5) == 42

    def test_map(self) -> None:
        with BudgetPool(memory_per_worker_gb=0.1) as pool:
            results = list(pool.map(_identity, range(10)))
            assert results == list(range(10))

    def test_map_preserves_order(self) -> None:
        with BudgetPool(memory_per_worker_gb=0.1) as pool:
            results = list(pool.map(_slow_identity, range(20)))
            assert results == list(range(20))

    def test_repr(self) -> None:
        with BudgetPool(memory_per_worker_gb=0.5) as pool:
            r = repr(pool)
            assert "BudgetPool" in r
            assert "workers=" in r
            assert "budget=" in r


class TestBudgetPoolWorkerCount:
    def test_budget_limits_workers(self) -> None:
        # 2GB budget with 1GB per worker → max 2 workers
        with BudgetPool(
            memory_budget_gb=2.0,
            memory_per_worker_gb=1.0,
            max_workers=8,
        ) as pool:
            assert pool.num_workers <= 2

    def test_cpu_limits_workers(self) -> None:
        # Huge budget but max_workers=2
        with BudgetPool(
            memory_budget_gb=999.0,
            memory_per_worker_gb=0.001,
            max_workers=2,
        ) as pool:
            assert pool.num_workers == 2

    def test_always_at_least_one_worker(self) -> None:
        with BudgetPool(
            memory_budget_gb=0.001,
            memory_per_worker_gb=999.0,
        ) as pool:
            assert pool.num_workers >= 1


class TestBudgetPoolBackpressure:
    def test_backpressure_limits_pending(self) -> None:
        with BudgetPool(
            memory_per_worker_gb=0.1,
            max_workers=2,
            max_pending=4,
        ) as pool:
            # Submit more tasks than max_pending — should not deadlock
            results = list(pool.map(_slow_identity, range(20)))
            assert len(results) == 20


class TestBudgetPoolErrors:
    def test_invalid_memory_per_worker(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            BudgetPool(memory_per_worker_gb=0)

    def test_submit_after_shutdown(self) -> None:
        pool = BudgetPool(memory_per_worker_gb=0.1)
        pool.shutdown()
        with pytest.raises(RuntimeError, match="shut down"):
            pool.submit(_identity, 1)

    def test_map_after_shutdown(self) -> None:
        pool = BudgetPool(memory_per_worker_gb=0.1)
        pool.shutdown()
        with pytest.raises(RuntimeError, match="shut down"):
            list(pool.map(_identity, [1]))


class TestBudgetPoolProperties:
    def test_memory_info(self) -> None:
        with BudgetPool(memory_per_worker_gb=0.1) as pool:
            info = pool.memory_info
            assert info.total_gb > 0

    def test_memory_budget(self) -> None:
        with BudgetPool(
            memory_budget_gb=8.0,
            memory_per_worker_gb=0.1,
        ) as pool:
            assert pool.memory_budget_gb == 8.0


def _raise_error(x: int) -> int:
    raise ValueError("test error")


class TestPoolStats:
    def test_stats_after_map(self) -> None:
        with BudgetPool(memory_per_worker_gb=0.1) as pool:
            list(pool.map(_identity, range(10)))
            assert pool.stats.tasks_submitted == 10
            assert pool.stats.tasks_completed == 10
            assert pool.stats.tasks_failed == 0
            assert pool.stats.peak_memory_percent > 0

    def test_stats_failed_tasks(self) -> None:
        with BudgetPool(memory_per_worker_gb=0.1) as pool:
            future = pool.submit(_raise_error, 1)
            try:
                future.result(timeout=5)
            except ValueError:
                pass
            assert pool.stats.tasks_failed == 1

    def test_on_task_complete_callback(self) -> None:
        completed: list[int] = []

        def on_complete(future: object) -> None:
            completed.append(1)

        with BudgetPool(
            memory_per_worker_gb=0.1,
            on_task_complete=on_complete,
        ) as pool:
            list(pool.map(_identity, range(5)))
            # Give callbacks time to fire
            import time
            time.sleep(0.1)
            assert len(completed) == 5
