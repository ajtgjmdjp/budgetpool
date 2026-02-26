"""Tests for memory monitoring utilities."""

from __future__ import annotations

import pytest

from budgetpool._monitor import MemoryInfo, get_memory_info, safe_worker_count


class TestMemoryInfo:
    def test_basic_snapshot(self) -> None:
        info = get_memory_info()
        assert info.total_gb > 0
        assert info.available_gb >= 0
        assert info.used_gb >= 0
        assert 0 <= info.percent <= 100

    def test_free_for_workers(self) -> None:
        info = get_memory_info()
        # free_for_workers should be less than available (reserves some)
        assert info.free_for_workers_gb <= info.available_gb

    def test_frozen(self) -> None:
        info = get_memory_info()
        with pytest.raises(AttributeError):
            info.total_gb = 999  # type: ignore[misc]


class TestSafeWorkerCount:
    def test_returns_at_least_one(self) -> None:
        # Even with huge memory per worker, should return >= 1
        count = safe_worker_count(memory_per_worker_gb=9999.0)
        assert count >= 1

    def test_respects_max_workers(self) -> None:
        count = safe_worker_count(memory_per_worker_gb=0.001, max_workers=3)
        assert count <= 3

    def test_invalid_memory_per_worker(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            safe_worker_count(memory_per_worker_gb=0)

        with pytest.raises(ValueError, match="positive"):
            safe_worker_count(memory_per_worker_gb=-1)

    def test_reasonable_default(self) -> None:
        count = safe_worker_count(memory_per_worker_gb=1.0)
        assert 1 <= count <= 128  # sane range
