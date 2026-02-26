#!/usr/bin/env python3
"""Benchmark: ProcessPoolExecutor vs BudgetPool under memory pressure.

Demonstrates that ProcessPoolExecutor with os.cpu_count() workers can
cause dangerous memory spikes, while BudgetPool keeps usage safe.

Usage:
    python examples/benchmark_oom.py
"""

from __future__ import annotations

import os
import time
from concurrent.futures import ProcessPoolExecutor

import psutil

from budgetpool import BudgetPool


def _allocate_and_compute(mb: int) -> dict[str, float]:
    """Simulate a memory-heavy task (e.g., portfolio optimization).

    Allocates `mb` megabytes and does some computation.
    """
    data = bytearray(mb * 1024 * 1024)
    # Simulate computation
    total = sum(data[i] for i in range(0, len(data), 4096))
    proc = psutil.Process(os.getpid())
    rss_mb = proc.memory_info().rss / (1024 * 1024)
    return {"rss_mb": rss_mb, "total": total}


def _get_system_memory_percent() -> float:
    return psutil.virtual_memory().percent


def run_naive(task_size_mb: int, n_tasks: int) -> dict[str, float]:
    """Run with ProcessPoolExecutor(max_workers=os.cpu_count())."""
    workers = os.cpu_count() or 4
    peak_percent = _get_system_memory_percent()

    start = time.monotonic()
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_allocate_and_compute, task_size_mb) for _ in range(n_tasks)]
        for f in futures:
            f.result()
            current = _get_system_memory_percent()
            peak_percent = max(peak_percent, current)
    elapsed = time.monotonic() - start

    return {
        "method": "ProcessPoolExecutor",
        "workers": workers,
        "elapsed_s": round(elapsed, 2),
        "peak_memory_percent": round(peak_percent, 1),
    }


def run_budgetpool(task_size_mb: int, n_tasks: int) -> dict[str, float]:
    """Run with BudgetPool (memory-aware)."""
    memory_per_worker_gb = task_size_mb / 1024 * 1.5  # 1.5x safety margin

    start = time.monotonic()
    with BudgetPool(
        memory_per_worker_gb=memory_per_worker_gb,
        warn_at_percent=80.0,
    ) as pool:
        futures = [pool.submit(_allocate_and_compute, task_size_mb) for _ in range(n_tasks)]
        for f in futures:
            f.result()
    elapsed = time.monotonic() - start

    return {
        "method": "BudgetPool",
        "workers": pool.num_workers,
        "elapsed_s": round(elapsed, 2),
        "peak_memory_percent": round(pool.stats.peak_memory_percent, 1),
    }


def main() -> None:
    task_size_mb = 512  # Each task allocates 512MB
    n_tasks = 16

    vm = psutil.virtual_memory()
    print(f"System: {vm.total / (1024**3):.1f}GB total, "
          f"{vm.available / (1024**3):.1f}GB available")
    print(f"Task: {task_size_mb}MB × {n_tasks} tasks")
    print(f"Naive would use: {os.cpu_count()} workers × {task_size_mb}MB "
          f"= {(os.cpu_count() or 4) * task_size_mb / 1024:.1f}GB peak")
    print()

    # BudgetPool (safe) first
    print("--- BudgetPool (memory-aware) ---")
    bp_result = run_budgetpool(task_size_mb, n_tasks)
    print(f"  Workers: {bp_result['workers']}")
    print(f"  Time: {bp_result['elapsed_s']}s")
    print(f"  Peak memory: {bp_result['peak_memory_percent']}%")
    print()

    # Naive ProcessPoolExecutor
    print("--- ProcessPoolExecutor (naive) ---")
    naive_result = run_naive(task_size_mb, n_tasks)
    print(f"  Workers: {naive_result['workers']}")
    print(f"  Time: {naive_result['elapsed_s']}s")
    print(f"  Peak memory: {naive_result['peak_memory_percent']}%")
    print()

    # Summary
    print("--- Summary ---")
    mem_diff = naive_result["peak_memory_percent"] - bp_result["peak_memory_percent"]
    print(f"  Memory saved: {mem_diff:+.1f} percentage points")
    print(f"  BudgetPool used {bp_result['workers']}/{naive_result['workers']} workers")
    if naive_result["peak_memory_percent"] > 85:
        print("  ⚠ Naive approach entered danger zone (>85%)")


if __name__ == "__main__":
    main()
