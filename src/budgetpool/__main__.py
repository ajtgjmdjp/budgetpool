"""CLI entry point: python -m budgetpool status."""

from __future__ import annotations

import argparse
import sys

from budgetpool._monitor import get_memory_info, safe_worker_count


def _status() -> None:
    info = get_memory_info()
    print(f"System Memory")
    print(f"  Total:          {info.total_gb:.1f} GB")
    print(f"  Available:      {info.available_gb:.1f} GB")
    print(f"  Used:           {info.used_gb:.1f} GB ({info.percent:.0f}%)")
    print(f"  Free for workers: {info.free_for_workers_gb:.1f} GB")
    print()
    print(f"Safe Worker Counts")
    for mem in [0.5, 1.0, 2.0, 4.0, 8.0]:
        n = safe_worker_count(memory_per_worker_gb=mem)
        print(f"  {mem:.1f} GB/worker → {n} workers")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="budgetpool",
        description="Memory-budget-aware parallel execution pool",
    )
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("status", help="Show memory status and safe worker counts")

    args = parser.parse_args()

    if args.command == "status":
        _status()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
