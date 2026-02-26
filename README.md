# budgetpool

[![CI](https://github.com/ajtgjmdjp/budgetpool/actions/workflows/ci.yml/badge.svg)](https://github.com/ajtgjmdjp/budgetpool/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/budgetpool)](https://pypi.org/project/budgetpool/)
[![Python](https://img.shields.io/pypi/pyversions/budgetpool)](https://pypi.org/project/budgetpool/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Memory-budget-aware parallel execution pool for Python.

A drop-in replacement for `ProcessPoolExecutor` that calculates safe worker counts based on available memory, prevents OOM crashes, and provides backpressure on task submission.

## Why?

`ProcessPoolExecutor` spawns workers based on CPU count, ignoring memory.
When each worker loads a large dataset or model, this easily causes OOM kills:

```python
# Dangerous — 8 workers × 3GB each = 24GB on a 16GB machine
with ProcessPoolExecutor(max_workers=os.cpu_count()) as pool:
    results = list(pool.map(run_backtest, param_grid))
```

`BudgetPool` fixes this by computing safe worker counts from your memory budget:

```python
from budgetpool import BudgetPool

# Safe — fits within 12GB, 2GB per worker → 6 workers max
with BudgetPool(memory_budget_gb=12.0, memory_per_worker_gb=2.0) as pool:
    results = list(pool.map(run_backtest, param_grid))
```

## Install

```bash
pip install budgetpool
```

## Quick Start

### Basic Usage

```python
from budgetpool import BudgetPool

with BudgetPool(memory_per_worker_gb=2.0) as pool:
    results = list(pool.map(heavy_func, items))
    print(f"Used {pool.num_workers} workers")
```

### With Explicit Budget

```python
with BudgetPool(
    memory_budget_gb=12.0,      # Total budget for all workers
    memory_per_worker_gb=2.0,   # Estimated peak per worker
    max_workers=8,              # CPU cap (optional)
) as pool:
    futures = [pool.submit(process, item) for item in items]
    results = [f.result() for f in futures]
```

### Memory Monitoring

```python
from budgetpool import get_memory_info, safe_worker_count

# Check system memory
info = get_memory_info()
print(f"Total: {info.total_gb:.1f}GB, Available: {info.available_gb:.1f}GB")
print(f"Safe for workers: {info.free_for_workers_gb:.1f}GB")

# Calculate worker count without creating a pool
n = safe_worker_count(memory_per_worker_gb=3.0)
print(f"Safe worker count: {n}")
```

## How It Works

1. **Startup**: Reads system memory (via `psutil`), respects cgroup limits in containers
2. **Worker calculation**: `min(budget ÷ per_worker, cpu_count, max_workers)`
3. **Backpressure**: Blocks `submit()` when pending tasks exceed `max_pending` (default: 2× workers)
4. **Runtime checks**: Warns at 85% memory usage, raises `MemoryBudgetExceeded` at 95%

## API Reference

### `BudgetPool`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `memory_budget_gb` | `float \| None` | Auto-detect | Total memory budget for all workers |
| `memory_per_worker_gb` | `float` | `1.0` | Estimated peak memory per worker |
| `max_workers` | `int \| None` | CPU count | Hard cap on worker count |
| `max_pending` | `int \| None` | `2 × workers` | Backpressure threshold |
| `warn_at_percent` | `float \| None` | `85.0` | Log warning at this memory % |
| `fail_at_percent` | `float \| None` | `95.0` | Raise error at this memory % |
| `mp_context` | | `None` | Multiprocessing start method |
| `on_task_complete` | `Callable \| None` | `None` | Callback fired on each task completion |

**Methods**: `submit()`, `map()`, `shutdown()` — same signatures as `ProcessPoolExecutor`.

**Properties**: `num_workers`, `memory_budget_gb`, `memory_info`, `stats`.

### `PoolStats`

Cumulative statistics available via `pool.stats`:

| Field | Type | Description |
|---|---|---|
| `tasks_submitted` | `int` | Total tasks submitted |
| `tasks_completed` | `int` | Successfully completed tasks |
| `tasks_failed` | `int` | Tasks that raised exceptions |
| `memory_warnings` | `int` | Times memory warning threshold was hit |
| `peak_memory_percent` | `float` | Highest observed system memory % |

### `safe_worker_count(memory_per_worker_gb, max_workers=None) → int`

Standalone function to calculate safe worker count without creating a pool.

### `get_memory_info() → MemoryInfo`

Returns a `MemoryInfo` dataclass with `total_gb`, `available_gb`, `used_gb`, `percent`, and `free_for_workers_gb`.

## CLI

Check system memory and safe worker counts:

```bash
$ python -m budgetpool status
System Memory
  Total:          16.0 GB
  Available:      6.5 GB
  Used:           8.1 GB (60%)
  Free for workers: 4.5 GB

Safe Worker Counts
  0.5 GB/worker → 8 workers
  1.0 GB/worker → 4 workers
  2.0 GB/worker → 2 workers
  4.0 GB/worker → 1 workers
  8.0 GB/worker → 1 workers
```

## Container Support

budgetpool automatically detects cgroup v1/v2 memory limits, so it works correctly inside Docker containers where `psutil.virtual_memory().total` would report host memory:

```bash
docker run --memory=4g python -c "
from budgetpool import get_memory_info
print(get_memory_info().total_gb)  # → 4.0, not host memory
"
```

## Requirements

- Python 3.10+
- [psutil](https://pypi.org/project/psutil/) ≥ 5.9.0

## License

[Apache-2.0](LICENSE)
