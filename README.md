# budgetpool

Memory-budget-aware parallel execution pool for Python.

A drop-in replacement for `ProcessPoolExecutor` that calculates safe worker counts based on available memory.

## Install

```bash
pip install budgetpool
```

## Quick Start

```python
from budgetpool import BudgetPool

# Instead of: ProcessPoolExecutor(max_workers=os.cpu_count())
with BudgetPool(memory_budget_gb=12.0, memory_per_worker_gb=2.0) as pool:
    results = list(pool.map(heavy_func, items))
```

## License

Apache-2.0
