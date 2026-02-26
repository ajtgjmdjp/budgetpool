"""budgetpool - Memory-budget-aware parallel execution pool for Python."""

from budgetpool._pool import BudgetPool, MemoryBudgetExceeded, PoolStats
from budgetpool._monitor import MemoryInfo, get_memory_info, safe_worker_count

__all__ = [
    "BudgetPool",
    "MemoryBudgetExceeded",
    "MemoryInfo",
    "PoolStats",
    "get_memory_info",
    "safe_worker_count",
]

__version__ = "0.2.0"
