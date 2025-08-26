from __future__ import annotations
from math import sqrt


def z_test_proportions(p_old: float, p_new: float, n_old: int, n_new: int) -> float | None:
    if n_old == 0 or n_new == 0:
        return None
    p_pool = (p_old * n_old + p_new * n_new) / (n_old + n_new)
    denom = sqrt(p_pool * (p_pool - 1) * (1 / n_old + 1 / n_new)) if p_pool not in (0, 1) else None
    if not denom:
        return None
    return (p_new - p_old) / denom
