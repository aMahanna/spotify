"""Timing helpers for enrichment."""

from __future__ import annotations

import threading
from typing import Dict

_timing_lock = threading.Lock()
_timings: Dict[str, float] = {}


def record_timing(name: str, elapsed: float) -> None:
    with _timing_lock:
        _timings[name] = _timings.get(name, 0.0) + elapsed


def reset_timing_report() -> None:
    with _timing_lock:
        _timings.clear()


def get_timing_report() -> Dict[str, float]:
    with _timing_lock:
        return dict(_timings)
