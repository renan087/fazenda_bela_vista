"""Limite de tentativas de login por e-mail e por IP (memoria do processo; o Render roda um processo)."""

from __future__ import annotations

import threading
import time

WINDOW_SECONDS = 15 * 60
MAX_PER_EMAIL = 5
MAX_PER_IP = 20

_failures: dict[str, list[float]] = {}
_lock = threading.Lock()


def _recent(key: str, now: float) -> list[float]:
    hits = [t for t in _failures.get(key, []) if now - t < WINDOW_SECONDS]
    _failures[key] = hits
    return hits


def blocked_for(email: str | None, ip: str | None, now: float | None = None) -> int:
    """Segundos restantes de bloqueio (0 = liberado)."""
    now = now or time.time()
    waits = []
    with _lock:
        for key, limit in ((f"e:{(email or '').lower()}", MAX_PER_EMAIL), (f"i:{ip or '-'}", MAX_PER_IP)):
            hits = _recent(key, now)
            if len(hits) >= limit:
                waits.append(int(WINDOW_SECONDS - (now - hits[-limit])) + 1)
    return max(waits, default=0)


def register_failure(email: str | None, ip: str | None, now: float | None = None) -> None:
    now = now or time.time()
    with _lock:
        for key in (f"e:{(email or '').lower()}", f"i:{ip or '-'}"):
            _recent(key, now).append(now)


def register_success(email: str | None) -> None:
    with _lock:
        _failures.pop(f"e:{(email or '').lower()}", None)


def reset() -> None:
    with _lock:
        _failures.clear()
