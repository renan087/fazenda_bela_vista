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
    if hits:
        _failures[key] = hits
    else:
        _failures.pop(key, None)
    return hits


def _email_key(email: str | None) -> str:
    return f"e:{(email or '').strip().lower()}"


def _blocked_wait(key: str, limit: int, now: float) -> int:
    hits = _recent(key, now)
    if len(hits) < limit:
        return 0
    return int(WINDOW_SECONDS - (now - hits[-limit])) + 1


def blocked_for(email: str | None, ip: str | None, now: float | None = None) -> int:
    """Segundos restantes de bloqueio (0 = liberado)."""
    now = time.time() if now is None else now
    with _lock:
        return max(
            _blocked_wait(_email_key(email), MAX_PER_EMAIL, now),
            _blocked_wait(f"i:{ip or '-'}", MAX_PER_IP, now),
        )


def email_blocked_for(email: str) -> int:
    """Segundos restantes do bloqueio associado ao e-mail."""
    with _lock:
        return _blocked_wait(_email_key(email), MAX_PER_EMAIL, time.time())


def unlock_email(email: str) -> bool:
    """Remove o bloqueio ativo do e-mail e preserva os limites por IP."""
    key = _email_key(email)
    with _lock:
        if not _blocked_wait(key, MAX_PER_EMAIL, time.time()):
            return False
        del _failures[key]
        return True


def register_failure(email: str | None, ip: str | None, now: float | None = None) -> None:
    now = time.time() if now is None else now
    with _lock:
        for key in (_email_key(email), f"i:{ip or '-'}"):
            hits = _recent(key, now)
            hits.append(now)
            _failures[key] = hits


def register_success(email: str | None) -> None:
    with _lock:
        _failures.pop(_email_key(email), None)


def reset() -> None:
    with _lock:
        _failures.clear()
