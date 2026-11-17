"""Modo manutencao: aviso programado, somente leitura ou fora do ar, com fim automatico."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models import SystemMaintenance

MODES = {"off": "Sistema normal", "leitura": "Somente leitura", "fora_do_ar": "Fora do ar"}
MAX_HOURS = 24

# Sempre acessiveis, mesmo fora do ar: saude do Render, pagamentos do Asaas, arquivos e telas publicas/login.
ALWAYS_OPEN_PREFIXES = ("/static/", "/health", "/favicon", "/api/webhooks/", "/login", "/auth/", "/logout")
ALWAYS_OPEN_EXACT = {"/", "/robots.txt", "/sitemap.xml", "/termos", "/privacidade"}
# Em somente leitura, POSTs que nao gravam dados de negocio continuam liberados.
READ_ONLY_WRITE_ALLOWED = ("/login", "/auth/", "/logout", "/contexto", "/api/webhooks/", "/notificacoes/lidas")


class MaintenanceError(ValueError):
    pass


@dataclass(frozen=True)
class MaintenanceState:
    mode: str = "off"
    message: str | None = None
    announce_at: datetime | None = None
    ends_at: datetime | None = None
    source: str = "banco"  # banco | variavel

    @property
    def active(self) -> bool:
        return self.mode in {"leitura", "fora_do_ar"}

    def announced(self, now: datetime) -> bool:
        return not self.active and self.announce_at is not None and self.announce_at > now

    def retry_after_seconds(self, now: datetime) -> int:
        return max(60, int((self.ends_at - now).total_seconds())) if self.ends_at and self.ends_at > now else 300


OFF = MaintenanceState()


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def compute_state(db: Session, now: datetime) -> MaintenanceState:
    # Plano B: MAINTENANCE_MODE no Render vale mesmo se a tela estiver inacessivel.
    forced = (getattr(get_settings(), "maintenance_mode", None) or "").strip().lower()
    if forced in {"leitura", "fora_do_ar"}:
        return MaintenanceState(mode=forced, message="Manutenção em andamento.", source="variavel")
    row = db.get(SystemMaintenance, 1)
    if not row:
        return OFF
    ends_at, announce_at = _utc(row.ends_at), _utc(row.announce_at)
    mode = row.mode if row.mode in MODES else "off"
    if mode != "off" and ends_at and ends_at <= now:
        mode = "off"  # fim automatico: ninguem esquece o sistema travado
    if mode == "off" and (not announce_at or announce_at <= now):
        return OFF
    return MaintenanceState(mode=mode, message=row.message, announce_at=announce_at, ends_at=ends_at)


_CACHE_SECONDS = 5
_cache: tuple[float, MaintenanceState] | None = None
_lock = threading.Lock()


def current_state(db_factory, now: datetime | None = None) -> MaintenanceState:
    global _cache
    mono = time.monotonic()
    with _lock:
        if _cache and mono - _cache[0] < _CACHE_SECONDS:
            return _cache[1]
    try:
        with db_factory() as db:
            state = compute_state(db, now or datetime.now(timezone.utc))
    except Exception:
        state = OFF  # sem banco, nao trava o sistema por causa do modo manutencao
    with _lock:
        _cache = (mono, state)
    return state


def invalidate() -> None:
    global _cache
    with _lock:
        _cache = None


def save_state(db: Session, *, mode: str, message: str | None, announce_at: datetime | None, ends_at: datetime | None,
               user_email: str | None, now: datetime) -> SystemMaintenance:
    if mode not in MODES:
        raise MaintenanceError("Escolha uma situação válida.")
    text = (message or "").strip()[:300] or None
    if mode != "off":
        if not ends_at or ends_at <= now:
            raise MaintenanceError("Informe a previsão de volta (no futuro). O sistema volta sozinho nesse horário.")
        if (ends_at - now).total_seconds() > MAX_HOURS * 3600:
            raise MaintenanceError(f"A manutenção pode durar no máximo {MAX_HOURS} horas.")
        announce_at = None
    elif announce_at:
        if announce_at <= now:
            raise MaintenanceError("O aviso precisa ser de uma manutenção futura.")
        if ends_at and ends_at <= announce_at:
            raise MaintenanceError("O fim previsto precisa ser depois do início.")
    row = db.get(SystemMaintenance, 1) or SystemMaintenance(id=1)
    row.mode, row.message, row.announce_at, row.ends_at, row.updated_by_email = mode, text, announce_at, ends_at if (mode != "off" or announce_at) else None, user_email
    db.add(row)
    db.commit()
    invalidate()
    return row


def is_open_path(path: str) -> bool:
    return path in ALWAYS_OPEN_EXACT or path.startswith(ALWAYS_OPEN_PREFIXES)
