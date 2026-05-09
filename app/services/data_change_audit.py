"""Auditoria automática de inclusões, alterações e exclusões de dados."""

from __future__ import annotations

import contextvars
import json
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import event, inspect
from sqlalchemy.orm import Session as SASession

from app.models.audit_log import AuditLog

logger = logging.getLogger(__name__)

_audit_context: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "data_change_audit_context",
    default=None,
)
_listeners_installed = False
_SENSITIVE_KEYWORDS = (
    "password",
    "senha",
    "secret",
    "token",
    "key",
    "credential",
    "hash",
    "code",
    "csrf",
)
_UPDATE_NOISE_FIELDS = {"created_at", "updated_at", "last_login_at"}
_MAX_METADATA_CHARS = 8000


def set_data_change_audit_context(**payload: Any) -> contextvars.Token:
    """Define o contexto do usuário/request para os listeners do SQLAlchemy."""

    return _audit_context.set({key: value for key, value in payload.items() if value is not None})


def reset_data_change_audit_context(token: contextvars.Token | None) -> None:
    if token is not None:
        _audit_context.reset(token)


def install_data_change_audit_listeners() -> None:
    """Registra listeners uma única vez no processo."""

    global _listeners_installed
    if _listeners_installed:
        return
    event.listen(SASession, "after_flush", _audit_after_flush)
    _listeners_installed = True


def _audit_after_flush(session: SASession, _flush_context: Any) -> None:
    context = _audit_context.get()
    if not context or session.info.get("_skip_data_change_audit"):
        return

    rows: list[dict[str, Any]] = []
    try:
        for obj in list(session.new):
            if _should_skip_object(obj):
                continue
            rows.append(_build_audit_row(obj, "insert", before={}, after=_snapshot_object(obj), context=context))

        for obj in list(session.dirty):
            if _should_skip_object(obj) or not session.is_modified(obj, include_collections=False):
                continue
            before, after = _changed_values(obj)
            before = {key: value for key, value in before.items() if key not in _UPDATE_NOISE_FIELDS}
            after = {key: value for key, value in after.items() if key not in _UPDATE_NOISE_FIELDS}
            if not before and not after:
                continue
            rows.append(_build_audit_row(obj, "update", before=before, after=after, context=context))

        for obj in list(session.deleted):
            if _should_skip_object(obj):
                continue
            rows.append(_build_audit_row(obj, "delete", before=_snapshot_object(obj), after={}, context=context))

        if rows:
            session.connection().execute(AuditLog.__table__.insert(), rows)
    except Exception:
        logger.exception("Falha ao gravar auditoria automática de alteração de dados")


def _should_skip_object(obj: Any) -> bool:
    try:
        mapper = inspect(obj).mapper
    except Exception:
        return True
    table_name = getattr(mapper.local_table, "name", "")
    return table_name == AuditLog.__tablename__


def _snapshot_object(obj: Any) -> dict[str, Any]:
    state = inspect(obj)
    values: dict[str, Any] = {}
    for attr in state.mapper.column_attrs:
        key = attr.key
        values[key] = _safe_value(key, getattr(obj, key, None))
    return values


def _changed_values(obj: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    state = inspect(obj)
    before: dict[str, Any] = {}
    after: dict[str, Any] = {}
    for attr in state.mapper.column_attrs:
        key = attr.key
        history = state.attrs[key].history
        if not history.has_changes():
            continue
        old_value = history.deleted[0] if history.deleted else None
        new_value = history.added[0] if history.added else getattr(obj, key, None)
        before[key] = _safe_value(key, old_value)
        after[key] = _safe_value(key, new_value)
    return before, after


def _build_audit_row(
    obj: Any,
    operation: str,
    *,
    before: dict[str, Any],
    after: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    state = inspect(obj)
    table_name = getattr(state.mapper.local_table, "name", "")
    metadata = {
        "operation": operation,
        "model": obj.__class__.__name__,
        "table": table_name,
        "record_pk": _primary_key_values(obj),
        "before": before,
        "after": after,
    }
    return {
        "actor_user_id": context.get("actor_user_id"),
        "actor_email": context.get("actor_email"),
        "organization_id": context.get("organization_id"),
        "event_type": f"data.{operation}"[:80],
        "outcome": "success",
        "http_method": (context.get("http_method") or "")[:12] or None,
        "path": context.get("path"),
        "status_code": None,
        "ip_address": context.get("ip_address"),
        "user_agent": context.get("user_agent"),
        "duration_ms": None,
        "metadata_json": _metadata_json(metadata),
    }


def _primary_key_values(obj: Any) -> dict[str, Any]:
    state = inspect(obj)
    result: dict[str, Any] = {}
    for column in state.mapper.primary_key:
        key = column.key
        result[key] = _serialize_value(getattr(obj, key, None))
    return result


def _safe_value(key: str, value: Any) -> Any:
    lowered = (key or "").lower()
    if any(marker in lowered for marker in _SENSITIVE_KEYWORDS):
        return "***"
    return _serialize_value(value)


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return "<bytes>"
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _metadata_json(metadata: dict[str, Any]) -> str:
    try:
        return json.dumps(metadata, ensure_ascii=False, default=str)[:_MAX_METADATA_CHARS]
    except (TypeError, ValueError):
        return "{}"
