"""Persistência de eventos de auditoria (uso dedicado; falhas não devem quebrar requests)."""

from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import Request
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.audit_log import AuditLog
from app.models.user import User

logger = logging.getLogger(__name__)

_MAX_UA = 400
_MAX_PATH = 500


def client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for") or request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()[:45]
    if request.client:
        return (request.client.host or "")[:45]
    return ""


def _truncate_path(path: str) -> str:
    path = path or ""
    if len(path) > _MAX_PATH:
        return path[: _MAX_PATH - 3] + "..."
    return path


def _truncate_ua(ua: str | None) -> str | None:
    if not ua:
        return None
    ua = ua.strip()
    if len(ua) > _MAX_UA:
        return ua[: _MAX_UA - 3] + "..."
    return ua


def append_audit_event(
    *,
    event_type: str,
    outcome: str = "success",
    request: Request | None = None,
    actor_user: User | None = None,
    actor_user_id: int | None = None,
    actor_email: str | None = None,
    organization_id: int | None = None,
    http_method: str | None = None,
    path: str | None = None,
    status_code: int | None = None,
    duration_ms: float | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Insere um evento de auditoria em sessão própria (não interfere na transação da request)."""

    def _write(session: Session) -> None:
        uid = actor_user_id
        email = (actor_email or "").strip().lower() or None
        org_id = organization_id
        if actor_user:
            uid = actor_user.id
            if email is None:
                email = (actor_user.email or "").strip().lower() or None
            if org_id is None:
                org_id = actor_user.organization_id
        meta_str: str | None = None
        if metadata:
            try:
                meta_str = json.dumps(metadata, ensure_ascii=False, default=str)[:8000]
            except (TypeError, ValueError):
                meta_str = None
        ip = client_ip(request) if request else None
        ua = _truncate_ua(request.headers.get("user-agent") if request else None)
        resolved_http_method = http_method
        resolved_path = path
        if request and resolved_http_method is None:
            resolved_http_method = request.method
        if request and resolved_path is None:
            resolved_path = _truncate_path(request.url.path)
        row = AuditLog(
            actor_user_id=uid,
            actor_email=email,
            organization_id=org_id,
            event_type=event_type[:80],
            outcome=(outcome or "success")[:20],
            http_method=(resolved_http_method or "")[:12] if resolved_http_method else None,
            path=resolved_path,
            status_code=status_code,
            ip_address=ip or None,
            user_agent=ua,
            duration_ms=duration_ms,
            metadata_json=meta_str,
        )
        session.add(row)
        session.commit()

    try:
        with SessionLocal() as session:
            _write(session)
    except Exception:
        logger.exception("Falha ao gravar auditoria event_type=%s", event_type)


def query_audit_logs(
    db: Session,
    *,
    actor_user_id: int | None = None,
    email_contains: str | None = None,
    event_type: str | None = None,
    date_from=None,
    date_to=None,
    page: int = 1,
    per_page: int = 50,
) -> tuple[list[AuditLog], int]:
    q = db.query(AuditLog)
    if actor_user_id:
        q = q.filter(AuditLog.actor_user_id == actor_user_id)
    if email_contains:
        term = f"%{(email_contains or '').strip()}%"
        q = q.filter(AuditLog.actor_email.ilike(term))
    if event_type:
        q = q.filter(AuditLog.event_type == event_type.strip())
    if date_from is not None:
        q = q.filter(AuditLog.created_at >= date_from)
    if date_to is not None:
        q = q.filter(AuditLog.created_at <= date_to)
    total = q.count()
    page = max(1, page)
    per_page = min(max(1, per_page), 200)
    offset = (page - 1) * per_page
    rows = (
        q.order_by(AuditLog.created_at.desc())
        .offset(offset)
        .limit(per_page)
        .all()
    )
    return rows, int(total)


def count_audit_logs(db: Session) -> int:
    return int(db.query(AuditLog).count())
