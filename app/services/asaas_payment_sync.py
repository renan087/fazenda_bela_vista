"""Sincronização de cobranças Asaas (webhook + persistência)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.models import AsaasPayment, User
from app.services.asaas_payments import is_asaas_status_paid

_USER_REF_PREFIX = re.compile(r"^user-(\d+)-", re.IGNORECASE)


def parse_user_id_from_external_reference(external_reference: str | None) -> int | None:
    if not external_reference:
        return None
    m = _USER_REF_PREFIX.match(str(external_reference).strip())
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def user_owns_asaas_payment_payload(user: User, payment: dict[str, Any]) -> bool:
    uid = parse_user_id_from_external_reference(payment.get("externalReference"))
    if uid is not None and uid == user.id:
        return True
    cust = str(payment.get("customer") or "").strip()
    ucid = (user.asaas_customer_id or "").strip()
    return bool(cust and ucid and cust == ucid)


def _parse_dt(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def infer_paid_at(payment: dict[str, Any]) -> datetime | None:
    for key in ("clientPaymentDate", "paymentDate", "confirmedDate", "creditDate"):
        dt = _parse_dt(payment.get(key))
        if dt:
            return dt
    return None


def upsert_asaas_payment_row(
    db: Session,
    *,
    payment: dict[str, Any],
    last_event: str | None,
    commit: bool = True,
) -> AsaasPayment:
    pid = str(payment.get("id") or "").strip()
    if not pid:
        raise ValueError("payment.id ausente")

    status = payment.get("status")
    status_s = str(status).strip() if status is not None else None
    ext = payment.get("externalReference")
    user_id = parse_user_id_from_external_reference(str(ext) if ext is not None else None)

    paid_at = infer_paid_at(payment)
    if is_asaas_status_paid(status_s) and paid_at is None:
        paid_at = datetime.now(timezone.utc)

    row = db.query(AsaasPayment).filter(AsaasPayment.payment_id == pid).first()
    if row is None:
        row = AsaasPayment(payment_id=pid, user_id=user_id)
        db.add(row)
    elif user_id is not None and row.user_id is None:
        row.user_id = user_id

    if status_s is not None:
        row.status = status_s
    if last_event:
        row.last_event = last_event[:80]
    if paid_at is not None:
        row.paid_at = paid_at
    elif is_asaas_status_paid(status_s) and row.paid_at is None:
        row.paid_at = datetime.now(timezone.utc)

    if commit:
        db.commit()
        db.refresh(row)
    else:
        db.flush()
    return row
