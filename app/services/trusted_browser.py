from datetime import timedelta, timezone

from fastapi import Request
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import generate_persistent_token, hash_browser_fingerprint, hash_persistent_token
from app.core.timezone import utc_now
from app.models import TrustedBrowserToken, User
from app.services.user_preferences import parse_user_agent_label


def _as_utc(value):
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def issue_trusted_browser_token(db: Session, user: User, request: Request) -> str:
    settings = get_settings()
    now = utc_now()
    user_agent = request.headers.get("user-agent", "")
    user_agent_hash = hash_browser_fingerprint(user_agent)

    db.query(TrustedBrowserToken).filter(
        TrustedBrowserToken.user_id == user.id,
        TrustedBrowserToken.user_agent_hash == user_agent_hash,
    ).delete(synchronize_session=False)

    token = generate_persistent_token()
    record = TrustedBrowserToken(
        user_id=user.id,
        token_hash=hash_persistent_token(token),
        user_agent_hash=user_agent_hash,
        device_label=parse_user_agent_label(user_agent),
        expires_at=now + timedelta(days=settings.trusted_browser_days),
        last_used_at=now,
    )
    db.add(record)
    db.commit()
    return token


def validate_trusted_browser_token(
    db: Session,
    user: User,
    request: Request,
    raw_token: str | None,
) -> TrustedBrowserToken | None:
    if not raw_token:
        return None

    now = utc_now()
    user_agent = request.headers.get("user-agent", "")
    record = (
        db.query(TrustedBrowserToken)
        .filter(
            TrustedBrowserToken.user_id == user.id,
            TrustedBrowserToken.token_hash == hash_persistent_token(raw_token),
            TrustedBrowserToken.revoked_at.is_(None),
        )
        .first()
    )
    if not record:
        return None

    if _as_utc(record.expires_at) < now or record.user_agent_hash != hash_browser_fingerprint(user_agent):
        record.revoked_at = now
        db.add(record)
        db.commit()
        return None

    record.last_used_at = now
    if not record.device_label:
        record.device_label = parse_user_agent_label(user_agent)
    db.add(record)
    db.commit()
    return record


def list_active_trusted_browsers(
    db: Session,
    user_id: int,
    *,
    current_raw_token: str | None = None,
    current_user_agent: str | None = None,
) -> list[dict]:
    now = utc_now()
    current_hash = hash_persistent_token(current_raw_token) if current_raw_token else None
    rows = (
        db.query(TrustedBrowserToken)
        .filter(
            TrustedBrowserToken.user_id == user_id,
            TrustedBrowserToken.revoked_at.is_(None),
        )
        .order_by(TrustedBrowserToken.last_used_at.desc(), TrustedBrowserToken.created_at.desc())
        .all()
    )
    items: list[dict] = []
    for row in rows:
        exp = _as_utc(row.expires_at)
        if exp is not None and exp < now:
            continue
        is_current = bool(current_hash and row.token_hash == current_hash)
        label = row.device_label or (parse_user_agent_label(current_user_agent) if is_current else "Navegador autorizado")
        items.append(
            {
                "id": row.id,
                "label": label,
                "is_current": is_current,
                "last_used_at": row.last_used_at or row.created_at,
                "created_at": row.created_at,
                "expires_at": row.expires_at,
            }
        )
    items.sort(key=lambda item: (not item["is_current"], -(item["last_used_at"].timestamp() if item["last_used_at"] else 0)))
    return items


def revoke_single_trusted_browser(db: Session, user_id: int, token_id: int) -> bool:
    record = (
        db.query(TrustedBrowserToken)
        .filter(
            TrustedBrowserToken.id == token_id,
            TrustedBrowserToken.user_id == user_id,
            TrustedBrowserToken.revoked_at.is_(None),
        )
        .first()
    )
    if not record:
        return False
    record.revoked_at = utc_now()
    db.add(record)
    db.commit()
    return True


def revoke_other_trusted_browsers(db: Session, user_id: int, current_raw_token: str | None = None) -> int:
    now = utc_now()
    query = db.query(TrustedBrowserToken).filter(
        TrustedBrowserToken.user_id == user_id,
        TrustedBrowserToken.revoked_at.is_(None),
    )
    if current_raw_token:
        query = query.filter(TrustedBrowserToken.token_hash != hash_persistent_token(current_raw_token))
    count = query.update({"revoked_at": now}, synchronize_session=False)
    db.commit()
    return int(count or 0)


def revoke_trusted_browser_token(db: Session, raw_token: str | None) -> None:
    if not raw_token:
        return

    record = (
        db.query(TrustedBrowserToken)
        .filter(
            TrustedBrowserToken.token_hash == hash_persistent_token(raw_token),
            TrustedBrowserToken.revoked_at.is_(None),
        )
        .first()
    )
    if not record:
        return

    record.revoked_at = utc_now()
    db.add(record)
    db.commit()


def revoke_user_trusted_browsers(db: Session, user_id: int, *, commit: bool = True) -> None:
    now = utc_now()
    db.query(TrustedBrowserToken).filter(
        TrustedBrowserToken.user_id == user_id,
        TrustedBrowserToken.revoked_at.is_(None),
    ).update({"revoked_at": now}, synchronize_session=False)
    if commit:
        db.commit()
