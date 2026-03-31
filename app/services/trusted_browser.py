from datetime import datetime, timedelta, timezone

from fastapi import Request
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import generate_persistent_token, hash_browser_fingerprint, hash_persistent_token
from app.models import TrustedBrowserToken, User


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def issue_trusted_browser_token(db: Session, user: User, request: Request) -> str:
    settings = get_settings()
    now = _utcnow()
    user_agent_hash = hash_browser_fingerprint(request.headers.get("user-agent", ""))

    db.query(TrustedBrowserToken).filter(
        TrustedBrowserToken.user_id == user.id,
        TrustedBrowserToken.user_agent_hash == user_agent_hash,
    ).delete(synchronize_session=False)

    token = generate_persistent_token()
    record = TrustedBrowserToken(
        user_id=user.id,
        token_hash=hash_persistent_token(token),
        user_agent_hash=user_agent_hash,
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

    now = _utcnow()
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

    if record.expires_at < now or record.user_agent_hash != hash_browser_fingerprint(request.headers.get("user-agent", "")):
        record.revoked_at = now
        db.add(record)
        db.commit()
        return None

    record.last_used_at = now
    db.add(record)
    db.commit()
    return record


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

    record.revoked_at = _utcnow()
    db.add(record)
    db.commit()


def revoke_user_trusted_browsers(db: Session, user_id: int) -> None:
    now = _utcnow()
    db.query(TrustedBrowserToken).filter(
        TrustedBrowserToken.user_id == user_id,
        TrustedBrowserToken.revoked_at.is_(None),
    ).update({"revoked_at": now}, synchronize_session=False)
    db.commit()
