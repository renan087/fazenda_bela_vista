from datetime import timedelta

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import generate_persistent_token, hash_persistent_token
from app.core.timezone import utc_now
from app.models import PasswordResetToken, User


def issue_password_reset_token(db: Session, user: User) -> str:
    settings = get_settings()
    now = utc_now()
    db.query(PasswordResetToken).filter(
        PasswordResetToken.user_id == user.id,
        PasswordResetToken.used_at.is_(None),
    ).delete(synchronize_session=False)

    token = generate_persistent_token()
    record = PasswordResetToken(
        user_id=user.id,
        token_hash=hash_persistent_token(token),
        expires_at=now + timedelta(minutes=settings.password_reset_token_minutes),
    )
    db.add(record)
    db.commit()
    return token


def get_valid_password_reset_token(db: Session, raw_token: str | None) -> PasswordResetToken | None:
    if not raw_token:
        return None
    now = utc_now()
    return (
        db.query(PasswordResetToken)
        .filter(
            PasswordResetToken.token_hash == hash_persistent_token(raw_token),
            PasswordResetToken.used_at.is_(None),
            PasswordResetToken.expires_at >= now,
        )
        .order_by(PasswordResetToken.created_at.desc())
        .first()
    )


def consume_password_reset_token(db: Session, raw_token: str | None) -> PasswordResetToken | None:
    record = get_valid_password_reset_token(db, raw_token)
    if not record:
        return None
    record.used_at = utc_now()
    db.add(record)
    db.commit()
    db.refresh(record)
    return record


def revoke_user_password_reset_tokens(db: Session, user_id: int) -> None:
    now = utc_now()
    db.query(PasswordResetToken).filter(
        PasswordResetToken.user_id == user_id,
        PasswordResetToken.used_at.is_(None),
    ).update({"used_at": now}, synchronize_session=False)
    db.commit()
