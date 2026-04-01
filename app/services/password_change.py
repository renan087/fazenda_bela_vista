from datetime import timedelta

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import generate_numeric_code, hash_verification_code, verify_verification_code
from app.core.timezone import utc_now
from app.models import PasswordChangeVerification, User


def issue_password_change_verification(db: Session, user: User, new_password_hash: str) -> str:
    settings = get_settings()
    now = utc_now()
    db.query(PasswordChangeVerification).filter(
        PasswordChangeVerification.user_id == user.id,
        PasswordChangeVerification.used_at.is_(None),
    ).delete(synchronize_session=False)

    code = generate_numeric_code(6)
    record = PasswordChangeVerification(
        user_id=user.id,
        code_hash=hash_verification_code(code),
        new_password_hash=new_password_hash,
        expires_at=now + timedelta(minutes=settings.two_factor_code_minutes),
        attempts_count=0,
        max_attempts=settings.two_factor_max_attempts,
    )
    db.add(record)
    db.commit()
    return code


def get_active_password_change_verification(db: Session, user_id: int) -> PasswordChangeVerification | None:
    now = utc_now()
    return (
        db.query(PasswordChangeVerification)
        .filter(
            PasswordChangeVerification.user_id == user_id,
            PasswordChangeVerification.used_at.is_(None),
            PasswordChangeVerification.expires_at >= now,
            PasswordChangeVerification.attempts_count < PasswordChangeVerification.max_attempts,
        )
        .order_by(PasswordChangeVerification.created_at.desc())
        .first()
    )


def verify_password_change_code(
    db: Session,
    user_id: int,
    code: str,
) -> tuple[bool, str, PasswordChangeVerification | None]:
    now = utc_now()
    record = (
        db.query(PasswordChangeVerification)
        .filter(
            PasswordChangeVerification.user_id == user_id,
            PasswordChangeVerification.used_at.is_(None),
        )
        .order_by(PasswordChangeVerification.created_at.desc())
        .first()
    )
    if not record:
        return False, "Solicite um novo codigo para alterar a senha.", None
    if record.expires_at < now:
        return False, "O codigo expirou. Solicite um novo codigo.", None
    if record.attempts_count >= record.max_attempts:
        return False, "Numero maximo de tentativas excedido. Solicite um novo codigo.", None
    if not verify_verification_code(code, record.code_hash):
        record.attempts_count += 1
        db.add(record)
        db.commit()
        if record.attempts_count >= record.max_attempts:
            return False, "Numero maximo de tentativas excedido. Solicite um novo codigo.", None
        return False, "Codigo invalido.", None
    record.used_at = now
    db.add(record)
    db.commit()
    db.refresh(record)
    return True, "", record


def revoke_password_change_verifications(db: Session, user_id: int) -> None:
    now = utc_now()
    db.query(PasswordChangeVerification).filter(
        PasswordChangeVerification.user_id == user_id,
        PasswordChangeVerification.used_at.is_(None),
    ).update({"used_at": now}, synchronize_session=False)
    db.commit()
