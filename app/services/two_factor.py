from datetime import timedelta

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import generate_numeric_code, hash_verification_code, verify_verification_code
from app.core.timezone import utc_now
from app.models import LoginVerificationCode, User


def issue_login_verification_code(db: Session, user: User) -> str:
    settings = get_settings()
    now = utc_now()
    db.query(LoginVerificationCode).filter(
        LoginVerificationCode.user_id == user.id,
        LoginVerificationCode.used_at.is_(None),
    ).delete(synchronize_session=False)
    code = generate_numeric_code(6)
    record = LoginVerificationCode(
        user_id=user.id,
        code_hash=hash_verification_code(code),
        expires_at=now + timedelta(minutes=settings.two_factor_code_minutes),
        attempts_count=0,
        max_attempts=settings.two_factor_max_attempts,
    )
    db.add(record)
    db.commit()
    return code


def revoke_active_login_codes(db: Session, user_id: int) -> None:
    db.query(LoginVerificationCode).filter(
        LoginVerificationCode.user_id == user_id,
        LoginVerificationCode.used_at.is_(None),
    ).delete(synchronize_session=False)
    db.commit()


def get_active_login_code(db: Session, user_id: int) -> LoginVerificationCode | None:
    now = utc_now()
    return (
        db.query(LoginVerificationCode)
        .filter(
            LoginVerificationCode.user_id == user_id,
            LoginVerificationCode.used_at.is_(None),
            LoginVerificationCode.expires_at >= now,
            LoginVerificationCode.attempts_count < LoginVerificationCode.max_attempts,
        )
        .order_by(LoginVerificationCode.created_at.desc())
        .first()
    )


def verify_login_code(db: Session, user_id: int, code: str) -> tuple[bool, str]:
    now = utc_now()
    record = (
        db.query(LoginVerificationCode)
        .filter(
            LoginVerificationCode.user_id == user_id,
            LoginVerificationCode.used_at.is_(None),
        )
        .order_by(LoginVerificationCode.created_at.desc())
        .first()
    )
    if not record:
        return False, "Solicite um novo codigo de acesso."
    if record.used_at is not None:
        return False, "Este codigo ja foi utilizado."
    if record.expires_at < now:
        return False, "O codigo expirou. Solicite um novo codigo."
    if record.attempts_count >= record.max_attempts:
        return False, "Numero maximo de tentativas excedido. Solicite um novo codigo."
    if not verify_verification_code(code, record.code_hash):
        record.attempts_count += 1
        db.add(record)
        db.commit()
        if record.attempts_count >= record.max_attempts:
            return False, "Numero maximo de tentativas excedido. Solicite um novo codigo."
        return False, "Codigo invalido."
    record.used_at = now
    db.add(record)
    db.commit()
    return True, ""
