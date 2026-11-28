from __future__ import annotations

import base64
import hmac
import math
import re
import secrets
from datetime import datetime, timedelta, timezone
from io import BytesIO

import pyotp
import qrcode
from cryptography.fernet import Fernet, InvalidToken
from fastapi import Request
from fastapi.responses import JSONResponse
from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.orm import Session

from app.core.admin_access import is_super_admin_email
from app.core.config import get_settings
from app.core.security import generate_numeric_code, hash_persistent_token, verify_password
from app.core.session import authenticator_session_version
from app.core.timezone import utc_now
from app.models import AuthenticatorChallenge, AuthenticatorRecoveryCode, User
from app.services.asaas_environment import current_asaas_environment
from app.services.audit_log_service import append_audit_event
from app.services.trusted_browser import revoke_user_trusted_browsers
from app.services.two_factor import revoke_active_login_codes

LOGIN_CHALLENGE = "login"
ENROLLMENT_CHALLENGE = "enrollment"
ATTEMPT_WINDOW = timedelta(minutes=5)
RESEND_DELAY = timedelta(seconds=60)
RECOVERY_CODE_COUNT = 10
NO_STORE = {"Cache-Control": "no-store"}


class AuthenticatorError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 400, *, retry_after: int | None = None):
        super().__init__(message)
        self.code = code
        self.status_code = status_code
        self.retry_after = retry_after


def authenticator_reply(payload: dict, *, status_code: int = 200, retry_after: int | None = None) -> JSONResponse:
    headers = dict(NO_STORE)
    if retry_after is not None:
        headers["Retry-After"] = str(retry_after)
    return JSONResponse(payload, status_code=status_code, headers=headers)


def authenticator_error_reply(error: AuthenticatorError) -> JSONResponse:
    return authenticator_reply(
        {"detail": {"code": error.code, "message": str(error)}},
        status_code=error.status_code,
        retry_after=error.retry_after,
    )


def record_authenticator_event(
    db: Session, request: Request, user: User, action: str, *, error: AuthenticatorError | None = None, **metadata
) -> None:
    if error:
        metadata["reason"] = error.code
    append_audit_event(
        db=db,
        request=request,
        actor_user=user,
        event_type=f"auth.totp.{action}",
        outcome="failure" if error else "success",
        status_code=error.status_code if error else None,
        metadata=metadata or None,
    )
    db.commit()


def lock_user(db: Session, user_id: int) -> User | None:
    return db.scalar(
        select(User)
        .where(User.id == user_id, User.is_active.is_(True))
        .with_for_update()
        .execution_options(populate_existing=True)
    )


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _fernet() -> Fernet:
    encoded_key = (get_settings().totp_encryption_key or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{43}=", encoded_key):
        raise AuthenticatorError("authenticator_unavailable", "Aplicativo autenticador indisponível.", 503)
    return Fernet(encoded_key.encode("ascii"))


def enrollment_available() -> bool:
    try:
        _fernet()
    except AuthenticatorError:
        return False
    return True


def new_encrypted_secret() -> str:
    return _fernet().encrypt(pyotp.random_base32().encode("ascii")).decode("ascii")


def decrypt_secret(ciphertext: str) -> str:
    try:
        return _fernet().decrypt(ciphertext.encode("ascii")).decode("ascii")
    except (InvalidToken, UnicodeError) as exc:
        raise AuthenticatorError("authenticator_unavailable", "Aplicativo autenticador indisponível.", 503) from exc


def enrollment_payload(user: User, challenge: AuthenticatorChallenge) -> dict:
    secret = decrypt_secret(challenge.pending_secret_encrypted)
    image = BytesIO()
    uri = pyotp.TOTP(secret, digits=6, interval=30).provisioning_uri(name=user.email, issuer_name="SiSFarm")
    qrcode.make(uri).save(image, format="PNG")
    return {
        "stage": "totp",
        "expires_at": _utc(challenge.expires_at).isoformat(),
        "manual_key": secret,
        "qr_code_data_url": "data:image/png;base64," + base64.b64encode(image.getvalue()).decode("ascii"),
        "digits": 6,
        "period": 30,
        "algorithm": "SHA1",
    }


def email_verification_available(user: User) -> bool:
    return user.is_two_factor_enabled or (
        is_super_admin_email(user.email) and current_asaas_environment().is_production
    )


def authenticator_preference(user: User) -> str:
    if user.totp_secret_encrypted:
        return "email_then_totp" if user.totp_email_first and email_verification_available(user) else "totp"
    return "email" if email_verification_available(user) else "none"


def _credential_stamp(user: User) -> str:
    return hash_persistent_token(
        f"mfa-credentials:{user.id}:{user.email}:{user.hashed_password}:"
        f"{authenticator_session_version(user)}:{email_verification_available(user)}"
    )


def _challenge_token_hash(token: str) -> str:
    return hash_persistent_token(f"mfa-challenge:{token}")


def _email_code_hash(challenge: AuthenticatorChallenge, code: str) -> str:
    return hash_persistent_token(f"mfa-email:{challenge.token_hash}:{code}")


def email_resend_remaining(challenge: AuthenticatorChallenge, now: datetime) -> int:
    return max(0, math.ceil((_utc(challenge.email_next_send_at) - now).total_seconds())) if challenge.email_next_send_at else 0


def _check_email_cooldown(challenge: AuthenticatorChallenge, now: datetime) -> None:
    seconds = email_resend_remaining(challenge, now)
    if seconds:
        raise AuthenticatorError("resend_cooldown", "Aguarde antes de solicitar outro código.", 429, retry_after=seconds)


def generate_challenge_email_code(challenge: AuthenticatorChallenge) -> str:
    now = utc_now()
    _check_email_cooldown(challenge, now)
    code = generate_numeric_code(6)
    challenge.email_code_hash = _email_code_hash(challenge, code)
    challenge.email_next_send_at = now + RESEND_DELAY
    return code


def start_challenge(
    db: Session, user: User, purpose: str, *, stage: str, pending_secret_encrypted: str | None = None
) -> tuple[AuthenticatorChallenge, str, str | None]:
    now = utc_now()
    challenge = db.get(AuthenticatorChallenge, (user.id, purpose))
    if challenge and stage == "email":
        _check_email_cooldown(challenge, now)
    if not challenge:
        challenge = AuthenticatorChallenge(user_id=user.id, purpose=purpose)
        db.add(challenge)
    token = secrets.token_urlsafe(32)
    challenge.token_hash = _challenge_token_hash(token)
    challenge.credential_hash = _credential_stamp(user)
    challenge.stage = stage
    challenge.expires_at = now + timedelta(minutes=get_settings().two_factor_code_minutes)
    challenge.pending_secret_encrypted = pending_secret_encrypted
    challenge.email_code_hash = None
    email_code = generate_challenge_email_code(challenge) if stage == "email" else None
    return challenge, token, email_code


def read_challenge(db: Session, user: User, purpose: str, token: str | None) -> AuthenticatorChallenge:
    if token:
        challenge = db.get(AuthenticatorChallenge, (user.id, purpose), populate_existing=True)
        if (
            challenge
            and _utc(challenge.expires_at) > utc_now()
            and hmac.compare_digest(challenge.token_hash, _challenge_token_hash(token))
            and hmac.compare_digest(challenge.credential_hash, _credential_stamp(user))
        ):
            return challenge
    raise AuthenticatorError("challenge_expired", "A verificação expirou. Inicie novamente.", 409)


def check_attempts(user: User, now: datetime) -> None:
    if not user.totp_retry_at or _utc(user.totp_retry_at) <= now:
        if user.totp_failed_attempts or user.totp_retry_at:
            user.totp_failed_attempts = 0
            user.totp_retry_at = None
    elif (user.totp_failed_attempts or 0) >= attempt_limit():
        seconds = math.ceil((_utc(user.totp_retry_at) - now).total_seconds())
        raise AuthenticatorError("rate_limited", "Muitas tentativas. Aguarde e tente novamente.", 429, retry_after=seconds)


def attempt_limit() -> int:
    return max(1, get_settings().two_factor_max_attempts)


def record_failed_attempt(user: User, now: datetime, *, reason: str = "invalid_code") -> None:
    user.totp_failed_attempts = (user.totp_failed_attempts or 0) + 1
    if user.totp_failed_attempts == 1 or user.totp_failed_attempts >= attempt_limit():
        user.totp_retry_at = now + ATTEMPT_WINDOW
    check_attempts(user, now)
    messages = {
        "invalid_credentials": "A senha atual não confere.",
        "reused_code": "Este código já foi utilizado. Aguarde o próximo código do aplicativo.",
        "invalid_code": "Código inválido, expirado ou já utilizado.",
    }
    raise AuthenticatorError(reason, messages[reason])


def clear_attempts(user: User) -> None:
    user.totp_failed_attempts = 0
    user.totp_retry_at = None


def check_current_password(user: User, password: str) -> None:
    now = utc_now()
    check_attempts(user, now)
    if not verify_password(password, user.hashed_password):
        record_failed_attempt(user, now, reason="invalid_credentials")


def verify_challenge_email(user: User, challenge: AuthenticatorChallenge, code: str) -> None:
    now = utc_now()
    check_attempts(user, now)
    normalized = code.strip()
    if not re.fullmatch(r"[0-9]{6}", normalized) or not challenge.email_code_hash or not hmac.compare_digest(
        challenge.email_code_hash, _email_code_hash(challenge, normalized)
    ):
        record_failed_attempt(user, now)
    challenge.email_code_hash = None
    challenge.stage = "totp"


def _matching_totp_step(ciphertext: str, code: str, now: datetime) -> int | None:
    if not re.fullmatch(r"[0-9]{6}", code):
        return None
    totp = pyotp.TOTP(decrypt_secret(ciphertext), digits=6, interval=30)
    step = totp.timecode(now)
    matched = [candidate for candidate in range(step - 1, step + 2) if hmac.compare_digest(totp.generate_otp(candidate), code)]
    return max(matched, default=None)


def verify_enrollment_code(user: User, challenge: AuthenticatorChallenge, code: str) -> int:
    now = utc_now()
    check_attempts(user, now)
    if not challenge.pending_secret_encrypted:
        raise AuthenticatorError("challenge_expired", "A vinculação expirou. Inicie novamente.", 409)
    step = _matching_totp_step(challenge.pending_secret_encrypted, code.strip(), now)
    if step is None:
        record_failed_attempt(user, now)
    return step


def _recovery_hash(user_id: int, code: str) -> str:
    return hash_persistent_token(f"mfa-recovery:{user_id}:{code}")


def recovery_code_count(db: Session, user_id: int) -> int:
    return db.scalar(
        select(func.count()).select_from(AuthenticatorRecoveryCode).where(AuthenticatorRecoveryCode.user_id == user_id)
    )


def replace_recovery_codes(db: Session, user_id: int) -> list[str]:
    db.execute(delete(AuthenticatorRecoveryCode).where(AuthenticatorRecoveryCode.user_id == user_id))
    codes = []
    while len(codes) < RECOVERY_CODE_COUNT:
        raw = secrets.token_hex(10).upper()
        formatted = "-".join(raw[index:index + 5] for index in range(0, len(raw), 5))
        if formatted not in codes:
            codes.append(formatted)
            db.add(AuthenticatorRecoveryCode(user_id=user_id, code_hash=_recovery_hash(user_id, raw)))
    return codes


def delete_recovery_codes(db: Session, user_id: int) -> None:
    db.execute(delete(AuthenticatorRecoveryCode).where(AuthenticatorRecoveryCode.user_id == user_id))


def verify_active_factor(db: Session, user: User, *, method: str, code: str) -> None:
    if not user.totp_secret_encrypted:
        raise AuthenticatorError("authenticator_disabled", "O aplicativo autenticador está desativado.", 409)
    now = utc_now()
    check_attempts(user, now)
    if method == "recovery":
        normalized = re.sub(r"[\s-]", "", code.upper())
        if re.fullmatch(r"[A-F0-9]{20}", normalized):
            consumed = db.execute(
                delete(AuthenticatorRecoveryCode).where(
                    AuthenticatorRecoveryCode.user_id == user.id,
                    AuthenticatorRecoveryCode.code_hash == _recovery_hash(user.id, normalized),
                )
            )
            if consumed.rowcount == 1:
                clear_attempts(user)
                return
        record_failed_attempt(user, now)

    step = _matching_totp_step(user.totp_secret_encrypted, code.strip(), now)
    if step is None:
        record_failed_attempt(user, now)
    accepted = db.execute(
        update(User)
        .where(
            User.id == user.id,
            User.totp_secret_encrypted == user.totp_secret_encrypted,
            or_(User.totp_last_step.is_(None), User.totp_last_step < step),
        )
        .values(totp_last_step=step)
        .execution_options(synchronize_session=False)
    )
    if accepted.rowcount != 1:
        record_failed_attempt(user, now, reason="reused_code")
    clear_attempts(user)


def invalidate_authenticator_sessions(db: Session, user: User) -> int:
    version = authenticator_session_version(user) + 1
    user.totp_session_version = version
    db.execute(delete(AuthenticatorChallenge).where(AuthenticatorChallenge.user_id == user.id))
    revoke_active_login_codes(db, user.id, commit=False)
    revoke_user_trusted_browsers(db, user.id, commit=False)
    return version
