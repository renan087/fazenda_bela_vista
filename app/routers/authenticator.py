from typing import Literal

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from sqlalchemy.orm import Session

from app.core.csrf import ensure_csrf_token, validate_csrf
from app.core.deps import get_current_user_web
from app.core.session import AUTHENTICATOR_SESSION_VERSION, authenticator_session_matches
from app.db.session import get_db
from app.models import User
from app.services import authenticator as mfa
from app.services.email_service import send_access_code_email

router = APIRouter(prefix="/meu-perfil/seguranca/autenticador", tags=["auth"])
ENROLLMENT_TOKEN = "totp_enrollment_challenge"


def _profile_state(db: Session, request: Request, user: User) -> dict:
    return {
        "enabled": bool(user.totp_secret_encrypted),
        "enrollment_available": mfa.enrollment_available(),
        "email_enabled": mfa.email_verification_available(user),
        "preferred_method": mfa.authenticator_preference(user),
        "recovery_codes_remaining": mfa.recovery_code_count(db, user.id),
        "csrf_token": ensure_csrf_token(request),
    }


def _locked_profile_user(db: Session, request: Request, user: User) -> User:
    locked_user = mfa.lock_user(db, user.id)
    if not locked_user or locked_user.email != request.session.get("user_email") or not authenticator_session_matches(request, locked_user):
        request.session.clear()
        raise HTTPException(status_code=401, detail="Sessão expirada.")
    return locked_user


def _profile_error(db: Session, request: Request, user: User, action: str, error: mfa.AuthenticatorError):
    mfa.record_authenticator_event(db, request, user, action, error=error)
    return mfa.authenticator_error_reply(error)


def _enrollment(db: Session, request: Request, user: User, expected_stage: str):
    challenge = mfa.read_challenge(db, user, mfa.ENROLLMENT_CHALLENGE, request.session.get(ENROLLMENT_TOKEN))
    if challenge.stage != expected_stage:
        raise mfa.AuthenticatorError("wrong_stage", "Conclua a etapa anterior da vinculação.", 409)
    return challenge


@router.get("")
def authenticator_status(request: Request, db: Session = Depends(get_db), user: User = Depends(get_current_user_web)):
    return mfa.authenticator_reply(_profile_state(db, request, user))


@router.post("/vincular/iniciar")
def start_authenticator_enrollment(
    request: Request,
    csrf_token: str = Form(...),
    current_password: str = Form(..., max_length=256),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    validate_csrf(request, csrf_token)
    user = _locked_profile_user(db, request, user)
    action = "enrollment.started"
    try:
        if user.totp_secret_encrypted:
            raise mfa.AuthenticatorError("authenticator_enabled", "Já existe um aplicativo autenticador vinculado.", 409)
        mfa.check_current_password(user, current_password)
        secret = mfa.new_encrypted_secret()
        stage = "email" if mfa.email_verification_available(user) else "totp"
        challenge, token, email_code = mfa.start_challenge(
            db, user, mfa.ENROLLMENT_CHALLENGE, stage=stage, pending_secret_encrypted=secret
        )
        if email_code is not None:
            send_access_code_email(user.email, email_code)
            payload = {"stage": "email", "email": user.email, "expires_at": challenge.expires_at.isoformat()}
        else:
            payload = mfa.enrollment_payload(user, challenge)
    except mfa.AuthenticatorError as error:
        return _profile_error(db, request, user, action, error)
    except (RuntimeError, OSError):
        db.rollback()
        error = mfa.AuthenticatorError("delivery_failed", "Não foi possível iniciar a vinculação. Tente novamente.", 503)
        return _profile_error(db, request, user, action, error)

    mfa.record_authenticator_event(db, request, user, action, stage=stage)
    request.session[ENROLLMENT_TOKEN] = token
    return mfa.authenticator_reply(payload)


@router.post("/vincular/email")
def verify_enrollment_email(
    request: Request,
    csrf_token: str = Form(...),
    code: str = Form(..., max_length=80),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    validate_csrf(request, csrf_token)
    user = _locked_profile_user(db, request, user)
    action = "enrollment.email_verified"
    try:
        challenge = _enrollment(db, request, user, "email")
        mfa.verify_challenge_email(user, challenge, code)
        payload = mfa.enrollment_payload(user, challenge)
    except mfa.AuthenticatorError as error:
        if error.status_code == 503:
            db.rollback()
        return _profile_error(db, request, user, action, error)
    mfa.record_authenticator_event(db, request, user, action)
    return mfa.authenticator_reply(payload)


@router.post("/vincular/confirmar")
def confirm_authenticator_enrollment(
    request: Request,
    csrf_token: str = Form(...),
    code: str = Form(..., max_length=80),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    validate_csrf(request, csrf_token)
    user = _locked_profile_user(db, request, user)
    action = "enabled"
    try:
        if user.totp_secret_encrypted:
            raise mfa.AuthenticatorError("authenticator_enabled", "Já existe um aplicativo autenticador vinculado.", 409)
        challenge = _enrollment(db, request, user, "totp")
        step = mfa.verify_enrollment_code(user, challenge, code)
    except mfa.AuthenticatorError as error:
        return _profile_error(db, request, user, action, error)

    user.totp_secret_encrypted = challenge.pending_secret_encrypted
    user.totp_last_step = step
    user.totp_email_first = False
    mfa.clear_attempts(user)
    codes = mfa.replace_recovery_codes(db, user.id)
    version = mfa.invalidate_authenticator_sessions(db, user)
    mfa.record_authenticator_event(db, request, user, action)
    request.session.pop(ENROLLMENT_TOKEN, None)
    request.session[AUTHENTICATOR_SESSION_VERSION] = version
    return mfa.authenticator_reply({**_profile_state(db, request, user), "recovery_codes": codes})


@router.post("/vincular/cancelar")
def cancel_authenticator_enrollment(
    request: Request,
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    validate_csrf(request, csrf_token)
    user = _locked_profile_user(db, request, user)
    token = request.session.pop(ENROLLMENT_TOKEN, None)
    try:
        challenge = mfa.read_challenge(db, user, mfa.ENROLLMENT_CHALLENGE, token)
    except mfa.AuthenticatorError:
        return mfa.authenticator_reply({"cancelled": True})
    db.delete(challenge)
    mfa.record_authenticator_event(db, request, user, "enrollment.cancelled")
    return mfa.authenticator_reply({"cancelled": True})


@router.post("/gerenciar")
def manage_authenticator(
    request: Request,
    csrf_token: str = Form(...),
    current_password: str = Form(..., max_length=256),
    code: str = Form(..., max_length=80),
    action: Literal["disable", "preference", "regenerate_recovery_codes"] = Form(...),
    method: Literal["totp", "recovery"] = Form("totp"),
    preferred_method: Literal["totp", "email_then_totp"] | None = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_web),
):
    validate_csrf(request, csrf_token)
    user = _locked_profile_user(db, request, user)
    event = {"disable": "disabled", "preference": "preference_changed", "regenerate_recovery_codes": "recovery_regenerated"}[action]
    try:
        if action == "preference" and (
            not preferred_method
            or (preferred_method == "email_then_totp" and not mfa.email_verification_available(user))
        ):
            raise mfa.AuthenticatorError("invalid_preference", "Selecione um método de autenticação disponível.")
        mfa.check_current_password(user, current_password)
        mfa.verify_active_factor(db, user, method=method, code=code)
    except mfa.AuthenticatorError as error:
        return _profile_error(db, request, user, event, error)

    extra = {}
    if action == "disable":
        user.totp_secret_encrypted = None
        user.totp_last_step = None
        user.totp_email_first = False
        mfa.delete_recovery_codes(db, user.id)
    elif action == "preference":
        user.totp_email_first = preferred_method == "email_then_totp"
    else:
        extra["recovery_codes"] = mfa.replace_recovery_codes(db, user.id)
    version = mfa.invalidate_authenticator_sessions(db, user)
    mfa.record_authenticator_event(db, request, user, event, method=method)
    request.session[AUTHENTICATOR_SESSION_VERSION] = version
    return mfa.authenticator_reply({**_profile_state(db, request, user), **extra})
