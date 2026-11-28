import logging
import secrets
import time
from typing import Literal
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.core.csrf import ensure_csrf_token, validate_csrf
from app.core.config import get_settings
from app.core.admin_access import is_super_admin_email
from app.core.session import AUTHENTICATOR_SESSION_VERSION, authenticator_session_version, clear_expired_session, touch_session_activity
from app.core.security import authenticate_user, create_access_token, get_password_hash, verify_password
from app.core.timezone import utc_now
from app.db.session import get_db
from app.models import User
from app.schemas.auth import Token
from app.services import authenticator as mfa, login_throttle
from app.services.asaas_environment import current_asaas_environment
from app.services.audit_log_service import append_audit_event
from app.services.email_service import send_access_code_email, send_password_reset_email
from app.services.password_reset import get_valid_password_reset_token, issue_password_reset_token, revoke_user_password_reset_tokens
from app.services.trusted_browser import (
    issue_trusted_browser_token,
    revoke_user_trusted_browsers,
    validate_trusted_browser_token,
)
from app.services.user_preferences import get_user_preferences
from app.services.two_factor import issue_login_verification_code, revoke_active_login_codes, verify_login_code

templates = Jinja2Templates(directory="app/templates")
templates.env.globals["google_tag_id"] = lambda: (getattr(get_settings(), "google_tag_id", None) or "").strip()
router = APIRouter()
api_router = APIRouter(prefix="/api/v1/auth", tags=["auth"])
logger = logging.getLogger(__name__)

PENDING_2FA_USER_ID = "pending_2fa_user_id"
PENDING_2FA_EMAIL = "pending_2fa_email"
PENDING_2FA_LAST_SENT_AT = "pending_2fa_last_sent_at"
PENDING_AUTHENTICATOR_TOKEN = "totp_login_challenge"
LOGIN_2FA_RESEND_COOLDOWN_SECONDS = 60
GOOGLE_OAUTH_STATE = "google_oauth_state"
GOOGLE_OAUTH_NONCE = "google_oauth_nonce"
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_TOKEN_INFO_URL = "https://oauth2.googleapis.com/tokeninfo"


def _mark_login_code_sent(request: Request) -> None:
    request.session[PENDING_2FA_LAST_SENT_AT] = int(utc_now().timestamp())


def _login_code_resend_cooldown_remaining(request: Request) -> int:
    last_sent_at = request.session.get(PENDING_2FA_LAST_SENT_AT)
    if not last_sent_at:
        return 0
    try:
        elapsed_seconds = int(utc_now().timestamp()) - int(last_sent_at)
    except (TypeError, ValueError):
        request.session.pop(PENDING_2FA_LAST_SENT_AT, None)
        return 0
    return max(0, LOGIN_2FA_RESEND_COOLDOWN_SECONDS - elapsed_seconds)


def _trusted_browser_cookie_name() -> str:
    return get_settings().trusted_browser_cookie_name


def _set_trusted_browser_cookie(response: RedirectResponse, token: str) -> None:
    settings = get_settings()
    response.set_cookie(
        key=settings.trusted_browser_cookie_name,
        value=token,
        max_age=settings.trusted_browser_seconds,
        httponly=True,
        samesite="lax",
        secure=settings.is_production,
        path="/",
    )


def _clear_trusted_browser_cookie(response: RedirectResponse) -> None:
    response.delete_cookie(key=_trusted_browser_cookie_name(), path="/")


def _trust_browser_requested(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "on", "true", "yes"}


def _pending_2fa_user(request: Request, db: Session) -> User | None:
    if clear_expired_session(request):
        return None

    pending_user_id = request.session.get(PENDING_2FA_USER_ID)
    if not pending_user_id:
        return None
    user = db.query(User).filter(User.id == pending_user_id, User.is_active.is_(True)).first()
    if user:
        touch_session_activity(request)
    return user


def _clear_pending_2fa(request: Request) -> None:
    for key in (PENDING_2FA_USER_ID, PENDING_2FA_EMAIL, PENDING_2FA_LAST_SENT_AT, PENDING_AUTHENTICATOR_TOKEN):
        request.session.pop(key, None)


def _complete_web_login(
    request: Request,
    db: Session,
    user: User,
    trusted_browser_token: str | None = None,
    clear_trusted_browser_cookie: bool = False,
    verified_second_factor: bool = False,
    verified_authenticator_version: int | None = None,
):
    session_version = authenticator_session_version(user)
    if (verified_authenticator_version is not None or mfa.authenticator_login_required(user)) and (
        verified_authenticator_version != session_version
    ):
        _clear_pending_2fa(request)
        return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)
    try:
        user.last_login_at = utc_now()
        db.add(user)
        db.commit()
        db.refresh(user)
    except Exception:
        db.rollback()
        logger.exception(
            "Falha ao registrar ultimo acesso do usuario",
            extra={"user_email": user.email},
        )

    prefs = get_user_preferences(user)
    if prefs.get("context_mode") == "fixed" and prefs.get("default_farm_id"):
        try:
            from app.models.farm import Farm
            fixed_farm = db.query(Farm).filter(Farm.id == int(prefs["default_farm_id"])).first()
            if fixed_farm and (user.is_admin or not user.organization_id or fixed_farm.organization_id == user.organization_id):
                user.active_farm_id = fixed_farm.id
                db.add(user)
                db.commit()
        except Exception:
            db.rollback()

    request.session["user_email"] = user.email
    request.session[AUTHENTICATOR_SESSION_VERSION] = session_version
    if verified_second_factor:
        request.session["commercial_mfa_at"] = utc_now().timestamp()
    else:
        request.session.pop("commercial_mfa_at", None)
    _clear_pending_2fa(request)
    touch_session_activity(request)
    append_audit_event(request=request, actor_user=user, event_type="auth.login.success", outcome="success")
    target_url = prefs.get("start_page") or "/dashboard"
    response = RedirectResponse(url=target_url, status_code=status.HTTP_303_SEE_OTHER)
    if clear_trusted_browser_cookie:
        _clear_trusted_browser_cookie(response)
    if trusted_browser_token:
        _set_trusted_browser_cookie(response, trusted_browser_token)
    return response


def _google_login_enabled() -> bool:
    settings = get_settings()
    return bool(settings.google_client_id and settings.google_client_secret and settings.google_redirect_uri)


def _clear_google_oauth_flow(request: Request) -> tuple[str | None, str | None]:
    return (
        request.session.pop(GOOGLE_OAUTH_STATE, None),
        request.session.pop(GOOGLE_OAUTH_NONCE, None),
    )


def _build_google_authorization_url(request: Request) -> str:
    settings = get_settings()
    state = secrets.token_urlsafe(32)
    nonce = secrets.token_urlsafe(32)
    request.session[GOOGLE_OAUTH_STATE] = state
    request.session[GOOGLE_OAUTH_NONCE] = nonce
    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": settings.google_redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "nonce": nonce,
        "prompt": "select_account",
    }
    return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"


def _exchange_google_code(code: str) -> dict:
    settings = get_settings()
    try:
        with httpx.Client(timeout=15.0) as client:
            response = client.post(
                GOOGLE_TOKEN_URL,
                data={
                    "code": code,
                    "client_id": settings.google_client_id,
                    "client_secret": settings.google_client_secret,
                    "redirect_uri": settings.google_redirect_uri,
                    "grant_type": "authorization_code",
                },
            )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise RuntimeError("Nao foi possivel concluir o login com Google agora. Tente novamente.") from exc
    payload = response.json()
    if not payload.get("id_token"):
        raise RuntimeError("O Google nao retornou um token de identificacao valido.")
    return payload


def _validate_google_id_token(id_token: str, expected_nonce: str | None = None) -> dict:
    settings = get_settings()
    try:
        with httpx.Client(timeout=15.0) as client:
            response = client.get(GOOGLE_TOKEN_INFO_URL, params={"id_token": id_token})
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise RuntimeError("Nao foi possivel validar o login com Google. Tente novamente.") from exc

    claims = response.json()
    if claims.get("aud") != settings.google_client_id:
        raise RuntimeError("A autenticacao Google retornou um cliente invalido.")
    if claims.get("iss") not in {"accounts.google.com", "https://accounts.google.com"}:
        raise RuntimeError("A autenticacao Google retornou um emissor invalido.")
    if str(claims.get("email_verified", "")).lower() != "true":
        raise RuntimeError("A conta Google precisa ter email verificado para acessar o sistema.")
    if expected_nonce and claims.get("nonce") != expected_nonce:
        raise RuntimeError("Falha de validacao do login com Google. Tente novamente.")
    email = (claims.get("email") or "").strip().lower()
    if not email:
        raise RuntimeError("O Google nao retornou um email valido para esta conta.")
    return claims


def _reactivate_super_admin_if_needed(db: Session, user: User | None) -> None:
    if not user or not is_super_admin_email(user.email) or user.is_active:
        return
    try:
        user.is_active = True
        db.add(user)
        db.commit()
        db.refresh(user)
    except Exception:
        db.rollback()
        logger.exception(
            "Falha ao reativar automaticamente o super admin no login",
            extra={"user_email": user.email},
        )


def _start_authenticator_login(request: Request, db: Session, user: User, *, clear_trusted_cookie: bool):
    credential = (user.email, user.hashed_password, authenticator_session_version(user))
    request.session.pop("user_email", None)
    request.session.pop("commercial_mfa_at", None)
    request.session.pop(AUTHENTICATOR_SESSION_VERSION, None)
    _clear_pending_2fa(request)
    user = mfa.lock_user(db, user.id)
    if (
        not user or not mfa.authenticator_login_required(user)
        or (user.email, user.hashed_password, authenticator_session_version(user)) != credential
    ):
        return _render_login(request, "As credenciais foram alteradas. Entre novamente.")

    try:
        mfa.check_attempts(user, utc_now())
        stage = "email" if mfa.authenticator_preference(user) == "email_then_totp" else "totp"
        _, token, email_code = mfa.start_challenge(db, user, mfa.LOGIN_CHALLENGE, stage=stage)
        if email_code is not None:
            send_access_code_email(user.email, email_code)
    except mfa.AuthenticatorError as error:
        mfa.record_authenticator_event(db, request, user, "login.started", error=error)
        return _render_login(
            request,
            str(error),
            error_status=error.status_code,
            retry_after=error.retry_after,
        )
    except (RuntimeError, OSError):
        db.rollback()
        error = mfa.AuthenticatorError("delivery_failed", "Não foi possível enviar o código de acesso.", 503)
        mfa.record_authenticator_event(db, request, user, "login.started", error=error)
        return _render_login(request, str(error))

    mfa.record_authenticator_event(db, request, user, "login.started", stage=stage)
    request.session[PENDING_2FA_USER_ID] = user.id
    request.session[PENDING_AUTHENTICATOR_TOKEN] = token
    touch_session_activity(request)
    response = RedirectResponse(url="/login/verificacao", status_code=status.HTTP_303_SEE_OTHER, headers=mfa.NO_STORE)
    if clear_trusted_cookie:
        _clear_trusted_browser_cookie(response)
    return response


def _start_web_login_challenge(request: Request, db: Session, user: User):
    trusted_browser_cookie = request.cookies.get(_trusted_browser_cookie_name())
    if mfa.authenticator_login_required(user):
        return _start_authenticator_login(request, db, user, clear_trusted_cookie=bool(trusted_browser_cookie))
    _clear_pending_2fa(request)
    clear_trusted_cookie = False
    commercial_admin = is_super_admin_email(user.email) and current_asaas_environment().is_production
    if trusted_browser_cookie and not commercial_admin:
        if validate_trusted_browser_token(db, user, request, trusted_browser_cookie):
            revoke_active_login_codes(db, user.id)
            return _complete_web_login(request, db, user)
        clear_trusted_cookie = True

    if not user.is_two_factor_enabled and not commercial_admin:
        revoke_active_login_codes(db, user.id)
        response = _complete_web_login(request, db, user)
        if clear_trusted_cookie:
            _clear_trusted_browser_cookie(response)
        return response

    try:
        code = issue_login_verification_code(db, user)
        send_access_code_email(user.email, code)
    except RuntimeError as exc:
        revoke_active_login_codes(db, user.id)
        logger.exception(
            "Falha controlada no envio do codigo 2FA",
            extra={"user_email": user.email},
        )
        return _render_login(request, str(exc))
    except Exception:
        revoke_active_login_codes(db, user.id)
        logger.exception(
            "Falha inesperada no envio do codigo 2FA",
            extra={"user_email": user.email},
        )
        return _render_login(request, "Nao foi possivel enviar o codigo de acesso. Tente novamente.")

    request.session.pop("user_email", None)
    request.session[PENDING_2FA_USER_ID] = user.id
    request.session[PENDING_2FA_EMAIL] = user.email
    _mark_login_code_sent(request)
    touch_session_activity(request)
    response = RedirectResponse(url="/login/verificacao", status_code=status.HTTP_303_SEE_OTHER)
    if clear_trusted_cookie:
        _clear_trusted_browser_cookie(response)
    return response


def _render_login(
    request: Request, error: str | None = None, *, error_status: int = 400, retry_after: int | None = None,
):
    notice_key = request.query_params.get("notice")
    info = None
    if notice_key == "password-reset-requested":
        info = "Se existir uma conta para este email, enviaremos as instrucoes."
    elif notice_key == "password-reset-success":
        info = "Senha redefinida com sucesso. Entre com sua nova senha."
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": error,
            "info": info,
            "csrf_token": ensure_csrf_token(request),
            "google_login_enabled": _google_login_enabled(),
            "page": "login",
        },
        status_code=error_status if error else status.HTTP_200_OK,
        headers={"Retry-After": str(retry_after)} if retry_after is not None else None,
    )


def _render_login_verification(
    request: Request,
    email: str,
    error: str | None = None,
    info: str | None = None,
    trust_browser_checked: bool = False,
    authenticator_challenge: dict | None = None,
):
    return templates.TemplateResponse(
        "login_verify.html",
        {
            "request": request,
            "error": error,
            "info": info,
            "email": email,
            "trust_browser_checked": trust_browser_checked,
            "trusted_browser_days": get_settings().trusted_browser_days,
            "resend_cooldown_seconds": _login_code_resend_cooldown_remaining(request),
            "csrf_token": ensure_csrf_token(request),
            "page": "login",
            "authenticator_challenge": authenticator_challenge,
        },
        status_code=status.HTTP_400_BAD_REQUEST if error else status.HTTP_200_OK,
        headers=mfa.NO_STORE if authenticator_challenge else None,
    )


def _render_forgot_password(request: Request, error: str | None = None, info: str | None = None):
    return templates.TemplateResponse(
        "forgot_password.html",
        {
            "request": request,
            "error": error,
            "info": info,
            "csrf_token": ensure_csrf_token(request),
            "page": "login",
        },
        status_code=status.HTTP_400_BAD_REQUEST if error else status.HTTP_200_OK,
    )


def _render_password_reset(
    request: Request,
    token: str,
    error: str | None = None,
    info: str | None = None,
):
    return templates.TemplateResponse(
        "reset_password.html",
        {
            "request": request,
            "token": token,
            "error": error,
            "info": info,
            "csrf_token": ensure_csrf_token(request),
            "page": "login",
        },
        status_code=status.HTTP_400_BAD_REQUEST if error else status.HTTP_200_OK,
    )


@router.get("/login")
def login_page(request: Request, db: Session = Depends(get_db)):
    clear_expired_session(request)
    if request.session.get("user_email"):
        touch_session_activity(request)
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)
    return _render_login(request)


@router.get("/auth/google")
def login_google_start(request: Request):
    clear_expired_session(request)
    if request.session.get("user_email"):
        touch_session_activity(request)
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)
    if not _google_login_enabled():
        return _render_login(request, "Login com Google nao esta configurado no ambiente.")
    return RedirectResponse(url=_build_google_authorization_url(request), status_code=status.HTTP_302_FOUND)


@router.get("/login/google")
def login_google_start_alias(request: Request):
    return login_google_start(request)


@router.get("/auth/google/callback")
def login_google_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    db: Session = Depends(get_db),
):
    clear_expired_session(request)
    if request.session.get("user_email"):
        touch_session_activity(request)
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)
    if not _google_login_enabled():
        return _render_login(request, "Login com Google nao esta configurado no ambiente.")

    expected_state, expected_nonce = _clear_google_oauth_flow(request)
    if error:
        return _render_login(request, "Nao foi possivel autenticar com Google. Tente novamente.")
    if not code or not state or not expected_state or state != expected_state:
        return _render_login(request, "Falha de validacao no retorno do Google. Tente entrar novamente.")

    try:
        token_payload = _exchange_google_code(code)
        claims = _validate_google_id_token(token_payload["id_token"], expected_nonce=expected_nonce)
    except RuntimeError as exc:
        return _render_login(request, str(exc))

    email = (claims.get("email") or "").strip().lower()
    user = db.query(User).filter(User.email == email).first()
    _reactivate_super_admin_if_needed(db, user)
    if not user:
        # Sem conta: segue para o autocadastro com o e-mail ja confirmado pelo Google.
        previous = request.session.get(SIGNUP_SESSION) or {}
        request.session[SIGNUP_SESSION] = {
            "email": email, "google": True, "plan_id": previous.get("plan_id"),
            "first_name": (claims.get("given_name") or "").strip(), "last_name": (claims.get("family_name") or "").strip(),
        }
        return RedirectResponse(url="/cadastro/dados", status_code=status.HTTP_303_SEE_OTHER)
    if not user.is_active:
        return _render_login(request, "Seu usuario esta inativo no sistema. Entre em contato com o administrador.")
    if not get_user_preferences(user).get("google_login_enabled", True):
        return _render_login(
            request,
            "O acesso com Google está desativado nas configurações de segurança desta conta. Entre com e-mail e senha.",
        )
    return _start_web_login_challenge(request, db, user)


@router.get("/login/google/callback")
def login_google_callback_alias(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    db: Session = Depends(get_db),
):
    return login_google_callback(request=request, code=code, state=state, error=error, db=db)


@router.get("/login/recuperar-senha")
def forgot_password_page(request: Request):
    clear_expired_session(request)
    return _render_forgot_password(request)


@router.post("/login/recuperar-senha")
def forgot_password_action(
    request: Request,
    email: str = Form(...),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    validate_csrf(request, csrf_token)
    normalized_email = (email or "").strip().lower()
    generic_info = "Se existir uma conta para este email, enviaremos as instrucoes."

    if not normalized_email:
        return _render_forgot_password(request, "Informe um email valido.")

    user = db.query(User).filter(User.email == normalized_email, User.is_active.is_(True)).first()
    if not user:
        return _render_forgot_password(request, info=generic_info)

    try:
        reset_token = issue_password_reset_token(db, user)
        reset_link = str(request.url_for("password_reset_page")) + f"?token={reset_token}"
        send_password_reset_email(user.email, reset_link, get_settings().password_reset_token_minutes)
    except RuntimeError:
        logger.exception(
            "Falha controlada ao enviar email de redefinicao de senha",
            extra={"user_email": user.email},
        )
        return _render_forgot_password(request, "Nao foi possivel processar sua solicitacao agora. Tente novamente.")
    except Exception:
        logger.exception(
            "Falha inesperada ao iniciar redefinicao de senha",
            extra={"user_email": user.email},
        )
        return _render_forgot_password(request, "Nao foi possivel processar sua solicitacao agora. Tente novamente.")

    return _render_forgot_password(request, info=generic_info)


@router.get("/login/redefinir-senha")
def password_reset_page(
    request: Request,
    token: str | None = None,
    db: Session = Depends(get_db),
):
    clear_expired_session(request)
    if not get_valid_password_reset_token(db, token):
        return templates.TemplateResponse(
            "reset_password.html",
            {
                "request": request,
                "token": "",
                "error": "Este link e invalido ou expirou. Solicite uma nova redefinicao.",
                "info": None,
                "csrf_token": ensure_csrf_token(request),
                "page": "login",
            },
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    return _render_password_reset(request, token or "")


@router.post("/login/redefinir-senha")
def password_reset_action(
    request: Request,
    token: str = Form(...),
    new_password: str = Form(...),
    confirm_password: str = Form(...),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    validate_csrf(request, csrf_token)
    normalized_token = (token or "").strip()
    normalized_new_password = (new_password or "").strip()
    normalized_confirm_password = (confirm_password or "").strip()

    if not normalized_token:
        return _render_password_reset(request, "", error="Este link e invalido ou expirou. Solicite uma nova redefinicao.")
    if not normalized_new_password:
        return _render_password_reset(request, normalized_token, error="Informe a nova senha.")
    if normalized_new_password != normalized_confirm_password:
        return _render_password_reset(request, normalized_token, error="A confirmacao da nova senha nao confere.")

    record = get_valid_password_reset_token(db, normalized_token)
    if not record:
        return _render_password_reset(request, "", error="Este link e invalido ou expirou. Solicite uma nova redefinicao.")

    user = mfa.lock_user(db, record.user_id)
    record = get_valid_password_reset_token(db, normalized_token)
    if not user or not record:
        return _render_password_reset(request, "", error="Este link e invalido ou expirou. Solicite uma nova redefinicao.")

    if verify_password(normalized_new_password, user.hashed_password):
        return _render_password_reset(request, normalized_token, error="A nova senha precisa ser diferente da senha atual.")

    user.hashed_password = get_password_hash(normalized_new_password)
    record.used_at = utc_now()
    db.add(user)
    db.add(record)
    if user.totp_secret_encrypted:
        mfa.invalidate_authenticator_sessions(db, user)
        mfa.record_authenticator_event(db, request, user, "password_reset")
    else:
        db.commit()
        revoke_user_trusted_browsers(db, user.id)
        revoke_active_login_codes(db, user.id)
    revoke_user_password_reset_tokens(db, user.id)

    request.session.clear()
    return RedirectResponse(url="/login?notice=password-reset-success", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/login")
def login_web(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    validate_csrf(request, csrf_token)
    normalized_email = email.strip().lower()
    ip = _client_ip(request)
    wait = login_throttle.blocked_for(normalized_email, ip)
    if wait:
        return _render_login(request, f"Muitas tentativas de acesso. Aguarde {max(1, wait // 60)} minuto(s) e tente novamente, ou use \"Esqueceu sua senha?\".")
    user = db.query(User).filter(User.email == normalized_email).first()
    _reactivate_super_admin_if_needed(db, user)
    if not authenticate_user(user, password):
        login_throttle.register_failure(normalized_email, ip)
        append_audit_event(
            request=request,
            actor_user=user if user else None,
            actor_email=normalized_email,
            actor_user_id=user.id if user else None,
            event_type="auth.login.failure",
            outcome="failure",
            metadata={"reason": "invalid_credentials"},
        )
        return _render_login(request, "Credenciais invalidas.")
    login_throttle.register_success(normalized_email)
    return _start_web_login_challenge(request, db, user)


def _wants_verification_json(request: Request) -> bool:
    return "application/json" in request.headers.get("accept", "")


def _expired_verification(request: Request):
    _clear_pending_2fa(request)
    if _wants_verification_json(request):
        return mfa.authenticator_error_reply(
            mfa.AuthenticatorError("challenge_expired", "A verificação expirou. Entre novamente.", 409)
        )
    return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER, headers=mfa.NO_STORE)


def _authenticator_login_state(request: Request, user: User, challenge) -> dict:
    email_step = challenge.stage == "email"
    return {
        "stage": challenge.stage,
        "authenticator_required": True,
        "methods": ["email"] if email_step else ["totp", "recovery"],
        "email": user.email if email_step else None,
        "expires_at": challenge.expires_at.isoformat(),
        "resend_available_in": mfa.email_resend_remaining(challenge, utc_now()) if email_step else None,
        "csrf_token": ensure_csrf_token(request),
    }


def _read_authenticator_login(db: Session, request: Request, user: User):
    if not mfa.authenticator_login_required(user):
        raise mfa.AuthenticatorError("challenge_expired", "A verificação expirou. Entre novamente.", 409)
    return mfa.read_challenge(db, user, mfa.LOGIN_CHALLENGE, request.session.get(PENDING_AUTHENTICATOR_TOKEN))


def _render_authenticator_login(request: Request, user: User, challenge, *, info: str | None = None):
    details = _authenticator_login_state(request, user, challenge)
    if _wants_verification_json(request):
        return mfa.authenticator_reply(details)
    return _render_login_verification(request, user.email, info=info, authenticator_challenge=details)


def _authenticator_login_error(
    db: Session, request: Request, user: User, challenge, action: str, error: mfa.AuthenticatorError,
):
    details = _authenticator_login_state(request, user, challenge) if challenge else None
    mfa.record_authenticator_event(db, request, user, action, error=error)
    if error.code == "challenge_expired":
        return _expired_verification(request)
    if _wants_verification_json(request):
        return mfa.authenticator_error_reply(error)
    return _render_login_verification(request, user.email, str(error), authenticator_challenge=details)


def _verify_authenticator_login(db: Session, request: Request, user: User, code: str, method: str | None):
    locked_user = mfa.lock_user(db, user.id)
    if not locked_user:
        return _expired_verification(request)
    user = locked_user
    challenge = None
    action = "login.verified"
    try:
        challenge = _read_authenticator_login(db, request, user)
        if challenge.stage == "email" and method in (None, "email"):
            mfa.verify_challenge_email(user, challenge, code)
            mfa.record_authenticator_event(db, request, user, "login.email_verified")
            return _render_authenticator_login(request, user, challenge)
        if challenge.stage != "totp" or method == "email":
            raise mfa.AuthenticatorError("wrong_stage", "Use um método disponível nesta etapa.", 409)
        method = method or "totp"
        mfa.verify_active_factor(db, user, method=method, code=code)
    except mfa.AuthenticatorError as error:
        return _authenticator_login_error(db, request, user, challenge, action, error)

    version = authenticator_session_version(user)
    db.delete(challenge)
    mfa.record_authenticator_event(db, request, user, action, method=method)
    response = _complete_web_login(
        request, db, user, verified_second_factor=True, verified_authenticator_version=version
    )
    if _wants_verification_json(request):
        if not request.session.get("user_email"):
            return _expired_verification(request)
        return mfa.authenticator_reply({"stage": "complete", "redirect_url": response.headers["location"]})
    return response


def _legacy_login_state(request: Request, user: User) -> dict:
    return {
        "stage": "email", "authenticator_required": False, "methods": ["email"], "email": user.email, "expires_at": None,
        "resend_available_in": _login_code_resend_cooldown_remaining(request),
        "csrf_token": ensure_csrf_token(request),
    }


@router.get("/login/verificacao/estado")
def login_verification_state(request: Request, db: Session = Depends(get_db)):
    user = _pending_2fa_user(request, db)
    if user and mfa.authenticator_login_required(user):
        try:
            challenge = _read_authenticator_login(db, request, user)
        except mfa.AuthenticatorError as error:
            _clear_pending_2fa(request)
            return mfa.authenticator_error_reply(error)
        return mfa.authenticator_reply(_authenticator_login_state(request, user, challenge))
    if not user or request.session.get(PENDING_AUTHENTICATOR_TOKEN):
        _clear_pending_2fa(request)
        return mfa.authenticator_error_reply(
            mfa.AuthenticatorError("challenge_expired", "A verificação expirou. Entre novamente.", 409)
        )
    return mfa.authenticator_reply(_legacy_login_state(request, user))


@router.get("/login/verificacao")
def login_verification_page(
    request: Request,
    db: Session = Depends(get_db),
):
    clear_expired_session(request)
    if request.session.get("user_email"):
        touch_session_activity(request)
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)
    user = _pending_2fa_user(request, db)
    if not user:
        return _expired_verification(request)
    if mfa.authenticator_login_required(user) or request.session.get(PENDING_AUTHENTICATOR_TOKEN):
        try:
            challenge = _read_authenticator_login(db, request, user)
        except mfa.AuthenticatorError:
            return _expired_verification(request)
        return _render_authenticator_login(request, user, challenge)
    return _render_login_verification(request, request.session.get(PENDING_2FA_EMAIL, user.email))


@router.post("/login/verificacao")
def login_verification_web(
    request: Request,
    code: str = Form(..., max_length=80),
    trust_browser: str | None = Form(None),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
    method: Literal["email", "totp", "recovery"] | None = Form(None),
):
    validate_csrf(request, csrf_token)
    user = _pending_2fa_user(request, db)
    if not user:
        return _expired_verification(request)
    if mfa.authenticator_login_required(user) or request.session.get(PENDING_AUTHENTICATOR_TOKEN):
        return _verify_authenticator_login(db, request, user, code, method)
    if method not in (None, "email"):
        return mfa.authenticator_error_reply(mfa.AuthenticatorError("wrong_stage", "Use o código por e-mail.", 409))
    trust_browser_checked = _trust_browser_requested(trust_browser)
    normalized_code = "".join(char for char in code if char.isdigit())[:6]
    if len(normalized_code) != 6:
        if _wants_verification_json(request):
            return mfa.authenticator_error_reply(mfa.AuthenticatorError("invalid_code", "Informe um código de seis dígitos."))
        return _render_login_verification(
            request,
            request.session.get(PENDING_2FA_EMAIL, user.email),
            "Informe um codigo numerico de 6 digitos.",
            trust_browser_checked=trust_browser_checked,
        )
    valid, message = verify_login_code(db, user.id, normalized_code)
    if not valid:
        append_audit_event(
            request=request,
            actor_user=user,
            event_type="auth.2fa.failure",
            outcome="failure",
            metadata={"detail": (message or "")[:120]},
        )
        if _wants_verification_json(request):
            return mfa.authenticator_error_reply(mfa.AuthenticatorError("invalid_code", message))
        return _render_login_verification(
            request,
            request.session.get(PENDING_2FA_EMAIL, user.email),
            message,
            trust_browser_checked=trust_browser_checked,
        )

    trusted_browser_token = None
    if trust_browser_checked:
        try:
            trusted_browser_token = issue_trusted_browser_token(db, user, request)
        except Exception:
            logger.exception(
                "Falha ao registrar navegador confiavel",
                extra={"user_email": user.email},
            )

    response = _complete_web_login(request, db, user, trusted_browser_token=trusted_browser_token, verified_second_factor=True)
    if _wants_verification_json(request):
        if not request.session.get("user_email"):
            return _expired_verification(request)
        result = mfa.authenticator_reply({"stage": "complete", "redirect_url": response.headers["location"]})
        for cookie in response.headers.getlist("set-cookie"):
            result.headers.append("set-cookie", cookie)
        return result
    return response


def _resend_authenticator_login(db: Session, request: Request, user: User):
    locked_user = mfa.lock_user(db, user.id)
    if not locked_user:
        return _expired_verification(request)
    user = locked_user
    challenge = None
    action = "login.email_resent"
    try:
        challenge = _read_authenticator_login(db, request, user)
        if challenge.stage != "email":
            raise mfa.AuthenticatorError("wrong_stage", "O e-mail está indisponível nesta etapa.", 409)
        mfa.check_attempts(user, utc_now())
        code = mfa.generate_challenge_email_code(challenge)
        send_access_code_email(user.email, code)
    except mfa.AuthenticatorError as error:
        return _authenticator_login_error(db, request, user, challenge, action, error)
    except (RuntimeError, OSError):
        db.rollback()
        error = mfa.AuthenticatorError("delivery_failed", "Não foi possível reenviar o código de acesso.", 503)
        return _authenticator_login_error(db, request, user, challenge, action, error)
    mfa.record_authenticator_event(db, request, user, action)
    return _render_authenticator_login(request, user, challenge, info="Enviamos um novo código por e-mail.")


@router.post("/login/verificacao/reenviar")
def resend_login_verification_code(
    request: Request,
    trust_browser: str | None = Form(None),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    validate_csrf(request, csrf_token)
    user = _pending_2fa_user(request, db)
    if not user:
        return _expired_verification(request)
    if mfa.authenticator_login_required(user) or request.session.get(PENDING_AUTHENTICATOR_TOKEN):
        return _resend_authenticator_login(db, request, user)
    cooldown_seconds = _login_code_resend_cooldown_remaining(request)
    if cooldown_seconds > 0:
        if _wants_verification_json(request):
            return mfa.authenticator_error_reply(mfa.AuthenticatorError(
                "resend_cooldown", "Aguarde antes de solicitar outro código.", 429, retry_after=cooldown_seconds,
            ))
        return _render_login_verification(
            request,
            user.email,
            info=f"Aguarde {cooldown_seconds}s para solicitar um novo codigo.",
            trust_browser_checked=_trust_browser_requested(trust_browser),
        )
    try:
        code = issue_login_verification_code(db, user)
        send_access_code_email(user.email, code)
    except RuntimeError as exc:
        logger.exception(
            "Falha controlada ao reenviar codigo 2FA",
            extra={"user_email": user.email},
        )
        if _wants_verification_json(request):
            return mfa.authenticator_error_reply(mfa.AuthenticatorError("delivery_failed", str(exc), 503))
        return _render_login_verification(
            request,
            user.email,
            str(exc),
            trust_browser_checked=_trust_browser_requested(trust_browser),
        )
    except Exception:
        logger.exception(
            "Falha inesperada ao reenviar codigo 2FA",
            extra={"user_email": user.email},
        )
        if _wants_verification_json(request):
            return mfa.authenticator_error_reply(
                mfa.AuthenticatorError("delivery_failed", "Não foi possível reenviar o código de acesso.", 503)
            )
        return _render_login_verification(
            request,
            user.email,
            "Nao foi possivel reenviar o codigo. Tente novamente.",
            trust_browser_checked=_trust_browser_requested(trust_browser),
        )
    _mark_login_code_sent(request)
    if _wants_verification_json(request):
        return mfa.authenticator_reply(_legacy_login_state(request, user))
    return _render_login_verification(
        request,
        user.email,
        info="Enviamos um novo codigo para o seu email.",
        trust_browser_checked=_trust_browser_requested(trust_browser),
    )


@router.get("/logout")
def logout(request: Request, db: Session = Depends(get_db)):
    email = request.session.get("user_email")
    actor = db.query(User).filter(User.email == email).first() if email else None
    if actor:
        append_audit_event(request=request, actor_user=actor, event_type="auth.logout", outcome="success")
    request.session.clear()
    return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)


@api_router.post("/token", response_model=Token)
def login_api(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db),
):
    api_email, ip = (form_data.username or "").strip().lower(), _client_ip(request)
    wait = login_throttle.blocked_for(api_email, ip)
    if wait:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Muitas tentativas. Tente novamente mais tarde.", headers={"Retry-After": str(wait)})
    user = db.query(User).filter(User.email == form_data.username).first()
    if not authenticate_user(user, form_data.password):
        login_throttle.register_failure(api_email, ip)
        append_audit_event(
            request=request,
            actor_user=user if user else None,
            actor_email=(form_data.username or "").strip().lower() or None,
            actor_user_id=user.id if user else None,
            event_type="auth.api.token.failure",
            outcome="failure",
            metadata={"reason": "invalid_credentials"},
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Credenciais invalidas")

    if user.is_two_factor_enabled or mfa.authenticator_login_required(user) or is_super_admin_email(user.email):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Use o acesso web com verificação em duas etapas.")
    login_throttle.register_success(api_email)
    append_audit_event(request=request, actor_user=user, event_type="auth.api.token.success", outcome="success")
    return Token(access_token=create_access_token(user.email, security_version=authenticator_session_version(user)))


# ---------------------------------------------------------------------------
# Autocadastro: e-mail (ou Google) -> dados -> codigo por e-mail -> conta criada
# ---------------------------------------------------------------------------
SIGNUP_SESSION = "signup"


def _signup_state(request: Request) -> dict:
    return dict(request.session.get(SIGNUP_SESSION) or {})


def _save_signup(request: Request, state: dict) -> None:
    request.session[SIGNUP_SESSION] = state


def _render_signup(request: Request, step: str, error: str | None = None, info: str | None = None, **extra):
    from app.services.self_signup import TRIAL_DAYS, MIN_PASSWORD

    state = _signup_state(request)
    context = {
        "request": request, "step": step, "error": error, "info": info, "state": state,
        "csrf_token": ensure_csrf_token(request), "page": "login", "trial_days": TRIAL_DAYS,
        "min_password": MIN_PASSWORD, "google_login_enabled": _google_login_enabled(), **extra,
    }
    return templates.TemplateResponse("signup.html", context, status_code=status.HTTP_400_BAD_REQUEST if error else status.HTTP_200_OK)


def _client_ip(request: Request) -> str | None:
    forwarded = request.headers.get("x-forwarded-for", "")
    return forwarded.split(",")[0].strip() or (request.client.host if request.client else None)


def _finish_signup(request: Request, db: Session, state: dict, password_hash: str | None):
    from app.services import email_service
    from app.services.self_signup import SignupData, SignupError, create_self_signup, TRIAL_DAYS
    from app.core.timezone import today_in_app_timezone

    data = SignupData(**state["data"])
    try:
        user = create_self_signup(db, email=state["email"], data=data, password_hash=password_hash,
                                  plan_id=state.get("plan_id"), today=today_in_app_timezone())
    except SignupError as exc:
        db.rollback()
        return _render_signup(request, "email", str(exc))
    request.session.pop(SIGNUP_SESSION, None)
    append_audit_event(request=request, actor_user=user, event_type="auth.signup", outcome="success",
                       metadata={"organization_id": user.organization_id, "via": "google" if state.get("google") else "email"})
    support = (get_settings().support_email or "").strip()
    if support:
        try:
            subject, text_body, html_body = email_service.build_new_signup_team_email(
                organization_name=user.organization.name,
                user_name=user.name,
                user_email=user.email,
                user_phone=user.phone,
                trial_days=TRIAL_DAYS,
            )
            email_service.dispatch_email(
                email_service.send_plain_email,
                support,
                subject,
                text_body,
                "aviso de novo cadastro",
                html_body=html_body,
            )
        except Exception:
            logger.exception("Falha ao avisar a equipe sobre novo cadastro")
    response = _complete_web_login(request, db, user)
    response.headers["location"] = "/primeiros-passos?cadastro=concluido"
    return response


@router.get("/cadastro")
def signup_page(request: Request, plano: int | None = None):
    clear_expired_session(request)
    if request.session.get("user_email"):
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)
    state = _signup_state(request)
    if plano:
        state["plan_id"] = plano
        _save_signup(request, state)
    return _render_signup(request, "email")


@router.post("/cadastro")
def signup_email_action(
    request: Request,
    email: str = Form(""),
    website: str = Form(""),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    from app.services.self_signup import SignupError, normalize_email, rate_limited

    validate_csrf(request, csrf_token)
    if website:  # campo invisivel: so robos preenchem
        return RedirectResponse(url="/cadastro", status_code=status.HTTP_303_SEE_OTHER)
    if rate_limited(_client_ip(request)):
        return _render_signup(request, "email", "Muitas tentativas a partir desta rede. Tente novamente mais tarde.")
    try:
        normalized = normalize_email(email)
    except SignupError as exc:
        return _render_signup(request, "email", str(exc))
    if db.query(User.id).filter(User.email == normalized).first():
        return _render_signup(request, "email", "Este e-mail já tem uma conta.", existing_account=True)
    state = _signup_state(request)
    _save_signup(request, {"email": normalized, "plan_id": state.get("plan_id")})
    return RedirectResponse(url="/cadastro/dados", status_code=status.HTTP_303_SEE_OTHER)


@router.get("/cadastro/google")
def signup_google_start(request: Request):
    if not _google_login_enabled():
        return RedirectResponse(url="/cadastro", status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(url=_build_google_authorization_url(request), status_code=status.HTTP_302_FOUND)


@router.get("/cadastro/dados")
def signup_details_page(request: Request):
    if not _signup_state(request).get("email"):
        return RedirectResponse(url="/cadastro", status_code=status.HTTP_303_SEE_OTHER)
    return _render_signup(request, "dados")


@router.post("/cadastro/dados")
def signup_details_action(
    request: Request,
    first_name: str = Form(""),
    last_name: str = Form(""),
    phone: str = Form(""),
    organization_name: str = Form(""),
    password: str = Form(""),
    accepted_terms: str | None = Form(None),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    from app.services import email_service
    from app.services.self_signup import (
        CODE_TTL_SECONDS, SignupError, code_digest, new_code, validate_password, validate_signup,
    )

    validate_csrf(request, csrf_token)
    state = _signup_state(request)
    if not state.get("email"):
        return RedirectResponse(url="/cadastro", status_code=status.HTTP_303_SEE_OTHER)
    form = {"first_name": first_name, "last_name": last_name, "phone": phone, "organization_name": organization_name}
    try:
        data = validate_signup(first_name, last_name, phone, organization_name, accepted_terms)
        password_hash = None if state.get("google") else get_password_hash(validate_password(password))
    except SignupError as exc:
        return _render_signup(request, "dados", str(exc), form=form)
    state["data"] = data.__dict__
    if state.get("google"):  # e-mail ja confirmado pelo Google
        return _finish_signup(request, db, state, None)

    code = new_code()
    try:
        subject, text_body, html_body = email_service.build_signup_code_email(
            code,
            first_name=data.first_name,
            is_resend=False,
            expires_in_minutes=CODE_TTL_SECONDS // 60,
        )
        email_service.dispatch_email(
            email_service.send_plain_email,
            state["email"],
            subject,
            text_body,
            "codigo de cadastro",
            html_body=html_body,
        )
    except Exception:
        logger.exception("Falha ao enviar codigo de cadastro")
        return _render_signup(request, "dados", "Não foi possível enviar o código agora. Tente novamente em instantes.", form=form)
    state.update(password_hash=password_hash, code=code_digest(state["email"], code),
                 code_expires=time.time() + CODE_TTL_SECONDS, code_sent_at=time.time(), attempts=0)
    _save_signup(request, state)
    return RedirectResponse(url="/cadastro/confirmar", status_code=status.HTTP_303_SEE_OTHER)


@router.get("/cadastro/confirmar")
def signup_confirm_page(request: Request):
    if not _signup_state(request).get("code"):
        return RedirectResponse(url="/cadastro", status_code=status.HTTP_303_SEE_OTHER)
    return _render_signup(request, "confirmar")


@router.post("/cadastro/confirmar")
def signup_confirm_action(
    request: Request,
    code: str = Form(""),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    from app.services.self_signup import CODE_MAX_ATTEMPTS, code_matches

    validate_csrf(request, csrf_token)
    state = _signup_state(request)
    if not state.get("code"):
        return RedirectResponse(url="/cadastro", status_code=status.HTTP_303_SEE_OTHER)
    if time.time() > state.get("code_expires", 0) or state.get("attempts", 0) >= CODE_MAX_ATTEMPTS:
        return _render_signup(request, "confirmar", "Este código expirou. Clique em reenviar para receber outro.")
    if not code_matches(state["email"], code, state["code"]):
        state["attempts"] = state.get("attempts", 0) + 1
        _save_signup(request, state)
        return _render_signup(request, "confirmar", "Código incorreto. Confira o e-mail e tente de novo.")
    return _finish_signup(request, db, state, state.get("password_hash"))


@router.post("/cadastro/reenviar")
def signup_resend_action(request: Request, csrf_token: str = Form(...)):
    from app.services import email_service
    from app.services.self_signup import CODE_TTL_SECONDS, RESEND_COOLDOWN_SECONDS, code_digest, new_code, rate_limited

    validate_csrf(request, csrf_token)
    state = _signup_state(request)
    if not state.get("code"):
        return RedirectResponse(url="/cadastro", status_code=status.HTTP_303_SEE_OTHER)
    wait = int(RESEND_COOLDOWN_SECONDS - (time.time() - state.get("code_sent_at", 0)))
    if wait > 0:
        return _render_signup(request, "confirmar", f"Aguarde {wait}s para pedir um novo código.")
    if rate_limited(_client_ip(request)):
        return _render_signup(request, "confirmar", "Muitas tentativas a partir desta rede. Tente novamente mais tarde.")
    code = new_code()
    try:
        first_name = (state.get("data") or {}).get("first_name")
        subject, text_body, html_body = email_service.build_signup_code_email(
            code,
            first_name=first_name,
            is_resend=True,
            expires_in_minutes=CODE_TTL_SECONDS // 60,
        )
        email_service.dispatch_email(
            email_service.send_plain_email,
            state["email"],
            subject,
            text_body,
            "codigo de cadastro",
            html_body=html_body,
        )
    except Exception:
        logger.exception("Falha ao reenviar codigo de cadastro")
        return _render_signup(request, "confirmar", "Não foi possível reenviar o código agora.")
    state.update(code=code_digest(state["email"], code), code_expires=time.time() + CODE_TTL_SECONDS,
                 code_sent_at=time.time(), attempts=0)
    _save_signup(request, state)
    return _render_signup(request, "confirmar", info="Enviamos um novo código.")
