import logging
import secrets
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
from app.core.session import clear_expired_session, touch_session_activity
from app.core.security import authenticate_user, create_access_token, get_password_hash, verify_password
from app.core.timezone import utc_now
from app.db.init_db import seed_admin
from app.db.session import get_db
from app.models import User
from app.schemas.auth import Token
from app.services.email_service import send_access_code_email, send_password_reset_email
from app.services.password_reset import get_valid_password_reset_token, issue_password_reset_token, revoke_user_password_reset_tokens
from app.services.trusted_browser import (
    issue_trusted_browser_token,
    revoke_user_trusted_browsers,
    validate_trusted_browser_token,
)
from app.services.two_factor import get_active_login_code, issue_login_verification_code, revoke_active_login_codes, verify_login_code

templates = Jinja2Templates(directory="app/templates")
router = APIRouter()
api_router = APIRouter(prefix="/api/v1/auth", tags=["auth"])
logger = logging.getLogger(__name__)

PENDING_2FA_USER_ID = "pending_2fa_user_id"
PENDING_2FA_EMAIL = "pending_2fa_email"
PENDING_2FA_LAST_SENT_AT = "pending_2fa_last_sent_at"
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


def _complete_web_login(
    request: Request,
    db: Session,
    user: User,
    trusted_browser_token: str | None = None,
    clear_trusted_browser_cookie: bool = False,
):
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

    request.session["user_email"] = user.email
    request.session.pop(PENDING_2FA_USER_ID, None)
    request.session.pop(PENDING_2FA_EMAIL, None)
    request.session.pop(PENDING_2FA_LAST_SENT_AT, None)
    touch_session_activity(request)
    response = RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)
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


def _start_web_login_challenge(request: Request, db: Session, user: User):
    trusted_browser_cookie = request.cookies.get(_trusted_browser_cookie_name())
    clear_trusted_cookie = False
    if trusted_browser_cookie:
        if validate_trusted_browser_token(db, user, request, trusted_browser_cookie):
            revoke_active_login_codes(db, user.id)
            return _complete_web_login(request, db, user)
        clear_trusted_cookie = True

    if not user.is_two_factor_enabled:
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


def _render_login(request: Request, error: str | None = None):
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
        status_code=status.HTTP_400_BAD_REQUEST if error else status.HTTP_200_OK,
    )


def _render_login_verification(
    request: Request,
    email: str,
    error: str | None = None,
    info: str | None = None,
    trust_browser_checked: bool = False,
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
        },
        status_code=status.HTTP_400_BAD_REQUEST if error else status.HTTP_200_OK,
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
    seed_admin(db)
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
    seed_admin(db)
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
        return _render_login(request, "Seu email Google nao esta cadastrado no sistema. Solicite acesso ao administrador.")
    if not user.is_active:
        return _render_login(request, "Seu usuario esta inativo no sistema. Entre em contato com o administrador.")
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

    user = db.query(User).filter(User.id == record.user_id, User.is_active.is_(True)).first()
    if not user:
        return _render_password_reset(request, "", error="Este link e invalido ou expirou. Solicite uma nova redefinicao.")

    if verify_password(normalized_new_password, user.hashed_password):
        return _render_password_reset(request, normalized_token, error="A nova senha precisa ser diferente da senha atual.")

    user.hashed_password = get_password_hash(normalized_new_password)
    record.used_at = utc_now()
    db.add(user)
    db.add(record)
    db.commit()
    revoke_user_password_reset_tokens(db, user.id)
    revoke_user_trusted_browsers(db, user.id)
    revoke_active_login_codes(db, user.id)

    request.session.pop("user_email", None)
    request.session.pop(PENDING_2FA_USER_ID, None)
    request.session.pop(PENDING_2FA_EMAIL, None)
    return RedirectResponse(url="/login?notice=password-reset-success", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/login")
def login_web(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    seed_admin(db)
    validate_csrf(request, csrf_token)
    user = db.query(User).filter(User.email == email.strip().lower()).first()
    _reactivate_super_admin_if_needed(db, user)
    if not authenticate_user(user, password):
        return _render_login(request, "Credenciais invalidas.")
    return _start_web_login_challenge(request, db, user)


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
        return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)
    return _render_login_verification(request, request.session.get(PENDING_2FA_EMAIL, user.email))


@router.post("/login/verificacao")
def login_verification_web(
    request: Request,
    code: str = Form(...),
    trust_browser: str | None = Form(None),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    validate_csrf(request, csrf_token)
    user = _pending_2fa_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)
    trust_browser_checked = _trust_browser_requested(trust_browser)
    normalized_code = "".join(char for char in code if char.isdigit())[:6]
    if len(normalized_code) != 6:
        return _render_login_verification(
            request,
            request.session.get(PENDING_2FA_EMAIL, user.email),
            "Informe um codigo numerico de 6 digitos.",
            trust_browser_checked=trust_browser_checked,
        )
    valid, message = verify_login_code(db, user.id, normalized_code)
    if not valid:
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

    return _complete_web_login(request, db, user, trusted_browser_token=trusted_browser_token)


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
        return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)
    cooldown_seconds = _login_code_resend_cooldown_remaining(request)
    if cooldown_seconds > 0:
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
        return _render_login_verification(
            request,
            user.email,
            "Nao foi possivel reenviar o codigo. Tente novamente.",
            trust_browser_checked=_trust_browser_requested(trust_browser),
        )
    _mark_login_code_sent(request)
    return _render_login_verification(
        request,
        user.email,
        info="Enviamos um novo codigo para o seu email.",
        trust_browser_checked=_trust_browser_requested(trust_browser),
    )


@router.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)


@api_router.post("/token", response_model=Token)
def login_api(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.email == form_data.username).first()
    if not authenticate_user(user, form_data.password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Credenciais invalidas")

    return Token(access_token=create_access_token(user.email))
