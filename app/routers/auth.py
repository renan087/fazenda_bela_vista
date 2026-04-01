import logging

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.core.csrf import ensure_csrf_token, validate_csrf
from app.core.config import get_settings
from app.core.session import clear_expired_session, touch_session_activity
from app.core.security import authenticate_user, create_access_token
from app.core.timezone import utc_now
from app.db.session import get_db
from app.models import User
from app.schemas.auth import Token
from app.services.email_service import send_access_code_email
from app.services.trusted_browser import (
    issue_trusted_browser_token,
    validate_trusted_browser_token,
)
from app.services.two_factor import get_active_login_code, issue_login_verification_code, revoke_active_login_codes, verify_login_code

templates = Jinja2Templates(directory="app/templates")
router = APIRouter()
api_router = APIRouter(prefix="/api/v1/auth", tags=["auth"])
logger = logging.getLogger(__name__)

PENDING_2FA_USER_ID = "pending_2fa_user_id"
PENDING_2FA_EMAIL = "pending_2fa_email"


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
    touch_session_activity(request)
    response = RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)
    if clear_trusted_browser_cookie:
        _clear_trusted_browser_cookie(response)
    if trusted_browser_token:
        _set_trusted_browser_cookie(response, trusted_browser_token)
    return response


def _render_login(request: Request, error: str | None = None):
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": error,
            "csrf_token": ensure_csrf_token(request),
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
            "csrf_token": ensure_csrf_token(request),
            "page": "login",
        },
        status_code=status.HTTP_400_BAD_REQUEST if error else status.HTTP_200_OK,
    )


@router.get("/login")
def login_page(request: Request):
    clear_expired_session(request)
    if request.session.get("user_email"):
        touch_session_activity(request)
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)
    return _render_login(request)


@router.post("/login")
def login_web(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    validate_csrf(request, csrf_token)
    user = db.query(User).filter(User.email == email.strip().lower()).first()
    if not authenticate_user(user, password):
        return _render_login(request, "Credenciais invalidas.")

    trusted_browser_cookie = request.cookies.get(_trusted_browser_cookie_name())
    clear_trusted_cookie = False
    if trusted_browser_cookie:
        if validate_trusted_browser_token(db, user, request, trusted_browser_cookie):
            revoke_active_login_codes(db, user.id)
            return _complete_web_login(request, db, user)
        clear_trusted_cookie = True

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
    touch_session_activity(request)
    response = RedirectResponse(url="/login/verificacao", status_code=status.HTTP_303_SEE_OTHER)
    if clear_trusted_cookie:
        _clear_trusted_browser_cookie(response)
    return response


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
