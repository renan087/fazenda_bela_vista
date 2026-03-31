from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.core.csrf import ensure_csrf_token, validate_csrf
from app.core.security import authenticate_user, create_access_token
from app.db.session import get_db
from app.models import User
from app.schemas.auth import Token
from app.services.email_service import send_access_code_email
from app.services.two_factor import get_active_login_code, issue_login_verification_code, revoke_active_login_codes, verify_login_code

templates = Jinja2Templates(directory="app/templates")
router = APIRouter()
api_router = APIRouter(prefix="/api/v1/auth", tags=["auth"])

PENDING_2FA_USER_ID = "pending_2fa_user_id"
PENDING_2FA_EMAIL = "pending_2fa_email"


def _pending_2fa_user(request: Request, db: Session) -> User | None:
    pending_user_id = request.session.get(PENDING_2FA_USER_ID)
    if not pending_user_id:
        return None
    return db.query(User).filter(User.id == pending_user_id, User.is_active.is_(True)).first()


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


def _render_login_verification(request: Request, email: str, error: str | None = None, info: str | None = None):
    return templates.TemplateResponse(
        "login_verify.html",
        {
            "request": request,
            "error": error,
            "info": info,
            "email": email,
            "csrf_token": ensure_csrf_token(request),
            "page": "login",
        },
        status_code=status.HTTP_400_BAD_REQUEST if error else status.HTTP_200_OK,
    )


@router.get("/login")
def login_page(request: Request):
    if request.session.get("user_email"):
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

    try:
        code = issue_login_verification_code(db, user)
        send_access_code_email(user.email, code)
    except RuntimeError as exc:
        revoke_active_login_codes(db, user.id)
        return _render_login(request, str(exc))
    except Exception:
        revoke_active_login_codes(db, user.id)
        return _render_login(request, "Nao foi possivel enviar o codigo de acesso. Tente novamente.")

    request.session.pop("user_email", None)
    request.session[PENDING_2FA_USER_ID] = user.id
    request.session[PENDING_2FA_EMAIL] = user.email
    return RedirectResponse(url="/login/verificacao", status_code=status.HTTP_303_SEE_OTHER)


@router.get("/login/verificacao")
def login_verification_page(
    request: Request,
    db: Session = Depends(get_db),
):
    if request.session.get("user_email"):
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)
    user = _pending_2fa_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)
    return _render_login_verification(request, request.session.get(PENDING_2FA_EMAIL, user.email))


@router.post("/login/verificacao")
def login_verification_web(
    request: Request,
    code: str = Form(...),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    validate_csrf(request, csrf_token)
    user = _pending_2fa_user(request, db)
    if not user:
        return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)
    normalized_code = "".join(char for char in code if char.isdigit())[:6]
    if len(normalized_code) != 6:
        return _render_login_verification(
            request,
            request.session.get(PENDING_2FA_EMAIL, user.email),
            "Informe um codigo numerico de 6 digitos.",
        )
    valid, message = verify_login_code(db, user.id, normalized_code)
    if not valid:
        return _render_login_verification(
            request,
            request.session.get(PENDING_2FA_EMAIL, user.email),
            message,
        )

    request.session["user_email"] = user.email
    request.session.pop(PENDING_2FA_USER_ID, None)
    request.session.pop(PENDING_2FA_EMAIL, None)
    return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/login/verificacao/reenviar")
def resend_login_verification_code(
    request: Request,
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
        return _render_login_verification(request, user.email, str(exc))
    except Exception:
        return _render_login_verification(request, user.email, "Nao foi possivel reenviar o codigo. Tente novamente.")
    return _render_login_verification(request, user.email, info="Enviamos um novo codigo para o seu email.")


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
