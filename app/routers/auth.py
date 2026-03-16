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

templates = Jinja2Templates(directory="app/templates")
router = APIRouter()
api_router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


@router.get("/login")
def login_page(request: Request):
    if request.session.get("user_email"):
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": None,
            "csrf_token": ensure_csrf_token(request),
            "page": "login",
        },
    )


@router.post("/login")
def login_web(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(...),
    db: Session = Depends(get_db),
):
    validate_csrf(request, csrf_token)
    user = db.query(User).filter(User.email == email).first()
    if not authenticate_user(user, password):
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Credenciais invalidas.",
                "csrf_token": ensure_csrf_token(request),
                "page": "login",
            },
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    request.session["user_email"] = user.email
    return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)


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
