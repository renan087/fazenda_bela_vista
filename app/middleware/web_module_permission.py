"""Bloqueia URLs web quando o usuario autenticado nao tem permissao de modulo."""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from app.core.path_access import is_public_web_prefix, required_permission_for_web_path
from app.db.session import SessionLocal
from app.models import User
from app.services.rbac_service import user_has_permission


class WebModulePermissionMiddleware(BaseHTTPMiddleware):
    """Roda dentro do SessionMiddleware (registrar no app depois do Session)."""

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path or ""
        if request.method == "OPTIONS" or path == "/" or is_public_web_prefix(path):
            return await call_next(request)
        required = required_permission_for_web_path(path)
        if required is None:
            return await call_next(request)
        try:
            email = request.session.get("user_email")
        except AssertionError:
            email = None
        if not email:
            return await call_next(request)
        with SessionLocal() as db:
            row = db.query(User).filter(User.email == email, User.is_active.is_(True)).first()
            if not row:
                return await call_next(request)
            if user_has_permission(db, row, required):
                return await call_next(request)
        request.session["flash"] = {
            "kind": "error",
            "message": "Voce nao tem permissao para acessar este modulo.",
        }
        return RedirectResponse(url="/dashboard", status_code=303)
