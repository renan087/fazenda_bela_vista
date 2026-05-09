"""Bloqueia URLs web quando o usuario autenticado nao tem permissao de modulo."""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from app.core.path_access import is_public_web_prefix, required_permissions_for_web_path
from app.db.session import SessionLocal
from app.models import User
from app.services.rbac_service import permission_codes_for_user


class WebModulePermissionMiddleware(BaseHTTPMiddleware):
    """Roda dentro do SessionMiddleware (registrar no app depois do Session)."""

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path or ""
        if request.method == "OPTIONS" or path == "/" or is_public_web_prefix(path):
            return await call_next(request)
        required_permissions = required_permissions_for_web_path(path, request.method)
        if not required_permissions:
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
            permission_codes = permission_codes_for_user(db, row)
            if all(code in permission_codes for code in required_permissions):
                return await call_next(request)
            if not permission_codes:
                request.session["flash"] = {
                    "kind": "warning",
                    "message": "Seu usuario ainda nao possui perfis de acesso configurados.",
                }
                return RedirectResponse(url="/sem-acesso", status_code=303)
        request.session["flash"] = {
            "kind": "error",
            "message": "Voce nao tem permissao para realizar esta acao.",
        }
        return RedirectResponse(url="/dashboard", status_code=303)
