"""Aplica os direitos do plano nas paginas web: modulos com cadeado e modo somente leitura por inadimplencia."""

from __future__ import annotations

import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from app.core.admin_access import is_super_admin_email
from app.core.path_access import is_public_web_prefix, required_permission_for_web_path
from app.core.timezone import today_in_app_timezone
from app.db.session import SessionLocal
from app.models import User
from app.services.entitlements import ALWAYS_ALLOWED_PREFIXES, cached_entitlements

logger = logging.getLogger(__name__)

SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
# Alteracoes que continuam permitidas no modo somente leitura (conta, sessao e pagamento).
READ_ONLY_WRITE_ALLOWED = ALWAYS_ALLOWED_PREFIXES + ("/meu-perfil", "/contexto", "/logout", "/login", "/auth")


def _starts_with_any(path: str, prefixes) -> bool:
    return any(path == prefix or path.startswith(prefix + "/") or path.startswith(prefix + "?") for prefix in prefixes)


def upgrade_url(module_code: str | None) -> str:
    return f"/planos-disponiveis?modulo={module_code}" if module_code else "/planos-disponiveis"


class SubscriptionEnforcementMiddleware(BaseHTTPMiddleware):
    """Roda dentro do SessionMiddleware. A equipe SiSFarm (super administrador) nunca e limitada."""

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path or ""
        if path == "/" or is_public_web_prefix(path) or _starts_with_any(path, ALWAYS_ALLOWED_PREFIXES):
            return await call_next(request)
        try:
            email = request.session.get("user_email")
        except AssertionError:
            email = None
        if not email or is_super_admin_email(email):
            return await call_next(request)
        try:
            with SessionLocal() as db:
                user = db.query(User).filter(User.email == email, User.is_active.is_(True)).first()
                entitlements = cached_entitlements(db, user.organization_id if user else None, today_in_app_timezone())
        except Exception:
            # Falha ao calcular direitos nunca derruba o sistema do cliente.
            logger.exception("Falha ao verificar o plano da organizacao")
            return await call_next(request)
        if entitlements.exempt:
            return await call_next(request)

        module = required_permission_for_web_path(path)
        if not entitlements.module_allowed(module):
            if request.method in SAFE_METHODS:
                return RedirectResponse(url=upgrade_url(module), status_code=303)
            request.session["flash"] = {"kind": "error", "message": "Este módulo não faz parte do seu plano."}
            return RedirectResponse(url=upgrade_url(module), status_code=303)

        if entitlements.read_only and request.method not in SAFE_METHODS and not _starts_with_any(path, READ_ONLY_WRITE_ALLOWED):
            request.session["flash"] = {
                "kind": "error",
                "message": f"Sistema em modo somente leitura: há pagamento em atraso há {entitlements.days_overdue} dia(s). Regularize em Minha assinatura para voltar a cadastrar e editar.",
            }
            referer = request.headers.get("referer") or ""
            target = referer if referer.startswith(str(request.base_url)) else "/minha-assinatura"
            return RedirectResponse(url=target, status_code=303)
        return await call_next(request)
