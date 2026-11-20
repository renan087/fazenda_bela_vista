"""Autocadastro: cria organizacao, usuario administrador e o teste gratis de 14 dias."""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, timedelta

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import get_password_hash
from app.models import Organization, User
from app.models.organization_access_grant import OrganizationAccessGrant
from app.models.subscription_plan import SubscriptionPlan
from app.services.rbac_service import ensure_organization_roles, sync_roles_from_admin_flag
from app.services.subscription_plans import active_plans, plan_modules

TRIAL_DAYS = 14
from app.services.access_grants import TRIAL_GRANT_REASON as TRIAL_REASON  # noqa: E402
CODE_TTL_SECONDS = 15 * 60
CODE_MAX_ATTEMPTS = 5
RESEND_COOLDOWN_SECONDS = 60
MIN_PASSWORD = 8
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$", re.IGNORECASE)

# Limite simples por IP (processo unico no Render): tentativas de cadastro por hora.
_RATE_WINDOW = 3600
_RATE_MAX = 8
_rate_hits: dict[str, list[float]] = {}


class SignupError(ValueError):
    pass


@dataclass
class SignupData:
    first_name: str
    last_name: str
    phone: str
    organization_name: str


def rate_limited(ip: str | None, now: float | None = None) -> bool:
    """Registra uma tentativa e informa se o IP passou do limite."""
    now = now or time.time()
    key = ip or "desconhecido"
    hits = [t for t in _rate_hits.get(key, []) if now - t < _RATE_WINDOW]
    hits.append(now)
    _rate_hits[key] = hits
    return len(hits) > _RATE_MAX


def normalize_email(raw: str | None) -> str:
    email = (raw or "").strip().lower()
    if len(email) > 120 or not EMAIL_RE.match(email):
        raise SignupError("Informe um e-mail válido.")
    return email


def format_phone(raw: str | None) -> str:
    digits = re.sub(r"\D", "", raw or "")
    if digits.startswith("55") and len(digits) in (12, 13):
        digits = digits[2:]
    if len(digits) not in (10, 11):
        raise SignupError("Informe o WhatsApp com DDD, por exemplo (35) 99999-9999.")
    return f"({digits[:2]}) {digits[2:-4]}-{digits[-4:]}"


def validate_signup(first_name, last_name, phone, organization_name, accepted_terms) -> SignupData:
    first, last, org = ((v or "").strip() for v in (first_name, last_name, organization_name))
    if not first or not last:
        raise SignupError("Informe nome e sobrenome.")
    if len(first) + len(last) > 110:
        raise SignupError("Nome muito longo.")
    if len(org) < 2 or len(org) > 140:
        raise SignupError("Informe o nome da fazenda ou empresa.")
    if not accepted_terms:
        raise SignupError("Para continuar, aceite os Termos de Uso e a Política de Privacidade.")
    return SignupData(first, last, format_phone(phone), org)


def validate_password(password: str | None) -> str:
    value = (password or "").strip()
    if len(value) < MIN_PASSWORD:
        raise SignupError(f"A senha precisa ter pelo menos {MIN_PASSWORD} caracteres.")
    return value


def new_code() -> str:
    return f"{secrets.randbelow(1_000_000):06d}"


def code_digest(email: str, code: str) -> str:
    """HMAC do codigo: o cookie de sessao e legivel no navegador, entao o codigo nunca vai em claro."""
    key = get_settings().secret_key.encode()
    return hmac.new(key, f"{email}:{code}".encode(), hashlib.sha256).hexdigest()


def code_matches(email: str, code: str, digest: str | None) -> bool:
    return bool(digest) and hmac.compare_digest(code_digest(email, (code or "").strip()), digest)


def trial_plan(db: Session, plan_id: int | None) -> SubscriptionPlan | None:
    plans = active_plans(db)
    if plan_id:
        chosen = next((p for p in plans if p.id == plan_id), None)
        if chosen:
            return chosen
    # Sem escolha: o teste libera o plano mais completo ("tudo liberado", como diz a landing).
    return max(plans, key=lambda p: (len(plan_modules(p)), p.price_monthly or 0), default=None)


def _slugify(name: str) -> str:
    text = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")[:170] or "organizacao"


def _unique(db: Session, column, base: str, sep: str) -> str:
    value, n = base, 2
    while db.query(Organization.id).filter(column == value).first():
        value = f"{base}{sep}{n}"
        n += 1
    return value


def create_self_signup(db: Session, *, email: str, data: SignupData, password_hash: str | None,
                       plan_id: int | None, today: date) -> User:
    """Cria tudo numa transacao. Sem senha (cadastro via Google) gera uma senha aleatoria inutilizavel."""
    if db.query(User.id).filter(User.email == email).first():
        raise SignupError("Este e-mail já tem uma conta. Use Entrar.")
    name = data.organization_name
    organization = Organization(name=name, slug=_unique(db, Organization.slug, _slugify(name), "-"), is_active=True)
    db.add(organization)
    db.flush()
    ensure_organization_roles(db, organization.id)
    full_name = f"{data.first_name} {data.last_name}"
    user = User(
        name=full_name[:120], display_name=data.first_name[:120], email=email, phone=data.phone,
        hashed_password=password_hash or get_password_hash(secrets.token_urlsafe(32)),
        is_active=True, is_admin=True, organization_id=organization.id,
    )
    db.add(user)
    db.flush()
    organization.primary_user_id = user.id
    from app.services.commercial_audit import record_commercial_event
    record_commercial_event(
        db, user=user, organization_id=organization.id, resource="organizacao", resource_id=organization.id,
        action="definir_responsavel", outcome="success",
        details={"anterior": None, "novo": {"id": user.id, "nome": user.name, "email": user.email}, "origem": "autocadastro"},
    )
    sync_roles_from_admin_flag(db, user)
    plan = trial_plan(db, plan_id)
    if plan:
        db.add(OrganizationAccessGrant(
            organization_id=organization.id, plan_id=plan.id, reason=TRIAL_REASON,
            justification=f"Teste grátis de {TRIAL_DAYS} dias pelo autocadastro.",
            starts_on=today, ends_on=today + timedelta(days=TRIAL_DAYS),
            created_by_user_id=user.id, created_by_email=email,
        ))
    db.commit()
    db.refresh(user)
    return user
