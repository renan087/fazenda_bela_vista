"""Tokens de acesso pontual. A autorizacao do registro deve ser verificada pelo chamador."""
from datetime import datetime, timedelta, timezone
import hashlib
import secrets
import threading
import time

from sqlalchemy import update

from app.core.permissions_catalog import PAGE_INPUTS, PAGE_OPERATIONS, PAGE_PRODUCTION
from app.models.crop_season import CropSeason
from app.models.farm import Farm
from app.models.fertilization import FertilizationRecord
from app.models.harvest import HarvestRecord
from app.models.purchased_input import PurchasedInput
from app.models.stock_output import StockOutput
from app.models.user import User
from app.models.whatsapp import WhatsAppLink
from app.models.whatsapp import WhatsAppRecordAccess

TOKEN_LIFETIME = timedelta(hours=48)
RECORD_KINDS = frozenset({"compra", "consumo", "aplicacao", "colheita"})
RECORD_MODELS = {"compra": PurchasedInput, "consumo": StockOutput,
                 "aplicacao": FertilizationRecord, "colheita": HarvestRecord}
RECORD_PERMISSIONS = {"compra": PAGE_INPUTS, "consumo": PAGE_INPUTS,
                      "aplicacao": PAGE_OPERATIONS, "colheita": PAGE_PRODUCTION}
_attempt_lock = threading.Lock()
_failed_attempts: dict[str, list[float]] = {}
_ATTEMPT_WINDOW = 300
_ATTEMPT_LIMIT = 30


def attempts_blocked(ip: str) -> bool:
    now = time.monotonic()
    with _attempt_lock:
        # Remove chaves antigas para que IPs esporadicos nao crescam sem limite.
        for key in list(_failed_attempts):
            _failed_attempts[key] = [at for at in _failed_attempts[key] if now - at < _ATTEMPT_WINDOW]
            if not _failed_attempts[key]:
                del _failed_attempts[key]
        return len(_failed_attempts.get(ip, [])) >= _ATTEMPT_LIMIT


def register_failed_attempt(ip: str) -> None:
    with _attempt_lock:
        _failed_attempts.setdefault(ip, []).append(time.monotonic())


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("ascii")).hexdigest()


def link_binding(link) -> str:
    verified = link.verified_at
    if not link.wa_id or not verified:
        return ""
    if verified.tzinfo is None:
        verified = verified.replace(tzinfo=timezone.utc)
    value = f"{link.wa_id}|{verified.astimezone(timezone.utc).isoformat()}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _valid_format(token) -> bool:
    return isinstance(token, str) and len(token) == 43 and all(c.isascii() and (c.isalnum() or c in "-_") for c in token)


def find_token(db, token: str, *, now=None, lock=False):
    if not _valid_format(token):
        return None
    now = now or datetime.now(timezone.utc)
    query = db.query(WhatsAppRecordAccess).filter(
        WhatsAppRecordAccess.token_hash == _digest(token), WhatsAppRecordAccess.consumed_at.is_(None),
        WhatsAppRecordAccess.expires_at > now,
    )
    return (query.with_for_update() if lock else query).first()


def _record_in_scope(record, kind, farm, season):
    if kind == "compra":
        return record.farm_id == farm.id
    if kind == "consumo":
        if record.farm_id != farm.id:
            return False
        if record.season_id is not None and record.season_id != season.id:
            return False
        if record.plot_id is not None and record.plot_id not in {plot.id for plot in season.plots}:
            return False
        return bool(record.movement_date and season.start_date <= record.movement_date <= season.end_date)
    if kind not in ("aplicacao", "colheita"):
        return False
    if not record.plot or record.plot.farm_id != farm.id or record.plot_id not in {plot.id for plot in season.plots}:
        return False
    if record.season_id is not None and record.season_id != season.id:
        return False
    record_date = record.application_date if kind == "aplicacao" else record.harvest_date
    return bool(record_date and season.start_date <= record_date <= season.end_date)


def authorized_record(db, access):
    """Revalida usuario, vinculo e permissao; nenhuma rota deve confiar apenas no token."""
    from app.services.whatsapp_bot import _access_denial
    from app.web import routes

    model = RECORD_MODELS.get(access.kind)
    if model is None:
        return None
    link = db.get(WhatsAppLink, access.link_id)
    user = db.get(User, access.user_id)
    if not link or not user or not link.verified_at or not link.wa_id:
        return None
    if not access.binding_hash or not secrets.compare_digest(access.binding_hash, link_binding(link)):
        return None
    if (link.user_id, link.organization_id, link.farm_id, link.season_id) != (
        access.user_id, access.organization_id, access.farm_id, access.season_id
    ) or _access_denial(db, routes, user, link):
        return None
    page = RECORD_PERMISSIONS[access.kind]
    if page not in routes.permission_codes_for_user(db, user) or not routes._entitlements_for(db, user).module_allowed(page):
        return None
    farm, season = db.get(Farm, access.farm_id), db.get(CropSeason, access.season_id)
    record = db.get(model, access.record_id)
    if not farm or not season or not record or not _record_in_scope(record, access.kind, farm, season):
        return None
    return record


def create_token(db, *, link, kind: str, record_id: int, now=None) -> str:
    """Cria o token sem confirmar a transacao; o chamador deve validar as permissoes."""
    if kind not in RECORD_KINDS or isinstance(record_id, bool) or not isinstance(record_id, int) or record_id < 1:
        raise ValueError("Registro inválido.")
    if not link.id or not link.user_id or not link.organization_id or not link.farm_id or not link.season_id or not link_binding(link):
        raise ValueError("Vínculo incompleto.")
    now = now or datetime.now(timezone.utc)
    token = secrets.token_urlsafe(32)
    db.add(WhatsAppRecordAccess(
        token_hash=_digest(token), binding_hash=link_binding(link), link_id=link.id, user_id=link.user_id, organization_id=link.organization_id,
        farm_id=link.farm_id, season_id=link.season_id, kind=kind, record_id=record_id,
        created_at=now, expires_at=now + TOKEN_LIFETIME,
    ))
    return token


def revoke_access(db, access_id, user_id, organization_id, *, now=None):
    """Revoga o acesso usando a coluna existente; tokens antigos usados permanecem invalidados."""
    now = now or datetime.now(timezone.utc)
    statement = (update(WhatsAppRecordAccess)
                 .where(WhatsAppRecordAccess.id == access_id, WhatsAppRecordAccess.user_id == user_id,
                        WhatsAppRecordAccess.organization_id == organization_id,
                        WhatsAppRecordAccess.consumed_at.is_(None))
                 .values(consumed_at=now).execution_options(synchronize_session=False))
    return db.execute(statement).rowcount == 1
