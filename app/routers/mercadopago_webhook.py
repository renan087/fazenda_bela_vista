import hashlib
import hmac
import logging
import re

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from app.core.config import get_settings
from app.db.session import get_db
from app.models import MercadoPagoEvent
from app.services.billing_environment import MERCADOPAGO_LIVE, MERCADOPAGO_TEST, mercadopago_environment
from app.services.mercadopago_pix_orders import pix_event_environment, pix_webhook_available
from app.services.mercadopago_subscriptions import TOPICS, configuration_error, resource_id

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/webhooks/mercadopago", tags=["mercadopago-webhook"])


def _signature_valid(secret, signature, request_id, identifier):
    fields = {}
    for item in signature.split(","):
        key, sep, value = item.strip().partition("=")
        if not sep or key in fields:
            return False
        fields[key] = value
    timestamp, digest = fields.get("ts", ""), fields.get("v1", "")
    if not re.fullmatch(r"[0-9]{1,20}", timestamp) or not re.fullmatch(r"[a-fA-F0-9]{64}", digest):
        return False
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,160}", request_id):
        return False
    manifest = f"id:{identifier.lower()};request-id:{request_id};ts:{timestamp};"
    computed = hmac.new(secret.encode(), manifest.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(computed, digest.lower())


def _enqueue(db, environment, topic, identifier, key, request_key):
    try:
        db.add(MercadoPagoEvent(environment=environment, topic=topic, resource_id=identifier,
                               delivery_key=key, request_key=request_key))
        db.commit()
    except IntegrityError:
        db.rollback()
        if not db.query(MercadoPagoEvent.id).filter(or_(
            MercadoPagoEvent.delivery_key == key, MercadoPagoEvent.request_key == request_key,
        )).first():
            raise
        return
    logger.info("Mercado Pago: webhook recebido ambiente=%s tópico=%s recurso=%s", environment, topic, identifier)


@router.post("/pix/test")
async def mercadopago_pix_test_webhook(request: Request, db: Session = Depends(get_db)):
    return await _pix_webhook(request, db, MERCADOPAGO_TEST)


@router.post("/pix")
async def mercadopago_pix_live_webhook(request: Request, db: Session = Depends(get_db)):
    return await _pix_webhook(request, db, MERCADOPAGO_LIVE)


async def _pix_webhook(request: Request, db: Session, environment: str):
    """Recebe notificações de Order (Pix) da aplicação independente SiSFarm Pix."""
    settings = get_settings()
    if not pix_webhook_available(environment, settings):
        raise HTTPException(status_code=503, detail="Webhook Pix indisponível neste ambiente")
    secret = (
        settings.mercadopago_pix_webhook_secret
        if environment == MERCADOPAGO_LIVE
        else settings.mercadopago_pix_test_webhook_secret
    )
    identifiers = request.query_params.getlist("data.id")
    identifier = resource_id(identifiers[0]) if len(identifiers) == 1 else None
    request_id = request.headers.get("x-request-id", "")
    signature = request.headers.get("x-signature", "")
    if not identifier or not secret or not _signature_valid(secret.strip(), signature, request_id, identifier):
        raise HTTPException(status_code=401, detail="Assinatura da notificação Pix inválida")
    try:
        body = await request.json()
    except ValueError:
        raise HTTPException(status_code=400, detail="Notificação inválida") from None
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict) or resource_id(data.get("id")) != identifier:
        raise HTTPException(status_code=400, detail="Recurso da notificação divergente")
    topic = str(body.get("type") or "").strip().lower()
    if topic not in {"order", "orders"}:
        return {"received": True}
    event_id = resource_id(body.get("id")) or request_id
    date_created = str(body.get("date_created") or body.get("date") or "")[:64]
    action = str(body.get("action") or "")[:64]
    prefix = "pix-live:" if environment == MERCADOPAGO_LIVE else "pix-test:"
    key = hashlib.sha256(f"{prefix}order:{identifier}:{event_id}:{date_created}:{action}".encode()).hexdigest()
    request_key = hashlib.sha256(f"{prefix}{identifier}:{request_id}".encode()).hexdigest()
    await run_in_threadpool(_enqueue, db, pix_event_environment(environment), "order", identifier, key, request_key)
    return {"received": True}


@router.post("/test")
async def mercadopago_test_webhook(request: Request, db: Session = Depends(get_db)):
    return await _webhook(request, db, MERCADOPAGO_TEST)


@router.post("")
async def mercadopago_live_webhook(request: Request, db: Session = Depends(get_db)):
    return await _webhook(request, db, MERCADOPAGO_LIVE)


async def _webhook(request: Request, db: Session, environment: str):
    settings = get_settings()
    if mercadopago_environment(settings) != environment or configuration_error(settings):
        raise HTTPException(status_code=503, detail="Webhook indisponível neste ambiente")
    secret = settings.mercadopago_live_webhook_secret if environment == MERCADOPAGO_LIVE else settings.mercadopago_webhook_secret
    identifiers = request.query_params.getlist("data.id")
    identifier = resource_id(identifiers[0]) if len(identifiers) == 1 else None
    request_id = request.headers.get("x-request-id", "")
    signature = request.headers.get("x-signature", "")
    if not identifier or not _signature_valid(secret.strip(), signature, request_id, identifier):
        raise HTTPException(status_code=401, detail="Assinatura da notificação inválida")
    try:
        body = await request.json()
    except ValueError:
        raise HTTPException(status_code=400, detail="Notificação inválida") from None
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict) or resource_id(data.get("id")) != identifier:
        raise HTTPException(status_code=400, detail="Recurso da notificação divergente")
    topic = body.get("type")
    if not isinstance(topic, str) or topic not in TOPICS:
        return {"received": True}
    # O worker consulta o recurso na API e valida a conta e o vínculo antes de aplicar pagamentos.
    # O identificador da notificação é distinto do ID da assinatura ou do pagamento.
    event_id = resource_id(body.get("id")) or request_id
    date_created = str(body.get("date_created") or body.get("date") or "")[:64]
    action = str(body.get("action") or "")[:64]
    prefix = "live:" if environment == MERCADOPAGO_LIVE else ""
    key = hashlib.sha256(f"{prefix}{topic}:{identifier}:{event_id}:{date_created}:{action}".encode()).hexdigest()
    request_key = hashlib.sha256(f"{prefix}{identifier}:{request_id}".encode()).hexdigest()
    await run_in_threadpool(_enqueue, db, environment, topic, identifier, key, request_key)
    return {"received": True}
