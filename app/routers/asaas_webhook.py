"""Webhook HTTP do Asaas (POST com eventos de cobrança)."""

from __future__ import annotations

import hmac
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.session import get_db
from app.services.organization_subscription_sync import persist_organization_webhook
from app.models import AsaasWebhookEvent, OrganizationBilling, OrganizationCardMigration
from app.services.subscription_reconciliation import close_card_checkout
from app.services.asaas_environment import asaas_env_name

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/webhooks", tags=["asaas-webhook"])


def _header_token(request: Request) -> str | None:
    return (
        request.headers.get("asaas-access-token")
        or request.headers.get("Asaas-Access-Token")
        or request.headers.get("ASAAS-ACCESS-TOKEN")
    )


@router.post("/asaas")
async def asaas_webhook(request: Request, db: Session = Depends(get_db)):
    """
    Recebe notificações do Asaas (PAYMENT_RECEIVED, PAYMENT_CONFIRMED, etc.).
    Configure em Asaas: URL = https://seu-dominio/api/webhooks/asaas
    e o mesmo token em ASAAS_WEBHOOK_TOKEN e no painel Asaas.
    """
    settings = get_settings()
    expected = (settings.asaas_webhook_token or "").strip()
    if not expected:
        logger.warning("Webhook Asaas rejeitado: ASAAS_WEBHOOK_TOKEN não configurado.")
        raise HTTPException(status_code=503, detail="Webhook não configurado no servidor")

    incoming = (_header_token(request) or "").strip()
    # Comparacao em tempo constante: nao revela, pelo tempo de resposta, quantos caracteres conferem.
    if not hmac.compare_digest(incoming.encode(), expected.encode()):
        logger.warning("Webhook Asaas rejeitado: token inválido.")
        raise HTTPException(status_code=401, detail="Token inválido")

    try:
        body: dict[str, Any] = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Evento inválido")
    event = str(body.get("event") or "").strip()
    if event in {"CHECKOUT_PAID", "CHECKOUT_CANCELED", "CHECKOUT_EXPIRED"}:
        checkout = body.get("checkout")
        event_id = body.get("id")
        if not isinstance(checkout, dict) or not isinstance(checkout.get("id"), str) or not 1 <= len(checkout["id"]) <= 80:
            raise HTTPException(status_code=400, detail="Checkout inválido")
        if not isinstance(event_id, str) or not event_id or len(event_id) > 200:
            raise HTTPException(status_code=400, detail="Identificador do evento inválido")

    try:
        checkout = body.get('checkout')
        if event in {'CHECKOUT_PAID', 'CHECKOUT_CANCELED', 'CHECKOUT_EXPIRED'} and isinstance(checkout, dict):
            migration = db.query(OrganizationCardMigration).filter_by(checkout_id=checkout['id'], environment=asaas_env_name()).with_for_update().first()
            if migration:
                billing = db.query(OrganizationBilling).filter_by(organization_id=migration.organization_id, asaas_environment=asaas_env_name()).first()
                if not billing or checkout.get('customer') != billing.asaas_customer_id:
                    raise ValueError('Cliente do checkout divergente')
                if db.query(AsaasWebhookEvent.id).filter_by(event_id=event_id).first():
                    return {"received": True}
                db.add(AsaasWebhookEvent(event_id=event_id, event_type=event, resource_id=checkout["id"]))
                if event == 'CHECKOUT_PAID' and checkout.get('status') == 'PAID' and migration.is_open:
                    migration.status = 'pago'
                elif event in {'CHECKOUT_CANCELED', 'CHECKOUT_EXPIRED'} and migration.is_open and migration.status in {'preparando', 'aguardando', 'revisao'}:
                    # Link que nao pode mais ser pago: encerra sozinho e restaura a assinatura anterior.
                    close_card_checkout(db, migration, remote_status=checkout.get('status') or event)
                db.commit()
                return {'received': True}
        # Eventos que nao pertencem a uma assinatura de organizacao sao apenas confirmados.
        persist_organization_webhook(db, body)
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("Falha ao persistir webhook Asaas event=%s", event)
        raise HTTPException(status_code=500, detail="Erro ao processar")

    return {"received": True}
