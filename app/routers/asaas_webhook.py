"""Webhook HTTP do Asaas (POST com eventos de cobrança)."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.session import get_db
from app.services.asaas_payment_sync import upsert_asaas_payment_row

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
    if incoming != expected:
        logger.warning("Webhook Asaas rejeitado: token inválido.")
        raise HTTPException(status_code=401, detail="Token inválido")

    try:
        body: dict[str, Any] = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")

    event = str(body.get("event") or "").strip()
    payment = body.get("payment")

    if isinstance(payment, dict) and payment.get("id"):
        try:
            upsert_asaas_payment_row(db, payment=payment, last_event=event or None, commit=True)
        except Exception:
            logger.exception("Falha ao persistir webhook Asaas event=%s", event)
            raise HTTPException(status_code=500, detail="Erro ao processar")

    return {"received": True}
