"""Webhook da WhatsApp Cloud API (Meta).

GET  /api/webhooks/whatsapp  -> verificacao (hub.mode / hub.verify_token / hub.challenge)
POST /api/webhooks/whatsapp  -> mensagens; exige X-Hub-Signature-256 valida. Responde 200 rapido e
processa em segundo plano (a Meta reenvia se demorarmos ou falharmos).
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import PlainTextResponse

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.services import whatsapp_bot
from app.services.document_signing import electronic as electronic_signing
from app.services.whatsapp import WhatsAppClient, extract_messages, extract_statuses, summarize, valid_signature, verify_subscription

# Filho do logger do uvicorn: aparece no log do Render tambem em nivel INFO.
logger = logging.getLogger("uvicorn.error.whatsapp")

router = APIRouter(prefix="/api/webhooks", tags=["whatsapp-webhook"])
MAX_BODY_BYTES = 1_000_000
SESSION_FACTORY = SessionLocal


@router.get("/whatsapp")
def whatsapp_verify(request: Request):
    params = request.query_params
    challenge = verify_subscription(params.get("hub.mode"), params.get("hub.verify_token"), params.get("hub.challenge"))
    if challenge is None:
        logger.warning("Webhook WhatsApp: verificacao recusada (token ausente ou diferente).")
        raise HTTPException(status_code=403, detail="Verificação recusada")
    return PlainTextResponse(challenge)


def _store(messages) -> list[int]:
    with SESSION_FACTORY() as db:
        return whatsapp_bot.store_inbound(db, messages)


def _record_statuses(statuses) -> int:
    with SESSION_FACTORY() as db:
        return electronic_signing.record_whatsapp_status_updates(db, statuses)


def process_queue(ids: list[int]) -> None:
    """Roda depois da resposta HTTP. Tambem recolhe mensagens que ficaram pendentes."""
    client = WhatsAppClient()
    with SESSION_FACTORY() as db:
        for row_id in list(ids) + [i for i in whatsapp_bot.pending_ids(db) if i not in ids]:
            whatsapp_bot.process_inbound(db, row_id, client)


@router.post("/whatsapp")
async def whatsapp_receive(request: Request, background_tasks: BackgroundTasks):
    if not (get_settings().whatsapp_app_secret or "").strip():
        logger.warning("Webhook WhatsApp rejeitado: WHATSAPP_APP_SECRET nao configurado.")
        raise HTTPException(status_code=503, detail="Webhook não configurado no servidor")
    raw = await request.body()
    if len(raw) > MAX_BODY_BYTES:
        raise HTTPException(status_code=413, detail="Payload grande demais")
    if not valid_signature(raw, request.headers.get("x-hub-signature-256")):
        logger.warning("Webhook WhatsApp rejeitado: assinatura invalida.")
        raise HTTPException(status_code=401, detail="Assinatura inválida")
    try:
        payload = json.loads(raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="JSON inválido") from None
    messages = extract_messages(payload)
    statuses = extract_statuses(payload)
    ids = await run_in_threadpool(_store, messages) if messages else []
    if statuses:
        await run_in_threadpool(_record_statuses, statuses)
    logger.info("Webhook WhatsApp recebido: %s aceitas=%s novas=%s", summarize(payload), len(messages), len(ids))
    if ids:
        background_tasks.add_task(process_queue, ids)
    return {"status": "ok"}
