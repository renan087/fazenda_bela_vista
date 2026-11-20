"""Prazos de atendimento do WhatsApp; chamadas externas executam fora do event loop."""
import asyncio
import logging
import secrets
from datetime import timedelta

from starlette.concurrency import run_in_threadpool

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.models.whatsapp import WhatsAppClosingRequest, WhatsAppInboundMessage, WhatsAppLink, WhatsAppSurvey
from app.services.whatsapp import SERVICE_WINDOW_SECONDS, WhatsAppClient, is_configured, wa_id_variants
from app.services.whatsapp_bot import _aware, _now

logger = logging.getLogger("uvicorn.error.whatsapp")


def _window(link, now):
    last = _aware(link.last_inbound_at)
    return bool(last and (now - last).total_seconds() < SERVICE_WINDOW_SECONDS - 300)


def _queued(db, link):
    return db.query(WhatsAppInboundMessage.id).filter(
        WhatsAppInboundMessage.wa_id.in_(wa_id_variants(link.wa_id)),
        WhatsAppInboundMessage.status.in_(["received", "processing"])).first() is not None


def _lock_link(db, link_id):
    return db.query(WhatsAppLink).filter_by(id=link_id).with_for_update(skip_locked=True).first()


def process_lifecycle(db, client, now=None):
    now = now or _now()
    settings = get_settings()
    idle = timedelta(minutes=max(1, settings.whatsapp_idle_minutes))
    closing = timedelta(minutes=max(1, settings.whatsapp_closing_minutes))
    survey_time = timedelta(minutes=max(1, settings.whatsapp_survey_minutes))
    # Only a conditional update can claim an outbound action across workers.
    blocked = db.query(WhatsAppClosingRequest.id).filter(
        WhatsAppClosingRequest.link_id == WhatsAppLink.id,
        WhatsAppClosingRequest.thread_id == WhatsAppLink.thread_id,
        WhatsAppClosingRequest.status.in_(["pending", "sending", "send_failed"]),
        WhatsAppClosingRequest.created_at >= WhatsAppLink.last_inbound_at).exists()
    ids = [x[0] for x in db.query(WhatsAppLink.id).filter(
        WhatsAppLink.verified_at.isnot(None), WhatsAppLink.wa_id.isnot(None),
        WhatsAppLink.thread_id.isnot(None), WhatsAppLink.last_inbound_at <= now - idle,
        WhatsAppLink.last_inbound_at > now - timedelta(seconds=SERVICE_WINDOW_SECONDS - 300),
        ~blocked).limit(200).all()]
    for link_id in ids:
        link = _lock_link(db, link_id)
        if not link or not link.thread_id or not link.last_inbound_at or _aware(link.last_inbound_at) > now - idle or _queued(db, link):
            db.rollback()
            continue
        pending = db.query(WhatsAppClosingRequest).filter_by(link_id=link.id, status="pending").first()
        if pending:
            db.rollback()
            continue
        # A failed or interrupted send is not retried automatically: Meta may have accepted it.
        attempted = db.query(WhatsAppClosingRequest).filter_by(link_id=link.id, thread_id=link.thread_id).filter(
            WhatsAppClosingRequest.status.in_(["sending", "send_failed"]), WhatsAppClosingRequest.created_at >= link.last_inbound_at).first()
        if attempted:
            db.rollback()
            continue
        if not _window(link, now):
            db.rollback()
            continue
        req = WhatsAppClosingRequest(id=secrets.token_hex(16), link_id=link.id, thread_id=link.thread_id,
                                     status="sending", created_at=now)
        db.add(req)
        db.commit()
        try:
            client.send_buttons(link.wa_id, "Estamos encerrando o atendimento por inatividade. Posso ajudar em mais alguma coisa? Sem resposta, encerraremos em alguns minutos. Lançamentos não confirmados não serão realizados.",
                                [(f"close:yes:{req.id}", "Sim, continuar"), (f"close:no:{req.id}", "Não, encerrar")])
            req.status = "pending"
        except Exception:
            logger.exception("WhatsApp: falha no aviso de encerramento")
            req.status = "send_failed"
        db.commit()
    ids = [x[0] for x in db.query(WhatsAppClosingRequest.id).filter_by(status="pending").filter(
        WhatsAppClosingRequest.created_at <= now - closing).limit(200).all()]
    for request_id in ids:
        req = db.get(WhatsAppClosingRequest, request_id)
        link = _lock_link(db, req.link_id) if req else None
        if not link:
            db.rollback()
            continue
        db.refresh(req)
        if req.status != "pending" or _queued(db, link):
            db.rollback()
            continue
        if link.thread_id != req.thread_id or (_aware(link.last_inbound_at) and _aware(link.last_inbound_at) > _aware(req.created_at)):
            req.status, req.resolved_at = "continued", now
        else:
            req.status, req.resolved_at, link.thread_id = "closed", now, None
        db.commit()
    ids = [x[0] for x in db.query(WhatsAppClosingRequest.id).filter_by(status="closed").outerjoin(
        WhatsAppSurvey, WhatsAppSurvey.closing_id == WhatsAppClosingRequest.id).filter(WhatsAppSurvey.id.is_(None)).limit(200).all()]
    for request_id in ids:
        req = db.get(WhatsAppClosingRequest, request_id)
        link = _lock_link(db, req.link_id) if req else None
        if not link or db.query(WhatsAppSurvey.id).filter_by(closing_id=request_id).first():
            db.rollback()
            continue
        # Do not interrupt a new conversation or send outside the service window.
        available = bool(link.verified_at and link.wa_id and link.thread_id is None and req.resolved_at
                         and now - _aware(req.resolved_at) <= timedelta(minutes=5)
                         and not _queued(db, link) and _window(link, now))
        survey = WhatsAppSurvey(id=secrets.token_hex(16), closing_id=req.id, link_id=link.id,
                                status="sending" if available else "unavailable", created_at=now,
                                resolved_at=None if available else now)
        db.add(survey)
        db.commit()
        if not available:
            continue
        try:
            client.send_buttons(link.wa_id, "Como foi sua experiência?",
                                [(f"survey:good:{survey.id}", "Bom"), (f"survey:fair:{survey.id}", "Regular"), (f"survey:bad:{survey.id}", "Ruim")])
            survey.status = "pending"
        except Exception:
            logger.exception("WhatsApp: falha ao enviar pesquisa")
            survey.status, survey.resolved_at = "unavailable", now
        db.commit()
    expired = [x[0] for x in db.query(WhatsAppSurvey.id).filter(
        WhatsAppSurvey.status == "pending", WhatsAppSurvey.created_at <= now - survey_time).limit(200).all()]
    for survey_id in expired:
        survey = db.get(WhatsAppSurvey, survey_id)
        link = _lock_link(db, survey.link_id) if survey else None
        if not link or _queued(db, link):
            db.rollback()
            continue
        db.refresh(survey)
        if survey.status == "pending":
            survey.status, survey.resolved_at = "unrated", now
        db.commit()
    # An interrupted outbound call has an unknown delivery outcome; never repeat it blindly.
    db.query(WhatsAppSurvey).filter(WhatsAppSurvey.status == "sending", WhatsAppSurvey.created_at <= now - timedelta(minutes=5)).update(
        {"status": "unavailable", "resolved_at": now}, synchronize_session=False)
    db.commit()


def _run_once():
    if not is_configured():
        return
    with SessionLocal() as db:
        process_lifecycle(db, WhatsAppClient())


async def run_whatsapp_lifecycle_loop():
    while True:
        try:
            await run_in_threadpool(_run_once)
        except Exception:
            logger.exception("WhatsApp: falha na rotina de atendimento")
        await asyncio.sleep(60)
