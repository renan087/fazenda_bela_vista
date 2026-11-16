"""Assistente SiSFarm no WhatsApp: vinculo seguro do numero e processamento das mensagens recebidas.

Reaproveita o mesmo agente, permissoes e rascunhos com confirmacao da tela web (app.web.routes).
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
import secrets
import threading
from datetime import datetime, timedelta, timezone

from sqlalchemy.exc import IntegrityError

from app.models import Farm, User
from app.models.crop_season import CropSeason
from app.models.whatsapp import WhatsAppInboundMessage, WhatsAppLink
from app.services.stock_assistant import normalized
from app.services.whatsapp import SERVICE_WINDOW_SECONDS, InboundMessage, WhatsAppClient, WhatsAppError, mask_phone

logger = logging.getLogger(__name__)

LINK_CODE_MINUTES = 15
LINK_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
LINK_CODE_RE = re.compile(r"\bSISFARM[\s:-]*([A-Z0-9]{8})\b")
MAX_ATTEMPTS = 3
UNKNOWN_TEXT = ("Olá! Este número ainda não está vinculado a uma conta SiSFarm. Para usar o assistente pelo WhatsApp, "
                "entre no SiSFarm, abra Assistente > WhatsApp e gere o seu código de vínculo.")
NEW_THREAD_WORDS = {"novo", "nova conversa", "/novo", "reiniciar"}
CONFIRM_WORDS = {"confirmar", "confirmo", "confirma"}
DISCARD_WORDS = {"descartar", "descarta", "cancelar"}

_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime | None) -> datetime | None:
    return value.replace(tzinfo=timezone.utc) if value is not None and value.tzinfo is None else value


def _lock_for(wa_id: str) -> threading.Lock:
    """Mensagens do mesmo numero sao processadas uma de cada vez (a conversa e um JSON unico)."""
    with _locks_guard:
        return _locks.setdefault(wa_id, threading.Lock())


def code_hash(code: str) -> str:
    return hashlib.sha256(code.strip().upper().encode()).hexdigest()


# ---------------------------------------------------------------- vinculo (tela web)

def start_link(db, user, farm, season) -> str:
    """Gera um codigo de uso unico (so o hash fica no banco). O numero e comprovado ao enviar o codigo pelo WhatsApp."""
    code = "".join(secrets.choice(LINK_CODE_ALPHABET) for _ in range(8))
    link = db.query(WhatsAppLink).filter_by(user_id=user.id).first()
    if link is None:
        link = WhatsAppLink(user_id=user.id, created_at=_now())
    link.organization_id, link.farm_id, link.season_id = user.organization_id, farm.id, season.id
    link.code_hash, link.code_expires_at = code_hash(code), _now() + timedelta(minutes=LINK_CODE_MINUTES)
    db.add(link)
    db.commit()
    return code


# ---------------------------------------------------------------- webhook -> fila

def store_inbound(db, messages: list[InboundMessage]) -> list[int]:
    """Grava as mensagens novas. O wamid e unico: reentregas da Meta sao ignoradas."""
    ids = []
    for message in messages:
        if db.query(WhatsAppInboundMessage.id).filter_by(wamid=message.wamid).first():
            continue
        row = WhatsAppInboundMessage(wamid=message.wamid, wa_id=message.wa_id, msg_type=message.msg_type,
                                     payload_json=json.dumps(message.raw, ensure_ascii=False)[:20000],
                                     status="received", attempts=0, received_at=_now())
        db.add(row)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            continue
        ids.append(row.id)
    return ids


def pending_ids(db, limit: int = 20) -> list[int]:
    """Mensagens que ficaram para tras (reinicio do servidor ou falha temporaria)."""
    now = _now()
    stuck = (db.query(WhatsAppInboundMessage)
             .filter(WhatsAppInboundMessage.status == "processing",
                     WhatsAppInboundMessage.received_at < now - timedelta(minutes=10),
                     WhatsAppInboundMessage.attempts < MAX_ATTEMPTS).all())
    for row in stuck:
        row.status = "received"
    if stuck:
        db.commit()
    rows = (db.query(WhatsAppInboundMessage.id)
            .filter(WhatsAppInboundMessage.status == "received",
                    WhatsAppInboundMessage.received_at < now - timedelta(minutes=2),
                    WhatsAppInboundMessage.attempts < MAX_ATTEMPTS)
            .order_by(WhatsAppInboundMessage.id).limit(limit).all())
    return [row[0] for row in rows]


def _claim(db, row_id: int) -> WhatsAppInboundMessage | None:
    updated = (db.query(WhatsAppInboundMessage)
               .filter(WhatsAppInboundMessage.id == row_id, WhatsAppInboundMessage.status == "received")
               .update({"status": "processing", "attempts": WhatsAppInboundMessage.attempts + 1},
                       synchronize_session=False))
    db.commit()
    return db.get(WhatsAppInboundMessage, row_id) if updated else None


def process_inbound(db, row_id: int, client: WhatsAppClient) -> str | None:
    """Processa uma mensagem da fila. Nunca levanta: erros ficam registrados na propria linha."""
    row = _claim(db, row_id)
    if row is None:
        return None
    wa_id, wamid = row.wa_id, row.wamid
    with _lock_for(wa_id):
        try:
            status = _handle(db, row, client) or "done"
            error = None
        except Exception as exc:  # noqa: BLE001 - registrar e seguir com a fila
            db.rollback()
            logger.exception("WhatsApp: falha ao processar mensagem %s de %s", wamid, mask_phone(wa_id))
            status, error = "failed", f"{type(exc).__name__}: {exc}"[:300]
            try:
                client.send_text(wa_id, "Tive um problema para processar sua mensagem. Pode tentar de novo em instantes?")
            except WhatsAppError:
                pass
        row = db.get(WhatsAppInboundMessage, row_id)
        row.status, row.error, row.processed_at = status, error, _now()
        db.commit()
        return status


# ---------------------------------------------------------------- regras da conversa

def _within_window(message: dict) -> bool:
    """Respostas livres so dentro da janela de atendimento de 24 h aberta pela mensagem do usuario."""
    try:
        sent_at = datetime.fromtimestamp(int(message.get("timestamp") or 0), tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return False
    return (_now() - sent_at).total_seconds() < SERVICE_WINDOW_SECONDS - 300


def _message_text(message: dict) -> str:
    kind = message.get("type")
    if kind == "text":
        return str((message.get("text") or {}).get("body") or "")
    if kind == "interactive":
        reply = (message.get("interactive") or {}).get("button_reply") or (message.get("interactive") or {}).get("list_reply") or {}
        return str(reply.get("title") or "")
    if kind == "button":
        return str((message.get("button") or {}).get("text") or "")
    if kind in ("image", "document"):
        return str((message.get(kind) or {}).get("caption") or "")
    return ""


def _button_action(message: dict) -> tuple[str, str] | None:
    reply = (message.get("interactive") or {}).get("button_reply") or {}
    match = re.fullmatch(r"sf:(confirmar|descartar):([0-9a-f]{32})", str(reply.get("id") or ""))
    return (match.group(1), match.group(2)) if match else None


def _access_denial(db, routes, user, link) -> str | None:
    if user is None or not user.is_active:
        return "Seu usuário do SiSFarm está inativo. Fale com o administrador da sua conta."
    organization = getattr(user, "organization", None)
    if organization is not None and getattr(organization, "is_active", True) is False:
        return "A conta da sua organização no SiSFarm está inativa."
    farm, season = db.get(Farm, link.farm_id), db.get(CropSeason, link.season_id)
    if not farm or farm.organization_id != user.organization_id or not season or season.farm_id != farm.id \
            or link.organization_id != user.organization_id:
        return "A fazenda ou safra vinculada a este WhatsApp não está mais disponível. Atualize em Assistente > WhatsApp no SiSFarm."
    if not routes._can_use_stock_assistant(db, user):
        return "Seu usuário não tem permissão para usar o assistente (ou o plano está em modo somente leitura)."
    return None


def _draft_summary(draft: dict) -> str:
    lines = [f"*{draft.get('item')}*" + (" (novo cadastro)" if draft.get("is_new") else "")]
    if draft.get("kind") == "compra":
        lines += [f"Compra: {draft.get('packs_label')} = {draft.get('quantity')} {draft.get('unit')}",
                  f"Total: {draft.get('total')} ({draft.get('unit_price')} cada)",
                  f"Pagamento: {draft.get('payment')}", draft.get("finance") or ""]
        if draft.get("supplier"):
            lines.append(f"Fornecedor: {draft['supplier']}")
        if draft.get("lot"):
            lines.append(draft["lot"])
        if draft.get("minimum"):
            lines.append(f"Estoque mínimo: {draft['minimum']}")
    else:
        lines += [f"Saída: {draft.get('quantity')} {draft.get('unit')}", f"Setor: {draft.get('plot')}"]
    lines.append(f"Data: {draft.get('date_label')}")
    lines.append(f"Estoque: {draft.get('balance')} → {draft.get('after')} {draft.get('unit')}")
    if draft.get("notes"):
        lines.append(f"Obs.: {draft['notes']}")
    return "\n".join(line for line in lines if line)


def send_assistant_reply(client: WhatsAppClient, wa_id: str, item: dict) -> None:
    draft = item.get("draft")
    if draft and draft.get("id"):
        body = f"{item.get('content') or 'Confere o lançamento?'}\n\n{_draft_summary(draft)}"
        client.send_buttons(wa_id, body, [(f"sf:confirmar:{draft['id']}", "Confirmar"),
                                          (f"sf:descartar:{draft['id']}", "Descartar")])
        return
    client.send_text(wa_id, item.get("content") or "Pode me contar um pouco mais?")


def _verify_link(db, row, client, code: str) -> str:
    link = db.query(WhatsAppLink).filter_by(code_hash=code_hash(code)).first()
    if link is None or not link.code_expires_at or _aware(link.code_expires_at) < _now():
        client.send_text(row.wa_id, "Esse código é inválido ou expirou. Gere um novo no SiSFarm em Assistente > WhatsApp.")
        return "link_failed"
    for other in db.query(WhatsAppLink).filter(WhatsAppLink.wa_id == row.wa_id, WhatsAppLink.id != link.id).all():
        other.wa_id, other.verified_at, other.thread_id = None, None, None  # o numero passa a ser so deste usuario
    db.flush()
    now = _now()
    link.wa_id, link.verified_at, link.last_inbound_at = row.wa_id, now, now
    link.code_hash, link.code_expires_at, link.thread_id = None, None, None
    db.commit()
    user, farm, season = db.get(User, link.user_id), db.get(Farm, link.farm_id), db.get(CropSeason, link.season_id)
    first_name = (user.name or "").split(" ")[0] if user else ""
    client.send_text(row.wa_id, (
        f"Pronto{', ' + first_name if first_name else ''}! Este WhatsApp está vinculado ao SiSFarm "
        f"(fazenda {farm.name if farm else '-'}, safra {season.name if season else '-'}).\n\n"
        "Pode me mandar, por exemplo: \"usei 20 litros de diesel hoje\", \"comprei 10 sacos de adubo\" ou "
        "\"quanto choveu este mês?\". Tudo que eu for lançar, mostro antes para você confirmar.\n"
        "Para começar outra conversa, envie *novo*."))
    logger.info("WhatsApp: numero %s vinculado ao usuario %s", mask_phone(row.wa_id), link.user_id)
    return "linked"


def _handle(db, row, client) -> str | None:
    from app.web import routes  # import tardio: routes importa muita coisa

    message = json.loads(row.payload_json or "{}")
    if not _within_window(message):
        logger.info("WhatsApp: mensagem %s fora da janela de 24 h, sem resposta", row.wamid)
        return "expired"
    text = _message_text(message).strip()
    code = LINK_CODE_RE.search(text.upper())
    if code:
        return _verify_link(db, row, client, code.group(1))

    link = db.query(WhatsAppLink).filter(WhatsAppLink.wa_id == row.wa_id, WhatsAppLink.verified_at.isnot(None)).first()
    if link is None:
        recent = (db.query(WhatsAppInboundMessage.id)
                  .filter(WhatsAppInboundMessage.wa_id == row.wa_id, WhatsAppInboundMessage.status == "unknown",
                          WhatsAppInboundMessage.received_at > _now() - timedelta(hours=24)).first())
        if not recent:
            client.send_text(row.wa_id, UNKNOWN_TEXT)
        return "unknown"

    user = db.get(User, link.user_id)
    denial = _access_denial(db, routes, user, link)
    link.last_inbound_at = _now()
    db.commit()
    client.mark_read(row.wamid)
    if denial:
        client.send_text(row.wa_id, denial)
        return "denied"

    farm, season = db.get(Farm, link.farm_id), db.get(CropSeason, link.season_id)
    scope = {"active_farm": farm, "active_season": season, "active_farm_id": farm.id, "active_season_id": season.id}
    repo = routes._repository(db)
    thread = routes._assistant_thread(db, user, scope, link.thread_id) if link.thread_id else None
    key = normalized(text)

    if key in NEW_THREAD_WORDS:
        link.thread_id = None
        db.commit()
        client.send_text(row.wa_id, "Certo, começamos uma conversa nova. Em que posso ajudar?")
        return None

    action = _button_action(message)
    if action is None and thread is not None and (key in CONFIRM_WORDS or key in DISCARD_WORDS):
        pending = next((m["draft"] for m in reversed(routes._thread_messages(thread))
                        if m.get("draft") and routes._assistant_draft_state(db, m["draft"]) == "pending"), None)
        if pending:
            action = ("confirmar" if key in CONFIRM_WORDS else "descartar", pending["id"])
    if action:
        verb, draft_id = action
        draft = routes._assistant_draft(db, user, draft_id)
        if not draft or (draft.farm_id, draft.season_id) != (farm.id, season.id):
            client.send_text(row.wa_id, "Esse lançamento não está mais disponível. Me peça de novo, por favor.")
            return None
        try:
            routes._assistant_resolve_draft(db, user, repo, scope, draft, verb, thread, channel="whatsapp")
        except ValueError as exc:
            db.rollback()
            client.send_text(row.wa_id, str(exc))
            return None
        last = routes._thread_messages(thread)[-1:] if thread is not None else []
        client.send_text(row.wa_id, last[0]["content"] if last else ("Lançamento registrado." if verb == "confirmar"
                                                                      else "Lançamento descartado."))
        return None

    image = attachment_type = None
    if row.msg_type in ("image", "document"):
        try:
            data, mime = client.download_media(str((message.get(row.msg_type) or {}).get("id") or ""))
        except WhatsAppError as exc:
            client.send_text(row.wa_id, str(exc))
            return None
        image = f"data:{mime};base64,{base64.b64encode(data).decode()}"
        attachment_type = "pdf" if mime == "application/pdf" else "image"
    elif row.msg_type not in ("text", "interactive", "button"):
        client.send_text(row.wa_id, "Por enquanto eu entendo mensagens de texto, fotos e PDF. Pode escrever para mim?")
        return None
    text = (text or ("Segue o arquivo." if image else "")).strip()[:600]
    if not text:
        return None
    thread, item = routes._assistant_turn(db, user, repo, scope, thread, text, image=image,
                                          attachment_type=attachment_type, title_prefix="WhatsApp · ")
    link = db.get(WhatsAppLink, link.id)
    link.thread_id = thread.id
    db.commit()
    send_assistant_reply(client, row.wa_id, item)
    return None
