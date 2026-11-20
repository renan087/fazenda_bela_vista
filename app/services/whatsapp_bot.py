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
from types import SimpleNamespace
from urllib.parse import urlsplit

from sqlalchemy.exc import IntegrityError

from app.models import Farm, User
from app.models.crop_season import CropSeason
from app.models.whatsapp import WhatsAppClosingRequest, WhatsAppInboundMessage, WhatsAppLink, WhatsAppSurvey
from app.services.chat_format import to_whatsapp
from app.services.stock_assistant import normalized
from app.services.farm_plan_guard import FarmPlanRestriction, bind_farm_plan_actor
from app.services.whatsapp import (SERVICE_WINDOW_SECONDS, InboundMessage, WhatsAppClient, WhatsAppError, mask_phone,
                                   wa_id_variants)

# Filho do logger do uvicorn: aparece no log do Render tambem em nivel INFO.
logger = logging.getLogger("uvicorn.error.whatsapp")

LINK_CODE_MINUTES = 15
LINK_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
LINK_CODE_RE = re.compile(r"\bSISFARM[\s:-]*([A-Z0-9]{8})\b")
MAX_ATTEMPTS = 3
UNKNOWN_REPLY_INTERVAL = timedelta(hours=1)  # a instrucao de vinculo pode ter falhado na entrega; nao silenciar por 24 h
UNKNOWN_TEXT = ("Olá! Este número ainda não está vinculado a uma conta SiSFarm. Para usar o assistente pelo WhatsApp, "
                "entre no SiSFarm, abra Assistente > WhatsApp e gere o seu código de vínculo.")
NEW_THREAD_WORDS = {"novo", "nova conversa", "novo atendimento", "novo chat", "iniciar nova conversa", "iniciar novo atendimento", "/novo", "reiniciar"}
CLOSE_WORDS = {"encerrar", "encerrar conversa", "encerrar atendimento", "finalizar", "finalizar conversa", "finalizar atendimento", "terminar conversa", "terminar atendimento", "finaliza essa conversa", "finalize esta conversa", "/encerrar"}
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
        # Serializa a entrada com o trabalhador de prazos antes de registrar a mensagem.
        db.query(WhatsAppLink.id).filter(WhatsAppLink.wa_id.in_(wa_id_variants(message.wa_id))).with_for_update().all()
        if db.query(WhatsAppInboundMessage.id).filter_by(wamid=message.wamid).first():
            db.rollback()
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
        logger.info("WhatsApp: mensagem de %s tipo=%s tentativa=%s -> %s", mask_phone(wa_id), row.msg_type, row.attempts, status)
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


def send_assistant_reply(client: WhatsAppClient, wa_id: str, item: dict, *, reply_to: str | None = None) -> None:
    context = {"reply_to": reply_to} if reply_to else {}
    module_proposal = item.get("module_proposal") or {}
    if re.fullmatch(r"[0-9a-f]{32}", str(module_proposal.get("id") or "")):
        proposal_id = module_proposal["id"]
        client.send_buttons(wa_id, to_whatsapp(item.get("content")) or "Confirma o lançamento?",
                            [(f"mod:confirmar:{proposal_id}", "Confirmar"),
                             (f"mod:cancelar:{proposal_id}", "Cancelar")], **context)
        return
    proposal = item.get("reminder_proposal") or {}
    if re.fullmatch(r"[0-9a-f]{32}", str(proposal.get("confirmation_id") or "")):
        confirmation_id = proposal["confirmation_id"]
        client.send_buttons(wa_id, to_whatsapp(item.get("content")) or "Confirma o lembrete?",
                            [(f"rem:confirmar:{confirmation_id}", "Confirmar"),
                             (f"rem:cancelar:{confirmation_id}", "Cancelar")], **context)
        return
    draft = item.get("draft")
    if draft and draft.get("id"):
        body = f"{to_whatsapp(item.get('content')) or 'Confere o lançamento?'}\n\n{_draft_summary(draft)}"
        client.send_buttons(wa_id, body, [(f"sf:confirmar:{draft['id']}", "Confirmar"),
                                          (f"sf:descartar:{draft['id']}", "Descartar")], **context)
        return
    client.send_text(wa_id, to_whatsapp(item.get("content")) or "Pode me contar um pouco mais?", **context)


def _record_link(db, link, reference):
    """Emite somente apos revalidar vinculo, autorizacao e escopo do registro."""
    from app.core.config import get_settings
    from app.services.whatsapp_record_access import RECORD_KINDS, authorized_record, create_token, find_token, link_binding

    kind, record_id = reference.get("kind"), reference.get("id")
    if kind not in RECORD_KINDS or isinstance(record_id, bool) or not isinstance(record_id, int) or record_id < 1:
        return None
    access = SimpleNamespace(kind=kind, record_id=record_id, binding_hash=link_binding(link), link_id=link.id, user_id=link.user_id,
                             organization_id=link.organization_id, farm_id=link.farm_id, season_id=link.season_id)
    if authorized_record(db, access) is None:
        return None
    configured = urlsplit(get_settings().app_public_url.strip())
    if configured.scheme != "https" or not configured.netloc or configured.username or configured.password or configured.query or configured.fragment:
        return None
    token = create_token(db, link=link, kind=kind, record_id=record_id)
    db.flush()
    access_id = find_token(db, token).id
    db.commit()
    return f"{configured.scheme}://{configured.netloc}/acesso-whatsapp/registro#{token}", access_id


def _record_choice_text(references):
    labels = {"compra": "Compra", "consumo": "Consumo", "aplicacao": "Aplicação", "colheita": "Colheita"}
    lines = ["Qual lançamento você quer consultar? Responda com o número:"]
    for index, ref in enumerate(references, 1):
        parts = [labels.get(ref.get("kind"), "Registro"), str(ref.get("label") or ""), str(ref.get("date") or "")]
        lines.append(f"{index}. " + " · ".join(part for part in parts if part))
    if len(references) >= 15:
        lines.append("Mostrei até 15 registros; pode haver outros.")
    return "\n".join(lines)


def _link_code_candidate(text: str) -> tuple[str | None, bool]:
    """Codigo de vinculo na mensagem, tolerando espacos, hifens, pontuacao e o codigo enviado sozinho.

    Devolve (codigo, citou_sisfarm). Codigo sem a palavra SISFARM so vale se existir um vinculo pendente com ele.
    """
    upper = (text or "").upper()
    match = LINK_CODE_RE.search(upper)
    if match:
        return match.group(1), True
    compact = re.sub(r"[^A-Z0-9]", "", upper)
    explicit = "SISFARM" in compact
    rest = compact.replace("SISFARM", "", 1)
    if len(rest) == 8 and all(char in LINK_CODE_ALPHABET for char in rest):
        return rest, explicit
    return None, explicit


def _pending_code_link(db, code: str) -> WhatsAppLink | None:
    link = db.query(WhatsAppLink).filter_by(code_hash=code_hash(code)).first()
    if link is None or not link.code_expires_at or _aware(link.code_expires_at) < _now():
        return None
    return link


def _verify_link(db, row, client, code: str) -> str:
    link = _pending_code_link(db, code)
    if link is None:
        client.send_text(row.wa_id, "Esse código é inválido ou expirou. Gere um novo no SiSFarm em Assistente > WhatsApp.")
        return "link_failed"
    for other in db.query(WhatsAppLink).filter(WhatsAppLink.wa_id.in_(wa_id_variants(row.wa_id)), WhatsAppLink.id != link.id).all():
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


def _find_link(db, wa_id: str) -> WhatsAppLink | None:
    """Vinculo verificado do numero, aceitando o celular brasileiro com ou sem o nono digito."""
    links = (db.query(WhatsAppLink)
             .filter(WhatsAppLink.wa_id.in_(wa_id_variants(wa_id)), WhatsAppLink.verified_at.isnot(None)).all())
    link = next((item for item in links if item.wa_id == wa_id), links[0] if links else None)
    if link is not None and link.wa_id != wa_id:
        logger.info("WhatsApp: vinculo de %s atualizado para o formato recebido da Meta", mask_phone(wa_id))
        link.wa_id = wa_id  # responde sempre no formato que a Meta acabou de usar
        db.commit()
    return link


def _handle(db, row, client) -> str | None:
    from app.web import routes  # import tardio: routes importa muita coisa

    message = json.loads(row.payload_json or "{}")
    if not _within_window(message):
        logger.info("WhatsApp: mensagem %s fora da janela de 24 h, sem resposta", row.wamid)
        return "expired"
    text = _message_text(message).strip()
    code, explicit = _link_code_candidate(text)
    if code and (explicit or _pending_code_link(db, code) is not None):
        return _verify_link(db, row, client, code)

    link = _find_link(db, row.wa_id)
    if link is None and explicit:
        client.send_text(row.wa_id, "Não reconheci o código. Envie exatamente como aparece no SiSFarm, por exemplo: "
                                    "SISFARM ABCD2345 (o código tem 8 letras e números).")
        return "link_failed"
    if link is None:
        recent = (db.query(WhatsAppInboundMessage.id)
                  .filter(WhatsAppInboundMessage.wa_id.in_(wa_id_variants(row.wa_id)), WhatsAppInboundMessage.id != row.id,
                          WhatsAppInboundMessage.status == "unknown",
                          WhatsAppInboundMessage.received_at > _now() - UNKNOWN_REPLY_INTERVAL).first())
        if recent:
            logger.info("WhatsApp: %s sem vinculo; instrucao ja enviada na ultima hora", mask_phone(row.wa_id))
            return "unknown_silenced"
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

    bind_farm_plan_actor(db, user)
    farm, season = db.get(Farm, link.farm_id), db.get(CropSeason, link.season_id)
    scope = {"active_farm": farm, "active_season": season, "active_farm_id": farm.id, "active_season_id": season.id}
    repo = routes._repository(db)
    thread = routes._assistant_thread(db, user, scope, link.thread_id) if link.thread_id else None
    key = normalized(text)

    button_reply = (message.get("interactive") or {}).get("button_reply") or {}
    survey_button = re.fullmatch(r"survey:(good|fair|bad):([0-9a-f]{32})", str(button_reply.get("id") or ""))
    if survey_button:
        survey = db.get(WhatsAppSurvey, survey_button.group(2))
        from app.core.config import get_settings
        minutes = max(1, get_settings().whatsapp_survey_minutes)
        if (survey is None or survey.link_id != link.id or survey.status != "pending"
                or _aware(survey.created_at) + timedelta(minutes=minutes) <= _aware(row.received_at)):
            if survey and survey.link_id == link.id and survey.status == "pending":
                survey.status, survey.resolved_at = "unrated", _now()
                db.commit()
            client.send_text(row.wa_id, "Essa pesquisa não está mais disponível. Se precisar de ajuda, envie uma nova mensagem.")
            return "survey_stale"
        survey.status, survey.rating, survey.resolved_at = "answered", survey_button.group(1), _now()
        db.commit()
        client.send_text(row.wa_id, "Obrigado pela avaliação! Sua opinião nos ajuda a melhorar.")
        return "survey_answered"
    closing_button = re.fullmatch(r"close:(yes|no):([0-9a-f]{32})", str(button_reply.get("id") or ""))
    pending_close = (db.query(WhatsAppClosingRequest).filter_by(link_id=link.id, status="pending")
                     .order_by(WhatsAppClosingRequest.created_at.desc()).first())
    if closing_button:
        if not pending_close or closing_button.group(2) != pending_close.id or pending_close.thread_id != link.thread_id:
            client.send_text(row.wa_id, "Essa confirmação de encerramento não está mais disponível.")
            return "close_stale"
        key = "sim" if closing_button.group(1) == "yes" else "nao"
    if pending_close:
        if pending_close.thread_id != link.thread_id:
            pending_close.status, pending_close.resolved_at = "superseded", _now()
            db.commit()
        elif key in {"nao", "pode encerrar", "nao preciso"}:
            pending_close.status, pending_close.resolved_at = "closed", _now()
            link.thread_id = None
            db.commit()
            client.send_text(row.wa_id, "Atendimento encerrado. Quando precisar, envie uma mensagem para iniciar uma nova conversa.")
            return "closed"
        elif key in {"sim", "continuar", "nao encerrar"}:
            pending_close.status, pending_close.resolved_at = "continued", _now()
            db.commit()
            client.send_text(row.wa_id, "Claro! Em que mais posso ajudar?")
            return "continued"
        else:
            # Uma nova dúvida mantém a conversa e segue o processamento normal.
            pending_close.status, pending_close.resolved_at = "continued", _now()
            db.commit()
    if row.msg_type == "text" and key in CLOSE_WORDS | NEW_THREAD_WORDS:
        request = WhatsAppClosingRequest(id=secrets.token_hex(16), link_id=link.id, thread_id=link.thread_id,
                                         status="pending", created_at=_now())
        db.add(request)
        db.flush()
        client.send_buttons(row.wa_id,
            "Antes de encerrarmos, posso ajudar em mais alguma coisa? Lançamentos ainda não confirmados não serão realizados.",
            [(f"close:yes:{request.id}", "Sim, continuar"), (f"close:no:{request.id}", "Não, encerrar")])
        db.commit()
        return "close_pending"
    module_button = re.fullmatch(r"mod:(confirmar|cancelar):([0-9a-f]{32})", str(button_reply.get("id") or ""))
    if module_button:
        entries = routes._thread_messages(thread) if thread else []
        proposal = (entries[-1].get("module_proposal") or {}) if entries and entries[-1].get("role") == "assistant" else {}
        if proposal.get("id") != module_button.group(2):
            client.send_text(row.wa_id, "Essa confirmação não está mais disponível. Solicite o lançamento novamente.")
            return None
        text = key = module_button.group(1)
    reminder_button = re.fullmatch(r"rem:(confirmar|cancelar):([0-9a-f]{32})", str(button_reply.get("id") or ""))
    if reminder_button:
        entries = routes._thread_messages(thread) if thread else []
        proposal = (entries[-1].get("reminder_proposal") or {}) if entries and entries[-1].get("role") == "assistant" else {}
        if proposal.get("confirmation_id") != reminder_button.group(2):
            client.send_text(row.wa_id, "Essa confirmação não está mais disponível. Peça novamente a alteração do lembrete.")
            return None
        text = key = reminder_button.group(1)

    action = _button_action(message)
    recent = routes._thread_messages(thread) if thread is not None else []
    reminder_pending = bool(recent and recent[-1].get("role") == "assistant" and (recent[-1].get("reminder_proposal") or recent[-1].get("module_proposal")))
    if action is None and not reminder_pending and thread is not None and (key in CONFIRM_WORDS or key in DISCARD_WORDS):
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
        except (ValueError, FarmPlanRestriction) as exc:
            db.rollback()
            client.send_text(row.wa_id, exc.detail if isinstance(exc, FarmPlanRestriction) else str(exc))
            return None
        last = routes._thread_messages(thread)[-1:] if thread is not None else []
        client.send_text(row.wa_id, last[0]["content"] if last else ("Lançamento registrado." if verb == "confirmar"
                                                                      else "Lançamento descartado."))
        return None

    history = routes._thread_messages(thread) if thread else []
    pending_choices = history[-1].get("record_references") if history and history[-1].get("wa_record_selection") else None
    choice = re.fullmatch(r"(?:registro\s*)?(\d{1,2})", key) if row.msg_type == "text" and pending_choices else None
    if choice:
        index = int(choice.group(1)) - 1
        if index < 0 or index >= len(pending_choices):
            client.send_text(row.wa_id, "Esse número não está na lista. " + _record_choice_text(pending_choices))
            return None
        issued = _record_link(db, link, pending_choices[index])
        response = ("Aqui está o link para consultar o lançamento. Ele pode ser compartilhado e vale por até 48 horas:\n" + issued[0]
                    if issued else "Não foi possível acessar esse lançamento. Verifique suas permissões ou faça uma nova consulta.")
        # O token nao e salvo no historico persistente da conversa.
        routes._save_thread(db, thread, history + [
            {"role": "user", "content": text, "at": routes._assistant_now_iso()},
            {"role": "assistant", "content": "Link de consulta enviado." if issued else response,
             **({"record_access_id": issued[1], "record_references": [pending_choices[index]]} if issued else {}),
             "at": routes._assistant_now_iso()},
        ])
        client.send_text(row.wa_id, response)
        return None

    image = attachment_type = None
    audio_record = None
    attachment_record = None
    if row.msg_type == "audio":
        from app.models.assistant_audio import AssistantAudio
        from app.services.assistant_audio import AUDIO_RETENTION_DAYS, transcribe_audio
        from uuid import uuid4
        try:
            data, mime = client.download_media(str((message.get("audio") or {}).get("id") or ""))
            text = transcribe_audio(data, mime)
        except (WhatsAppError, ValueError) as exc:
            client.send_text(row.wa_id, str(exc), reply_to=row.wamid)
            return None
        audio_record = AssistantAudio(id=uuid4().hex, user_id=user.id, organization_id=user.organization_id,
                                      farm_id=farm.id, season_id=season.id, mime=mime, data=data,
                                      expires_at=_now() + timedelta(days=AUDIO_RETENTION_DAYS))
    if row.msg_type in ("image", "document"):
        try:
            data, mime = client.download_media(str((message.get(row.msg_type) or {}).get("id") or ""))
        except WhatsAppError as exc:
            client.send_text(row.wa_id, str(exc))
            return None
        image = f"data:{mime};base64,{base64.b64encode(data).decode()}"
        attachment_type = "pdf" if mime == "application/pdf" else "image"
        try:
            filename = (message.get(row.msg_type) or {}).get("filename")
            attachment_record = routes._new_assistant_attachment(user, farm, season, data, mime, filename)
        except ValueError as exc:
            client.send_text(row.wa_id, str(exc))
            return None
    elif row.msg_type not in ("text", "interactive", "button", "audio"):
        client.send_text(row.wa_id, "Por enquanto eu entendo mensagens de texto, fotos e PDF. Pode escrever para mim?")
        return None
    text = (text or ("Segue o arquivo." if image else "")).strip()[:600]
    if not text:
        return None
    try:
        thread, item = routes._assistant_turn(db, user, repo, scope, thread, text, image=image,
                                              attachment_type=attachment_type, title_prefix="WhatsApp · ",
                                              **({"audio": audio_record} if audio_record else {}),
                                              **({"attachment": attachment_record} if attachment_record else {}))
    except FarmPlanRestriction as exc:
        db.rollback()
        client.send_text(row.wa_id, exc.detail)
        return None
    link = db.get(WhatsAppLink, link.id)
    link.thread_id = thread.id
    db.commit()
    if item.get("request_chat_closure"):
        request = WhatsAppClosingRequest(id=secrets.token_hex(16), link_id=link.id, thread_id=thread.id,
                                         status="pending", created_at=_now())
        db.add(request)
        db.flush()
        client.send_buttons(row.wa_id,
            "Antes de encerrarmos, posso ajudar em mais alguma coisa? Lançamentos ainda não confirmados não serão realizados.",
            [(f"close:yes:{request.id}", "Sim, continuar"), (f"close:no:{request.id}", "Não, encerrar")])
        db.commit()
        return "close_pending"
    response_item = dict(item)
    references = item.get("record_references") or []
    if item.get("_shared_url"):
        response_item["content"] = (item.get("content") or "") + "\n\n" + item["_shared_url"]
    elif references and not item.get("draft") and not item.get("error"):
        if len(references) == 1:
            issued = _record_link(db, link, references[0])
            if issued:
                history = routes._thread_messages(thread)
                if history:
                    history[-1]["record_access_id"] = issued[1]
                    routes._save_thread(db, thread, history)
                response_item["content"] = (item.get("content") or "") + "\n\nConsultar lançamento (link compartilhável, válido por até 48 horas):\n" + issued[0]
        else:
            history = routes._thread_messages(thread)
            if history:
                history[-1]["wa_record_selection"] = True
                routes._save_thread(db, thread, history)
            response_item["content"] = (item.get("content") or "") + "\n\n" + _record_choice_text(references)
    send_assistant_reply(client, row.wa_id, response_item, **({"reply_to": row.wamid} if row.msg_type == "audio" else {}))
    return None
