"""Authenticated web entry points for signing, validation and certificate purposes."""
import base64
import binascii
import json
import logging
import re
import secrets
from datetime import datetime, timezone
from urllib.parse import quote, unquote

from cryptography.exceptions import InvalidTag
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from app.core.csrf import ensure_csrf_token, validate_csrf
from app.core.deps import get_current_user_web
from app.db.session import get_db
from app.models import Farm, User
from app.models.digital_certificate import CertificateHolderAssignment, CertificatePurpose, DigitalCertificate
from app.models.signed_document import SigningDocument, SigningParticipant, SigningRevision
from app.models.whatsapp import WhatsAppLink
from app.services import digital_certificates as certificates, login_throttle
from app.services.audit_log_service import client_ip
from app.services.document_signing import workflow
from app.services.document_signing.pdf import MAX_UPLOAD_BYTES, read_pdf, signing_options, validate_pdf
from app.services.document_signing.policy import signing_mode
from app.services.email_service import send_plain_email
from app.services.nfe_permissions import has_fiscal_permission
from app.services.whatsapp import WhatsAppClient, WhatsAppError, is_configured as whatsapp_is_configured
from app.web.routes import _base_context, _repository, templates

logger = logging.getLogger(__name__)
router = APIRouter()
PRIVATE = {"Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff"}
SESSION_NONCE = "document_signing_session"


def _farm(request, db, user, *, required=True):
    farm_id = request.session.get("active_farm_id")
    farm = db.scalar(select(Farm).filter_by(id=farm_id, organization_id=user.organization_id)) if (
        isinstance(farm_id, int) and user.organization_id is not None
    ) else None
    if farm is None and required:
        raise HTTPException(403, "Selecione uma fazenda da sua organização.")
    return farm


def _page(request, user, db, name, template, title, **kwargs):
    return templates.TemplateResponse(request, template, _base_context(
        request, user, ensure_csrf_token(request), name, _repo=_repository(db), title=title, **kwargs,
    ), headers=PRIVATE)


def _json(data, status=200):
    return JSONResponse(data, status_code=status, headers=PRIVATE)


def _csrf(request):
    validate_csrf(request, request.headers.get("X-CSRF-Token"))


async def _body(request, limit, expected_type):
    _csrf(request)
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != expected_type:
        raise HTTPException(415, "Formato de envio inválido.")
    size = request.headers.get("content-length")
    if size and (not size.isdecimal() or len(size) > 10 or int(size) > limit):
        raise HTTPException(413, "O arquivo excede o limite de tamanho.")
    data = bytearray()
    async for chunk in request.stream():
        if len(data) + len(chunk) > limit:
            raise HTTPException(413, "O arquivo excede o limite de tamanho.")
        data.extend(chunk)
    return bytes(data)


async def _payload(request, limit=8192):
    try:
        data = json.loads(await _body(request, limit, "application/json"))
    except (ValueError, RecursionError) as exc:
        raise HTTPException(400, "Dados de envio inválidos.") from exc
    if not isinstance(data, dict):
        raise HTTPException(400, "Dados de envio inválidos.")
    return data


def _error(exc):
    if isinstance(exc, workflow.WorkflowError):
        return _json({"detail": str(exc)}, exc.status)
    if isinstance(exc, PermissionError):
        return _json({"detail": str(exc)}, 403)
    if isinstance(exc, certificates.EncryptionConfigurationError):
        return _json({"detail": str(exc)}, 503)
    if isinstance(exc, InvalidTag):
        return _json({"detail": "Não foi possível abrir o material protegido."}, 503)
    if isinstance(exc, IntegrityError):
        return _json({"detail": "Houve uma alteração simultânea. Atualize a página e tente novamente."}, 409)
    return _json({"detail": str(exc)}, 422)


def _limited(fn, *args, **kwargs):
    if not workflow.CRYPTO_SLOTS.acquire(blocking=False):
        raise workflow.WorkflowError("O serviço está processando outros PDFs. Tente novamente em instantes.", 503)
    try:
        return fn(*args, **kwargs)
    finally:
        workflow.CRYPTO_SLOTS.release()


def _doc(db, user, farm, document_id, **kwargs):
    try:
        return workflow.accessible_document(db, user, farm.id, document_id, **kwargs)
    except workflow.WorkflowError as exc:
        raise HTTPException(exc.status, str(exc)) from exc


def _document_summaries(db, user, farm):
    if not farm:
        return [], {"total": 0, "your_turn": 0, "waiting": 0, "done": 0, "attention": 0}, None
    docs = db.scalars(
        workflow.document_query(db, user, farm.id)
        .order_by(SigningDocument.updated_at.desc(), SigningDocument.id)
        .limit(100)
    ).all()
    if not docs:
        return [], {"total": 0, "your_turn": 0, "waiting": 0, "done": 0, "attention": 0}, None

    doc_ids = [d.id for d in docs]
    part_map: dict[str, list[int]] = {doc_id: [] for doc_id in doc_ids}
    for doc_id, uid in db.execute(
        select(SigningParticipant.document_id, SigningParticipant.user_id).where(
            SigningParticipant.document_id.in_(doc_ids)
        )
    ).all():
        part_map.setdefault(doc_id, []).append(uid)

    signed_by_map: dict[str, set[int]] = {doc_id: set() for doc_id in doc_ids}
    for doc_id, uid, sig in db.execute(
        select(SigningRevision.document_id, SigningRevision.user_id, SigningRevision.signature).where(
            SigningRevision.document_id.in_(doc_ids), SigningRevision.signature.is_not(None)
        )
    ).all():
        if uid is not None and sig:
            signed_by_map.setdefault(doc_id, set()).add(uid)

    summaries = []
    counts = {"total": len(docs), "your_turn": 0, "waiting": 0, "done": 0, "attention": 0}
    next_to_sign = None

    for doc in docs:
        validation = doc.validation or {}
        val_state = validation.get("state") or "unsigned"
        sig_count = len(validation.get("signatures") or [])
        participants = part_map.get(doc.id, [])
        signed_users = signed_by_map.get(doc.id, set())
        owner_must_sign = bool(getattr(doc, "owner_signs", True))
        expected_list = ([doc.owner_id] if owner_must_sign else []) + participants
        expected_set = {uid for uid in expected_list if uid is not None}
        pending_count = len(expected_set - signed_users)
        expected_count = max(sig_count + pending_count, len(expected_set), 1)
        signed_by_me = user.id in signed_users
        user_must_sign = (user.id in participants) or (doc.owner_id == user.id and owner_must_sign)

        if val_state == "invalid":
            status_key, status_label, status_tone = "attention", "Com alerta", "err"
            counts["attention"] += 1
        elif not signed_by_me and user_must_sign:
            status_key, status_label, status_tone = "your_turn", "Sua vez de assinar", "warn"
            counts["your_turn"] += 1
        elif pending_count > 0:
            status_key, status_label, status_tone = "waiting", "Aguardando outros", "info"
            counts["waiting"] += 1
        else:
            status_key, status_label, status_tone = "done", "Concluído", "done"
            counts["done"] += 1

        display_title = doc.title or (doc.filename[:-4] if doc.filename.lower().endswith(".pdf") else doc.filename)
        item = {
            "doc": doc,
            "id": doc.id,
            "title": display_title,
            "filename": doc.filename,
            "category": doc.category or "Outros",
            "shared": doc.owner_id != user.id,
            "sig_count": sig_count,
            "expected_count": expected_count,
            "progress_pct": min(100, round((sig_count / max(expected_count, 1)) * 100)),
            "status_key": status_key,
            "status_label": status_label,
            "status_tone": status_tone,
            "validation_state": val_state,
            "can_delete": bool(doc.owner_id == user.id),
            "updated_at": doc.updated_at,
        }
        summaries.append(item)
        if status_key == "your_turn" and next_to_sign is None:
            next_to_sign = item

    return summaries, counts, next_to_sign


def _certificate_inventory(db, user, farm):
    rows = []
    organization_users = []
    expiring_certs = []
    if not farm:
        return rows, organization_users, expiring_certs
    manage = has_fiscal_permission(user, "nfe.certificate.manage")
    inventory = select(DigitalCertificate).where(
        DigitalCertificate.organization_id == user.organization_id,
        DigitalCertificate.farm_id == farm.id,
        DigitalCertificate.deleted_at.is_(None),
    )
    if manage:
        organization_users = db.scalars(
            select(User).filter_by(organization_id=user.organization_id, is_active=True).order_by(User.name, User.id)
        ).all()
    else:
        relevant = select(CertificatePurpose.certificate_id).filter_by(user_id=user.id, identity_email=user.email)
        assigned = select(CertificateHolderAssignment.certificate_id).filter_by(user_id=user.id, email=user.email)
        inventory = inventory.where(
            or_(DigitalCertificate.uploaded_by == user.id, DigitalCertificate.id.in_(relevant), DigitalCertificate.id.in_(assigned))
        )
    active_fiscal = {
        cert.id for cert in certificates.fiscal_certificate_query(db, user.organization_id, farm.id, active=True)
    }
    user_lookup = {
        u.id: u for u in db.scalars(select(User).filter_by(organization_id=user.organization_id)).all()
    }
    now = datetime.now(timezone.utc)
    for cert in db.scalars(inventory.order_by(DigitalCertificate.created_at.desc())).all():
        docs = certificates.purpose_for_user(db, cert, user, certificates.DOCUMENTS, farm_id=farm.id, enabled=False)
        fiscal = certificates.purpose_for_user(db, cert, user, certificates.FISCAL, farm_id=farm.id, enabled=False)
        assigned = certificates.assigned_to_user(db, cert, user)
        assignment = db.get(CertificateHolderAssignment, cert.id)
        existing = db.get(CertificatePurpose, (cert.id, certificates.DOCUMENTS))
        holder_id = assignment.user_id if assignment else existing.user_id if existing else None
        holder_user = user_lookup.get(holder_id) if holder_id else None
        valid_until = certificates.utc(cert.valid_until)
        valid_from = certificates.utc(cert.valid_from)
        is_valid = valid_from <= now <= valid_until
        days_left = max(0, (valid_until - now).days) if is_valid else 0
        info = certificates.friendly_cert_info(cert)
        is_fiscal_active = cert.id in active_fiscal
        can_delete = bool(manage or (not is_fiscal_active and (cert.uploaded_by == user.id or assigned or docs is not None)))
        entry = {
            "cert": cert,
            "display_name": info["display_name"],
            "cert_type": info["cert_type"],
            "masked_doc": info["masked_doc"],
            "issuer": info["issuer"],
            "fiscal_active": is_fiscal_active,
            "can_set_fiscal": bool(manage and (fiscal or (assigned and not is_fiscal_active))),
            "holder_id": holder_id,
            "holder_name": (holder_user.display_name or holder_user.name) if holder_user else None,
            "holder_email": holder_user.email if holder_user else None,
            "holder_phone": (holder_user.phone or "") if holder_user else "",
            "confirm_cpf": assigned and docs is None,
            "can_set_docs": bool(docs or assigned),
            "docs_enabled": bool(docs and docs.enabled_at and not docs.disabled_at),
            "valid": is_valid,
            "days_left": days_left,
            "expiring_soon": bool(is_valid and days_left <= 30),
            "can_delete": can_delete,
        }
        rows.append(entry)
        if entry["expiring_soon"]:
            expiring_certs.append(entry)
    return rows, organization_users, expiring_certs


@router.get("/assinatura-digital")
def signing_home(request: Request, db: Session = Depends(get_db), user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user, required=False)
    summaries, doc_counts, next_to_sign = _document_summaries(db, user, farm)
    cert_rows, _, expiring_certs = _certificate_inventory(db, user, farm)
    org_colleagues = (
        db.scalars(
            select(User)
            .where(User.organization_id == user.organization_id, User.is_active.is_(True), User.id != user.id)
            .order_by(User.name, User.id)
        ).all()
        if farm
        else []
    )
    active_tab = "validate" if request.query_params.get("aba") == "validar" else "overview"
    return _page(
        request, user, db, "digital_signing", "digital_signing.html", "Documentos",
        farm=farm, history=summaries, doc_counts=doc_counts, next_to_sign=next_to_sign,
        cert_count=len(cert_rows), expiring_certs=expiring_certs,
        categories=workflow.DOCUMENT_CATEGORIES, org_colleagues=org_colleagues,
        active_tab=active_tab, mode=signing_mode(farm.id if farm else None),
    )


@router.get("/assinatura-digital/novo")
def new_document_wizard(request: Request, db: Session = Depends(get_db), user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user, required=False)
    org_colleagues = (
        db.scalars(
            select(User)
            .where(User.organization_id == user.organization_id, User.is_active.is_(True), User.id != user.id)
            .order_by(User.name, User.id)
        ).all()
        if farm
        else []
    )
    return _page(
        request, user, db, "digital_signing", "digital_signing_new.html", "Novo documento",
        farm=farm, categories=workflow.DOCUMENT_CATEGORIES, org_colleagues=org_colleagues,
        mode=signing_mode(farm.id if farm else None),
    )


@router.post("/assinatura-digital/documentos")
async def document_import(request: Request, db: Session = Depends(get_db), user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    try:
        data = await _body(request, MAX_UPLOAD_BYTES, "application/pdf")
        name = unquote(request.headers.get("X-Document-Name", "Documento.pdf"))
        title = unquote(request.headers.get("X-Document-Title", "")) or None
        category = unquote(request.headers.get("X-Document-Category", "")) or None
        owner_signs_raw = (request.headers.get("X-Document-Owner-Signs", "1") or "1").strip().lower()
        owner_signs = owner_signs_raw not in {"0", "false", "no"}
        doc_id = await run_in_threadpool(
            _limited,
            workflow.import_pdf,
            db,
            user,
            farm.id,
            name,
            data,
            title=title,
            category=category,
            owner_signs=owner_signs,
        )
        return _json({"id": doc_id, "url": f"/assinatura-digital/documentos/{doc_id}"}, 201)
    except (ValueError, workflow.WorkflowError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


@router.delete("/assinatura-digital/documentos/{document_id}")
def delete_signing_document(document_id: str, request: Request, db: Session = Depends(get_db),
                            user: User = Depends(get_current_user_web)):
    _csrf(request)
    farm = _farm(request, db, user)
    try:
        workflow.delete_document(db, user, farm.id, document_id)
        return _json({"ok": True})
    except (ValueError, workflow.WorkflowError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


@router.post("/assinatura-digital/validar")
async def validate_external(request: Request, user: User = Depends(get_current_user_web)):
    try:
        data = await _body(request, MAX_UPLOAD_BYTES, "application/pdf")
        return _json(await run_in_threadpool(_limited, validate_pdf, data))
    except (ValueError, workflow.WorkflowError) as exc:
        return _error(exc)


@router.get("/assinatura-digital/documentos/{document_id}")
def document_workspace(document_id: str, request: Request, db: Session = Depends(get_db),
                       user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    doc = _doc(db, user, farm, document_id)
    try:
        revision = workflow.current_revision(db, doc)
        options = _limited(lambda: signing_options(read_pdf(workflow.revision_bytes(doc, revision))))
    except (ValueError, workflow.WorkflowError) as exc:
        raise HTTPException(getattr(exc, "status", 422), str(exc)) from exc
    options["sha256"] = revision.sha256
    options["number"] = revision.number
    all_participants = db.scalars(
        select(User)
        .join(SigningParticipant, SigningParticipant.user_id == User.id)
        .where(SigningParticipant.document_id == doc.id, User.organization_id == user.organization_id)
        .order_by(User.name)
    ).all()
    participants = all_participants if doc.owner_id == user.id else []
    available_colleagues = []
    if doc.owner_id == user.id:
        existing_ids = {p.id for p in all_participants} | {user.id}
        available_colleagues = db.scalars(
            select(User)
            .where(User.organization_id == user.organization_id, User.is_active.is_(True), User.id.not_in(existing_ids))
            .order_by(User.name, User.id)
        ).all()
    revisions = db.scalars(select(SigningRevision).filter_by(document_id=doc.id)
                           .order_by(SigningRevision.number.desc())).all()
    usable_certs = workflow.usable_certificates(db, user, farm.id)
    cert_choices = []
    for cert in usable_certs:
        info = certificates.friendly_cert_info(cert)
        cert_choices.append({
            "id": cert.id,
            "display_name": info["display_name"],
            "cert_type": info["cert_type"],
            "masked_doc": info["masked_doc"],
            "subject_cn": cert.subject_cn,
            "valid_until": cert.valid_until,
        })
    display_title = doc.title or (doc.filename[:-4] if doc.filename.lower().endswith(".pdf") else doc.filename)
    sig_count = len((doc.validation or {}).get("signatures") or [])
    owner_must_sign = bool(getattr(doc, "owner_signs", True))
    signed_user_ids = {rev.user_id for rev in revisions if rev.signature and rev.user_id is not None}
    expected_sisfarm_ids = {
        uid for uid in (([doc.owner_id] if owner_must_sign else []) + [p.id for p in all_participants]) if uid is not None
    }
    pending_count = len(expected_sisfarm_ids - signed_user_ids)
    expected_count = max(sig_count + pending_count, len(expected_sisfarm_ids), 1)
    user_already_signed = user.id in signed_user_ids
    user_must_sign = (doc.owner_id == user.id and owner_must_sign) or any(p.id == user.id for p in all_participants)
    all_signed = bool(expected_sisfarm_ids) and pending_count == 0
    show_signing_form = (not user_already_signed) and (not all_signed) and user_must_sign
    active_view = "detalhes" if request.query_params.get("aba") == "detalhes" else "assinar"
    wizard_flow = request.query_params.get("fluxo") or ""
    notice = (request.query_params.get("aviso") or "").strip()[:260]
    filename_stem = doc.filename[:-4] if doc.filename.lower().endswith(".pdf") else doc.filename
    show_filename_sub = filename_stem.strip().lower() != display_title.strip().lower()
    return _page(
        request, user, db, "digital_signing", "digital_signing_workspace.html", display_title,
        farm=farm, doc=doc, display_title=display_title, show_filename_sub=show_filename_sub,
        options=options, revisions=revisions,
        participants=participants, available_colleagues=available_colleagues,
        certificate_options=cert_choices, sig_count=sig_count, expected_count=expected_count,
        owner_must_sign=owner_must_sign, user_already_signed=user_already_signed,
        user_must_sign=user_must_sign, pending_count=pending_count,
        all_signed=all_signed, show_signing_form=show_signing_form,
        notice=notice,
        active_view=active_view, wizard_flow=wizard_flow, mode=signing_mode(farm.id),
    )


@router.get("/assinatura-digital/documentos/{document_id}/revisoes/{number}/pdf")
def document_pdf(document_id: str, number: int, request: Request, db: Session = Depends(get_db),
                 user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    doc = _doc(db, user, farm, document_id)
    revision = db.scalar(select(SigningRevision).filter_by(document_id=doc.id, number=number))
    if revision is None:
        raise HTTPException(404, "Versão não encontrada.")
    try:
        data = workflow.revision_bytes(doc, revision)
    except workflow.WorkflowError as exc:
        raise HTTPException(exc.status, str(exc)) from exc
    filename = doc.filename if number == 1 else f"{doc.filename[:-4]} - assinado v{number - 1}.pdf"
    return Response(data, media_type="application/pdf", headers={
        **PRIVATE, "Content-Disposition": "attachment; filename*=UTF-8''" + quote(filename, safe=""),
        "Content-Security-Policy": "sandbox", "X-Frame-Options": "DENY",
    })


@router.post("/assinatura-digital/documentos/{document_id}/validar")
def validate_document(document_id: str, request: Request, db: Session = Depends(get_db),
                      user: User = Depends(get_current_user_web)):
    _csrf(request)
    farm = _farm(request, db, user)
    try:
        doc = workflow.accessible_document(db, user, farm.id, document_id, lock=True)
        revision = workflow.current_revision(db, doc)
        doc.validation = _limited(validate_pdf, workflow.revision_bytes(doc, revision))
        result = doc.validation
        db.commit()
        return _json(result)
    except (ValueError, workflow.WorkflowError) as exc:
        db.rollback()
        return _error(exc)


@router.post("/assinatura-digital/documentos/{document_id}/autorizar")
async def request_signing_code(document_id: str, request: Request, db: Session = Depends(get_db),
                               user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    data = await _payload(request)
    if (data.get("consent") is not True or not isinstance(data.get("appearance"), dict)
            or not isinstance(data.get("certificate_id"), str) or not isinstance(data.get("sha256"), str)):
        raise HTTPException(400, "Confira o documento e autorize expressamente a assinatura.")
    session_nonce = request.session.get(SESSION_NONCE)
    if not session_nonce:
        session_nonce = request.session[SESSION_NONCE] = secrets.token_urlsafe(32)
    try:
        result = await run_in_threadpool(_limited, workflow.issue_challenge, db, user, farm.id, document_id,
                                         data["certificate_id"], data["appearance"], data["sha256"], session_nonce)
        return _json(result, 201)
    except (ValueError, workflow.WorkflowError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


@router.post("/assinatura-digital/documentos/{document_id}/assinar")
async def apply_signature(document_id: str, request: Request, db: Session = Depends(get_db),
                          user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    data = await _payload(request)
    session_nonce = request.session.get(SESSION_NONCE)
    password = data.get("password", "")
    if (data.get("consent") is not True or not session_nonce
            or not isinstance(data.get("challenge_id"), str) or not isinstance(data.get("code"), str)
            or not isinstance(password, str)):
        raise HTTPException(400, "Confirme a assinatura e informe o código enviado ao seu e-mail.")
    try:
        result = await run_in_threadpool(_limited, workflow.confirm_signature, db, user, farm.id, document_id,
                                         data["challenge_id"], data["code"].strip(), password, session_nonce)
        return _json(result)
    except (ValueError, workflow.WorkflowError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


def _normalize_br_whatsapp(phone: str | None) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if not digits:
        return ""
    if len(digits) in (10, 11) and not digits.startswith("55"):
        return "55" + digits
    return digits


def _notify_participant(db: Session, *, doc: SigningDocument, farm: Farm, sender: User, collaborator: User, channel: str, doc_url: str) -> tuple[bool, str | None]:
    doc_title = doc.title or doc.filename
    sender_name = sender.display_name or sender.name
    if channel == "whatsapp":
        link = db.scalar(select(WhatsAppLink).where(WhatsAppLink.user_id == collaborator.id, WhatsAppLink.wa_id.is_not(None)))
        wa_to = (link.wa_id if link and link.wa_id else "") or _normalize_br_whatsapp(collaborator.phone)
        if not wa_to:
            return False, f"{collaborator.display_name or collaborator.name} não possui telefone cadastrado para WhatsApp."
        if not whatsapp_is_configured():
            return False, "Envio por WhatsApp não está configurado no servidor."
        body = (
            f"Olá, {collaborator.display_name or collaborator.name}!\n\n"
            f"*{sender_name}* enviou o documento *{doc_title}* ({farm.name}) para sua assinatura digital no SiSFarm.\n\n"
            f"Acesse para revisar e assinar:\n{doc_url}"
        )
        try:
            WhatsAppClient().send_text(wa_to, body)
            return True, None
        except WhatsAppError as exc:
            logger.warning("Falha ao enviar convite de assinatura por WhatsApp (doc=%s, user=%s): %s", doc.id, collaborator.id, exc)
            return False, f"Não foi possível enviar o WhatsApp para {collaborator.display_name or collaborator.name}: {exc}"
    try:
        send_plain_email(
            collaborator.email,
            f"Documento para assinatura no SiSFarm: {doc_title}",
            f"Olá, {collaborator.display_name or collaborator.name}.\n\n"
            f"{sender_name} compartilhou o documento \"{doc_title}\" ({farm.name}) para sua assinatura digital no SiSFarm.\n\n"
            f"Acesse o link abaixo para revisar e assinar:\n{doc_url}\n",
            "document_signing_invite",
        )
        return True, None
    except RuntimeError as exc:
        logger.warning("Falha ao enviar convite de assinatura por e-mail (doc=%s, user=%s): %s", doc.id, collaborator.id, exc)
        return False, f"Não foi possível enviar o e-mail para {collaborator.email}."


@router.post("/assinatura-digital/documentos/{document_id}/participantes")
async def add_participant(document_id: str, request: Request, db: Session = Depends(get_db),
                          user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    data = await _payload(request)
    email = str(data.get("email", "")).strip().lower()
    channel = str(data.get("channel") or "email").strip().lower()
    if channel not in {"email", "whatsapp"}:
        channel = "email"
    if data.get("consent") is not True:
        raise HTTPException(400, "Confirme o compartilhamento com a pessoa indicada.")
    doc = _doc(db, user, farm, document_id, owner=True, lock=True)
    collaborator = db.scalar(select(User).filter_by(email=email, organization_id=user.organization_id, is_active=True))
    if collaborator is None or collaborator.id == user.id:
        raise HTTPException(404, "Selecione outra conta ativa da sua organização.")
    if db.get(SigningParticipant, (doc.id, collaborator.id)) is None:
        db.add(SigningParticipant(document_id=doc.id, user_id=collaborator.id))
        workflow.audit(db, user, "compartilhar", document_id=doc.id, farm_id=farm.id, participant_id=collaborator.id)
        db.commit()
    doc_url = str(request.base_url).rstrip("/") + f"/assinatura-digital/documentos/{doc.id}"
    sent, err = await run_in_threadpool(
        _notify_participant, db, doc=doc, farm=farm, sender=user, collaborator=collaborator, channel=channel, doc_url=doc_url
    )
    return _json({"ok": True, "channel": channel, "notification_sent": sent, "notification_error": err})


@router.delete("/assinatura-digital/documentos/{document_id}/participantes/{participant_id}")
def remove_participant(document_id: str, participant_id: int, request: Request, db: Session = Depends(get_db),
                       user: User = Depends(get_current_user_web)):
    _csrf(request)
    farm = _farm(request, db, user)
    doc = _doc(db, user, farm, document_id, owner=True, lock=True)
    row = db.get(SigningParticipant, (doc.id, participant_id))
    if row is not None:
        db.delete(row)
        workflow.audit(db, user, "remover_participante", document_id=doc.id, farm_id=farm.id, participant_id=participant_id)
        db.commit()
    return _json({"ok": True})


@router.get("/certificados-digitais")
def certificate_center(request: Request, db: Session = Depends(get_db), user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user, required=False)
    rows, organization_users, _ = _certificate_inventory(db, user, farm)
    _, doc_counts, _ = _document_summaries(db, user, farm)
    return _page(
        request, user, db, "certificate_center", "certificate_center.html", "Certificados A1",
        farm=farm, inventory=rows, organization_users=organization_users,
        doc_counts=doc_counts, cert_count=len(rows),
    )


@router.post("/certificados-digitais")
async def register_certificate(request: Request, db: Session = Depends(get_db), user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    data = await _payload(request, limit=272_000)
    if not isinstance(data.get("file"), str) or not isinstance(data.get("password"), str):
        raise HTTPException(400, "Selecione um arquivo A1 e informe a senha.")
    holder_user_id = data.get("holder_user_id")
    enable_documents = bool(data.get("enable_documents"))
    cpf = str(data.get("cpf", "") or "").strip()
    whatsapp = str(data.get("whatsapp", "") or "").strip()[:40]
    if holder_user_id is not None and type(holder_user_id) is not int:
        raise HTTPException(400, "Responsável selecionado inválido.")
    try:
        pfx = base64.b64decode(data["file"], validate=True)
        cert = await run_in_threadpool(_limited, certificates.store_a1, db, organization_id=user.organization_id,
                                       farm_id=farm.id, user_id=user.id, pfx=pfx, password=data["password"])
        db.flush()
        holder = None
        if holder_user_id is not None:
            if holder_user_id == user.id:
                holder = user
            elif has_fiscal_permission(user, "nfe.certificate.manage"):
                holder = db.scalar(select(User).filter_by(id=holder_user_id, organization_id=user.organization_id, is_active=True))
                if holder is None:
                    raise ValueError("Selecione um usuário ativo da organização.")
            else:
                raise PermissionError("Sua conta precisa de permissão de gestão para vincular outro usuário.")
            if whatsapp and not (holder.phone or "").strip():
                holder.phone = whatsapp
            certificates.assign_certificate_holder(db, cert, holder)
            db.flush()
            if cpf and holder.id != user.id:
                await run_in_threadpool(
                    _limited, certificates.verify_responsible_identity, db, cert=cert, user=holder,
                    cpf=cpf, password=data["password"], reference="CPF e senha do A1 confirmados no cadastro",
                )
        if enable_documents and holder is not None and holder.id == user.id:
            if not cpf:
                raise ValueError("Informe o CPF do titular para liberar o certificado para assinatura.")
            await run_in_threadpool(
                _limited, certificates.authorize_purpose, db, cert=cert, user=user, farm_id=farm.id,
                purpose=certificates.DOCUMENTS, enable=True, password=data["password"], cpf=cpf,
            )
        workflow.audit(db, user, "cadastrar_certificado", farm_id=farm.id, fingerprint=cert.fingerprint_sha256)
        db.commit()
        return _json({"id": cert.id}, 201)
    except (ValueError, PermissionError, binascii.Error, workflow.WorkflowError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


@router.delete("/certificados-digitais/{certificate_id}")
def remove_certificate(certificate_id: str, request: Request, db: Session = Depends(get_db),
                       user: User = Depends(get_current_user_web)):
    _csrf(request)
    farm = _farm(request, db, user)
    cert = db.scalar(select(DigitalCertificate).where(
        DigitalCertificate.id == certificate_id,
        DigitalCertificate.organization_id == user.organization_id,
        DigitalCertificate.farm_id == farm.id,
        DigitalCertificate.deleted_at.is_(None),
    ))
    if cert is None:
        raise HTTPException(404, "Certificado não encontrado nesta fazenda.")
    try:
        fp = cert.fingerprint_sha256
        certificates.delete_certificate(
            db, cert=cert, user=user, farm_id=farm.id,
            can_manage_fiscal=has_fiscal_permission(user, "nfe.certificate.manage"),
        )
        workflow.audit(db, user, "remover_certificado", farm_id=farm.id, certificate_id=certificate_id, fingerprint=fp)
        db.commit()
        return _json({"ok": True})
    except (ValueError, PermissionError, workflow.WorkflowError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


@router.post("/certificados-digitais/{certificate_id}/finalidades")
async def set_certificate_purpose(certificate_id: str, request: Request, db: Session = Depends(get_db),
                                  user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    data = await _payload(request)
    purpose = data.get("purpose")
    enable = data.get("enable")
    password = data.get("password", "")
    cpf = data.get("cpf", "")
    environment = data.get("environment", "homologacao")
    if (purpose not in (certificates.DOCUMENTS, certificates.FISCAL) or type(enable) is not bool
            or not isinstance(password, str) or not isinstance(cpf, str) or not isinstance(environment, str)
            or enable and data.get("consent") is not True):
        raise HTTPException(400, "Confirme a finalidade e a autorização para usar o certificado.")
    source = db.scalar(select(DigitalCertificate).filter_by(
        id=certificate_id, organization_id=user.organization_id, farm_id=farm.id,
    ))
    if source is None:
        raise HTTPException(404, "Certificado não encontrado.")
    if enable and purpose == certificates.FISCAL:
        if not has_fiscal_permission(user, "nfe.certificate.manage"):
            raise HTTPException(403, "Sua conta precisa da permissão de gestão fiscal.")
        if environment == "producao" and source.environment != "producao":
            raise HTTPException(403, "O cadastro fiscal do SiSFarm está habilitado para homologação.")
    email = f"certificado:{user.email}"
    ip = f"certificado:{client_ip(request)}"
    if enable and login_throttle.blocked_for(email, ip):
        raise HTTPException(429, "Aguarde alguns minutos antes de tentar novamente.")
    try:
        await run_in_threadpool(_limited, certificates.authorize_purpose, db, cert=source, user=user, farm_id=farm.id,
                                purpose=purpose, enable=enable, password=password, cpf=cpf.strip(), environment=environment)
        workflow.audit(db, user, "finalidade_certificado", farm_id=farm.id, purpose=purpose,
                       enabled=enable, fingerprint=source.fingerprint_sha256)
        db.commit()
        if enable:
            login_throttle.register_success(email)
        return _json({"ok": True})
    except (ValueError, PermissionError, InvalidTag, workflow.WorkflowError, IntegrityError) as exc:
        db.rollback()
        if enable and isinstance(exc, ValueError) and not isinstance(exc, certificates.EncryptionConfigurationError):
            login_throttle.register_failure(email, ip)
        return _error(exc)


@router.post("/certificados-digitais/{certificate_id}/vinculo")
async def bind_certificate_holder(certificate_id: str, request: Request, db: Session = Depends(get_db),
                                  user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    if not has_fiscal_permission(user, "nfe.certificate.manage"):
        raise HTTPException(403, "Sua conta precisa da permissão de gestão de certificados.")
    user_id = (await _payload(request)).get("user_id")
    if type(user_id) is not int:
        raise HTTPException(400, "Selecione um usuário da organização.")
    cert = db.scalar(select(DigitalCertificate).where(
        DigitalCertificate.id == certificate_id,
        DigitalCertificate.organization_id == user.organization_id,
        DigitalCertificate.farm_id == farm.id,
        DigitalCertificate.deleted_at.is_(None),
    ))
    if cert is None:
        raise HTTPException(404, "Certificado não encontrado nesta fazenda.")
    holder = db.scalar(select(User).filter_by(id=user_id, organization_id=user.organization_id, is_active=True))
    if holder is None:
        raise HTTPException(404, "Selecione um usuário ativo da organização.")
    try:
        certificates.assign_certificate_holder(db, cert, holder)
        workflow.audit(db, user, "vincular_responsavel", farm_id=farm.id, certificate_id=cert.id, holder_user_id=holder.id)
        db.commit()
        return _json({"ok": True})
    except (ValueError, PermissionError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)
