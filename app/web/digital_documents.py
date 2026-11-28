"""Authenticated web entry points for signing, validation and certificate purposes."""
import base64
import binascii
import json
import logging
import re
import secrets
from datetime import datetime, timezone
from hashlib import sha256
from urllib.parse import quote, unquote

from cryptography.exceptions import InvalidTag
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from app.core.admin_access import is_super_admin_email
from app.core.config import get_settings
from app.core.csrf import ensure_csrf_token, validate_csrf
from app.core.deps import get_current_user_web
from app.db.session import get_db
from app.models import Farm, User
from app.models.digital_certificate import CertificateHolderAssignment, CertificatePurpose, DigitalCertificate
from app.models.signed_document import (
    InstitutionalSealingCertificate,
    SigningDocument,
    SigningEvidence,
    SigningExternalSigner,
    SigningParticipant,
    SigningRevision,
)
from app.models.whatsapp import WhatsAppLink
from app.services import digital_certificates as certificates, login_throttle
from app.services.audit_log_service import client_ip
from app.services.document_signing import electronic, workflow, workspace_view
from app.services.document_signing.pdf import MAX_PDF_BYTES, MAX_UPLOAD_BYTES, read_pdf, signing_options, validate_pdf
from app.services.document_signing.policy import signing_mode
from app.services.document_signing.receipt_pdf import build_validation_receipt_pdf
from app.services.email_service import send_plain_email
from app.services.nfe_permissions import has_fiscal_permission
from app.services.whatsapp import WhatsAppClient, WhatsAppError, is_configured as whatsapp_is_configured
from app.web.routes import _base_context, _repository, templates

logger = logging.getLogger(__name__)
router = APIRouter()
PRIVATE = {"Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff"}
SESSION_NONCE = "document_signing_session"


def _public_base_url(request: Request) -> str:
    """Resolve a URL publica oficial em HTTPS (sempre app.sisfarm.com.br fora de testes locais)."""
    req_url = str(request.base_url).rstrip("/")
    host = (request.headers.get("x-forwarded-host") or request.url.hostname or "").lower()
    if host not in ("", "testserver", "localhost", "127.0.0.1"):
        configured = (get_settings().app_public_url or "").strip().rstrip("/")
        if (configured.startswith("https://") or configured.startswith("http://")) and ".onrender.com" not in configured.lower():
            return configured
        return electronic.OFFICIAL_PUBLIC_BASE_URL
    proto = (request.headers.get("x-forwarded-proto") or "").split(",", 1)[0].strip().lower()
    if proto == "https" and req_url.startswith("http://"):
        return "https://" + req_url[len("http://"):]
    return req_url


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

    ev_electronic_count: dict[str, int] = {doc_id: 0 for doc_id in doc_ids}
    for doc_id, uid, mod in db.execute(
        select(SigningEvidence.document_id, SigningEvidence.user_id, SigningEvidence.modality).where(
            SigningEvidence.document_id.in_(doc_ids)
        )
    ).all():
        if uid is not None:
            signed_by_map.setdefault(doc_id, set()).add(uid)
        if mod == "electronic_sisfarm":
            ev_electronic_count[doc_id] = ev_electronic_count.get(doc_id, 0) + 1

    ext_pending_map: dict[str, int] = {doc_id: 0 for doc_id in doc_ids}
    for doc_id, st in db.execute(
        select(SigningExternalSigner.document_id, SigningExternalSigner.status).where(
            SigningExternalSigner.document_id.in_(doc_ids)
        )
    ).all():
        if st == "pending":
            ext_pending_map[doc_id] = ext_pending_map.get(doc_id, 0) + 1

    summaries = []
    counts = {"total": len(docs), "your_turn": 0, "waiting": 0, "done": 0, "attention": 0}
    next_to_sign = None

    for doc in docs:
        validation = doc.validation or {}
        val_state = validation.get("state") or "unsigned"
        icp_sig_count = len(validation.get("signatures") or [])
        elec_sig_count = ev_electronic_count.get(doc.id, 0)
        sig_count = icp_sig_count + elec_sig_count
        participants = part_map.get(doc.id, [])
        signed_users = signed_by_map.get(doc.id, set())
        owner_must_sign = bool(getattr(doc, "owner_signs", True))
        expected_list = ([doc.owner_id] if owner_must_sign else []) + participants
        expected_set = {uid for uid in expected_list if uid is not None}
        ext_pending = ext_pending_map.get(doc.id, 0)
        pending_count = len(expected_set - signed_users) + ext_pending
        expected_count = max(sig_count + pending_count, len(expected_set) + ext_pending, 1)
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


def _format_phone_br(phone: str | None) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if len(digits) in (12, 13) and digits.startswith("55"):
        digits = digits[2:]
    if len(digits) == 11:
        return f"({digits[:2]}) {digits[2:7]}-{digits[7:]}"
    if len(digits) == 10:
        return f"({digits[:2]}) {digits[2:6]}-{digits[6:]}"
    return (phone or "").strip()


def _user_initials(name: str | None) -> str:
    parts = [p for p in (name or "").strip().split() if p]
    if not parts:
        return "US"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[-1][0]).upper()


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
    user_ids = [user.id] + [c.id for c in org_colleagues]
    wa_map: dict[int, str] = {}
    if user_ids:
        for link in db.scalars(select(WhatsAppLink).where(WhatsAppLink.user_id.in_(user_ids), WhatsAppLink.wa_id.is_not(None))).all():
            if link.wa_id and link.user_id not in wa_map:
                wa_map[link.user_id] = link.wa_id
    colleagues_payload = []
    for c in org_colleagues:
        cname = c.display_name or c.name
        cphone = _format_phone_br(wa_map.get(c.id) or c.phone)
        colleagues_payload.append({
            "id": c.id,
            "name": cname,
            "initials": _user_initials(cname),
            "role": (c.job_title or "").strip(),
            "email": c.email,
            "phone": cphone,
        })
    self_name = user.display_name or user.name
    self_user_payload = {
        "name": self_name,
        "initials": _user_initials(self_name),
        "email": user.email,
        "phone": _format_phone_br(wa_map.get(user.id) or user.phone),
    }
    return _page(
        request, user, db, "digital_signing", "digital_signing_new.html", "Novo documento",
        farm=farm, categories=workflow.DOCUMENT_CATEGORIES, org_colleagues=org_colleagues,
        colleagues_payload=colleagues_payload, self_user_payload=self_user_payload,
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
        signing_policy = (request.headers.get("X-Document-Signing-Policy", "flexible") or "flexible").strip().lower()
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
            signing_policy=signing_policy,
        )
        doc_row = db.get(SigningDocument, doc_id)
        return _json({
            "id": doc_id,
            "url": f"/assinatura-digital/documentos/{doc_id}",
            "verification_code": doc_row.verification_code if doc_row else None,
        }, 201)
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
    external_signers = db.scalars(
        select(SigningExternalSigner).filter_by(document_id=doc.id).order_by(SigningExternalSigner.created_at.asc())
    ).all()
    evidences = db.scalars(
        select(SigningEvidence).filter_by(document_id=doc.id).order_by(SigningEvidence.signed_at.asc(), SigningEvidence.revision_number.asc())
    ).all()
    icp_sig_count = len((doc.validation or {}).get("signatures") or [])
    elec_sig_count = sum(1 for ev in evidences if ev.modality == "electronic_sisfarm")
    sig_count = icp_sig_count + elec_sig_count
    owner_must_sign = bool(getattr(doc, "owner_signs", True))
    signed_user_ids = {rev.user_id for rev in revisions if rev.signature and rev.user_id is not None} | {
        ev.user_id for ev in evidences if ev.user_id is not None
    }
    expected_sisfarm_ids = {
        uid for uid in (([doc.owner_id] if owner_must_sign else []) + [p.id for p in all_participants]) if uid is not None
    }
    ext_pending_count = sum(1 for ext in external_signers if ext.status == "pending")
    pending_count = len(expected_sisfarm_ids - signed_user_ids) + ext_pending_count
    expected_count = max(sig_count + pending_count, len(expected_sisfarm_ids) + ext_pending_count, 1)
    user_already_signed = user.id in signed_user_ids
    user_must_sign = (doc.owner_id == user.id and owner_must_sign) or any(p.id == user.id for p in all_participants)
    all_signed = (bool(expected_sisfarm_ids) or bool(external_signers)) and pending_count == 0
    show_signing_form = (not user_already_signed) and (not all_signed) and user_must_sign
    signing_policy = getattr(doc, "signing_policy", "flexible") or "flexible"
    allow_electronic = signing_policy != "icp_brasil"
    allow_icp = signing_policy != "electronic"
    default_sign_mode = "icp" if (signing_policy == "icp_brasil" or (allow_icp and cert_choices and signing_policy != "electronic")) else "electronic"
    wa_link = db.scalar(select(WhatsAppLink).where(WhatsAppLink.user_id == user.id, WhatsAppLink.wa_id.is_not(None)))
    user_whatsapp_phone = _format_phone_br((wa_link.wa_id if wa_link and wa_link.wa_id else "") or (user.phone or ""))
    user_initials = _user_initials(user.display_name or user.name)
    verification_code = electronic.ensure_document_verification_code(db, doc)
    db.commit()
    owner_user = db.get(User, doc.owner_id) if doc.owner_id else None
    granted = {
        row.user_id: row.granted_at
        for row in db.scalars(select(SigningParticipant).filter_by(document_id=doc.id))
    }
    signer_rows = workspace_view.build_signers(
        doc=doc, owner=owner_user, current_user=user, participants=all_participants,
        external_signers=external_signers, evidences=evidences, revisions=revisions,
    )
    history = workspace_view.build_history(
        doc=doc, owner=owner_user, participants=all_participants, external_signers=external_signers,
        evidences=evidences, revisions=revisions, granted=granted,
    )
    integrity = workspace_view.integrity_summary(doc.validation, sig_count)
    signed_rows = sum(1 for r in signer_rows if r["status"] == "signed")
    signing_rows = [r for r in signer_rows if r["status"] != "viewer"]
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
        external_signers=external_signers, evidences=evidences,
        certificate_options=cert_choices, sig_count=sig_count, expected_count=expected_count,
        owner_must_sign=owner_must_sign, user_already_signed=user_already_signed,
        user_must_sign=user_must_sign, pending_count=pending_count,
        all_signed=all_signed, show_signing_form=show_signing_form,
        signing_policy=signing_policy, allow_electronic=allow_electronic, allow_icp=allow_icp,
        default_sign_mode=default_sign_mode, user_whatsapp_phone=user_whatsapp_phone,
        user_initials=user_initials,
        verification_code=verification_code,
        signer_rows=signer_rows, history=history, integrity=integrity,
        signed_rows=signed_rows, total_rows=len(signing_rows), owner_user=owner_user,
        is_doc_owner=doc.owner_id == user.id,
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
        result = await run_in_threadpool(
            _limited,
            workflow.confirm_signature,
            db,
            user,
            farm.id,
            document_id,
            data["challenge_id"],
            data["code"].strip(),
            password,
            session_nonce,
            ip_address=client_ip(request),
            user_agent=request.headers.get("user-agent", ""),
            farm_name=farm.name,
            base_url=_public_base_url(request),
        )
        return _json(result)
    except (ValueError, workflow.WorkflowError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


# ---------------------------------------------------------------------------
# Assinatura Eletrônica SiSFarm (Interna) + Signatários Externos
# ---------------------------------------------------------------------------


@router.post("/assinatura-digital/documentos/{document_id}/eletronica/autorizar")
async def request_electronic_code(document_id: str, request: Request, db: Session = Depends(get_db),
                                  user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    data = await _payload(request)
    doc = _doc(db, user, farm, document_id, lock=True)
    if getattr(doc, "signing_policy", "flexible") == "icp_brasil":
        raise HTTPException(403, "Este documento exige assinatura qualificada ICP-Brasil.")
    try:
        revision = workflow.current_revision(db, doc)
        pdf_bytes = workflow.revision_bytes(doc, revision)
        result = await run_in_threadpool(
            _limited,
            electronic.issue_electronic_challenge,
            db,
            doc=doc,
            revision=revision,
            pdf_bytes=pdf_bytes,
            user=user,
            external_signer=None,
            signer_name=str(data.get("signer_name") or user.display_name or user.name),
            cpf_raw=str(data.get("cpf") or ""),
            auth_channel=str(data.get("channel") or "email"),
            appearance=data.get("appearance") if isinstance(data.get("appearance"), dict) else {"visible": False},
            consent=data.get("consent") is True,
            override_phone=str(data.get("phone") or "").strip() or None,
        )
        return _json(result, 201)
    except (ValueError, PermissionError, workflow.WorkflowError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


@router.post("/assinatura-digital/documentos/{document_id}/eletronica/assinar")
async def apply_electronic_signature(document_id: str, request: Request, db: Session = Depends(get_db),
                                     user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    data = await _payload(request)
    if data.get("consent") is not True or not isinstance(data.get("challenge_id"), str) or not isinstance(data.get("code"), str):
        raise HTTPException(400, "Confirme a assinatura e informe o código recebido.")
    doc = _doc(db, user, farm, document_id, lock=True)
    try:
        revision = workflow.current_revision(db, doc)
        pdf_bytes = workflow.revision_bytes(doc, revision)
        result = await run_in_threadpool(
            _limited,
            electronic.confirm_electronic_signature,
            db,
            doc=doc,
            revision=revision,
            pdf_bytes=pdf_bytes,
            user=user,
            external_signer=None,
            challenge_id=data["challenge_id"],
            code=data["code"].strip(),
            ip_address=client_ip(request),
            user_agent=request.headers.get("user-agent", ""),
            farm_name=farm.name,
            base_url=_public_base_url(request),
            store_revision_fn=workflow._store_revision,
        )
        return _json(result)
    except (ValueError, PermissionError, workflow.WorkflowError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


@router.post("/assinatura-digital/documentos/{document_id}/externos")
async def add_external_signer(document_id: str, request: Request, db: Session = Depends(get_db),
                              user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    data = await _payload(request)
    doc = _doc(db, user, farm, document_id, owner=True, lock=True)
    if getattr(doc, "signing_policy", "flexible") == "icp_brasil":
        raise HTTPException(403, "Signatários externos sem certificado só podem ser convidados em documentos com assinatura eletrônica.")
    try:
        signer, sign_url, sent, err, wa_share_url = await run_in_threadpool(
            electronic.create_external_signer,
            db,
            doc=doc,
            farm=farm,
            sender=user,
            name=str(data.get("name") or ""),
            email=str(data.get("email") or "") or None,
            phone=str(data.get("phone") or "") or None,
            channel=str(data.get("channel") or "email"),
            role_label=str(data.get("role_label") or "Signatário"),
            base_url=_public_base_url(request),
            cpf_raw=str(data.get("cpf") or "") or None,
            appearance=data.get("appearance") if isinstance(data.get("appearance"), dict) else None,
        )
        return _json({
            "ok": True,
            "signer_id": signer.id,
            "signer_name": signer.name,
            "cpf_masked": signer.cpf_masked,
            "appearance": signer.appearance,
            "sign_url": sign_url,
            "whatsapp_share_url": wa_share_url,
            "channel": signer.channel,
            "notification_sent": sent,
            "notification_error": err,
        }, 201)
    except (ValueError, PermissionError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


@router.post("/assinatura-digital/documentos/{document_id}/externos/{signer_id}/reenviar")
async def resend_external_signer_invite(document_id: str, signer_id: str, request: Request, db: Session = Depends(get_db),
                                        user: User = Depends(get_current_user_web)):
    _csrf(request)
    farm = _farm(request, db, user)
    doc = _doc(db, user, farm, document_id, owner=True, lock=True)
    row = db.scalar(select(SigningExternalSigner).filter_by(id=signer_id, document_id=doc.id))
    if row is None or row.status == "revoked":
        raise HTTPException(404, "Signatário externo não encontrado.")
    try:
        sign_url, sent, err, wa_share_url = await run_in_threadpool(
            electronic.resend_external_signer,
            db,
            doc=doc,
            farm=farm,
            sender=user,
            signer=row,
            base_url=_public_base_url(request),
            notify=request.query_params.get("somente_link") != "1",
        )
        return _json({
            "ok": True,
            "signer_id": row.id,
            "signer_name": row.name,
            "sign_url": sign_url,
            "whatsapp_share_url": wa_share_url,
            "channel": row.channel,
            "notification_sent": sent,
            "notification_error": err,
        })
    except (ValueError, PermissionError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


@router.post("/assinatura-digital/documentos/{document_id}/externos/{signer_id}/canal")
async def change_external_signer_channel(document_id: str, signer_id: str, request: Request, db: Session = Depends(get_db),
                                         user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    data = await _payload(request)
    doc = _doc(db, user, farm, document_id, owner=True, lock=True)
    row = db.scalar(select(SigningExternalSigner).filter_by(id=signer_id, document_id=doc.id))
    if row is None or row.status == "revoked":
        raise HTTPException(404, "Signatário externo não encontrado.")
    try:
        sign_url, sent, err, wa_share_url = await run_in_threadpool(
            electronic.change_external_signer_channel,
            db,
            doc=doc,
            farm=farm,
            sender=user,
            signer=row,
            channel=str(data.get("channel") or ""),
            email=str(data.get("email") or "") or None,
            phone=str(data.get("phone") or "") or None,
            base_url=_public_base_url(request),
        )
        workflow.audit(db, user, "trocar_canal_externo", document_id=doc.id, farm_id=farm.id, signer_id=row.id, channel=row.channel)
        db.commit()
        return _json({
            "ok": True,
            "signer_id": row.id,
            "signer_name": row.name,
            "sign_url": sign_url,
            "whatsapp_share_url": wa_share_url,
            "channel": row.channel,
            "notification_sent": sent,
            "notification_error": err,
        })
    except (ValueError, PermissionError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


@router.delete("/assinatura-digital/documentos/{document_id}/externos/{signer_id}")
def remove_external_signer(document_id: str, signer_id: str, request: Request, db: Session = Depends(get_db),
                           user: User = Depends(get_current_user_web)):
    _csrf(request)
    farm = _farm(request, db, user)
    doc = _doc(db, user, farm, document_id, owner=True, lock=True)
    row = db.scalar(select(SigningExternalSigner).filter_by(id=signer_id, document_id=doc.id))
    if row is not None and row.status != "signed":
        row.status = "revoked"
        db.commit()
    return _json({"ok": True})


def _normalize_br_whatsapp(phone: str | None) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if not digits:
        return ""
    if len(digits) in (10, 11) and not digits.startswith("55"):
        return "55" + digits
    return digits


def _notify_participant(db: Session, *, doc: SigningDocument, farm: Farm, sender: User, collaborator: User, channel: str, doc_url: str, phone_override: str | None = None) -> tuple[bool, str | None]:
    doc_title = doc.title or doc.filename
    sender_name = sender.display_name or sender.name
    if channel == "whatsapp":
        link = db.scalar(select(WhatsAppLink).where(WhatsAppLink.user_id == collaborator.id, WhatsAppLink.wa_id.is_not(None)))
        wa_to = _normalize_br_whatsapp(phone_override) or (link.wa_id if link and link.wa_id else "") or _normalize_br_whatsapp(collaborator.phone)
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
        from app.services.email_service import build_document_signing_internal_invite_email, dispatch_email

        subject, text_body, html_body = build_document_signing_internal_invite_email(
            collaborator_name=collaborator.display_name or collaborator.name,
            sender_name=sender_name,
            doc_title=doc_title,
            farm_name=farm.name,
            doc_url=doc_url,
        )
        dispatch_email(
            send_plain_email,
            collaborator.email,
            subject,
            text_body,
            "document_signing_invite",
            html_body=html_body,
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
    phone_override = str(data.get("phone") or "").strip() or None
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
    doc_url = _public_base_url(request) + f"/assinatura-digital/documentos/{doc.id}"
    sent, err = await run_in_threadpool(
        _notify_participant, db, doc=doc, farm=farm, sender=user, collaborator=collaborator, channel=channel, doc_url=doc_url, phone_override=phone_override
    )
    return _json({"ok": True, "channel": channel, "notification_sent": sent, "notification_error": err})


@router.post("/assinatura-digital/documentos/{document_id}/participantes/{participant_id}/lembrar")
async def remind_participant(document_id: str, participant_id: int, request: Request, db: Session = Depends(get_db),
                             user: User = Depends(get_current_user_web)):
    _csrf(request)
    farm = _farm(request, db, user)
    doc = _doc(db, user, farm, document_id, owner=True)
    if db.get(SigningParticipant, (doc.id, participant_id)) is None:
        raise HTTPException(404, "Esta pessoa não está mais neste documento.")
    collaborator = db.get(User, participant_id)
    if collaborator is None or not collaborator.is_active:
        raise HTTPException(404, "Esta conta não está mais ativa.")
    if any(ev.user_id == participant_id for ev in db.scalars(select(SigningEvidence).filter_by(document_id=doc.id))) or any(
        rev.user_id == participant_id and rev.signature for rev in db.scalars(select(SigningRevision).filter_by(document_id=doc.id))
    ):
        raise HTTPException(409, "Esta pessoa já assinou o documento.")
    has_wa = db.scalar(select(WhatsAppLink).where(WhatsAppLink.user_id == collaborator.id, WhatsAppLink.wa_id.is_not(None))) is not None
    channel = "whatsapp" if has_wa and whatsapp_is_configured() else "email"
    doc_url = _public_base_url(request) + f"/assinatura-digital/documentos/{doc.id}"
    sent, err = await run_in_threadpool(
        _notify_participant, db, doc=doc, farm=farm, sender=user, collaborator=collaborator, channel=channel, doc_url=doc_url
    )
    if sent:
        workflow.audit(db, user, "lembrar_participante", document_id=doc.id, farm_id=farm.id, participant_id=participant_id)
        db.commit()
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


# ---------------------------------------------------------------------------
# Fluxo Público de Signatário Externo (/assinar/{token} — sem login)
# ---------------------------------------------------------------------------


@router.get("/assinar/{token}")
def external_signing_page(token: str, request: Request, db: Session = Depends(get_db)):
    found = electronic.get_external_signer_by_token(db, token)
    if found is None:
        return templates.TemplateResponse(
            request,
            "digital_signing_external.html",
            {"request": request, "invalid_link": True, "csrf_token": ensure_csrf_token(request)},
            status_code=404,
            headers=PRIVATE,
        )
    signer, doc, farm = found
    revision = workflow.current_revision(db, doc)
    pdf_bytes = workflow.revision_bytes(doc, revision)
    options = _limited(lambda: signing_options(read_pdf(pdf_bytes)))
    options["sha256"] = revision.sha256
    options["number"] = revision.number
    display_title = doc.title or (doc.filename[:-4] if doc.filename.lower().endswith(".pdf") else doc.filename)
    ver_code = electronic.ensure_document_verification_code(db, doc)
    pending_count = electronic._pending_signer_count(db, doc)
    settings = get_settings()
    otp_template = (getattr(settings, "whatsapp_signing_otp_template", None) or "").strip()
    whatsapp_ready = bool(
        signer.phone
        and electronic.whatsapp_is_configured()
        and (otp_template or electronic.has_active_whatsapp_window(db, signer.phone))
    )
    bot_wa = electronic.normalize_br_whatsapp(getattr(settings, "whatsapp_display_number", None))
    whatsapp_bot_url = (
        f"https://wa.me/{bot_wa}?text=" + quote("Olá, quero ativar o recebimento do código de assinatura no SiSFarm.", safe="")
        if bot_wa
        else ""
    )
    cpf_digits = ""
    if signer.cpf_encrypted:
        try:
            cpf_digits = electronic.decrypt_cpf(signer.cpf_encrypted, doc.id)
        except Exception:
            cpf_digits = ""
    cpf_display = electronic.format_cpf(cpf_digits) if cpf_digits else ""
    cpf_masked_for_stamp = signer.cpf_masked or (electronic.mask_cpf(cpf_digits) if cpf_digits else "")
    locked_channel = "whatsapp" if signer.channel == "whatsapp" else "email"
    locked_contact_display = (
        _format_phone_br(signer.phone)
        if locked_channel == "whatsapp"
        else (signer.email or "")
    )
    preset_appearance = signer.appearance if isinstance(signer.appearance, dict) else {"visible": False}
    electronic.record_real_external_access(
        db,
        signer,
        method=request.method,
        user_agent=request.headers.get("user-agent"),
        headers=dict(request.headers),
    )
    db.commit()
    return templates.TemplateResponse(
        request,
        "digital_signing_external.html",
        {
            "request": request,
            "invalid_link": False,
            "csrf_token": ensure_csrf_token(request),
            "token": token,
            "signer": signer,
            "signer_initials": _user_initials(signer.name),
            "doc": doc,
            "farm": farm,
            "display_title": display_title,
            "options": options,
            "verification_code": ver_code,
            "pending_count": pending_count,
            "already_signed": signer.status == "signed",
            "cpf_display": cpf_display,
            "cpf_masked_for_stamp": cpf_masked_for_stamp,
            "has_preset_cpf": bool(cpf_display),
            "locked_channel": locked_channel,
            "locked_contact_display": locked_contact_display,
            "preset_appearance": preset_appearance,
            "phone_masked": electronic.mask_phone(signer.phone) if signer.phone else "",
            "email_masked": electronic.mask_email(signer.email) if signer.email else "",
            "whatsapp_ready": whatsapp_ready,
            "whatsapp_bot_url": whatsapp_bot_url,
        },
        headers=PRIVATE,
    )


@router.get("/assinar/{token}/pdf")
def external_signing_pdf(token: str, db: Session = Depends(get_db)):
    found = electronic.get_external_signer_by_token(db, token)
    if found is None:
        raise HTTPException(404, "Link de assinatura indisponível ou expirado.")
    _, doc, _ = found
    revision = workflow.current_revision(db, doc)
    data = workflow.revision_bytes(doc, revision)
    filename = doc.filename if doc.current_revision == 1 else f"{doc.filename[:-4]} - assinado.pdf"
    return Response(
        data,
        media_type="application/pdf",
        headers={
            **PRIVATE,
            "Content-Disposition": "inline; filename*=UTF-8''" + quote(filename, safe=""),
            "Content-Security-Policy": "sandbox",
            "X-Frame-Options": "DENY",
        },
    )


@router.post("/assinar/{token}/autorizar")
async def external_request_code(token: str, request: Request, db: Session = Depends(get_db)):
    data = await _payload(request)
    found = electronic.get_external_signer_by_token(db, token)
    if found is None:
        raise HTTPException(404, "Link de assinatura indisponível ou expirado.")
    signer, doc, _ = found
    try:
        revision = workflow.current_revision(db, doc)
        pdf_bytes = workflow.revision_bytes(doc, revision)
        preset_cpf = ""
        if signer.cpf_encrypted:
            try:
                preset_cpf = electronic.decrypt_cpf(signer.cpf_encrypted, doc.id)
            except Exception:
                preset_cpf = ""
        locked_channel = "whatsapp" if signer.channel == "whatsapp" else "email"
        locked_appearance = (
            signer.appearance
            if isinstance(signer.appearance, dict)
            else (data.get("appearance") if isinstance(data.get("appearance"), dict) else {"visible": False})
        )
        result = await run_in_threadpool(
            _limited,
            electronic.issue_electronic_challenge,
            db,
            doc=doc,
            revision=revision,
            pdf_bytes=pdf_bytes,
            user=None,
            external_signer=signer,
            signer_name=signer.name,
            cpf_raw=preset_cpf or str(data.get("cpf") or ""),
            auth_channel=locked_channel,
            appearance=locked_appearance,
            consent=data.get("consent") is True,
            override_email=None,
        )
        return _json(result, 201)
    except (ValueError, PermissionError, workflow.WorkflowError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


@router.post("/assinar/{token}/assinar")
async def external_confirm_signature(token: str, request: Request, db: Session = Depends(get_db)):
    data = await _payload(request)
    if data.get("consent") is not True or not isinstance(data.get("challenge_id"), str) or not isinstance(data.get("code"), str):
        raise HTTPException(400, "Confirme a assinatura e informe o código recebido.")
    found = electronic.get_external_signer_by_token(db, token)
    if found is None:
        raise HTTPException(404, "Link de assinatura indisponível ou expirado.")
    signer, doc, farm = found
    try:
        revision = workflow.current_revision(db, doc)
        pdf_bytes = workflow.revision_bytes(doc, revision)
        result = await run_in_threadpool(
            _limited,
            electronic.confirm_electronic_signature,
            db,
            doc=doc,
            revision=revision,
            pdf_bytes=pdf_bytes,
            user=None,
            external_signer=signer,
            challenge_id=data["challenge_id"],
            code=data["code"].strip(),
            ip_address=client_ip(request),
            user_agent=request.headers.get("user-agent", ""),
            farm_name=farm.name,
            base_url=_public_base_url(request),
            store_revision_fn=workflow._store_revision,
        )
        return _json(result)
    except (ValueError, PermissionError, workflow.WorkflowError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


# ---------------------------------------------------------------------------
# Página Pública de Validação (/validar e /validar/{code} — sem login)
# ---------------------------------------------------------------------------


def _format_sp_dt(dt_val) -> str:
    if not dt_val:
        return ""
    if isinstance(dt_val, str):
        try:
            dt_val = datetime.fromisoformat(dt_val.replace("Z", "+00:00"))
        except ValueError:
            return dt_val
    if dt_val.tzinfo is None:
        dt_val = dt_val.replace(tzinfo=timezone.utc)
    return dt_val.astimezone(electronic.SP_TZ).strftime("%d/%m/%Y às %H:%M")


def _seal_transparency_note(seal_mode: str | None) -> str:
    if seal_mode == "institutional_ecnpj":
        return (
            "Ao concluir as assinaturas eletrônicas, o SiSFarm aplica o Selo Criptográfico "
            "Institucional (e-CNPJ A1 Institucional SiSFarm) exclusivamente para lacrar o arquivo "
            "e garantir que nenhuma página ou conteúdo seja alterado depois da assinatura. "
            "Este selo institucional não substitui nem equivale à assinatura qualificada ICP-Brasil "
            "individual dos signatários eletrônicos."
        )
    return (
        "Ao concluir as assinaturas eletrônicas, o SiSFarm anexa o Manifesto de Assinaturas e "
        "aplica um Selo Criptográfico de Integridade para garantir que o arquivo não seja modificado "
        "após a assinatura. Este selo protege a integridade do documento e não equivale à assinatura "
        "qualificada ICP-Brasil individual dos signatários eletrônicos."
    )


def _receipt_payload_from_snapshot(snapshot: dict, *, via_pdf: bool = False, filename: str = "") -> dict:
    completed = bool(snapshot.get("completed"))
    pending = int(snapshot.get("pending_count") or 0)
    evidences = snapshot.get("evidences") or []
    tone = "valid" if completed else "partial"
    signers = []
    for ev in evidences:
        is_icp = ev.modality == "qualified_icp_brasil"
        if is_icp:
            tech_line = (
                f"Certificado {ev.cert_issuer or 'ICP-Brasil A1'} · "
                f"Lei 14.063/2020 art. 4º III · MP 2.200-2/2001 art. 10 §1º · "
                f"Evidência {ev.evidence_code}"
            )
        else:
            channel = "WhatsApp" if "whatsapp" in (ev.auth_channel or "") else "E-mail"
            tech_line = (
                f"Autenticação por {channel} ({ev.auth_destination_masked or 'confirmado'}) · "
                f"IP {ev.ip_masked or 'LGPD'} · {ev.device_summary or 'Navegador web'} · "
                f"Lei 14.063/2020 art. 4º II · Evidência {ev.evidence_code}"
            )
        signers.append({
            "name": ev.signer_name,
            "cpf": ev.cpf_masked or "Conforme certificado",
            "signed_at": _format_sp_dt(ev.signed_at),
            "type": "Qualificada ICP-Brasil" if is_icp else "Eletrônica SiSFarm (Avançada)",
            "type_tone": "brand" if is_icp else "info",
            "status": "valid",
            "evidence": ev.evidence_code or "",
            "tech": tech_line,
        })

    if completed:
        headline = "Documento autêntico"
        summary = (
            "Todas as assinaturas foram confirmadas e o arquivo enviado não sofreu nenhuma alteração."
            if via_pdf
            else "Este código corresponde a um documento concluído e registrado oficialmente no SiSFarm."
        )
        guidance = ""
    else:
        headline = "Assinatura em andamento"
        summary = (
            f"O arquivo está íntegro e possui {len(evidences)} assinatura(s) confirmada(s), "
            f"mas ainda aguarda {pending} participante(s) para ser concluído."
        )
        guidance = "Aguarde a conclusão de todos os participantes antes de considerar este documento finalizado."

    seal_mode = snapshot.get("seal_mode")
    seal_label = (
        "e-CNPJ A1 Institucional SiSFarm"
        if seal_mode == "institutional_ecnpj"
        else "Selo Criptográfico Interno SiSFarm"
        if seal_mode == "internal_hmac"
        else "Em andamento"
    )
    now_sp = datetime.now(electronic.SP_TZ)
    code = snapshot["code"]
    return {
        "tone": tone,
        "source": "pdf" if via_pdf else "code",
        "headline": headline,
        "summary": summary,
        "guidance": guidance,
        "title": snapshot["title"],
        "origin": snapshot["farm"].name if snapshot.get("farm") else "SiSFarm",
        "code": code,
        "date": _format_sp_dt(snapshot.get("sealed_at")) if completed else f"{len(evidences)} assinatura(s) · {pending} pendente(s)",
        "integrity": "Íntegro (sem alterações)" if via_pdf else ("Registro confirmado" if completed else "Em andamento"),
        "integrity_bad": False,
        "file_name": filename,
        "sha256": snapshot.get("sealed_sha256") or snapshot.get("original_sha256") or "",
        "policy": f"{seal_label} · Registro oficial SiSFarm",
        "seal_note": _seal_transparency_note(seal_mode),
        "checked_at": now_sp.strftime("%d/%m/%Y às %H:%M"),
        "protocol": f"VAL-{now_sp.strftime('%Y%m%d-%H%M')}",
        "receipt_url": f"/validar/{code}/comprovante.pdf",
        "signers": signers,
    }


def _public_result_from_pdf_report(report: dict, *, uploaded_sha: str, filename: str) -> dict:
    state = report.get("state") or "unsigned"
    raw_sigs = report.get("signatures") or []
    now_sp = datetime.now(electronic.SP_TZ)
    checked_at = _format_sp_dt(report.get("checked_at")) or now_sp.strftime("%d/%m/%Y às %H:%M")

    if state == "valid":
        tone = "valid"
        headline = "Documento autêntico"
        summary = "Todas as assinaturas digitais foram confirmadas e o arquivo não sofreu nenhuma alteração."
        guidance = ""
        integrity = "Íntegro (sem alterações)"
        integrity_bad = False
    elif state == "invalid":
        tone = "invalid"
        headline = "Documento inválido"
        summary = "Este arquivo foi alterado depois de assinado ou possui uma assinatura inválida."
        guidance = "Não aceite esta cópia. Solicite ao remetente o arquivo PDF original sem modificações."
        integrity = "Alteração detectada"
        integrity_bad = True
    elif state == "indeterminate":
        tone = "indeterminate"
        headline = "Não foi possível confirmar a validade"
        summary = report.get("message") or (
            "O arquivo parece intacto, mas não conseguimos consultar a autoridade certificadora de uma assinatura agora."
        )
        guidance = (
            "Isso não significa necessariamente fraude. Tente validar novamente em alguns minutos ou "
            "confirme a origem diretamente com quem emitiu o documento."
        )
        integrity = "Conteúdo preservado" if all(s.get("integrity") for s in raw_sigs) and raw_sigs else "Não confirmada"
        integrity_bad = False
    else:
        tone = "unsigned"
        headline = "Sem assinatura digital"
        summary = "Este arquivo PDF não possui nenhuma assinatura digital incorporada."
        guidance = (
            "Documentos impressos e escaneados ou salvos via 'Imprimir > Salvar como PDF' perdem a assinatura digital. "
            "Peça sempre o arquivo PDF original."
        )
        integrity = "Sem proteção digital"
        integrity_bad = False

    signers = []
    latest_dt = ""
    for idx, sig in enumerate(raw_sigs):
        s_state = sig.get("state") or "indeterminate"
        s_date = _format_sp_dt(sig.get("signed_at"))
        if s_date:
            latest_dt = s_date
        if s_state == "valid":
            type_label, type_tone = "Qualificada ICP-Brasil", "brand"
        elif s_state == "invalid":
            type_label, type_tone = "Invalidada", "err"
        else:
            type_label, type_tone = "Não confirmada", "warn"

        parts = []
        if sig.get("issuer"):
            parts.append(f"Emissor: {sig['issuer']}")
        if sig.get("valid_until"):
            parts.append(f"Certificado válido até {_format_sp_dt(sig['valid_until']).split(' ')[0]}")
        if sig.get("message"):
            parts.append(sig["message"])
        signers.append({
            "name": sig.get("holder") or f"Signatário {idx + 1}",
            "cpf": "Conforme certificado",
            "signed_at": s_date or "Data não informada no certificado",
            "type": type_label,
            "type_tone": type_tone,
            "status": s_state,
            "evidence": sig.get("field") or f"Assinatura {idx + 1}",
            "tech": " · ".join(parts) or "Assinatura digital embarcada no PDF.",
        })

    display_title = filename[:-4] if filename.lower().endswith(".pdf") else (filename or "Documento PDF analisado")
    return {
        "tone": tone,
        "source": "pdf",
        "headline": headline,
        "summary": summary,
        "guidance": guidance,
        "title": display_title,
        "origin": "Documento PDF externo",
        "code": "",
        "date": latest_dt or "—",
        "integrity": integrity,
        "integrity_bad": integrity_bad,
        "file_name": filename,
        "sha256": uploaded_sha,
        "policy": "Verificação criptográfica PDF (PAdES / ICP-Brasil)",
        "seal_note": "Análise criptográfica direta das assinaturas incorporadas no arquivo PDF enviado, sem armazenamento do documento.",
        "checked_at": checked_at,
        "protocol": f"VAL-{now_sp.strftime('%Y%m%d-%H%M')}",
        "receipt_url": "",
        "signers": signers,
    }


@router.get("/validar")
def public_validation_home(request: Request, codigo: str = ""):
    normalized = electronic.normalize_verification_code(codigo)
    if normalized:
        return RedirectResponse(url=f"/validar/{normalized}", status_code=303)
    return templates.TemplateResponse(
        request,
        "digital_signing_validate.html",
        {
            "request": request,
            "csrf_token": ensure_csrf_token(request),
            "query_code": codigo.strip().upper(),
            "not_found": bool(codigo.strip()),
            "snapshot": None,
            "receipt_payload": None,
        },
        headers=PRIVATE,
    )


@router.post("/validar/arquivo")
async def public_validation_upload_pdf(request: Request, db: Session = Depends(get_db)):
    ip = client_ip(request)
    if login_throttle.blocked_for("pub_val_pdf", ip):
        return _json({"detail": "Muitas consultas seguidas. Aguarde alguns instantes e tente novamente."}, 429)
    try:
        pdf_bytes = await _body(request, MAX_UPLOAD_BYTES, "application/pdf")
    except HTTPException as exc:
        return _json({"detail": exc.detail}, exc.status_code)

    raw_name = unquote(request.headers.get("X-Document-Name") or "documento.pdf").strip()
    filename = re.sub(r"[\r\n\x00/\\]+", " ", raw_name)[:160] or "documento.pdf"
    uploaded_sha = sha256(pdf_bytes).hexdigest()

    # 1. Procura primeiro se este PDF é um documento assinado/selado oficial do SiSFarm (por hash exato)
    doc = db.scalar(select(SigningDocument).filter_by(sealed_sha256=uploaded_sha))
    if doc is None:
        rev = db.scalar(
            select(SigningRevision)
            .where(SigningRevision.sha256 == uploaded_sha, SigningRevision.number > 1)
            .order_by(SigningRevision.number.desc())
        )
        if rev is not None:
            candidate = db.get(SigningDocument, rev.document_id)
            if candidate is not None and candidate.current_revision == rev.number:
                doc = candidate
    if doc is not None and doc.verification_code:
        snapshot = electronic.public_validation_snapshot(db, doc.verification_code)
        if snapshot is not None:
            return _json(_receipt_payload_from_snapshot(snapshot, via_pdf=True, filename=filename))

    # 2. Caso seja um PDF externo (ou alterado), executa a validação criptográfica completa
    try:
        report = await run_in_threadpool(_limited, validate_pdf, pdf_bytes)
    except (ValueError, workflow.WorkflowError) as exc:
        return _error(exc)
    return _json(_public_result_from_pdf_report(report, uploaded_sha=uploaded_sha, filename=filename))


@router.post("/validar/comprovante.pdf")
async def public_validation_receipt_from_payload(request: Request):
    try:
        payload = await _payload(request, limit=32768)
    except HTTPException as exc:
        return _json({"detail": exc.detail}, exc.status_code)
    pdf_bytes = build_validation_receipt_pdf(payload)
    safe_proto = re.sub(r"[^A-Za-z0-9_-]", "", str(payload.get("code") or payload.get("protocol") or "SiSFarm"))[:32] or "SiSFarm"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="Comprovante_Validacao_{safe_proto}.pdf"',
            **PRIVATE,
        },
    )


@router.get("/validar/{code}")
def public_validation_detail(code: str, request: Request, db: Session = Depends(get_db)):
    snapshot = electronic.public_validation_snapshot(db, code)
    if snapshot is None:
        return templates.TemplateResponse(
            request,
            "digital_signing_validate.html",
            {
                "request": request,
                "csrf_token": ensure_csrf_token(request),
                "query_code": code.strip().upper(),
                "not_found": True,
                "snapshot": None,
                "receipt_payload": None,
            },
            status_code=404,
            headers=PRIVATE,
        )
    receipt_payload = _receipt_payload_from_snapshot(snapshot, via_pdf=False)
    return templates.TemplateResponse(
        request,
        "digital_signing_validate.html",
        {
            "request": request,
            "csrf_token": ensure_csrf_token(request),
            "query_code": snapshot["code"],
            "not_found": False,
            "snapshot": snapshot,
            "receipt_payload": receipt_payload,
            "sp_tz": electronic.SP_TZ,
        },
        headers=PRIVATE,
    )


@router.get("/validar/{code}/comprovante.pdf")
def public_validation_receipt_by_code(code: str, db: Session = Depends(get_db)):
    snapshot = electronic.public_validation_snapshot(db, code)
    if snapshot is None:
        raise HTTPException(404, "Código de verificação não encontrado.")
    payload = _receipt_payload_from_snapshot(snapshot, via_pdf=False)
    pdf_bytes = build_validation_receipt_pdf(payload)
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="Comprovante_Validacao_{snapshot["code"]}.pdf"',
            **PRIVATE,
        },
    )


@router.post("/validar/{code}/conferir")
async def public_validation_check_pdf(code: str, request: Request, db: Session = Depends(get_db)):
    ip = client_ip(request)
    if login_throttle.blocked_for(f"pub_val:{code}", ip):
        return _json({"detail": "Muitas consultas seguidas. Aguarde alguns instantes e tente novamente."}, 429)
    snapshot = electronic.public_validation_snapshot(db, code)
    if snapshot is None:
        login_throttle.register_failure(f"pub_val:{code}", ip)
        return _json({"detail": "Código de verificação não encontrado."}, 404)
    try:
        pdf_bytes = await _body(request, MAX_PDF_BYTES, "application/pdf")
    except HTTPException as exc:
        return _json({"detail": exc.detail}, exc.status_code)
    uploaded_sha = sha256(pdf_bytes).hexdigest()
    matches = uploaded_sha in snapshot["accepted_hashes"]
    return _json({
        "matches": matches,
        "uploaded_sha256": uploaded_sha,
        "expected_sha256": snapshot["sealed_sha256"],
        "headline": "Documento original confirmado" if matches else "Este arquivo não é o mesmo que foi assinado",
        "message": (
            "O arquivo que você enviou é exatamente igual à versão assinada e não sofreu nenhuma alteração desde então."
            if matches
            else "Este PDF não confere com a versão assinada guardada no SiSFarm. Peça novamente o arquivo original a quem enviou o documento."
        ),
    })


# ---------------------------------------------------------------------------
# Painel Administrativo Geral — Selo Institucional e-CNPJ A1 (Superadmin)
# ---------------------------------------------------------------------------


def _require_super_admin(user: User):
    if not user or not is_super_admin_email(user.email):
        raise HTTPException(403, "Acesso restrito ao administrador geral do SiSFarm.")


@router.get("/admin/selo-institucional")
def admin_institutional_seal_page(request: Request, db: Session = Depends(get_db), user: User = Depends(get_current_user_web)):
    _require_super_admin(user)
    active_cert = electronic.get_active_institutional_certificate(db)
    history = db.scalars(
        select(InstitutionalSealingCertificate).order_by(InstitutionalSealingCertificate.created_at.desc()).limit(10)
    ).all()
    days_left = 0
    if active_cert is not None:
        days_left = max(0, (certificates.utc(active_cert.valid_until) - datetime.now(timezone.utc)).days)
    return _page(
        request,
        user,
        db,
        "organizations",
        "admin_institutional_seal.html",
        "Selo Institucional do SiSFarm",
        active_cert=active_cert,
        days_left=days_left,
        history=history,
    )


@router.post("/admin/selo-institucional")
async def admin_upload_institutional_seal(request: Request, db: Session = Depends(get_db), user: User = Depends(get_current_user_web)):
    _require_super_admin(user)
    data = await _payload(request, limit=272_000)
    raw_file = data.get("file")
    password = data.get("password")
    if data.get("consent") is not True or not isinstance(raw_file, str) or not isinstance(password, str):
        raise HTTPException(400, "Envie o arquivo do certificado, a senha e confirme o termo de uso institucional.")
    try:
        pfx_bytes = base64.b64decode(raw_file.encode("ascii"), validate=True)
        row = electronic.store_institutional_certificate(db, pfx_bytes=pfx_bytes, password=password, user=user)
        return _json({"ok": True, "id": row.id, "subject_cn": row.subject_cn}, 201)
    except (ValueError, binascii.Error, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


@router.post("/admin/selo-institucional/desativar")
async def admin_deactivate_institutional_seal(request: Request, db: Session = Depends(get_db), user: User = Depends(get_current_user_web)):
    _require_super_admin(user)
    await _payload(request)
    electronic.deactivate_institutional_certificate(db, user=user)
    return _json({"ok": True})
