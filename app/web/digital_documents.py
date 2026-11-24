"""Authenticated web entry points for signing, validation and certificate purposes."""
import base64
import binascii
import json
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
from app.services import digital_certificates as certificates, login_throttle
from app.services.audit_log_service import client_ip
from app.services.document_signing import workflow
from app.services.document_signing.pdf import MAX_UPLOAD_BYTES, read_pdf, signing_options, validate_pdf
from app.services.document_signing.policy import signing_mode
from app.services.nfe_permissions import has_fiscal_permission
from app.web.routes import _base_context, _repository, templates

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


@router.get("/assinatura-digital")
def signing_home(request: Request, db: Session = Depends(get_db), user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user, required=False)
    history = db.scalars(workflow.document_query(db, user, farm.id)
                         .order_by(SigningDocument.updated_at.desc(), SigningDocument.id).limit(100)).all() if farm else []
    return _page(request, user, db, "digital_signing", "digital_signing.html", "Assinatura de documentos",
                 farm=farm, history=history, mode=signing_mode(farm.id if farm else None))


@router.post("/assinatura-digital/documentos")
async def document_import(request: Request, db: Session = Depends(get_db), user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    try:
        data = await _body(request, MAX_UPLOAD_BYTES, "application/pdf")
        name = unquote(request.headers.get("X-Document-Name", "Documento.pdf"))
        doc_id = await run_in_threadpool(_limited, workflow.import_pdf, db, user, farm.id, name, data)
        return _json({"url": f"/assinatura-digital/documentos/{doc_id}"}, 201)
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
    participants = []
    if doc.owner_id == user.id:
        participants = db.scalars(select(User).join(SigningParticipant, SigningParticipant.user_id == User.id)
                                  .where(SigningParticipant.document_id == doc.id,
                                         User.organization_id == user.organization_id).order_by(User.name)).all()
    revisions = db.scalars(select(SigningRevision).filter_by(document_id=doc.id)
                           .order_by(SigningRevision.number.desc())).all()
    return _page(request, user, db, "digital_signing", "digital_signing_workspace.html", doc.filename,
                 farm=farm, doc=doc, options=options, revisions=revisions, participants=participants,
                 certificate_options=workflow.usable_certificates(db, user, farm.id), mode=signing_mode(farm.id))


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
    if (data.get("consent") is not True or not session_nonce or
            not all(isinstance(data.get(key), str) for key in ("challenge_id", "code", "password"))):
        raise HTTPException(400, "Confirme a assinatura e informe o código e a senha do A1.")
    try:
        result = await run_in_threadpool(_limited, workflow.confirm_signature, db, user, farm.id, document_id,
                                         data["challenge_id"], data["code"].strip(), data["password"], session_nonce)
        return _json(result)
    except (ValueError, workflow.WorkflowError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)


@router.post("/assinatura-digital/documentos/{document_id}/participantes")
async def add_participant(document_id: str, request: Request, db: Session = Depends(get_db),
                          user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    data = await _payload(request)
    email = str(data.get("email", "")).strip().lower()
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
    return _json({"ok": True})


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
    manage = has_fiscal_permission(user, "nfe.certificate.manage")
    rows = []
    organization_users = []
    if farm:
        inventory = select(DigitalCertificate).filter_by(organization_id=user.organization_id, farm_id=farm.id)
        if manage:
            organization_users = db.scalars(
                select(User).filter_by(organization_id=user.organization_id, is_active=True).order_by(User.name, User.id)
            ).all()
        else:
            relevant = select(CertificatePurpose.certificate_id).filter_by(user_id=user.id, identity_email=user.email)
            assigned = select(CertificateHolderAssignment.certificate_id).filter_by(user_id=user.id, email=user.email)
            inventory = inventory.where(or_(DigitalCertificate.uploaded_by == user.id, DigitalCertificate.id.in_(relevant),
                                            DigitalCertificate.id.in_(assigned)))
        active_fiscal = {cert.id for cert in certificates.fiscal_certificate_query(
            db, user.organization_id, farm.id, active=True,
        )}
        for cert in db.scalars(inventory.order_by(DigitalCertificate.created_at.desc())).all():
            docs = certificates.purpose_for_user(db, cert, user, certificates.DOCUMENTS, farm_id=farm.id, enabled=False)
            fiscal = certificates.purpose_for_user(db, cert, user, certificates.FISCAL, farm_id=farm.id, enabled=False)
            assigned = certificates.assigned_to_user(db, cert, user)
            assignment = db.get(CertificateHolderAssignment, cert.id)
            existing = db.get(CertificatePurpose, (cert.id, certificates.DOCUMENTS))
            now = datetime.now(timezone.utc)
            rows.append({
                "cert": cert, "fiscal_active": cert.id in active_fiscal, "can_set_fiscal": bool(manage and (fiscal or (assigned and cert.id not in active_fiscal))),
                "holder_id": assignment.user_id if assignment else existing.user_id if existing else None,
                "confirm_cpf": assigned and docs is None,
                "can_set_docs": bool(docs or assigned), "docs_enabled": bool(docs and docs.enabled_at and not docs.disabled_at),
                "valid": certificates.utc(cert.valid_from) <= now <= certificates.utc(cert.valid_until),
            })
    return _page(request, user, db, "certificate_center", "certificate_center.html", "Central de Certificados",
                 farm=farm, inventory=rows, organization_users=organization_users)


@router.post("/certificados-digitais")
async def register_certificate(request: Request, db: Session = Depends(get_db), user: User = Depends(get_current_user_web)):
    farm = _farm(request, db, user)
    data = await _payload(request, limit=272_000)
    if not isinstance(data.get("file"), str) or not isinstance(data.get("password"), str):
        raise HTTPException(400, "Selecione um arquivo A1 e informe a senha.")
    try:
        pfx = base64.b64decode(data["file"], validate=True)
        cert = await run_in_threadpool(_limited, certificates.store_a1, db, organization_id=user.organization_id,
                                       farm_id=farm.id, user_id=user.id, pfx=pfx, password=data["password"])
        workflow.audit(db, user, "cadastrar_certificado", farm_id=farm.id, fingerprint=cert.fingerprint_sha256)
        db.commit()
        return _json({"id": cert.id}, 201)
    except (ValueError, binascii.Error, workflow.WorkflowError, IntegrityError) as exc:
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
    cert = db.scalar(select(DigitalCertificate).filter_by(
        id=certificate_id, organization_id=user.organization_id, farm_id=farm.id,
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
    except (ValueError, IntegrityError) as exc:
        db.rollback()
        return _error(exc)
