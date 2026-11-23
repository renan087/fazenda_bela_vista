"""Authenticated signing operations and encrypted document revisions."""
import hmac
import json
import re
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from threading import BoundedSemaphore
from uuid import uuid4

from cryptography.exceptions import InvalidTag
from sqlalchemy import or_, select, update

from app.core.security import generate_numeric_code, hash_persistent_token, hash_verification_code
from app.models.audit_log import AuditLog
from app.models.digital_certificate import CertificatePurpose, DigitalCertificate
from app.models.signed_document import SigningChallenge, SigningDocument, SigningParticipant, SigningRevision
from app.models.user import User
from app.services.digital_certificates import (
    DOCUMENTS, encryption_key, open_for_documents, purpose_for_user, seal, unseal, utc,
)
from app.services.document_signing.pdf import (
    MAX_UPLOAD_BYTES, a1_signer, check_appearance, read_pdf, sign_document, validate_pdf,
)
from app.services.document_signing.policy import POLICY_OID, signing_mode
from app.services.email_service import send_plain_email

CHALLENGE_MINUTES = 5
MAX_ATTEMPTS = 5
CRYPTO_SLOTS = BoundedSemaphore(2)


class WorkflowError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def audit(db, user, event, *, outcome="success", **metadata):
    db.add(AuditLog(
        actor_user_id=user.id, actor_email=user.email, organization_id=user.organization_id,
        event_type=f"documentos.{event}", outcome=outcome, metadata_json=json.dumps(metadata, ensure_ascii=False),
    ))


def document_query(db, user, farm_id):
    access = select(SigningParticipant.document_id).filter_by(user_id=user.id)
    return select(SigningDocument).where(
        SigningDocument.organization_id == user.organization_id, SigningDocument.farm_id == farm_id,
        or_(SigningDocument.owner_id == user.id, SigningDocument.id.in_(access)),
    )


def accessible_document(db, user, farm_id, document_id, *, lock=False, owner=False):
    if lock:
        # Lock before checking participation; a concurrent revocation must win or wait.
        stmt = select(SigningDocument).filter_by(
            id=document_id, organization_id=user.organization_id, farm_id=farm_id,
        ).with_for_update()
        row = db.scalar(stmt.execution_options(populate_existing=True))
        if row is not None and (row.owner_id == user.id or not owner and
                               db.get(SigningParticipant, (row.id, user.id), populate_existing=True) is not None):
            return row
    else:
        row = db.scalar(document_query(db, user, farm_id).filter_by(id=document_id))
        if row is not None and (not owner or row.owner_id == user.id):
            return row
    raise WorkflowError("Documento não encontrado.", 404)


def current_revision(db, doc):
    revision = db.scalar(select(SigningRevision).filter_by(document_id=doc.id, number=doc.current_revision))
    if revision is None:
        raise WorkflowError("Versão do documento indisponível.", 409)
    return revision


def _associated_data(doc, revision):
    return f"sisfarm-pdf-v1:{doc.organization_id}:{doc.farm_id}:{doc.id}:{revision.id}:{revision.number}:{revision.sha256}".encode()


def revision_bytes(doc, revision):
    try:
        raw = unseal(revision.encrypted_pdf, encryption_key("digital_document_key"), _associated_data(doc, revision))
    except (InvalidTag, ValueError) as exc:
        raise WorkflowError("Não foi possível abrir a versão protegida do documento.", 503) from exc
    if not hmac.compare_digest(sha256(raw).hexdigest(), revision.sha256):
        raise WorkflowError("A integridade da versão armazenada falhou.", 409)
    return raw


def _store_revision(db, doc, user, data, *, signature=None):
    revision = SigningRevision(id=uuid4().hex, document_id=doc.id, number=doc.current_revision,
                               user_id=user.id, sha256=sha256(data).hexdigest(), signature=signature)
    revision.encrypted_pdf = seal(data, encryption_key("digital_document_key"), _associated_data(doc, revision))
    db.add(revision)
    return revision


def _filename(name):
    name = name.replace("\\", "/").rsplit("/", 1)[-1]
    name = re.sub(r"[\x00-\x1f\x7f\u202a-\u202e\u2066-\u2069]", "", name).strip()
    if name.lower().endswith(".pdf"):
        name = name[:-4]
    return f"{name[:170] or 'Documento'}.pdf"


def import_pdf(db, user, farm_id, name, data):
    if len(data) > MAX_UPLOAD_BYTES:
        raise WorkflowError("O PDF ultrapassa o limite de 12 MB.", 413)
    # Existing signatures are inspected on import without keeping network requests inside this transaction.
    validation = validate_pdf(data, network=False)
    doc = SigningDocument(id=uuid4().hex, organization_id=user.organization_id, farm_id=farm_id,
                          owner_id=user.id, filename=_filename(name), current_revision=1, validation=validation)
    db.add(doc)
    db.flush()
    revision = _store_revision(db, doc, user, data)
    audit(db, user, "importar", farm_id=farm_id, document_id=doc.id, sha256=revision.sha256)
    db.commit()
    return doc.id


def document_certificate(db, user, farm_id, certificate_id, *, lock=False):
    cert = db.scalar(select(DigitalCertificate).filter_by(
        id=certificate_id, organization_id=user.organization_id, farm_id=farm_id,
    ))
    if cert is None:
        raise WorkflowError("Certificado não encontrado.", 404)
    grant = purpose_for_user(db, cert, user, DOCUMENTS, farm_id=farm_id, lock=lock)
    now = datetime.now(timezone.utc)
    if grant is None or not utc(cert.valid_from) <= now <= utc(cert.valid_until):
        raise WorkflowError("Este certificado não está válido e habilitado para assinatura pela sua conta.", 403)
    return cert, grant


def usable_certificates(db, user, farm_id):
    now = datetime.now(timezone.utc)
    return db.scalars(
        select(DigitalCertificate).join(CertificatePurpose, CertificatePurpose.certificate_id == DigitalCertificate.id)
        .where(
            DigitalCertificate.organization_id == user.organization_id, DigitalCertificate.farm_id == farm_id,
            DigitalCertificate.valid_from <= now, DigitalCertificate.valid_until >= now,
            CertificatePurpose.purpose == DOCUMENTS, CertificatePurpose.user_id == user.id,
            CertificatePurpose.identity_email == user.email, CertificatePurpose.enabled_at.is_not(None),
            CertificatePurpose.disabled_at.is_(None),
        ).order_by(DigitalCertificate.subject_cn)
    ).all()


def require_signing(farm_id):
    if signing_mode(farm_id) == "blocked":
        raise WorkflowError("A assinatura está aguardando homologação no VALIDAR do ITI.", 503)


def _lock_account(db, user):
    email = user.email
    account = db.scalar(select(User).where(User.id == user.id).with_for_update().execution_options(populate_existing=True))
    if account is None or not account.is_active or account.email != email:
        raise WorkflowError("Sua conta foi alterada. Inicie uma nova sessão.", 403)


def issue_challenge(db, user, farm_id, document_id, certificate_id, appearance, revision_sha256, session_nonce):
    require_signing(farm_id)
    _lock_account(db, user)
    doc = accessible_document(db, user, farm_id, document_id, lock=True)
    revision = current_revision(db, doc)
    if not hmac.compare_digest(revision.sha256, revision_sha256):
        raise WorkflowError("O documento foi atualizado. Confira a nova versão antes de assinar.", 409)
    cert, grant = document_certificate(db, user, farm_id, certificate_id, lock=True)
    approved = check_appearance(read_pdf(revision_bytes(doc, revision)), appearance)
    now = datetime.now(timezone.utc)
    latest = db.scalar(select(SigningChallenge.created_at).filter_by(user_id=user.id)
                       .order_by(SigningChallenge.created_at.desc()).limit(1))
    if latest and now - utc(latest) < timedelta(seconds=40):
        raise WorkflowError("Aguarde 40 segundos antes de solicitar outro código.", 429)
    db.execute(update(SigningChallenge).where(
        SigningChallenge.user_id == user.id, SigningChallenge.consumed_at.is_(None),
    ).values(consumed_at=now))
    code = generate_numeric_code(8)
    challenge_id = uuid4().hex
    challenge = SigningChallenge(
        id=challenge_id, document_id=doc.id, revision_id=revision.id, revision_sha256=revision.sha256,
        certificate_id=cert.id, purpose_generation=grant.generation, user_id=user.id,
        session_binding=hash_persistent_token(session_nonce), appearance=approved,
        code_hash=hash_verification_code(challenge_id + code), expires_at=now + timedelta(minutes=CHALLENGE_MINUTES),
    )
    db.add(challenge)
    digest = revision.sha256
    filename = doc.filename
    holder = cert.subject_cn
    email = user.email
    db.commit()
    try:
        send_plain_email(
            email, "Confirme a assinatura do seu PDF no SiSFarm",
            f"Documento: {filename}\nTitular do certificado: {holder}\nSHA-256: {digest}\n\n"
            f"Código de autorização: {code}\nVálido por {CHALLENGE_MINUTES} minutos e para esta versão do PDF.\n"
            "Insira o código somente na tela do SiSFarm em que você iniciou a assinatura.",
            "document_signing",
        )
    except RuntimeError as exc:
        db.execute(update(SigningChallenge).filter_by(id=challenge_id).values(consumed_at=now))
        db.commit()
        raise WorkflowError("O código não pôde ser enviado. Tente novamente em instantes.", 503) from exc
    return {"challenge_id": challenge_id, "holder": holder, "expires_minutes": CHALLENGE_MINUTES, "sha256": digest}


def confirm_signature(db, user, farm_id, document_id, challenge_id, code, password, session_nonce):
    require_signing(farm_id)
    _lock_account(db, user)
    doc = accessible_document(db, user, farm_id, document_id, lock=True)
    challenge = db.scalar(select(SigningChallenge).filter_by(
        id=challenge_id, user_id=user.id, document_id=doc.id,
    ).with_for_update().execution_options(populate_existing=True))
    now = datetime.now(timezone.utc)
    if (challenge is None or challenge.consumed_at or challenge.attempts >= MAX_ATTEMPTS
            or utc(challenge.expires_at) <= now
            or not hmac.compare_digest(challenge.session_binding, hash_persistent_token(session_nonce))):
        raise WorkflowError("Autorização indisponível. Solicite um novo código.", 409)
    revision = current_revision(db, doc)
    if revision.id != challenge.revision_id or revision.sha256 != challenge.revision_sha256:
        raise WorkflowError("O documento mudou. Confira a nova versão e solicite outro código.", 409)
    cert, grant = document_certificate(db, user, farm_id, challenge.certificate_id, lock=True)
    if grant.generation != challenge.purpose_generation:
        raise WorkflowError("A autorização do certificado mudou. Solicite um novo código.", 409)
    challenge.attempts += 1
    try:
        if not hmac.compare_digest(challenge.code_hash, hash_verification_code(challenge.id + code)):
            raise ValueError("Código de autorização incorreto.")
        material = open_for_documents(db, cert, user, farm_id, password, challenge.purpose_generation)
    except (InvalidTag, PermissionError, ValueError) as exc:
        if challenge.attempts >= MAX_ATTEMPTS:
            challenge.consumed_at = now
        audit(db, user, "autorizar", outcome="denied", farm_id=farm_id, document_id=doc.id)
        db.commit()
        raise WorkflowError("Confira o código e a senha do A1 ou solicite uma nova autorização.", 403) from exc

    data = revision_bytes(doc, revision)
    challenge.consumed_at = now
    try:
        signed, field_name, validation = sign_document(data, a1_signer(material), challenge.appearance)
    except ValueError as exc:
        audit(db, user, "assinar", outcome="failed", farm_id=farm_id, document_id=doc.id, sha256=revision.sha256)
        db.commit()
        raise WorkflowError(str(exc), 422) from exc
    finally:
        del material
    doc.current_revision += 1
    signature = {
        "field": field_name, "holder": cert.subject_cn, "operator": user.display_name or user.name,
        "fingerprint": cert.fingerprint_sha256, "policy": POLICY_OID, "authorized_at": now.isoformat(),
        "input_sha256": revision.sha256, "appearance": challenge.appearance,
    }
    output = _store_revision(db, doc, user, signed, signature=signature)
    doc.validation = validation
    audit(db, user, "assinar", farm_id=farm_id, document_id=doc.id,
          output_sha256=output.sha256, **signature)
    db.commit()
    return {"revision": doc.current_revision, "sha256": output.sha256, "validation": validation}
