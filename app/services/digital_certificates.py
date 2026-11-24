"""Certificate inventory and purpose boundaries shared by fiscal and document services."""
import base64
import hmac
import secrets
import re
from datetime import datetime, timezone
from uuid import uuid4

from asn1crypto.core import BMPString, IA5String, OctetString, PrintableString, UTF8String
from cryptography import x509
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy import or_, select

from app.core.config import get_settings
from app.models.digital_certificate import CertificateHolderAssignment, CertificatePurpose, DigitalCertificate, FiscalCertificateSelection
from app.models.farm import Farm
from app.models.signed_document import SigningChallenge
from app.models.user import User
from app.services.certificate_a1 import parse_and_validate_pfx

CENTRAL = "central"
DOCUMENTS = "documents"
FISCAL = "fiscal"
FISCAL_ENVIRONMENTS = ("homologacao", "producao")
HOLDER_OID = x509.ObjectIdentifier("2.16.76.1.3.1")
REPRESENTATIVE_OID = x509.ObjectIdentifier("2.16.76.1.3.4")
LEGAL_ENTITY_OID = x509.ObjectIdentifier("2.16.76.1.3.3")


def utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


class EncryptionConfigurationError(ValueError):
    """The configured encryption secret is absent or invalid."""


def encryption_key(setting: str) -> bytes:
    variable = setting.upper()
    configured = (getattr(get_settings(), setting) or "").strip()
    if not configured:
        raise EncryptionConfigurationError(f"A variável {variable} está ausente na implantação atual.")
    try:
        raw = base64.b64decode(configured, validate=True)
    except ValueError:
        raise EncryptionConfigurationError(f"A variável {variable} deve conter uma chave de 32 bytes em Base64.") from None
    if len(raw) != 32:
        raise EncryptionConfigurationError(f"A variável {variable} deve conter uma chave de 32 bytes em Base64.")
    return raw


def seal(value: bytes, key: bytes, context: bytes) -> bytes:
    nonce = secrets.token_bytes(12)
    return nonce + AESGCM(key).encrypt(nonce, value, context)


def unseal(value: bytes, key: bytes, context: bytes) -> bytes:
    return AESGCM(key).decrypt(value[:12], value[12:], context)


def _cert_context(cert: DigitalCertificate, use: str) -> bytes:
    return f"sisfarm-certificate-v1:{cert.organization_id}:{cert.farm_id}:{cert.id}:{cert.fingerprint_sha256}:{use}".encode()


def store_a1(db, *, organization_id: int, farm_id: int, user_id: int, pfx: bytes, password: str) -> DigitalCertificate:
    meta = parse_and_validate_pfx(pfx, password)
    cert = DigitalCertificate(
        id=uuid4().hex, organization_id=organization_id, farm_id=farm_id, uploaded_by=user_id,
        environment=CENTRAL, is_active=False, salt_b64="", nonce_b64="",
        subject_cn=meta["subject_cn"], issuer_cn=meta["issuer_cn"], serial_number=meta["serial_number"],
        fingerprint_sha256=meta["fingerprint_sha256"], valid_from=meta["valid_from"], valid_until=meta["valid_until"],
    )
    cert.ciphertext_b64 = base64.b64encode(
        seal(pfx, encryption_key("digital_certificate_key"), _cert_context(cert, "pkcs12"))
    ).decode()
    db.add(cert)
    return cert


def _open_pkcs12(cert: DigitalCertificate, password: str) -> dict:
    from app.services.nfe_certificate_vault import decrypt_certificate_from_vault

    if not password or len(password.encode()) > 1024:
        raise ValueError("Informe a senha do certificado A1.")
    if cert.environment == CENTRAL:
        pfx = unseal(base64.b64decode(cert.ciphertext_b64, validate=True),
                     encryption_key("digital_certificate_key"), _cert_context(cert, "pkcs12"))
    else:
        pfx, saved_password = decrypt_certificate_from_vault(cert)
        if not hmac.compare_digest(password.encode(), saved_password.encode()):
            raise ValueError("Senha do certificado A1 incorreta.")
    parsed = parse_and_validate_pfx(pfx, password)
    if not hmac.compare_digest(cert.fingerprint_sha256, parsed["fingerprint_sha256"]):
        raise ValueError("O certificado armazenado diverge do cadastro.")
    return parsed


def _responsible_cpf(certificate: x509.Certificate) -> str:
    try:
        other_names = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    except x509.ExtensionNotFound:
        return ""
    other_names = {item.type_id: item.value for item in other_names.get_values_for_type(x509.OtherName)}
    oid = REPRESENTATIVE_OID if LEGAL_ENTITY_OID in other_names else HOLDER_OID
    encoded = other_names.get(oid, b"")
    string_class = {4: OctetString, 12: UTF8String, 19: PrintableString, 22: IA5String, 30: BMPString}.get(
        encoded[0] if encoded else None
    )
    if not string_class:
        return ""
    try:
        value = string_class.load(encoded, strict=True).native
        value = value.decode("ascii") if isinstance(value, bytes) else value
    except (ValueError, UnicodeError):
        return ""
    cpf = value[8:19]
    return cpf if len(cpf) == 11 and cpf.isascii() and cpf.isdigit() else ""


def verify_responsible_identity(db, *, cert: DigitalCertificate, user: User, cpf: str, password: str, reference: str):
    """Called by the identity verification operator after validating identity outside the application."""
    check_responsible_identity(cert, user, cpf, password)
    assign_certificate_holder(db, cert, user)
    _bind_responsible_identity(db, cert, user, reference)


def check_responsible_identity(cert, user, cpf, password):
    if not user.is_active or user.organization_id != cert.organization_id:
        raise ValueError("A conta ativa precisa pertencer à organização do certificado.")
    if not re.fullmatch(r"[0-9]{11}|[0-9]{3}\.[0-9]{3}\.[0-9]{3}-[0-9]{2}", cpf):
        raise ValueError("Informe o CPF com 11 dígitos.")
    cpf = cpf.replace(".", "").replace("-", "")
    parsed = _open_pkcs12(cert, password)
    if not hmac.compare_digest(_responsible_cpf(parsed["certificate"]), cpf):
        raise ValueError("O CPF diverge do titular ou responsável identificado no certificado.")


def assign_certificate_holder(db, cert, user):
    if not user.is_active or user.organization_id != cert.organization_id:
        raise ValueError("Selecione um usuário ativo da organização.")
    lock_fiscal_farm(db, cert.organization_id, cert.farm_id)
    assignment = db.get(CertificateHolderAssignment, cert.id, populate_existing=True)
    if assignment is not None and assignment.user_id == user.id and assignment.email == user.email:
        return
    if assignment is not None and assignment.user_id != user.id:
        raise PermissionError(
            "Este certificado já possui um responsável vinculado e não pode ser alterado. "
            "Remova e cadastre novamente para vincular a outro usuário."
        )
    existing = db.get(CertificatePurpose, (cert.id, DOCUMENTS), with_for_update=True, populate_existing=True)
    if existing is not None and existing.user_id is not None and existing.user_id != user.id:
        raise PermissionError(
            "Este certificado já possui um responsável vinculado e não pode ser alterado. "
            "Remova e cadastre novamente para vincular a outro usuário."
        )
    if assignment is None:
        assignment = CertificateHolderAssignment(certificate_id=cert.id)
        db.add(assignment)
    assignment.user_id = user.id
    assignment.email = user.email


def assigned_to_user(db, cert, user):
    assignment = db.get(CertificateHolderAssignment, cert.id, populate_existing=True)
    return (assignment is not None and assignment.user_id == user.id and assignment.email == user.email
            and user.is_active and cert.organization_id == user.organization_id)


def _bind_responsible_identity(db, cert, user, reference):
    now = datetime.now(timezone.utc)
    for purpose in (DOCUMENTS, FISCAL):
        grant = db.get(CertificatePurpose, (cert.id, purpose), with_for_update=True)
        if grant is None:
            grant = CertificatePurpose(certificate_id=cert.id, purpose=purpose)
            db.add(grant)
        grant.user_id = user.id
        grant.identity_email = user.email
        grant.verification_reference = reference
        grant.verified_at = now
        grant.generation = uuid4().hex
        grant.secret_ciphertext = None
        grant.enabled_at = None
        grant.disabled_at = now
    if cert.environment == CENTRAL:
        selection = db.get(FiscalCertificateSelection, cert.id, with_for_update=True)
        if selection is not None:
            db.delete(selection)


def purpose_for_user(db, cert: DigitalCertificate, user: User, purpose: str, *, farm_id, enabled=True, lock=False):
    if (not user.is_active or cert.organization_id != user.organization_id
            or cert.farm_id != farm_id):
        return None
    grant = db.get(CertificatePurpose, (cert.id, purpose), with_for_update=lock, populate_existing=lock)
    if grant is None or grant.user_id != user.id or grant.identity_email != user.email:
        return None
    assignment = db.get(CertificateHolderAssignment, cert.id, populate_existing=True)
    if assignment is not None and (assignment.user_id != user.id or assignment.email != user.email):
        return None
    if enabled and (grant.enabled_at is None or grant.disabled_at is not None):
        return None
    return grant


def authorize_purpose(db, *, cert, user, farm_id, purpose, enable, password="", cpf="", environment="homologacao"):
    lock_fiscal_farm(db, cert.organization_id, cert.farm_id)
    grant = purpose_for_user(db, cert, user, purpose, farm_id=farm_id, enabled=False, lock=True)
    if grant is not None and enable:
        _open_pkcs12(cert, password)
    if grant is None:
        if not enable or cert.farm_id != farm_id or not assigned_to_user(db, cert, user):
            raise PermissionError("Somente a conta vinculada do titular ou responsável pode autorizar este uso.")
        check_responsible_identity(cert, user, cpf, password)
        _bind_responsible_identity(db, cert, user, "CPF e senha do A1 confirmados na Central")
        db.flush()
        grant = db.get(CertificatePurpose, (cert.id, purpose))
    now = datetime.now(timezone.utc)
    if purpose == FISCAL:
        _configure_fiscal(db, cert, enable, password, environment)
    grant.generation = uuid4().hex
    if purpose == DOCUMENTS:
        if enable:
            sealed = seal(
                password.encode(),
                encryption_key("digital_certificate_key"),
                _cert_context(cert, f"documents:{grant.generation}"),
            )
            grant.secret_ciphertext = base64.b64encode(sealed).decode()
        else:
            grant.secret_ciphertext = None
    grant.enabled_at = now if enable else grant.enabled_at
    grant.disabled_at = None if enable else now


def _configure_fiscal(db, cert, enable, password, environment):
    if environment not in FISCAL_ENVIRONMENTS:
        raise ValueError("Ambiente fiscal inválido.")
    if cert.environment != CENTRAL:
        if cert.environment != environment:
            raise ValueError("O ambiente de um certificado fiscal legado é fixo.")
        if enable:
            clear_fiscal_selection(db, cert.organization_id, cert.farm_id, environment)
            db.query(DigitalCertificate).filter_by(
                organization_id=cert.organization_id, farm_id=cert.farm_id, environment=environment,
            ).update({"is_active": False})
        cert.is_active = enable
        return
    selection = db.get(FiscalCertificateSelection, cert.id, with_for_update=True)
    if selection is not None:
        db.delete(selection)
    if not enable:
        return
    clear_fiscal_selection(db, cert.organization_id, cert.farm_id, environment)
    # Replacing a fiscal certificate never reactivates an older one on revocation.
    db.query(DigitalCertificate).filter_by(
        organization_id=cert.organization_id, farm_id=cert.farm_id, environment=environment,
    ).update({"is_active": False})
    sealed = seal(password.encode(), encryption_key("digital_certificate_key"), _cert_context(cert, f"fiscal:{environment}"))
    db.flush()
    db.add(FiscalCertificateSelection(certificate_id=cert.id, organization_id=cert.organization_id,
                                      farm_id=cert.farm_id, environment=environment,
                                      secret_ciphertext=base64.b64encode(sealed).decode()))


def lock_fiscal_farm(db, organization_id, farm_id):
    db.scalar(select(Farm.id).filter_by(id=farm_id, organization_id=organization_id).with_for_update())


def clear_fiscal_selection(db, organization_id: int, farm_id: int, environment: str):
    db.query(FiscalCertificateSelection).filter_by(
        organization_id=organization_id, farm_id=farm_id, environment=environment,
    ).delete(synchronize_session="fetch")


def fiscal_selection_for(cert, db):
    return db.scalar(select(FiscalCertificateSelection).filter_by(
        certificate_id=cert.id, organization_id=cert.organization_id, farm_id=cert.farm_id,
    ))


def fiscal_certificate_query(db, organization_id, farm_id, *, environment=None, active=False):
    assignments = (
        select(FiscalCertificateSelection.certificate_id)
        .join(CertificatePurpose, CertificatePurpose.certificate_id == FiscalCertificateSelection.certificate_id)
        .join(User, User.id == CertificatePurpose.user_id)
        .where(
            FiscalCertificateSelection.organization_id == organization_id,
            FiscalCertificateSelection.farm_id == farm_id,
            CertificatePurpose.purpose == FISCAL, CertificatePurpose.enabled_at.is_not(None),
            CertificatePurpose.disabled_at.is_(None), User.is_active.is_(True),
            User.organization_id == organization_id, User.email == CertificatePurpose.identity_email,
        )
    )
    if environment is not None:
        assignments = assignments.where(FiscalCertificateSelection.environment == environment)
    legacy = DigitalCertificate.environment.in_(FISCAL_ENVIRONMENTS)
    if environment is not None:
        legacy = legacy & (DigitalCertificate.environment == environment)
    if active:
        legacy = legacy & DigitalCertificate.is_active.is_(True)
    return db.query(DigitalCertificate).filter(
        DigitalCertificate.organization_id == organization_id, DigitalCertificate.farm_id == farm_id,
        or_(legacy, DigitalCertificate.id.in_(assignments)),
    )


def load_fiscal_pkcs12(cert, db):
    if cert.environment != CENTRAL:
        from app.services.nfe_certificate_vault import decrypt_certificate_from_vault
        return decrypt_certificate_from_vault(cert)
    selection = fiscal_selection_for(cert, db)
    grant = db.get(CertificatePurpose, (cert.id, FISCAL))
    holder = db.get(User, grant.user_id) if grant and grant.user_id is not None else None
    if (selection is None or holder is None or not holder.is_active or holder.email != grant.identity_email
            or holder.organization_id != cert.organization_id or not grant.enabled_at or grant.disabled_at):
        raise PermissionError("Certificado sem autorização fiscal ativa.")
    key = encryption_key("digital_certificate_key")
    return (
        unseal(base64.b64decode(cert.ciphertext_b64), key, _cert_context(cert, "pkcs12")),
        unseal(base64.b64decode(selection.secret_ciphertext), key,
               _cert_context(cert, f"fiscal:{selection.environment}")).decode(),
    )


def open_for_documents(db, cert, user, farm_id, password, generation):
    grant = purpose_for_user(db, cert, user, DOCUMENTS, farm_id=farm_id, lock=True)
    if grant is None or grant.generation != generation:
        raise PermissionError("O titular ou responsável precisa autorizar o uso deste certificado.")
    resolved_password = password or ""
    if not resolved_password:
        if grant.secret_ciphertext:
            resolved_password = unseal(
                base64.b64decode(grant.secret_ciphertext, validate=True),
                encryption_key("digital_certificate_key"),
                _cert_context(cert, f"documents:{grant.generation}"),
            ).decode()
        elif cert.environment != CENTRAL:
            from app.services.nfe_certificate_vault import decrypt_certificate_from_vault
            _, resolved_password = decrypt_certificate_from_vault(cert)
        else:
            raise ValueError("Reative o uso deste certificado na aba Certificados A1 para assinar sem digitar a senha.")
    return _open_pkcs12(cert, resolved_password)


def friendly_cert_info(cert: DigitalCertificate) -> dict:
    raw_cn = (cert.subject_cn or "").strip()
    name_part = raw_cn
    doc_part = ""
    if ":" in raw_cn:
        left, right = raw_cn.rsplit(":", 1)
        digits = re.sub(r"\D", "", right)
        if len(digits) in (11, 14):
            name_part = left.strip() or raw_cn
            doc_part = digits
    if len(doc_part) == 14:
        cert_type = "e-CNPJ A1"
        masked_doc = f"CNPJ {doc_part[:2]}.{doc_part[2:5]}.{doc_part[5:8]}/{doc_part[8:12]}-{doc_part[12:]}"
    elif len(doc_part) == 11:
        cert_type = "e-CPF A1"
        masked_doc = f"CPF •••.{doc_part[3:6]}.{doc_part[6:9]}-••"
    else:
        cert_type = "Certificado A1"
        masked_doc = ""
    issuer = (cert.issuer_cn or "").strip()
    return {
        "display_name": name_part or "Certificado A1",
        "cert_type": cert_type,
        "masked_doc": masked_doc,
        "issuer": issuer,
    }


def delete_certificate(db, *, cert: DigitalCertificate, user: User, farm_id: int, can_manage_fiscal: bool = False) -> None:
    if cert.organization_id != user.organization_id or cert.farm_id != farm_id:
        raise PermissionError("Certificado não pertence a esta fazenda.")
    lock_fiscal_farm(db, cert.organization_id, cert.farm_id)
    active_fiscal_ids = {
        row.id for row in fiscal_certificate_query(db, user.organization_id, farm_id, active=True).all()
    }
    is_fiscal_active = cert.id in active_fiscal_ids
    if is_fiscal_active and not can_manage_fiscal:
        raise PermissionError("Sua conta precisa da permissão fiscal para remover um certificado ativo na Nota Fiscal.")
    is_related_user = (
        cert.uploaded_by == user.id
        or assigned_to_user(db, cert, user)
        or purpose_for_user(db, cert, user, DOCUMENTS, farm_id=farm_id, enabled=False) is not None
    )
    if not can_manage_fiscal and not is_related_user:
        raise PermissionError("Você não tem permissão para remover este certificado.")
    db.query(SigningChallenge).filter_by(certificate_id=cert.id).delete(synchronize_session="fetch")
    db.query(FiscalCertificateSelection).filter_by(certificate_id=cert.id).delete(synchronize_session="fetch")
    db.query(CertificatePurpose).filter_by(certificate_id=cert.id).delete(synchronize_session="fetch")
    db.query(CertificateHolderAssignment).filter_by(certificate_id=cert.id).delete(synchronize_session="fetch")
    db.delete(cert)
