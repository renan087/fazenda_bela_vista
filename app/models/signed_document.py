from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, JSON, LargeBinary, String, Text, UniqueConstraint, func, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class SigningDocument(Base):
    __tablename__ = "signing_documents"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id", ondelete="CASCADE"), index=True)
    owner_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True)
    owner_signs: Mapped[bool] = mapped_column(Boolean, default=True, server_default=text("true"))
    filename: Mapped[str] = mapped_column(String(180))
    title: Mapped[str | None] = mapped_column(String(180), nullable=True)
    category: Mapped[str | None] = mapped_column(String(48), nullable=True)
    verification_code: Mapped[str | None] = mapped_column(String(24), unique=True, index=True, nullable=True)
    signing_policy: Mapped[str] = mapped_column(String(24), default="flexible", server_default=text("'flexible'"))
    original_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    sealed_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    seal_mode: Mapped[str | None] = mapped_column(String(24), nullable=True)
    sealed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    current_revision: Mapped[int] = mapped_column(Integer, default=1)
    validation: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class SigningParticipant(Base):
    __tablename__ = "signing_participants"

    document_id: Mapped[str] = mapped_column(ForeignKey("signing_documents.id", ondelete="CASCADE"), primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True, index=True)
    granted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SigningRevision(Base):
    __tablename__ = "signing_document_revisions"
    __table_args__ = (UniqueConstraint("document_id", "number"),)

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    document_id: Mapped[str] = mapped_column(ForeignKey("signing_documents.id", ondelete="CASCADE"), index=True)
    number: Mapped[int] = mapped_column(Integer)
    sha256: Mapped[str] = mapped_column(String(64))
    encrypted_pdf: Mapped[bytes] = mapped_column(LargeBinary, deferred=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    signature: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SigningChallenge(Base):
    __tablename__ = "signing_challenges"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    document_id: Mapped[str] = mapped_column(ForeignKey("signing_documents.id", ondelete="CASCADE"), index=True)
    revision_id: Mapped[str] = mapped_column(ForeignKey("signing_document_revisions.id", ondelete="CASCADE"))
    revision_sha256: Mapped[str] = mapped_column(String(64))
    certificate_id: Mapped[str] = mapped_column(ForeignKey("nfe_certificate_vaults.id"))
    purpose_generation: Mapped[str] = mapped_column(String(32))
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    session_binding: Mapped[str] = mapped_column(String(64))
    appearance: Mapped[dict] = mapped_column(JSON)
    code_hash: Mapped[str] = mapped_column(String(64))
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SigningExternalSigner(Base):
    __tablename__ = "signing_external_signers"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    document_id: Mapped[str] = mapped_column(ForeignKey("signing_documents.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(160))
    email: Mapped[str | None] = mapped_column(String(180), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(40), nullable=True)
    channel: Mapped[str] = mapped_column(String(16), default="email")
    role_label: Mapped[str] = mapped_column(String(64), default="Signatário")
    access_token_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    token_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(24), default="pending")
    signed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SigningElectronicChallenge(Base):
    __tablename__ = "signing_electronic_challenges"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    document_id: Mapped[str] = mapped_column(ForeignKey("signing_documents.id", ondelete="CASCADE"), index=True)
    revision_id: Mapped[str] = mapped_column(ForeignKey("signing_document_revisions.id", ondelete="CASCADE"))
    revision_sha256: Mapped[str] = mapped_column(String(64))
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=True, index=True)
    external_signer_id: Mapped[str | None] = mapped_column(
        ForeignKey("signing_external_signers.id", ondelete="CASCADE"), nullable=True, index=True
    )
    signer_name: Mapped[str] = mapped_column(String(160))
    cpf_masked: Mapped[str] = mapped_column(String(24))
    cpf_encrypted: Mapped[str] = mapped_column(Text)
    cpf_sha256: Mapped[str] = mapped_column(String(64))
    auth_channel: Mapped[str] = mapped_column(String(16))
    auth_destination_masked: Mapped[str] = mapped_column(String(180))
    appearance: Mapped[dict] = mapped_column(JSON)
    consent_version: Mapped[str] = mapped_column(String(16), default="v1")
    consent_text_sha256: Mapped[str] = mapped_column(String(64))
    code_hash: Mapped[str] = mapped_column(String(64))
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SigningEvidence(Base):
    __tablename__ = "signing_evidences"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    evidence_code: Mapped[str] = mapped_column(String(24), unique=True, index=True)
    document_id: Mapped[str] = mapped_column(ForeignKey("signing_documents.id", ondelete="CASCADE"), index=True)
    revision_number: Mapped[int] = mapped_column(Integer)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    external_signer_id: Mapped[str | None] = mapped_column(
        ForeignKey("signing_external_signers.id", ondelete="SET NULL"), nullable=True
    )
    modality: Mapped[str] = mapped_column(String(32))
    signer_name: Mapped[str] = mapped_column(String(160))
    role_label: Mapped[str] = mapped_column(String(64), default="Signatário")
    cpf_masked: Mapped[str] = mapped_column(String(24), default="")
    cpf_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    cpf_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    auth_channel: Mapped[str] = mapped_column(String(32))
    auth_destination_masked: Mapped[str] = mapped_column(String(180), default="")
    ip_address: Mapped[str] = mapped_column(String(64), default="")
    ip_masked: Mapped[str] = mapped_column(String(64), default="")
    user_agent: Mapped[str] = mapped_column(String(300), default="")
    device_summary: Mapped[str] = mapped_column(String(120), default="")
    consent_version: Mapped[str] = mapped_column(String(16), default="v1")
    consent_text_sha256: Mapped[str] = mapped_column(String(64), default="")
    document_sha256_before: Mapped[str] = mapped_column(String(64), default="")
    document_sha256_after: Mapped[str] = mapped_column(String(64), default="")
    evidence_hash: Mapped[str] = mapped_column(String(64))
    cert_issuer: Mapped[str | None] = mapped_column(String(180), nullable=True)
    signed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class InstitutionalSealingCertificate(Base):
    __tablename__ = "institutional_sealing_certificates"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    subject_cn: Mapped[str] = mapped_column(String(255))
    cnpj_masked: Mapped[str] = mapped_column(String(32), default="")
    issuer_cn: Mapped[str] = mapped_column(String(255), default="")
    fingerprint_sha256: Mapped[str] = mapped_column(String(64))
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    valid_until: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    pfx_encrypted: Mapped[str] = mapped_column(Text)
    password_encrypted: Mapped[str] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default=text("true"))
    uploaded_by: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    deactivated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
