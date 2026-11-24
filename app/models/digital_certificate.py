from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DigitalCertificate(Base):
    """Armazenamento compartilhado; mantém a tabela e o formato dos registros fiscais legados."""
    __tablename__ = 'nfe_certificate_vaults'
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    environment: Mapped[str] = mapped_column(String(20), nullable=False, default='homologacao')
    subject_cn: Mapped[str] = mapped_column(String(240), nullable=False, default='')
    issuer_cn: Mapped[str] = mapped_column(String(240), nullable=False, default='')
    serial_number: Mapped[str] = mapped_column(String(80), nullable=False, default='')
    fingerprint_sha256: Mapped[str] = mapped_column(String(64), nullable=False, default='')
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    salt_b64: Mapped[str] = mapped_column(String(64), nullable=False)
    nonce_b64: Mapped[str] = mapped_column(String(64), nullable=False)
    ciphertext_b64: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    uploaded_by: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class CertificatePurpose(Base):
    __tablename__ = "digital_certificate_purposes"

    certificate_id: Mapped[str] = mapped_column(ForeignKey("nfe_certificate_vaults.id"), primary_key=True)
    purpose: Mapped[str] = mapped_column(String(24), primary_key=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True)
    identity_email: Mapped[str] = mapped_column(String(120))
    verification_reference: Mapped[str] = mapped_column(String(160))
    verified_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    generation: Mapped[str] = mapped_column(String(32))
    secret_ciphertext: Mapped[str | None] = mapped_column(Text, nullable=True)
    enabled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    disabled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class FiscalCertificateSelection(Base):
    __tablename__ = "digital_certificate_fiscal_selections"
    __table_args__ = (UniqueConstraint("organization_id", "farm_id", "environment"),)

    certificate_id: Mapped[str] = mapped_column(ForeignKey("nfe_certificate_vaults.id"), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True)
    farm_id: Mapped[int] = mapped_column(Integer, index=True)
    environment: Mapped[str] = mapped_column(String(20))
    secret_ciphertext: Mapped[str] = mapped_column(Text)


class CertificateHolderAssignment(Base):
    __tablename__ = "digital_certificate_holder_assignments"

    certificate_id: Mapped[str] = mapped_column(ForeignKey("nfe_certificate_vaults.id"), primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    email: Mapped[str] = mapped_column(String(120))
