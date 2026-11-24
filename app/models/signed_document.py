from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, JSON, LargeBinary, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class SigningDocument(Base):
    __tablename__ = "signing_documents"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id", ondelete="CASCADE"), index=True)
    owner_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True)
    filename: Mapped[str] = mapped_column(String(180))
    title: Mapped[str | None] = mapped_column(String(180), nullable=True)
    category: Mapped[str | None] = mapped_column(String(48), nullable=True)
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
