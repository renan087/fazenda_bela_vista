from datetime import datetime
from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base


class NfeDraft(Base):
    """Rascunho isolado; não representa NF-e nem possui validade fiscal."""
    __tablename__ = 'nfe_drafts'
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    sale_code: Mapped[str] = mapped_column(String(32), nullable=False)
    snapshot_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


from sqlalchemy import UniqueConstraint


class NfeIssuerProfile(Base):
    """Cadastro preparatório, sem certificado, credenciamento ou autorização fiscal."""
    __tablename__ = 'nfe_issuer_profiles'
    __table_args__ = (UniqueConstraint('organization_id', 'farm_id', name='uq_nfe_issuer_farm'),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(180), nullable=False, default='')
    cpf: Mapped[str] = mapped_column(String(11), nullable=False, default='')
    state_registration: Mapped[str] = mapped_column(String(30), nullable=False, default='')
    postal_code: Mapped[str] = mapped_column(String(8), nullable=False, default='')
    street: Mapped[str] = mapped_column(String(180), nullable=False, default='')
    number: Mapped[str] = mapped_column(String(30), nullable=False, default='')
    complement: Mapped[str] = mapped_column(String(120), nullable=False, default='')
    district: Mapped[str] = mapped_column(String(120), nullable=False, default='')
    city: Mapped[str] = mapped_column(String(120), nullable=False, default='')
    municipality_code: Mapped[str] = mapped_column(String(7), nullable=False, default='')
    state: Mapped[str] = mapped_column(String(2), nullable=False, default='BA')
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class NfeDraftDetails(Base):
    """Dados editáveis apenas para preparação, sem regras fiscais presumidas."""
    __tablename__ = 'nfe_draft_details'
    draft_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    data_json: Mapped[str] = mapped_column(Text, nullable=False, default='{}')
    updated_by: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
