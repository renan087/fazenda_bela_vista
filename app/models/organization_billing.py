from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class OrganizationBilling(Base):
    __tablename__ = "organization_billing"

    id: Mapped[int] = mapped_column(primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    contact_name: Mapped[str] = mapped_column(String(160), nullable=False)
    contact_email: Mapped[str] = mapped_column(String(254), nullable=False)
    contact_phone: Mapped[str | None] = mapped_column(String(40), nullable=True)
    cpf_cnpj: Mapped[str | None] = mapped_column(String(14), nullable=True)
    postal_code: Mapped[str | None] = mapped_column(String(8), nullable=True)
    address_number: Mapped[str | None] = mapped_column(String(40), nullable=True)
    address: Mapped[str | None] = mapped_column(String(255), nullable=True)
    address_complement: Mapped[str | None] = mapped_column(String(255), nullable=True)
    province: Mapped[str | None] = mapped_column(String(120), nullable=True)
    asaas_customer_id: Mapped[str | None] = mapped_column(String(40), nullable=True, unique=True)
    asaas_environment: Mapped[str | None] = mapped_column(String(20), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    organization = relationship("Organization")
