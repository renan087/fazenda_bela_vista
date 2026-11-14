from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Date, DateTime, ForeignKey, Integer, Numeric, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class OrganizationSubscription(Base):
    __tablename__ = "organization_subscriptions"

    id: Mapped[int] = mapped_column(primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id", ondelete="RESTRICT"), nullable=False, unique=True, index=True)
    plan_name: Mapped[str] = mapped_column(String(120), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    cycle: Mapped[str] = mapped_column(String(20), nullable=False)
    billing_type: Mapped[str] = mapped_column(String(20), nullable=False)
    first_due_date: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="criando")
    asaas_subscription_id: Mapped[str | None] = mapped_column(String(80), nullable=True, unique=True)
    asaas_status: Mapped[str | None] = mapped_column(String(30), nullable=True)
    asaas_last_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    environment: Mapped[str] = mapped_column(String(20), nullable=False, default="sandbox")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    organization = relationship("Organization")
