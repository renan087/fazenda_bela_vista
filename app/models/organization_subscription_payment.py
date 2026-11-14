from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Date, DateTime, ForeignKey, Numeric, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class OrganizationSubscriptionPayment(Base):
    __tablename__ = "organization_subscription_payments"

    id: Mapped[int] = mapped_column(primary_key=True)
    subscription_id: Mapped[int] = mapped_column(ForeignKey("organization_subscriptions.id", ondelete="RESTRICT"), nullable=False, index=True)
    asaas_payment_id: Mapped[str] = mapped_column(String(80), nullable=False, unique=True, index=True)
    status: Mapped[str] = mapped_column(String(40), nullable=False)
    amount: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    due_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    paid_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    last_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    subscription = relationship("OrganizationSubscription")


class AsaasWebhookEvent(Base):
    __tablename__ = "asaas_webhook_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[str] = mapped_column(String(200), nullable=False, unique=True, index=True)
    event_type: Mapped[str] = mapped_column(String(80), nullable=False)
    resource_id: Mapped[str] = mapped_column(String(80), nullable=False)
    event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
