from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Index, Integer, Numeric, String, func, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class OrganizationSubscription(Base):
    __tablename__ = "organization_subscriptions"
    __table_args__ = (
        Index('uq_org_subscription_current', 'organization_id', unique=True, postgresql_where=text('is_current'), sqlite_where=text('is_current = 1')),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id", ondelete="RESTRICT"), nullable=False, index=True)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text('true'))
    plan_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("subscription_plans.id", ondelete="RESTRICT"), nullable=True, index=True)
    plan_name: Mapped[str] = mapped_column(String(120), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    cycle: Mapped[str] = mapped_column(String(20), nullable=False)
    billing_type: Mapped[str] = mapped_column(String(20), nullable=False)
    first_due_date: Mapped[date] = mapped_column(Date, nullable=False)
    next_due_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="criando")
    asaas_subscription_id: Mapped[str | None] = mapped_column(String(80), nullable=True, unique=True)
    asaas_status: Mapped[str | None] = mapped_column(String(30), nullable=True)
    asaas_last_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    mercadopago_id: Mapped[str | None] = mapped_column(String(80), unique=True)
    mercadopago_reference: Mapped[str | None] = mapped_column(String(80), unique=True)
    mercadopago_status: Mapped[str | None] = mapped_column(String(30))
    mercadopago_version: Mapped[int | None] = mapped_column(Integer)
    mercadopago_checkout_url: Mapped[str | None] = mapped_column(String(512))
    mercadopago_payer_email: Mapped[str | None] = mapped_column(String(254))
    mercadopago_ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False))
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    sync_issue: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # Troca de plano: data da ultima troca e downgrade agendado para o fim do ciclo pago.
    plan_changed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    pending_plan_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("subscription_plans.id", ondelete="RESTRICT"), nullable=True)
    pending_plan_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    pending_amount: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    pending_cycle: Mapped[str | None] = mapped_column(String(20), nullable=True)
    pending_effective_on: Mapped[date | None] = mapped_column(Date, nullable=True)
    pending_previous_amount: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    pending_previous_cycle: Mapped[str | None] = mapped_column(String(20), nullable=True)
    environment: Mapped[str] = mapped_column(String(20), nullable=False, default="sandbox")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    organization = relationship("Organization")
    plan = relationship("SubscriptionPlan", foreign_keys=[plan_id])
    pending_plan = relationship("SubscriptionPlan", foreign_keys=[pending_plan_id])

    @property
    def billing_status(self) -> str | None:
        if self.mercadopago_reference:
            return {"authorized": "ACTIVE", "paused": "INACTIVE", "pending": "PENDING",
                    "cancelled": "DELETED", "canceled": "DELETED"}.get(self.mercadopago_status)
        return self.asaas_status
