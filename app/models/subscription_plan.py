from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, Integer, Numeric, String, Text, UniqueConstraint, func, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class SubscriptionPlan(Base):
    """Versao de um plano comercial. Planos com assinantes nao mudam condicoes; cria-se nova versao."""

    __tablename__ = "subscription_plans"
    __table_args__ = (UniqueConstraint("code", "version", name="uq_subscription_plan_code_version"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    description: Mapped[str | None] = mapped_column(String(500), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="rascunho")
    price_monthly: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    price_quarterly: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    price_semiannual: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    price_yearly: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    max_farms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_users: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_hectares: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    modules_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    trial_days: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    display_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    is_featured: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
