from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, String, func, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class OrganizationAccessGrant(Base):
    """Liberacao manual (contingencial) do plano para uma organizacao.

    Nao altera cobrancas nem registra pagamento: apenas garante o acesso no periodo informado,
    com motivo, justificativa e responsavel registrados. Encerrada por data ou manualmente.
    """

    __tablename__ = "organization_access_grants"

    id: Mapped[int] = mapped_column(primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    plan_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("subscription_plans.id", ondelete="RESTRICT"), nullable=True)
    reason: Mapped[str] = mapped_column(String(40), nullable=False)
    justification: Mapped[str] = mapped_column(String(500), nullable=False)
    starts_on: Mapped[date] = mapped_column(Date, nullable=False)
    ends_on: Mapped[date] = mapped_column(Date, nullable=False)
    created_by_user_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_by_email: Mapped[str | None] = mapped_column(String(254), nullable=True)
    extension_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_by_email: Mapped[str | None] = mapped_column(String(254), nullable=True)
    revoke_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    organization = relationship("Organization")
    plan = relationship("SubscriptionPlan")
