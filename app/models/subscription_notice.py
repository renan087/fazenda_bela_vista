from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class SubscriptionNotice(Base):
    """Aviso de cobranca enviado (ou deliberadamente pulado). Garante um unico envio por cobranca e tipo."""

    __tablename__ = "subscription_notices"
    __table_args__ = (UniqueConstraint("payment_id", "kind", name="uq_subscription_notice_payment_kind"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    payment_id: Mapped[int] = mapped_column(Integer, ForeignKey("organization_subscription_payments.id", ondelete="CASCADE"), nullable=False)
    kind: Mapped[str] = mapped_column(String(30), nullable=False)
    recipient: Mapped[str | None] = mapped_column(String(254), nullable=True)
    outcome: Mapped[str] = mapped_column(String(20), nullable=False, default="enviado")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
