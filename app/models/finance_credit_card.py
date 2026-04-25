from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class FinanceCreditCard(Base):
    __tablename__ = "finance_credit_cards"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id", ondelete="CASCADE"), nullable=False, index=True)
    payment_account_id: Mapped[int] = mapped_column(ForeignKey("finance_accounts.id", ondelete="SET NULL"), nullable=True, index=True)
    card_name: Mapped[str] = mapped_column(String(180), nullable=False)
    issuer: Mapped[str] = mapped_column(String(120), nullable=False)
    brand: Mapped[str] = mapped_column(String(40), nullable=True)
    closing_day: Mapped[int] = mapped_column(nullable=False)
    due_day: Mapped[int] = mapped_column(nullable=False)
    credit_limit: Mapped[float] = mapped_column(Numeric(14, 2), nullable=True)
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    notes: Mapped[str] = mapped_column(String(240), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    farm = relationship("Farm", back_populates="finance_credit_cards")
    payment_account = relationship("FinanceAccount", back_populates="credit_cards")
