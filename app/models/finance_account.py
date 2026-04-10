from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class FinanceAccount(Base):
    __tablename__ = "finance_accounts"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id", ondelete="CASCADE"), nullable=False, index=True)
    account_name: Mapped[str] = mapped_column(String(180), nullable=False)
    initial_balance_date: Mapped[date] = mapped_column(Date, nullable=False)
    initial_balance: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    bank_code: Mapped[str] = mapped_column(String(12), nullable=False)
    bank_name: Mapped[str] = mapped_column(String(180), nullable=False)
    custom_bank_id: Mapped[int] = mapped_column(ForeignKey("finance_custom_banks.id", ondelete="SET NULL"), nullable=True, index=True)
    branch_number: Mapped[str] = mapped_column(String(30), nullable=True)
    account_number: Mapped[str] = mapped_column(String(60), nullable=True)
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    farm = relationship("Farm", back_populates="finance_accounts")
    custom_bank = relationship("FinanceCustomBank", back_populates="accounts")
