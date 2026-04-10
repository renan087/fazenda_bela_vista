from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class FinanceTransaction(Base):
    __tablename__ = "finance_transactions"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id", ondelete="CASCADE"), nullable=False, index=True)
    finance_account_id: Mapped[int] = mapped_column(ForeignKey("finance_accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    operation_type: Mapped[str] = mapped_column(String(20), nullable=False)
    launch_date: Mapped[date] = mapped_column(Date, nullable=False)
    amount: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False)
    category: Mapped[str] = mapped_column(String(160), nullable=False)
    product_service: Mapped[str] = mapped_column(String(180), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    counterparty_name: Mapped[str] = mapped_column(String(180), nullable=True)
    document_number: Mapped[str] = mapped_column(String(120), nullable=True)
    payment_method: Mapped[str] = mapped_column(String(80), nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    farm = relationship("Farm", back_populates="finance_transactions")
    finance_account = relationship("FinanceAccount", back_populates="transactions")
    attachments = relationship("FinanceTransactionAttachment", back_populates="transaction", cascade="all, delete-orphan")
