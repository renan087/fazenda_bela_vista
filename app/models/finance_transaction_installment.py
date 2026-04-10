from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class FinanceTransactionInstallment(Base):
    __tablename__ = "finance_transaction_installments"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    finance_transaction_id: Mapped[int] = mapped_column(ForeignKey("finance_transactions.id", ondelete="CASCADE"), nullable=False, index=True)
    installment_number: Mapped[int] = mapped_column(Integer, nullable=False)
    due_date: Mapped[date] = mapped_column(Date, nullable=False)
    amount: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="pendente")
    paid_at: Mapped[date] = mapped_column(Date, nullable=True)
    payment_notes: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    transaction = relationship("FinanceTransaction", back_populates="installments")
