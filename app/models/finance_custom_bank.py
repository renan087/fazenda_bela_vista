from datetime import datetime

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class FinanceCustomBank(Base):
    __tablename__ = "finance_custom_banks"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    bank_code: Mapped[str] = mapped_column(String(12), nullable=False)
    bank_name: Mapped[str] = mapped_column(String(180), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    accounts = relationship("FinanceAccount", back_populates="custom_bank")
