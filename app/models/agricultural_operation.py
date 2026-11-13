from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class AgriculturalOperation(Base):
    __tablename__ = "agricultural_operations"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    plot_id: Mapped[int] = mapped_column(ForeignKey("plots.id", ondelete="CASCADE"), nullable=False, index=True)
    season_id: Mapped[int] = mapped_column(ForeignKey("crop_seasons.id", ondelete="SET NULL"), nullable=True, index=True)
    finance_transaction_id: Mapped[int] = mapped_column(ForeignKey("finance_transactions.id", ondelete="SET NULL"), nullable=True, index=True)
    operation_date: Mapped[date] = mapped_column(Date, nullable=False)
    operation_type: Mapped[str] = mapped_column(String(80), nullable=False)
    responsible_name: Mapped[str] = mapped_column(String(160), nullable=False)
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    plot = relationship("Plot")
    season = relationship("CropSeason")
    finance_transaction = relationship("FinanceTransaction")
