from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class StockOutput(Base):
    __tablename__ = "stock_outputs"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    input_id: Mapped[int] = mapped_column(ForeignKey("input_catalog.id"), nullable=False)
    purchased_input_id: Mapped[int] = mapped_column(ForeignKey("purchased_inputs.id"), nullable=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id"), nullable=True)
    plot_id: Mapped[int] = mapped_column(ForeignKey("plots.id"), nullable=True)
    movement_date: Mapped[date] = mapped_column(Date, nullable=False)
    quantity: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    unit: Mapped[str] = mapped_column(String(20), nullable=False)
    origin: Mapped[str] = mapped_column(String(40), nullable=False)
    reference_type: Mapped[str] = mapped_column(String(40), nullable=True)
    reference_id: Mapped[int] = mapped_column(Integer, nullable=True)
    unit_cost: Mapped[float] = mapped_column(Numeric(10, 4), nullable=True)
    total_cost: Mapped[float] = mapped_column(Numeric(12, 2), nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    input_catalog = relationship("InputCatalog", back_populates="stock_outputs")
    purchased_input = relationship("PurchasedInput", back_populates="stock_outputs")
    farm = relationship("Farm")
    plot = relationship("Plot")
