from datetime import date

from sqlalchemy import Date, ForeignKey, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class PurchasedInput(Base):
    __tablename__ = "purchased_inputs"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id"), nullable=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    quantity_purchased: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    package_size: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    package_unit: Mapped[str] = mapped_column(String(20), nullable=False)
    unit_price: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    purchase_date: Mapped[date] = mapped_column(Date, nullable=True)
    total_quantity: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    available_quantity: Mapped[float] = mapped_column(Numeric(12, 2), nullable=True)
    total_cost: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    low_stock_threshold: Mapped[float] = mapped_column(Numeric(10, 2), nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)

    farm = relationship("Farm", back_populates="purchased_inputs")
    recommendations = relationship("InputRecommendation", back_populates="purchased_input")
    recommendation_items = relationship("InputRecommendationItem", back_populates="purchased_input")
    schedule_items = relationship("FertilizationScheduleItem", back_populates="purchased_input")
    stock_allocations = relationship("FertilizationStockAllocation", back_populates="purchased_input")
