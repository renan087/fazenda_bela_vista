from sqlalchemy import ForeignKey, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class FertilizationItem(Base):
    __tablename__ = "fertilization_items"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    fertilization_record_id: Mapped[int] = mapped_column(ForeignKey("fertilization_records.id"), nullable=False)
    input_id: Mapped[int] = mapped_column(ForeignKey("input_catalog.id"), nullable=True)
    purchased_input_id: Mapped[int] = mapped_column(ForeignKey("purchased_inputs.id"), nullable=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    unit: Mapped[str] = mapped_column(String(40), nullable=False)
    quantity_per_hectare: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    total_quantity: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    unit_cost: Mapped[float] = mapped_column(Numeric(10, 4), nullable=True)
    total_cost: Mapped[float] = mapped_column(Numeric(12, 2), nullable=True)

    fertilization = relationship("FertilizationRecord", back_populates="items")
    input_catalog = relationship("InputCatalog", back_populates="fertilization_items")
    purchased_input = relationship("PurchasedInput")
    stock_allocations = relationship("FertilizationStockAllocation", back_populates="fertilization_item", cascade="all, delete-orphan")
