from sqlalchemy import ForeignKey, Numeric
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class FertilizationStockAllocation(Base):
    __tablename__ = "fertilization_stock_allocations"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    fertilization_item_id: Mapped[int] = mapped_column(ForeignKey("fertilization_items.id", ondelete="CASCADE"), nullable=False)
    purchased_input_id: Mapped[int] = mapped_column(ForeignKey("purchased_inputs.id", ondelete="CASCADE"), nullable=False)
    quantity_used: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    unit_cost: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    total_cost: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)

    fertilization_item = relationship("FertilizationItem", back_populates="stock_allocations")
    purchased_input = relationship("PurchasedInput", back_populates="stock_allocations")
