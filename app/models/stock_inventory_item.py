from sqlalchemy import ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class StockInventoryItem(Base):
    __tablename__ = "stock_inventory_items"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    session_id: Mapped[int] = mapped_column(ForeignKey("stock_inventory_sessions.id", ondelete="CASCADE"), nullable=False, index=True)
    input_id: Mapped[int] = mapped_column(ForeignKey("input_catalog.id"), nullable=False, index=True)
    system_quantity: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    counted_quantity: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    difference_quantity: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    unit: Mapped[str] = mapped_column(String(20), nullable=False)
    adjustment_entry_id: Mapped[int] = mapped_column(Integer, nullable=True)
    adjustment_output_id: Mapped[int] = mapped_column(Integer, nullable=True)

    session = relationship("StockInventorySession", back_populates="items")
    input_catalog = relationship("InputCatalog")
