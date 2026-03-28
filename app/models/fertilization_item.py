from sqlalchemy import ForeignKey, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class FertilizationItem(Base):
    __tablename__ = "fertilization_items"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    fertilization_record_id: Mapped[int] = mapped_column(ForeignKey("fertilization_records.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    unit: Mapped[str] = mapped_column(String(40), nullable=False)
    quantity_per_hectare: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    total_quantity: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)

    fertilization = relationship("FertilizationRecord", back_populates="items")
