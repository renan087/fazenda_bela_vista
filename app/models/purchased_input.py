from sqlalchemy import ForeignKey, Numeric, String, Text
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
    total_quantity: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    total_cost: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    notes: Mapped[str] = mapped_column(Text, nullable=True)

    farm = relationship("Farm", back_populates="purchased_inputs")
    recommendations = relationship("InputRecommendation", back_populates="purchased_input")
