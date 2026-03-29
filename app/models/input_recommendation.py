from sqlalchemy import ForeignKey, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class InputRecommendation(Base):
    __tablename__ = "input_recommendations"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id"), nullable=True)
    plot_id: Mapped[int] = mapped_column(ForeignKey("plots.id"), nullable=True)
    application_name: Mapped[str] = mapped_column(String(160), nullable=False)
    purchased_input_id: Mapped[int] = mapped_column(ForeignKey("purchased_inputs.id"), nullable=True)
    unit: Mapped[str] = mapped_column(String(20), nullable=True)
    quantity_per_hectare: Mapped[float] = mapped_column(Numeric(10, 2), nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)

    farm = relationship("Farm", back_populates="input_recommendations")
    plot = relationship("Plot", back_populates="input_recommendations")
    purchased_input = relationship("PurchasedInput", back_populates="recommendations")
    items = relationship("InputRecommendationItem", back_populates="recommendation", cascade="all, delete-orphan")
