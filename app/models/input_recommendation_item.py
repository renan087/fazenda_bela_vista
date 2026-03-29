from sqlalchemy import ForeignKey, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class InputRecommendationItem(Base):
    __tablename__ = "input_recommendation_items"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    recommendation_id: Mapped[int] = mapped_column(ForeignKey("input_recommendations.id", ondelete="CASCADE"), nullable=False)
    purchased_input_id: Mapped[int] = mapped_column(ForeignKey("purchased_inputs.id", ondelete="CASCADE"), nullable=False)
    unit: Mapped[str] = mapped_column(String(20), nullable=False)
    quantity: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)

    recommendation = relationship("InputRecommendation", back_populates="items")
    purchased_input = relationship("PurchasedInput", back_populates="recommendation_items")
