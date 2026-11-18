import json

from sqlalchemy import ForeignKey, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class CoffeeSensoryProfile(Base):
    __tablename__ = "coffee_sensory_profiles"
    id: Mapped[int] = mapped_column(primary_key=True)
    classification_id: Mapped[int] = mapped_column(ForeignKey("coffee_classifications.id", ondelete="CASCADE"), unique=True, index=True)
    scale_max: Mapped[int] = mapped_column(Integer, nullable=False)
    attributes_json: Mapped[str] = mapped_column(Text, nullable=False)
    classification = relationship("CoffeeClassification", back_populates="sensory_profile")

    @property
    def attributes(self):
        try:
            data = json.loads(self.attributes_json)
            return data if isinstance(data, dict) else {}
        except (TypeError, ValueError):
            return {}
