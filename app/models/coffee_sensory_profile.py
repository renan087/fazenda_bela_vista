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
            return {key: (value.get("value") if isinstance(value, dict) else value) for key, value in data.items()} if isinstance(data, dict) else {}
        except (TypeError, ValueError):
            return {}

    @property
    def attribute_pairs(self):
        from app.services.coffee_classification import SENSORY_ATTRIBUTES
        try:
            data = json.loads(self.attributes_json)
        except (TypeError, ValueError):
            return []
        if not isinstance(data, dict):
            return []
        defaults = dict(SENSORY_ATTRIBUTES)
        return [(str(value.get("label") or defaults.get(key, key)), value.get("value")) if isinstance(value, dict)
                else (defaults.get(key, key), value) for key, value in data.items()]
