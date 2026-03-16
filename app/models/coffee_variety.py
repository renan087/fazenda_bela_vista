from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class CoffeeVariety(Base):
    __tablename__ = "coffee_varieties"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    species: Mapped[str] = mapped_column(String(80), nullable=False, default="Arabica")
    maturation_cycle: Mapped[str] = mapped_column(String(80), nullable=False, default="Media")
    flavor_profile: Mapped[str] = mapped_column(String(180), nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)

    plots = relationship("Plot", back_populates="variety")
