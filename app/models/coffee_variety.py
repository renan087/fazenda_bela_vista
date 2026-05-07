from sqlalchemy import ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class CoffeeVariety(Base):
    __tablename__ = "coffee_varieties"
    __table_args__ = (UniqueConstraint("organization_id", "name", name="uq_coffee_varieties_organization_name"),)

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id", ondelete="SET NULL"), nullable=True, index=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    species: Mapped[str] = mapped_column(String(80), nullable=False, default="Arabica")
    maturation_cycle: Mapped[str] = mapped_column(String(80), nullable=False, default="Media")
    flavor_profile: Mapped[str] = mapped_column(String(180), nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)

    organization = relationship("Organization")
    plots = relationship("Plot", back_populates="variety")
    crop_seasons = relationship("CropSeason", back_populates="variety")
