from datetime import date

from sqlalchemy import Date, ForeignKey, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class EquipmentAsset(Base):
    __tablename__ = "equipment_assets"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id"), nullable=True)
    name: Mapped[str] = mapped_column(String(180), nullable=False)
    category: Mapped[str] = mapped_column(String(120), nullable=False)
    brand_model: Mapped[str] = mapped_column(String(180), nullable=True)
    asset_code: Mapped[str] = mapped_column(String(120), nullable=True)
    acquisition_date: Mapped[date] = mapped_column(Date, nullable=True)
    acquisition_value: Mapped[float] = mapped_column(Numeric(12, 2), nullable=True)
    status: Mapped[str] = mapped_column(String(60), nullable=False, default="ativo")
    notes: Mapped[str] = mapped_column(Text, nullable=True)

    farm = relationship("Farm", back_populates="equipment_assets")
