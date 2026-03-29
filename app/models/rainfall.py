from datetime import date

from sqlalchemy import Date, ForeignKey, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class RainfallRecord(Base):
    __tablename__ = "rainfall_records"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id"), nullable=False)
    rainfall_date: Mapped[date] = mapped_column(Date, nullable=False)
    millimeters: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    source: Mapped[str] = mapped_column(String(120), nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)

    farm = relationship("Farm", back_populates="rainfalls")
