from datetime import date

from sqlalchemy import Date, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class PestIncident(Base):
    __tablename__ = "pest_incidents"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    plot_id: Mapped[int] = mapped_column(ForeignKey("plots.id"), nullable=False)
    occurrence_date: Mapped[date] = mapped_column(Date, nullable=False)
    category: Mapped[str] = mapped_column(String(80), nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    severity: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    treatment: Mapped[str] = mapped_column(String(180), nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)

    plot = relationship("Plot", back_populates="pest_incidents")
