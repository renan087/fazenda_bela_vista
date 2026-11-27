from datetime import date

from sqlalchemy import Date, ForeignKey, Integer, LargeBinary, Numeric, String, Text
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
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="ativa")
    incidence_pct: Mapped[float | None] = mapped_column(Numeric(5, 2), nullable=True)
    reinspection_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    resolved_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    treatment: Mapped[str] = mapped_column(String(180), nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    photo_filename: Mapped[str | None] = mapped_column(String(255), nullable=True)
    photo_content_type: Mapped[str | None] = mapped_column(String(120), nullable=True)
    photo_data: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)

    plot = relationship("Plot", back_populates="pest_incidents")
