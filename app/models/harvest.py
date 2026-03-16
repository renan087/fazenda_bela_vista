from datetime import date

from sqlalchemy import Date, ForeignKey, Numeric, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class HarvestRecord(Base):
    __tablename__ = "harvest_records"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    plot_id: Mapped[int] = mapped_column(ForeignKey("plots.id"), nullable=False)
    harvest_date: Mapped[date] = mapped_column(Date, nullable=False)
    sacks_produced: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    notes: Mapped[str] = mapped_column(Text, nullable=True)

    plot = relationship("Plot", back_populates="harvests")
