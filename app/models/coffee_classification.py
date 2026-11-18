from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, LargeBinary, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class CoffeeClassification(Base):
    __tablename__ = "coffee_classifications"

    id: Mapped[int] = mapped_column(primary_key=True)
    harvest_id: Mapped[int] = mapped_column(ForeignKey("harvest_records.id", ondelete="CASCADE"), index=True)
    analysis_date: Mapped[date] = mapped_column(Date, nullable=False)
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    evaluator: Mapped[str | None] = mapped_column(String(180))
    method: Mapped[str | None] = mapped_column(String(180))
    coffee_type: Mapped[str | None] = mapped_column(String(80))
    sieve: Mapped[str | None] = mapped_column(String(80))
    moisture: Mapped[float | None] = mapped_column(Numeric(5, 2))
    defects: Mapped[str | None] = mapped_column(String(180))
    beverage: Mapped[str | None] = mapped_column(String(120))
    sensory_score: Mapped[float | None] = mapped_column(Numeric(5, 2))
    sensory_notes: Mapped[str | None] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text)
    report_filename: Mapped[str | None] = mapped_column(String(255))
    report_data: Mapped[bytes | None] = mapped_column(LargeBinary)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    harvest = relationship("HarvestRecord")
    sensory_profile = relationship("CoffeeSensoryProfile", back_populates="classification", uselist=False, cascade="all, delete-orphan")
