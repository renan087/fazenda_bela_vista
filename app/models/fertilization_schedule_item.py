from sqlalchemy import ForeignKey, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class FertilizationScheduleItem(Base):
    __tablename__ = "fertilization_schedule_items"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    schedule_id: Mapped[int] = mapped_column(ForeignKey("fertilization_schedules.id", ondelete="CASCADE"), nullable=False)
    input_id: Mapped[int] = mapped_column(ForeignKey("input_catalog.id", ondelete="CASCADE"), nullable=True)
    purchased_input_id: Mapped[int] = mapped_column(ForeignKey("purchased_inputs.id", ondelete="CASCADE"), nullable=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    unit: Mapped[str] = mapped_column(String(20), nullable=False)
    quantity: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)

    schedule = relationship("FertilizationSchedule", back_populates="items")
    input_catalog = relationship("InputCatalog", back_populates="schedule_items")
    purchased_input = relationship("PurchasedInput", back_populates="schedule_items")
