from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class EquipmentMaintenancePlan(Base):
    __tablename__ = "equipment_maintenance_plans"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("equipment_assets.id", ondelete="CASCADE"), nullable=False, index=True)
    maintenance_type: Mapped[str] = mapped_column(String(30), nullable=False, default="preventiva")
    description: Mapped[str] = mapped_column(String(220), nullable=False)
    responsible_name: Mapped[str] = mapped_column(String(160), nullable=True)
    supplier_name: Mapped[str] = mapped_column(String(180), nullable=True)
    interval_days: Mapped[int] = mapped_column(Integer, nullable=True)
    interval_hours: Mapped[float] = mapped_column(Numeric(12, 2), nullable=True)
    next_due_date: Mapped[date] = mapped_column(Date, nullable=True)
    next_due_hours: Mapped[float] = mapped_column(Numeric(12, 2), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    asset = relationship("EquipmentAsset", back_populates="maintenance_plans")
    maintenance_records = relationship("EquipmentMaintenance", back_populates="plan")
