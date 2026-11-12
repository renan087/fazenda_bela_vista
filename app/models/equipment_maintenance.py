from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class EquipmentMaintenance(Base):
    __tablename__ = "equipment_maintenances"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("equipment_assets.id", ondelete="CASCADE"), nullable=False, index=True)
    plan_id: Mapped[int] = mapped_column(ForeignKey("equipment_maintenance_plans.id", ondelete="SET NULL"), nullable=True, index=True)
    maintenance_date: Mapped[date] = mapped_column(Date, nullable=False)
    maintenance_type: Mapped[str] = mapped_column(String(30), nullable=False, default="preventiva")
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="agendada")
    description: Mapped[str] = mapped_column(String(220), nullable=False)
    responsible_name: Mapped[str] = mapped_column(String(160), nullable=True)
    supplier_name: Mapped[str] = mapped_column(String(180), nullable=True)
    meter_reading_hours: Mapped[float] = mapped_column(Numeric(12, 2), nullable=True)
    service_cost: Mapped[float] = mapped_column(Numeric(12, 2), nullable=True)
    parts_cost: Mapped[float] = mapped_column(Numeric(12, 2), nullable=True)
    next_due_date: Mapped[date] = mapped_column(Date, nullable=True)
    next_due_hours: Mapped[float] = mapped_column(Numeric(12, 2), nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    asset = relationship("EquipmentAsset", back_populates="maintenance_records")
    plan = relationship("EquipmentMaintenancePlan", back_populates="maintenance_records")
    attachments = relationship("EquipmentMaintenanceAttachment", back_populates="maintenance", cascade="all, delete-orphan")
