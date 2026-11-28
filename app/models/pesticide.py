from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, LargeBinary, Numeric, String, Text, func
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
    sampled_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    affected_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    control_threshold_pct: Mapped[float | None] = mapped_column(Numeric(5, 2), nullable=True)
    location_in_plot: Mapped[str | None] = mapped_column(String(40), nullable=True)
    created_by: Mapped[str | None] = mapped_column(String(160), nullable=True)

    plot = relationship("Plot", back_populates="pest_incidents")
    inspections = relationship(
        "PestInspection",
        back_populates="incident",
        cascade="all, delete-orphan",
        order_by="PestInspection.inspection_date, PestInspection.id",
    )


class PestInspection(Base):
    """Cada avaliação de um foco: registro inicial, reinspeção, ronda ou correção."""

    __tablename__ = "pest_inspections"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    incident_id: Mapped[int] = mapped_column(ForeignKey("pest_incidents.id", ondelete="CASCADE"), nullable=False, index=True)
    inspection_date: Mapped[date] = mapped_column(Date, nullable=False)
    kind: Mapped[str] = mapped_column(String(20), nullable=False, default="registro")
    incidence_pct: Mapped[float | None] = mapped_column(Numeric(5, 2), nullable=True)
    sampled_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    affected_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    level: Mapped[int | None] = mapped_column(Integer, nullable=True)
    decision: Mapped[str | None] = mapped_column(String(20), nullable=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str | None] = mapped_column(String(160), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    incident = relationship("PestIncident", back_populates="inspections")


class PestRound(Base):
    """Ronda de monitoramento: planejada no escritório, feita no celular."""

    __tablename__ = "pest_rounds"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    farm_id: Mapped[int | None] = mapped_column(ForeignKey("farms.id", ondelete="CASCADE"), nullable=True, index=True)
    round_date: Mapped[date] = mapped_column(Date, nullable=False)
    responsible_name: Mapped[str | None] = mapped_column(String(160), nullable=True)
    plot_ids: Mapped[str] = mapped_column(Text, nullable=False, default="")
    pest_keys: Mapped[str] = mapped_column(Text, nullable=False, default="")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="planejada")
    new_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reinspect_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    worse_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
