from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class BackupAutomationSetting(Base):
    __tablename__ = "backup_automation_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    automatic_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    interval_days: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_auto_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_auto_run_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    last_error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    scheduler_locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
