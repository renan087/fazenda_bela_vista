from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class BackupRun(Base):
    __tablename__ = "backup_runs"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    initiated_by_user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    trigger_source: Mapped[str] = mapped_column(String(40), nullable=False, default="manual")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running")
    database_bucket: Mapped[str] = mapped_column(String(120), nullable=True)
    database_object_path: Mapped[str] = mapped_column(String(500), nullable=True)
    database_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=True)
    files_bucket: Mapped[str] = mapped_column(String(120), nullable=True)
    files_object_path: Mapped[str] = mapped_column(String(500), nullable=True)
    files_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=True)
    details_json: Mapped[str] = mapped_column(Text, nullable=True)
    error_message: Mapped[str] = mapped_column(Text, nullable=True)
    deleted_from_storage_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    deleted_from_storage_reason: Mapped[str] = mapped_column(String(40), nullable=True)
    deleted_from_storage_source: Mapped[str] = mapped_column(String(40), nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    initiated_by_user = relationship("User", foreign_keys=[initiated_by_user_id])
