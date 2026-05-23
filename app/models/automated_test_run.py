from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class AutomatedTestRun(Base):
    __tablename__ = "automated_test_runs"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    trigger_source: Mapped[str] = mapped_column(String(40), nullable=False, default="manual")
    overall_status: Mapped[str] = mapped_column(String(20), nullable=False, default="running")
    is_running: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    deploy_revision: Mapped[str] = mapped_column(String(64), nullable=True, index=True)
    environment: Mapped[str] = mapped_column(String(20), nullable=False, default="development")
    initiated_by_user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True
    )
    duration_seconds: Mapped[float] = mapped_column(Numeric(10, 2), nullable=True)
    summary_json: Mapped[str] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    initiated_by_user = relationship("User", foreign_keys=[initiated_by_user_id])
    suite_results = relationship(
        "AutomatedTestSuiteResult",
        back_populates="run",
        cascade="all, delete-orphan",
    )


class AutomatedTestSuiteResult(Base):
    __tablename__ = "automated_test_suite_results"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    run_id: Mapped[int] = mapped_column(Integer, ForeignKey("automated_test_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    suite_id: Mapped[str] = mapped_column(String(60), nullable=False, index=True)
    suite_name: Mapped[str] = mapped_column(String(160), nullable=False)
    suite_domain: Mapped[str] = mapped_column(String(120), nullable=False, default="")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="unknown")
    passed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    skipped_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duration_seconds: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False, default=0)
    failures_json: Mapped[str] = mapped_column(Text, nullable=True)
    output_tail: Mapped[str] = mapped_column(Text, nullable=True)

    run = relationship("AutomatedTestRun", back_populates="suite_results")
