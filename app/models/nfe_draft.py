from datetime import datetime
from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base


class NfeDraft(Base):
    """Rascunho isolado; não representa NF-e nem possui validade fiscal."""
    __tablename__ = 'nfe_drafts'
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    farm_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    sale_code: Mapped[str] = mapped_column(String(32), nullable=False)
    snapshot_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
