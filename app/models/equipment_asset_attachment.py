from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, LargeBinary, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class EquipmentAssetAttachment(Base):
    __tablename__ = "equipment_asset_attachments"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    equipment_asset_id: Mapped[int] = mapped_column(ForeignKey("equipment_assets.id", ondelete="CASCADE"), nullable=False, index=True)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    content_type: Mapped[str] = mapped_column(String(120), nullable=False)
    file_data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    equipment_asset = relationship("EquipmentAsset", back_populates="attachments")
