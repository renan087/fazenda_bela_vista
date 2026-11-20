from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, LargeBinary, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class FarmPreviewCache(Base):
    """Cache persistente em banco das imagens de prévia do mapa da fazenda (sobrevive a deploys no Render)."""

    __tablename__ = "farm_preview_cache"

    farm_id: Mapped[int] = mapped_column(ForeignKey("farms.id", ondelete="CASCADE"), primary_key=True)
    geojson_hash: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    preview_png: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True, deferred=True)
    thumb_jpg: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True, deferred=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
