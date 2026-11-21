from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base


class AssistantCapability(Base):
    __tablename__ = 'assistant_capabilities'
    name: Mapped[str] = mapped_column(String(40), primary_key=True)
    user_id: Mapped[int | None] = mapped_column(Integer, ForeignKey('users.id', ondelete='SET NULL'), nullable=True)
