from sqlalchemy import Boolean, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class CoffeeSensoryAttribute(Base):
    __tablename__ = "coffee_sensory_attributes"
    __table_args__ = (UniqueConstraint("organization_id", "key", name="uq_coffee_sensory_attribute_org_key"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    organization_id: Mapped[int] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), index=True)
    key: Mapped[str] = mapped_column(String(50), nullable=False)
    label: Mapped[str] = mapped_column(String(80), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
