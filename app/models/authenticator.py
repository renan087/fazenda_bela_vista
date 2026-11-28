from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class AuthenticatorChallenge(Base):
    __tablename__ = "authenticator_challenges"

    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    purpose: Mapped[str] = mapped_column(String(16), primary_key=True)
    token_hash: Mapped[str] = mapped_column(String(64))
    credential_hash: Mapped[str] = mapped_column(String(64))
    stage: Mapped[str] = mapped_column(String(16))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    email_code_hash: Mapped[str | None] = mapped_column(String(64))
    email_next_send_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    pending_secret_encrypted: Mapped[str | None] = mapped_column(Text)


class AuthenticatorRecoveryCode(Base):
    __tablename__ = "authenticator_recovery_codes"

    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    code_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
