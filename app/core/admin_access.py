from app.core.config import get_settings
from app.models.user import User


def is_super_admin_email(email: str | None) -> bool:
    normalized_email = (email or "").strip().lower()
    settings = get_settings()
    fallback_email = (settings.admin_email or "").strip().lower()
    configured_super_admin = (getattr(settings, "super_admin_email", None) or "").strip().lower()
    target_email = configured_super_admin or fallback_email
    return bool(normalized_email and target_email and normalized_email == target_email)


def has_admin_access(user: User | None) -> bool:
    if not user:
        return False
    return bool(user.is_admin or is_super_admin_email(user.email))
