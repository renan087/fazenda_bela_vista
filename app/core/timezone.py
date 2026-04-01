from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from app.core.config import get_settings


def get_app_timezone() -> ZoneInfo:
    return ZoneInfo(get_settings().app_timezone)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def app_now() -> datetime:
    return utc_now().astimezone(get_app_timezone())


def today_in_app_timezone() -> date:
    return app_now().date()


def as_app_timezone(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    # Defensive fallback: older records or adapters may hand us naive datetimes.
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(get_app_timezone())


def format_app_datetime(value: datetime | None, fmt: str = "%d/%m/%Y %H:%M") -> str:
    localized = as_app_timezone(value)
    if localized is None:
        return ""
    return localized.strftime(fmt)
