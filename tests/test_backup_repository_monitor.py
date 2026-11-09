from datetime import date, datetime
from zoneinfo import ZoneInfo

from app.services import backup_service


def test_repository_monitor_minute_is_stable_for_same_day():
    check_day = date(2026, 11, 9)

    assert backup_service._backup_repository_monitor_minute(check_day) == backup_service._backup_repository_monitor_minute(check_day)
    assert 0 <= backup_service._backup_repository_monitor_minute(check_day) <= 59


def test_repository_monitor_only_runs_inside_daily_window(monkeypatch):
    backup_service._repository_monitor_attempted_dates.clear()
    monkeypatch.setattr(backup_service, "_backup_repository_monitor_minute", lambda _day: 20)
    monkeypatch.setattr(backup_service.settings, "supabase_backup_monitor_enabled", True)

    before_window = datetime(2026, 11, 9, 3, 19, tzinfo=ZoneInfo("America/Sao_Paulo"))
    after_window = datetime(2026, 11, 9, 4, 0, tzinfo=ZoneInfo("America/Sao_Paulo"))

    assert backup_service.process_due_backup_repository_monitor_tick(before_window) is False
    assert backup_service.process_due_backup_repository_monitor_tick(after_window) is False


def test_next_repository_monitor_is_today_before_its_scheduled_minute(monkeypatch):
    monkeypatch.setattr(backup_service, "_backup_repository_monitor_minute", lambda _day: 20)
    now = datetime(2026, 11, 9, 3, 10, tzinfo=ZoneInfo("America/Sao_Paulo"))

    next_run, window_active = backup_service._next_backup_repository_monitor_at(now)

    assert next_run == datetime(2026, 11, 9, 3, 20, tzinfo=ZoneInfo("America/Sao_Paulo"))
    assert window_active is False


def test_next_repository_monitor_marks_active_window_after_scheduled_minute(monkeypatch):
    monkeypatch.setattr(backup_service, "_backup_repository_monitor_minute", lambda _day: 20)
    now = datetime(2026, 11, 9, 3, 30, tzinfo=ZoneInfo("America/Sao_Paulo"))

    next_run, window_active = backup_service._next_backup_repository_monitor_at(now)

    assert next_run == datetime(2026, 11, 9, 4, 0, tzinfo=ZoneInfo("America/Sao_Paulo"))
    assert window_active is True
