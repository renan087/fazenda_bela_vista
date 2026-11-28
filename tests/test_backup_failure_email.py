import json
from contextlib import nullcontext
from datetime import timedelta
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import BackupRun
from app.services import backup_service as backup
from app.services import email_service


@pytest.fixture
def backup_sessions(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'backups.sqlite3'}")
    BackupRun.__table__.create(engine)
    try:
        yield sessionmaker(bind=engine, autoflush=False)
    finally:
        engine.dispose()


@pytest.fixture
def smtp(monkeypatch):
    monkeypatch.setattr(backup.settings, "backup_failure_alert_email", "  operacoes@example.com  ")
    monkeypatch.setattr(backup.settings, "super_admin_email", "superadmin@example.com")
    monkeypatch.setattr(backup.settings, "admin_email", "admin@example.com")
    monkeypatch.setattr(backup.settings, "app_public_url", "https://app.example.com/")
    monkeypatch.setattr(backup.settings, "smtp_host", "smtp.example.com")
    monkeypatch.setattr(backup.settings, "smtp_from_name", "SiSFarm")
    monkeypatch.setattr(backup.settings, "smtp_from_email", "sisfarm@example.com")
    transport = Mock()
    monkeypatch.setattr(email_service.smtplib, "SMTP", lambda *args, **kwargs: nullcontext(transport))
    return transport


@pytest.mark.parametrize(
    ("database_saved", "files_saved", "expected_status", "trigger_source", "recipient"),
    [
        (False, False, "failed", "automatic", "operacoes@example.com"),
        (True, False, "partial", "web_manual", "superadmin@example.com"),
        (False, True, "partial", "cli", "admin@example.com"),
        (True, True, "success", "web_manual", None),
    ],
)
def test_backup_sends_branded_alert_after_persisting_outcome(
    backup_sessions, monkeypatch, smtp, database_saved, files_saved, expected_status, trigger_source, recipient
):
    def prepare(component, saved):
        if not saved:
            raise RuntimeError("detalhe técnico interno")
        return {"object_path": f"{component}/backup", "size_bytes": 100, "details": {}}

    if recipient != "operacoes@example.com":
        monkeypatch.setattr(backup.settings, "backup_failure_alert_email", "  ")
    if recipient == "admin@example.com":
        monkeypatch.setattr(backup.settings, "super_admin_email", None)

    monkeypatch.setattr(backup, "_backup_database_dump", lambda *args, **kwargs: prepare("db", database_saved))
    monkeypatch.setattr(backup, "_backup_files_archive", lambda *args, **kwargs: prepare("files", files_saved))
    for name in (
        "_validate_storage_configuration", "_ensure_backup_storage_capacity", "_upload_prepared_backup",
        "_record_supabase_storage_snapshot", "get_supabase_backup_storage_usage",
    ):
        monkeypatch.setattr(backup, name, lambda *args, **kwargs: None)

    observed = []

    def record_email(message, *, from_addr, to_addrs):
        with backup_sessions() as db:
            saved_run = db.query(BackupRun).one()
            observed.append((saved_run.status, json.loads(saved_run.details_json)["progress_ui"]["stage"]))

    smtp.send_message.side_effect = record_email
    with backup_sessions() as db:
        run = backup.execute_backup(db, trigger_source=trigger_source)
        assert run.status == expected_status
        backup.execute_backup_run(db, run.id)
        run_id = run.id

    if expected_status == "success":
        smtp.send_message.assert_not_called()
        return

    assert observed == [(expected_status, expected_status)]
    smtp.send_message.assert_called_once()
    message = smtp.send_message.call_args.args[0]
    assert smtp.send_message.call_args.kwargs["to_addrs"] == [recipient]
    assert f"#{run_id}" in message["Subject"]
    plain = message.get_body(preferencelist=("plain",)).get_content()
    html = message.get_body(preferencelist=("html",)).get_content()
    assert "logo-wordmark.png" in html
    assert "Este é um e-mail automático do SiSFarm" in html
    for body in (plain, html):
        assert "https://app.example.com/backups" in body
        assert "detalhe técnico interno" not in body
        if expected_status == "partial":
            assert ("Arquivos" if database_saved else "Banco de dados") in body


def test_unexpected_background_failure_survives_smtp_error(backup_sessions, monkeypatch, smtp, caplog):
    monkeypatch.setattr(backup, "SessionLocal", backup_sessions)
    monkeypatch.setattr(backup, "_execute_backup_run", Mock(side_effect=RuntimeError("backup interrompido")))
    smtp.send_message.side_effect = email_service.smtplib.SMTPServerDisconnected()
    with backup_sessions() as db:
        run_id = backup._create_backup_run(db, initiated_by=None, trigger_source="web_manual").id

    assert backup.process_backup_run_in_background(run_id) == run_id
    with backup_sessions() as db:
        run = db.get(BackupRun, run_id)
        assert run.status == "failed"
        assert run.finished_at is not None
        assert run.error_message == "Falha inesperada ao executar o backup."
        assert json.loads(run.details_json)["progress_ui"]["stage"] == "failed"
    smtp.send_message.assert_called_once()
    assert "Falha ao enviar o alerta de backup" in caplog.text


def test_expired_backup_sends_one_alert(backup_sessions, smtp):
    with backup_sessions() as db:
        started_at = backup._utc_now() - timedelta(hours=1)
        db.add(BackupRun(trigger_source="automatic", status="running", started_at=started_at))
        db.commit()

        assert backup.expire_stale_running_backup_runs(db) == 1
        assert backup.expire_stale_running_backup_runs(db) == 0
        assert db.query(BackupRun).one().status == "failed"

    smtp.send_message.assert_called_once()
    message = smtp.send_message.call_args.args[0]
    assert "Execução interrompida" in message.get_body(preferencelist=("plain",)).get_content()
