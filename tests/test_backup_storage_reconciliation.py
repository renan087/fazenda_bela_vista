from app.services.backup_service import _backup_run_size_bytes


class _BackupRun:
    database_size_bytes = 1024
    files_size_bytes = 256


def test_backup_run_size_combines_database_and_files():
    assert _backup_run_size_bytes(_BackupRun()) == 1280
