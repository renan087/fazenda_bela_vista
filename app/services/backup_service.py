import asyncio
import json
import gzip
import logging
import os
import shutil
import subprocess
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import httpx
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.timezone import app_now, get_app_timezone
from app.db.session import SessionLocal
from app.models import BackupAutomationSetting, BackupRun, User
from app.repositories.farm import FarmRepository

logger = logging.getLogger(__name__)
settings = get_settings()
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOCAL_BACKUP_DIR_CANDIDATES = [
    "app/static/uploads",
    "app/uploads",
    "uploads",
    "storage/uploads",
]
AUTOMATION_DEFAULT_INTERVAL_DAYS = 5
AUTOMATION_POLL_INTERVAL_SECONDS = 60
AUTOMATION_LOCK_TTL = timedelta(hours=2)
AUTOMATION_TRIGGER_SOURCE = "automatic"
AUTOMATION_DEFAULT_SCHEDULE_HOUR = 3
AUTOMATION_DEFAULT_SCHEDULE_MINUTE = 0
AUTOMATION_DEFAULT_STORAGE_LIMIT_GB = 1
PG_DUMP_TIMEOUT_SECONDS = 300
BACKUP_UPLOAD_TIMEOUT_SECONDS = 600.0
STALE_RUNNING_BACKUP_TTL = timedelta(minutes=10)
STORAGE_LIMIT_ERROR_HINTS = (
    "limit",
    "quota",
    "exceed",
    "insufficient",
    "storage",
    "space",
    "50 gb",
)

BACKUP_PROGRESS_STAGES = {
    "starting": {"progress": 6, "title": "Preparando backup", "message": "Organizando a execução inicial do backup."},
    "database_preparing": {"progress": 20, "title": "Salvando banco", "message": "Montando a cópia do banco de dados."},
    "database_uploading": {"progress": 46, "title": "Enviando banco", "message": "Enviando a cópia principal para o storage."},
    "files_preparing": {"progress": 64, "title": "Separando arquivos", "message": "Agrupando os arquivos complementares do sistema."},
    "files_uploading": {"progress": 84, "title": "Enviando arquivos", "message": "Transferindo os arquivos complementares para o storage."},
    "finalizing": {"progress": 96, "title": "Finalizando", "message": "Registrando o resultado final do backup."},
    "success": {"progress": 100, "title": "Backup concluído", "message": "Seu backup foi concluído com sucesso."},
    "partial": {"progress": 100, "title": "Backup parcial", "message": "O backup terminou parcialmente. Revise o histórico para os detalhes."},
    "failed": {"progress": 100, "title": "Backup não concluído", "message": "Não foi possível concluir o backup. Revise o histórico para os detalhes."},
}


def execute_backup(
    db: Session,
    initiated_by: User | None = None,
    trigger_source: str = "manual",
) -> BackupRun:
    run = _create_backup_run(db, initiated_by=initiated_by, trigger_source=trigger_source)
    return execute_backup_run(db, run.id)


def _create_backup_run(
    db: Session,
    *,
    initiated_by: User | None,
    trigger_source: str,
) -> BackupRun:
    run = BackupRun(
        initiated_by_user_id=initiated_by.id if initiated_by else None,
        trigger_source=trigger_source,
        status="running",
        database_bucket=settings.supabase_bucket_db,
        files_bucket=settings.supabase_bucket_files,
        deleted_from_storage_at=None,
        deleted_from_storage_reason=None,
        deleted_from_storage_source=None,
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    _update_backup_progress(db, run.id, stage="starting")
    return run


def execute_backup_run(db: Session, run_id: int) -> BackupRun:
    run = db.query(BackupRun).filter(BackupRun.id == run_id).first()
    if not run:
        raise RuntimeError("Execução de backup não encontrada.")

    started_at = _utc_now()
    db_result: dict | None = None
    files_result: dict | None = None
    errors: list[str] = []

    try:
        _validate_storage_configuration()
        _update_backup_progress(db, run.id, stage="database_preparing")
        db_result = _backup_database_dump(
            started_at,
            on_upload_start=lambda: _update_backup_progress(db, run.id, stage="database_uploading"),
        )
    except Exception as exc:
        logger.exception("Falha ao gerar ou enviar backup do banco.")
        errors.append(f"Banco: {exc}")

    try:
        _validate_storage_configuration()
        _update_backup_progress(db, run.id, stage="files_preparing")
        files_result = _backup_files_archive(
            started_at,
            on_upload_start=lambda: _update_backup_progress(db, run.id, stage="files_uploading"),
        )
    except Exception as exc:
        logger.exception("Falha ao gerar ou enviar backup de arquivos.")
        errors.append(f"Arquivos: {exc}")

    success_count = int(db_result is not None) + int(files_result is not None)
    run.status = "success" if success_count == 2 else "partial" if success_count == 1 else "failed"
    run.database_bucket = settings.supabase_bucket_db
    run.files_bucket = settings.supabase_bucket_files
    run.database_object_path = db_result["object_path"] if db_result else None
    run.database_size_bytes = db_result["size_bytes"] if db_result else None
    run.files_object_path = files_result["object_path"] if files_result else None
    run.files_size_bytes = files_result["size_bytes"] if files_result else None
    run.details_json = json.dumps(
        {
            "database": db_result["details"] if db_result else None,
            "files": files_result["details"] if files_result else None,
        },
        ensure_ascii=False,
    )
    run.error_message = "\n".join(errors) if errors else None
    run.finished_at = _utc_now()
    progress_stage = "success" if run.status == "success" else "partial" if run.status == "partial" else "failed"
    _update_backup_progress(
        db,
        run.id,
        stage="finalizing",
        preserve_existing=True,
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    _update_backup_progress(
        db,
        run.id,
        stage=progress_stage,
        preserve_existing=True,
    )
    db.refresh(run)
    return run


def process_backup_run_in_background(run_id: int) -> int:
    try:
        with SessionLocal() as db:
            execute_backup_run(db, run_id)
    except Exception:
        logger.exception("Falha inesperada ao executar backup em segundo plano. run_id=%s", run_id)
        with SessionLocal() as db:
            _update_backup_progress(db, run_id, stage="failed", preserve_existing=True)
            run = db.query(BackupRun).filter(BackupRun.id == run_id).first()
            if run and not run.error_message:
                run.error_message = "Falha inesperada ao executar o backup em segundo plano."
                run.finished_at = _utc_now()
                run.status = "failed"
                db.add(run)
                db.commit()
    return run_id


def expire_stale_running_backup_runs(db: Session, ttl: timedelta = STALE_RUNNING_BACKUP_TTL) -> int:
    cutoff = _utc_now() - ttl
    stale_runs = (
        db.query(BackupRun)
        .filter(BackupRun.status == "running")
        .filter(BackupRun.started_at < cutoff)
        .all()
    )
    if not stale_runs:
        return 0

    for run in stale_runs:
        try:
            parsed = json.loads(run.details_json) if run.details_json else {}
        except json.JSONDecodeError:
            parsed = {}
        if not isinstance(parsed, dict):
            parsed = {}
        parsed["progress_ui"] = _backup_progress_payload("failed")
        run.details_json = json.dumps(parsed, ensure_ascii=False)
        run.status = "failed"
        run.finished_at = _utc_now()
        run.error_message = run.error_message or (
            "A execução do backup foi interrompida ou excedeu o tempo esperado. "
            "Inicie um novo backup."
        )
        db.add(run)
    db.commit()
    return len(stale_runs)


def get_or_create_backup_automation_setting(db: Session) -> BackupAutomationSetting:
    setting = db.query(BackupAutomationSetting).filter(BackupAutomationSetting.id == 1).first()
    if setting:
        changed = False
        interval_days = _normalize_interval_days(setting.interval_days)
        if setting.interval_days != interval_days:
            setting.interval_days = interval_days
            changed = True
        storage_limit_gb = _normalize_storage_limit_gb(getattr(setting, "storage_limit_gb", AUTOMATION_DEFAULT_STORAGE_LIMIT_GB))
        if getattr(setting, "storage_limit_gb", None) != storage_limit_gb:
            setting.storage_limit_gb = storage_limit_gb
            changed = True
        scheduled_hour, scheduled_minute = _normalize_schedule_time(
            getattr(setting, "scheduled_hour", AUTOMATION_DEFAULT_SCHEDULE_HOUR),
            getattr(setting, "scheduled_minute", AUTOMATION_DEFAULT_SCHEDULE_MINUTE),
        )
        if getattr(setting, "scheduled_hour", None) != scheduled_hour:
            setting.scheduled_hour = scheduled_hour
            changed = True
        if getattr(setting, "scheduled_minute", None) != scheduled_minute:
            setting.scheduled_minute = scheduled_minute
            changed = True
        if setting.automatic_enabled and setting.next_run_at is None:
            setting.next_run_at = _compute_next_run_at(
                interval_days=setting.interval_days,
                scheduled_hour=setting.scheduled_hour,
                scheduled_minute=setting.scheduled_minute,
            )
            changed = True
        if changed:
            db.add(setting)
            db.commit()
            db.refresh(setting)
        return setting
    setting = BackupAutomationSetting(
        id=1,
        automatic_enabled=True,
        interval_days=AUTOMATION_DEFAULT_INTERVAL_DAYS,
        storage_limit_gb=AUTOMATION_DEFAULT_STORAGE_LIMIT_GB,
        scheduled_hour=AUTOMATION_DEFAULT_SCHEDULE_HOUR,
        scheduled_minute=AUTOMATION_DEFAULT_SCHEDULE_MINUTE,
        next_run_at=_compute_next_run_at(
            interval_days=AUTOMATION_DEFAULT_INTERVAL_DAYS,
            scheduled_hour=AUTOMATION_DEFAULT_SCHEDULE_HOUR,
            scheduled_minute=AUTOMATION_DEFAULT_SCHEDULE_MINUTE,
        ),
    )
    db.add(setting)
    db.commit()
    db.refresh(setting)
    return setting


def update_backup_automation_setting(
    db: Session,
    automatic_enabled: bool,
    interval_days: int,
    scheduled_hour: int,
    scheduled_minute: int,
) -> BackupAutomationSetting:
    setting = get_or_create_backup_automation_setting(db)
    interval_days = _normalize_interval_days(interval_days)
    scheduled_hour, scheduled_minute = _normalize_schedule_time(scheduled_hour, scheduled_minute)
    setting.automatic_enabled = automatic_enabled
    setting.interval_days = interval_days
    setting.scheduled_hour = scheduled_hour
    setting.scheduled_minute = scheduled_minute
    setting.scheduler_locked_at = None
    setting.next_run_at = (
        _compute_next_run_at(
            interval_days=interval_days,
            scheduled_hour=scheduled_hour,
            scheduled_minute=scheduled_minute,
        )
        if automatic_enabled
        else None
    )
    db.add(setting)
    db.commit()
    db.refresh(setting)
    return setting


def update_backup_storage_limit_setting(db: Session, storage_limit_gb: int) -> BackupAutomationSetting:
    setting = get_or_create_backup_automation_setting(db)
    setting.storage_limit_gb = _normalize_storage_limit_gb(storage_limit_gb)
    db.add(setting)
    db.commit()
    db.refresh(setting)
    return setting


async def run_backup_automation_loop() -> None:
    while True:
        try:
            await asyncio.to_thread(process_due_automatic_backup_tick)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Falha inesperada no loop de backup automatico.")
        await asyncio.sleep(AUTOMATION_POLL_INTERVAL_SECONDS)


def process_due_automatic_backup_tick() -> BackupRun | None:
    from app.db.session import SessionLocal

    with SessionLocal() as db:
        return execute_due_automatic_backup(db)


def execute_due_automatic_backup(db: Session) -> BackupRun | None:
    setting = _claim_due_automatic_backup(db)
    if not setting:
        return None

    run: BackupRun | None = None
    error_message: str | None = None
    try:
        run = execute_automatic_backup_cycle(db)
        error_message = run.error_message if run and run.error_message else None
        return run
    except Exception as exc:
        logger.exception("Falha ao executar backup automatico.")
        error_message = str(exc)
        return None
    finally:
        _finalize_due_automatic_backup(db, run=run, error_message=error_message)


def execute_automatic_backup_cycle(db: Session) -> BackupRun:
    repo = FarmRepository(db)
    if _is_storage_limit_reached(db):
        oldest = repo.get_oldest_active_backup_run()
        if oldest:
            mark_backup_run_storage_deleted(
                db,
                oldest,
                reason="storage_limit",
                source=AUTOMATION_TRIGGER_SOURCE,
            )

    run = execute_backup(db, initiated_by=None, trigger_source=AUTOMATION_TRIGGER_SOURCE)
    if _run_failed_due_to_storage_limit(run):
        oldest = repo.get_oldest_active_backup_run()
        if oldest:
            mark_backup_run_storage_deleted(
                db,
                oldest,
                reason="storage_limit",
                source=AUTOMATION_TRIGGER_SOURCE,
            )
            run = execute_backup(db, initiated_by=None, trigger_source=AUTOMATION_TRIGGER_SOURCE)
    return run


def mark_backup_run_storage_deleted(
    db: Session,
    run: BackupRun,
    *,
    reason: str,
    source: str,
) -> list[str]:
    warnings: list[str] = []
    storage_errors: list[str] = []

    if run.deleted_from_storage_at:
        return warnings

    objects_to_delete = [
        ("banco", run.database_bucket, run.database_object_path),
        ("arquivos", run.files_bucket, run.files_object_path),
    ]

    for label, bucket, object_path in objects_to_delete:
        if not bucket or not object_path:
            continue
        try:
            deleted, missing = _delete_object_from_supabase(bucket=bucket, object_path=object_path)
            if missing or not deleted:
                warning_message = f"O arquivo de {label} ja nao existia mais no storage."
                warnings.append(warning_message)
                logger.warning("%s BackupRun id=%s path=%s", warning_message, run.id, object_path)
        except Exception as exc:
            logger.exception(
                "Falha ao excluir arquivo de backup no Supabase. run_id=%s bucket=%s object=%s",
                run.id,
                bucket,
                object_path,
            )
            storage_errors.append(f"{label.title()}: {exc}")

    if storage_errors:
        raise RuntimeError(" ".join(storage_errors))

    deleted_at = _utc_now()
    run.deleted_from_storage_at = deleted_at
    run.deleted_from_storage_reason = reason
    run.deleted_from_storage_source = source
    run.details_json = _with_storage_deletion_details(
        run.details_json,
        deleted_at=deleted_at,
        reason=reason,
        source=source,
        warnings=warnings,
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return warnings


def delete_backup_run(db: Session, run: BackupRun) -> list[str]:
    if run.deleted_from_storage_at:
        db.delete(run)
        db.commit()
        return []

    warnings = mark_backup_run_storage_deleted(
        db,
        run,
        reason="manual_delete",
        source="manual",
    )
    db.delete(run)
    db.commit()
    return warnings


def _claim_due_automatic_backup(db: Session) -> BackupAutomationSetting | None:
    get_or_create_backup_automation_setting(db)
    now = _utc_now()
    setting = (
        db.query(BackupAutomationSetting)
        .filter(BackupAutomationSetting.id == 1)
        .with_for_update()
        .first()
    )
    if not setting:
        return None

    setting.interval_days = _normalize_interval_days(setting.interval_days)
    setting.scheduled_hour, setting.scheduled_minute = _normalize_schedule_time(
        getattr(setting, "scheduled_hour", AUTOMATION_DEFAULT_SCHEDULE_HOUR),
        getattr(setting, "scheduled_minute", AUTOMATION_DEFAULT_SCHEDULE_MINUTE),
    )
    if not setting.automatic_enabled:
        setting.scheduler_locked_at = None
        db.add(setting)
        db.commit()
        return None

    if setting.next_run_at is None:
        setting.next_run_at = _compute_next_run_at(
            interval_days=setting.interval_days,
            scheduled_hour=setting.scheduled_hour,
            scheduled_minute=setting.scheduled_minute,
        )
        db.add(setting)
        db.commit()
        return None

    if setting.scheduler_locked_at and setting.scheduler_locked_at > now - AUTOMATION_LOCK_TTL:
        return None

    expire_stale_running_backup_runs(db)
    has_running = db.query(BackupRun.id).filter(BackupRun.status == "running").first() is not None
    if has_running or setting.next_run_at > now:
        return None

    setting.scheduler_locked_at = now
    db.add(setting)
    db.commit()
    db.refresh(setting)
    return setting


def _finalize_due_automatic_backup(
    db: Session,
    *,
    run: BackupRun | None,
    error_message: str | None,
) -> None:
    setting = get_or_create_backup_automation_setting(db)
    now = _utc_now()
    setting.scheduler_locked_at = None
    setting.last_auto_run_at = now
    setting.last_auto_run_status = run.status if run else "failed"
    setting.last_error_message = error_message
    if setting.automatic_enabled:
        setting.next_run_at = _compute_next_run_at(
            interval_days=_normalize_interval_days(setting.interval_days),
            scheduled_hour=getattr(setting, "scheduled_hour", AUTOMATION_DEFAULT_SCHEDULE_HOUR),
            scheduled_minute=getattr(setting, "scheduled_minute", AUTOMATION_DEFAULT_SCHEDULE_MINUTE),
        )
    else:
        setting.next_run_at = None
    db.add(setting)
    db.commit()


def _is_storage_limit_reached(db: Session) -> bool:
    repo = FarmRepository(db)
    setting = get_or_create_backup_automation_setting(db)
    usage = repo.summarize_backup_storage_usage()
    total_bytes = int(usage.get("database_bytes", 0) or 0) + int(usage.get("files_bytes", 0) or 0)
    limit_bytes = _normalize_storage_limit_gb(getattr(setting, "storage_limit_gb", AUTOMATION_DEFAULT_STORAGE_LIMIT_GB)) * 1024 * 1024 * 1024
    return total_bytes >= limit_bytes


def _run_failed_due_to_storage_limit(run: BackupRun | None) -> bool:
    if not run or not run.error_message:
        return False
    lowered = run.error_message.lower()
    return any(hint in lowered for hint in STORAGE_LIMIT_ERROR_HINTS)


def _with_storage_deletion_details(
    raw_details: str | None,
    *,
    deleted_at: datetime,
    reason: str,
    source: str,
    warnings: list[str],
) -> str:
    try:
        parsed = json.loads(raw_details) if raw_details else {}
    except json.JSONDecodeError:
        parsed = {}
    if not isinstance(parsed, dict):
        parsed = {}
    parsed["storage_deletion"] = {
        "deleted_at": deleted_at.isoformat(),
        "reason": reason,
        "source": source,
        "warnings": warnings,
    }
    return json.dumps(parsed, ensure_ascii=False)


def _backup_progress_payload(stage: str) -> dict[str, object]:
    base = BACKUP_PROGRESS_STAGES.get(stage) or BACKUP_PROGRESS_STAGES["starting"]
    return {
        "stage": stage,
        "progress": int(base["progress"]),
        "title": str(base["title"]),
        "message": str(base["message"]),
        "updated_at": _utc_now().isoformat(),
    }


def _update_backup_progress(
    db: Session,
    run_id: int,
    *,
    stage: str,
    preserve_existing: bool = False,
) -> None:
    run = db.query(BackupRun).filter(BackupRun.id == run_id).first()
    if not run:
        return
    try:
        parsed = json.loads(run.details_json) if run.details_json else {}
    except json.JSONDecodeError:
        parsed = {}
    if not isinstance(parsed, dict):
        parsed = {}
    progress_payload = _backup_progress_payload(stage)
    if preserve_existing and isinstance(parsed.get("progress_ui"), dict):
        current = parsed["progress_ui"]
        if int(current.get("progress", 0) or 0) > int(progress_payload["progress"]):
            progress_payload["progress"] = int(current.get("progress", 0) or 0)
    parsed["progress_ui"] = progress_payload
    run.details_json = json.dumps(parsed, ensure_ascii=False)
    if stage in {"success", "partial", "failed"}:
        run.status = "success" if stage == "success" else "partial" if stage == "partial" else "failed"
        if run.finished_at is None:
            run.finished_at = _utc_now()
    db.add(run)
    db.commit()


def _normalize_interval_days(value: int | None) -> int:
    try:
        parsed = int(value or AUTOMATION_DEFAULT_INTERVAL_DAYS)
    except (TypeError, ValueError):
        parsed = AUTOMATION_DEFAULT_INTERVAL_DAYS
    return max(1, min(parsed, 365))


def _normalize_storage_limit_gb(value: int | None) -> int:
    try:
        parsed = int(value or AUTOMATION_DEFAULT_STORAGE_LIMIT_GB)
    except (TypeError, ValueError):
        parsed = AUTOMATION_DEFAULT_STORAGE_LIMIT_GB
    return max(1, min(parsed, 1024))


def _normalize_schedule_time(hour: int | None, minute: int | None) -> tuple[int, int]:
    try:
        parsed_hour = int(AUTOMATION_DEFAULT_SCHEDULE_HOUR if hour is None else hour)
    except (TypeError, ValueError):
        parsed_hour = AUTOMATION_DEFAULT_SCHEDULE_HOUR
    try:
        parsed_minute = int(AUTOMATION_DEFAULT_SCHEDULE_MINUTE if minute is None else minute)
    except (TypeError, ValueError):
        parsed_minute = AUTOMATION_DEFAULT_SCHEDULE_MINUTE
    return max(0, min(parsed_hour, 23)), max(0, min(parsed_minute, 59))


def _compute_next_run_at(*, interval_days: int, scheduled_hour: int, scheduled_minute: int) -> datetime:
    local_now = app_now()
    local_candidate = local_now.replace(
        hour=scheduled_hour,
        minute=scheduled_minute,
        second=0,
        microsecond=0,
    )
    if local_candidate <= local_now:
        local_candidate = local_candidate + timedelta(days=interval_days)
    return local_candidate.astimezone(timezone.utc)


def _validate_storage_configuration() -> None:
    if not settings.supabase_url:
        raise RuntimeError("SUPABASE_URL nao configurada.")
    if not settings.supabase_service_role_key:
        raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY nao configurada.")
    if not settings.supabase_bucket_db:
        raise RuntimeError("SUPABASE_BUCKET_DB nao configurado.")
    if not settings.supabase_bucket_files:
        raise RuntimeError("SUPABASE_BUCKET_FILES nao configurado.")


def _backup_database_dump(started_at: datetime, on_upload_start=None) -> dict:
    timestamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    with tempfile.NamedTemporaryFile(prefix="backup_db_", suffix=".sql", delete=False) as temp_file:
        sql_path = Path(temp_file.name)
    gzip_path = sql_path.with_suffix(".sql.gz")
    object_path = f"database/{started_at:%Y/%m/%d}/{timestamp}_postgres_backup.sql.gz"

    try:
        command = [
            "pg_dump",
            "--dbname",
            settings.database_backup_url,
            "--no-owner",
            "--no-privileges",
            "--lock-wait-timeout=30s",
            "--file",
            str(sql_path),
        ]
        subprocess.run(
            command,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=PG_DUMP_TIMEOUT_SECONDS,
            env={**os.environ, "PGCONNECT_TIMEOUT": "15"},
        )
        with sql_path.open("rb") as source, gzip.open(gzip_path, "wb") as target:
            shutil.copyfileobj(source, target)
        if callable(on_upload_start):
            on_upload_start()
        _upload_file_to_supabase(
            bucket=settings.supabase_bucket_db,
            object_path=object_path,
            file_path=gzip_path,
            content_type="application/gzip",
        )
        return {
            "object_path": object_path,
            "size_bytes": gzip_path.stat().st_size,
            "details": {
                "timestamp": timestamp,
                "format": "sql.gz",
                "database_url_mode": "pg_dump",
            },
        }
    except FileNotFoundError as exc:
        raise RuntimeError(
            "Comando pg_dump nao encontrado no ambiente. "
            "Verifique se o pacote postgresql-client esta instalado no container."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"pg_dump excedeu o tempo limite de {PG_DUMP_TIMEOUT_SECONDS}s. "
            "Verifique a conectividade com o banco de dados."
        ) from exc
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        raise RuntimeError(stderr or "pg_dump retornou erro ao gerar o dump do banco.") from exc
    finally:
        sql_path.unlink(missing_ok=True)
        gzip_path.unlink(missing_ok=True)


def _backup_files_archive(started_at: datetime, on_upload_start=None) -> dict:
    timestamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    with tempfile.NamedTemporaryFile(prefix="backup_files_", suffix=".zip", delete=False) as temp_file:
        archive_path = Path(temp_file.name)
    object_path = f"files/{started_at:%Y/%m/%d}/{timestamp}_system_files_backup.zip"

    details = {
        "timestamp": timestamp,
        "local_directories": [],
        "local_file_count": 0,
        "storage_scope": "local_directories_only",
        "notes": [],
    }

    try:
        with zipfile.ZipFile(archive_path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            for directory in _existing_local_directories():
                try:
                    details["local_directories"].append(str(directory.relative_to(PROJECT_ROOT)))
                except ValueError:
                    details["local_directories"].append(str(directory))
                for file_path in sorted(path for path in directory.rglob("*") if path.is_file()):
                    arcname = Path("local") / file_path.relative_to(PROJECT_ROOT)
                    archive.write(file_path, arcname.as_posix())
                    details["local_file_count"] += 1

            if not details["local_directories"]:
                details["notes"].append("Nenhum diretorio local de upload foi encontrado no projeto atual.")
                details["notes"].append("No estado atual do projeto, anexos operacionais permanecem cobertos pelo dump do PostgreSQL.")
                details["notes"].append("O bucket de arquivos fica preparado para futuros diretorios locais ou migracao de storage.")
            archive.writestr("manifest.json", json.dumps(details, ensure_ascii=False, indent=2))

        if callable(on_upload_start):
            on_upload_start()
        _upload_file_to_supabase(
            bucket=settings.supabase_bucket_files,
            object_path=object_path,
            file_path=archive_path,
            content_type="application/zip",
        )
        return {
            "object_path": object_path,
            "size_bytes": archive_path.stat().st_size,
            "details": details,
        }
    finally:
        archive_path.unlink(missing_ok=True)


def _existing_local_directories() -> list[Path]:
    configured = [
        value.strip()
        for value in (settings.backup_local_dirs or "").split(",")
        if value.strip()
    ]
    directories: list[Path] = []
    seen: set[Path] = set()
    for raw_path in configured + LOCAL_BACKUP_DIR_CANDIDATES:
        path = Path(raw_path)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        path = path.resolve()
        if path in seen or not path.exists() or not path.is_dir():
            continue
        seen.add(path)
        directories.append(path)
    return directories


def _upload_file_to_supabase(bucket: str, object_path: str, file_path: Path, content_type: str) -> None:
    upload_url = (
        f"{settings.supabase_url.rstrip('/')}/storage/v1/object/"
        f"{quote(bucket, safe='')}/{quote(object_path, safe='/')}"
    )
    headers = {
        "Authorization": f"Bearer {settings.supabase_service_role_key}",
        "apikey": settings.supabase_service_role_key or "",
        "Content-Type": content_type,
        "x-upsert": "true",
    }
    timeout = httpx.Timeout(BACKUP_UPLOAD_TIMEOUT_SECONDS, connect=30.0)
    try:
        with httpx.Client(timeout=timeout) as client, file_path.open("rb") as body:
            response = client.post(upload_url, headers=headers, content=body)
            if response.status_code not in (200, 201):
                detail = (response.text or "")[:800].strip()
                raise RuntimeError(
                    detail or f"Supabase Storage retornou status {response.status_code} ao enviar backup."
                )
    except httpx.RequestError as exc:
        raise RuntimeError(f"Falha de rede ao enviar backup para o Supabase: {exc}") from exc


def _delete_object_from_supabase(bucket: str, object_path: str) -> tuple[bool, bool]:
    _validate_storage_configuration()
    delete_url = (
        f"{settings.supabase_url.rstrip('/')}/storage/v1/object/"
        f"{quote(bucket, safe='')}/{quote(object_path, safe='/')}"
    )
    request = Request(
        delete_url,
        method="DELETE",
        headers={
            "Authorization": f"Bearer {settings.supabase_service_role_key}",
            "apikey": settings.supabase_service_role_key or "",
        },
    )
    try:
        with urlopen(request, timeout=120) as response:
            if response.status not in (200, 204):
                raise RuntimeError(f"Supabase Storage retornou status {response.status}.")
            return True, False
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore").strip()
        normalized_body = body.lower()
        if exc.code == 404 or "not found" in normalized_body or "no such object" in normalized_body:
            return False, True
        raise RuntimeError(body or f"Erro HTTP {exc.code} ao excluir backup no Supabase.") from exc
    except URLError as exc:
        raise RuntimeError(f"Falha de rede ao excluir backup no Supabase: {exc.reason}") from exc


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def main() -> None:
    from app.db.session import SessionLocal

    with SessionLocal() as db:
        run = execute_backup(db, initiated_by=None, trigger_source="cli")
        print(
            json.dumps(
                {
                    "id": run.id,
                    "status": run.status,
                    "database_object_path": run.database_object_path,
                    "files_object_path": run.files_object_path,
                    "error_message": run.error_message,
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()
