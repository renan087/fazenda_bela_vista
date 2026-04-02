import gzip
import json
import logging
import shutil
import subprocess
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models import BackupRun, User

logger = logging.getLogger(__name__)
settings = get_settings()
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOCAL_BACKUP_DIR_CANDIDATES = [
    "app/static/uploads",
    "app/uploads",
    "uploads",
    "storage/uploads",
]


def execute_backup(
    db: Session,
    initiated_by: User | None = None,
    trigger_source: str = "manual",
) -> BackupRun:
    run = BackupRun(
        initiated_by_user_id=initiated_by.id if initiated_by else None,
        trigger_source=trigger_source,
        status="running",
        database_bucket=settings.supabase_bucket_db,
        files_bucket=settings.supabase_bucket_files,
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    started_at = _utc_now()
    db_result: dict | None = None
    files_result: dict | None = None
    errors: list[str] = []

    try:
        _validate_storage_configuration()
        db_result = _backup_database_dump(started_at)
    except Exception as exc:
        logger.exception("Falha ao gerar ou enviar backup do banco.")
        errors.append(f"Banco: {exc}")

    try:
        _validate_storage_configuration()
        files_result = _backup_files_archive(started_at)
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
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def delete_backup_run(db: Session, run: BackupRun) -> list[str]:
    warnings: list[str] = []
    storage_errors: list[str] = []

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

    db.delete(run)
    db.commit()
    return warnings


def _validate_storage_configuration() -> None:
    if not settings.supabase_url:
        raise RuntimeError("SUPABASE_URL nao configurada.")
    if not settings.supabase_service_role_key:
        raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY nao configurada.")
    if not settings.supabase_bucket_db:
        raise RuntimeError("SUPABASE_BUCKET_DB nao configurado.")
    if not settings.supabase_bucket_files:
        raise RuntimeError("SUPABASE_BUCKET_FILES nao configurado.")


def _backup_database_dump(started_at: datetime) -> dict:
    timestamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    with tempfile.NamedTemporaryFile(prefix="backup_db_", suffix=".sql", delete=False) as temp_file:
        sql_path = Path(temp_file.name)
    gzip_path = sql_path.with_suffix(".sql.gz")
    object_path = f"database/{started_at:%Y/%m/%d}/{timestamp}_postgres_backup.sql.gz"

    try:
        command = [
            "pg_dump",
            settings.database_backup_url,
            "--no-owner",
            "--no-privileges",
            "--file",
            str(sql_path),
        ]
        subprocess.run(command, check=True, capture_output=True, text=True)
        with sql_path.open("rb") as source, gzip.open(gzip_path, "wb") as target:
            shutil.copyfileobj(source, target)
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
        raise RuntimeError("Comando pg_dump nao encontrado no ambiente.") from exc
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        raise RuntimeError(stderr or "pg_dump retornou erro ao gerar o dump do banco.") from exc
    finally:
        sql_path.unlink(missing_ok=True)
        gzip_path.unlink(missing_ok=True)


def _backup_files_archive(started_at: datetime) -> dict:
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
    request = Request(
        upload_url,
        data=file_path.read_bytes(),
        method="POST",
        headers={
            "Authorization": f"Bearer {settings.supabase_service_role_key}",
            "apikey": settings.supabase_service_role_key or "",
            "Content-Type": content_type,
            "x-upsert": "true",
        },
    )
    try:
        with urlopen(request, timeout=120) as response:
            if response.status not in (200, 201):
                raise RuntimeError(f"Supabase Storage retornou status {response.status}.")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore").strip()
        raise RuntimeError(body or f"Erro HTTP {exc.code} ao enviar backup para o Supabase.") from exc
    except URLError as exc:
        raise RuntimeError(f"Falha de rede ao enviar backup para o Supabase: {exc.reason}") from exc


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
