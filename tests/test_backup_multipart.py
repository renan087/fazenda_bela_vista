import json
from types import SimpleNamespace

import pytest

from app.services import backup_service as backup
from scripts.reconstruir_backup import reconstruir


def test_multipart_round_trip_and_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(backup, "BACKUP_PART_SIZE_BYTES", 5)
    objects = {}
    monkeypatch.setattr(backup, "_upload_file_to_supabase", lambda bucket, object_path, file_path, content_type:
                        objects.update({object_path: file_path.read_bytes()}))
    original = tmp_path / "original"
    original.write_bytes(b"abcdefghijklmnop")
    prepared = {"bucket": "db", "object_path": "database/backup.gz", "local_path": original,
                "content_type": "application/gzip", "details": {}}
    backup._upload_prepared_backup(prepared)
    manifest = json.loads(objects[prepared["object_path"]])
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text(json.dumps(manifest))
    parts_dir = tmp_path / "parts"
    parts_dir.mkdir()
    for part in manifest["parts"]:
        (parts_dir / part["path"].rsplit("/", 1)[-1]).write_bytes(objects[part["path"]])
    output = tmp_path / "restored.gz"
    reconstruir(manifest_file, parts_dir, output)
    assert output.read_bytes() == original.read_bytes()
    run = SimpleNamespace(details_json=json.dumps({"database": prepared["details"]}))
    assert set(backup._backup_object_paths(run, "banco", prepared["object_path"])) == set(objects)
    (parts_dir / "part-000001").write_bytes(b"broken")
    with pytest.raises(ValueError, match="corrompida"):
        reconstruir(manifest_file, parts_dir, tmp_path / "corrupt.gz")
    assert not (tmp_path / "corrupt.gz").exists()


def test_failed_part_cleans_uploaded_objects(monkeypatch, tmp_path):
    monkeypatch.setattr(backup, "BACKUP_PART_SIZE_BYTES", 3)
    uploaded, deleted = [], []

    def upload(bucket, object_path, file_path, content_type):
        if object_path.endswith("000002"):
            raise RuntimeError("upload falhou")
        uploaded.append(object_path)

    monkeypatch.setattr(backup, "_upload_file_to_supabase", upload)
    monkeypatch.setattr(backup, "_delete_object_from_supabase", lambda bucket, path: deleted.append(path))
    path = tmp_path / "backup"
    path.write_bytes(b"abcdefgh")
    with pytest.raises(RuntimeError, match="upload falhou"):
        backup._upload_prepared_backup({"bucket": "db", "object_path": "backup", "local_path": path,
                                        "content_type": "application/gzip", "details": {}})
    assert deleted == uploaded


def test_single_file_remains_compatible(monkeypatch, tmp_path):
    monkeypatch.setattr(backup, "BACKUP_PART_SIZE_BYTES", 10)
    calls = []
    monkeypatch.setattr(backup, "_upload_file_to_supabase", lambda *args: calls.append(args))
    path = tmp_path / "small"
    path.write_bytes(b"abc")
    prepared = {"bucket": "db", "object_path": "backup.gz", "local_path": path,
                "content_type": "application/gzip", "details": {}}
    backup._upload_prepared_backup(prepared)
    assert len(calls) == 1 and prepared["object_path"] == "backup.gz"
