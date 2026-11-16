"""Reconstrói um backup dividido a partir do manifesto e das partes baixadas."""

import argparse
import hashlib
import json
from pathlib import Path


def reconstruir(manifest_path: Path, parts_dir: Path, output: Path) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    parts = manifest.get("parts")
    if manifest.get("version") != 1 or not isinstance(parts, list) or not parts:
        raise ValueError("Manifesto de backup inválido.")
    digest = hashlib.sha256()
    total = 0
    created = False
    try:
        with output.open("xb") as target:
            created = True
            for index, part in enumerate(parts, 1):
                expected_name = f"part-{index:06d}"
                if not isinstance(part, dict) or Path(part.get("path", "")).name != expected_name:
                    raise ValueError("Ordem ou nome de parte inválido no manifesto.")
                part_digest = hashlib.sha256()
                size = 0
                with (parts_dir / expected_name).open("rb") as source:
                    while chunk := source.read(1024 * 1024):
                        target.write(chunk)
                        digest.update(chunk)
                        part_digest.update(chunk)
                        size += len(chunk)
                if size != part.get("size_bytes") or part_digest.hexdigest() != part.get("sha256"):
                    raise ValueError(f"Parte {expected_name} incompleta ou corrompida.")
                total += size
        if total != manifest.get("size_bytes") or digest.hexdigest() != manifest.get("sha256"):
            raise ValueError("Arquivo reconstruído não corresponde ao manifesto.")
    except Exception:
        if created:
            output.unlink(missing_ok=True)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifesto", type=Path)
    parser.add_argument("diretorio_partes", type=Path)
    parser.add_argument("arquivo_saida", type=Path)
    args = parser.parse_args()
    reconstruir(args.manifesto, args.diretorio_partes, args.arquivo_saida)
    print("Backup reconstruído e integridade verificada:", args.arquivo_saida)


if __name__ == "__main__":
    main()
