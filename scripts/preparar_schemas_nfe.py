"""Prepara pacote XSD para revisão manual; não comprova origem e não ativa validação."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import stat
import zipfile

MAX_ENTRIES = 500
MAX_FILE = 10 * 1024 * 1024
MAX_TOTAL = 50 * 1024 * 1024


def prepare(archive, destination, *, version, entry='nfe_v4.00.xsd'):
    archive, destination = Path(archive), Path(destination)
    if destination.exists():
        raise ValueError('O destino já existe; escolha um diretório novo.')
    if not version.strip() or '/' in entry or '\\' in entry:
        raise ValueError('Versão ou schema principal inválido.')
    if archive.stat().st_size > MAX_TOTAL:
        raise ValueError('Arquivo ZIP excede o tamanho permitido.')
    zip_hash = hashlib.sha256(archive.read_bytes()).hexdigest()
    files = {}
    with zipfile.ZipFile(archive) as zipped:
        infos = zipped.infolist()
        if len(infos) > MAX_ENTRIES:
            raise ValueError('Pacote possui arquivos demais.')
        total = 0
        for item in infos:
            raw = item.filename
            parts = PurePosixPath(raw).parts
            if '\\' in raw or raw.startswith('/') or '..' in parts or stat.S_ISLNK(item.external_attr >> 16):
                raise ValueError('Caminho ou arquivo não permitido no pacote.')
            if item.is_dir() or not raw.lower().endswith('.xsd'):
                continue
            if item.flag_bits & 1 or item.file_size > MAX_FILE:
                raise ValueError('XSD criptografado ou maior que o limite.')
            name = PurePosixPath(raw).name
            if not name.endswith('.xsd') or name.casefold() in {item.casefold() for item in files}:
                raise ValueError('O pacote contém nomes de XSD inválidos ou duplicados.')
            total += item.file_size
            if total > MAX_TOTAL:
                raise ValueError('Pacote excede o tamanho permitido.')
            value = zipped.read(item)
            if len(value) != item.file_size:
                raise ValueError('Tamanho do arquivo divergente.')
            files[name] = value
    if entry not in files:
        raise ValueError('Schema principal não encontrado no pacote.')
    # Extrair para um diretório novo e manter desativado até revisão de origem e imports.
    destination.mkdir(parents=True, exist_ok=False)
    for name, value in files.items():
        (destination / name).write_bytes(value)
    manifest = {'version': version, 'entry': entry, 'reviewed': False, 'archive_sha256': zip_hash,
                'files': {name: hashlib.sha256(value).hexdigest() for name, value in sorted(files.items())}}
    (destination / 'manifest.json').write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', help='Arquivo ZIP obtido de fonte oficial')
    parser.add_argument('destination', help='Diretório novo para revisão')
    parser.add_argument('--version', required=True, help='Versão informada na publicação oficial')
    parser.add_argument('--entry', default='nfe_v4.00.xsd', help='Schema principal a revisar')
    args = parser.parse_args()
    manifest = prepare(args.archive, args.destination, version=args.version, entry=args.entry)
    print(f"Preparados {len(manifest['files'])} arquivos; SHA-256 do ZIP: {manifest['archive_sha256']}. Validação permanece desativada.")


if __name__ == '__main__':
    main()
