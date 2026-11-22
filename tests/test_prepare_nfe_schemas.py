import json
import zipfile
import pytest
from scripts.preparar_schemas_nfe import prepare
from app.services.nfe_schema import validate_xml, SchemaUnavailable


def test_importer_keeps_unreviewed_package_disabled(tmp_path):
    archive = tmp_path / 'schema.zip'
    with zipfile.ZipFile(archive, 'w') as z:
        z.writestr('pacote/nfe_v4.00.xsd', '<schema/>')
    dest = tmp_path / 'review'
    manifest = prepare(archive, dest, version='fixture')
    assert manifest['reviewed'] is False and len(manifest['archive_sha256']) == 64
    assert json.loads((dest / 'manifest.json').read_text())['reviewed'] is False
    with pytest.raises(SchemaUnavailable):
        validate_xml(b'<x/>', schema_directory=dest)
    with pytest.raises(ValueError):
        prepare(archive, dest, version='fixture')


def test_importer_rejects_traversal_and_duplicates(tmp_path):
    for idx, names in enumerate([['../nfe_v4.00.xsd'], ['a/nfe_v4.00.xsd', 'b/nfe_v4.00.xsd']]):
        archive = tmp_path / f'{idx}.zip'
        with zipfile.ZipFile(archive, 'w') as z:
            for name in names:
                z.writestr(name, '<schema/>')
        with pytest.raises(ValueError):
            prepare(archive, tmp_path / f'out{idx}', version='fixture')
        assert not (tmp_path / f'out{idx}').exists()
