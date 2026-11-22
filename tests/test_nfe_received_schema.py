"""Testes estruturais com o pacote recebido; não comprovam autenticidade nem homologação."""
from pathlib import Path
import json
import shutil
import pytest
from app.services.nfe_schema import SchemaUnavailable, validate_xml
from app.services.nfe_preview import build_preview

BUNDLE = Path(__file__).resolve().parents[1] / 'app' / 'fiscal' / 'schemas' / 'nfe'


def test_unverified_bundle_is_disabled():
    manifest = json.loads((BUNDLE / 'manifest.json').read_text())
    assert manifest['reviewed'] is False
    assert manifest['archive_sha256'] == 'bcbf6b277f79a6983d69016dfe0430f09b8a191997a4706a24938acddc18dc81'
    with pytest.raises(SchemaUnavailable):
        validate_xml(b'<NFe/>')


def test_received_schema_compiles_offline_and_rejects_incomplete_document(tmp_path):
    copy = tmp_path / 'schema'
    shutil.copytree(BUNDLE, copy)
    manifest_path = copy / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    manifest['reviewed'] = True  # fixture temporária, não ativa o pacote na aplicação
    manifest_path.write_text(json.dumps(manifest))
    result = validate_xml(b'<NFe xmlns="http://www.portalfiscal.inf.br/nfe"/>', schema_directory=copy)
    assert result['schema_version'] == '010h-v1.03' and not result['valid']
    assert any('infNFe' in error['message'] for error in result['errors'])
    preview = build_preview({'itens': [{'lote': 'Teste'}]}, None, {}, {})
    assert not validate_xml(preview, schema_directory=copy)['valid']
