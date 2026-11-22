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


def test_fictional_example_matches_received_schema_but_signature_is_placeholder(tmp_path):
    from lxml import etree
    copy = tmp_path / 'schema'
    shutil.copytree(BUNDLE, copy)
    manifest_path = copy / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    manifest['reviewed'] = True  # somente na cópia temporária para teste estrutural
    manifest_path.write_text(json.dumps(manifest))
    fixture = Path(__file__).parent / 'fixtures' / 'nfe' / 'estrutura_ficticia_xsd.xml'
    xml = fixture.read_bytes()
    result = validate_xml(xml, schema_directory=copy)
    assert result['valid'] and result['schema_version'] == '010h-v1.03'
    root = etree.fromstring(xml)
    ns = {'n': 'http://www.portalfiscal.inf.br/nfe', 'ds': 'http://www.w3.org/2000/09/xmldsig#'}
    assert root.xpath('string(n:infNFe/n:ide/n:tpAmb)', namespaces=ns) == '2'
    assert root.xpath('string(ds:Signature/ds:SignatureValue)', namespaces=ns) == 'AA=='
    # XSD não verifica a assinatura, nem chave, tributação ou regras de negócio da SEFAZ.
    altered = xml.replace(b'<mod>55</mod>', b'<mod>99</mod>')
    assert not validate_xml(altered, schema_directory=copy)['valid']
