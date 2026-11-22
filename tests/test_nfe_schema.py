from hashlib import sha256
import json
import pytest
from app.services.nfe_schema import validate_xml, SchemaUnavailable, UnsafeXML


def bundle(path, files, entry='main.xsd'):
    for name, value in files.items():
        (path / name).write_bytes(value)
    manifest = {'version': 'fixture-nao-oficial', 'entry': entry, 'reviewed': True,
                'files': {name: sha256(value).hexdigest() for name, value in files.items()}}
    (path / 'manifest.json').write_text(json.dumps(manifest))


def test_xsd_validation_offline_and_tamper_detection(tmp_path):
    schema = b'''<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:element name="valor" type="xs:integer"/></xs:schema>'''
    bundle(tmp_path, {'main.xsd': schema})
    assert validate_xml(b'<valor>1</valor>', schema_directory=tmp_path)['valid']
    assert not validate_xml(b'<valor>abc</valor>', schema_directory=tmp_path)['valid']
    (tmp_path / 'main.xsd').write_bytes(schema + b' ')
    with pytest.raises(SchemaUnavailable):
        validate_xml(b'<valor>1</valor>', schema_directory=tmp_path)


def test_missing_schema_and_external_entity_fail_closed(tmp_path):
    with pytest.raises(SchemaUnavailable):
        validate_xml(b'<valor>1</valor>', schema_directory=tmp_path)
    schema = b'''<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:element name="valor" type="xs:string"/></xs:schema>'''
    bundle(tmp_path, {'main.xsd': schema})
    with pytest.raises(UnsafeXML):
        validate_xml(b'<!DOCTYPE valor [<!ENTITY x SYSTEM "file:///etc/passwd">]><valor>&x;</valor>', schema_directory=tmp_path)


def test_schema_cannot_read_external_reference(tmp_path):
    schema = b'''<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:include schemaLocation="https://example.com/evil.xsd"/></xs:schema>'''
    bundle(tmp_path, {'main.xsd': schema})
    with pytest.raises(SchemaUnavailable):
        validate_xml(b'<valor/>', schema_directory=tmp_path)


def test_verified_local_schema_includes_work(tmp_path):
    main = b'''<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:include schemaLocation="types.xsd"/><xs:element name="valor" type="Positive"/></xs:schema>'''
    nested = b'''<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:simpleType name="Positive"><xs:restriction base="xs:positiveInteger"/></xs:simpleType></xs:schema>'''
    bundle(tmp_path, {'main.xsd': main, 'types.xsd': nested})
    assert validate_xml(b'<valor>3</valor>', schema_directory=tmp_path)['valid']
    assert not validate_xml(b'<valor>-1</valor>', schema_directory=tmp_path)['valid']


def test_schema_rejects_local_file_escape(tmp_path):
    schema = b'''<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:include schemaLocation="../outside.xsd"/></xs:schema>'''
    bundle(tmp_path, {'main.xsd': schema})
    with pytest.raises(SchemaUnavailable):
        validate_xml(b'<valor/>', schema_directory=tmp_path)


def test_preparation_is_not_validated_as_official_without_schema(tmp_path):
    from app.services.nfe_preview import build_preview
    preview = build_preview({'itens': [{'lote': 'Teste'}]}, None, {}, {})
    with pytest.raises(SchemaUnavailable):
        validate_xml(preview, schema_directory=tmp_path)
