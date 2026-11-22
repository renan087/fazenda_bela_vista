import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET
import pytest
from app.services.nfe_candidate import build_incomplete_candidate
from app.services.nfe_schema import validate_xml


def example():
    return {'itens': [{'lote': 'L1', 'quantidade_sacas': '1.5000', 'valor_bruto': '100.00'}]}


def test_maps_only_saved_values_and_cannot_be_emitted(tmp_path):
    producer = SimpleNamespace(name='Teste & Filho', cpf='52998224725', state='BA')
    details = {'recipient_cpf': '11144477735', 'recipient_name': 'Teste', 'recipient_ie_indicator': 'nao_contribuinte',
               'operation_nature': 'Venda', 'operation_cfop': '5102', 'product_description': 'Cafe', 'product_ncm': '09011110'}
    xml = build_incomplete_candidate(example(), producer, details, {'freight_mode': '9', 'items': [{'item_index': 1, 'commercial_quantity': '1.5000', 'commercial_unit': 'SC'}]})
    root = ET.fromstring(xml)
    ns = {'n':'http://www.portalfiscal.inf.br/nfe'}
    assert root.findtext('n:infNFe/n:ide/n:tpAmb', namespaces=ns) == '2'
    assert root.findtext('n:infNFe/n:emit/n:xNome', namespaces=ns) == 'Teste & Filho'
    assert root.findtext('n:infNFe/n:dest/n:indIEDest', namespaces=ns) == '9'
    assert root.findtext('n:infNFe/n:det/n:prod/n:qCom', namespaces=ns) == '1.5000'
    assert root.find('n:infNFe', ns).get('Id') is None and b'Signature' not in xml and b'<imposto' not in xml
    bundle = Path(__file__).resolve().parents[1] / 'app/fiscal/schemas/nfe'
    copy = tmp_path / 'schema'; shutil.copytree(bundle, copy)
    manifest = copy / 'manifest.json'; obj = json.loads(manifest.read_text()); obj['reviewed'] = True; manifest.write_text(json.dumps(obj))
    assert not validate_xml(xml, schema_directory=copy)['valid']


def test_rejects_invalid_amounts_and_does_not_invent_missing_fields():
    with pytest.raises(ValueError):
        build_incomplete_candidate({'itens': [{'quantidade_sacas': '1', 'valor_bruto': 'NaN'}]}, None, {}, {})
    xml = build_incomplete_candidate(example(), None, {}, {})
    assert b'<CPF>' not in xml and b'<natOp>' not in xml and b'<modFrete>' not in xml


def test_candidate_route_denies_another_organization(monkeypatch):
    from fastapi import HTTPException
    from app.models.nfe_draft import NfeDraft
    from app.web import routes
    from tests.test_stock_assistant import _route_env
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    draft = NfeDraft(id='9' * 32, organization_id=user.organization_id + 1, farm_id=scope['active_farm_id'],
                     sale_code='V-1', created_by=user.id, snapshot_json=json.dumps(example()))
    db.add(draft); db.commit()
    monkeypatch.setattr(routes, '_launch_scope_or_redirect', lambda *args: (scope, None))
    with pytest.raises(HTTPException) as exc:
        routes.fiscal_candidate_xml(draft.id, SimpleNamespace(), db, user)
    assert exc.value.status_code == 404
    db.close()
