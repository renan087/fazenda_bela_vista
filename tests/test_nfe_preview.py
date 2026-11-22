from types import SimpleNamespace
from xml.etree import ElementTree as ET
import pytest
from app.services.nfe_preview import build_preview


def test_preview_is_not_nfe_and_escapes_untrusted_content():
    snapshot = {'venda': 'V-1', 'data_venda': '2026-11-21', 'comprador': 'A & <B>', 'valor_bruto': '5.00',
                'itens': [{'lote': 'L<1>', 'quantidade_sacas': '1', 'valor_bruto': '5.00'}]}
    issuer = SimpleNamespace(name='Teste', cpf='52998224725')
    xml = build_preview(snapshot, issuer, {'product_description': 'Café & leite', 'unknown': 'segredo'},
                        {'freight_mode': '9', 'items': [{'item_index': 1, 'icms_cst': '20'}]})
    root = ET.fromstring(xml)
    assert root.tag == 'preparacao_sisfarm' and root.attrib['natureza'] == 'simulacao_sem_validade_fiscal'
    assert root.findtext('venda/comprador') == 'A & <B>' and root.findtext('lotes/lote/icms_cst') == '20'
    assert root.find('NFe') is None and b'segredo' not in xml and b'<Signature' not in xml


def test_preview_rejects_mismatched_technical_lines():
    snapshot = {'itens': [{'lote': 'L1'}]}
    for technical in ({'items': [{'item_index': 2}]}, {'items': [{'item_index': 1}, {'item_index': 1}]}, {'items': 'bad'}):
        with pytest.raises(ValueError):
            build_preview(snapshot, None, {}, technical)


def test_preview_route_denies_another_organization(monkeypatch):
    import json
    from fastapi import HTTPException
    from app.models.nfe_draft import NfeDraft
    from app.web import routes
    from tests.test_stock_assistant import _route_env
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    draft = NfeDraft(id='e' * 32, organization_id=user.organization_id + 999, farm_id=scope['active_farm_id'],
                     sale_code='V-1', created_by=user.id, snapshot_json=json.dumps({'itens': [{}]}))
    db.add(draft); db.commit()
    monkeypatch.setattr(routes, '_launch_scope_or_redirect', lambda *args: (scope, None))
    with pytest.raises(HTTPException) as exc:
        routes.fiscal_preview_xml(draft.id, SimpleNamespace(), db, user)
    assert exc.value.status_code == 404
    db.close()


def test_preview_rejects_invalid_xml_characters():
    with pytest.raises(ValueError):
        build_preview({'itens': [{'lote': 'L\x01'}]}, None, {}, {})


def test_preview_maps_saved_test_values_per_lot_without_inventing_tax():
    snapshot = {'itens': [{'lote': 'A'}, {'lote': 'B'}]}
    technical = {'items': [{'item_index': 2, 'icms_base': '23.45', 'cbs_value': '0',
                            'tax_unit': 'KG', 'tax_quantity': '10', 'tax_unit_price': '2.345',
                            'cbs_rate': '0', 'unknown_tax': 'secret'}, {'item_index': 1, 'icms_base': '0'}]}
    root = ET.fromstring(build_preview(snapshot, None, {}, technical))
    rows = root.findall('lotes/lote')
    assert rows[0].findtext('icms_base') == '0'
    assert rows[0].findtext('cbs_value') == ''
    assert rows[1].findtext('icms_base') == '23.45'
    assert rows[1].findtext('cbs_value') == '0'
    assert rows[1].findtext('cbs_rate') == '0' and rows[0].findtext('cbs_rate') == ''
    assert rows[1].findtext('tax_unit') == 'KG'
    assert rows[1].findtext('tax_quantity') == '10'
    assert rows[1].findtext('tax_unit_price') == '2.345'
    assert b'secret' not in ET.tostring(root)
    assert root.tag == 'preparacao_sisfarm' and root.find('NFe') is None
