from datetime import date
from decimal import Decimal
from types import SimpleNamespace
import json
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.db.base import Base
from app.services.nfe_draft import create_draft


def test_draft_is_an_independent_snapshot_and_no_fiscal_number():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    db = Session(engine)
    row = SimpleNamespace(id=7, farm_id=1, sale_code='V-0001', buyer_id=4, buyer_name='Comprador',
                          sale_date=date(2026, 11, 20), total_value=Decimal('120.10'),
                          equivalent_sacks=Decimal('2.0000'), lot_label='Lote 1')
    draft = create_draft(db, organization_id=2, farm_id=1, user_id=3, records=[row])
    row.buyer_name = 'Outro'
    snapshot = json.loads(draft.snapshot_json)
    assert snapshot['comprador'] == 'Comprador' and snapshot['valor_bruto'] == '120.10'
    assert not {'numero', 'chave', 'autorizada'} & set(snapshot)
    with pytest.raises(ValueError):
        create_draft(db, organization_id=2, farm_id=999, user_id=3, records=[row])
    db.close()


def test_issuer_format_validation_and_bahia_scope():
    from app.services.nfe_draft import clean_issuer
    valid = clean_issuer({'name': 'Produtor de teste', 'cpf': '529.982.247-25', 'postal_code': '40000-000',
                          'municipality_code': '2927408', 'state': 'BA'})
    assert valid['cpf'] == '52998224725' and valid['municipality_code'] == '2927408'
    assert clean_issuer({})['state'] == 'BA'
    with pytest.raises(ValueError):
        clean_issuer({'cpf': '111.111.111-11'})
    with pytest.raises(ValueError):
        clean_issuer({'state': 'SP'})
    with pytest.raises(ValueError):
        clean_issuer({'municipality_code': '123'})


def test_issuer_profiles_are_separated_by_farm_and_organization():
    from app.models.nfe_draft import NfeIssuerProfile
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    db = Session(engine)
    db.add_all([NfeIssuerProfile(id='a' * 32, organization_id=1, farm_id=10, name='A'),
                NfeIssuerProfile(id='b' * 32, organization_id=2, farm_id=11, name='B')])
    db.commit()
    assert db.query(NfeIssuerProfile).filter_by(organization_id=1, farm_id=11).first() is None
    assert db.query(NfeIssuerProfile).filter_by(organization_id=2, farm_id=10).first() is None
    db.close()


def test_fiscal_draft_template_renders_issuer_values():
    from jinja2 import ChoiceLoader, DictLoader
    from app.web import routes
    env = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({'base.html': '{% block content %}{% endblock %}'}), routes.templates.env.loader]))
    issuer = SimpleNamespace(name='Produtor teste', cpf='52998224725', state_registration='', postal_code='',
                             street='', number='', complement='', district='', city='', municipality_code='')
    snapshot = {'venda': 'V-0001', 'comprador': 'Teste', 'data_venda': '2026-11-20', 'itens': [], 'valor_bruto': '0'}
    page = env.get_template('nfe_draft.html').render(draft=SimpleNamespace(id='id'), issuer=issuer,
                                                    snapshot=snapshot, details={}, technical={}, technical_items={}, csrf_token='x', can_write_page=True)
    assert 'Produtor teste' in page and 'não foi transmitido' in page


def test_preparation_requires_valid_formats_and_no_default_tax():
    from app.services.nfe_draft import clean_draft_details
    result = clean_draft_details({'recipient_cpf': '529.982.247-25', 'recipient_state': 'BA',
                                  'product_ncm': '0901.11.10', 'operation_cfop': '5102'})
    assert result['recipient_cpf'] == '52998224725' and result['product_ncm'] == '09011110'
    assert result['recipient_ie_indicator'] == '' and 'tax' not in result
    assert clean_draft_details({})['operation_cfop'] == ''
    for value in ({'recipient_cpf': '11111111111'}, {'recipient_state': 'SP'},
                  {'product_ncm': 'ABC'}, {'recipient_ie_indicator': 'errado'}):
        with pytest.raises(ValueError):
            clean_draft_details(value)


def test_buyer_suggestion_is_optional_and_rejects_company():
    from app.services.nfe_draft import buyer_prefill
    assert buyer_prefill(None) == {}
    assert buyer_prefill(SimpleNamespace(person_type='pj')) == {}
    buyer = SimpleNamespace(person_type='pf', name='Exemplo', document='52998224725', state_registration=None,
                            postal_code=None, street=None, number=None, complement=None, district=None, city=None, state='BA')
    assert buyer_prefill(buyer)['recipient_cpf'] == '52998224725'


def test_details_route_rejects_other_organization(monkeypatch):
    import asyncio
    from fastapi import HTTPException
    from app.models.nfe_draft import NfeDraft, NfeDraftDetails
    from app.web import routes
    from tests.test_stock_assistant import _route_env
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    draft = NfeDraft(id='f' * 32, organization_id=user.organization_id + 999,
                     farm_id=scope['active_farm_id'], sale_code='V-0001', created_by=user.id, snapshot_json='{}')
    db.add(draft)
    db.commit()
    class Request:
        async def form(self):
            return {'csrf_token': 'x', 'recipient_name': 'Teste'}
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_launch_scope_or_redirect', lambda *args: (scope, None))
    with pytest.raises(HTTPException) as exc:
        asyncio.run(routes.fiscal_details_save(draft.id, Request(), db, user))
    assert exc.value.status_code == 404 and db.query(NfeDraftDetails).count() == 0
    db.close()


def test_review_reports_missing_fields_and_never_authorizes_emission():
    from app.services.nfe_draft import review_preparation
    snapshot = {'data_venda': '2026-11-20', 'itens': [{'lote': 'L1', 'quantidade_sacas': '1', 'valor_bruto': '100.00'}], 'valor_bruto': '100.00'}
    review = review_preparation(snapshot, None, {}, details_saved=False)
    assert review['pending_count'] > 0
    assert any('salve' in issue.lower() for g in review['groups'] for issue in g['issues'])
    assert 'ready' not in review and 'authorized' not in review


def test_review_detects_total_mismatch_and_unsaved_buyer_suggestions():
    from app.services.nfe_draft import review_preparation
    issuer = SimpleNamespace(name='Teste', cpf='52998224725', state_registration='123', postal_code='40000000',
                             street='Rua', number='1', district='Centro', city='Salvador', municipality_code='2927408', state='BA')
    details = {'recipient_name': 'Teste', 'recipient_cpf': '52998224725', 'recipient_postal_code': '40000000',
               'recipient_street': 'Rua', 'recipient_number': '1', 'recipient_district': 'Centro', 'recipient_city': 'Salvador',
               'recipient_municipality_code': '2927408', 'recipient_state': 'BA', 'recipient_ie_indicator': 'nao_contribuinte',
               'product_description': 'Café', 'product_ncm': '09011110', 'operation_nature': 'Venda', 'operation_cfop': '5102'}
    snapshot = {'data_venda': '2026-11-20', 'itens': [{'lote': 'L1', 'quantidade_sacas': '1.00', 'valor_bruto': '100.00'}], 'valor_bruto': '99.99'}
    review = review_preparation(snapshot, issuer, details, details_saved=False)
    issues = [issue for group in review['groups'] for issue in group['issues']]
    assert any('Total dos itens' in issue for issue in issues)
    assert any('Confirme e salve' in issue for issue in issues)
    snapshot['valor_bruto'] = '100.00'
    assert review_preparation(snapshot, issuer, details, details_saved=True)['pending_count'] == 0
    details['recipient_ie_indicator'] = 'contribuinte'
    assert any('Inscrição estadual do destinatário' in issue for g in review_preparation(snapshot, issuer, details, details_saved=True)['groups'] for issue in g['issues'])


def test_technical_data_is_versioned_per_snapshot_line_without_tax_calculation():
    from app.services.nfe_draft import clean_technical_data
    snapshot = {'origem_ids': [8, 3], 'itens': [{'lote': 'L2'}, {'lote': 'L1'}]}
    result = clean_technical_data({'emission_date': '2026-11-20', 'freight_mode': '9',
                                   'item_1_origin': '0', 'item_1_icms_cst': '20',
                                   'item_1_ibs_cbs_cst': '000', 'item_1_ibs_cbs_class': '000001',
                                   'item_999_icms_cst': '12'}, snapshot)
    assert result['version'] == 1 and result['items'][0]['item_index'] == 1
    assert result['items'][0]['icms_cst'] == '20' and result['items'][1]['icms_cst'] == ''
    assert len(result['items']) == 2 and 'impostos' not in result
    for data in ({'freight_mode': '5'}, {'item_1_icms_cst': 'XYZ'}, {'emission_date': '20260230'}, {'emission_date': '20261120'}):
        with pytest.raises(ValueError):
            clean_technical_data(data, snapshot)
    with pytest.raises(ValueError):
        clean_technical_data({}, {'origem_ids': [3, 3], 'itens': [{}, {}]})


def test_technical_route_rejects_other_organization(monkeypatch):
    import asyncio
    from fastapi import HTTPException
    from app.models.nfe_draft import NfeDraft, NfeDraftTechnicalData
    from app.web import routes
    from tests.test_stock_assistant import _route_env
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    draft = NfeDraft(id='d' * 32, organization_id=user.organization_id + 999,
                     farm_id=scope['active_farm_id'], sale_code='V-0001', created_by=user.id, snapshot_json='{}')
    db.add(draft); db.commit()
    class Request:
        async def form(self):
            return {'csrf_token': 'x'}
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_launch_scope_or_redirect', lambda *args: (scope, None))
    with pytest.raises(HTTPException) as exc:
        asyncio.run(routes.fiscal_technical_save(draft.id, Request(), db, user))
    assert exc.value.status_code == 404 and db.query(NfeDraftTechnicalData).count() == 0
    db.close()


def test_technical_fields_render_for_the_correct_snapshot_line():
    from jinja2 import ChoiceLoader, DictLoader
    from app.web import routes
    env = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({'base.html': '{% block content %}{% endblock %}'}), routes.templates.env.loader]))
    snapshot = {'venda': 'V-1', 'comprador': 'Teste', 'data_venda': '2026-11-20', 'valor_bruto': '1',
                'itens': [{'lote': 'Primeiro', 'quantidade_sacas': '1', 'valor_bruto': '1'},
                          {'lote': 'Segundo', 'quantidade_sacas': '1', 'valor_bruto': '1'}]}
    page = env.get_template('nfe_draft.html').render(draft=SimpleNamespace(id='x'), issuer=None, details={},
        technical={'freight_mode': '9'}, technical_items={1: {'icms_cst': '20', 'icms_rate': '0'}, 2: {'icms_cst': '40', 'cbs_rate': '1.2345'}},
        snapshot=snapshot, csrf_token='x', can_write_page=True)
    assert 'name="item_1_icms_cst" value="20"' in page
    assert 'name="item_2_icms_cst" value="40"' in page
    assert 'name="item_1_icms_rate" value="0"' in page
    assert 'name="item_2_cbs_rate" value="1.2345"' in page


def test_technical_route_saves_test_data_for_authorized_farm(monkeypatch):
    import asyncio
    from app.models.nfe_draft import NfeDraft, NfeDraftTechnicalData
    from app.web import routes
    from tests.test_stock_assistant import _route_env
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    draft = NfeDraft(id='a' * 32, organization_id=user.organization_id, farm_id=scope['active_farm_id'],
                     sale_code='V-0001', created_by=user.id,
                     snapshot_json=json.dumps({'origem_ids': [7], 'itens': [{'lote': 'Teste'}]}))
    db.add(draft); db.commit()
    class Request:
        async def form(self):
            return {'csrf_token': 'x', 'freight_mode': '9', 'item_1_icms_cst': '20', 'item_1_cbs_rate': '1,2345'}
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_flash', lambda *args: None)
    monkeypatch.setattr(routes, '_launch_scope_or_redirect', lambda *args: (scope, None))
    result = asyncio.run(routes.fiscal_technical_save(draft.id, Request(), db, user))
    record = db.query(NfeDraftTechnicalData).one()
    assert result.status_code in (302, 303, 307) and record.organization_id == user.organization_id
    assert json.loads(record.data_json)['items'][0]['icms_cst'] == '20'
    assert json.loads(record.data_json)['items'][0]['cbs_rate'] == '1.2345'
    db.close()


def test_units_and_decimal_values_are_explicit_and_bounded():
    from app.services.nfe_draft import clean_technical_data, review_item_values
    snapshot = {'origem_ids': [8], 'itens': [{'lote': 'L1', 'valor_bruto': '100.00'}]}
    data = {'item_1_commercial_unit': 'sc', 'item_1_commercial_quantity': '1,2500',
            'item_1_commercial_unit_price': '80,0000000000', 'item_1_tax_unit': 'KG',
            'item_1_tax_quantity': '75.0000', 'item_1_tax_unit_price': '1.3333333333'}
    result = clean_technical_data(data, snapshot)
    assert result['items'][0]['commercial_quantity'] == '1.2500'
    assert result['items'][0]['tax_unit'] == 'KG'
    assert review_item_values(snapshot, result)[0]['issues'] == []
    data['item_1_tax_unit_price'] = '2'
    assert 'difere' in review_item_values(snapshot, clean_technical_data(data, snapshot))[0]['issues'][0]
    for field, value in [('item_1_commercial_quantity', '1.00001'), ('item_1_tax_unit_price', '-1'),
                         ('item_1_tax_quantity', 'NaN'), ('item_1_tax_unit', 'KG 2')]:
        with pytest.raises(ValueError):
            clean_technical_data({field: value}, snapshot)


def test_fictional_tax_values_preserve_zero_and_require_explicit_valid_amounts():
    from app.services.nfe_draft import clean_technical_data, summarize_test_tax_values
    snapshot = {'origem_ids': [1, 2], 'itens': [{'lote': 'A'}, {'lote': 'B'}]}
    data = {'item_1_icms_value': '0', 'item_2_icms_value': '12,50',
            'item_1_cbs_value': 0, 'item_2_cbs_value': ''}
    technical = clean_technical_data(data, snapshot)
    assert technical['items'][0]['icms_value'] == '0'
    assert technical['items'][0]['cbs_value'] == '0'
    summary = {row['field']: row for row in summarize_test_tax_values(snapshot, technical)}
    assert summary['icms_value']['total'] == '12.50'
    assert summary['cbs_value']['total'] is None and summary['cbs_value']['missing'] == 1
    assert summary['icms_base']['total'] is None
    for invalid in ('-1', '1.001', 'NaN', '10000000000000'):
        with pytest.raises(ValueError):
            clean_technical_data({'item_1_icms_value': invalid}, snapshot)
    technical['items'][1]['icms_value'] = 'NaN'
    assert next(x for x in summarize_test_tax_values(snapshot, technical) if x['field'] == 'icms_value')['total'] is None


def test_tax_rates_are_explicit_bounded_and_independent_per_lot():
    from app.services.nfe_draft import clean_technical_data
    snapshot = {'origem_ids': [1, 2], 'itens': [{}, {}]}
    row = clean_technical_data({'item_1_icms_rate': 0, 'item_1_ibs_uf_rate': '1,2345',
                                'item_2_cbs_rate': '100.0000'}, snapshot)['items']
    assert row[0]['icms_rate'] == '0' and row[0]['ibs_uf_rate'] == '1.2345'
    assert row[0]['cbs_rate'] == '' and row[1]['cbs_rate'] == '100.0000'
    for invalid in ('-1', '100.0001', '1.23456', 'NaN', '1e2'):
        with pytest.raises(ValueError):
            clean_technical_data({'item_1_cbs_rate': invalid}, snapshot)


def test_tax_review_identifies_missing_saved_fields_per_lot_without_judging_tax_rules():
    from app.services.nfe_draft import review_test_tax_fields
    snapshot = {'itens': [{'lote': 'A'}, {'lote': 'B'}, {'lote': 'C'}]}
    technical = {'items': [
        {'item_index': 2, 'origin': '0', 'icms_cst': '00', 'icms_base': '0', 'icms_rate': '0', 'icms_value': '0'},
        {'item_index': 1, 'icms_cst': '20', 'cbs_rate': '0'},
    ]}
    review = review_test_tax_fields(snapshot, technical)
    assert review[1][0]['missing'] == ['origem', 'base', 'alíquota', 'valor']
    assert 'alíquota CBS' not in review[1][1]['missing']
    assert review[2][0]['missing'] == []
    assert review[2][1]['all_absent']
    assert all(group['all_absent'] for group in review[3])


def test_tax_review_renders_saved_missing_values_with_conditional_disclaimer():
    from jinja2 import ChoiceLoader, DictLoader
    from app.services.nfe_draft import review_test_tax_fields
    from app.web import routes
    env = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({'base.html': '{% block content %}{% endblock %}'}), routes.templates.env.loader]))
    snapshot = {'venda': 'V-1', 'comprador': 'Teste', 'data_venda': '2026-11-20', 'valor_bruto': '1',
                'itens': [{'lote': 'L1', 'quantidade_sacas': '1', 'valor_bruto': '1'}]}
    technical = {'items': [{'item_index': 1, 'icms_cst': '20'}]}
    page = env.get_template('nfe_draft.html').render(draft=SimpleNamespace(id='x'), issuer=None, details={},
        technical=technical, technical_items={1: technical['items'][0]}, snapshot=snapshot,
        tax_review=review_test_tax_fields(snapshot, technical), csrf_token='x', can_write_page=True)
    assert 'não informados: origem, base, alíquota, valor' in page
    assert 'nenhum campo informado' in page
    assert 'aplicabilidade e obrigatoriedade dependem da operação' in page
