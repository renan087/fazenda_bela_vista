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
                                                    snapshot=snapshot, details={}, csrf_token='x', can_write_page=True)
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
