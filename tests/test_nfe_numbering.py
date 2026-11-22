import asyncio
from types import SimpleNamespace
from xml.etree import ElementTree as ET
import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db.base import Base
from app.models.nfe_draft import NfeDraft, NfeDraftIdentification, NfeTestSequence
from app.services.nfe_access_key import validate_access_key
from app.services.nfe_numbering import (
    TEST_ENVIRONMENT,
    build_simulated_access_key_summary,
    clean_identification_input,
    reserve_or_update_test_identification,
)
from app.services.nfe_preview import build_preview


def _new_db():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    return Session(engine)


def test_clean_identification_input_validates_series_and_indicators():
    cleaned = clean_identification_input({
        'series': '920',
        'consumer_indicator': '1',
        'presence_indicator': '1',
        'payment_method': '17',
    })
    assert cleaned == {
        'series': 920,
        'consumer_indicator': '1',
        'presence_indicator': '1',
        'payment_method': '17',
    }
    assert clean_identification_input({})['series'] == 920

    for invalid in (
        {'series': '1'},
        {'series': '970'},
        {'series': 'abc'},
        {'consumer_indicator': '9'},
        {'presence_indicator': '8'},
        {'payment_method': '00'},
    ):
        with pytest.raises(ValueError):
            clean_identification_input(invalid)


def test_test_numbering_is_sequential_idempotent_and_isolated_by_farm_and_org():
    db = _new_db()
    first = reserve_or_update_test_identification(
        db, organization_id=1, farm_id=10, draft_id='d' * 31 + '1', user_id=5,
        data={'series': '920', 'consumer_indicator': '1', 'presence_indicator': '1', 'payment_method': '17'},
    )
    second = reserve_or_update_test_identification(
        db, organization_id=1, farm_id=10, draft_id='d' * 31 + '2', user_id=5,
        data={'series': '920'},
    )
    other_farm = reserve_or_update_test_identification(
        db, organization_id=1, farm_id=11, draft_id='d' * 31 + '3', user_id=5,
        data={'series': '920'},
    )
    other_org = reserve_or_update_test_identification(
        db, organization_id=2, farm_id=10, draft_id='d' * 31 + '4', user_id=6,
        data={'series': '920'},
    )

    assert first.environment == TEST_ENVIRONMENT
    assert first.series == 920 and first.number == 1
    assert second.series == 920 and second.number == 2
    assert other_farm.series == 920 and other_farm.number == 1
    assert other_org.series == 920 and other_org.number == 1

    # Re-salvar o mesmo rascunho na mesma série mantém número e cNF intactos.
    first_cnf = first.numeric_code
    updated = reserve_or_update_test_identification(
        db, organization_id=1, farm_id=10, draft_id='d' * 31 + '1', user_id=5,
        data={'series': '920', 'consumer_indicator': '0', 'presence_indicator': '9', 'payment_method': '01'},
    )
    assert updated.number == 1 and updated.numeric_code == first_cnf
    assert updated.consumer_indicator == '0' and updated.payment_method == '01'

    # Trocar de série reserva o primeiro número da nova série sem alterar o contador da 920.
    moved = reserve_or_update_test_identification(
        db, organization_id=1, farm_id=10, draft_id='d' * 31 + '1', user_id=5,
        data={'series': '921', 'payment_method': '17'},
    )
    assert moved.series == 921 and moved.number == 1
    seq_920 = db.query(NfeTestSequence).filter_by(organization_id=1, farm_id=10, series=920).first()
    seq_921 = db.query(NfeTestSequence).filter_by(organization_id=1, farm_id=10, series=921).first()
    assert seq_920.last_number == 2 and seq_921.last_number == 1
    db.close()


def test_simulated_access_key_summary_and_preview_xml():
    db = _new_db()
    ident = reserve_or_update_test_identification(
        db, organization_id=1, farm_id=10, draft_id='a' * 32, user_id=5,
        data={'series': '920', 'consumer_indicator': '1', 'presence_indicator': '1', 'payment_method': '17'},
    )
    # Sem CPF e data de emissão, lista pendências sem gerar chave.
    incomplete = build_simulated_access_key_summary(None, {}, ident)
    assert incomplete['reserved'] is True and incomplete['key'] == ''
    assert len(incomplete['missing']) == 3

    # Com produtor e data de emissão válidos, gera chave simulada válida (44 dígitos).
    issuer = SimpleNamespace(name='Produtor Ficticio', cpf='52998224725', state='BA')
    technical = {'emission_date': '2026-11-21', 'freight_mode': '9', 'items': []}
    complete = build_simulated_access_key_summary(issuer, technical, ident)
    assert complete['reserved'] is True and complete['missing'] == []
    assert len(complete['key']) == 44 and validate_access_key(complete['key']) is True

    # A prévia interna inclui identificacao_teste sem se tornar NF-e.
    snapshot = {'venda': 'V-1', 'data_venda': '2026-11-21', 'comprador': 'Comprador', 'valor_bruto': '100.00',
                'itens': [{'lote': 'L1', 'quantidade_sacas': '1', 'valor_bruto': '100.00'}]}
    xml_bytes = build_preview(snapshot, issuer, {}, technical, identification=ident)
    root = ET.fromstring(xml_bytes)
    assert root.tag == 'preparacao_sisfarm' and root.find('NFe') is None
    assert root.findtext('identificacao_teste/ambiente') == TEST_ENVIRONMENT
    assert root.findtext('identificacao_teste/serie') == '920'
    assert root.findtext('identificacao_teste/numero') == '1'
    assert root.findtext('identificacao_teste/chave_simulada') == complete['key']
    db.close()


def test_identification_route_rejects_other_organization(monkeypatch):
    from app.web import routes
    from tests.test_stock_assistant import _route_env
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    draft = NfeDraft(
        id='c' * 32,
        organization_id=user.organization_id + 999,
        farm_id=scope['active_farm_id'],
        sale_code='V-0001',
        created_by=user.id,
        snapshot_json='{}',
    )
    db.add(draft)
    db.commit()

    class Request:
        async def form(self):
            return {'csrf_token': 'x', 'series': '920'}

    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_launch_scope_or_redirect', lambda *args: (scope, None))
    with pytest.raises(HTTPException) as exc:
        asyncio.run(routes.fiscal_identification_save(draft.id, Request(), db, user))
    assert exc.value.status_code == 404
    assert db.query(NfeDraftIdentification).count() == 0
    db.close()


def test_draft_template_renders_identification_and_simulated_key():
    from jinja2 import ChoiceLoader, DictLoader
    from app.web import routes
    env = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({'base.html': '{% block content %}{% endblock %}'}),
        routes.templates.env.loader,
    ]))
    issuer = SimpleNamespace(
        name='Produtor Ficticio', cpf='52998224725', state_registration='', postal_code='',
        street='', number='', complement='', district='', city='', municipality_code='', state='BA',
    )
    snapshot = {'venda': 'V-0001', 'comprador': 'Teste', 'data_venda': '2026-11-21', 'itens': [], 'valor_bruto': '0'}
    ident = SimpleNamespace(
        model='55', series=920, number=1, numeric_code='24681357',
        consumer_indicator='1', presence_indicator='1', payment_method='17',
    )
    key_summary = {'reserved': True, 'key': '29261100052998224725559200000000011246813570', 'missing': []}
    html = env.get_template('nfe_draft.html').render(
        draft=SimpleNamespace(id='id'), issuer=issuer, snapshot=snapshot, details={},
        technical={}, technical_items={}, identification=ident, key_summary=key_summary,
        csrf_token='x', can_write_page=True,
    )
    assert 'Identificação e numeração fictícia (teste)' in html
    assert 'Isolado de produção' in html
    assert '29261100052998224725559200000000011246813570' in html
