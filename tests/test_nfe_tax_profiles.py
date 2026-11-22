import asyncio
import json
from types import SimpleNamespace
from xml.etree import ElementTree as ET
import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db.base import Base
from app.models.nfe_draft import NfeDraft, NfeDraftExtraData
from app.services.nfe_preview import build_preview
from app.services.nfe_tax_profiles import (
    clean_extra_data,
    create_tax_profile,
    find_matching_tax_profiles,
    transition_tax_profile_status,
)


def _new_db():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    return Session(engine)


def test_tax_profile_versioning_governance_and_vigency_lookup():
    db = _new_db()
    p1 = create_tax_profile(
        db, organization_id=1, user_id=2,
        data={
            'name': 'Café BA PF -> PF',
            'operation_code': 'venda_cafe_interna',
            'origin_state': 'BA',
            'destination_state': 'BA',
            'issuer_person_type': 'pf',
            'recipient_person_type': 'pf',
            'valid_from': '2026-01-01',
            'valid_until': '2026-12-31',
        },
    )
    assert p1.version == 1 and p1.status == 'rascunho'

    # Não permite aprovar para teste sem responsável e embasamento.
    with pytest.raises(ValueError):
        transition_tax_profile_status(db, p1, new_status='aprovado_teste')
    # Não permite status de produção nesta etapa.
    with pytest.raises(ValueError):
        transition_tax_profile_status(db, p1, new_status='aprovado_producao')

    transition_tax_profile_status(
        db, p1, new_status='aprovado_teste',
        approved_by_name='Contador Teste CRC-BA',
        reference_notes='Parecer preliminar para testes internos.',
    )
    assert p1.status == 'aprovado_teste'

    # Nova versão para a mesma chave incrementa version para 2.
    p2 = create_tax_profile(
        db, organization_id=1, user_id=2,
        data={
            'name': 'Café BA PF -> PF v2',
            'valid_from': '2027-01-01',
        },
    )
    assert p2.version == 2

    # Busca por vigência filtra corretamente e respeita isolamento por organização.
    matched_2026 = find_matching_tax_profiles(
        db, organization_id=1, origin_state='BA', destination_state='BA', reference_date='2026-11-21',
    )
    assert [p.id for p in matched_2026] == [p1.id]
    assert find_matching_tax_profiles(db, organization_id=999, origin_state='BA', destination_state='BA') == []

    # Perfil arquivado não pode voltar a ser editado.
    transition_tax_profile_status(db, p1, new_status='arquivado')
    with pytest.raises(ValueError):
        transition_tax_profile_status(db, p1, new_status='rascunho')
    db.close()


def test_clean_extra_data_validates_transport_payment_and_infcpl():
    cleaned = clean_extra_data({
        'carrier_name': 'Transportadora Fictícia',
        'vehicle_plate': 'abc-1d23',
        'vehicle_uf': 'ba',
        'volume_quantity': '10',
        'volume_species': 'SACAS',
        'net_weight_kg': '600',
        'gross_weight_kg': '605,5',
        'payment_indicator': '0',
        'payment_amount': '1000,5',
        'additional_info': 'Observação fictícia de teste.',
    })
    assert cleaned['vehicle_plate'] == 'ABC1D23'
    assert cleaned['vehicle_uf'] == 'BA'
    assert cleaned['net_weight_kg'] == '600.000'
    assert cleaned['gross_weight_kg'] == '605.500'
    assert cleaned['payment_amount'] == '1000.50'

    for invalid in (
        {'vehicle_plate': '1234567'},
        {'vehicle_uf': 'XX'},
        {'volume_quantity': '0'},
        {'net_weight_kg': '610', 'gross_weight_kg': '600'},
        {'payment_indicator': '9'},
        {'payment_amount': '-10'},
        {'additional_info': 'texto\x00invalido'},
    ):
        with pytest.raises(ValueError):
            clean_extra_data(invalid)

    # Preview XML inclui complementos_teste quando presentes.
    snapshot = {'venda': 'V-1', 'data_venda': '2026-11-21', 'comprador': 'Teste', 'valor_bruto': '100.00',
                'itens': [{'lote': 'L1', 'quantidade_sacas': '1', 'valor_bruto': '100.00'}]}
    root = ET.fromstring(build_preview(snapshot, None, {}, {}, extra_data=cleaned))
    assert root.findtext('complementos_teste/vehicle_plate') == 'ABC1D23'
    assert root.findtext('complementos_teste/additional_info') == 'Observação fictícia de teste.'


def test_extra_data_route_rejects_other_organization(monkeypatch):
    from app.web import routes
    from tests.test_stock_assistant import _route_env
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    draft = NfeDraft(
        id='x' * 32, organization_id=user.organization_id + 999,
        farm_id=scope['active_farm_id'], sale_code='V-1', created_by=user.id,
        snapshot_json=json.dumps({'itens': []}),
    )
    db.add(draft)
    db.commit()

    class Request:
        async def form(self):
            return {'csrf_token': 'x', 'additional_info': 'Teste'}

    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_launch_scope_or_redirect', lambda *args: (scope, None))
    with pytest.raises(HTTPException) as exc:
        asyncio.run(routes.fiscal_extra_data_save(draft.id, Request(), db, user))
    assert exc.value.status_code == 404
    assert db.query(NfeDraftExtraData).count() == 0
    db.close()
