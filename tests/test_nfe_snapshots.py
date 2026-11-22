import asyncio
import json
from types import SimpleNamespace
import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db.base import Base
from app.models.nfe_draft import NfeDraft, NfeDraftEvent, NfeDraftSnapshot, NfeIssuerProfile
from app.services.nfe_numbering import reserve_or_update_test_identification
from app.services.nfe_snapshots import (
    SNAPSHOT_STATUS_TEST,
    export_snapshot_json_bytes,
    freeze_draft_snapshot,
    list_draft_events,
    list_draft_snapshots,
    record_draft_event,
    verify_snapshot_integrity,
)


def _new_db():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    return Session(engine)


def _sample_draft(db, *, draft_id='s' * 32, organization_id=1, farm_id=10, user_id=5):
    sale_snapshot = {
        'versao': 1,
        'venda': 'V-0001',
        'origem_ids': [1],
        'comprador': 'Comprador Ficticio',
        'comprador_id': 9,
        'data_venda': '2026-11-21',
        'itens': [{'lote': 'L1', 'quantidade_sacas': '1', 'valor_bruto': '100.00'}],
        'valor_bruto': '100.00',
    }
    draft = NfeDraft(
        id=draft_id,
        organization_id=organization_id,
        farm_id=farm_id,
        sale_code='V-0001',
        snapshot_json=json.dumps(sale_snapshot, ensure_ascii=False),
        created_by=user_id,
    )
    db.add(draft)
    db.commit()
    return draft


def test_freeze_snapshot_is_immutable_versioned_and_verifies_sha256():
    db = _new_db()
    draft = _sample_draft(db)
    issuer = NfeIssuerProfile(
        id='i' * 32, organization_id=1, farm_id=10,
        name='Produtor Original', cpf='52998224725', state='BA',
    )
    db.add(issuer)
    db.commit()

    ident = reserve_or_update_test_identification(
        db, organization_id=1, farm_id=10, draft_id=draft.id, user_id=5,
        data={'series': '920', 'consumer_indicator': '1', 'presence_indicator': '1', 'payment_method': '17'},
    )
    v1 = freeze_draft_snapshot(
        db, draft=draft, issuer=issuer,
        details={'recipient_name': 'Comprador A', 'recipient_state': 'BA'},
        details_saved=True,
        technical={'emission_date': '2026-11-21', 'freight_mode': '9', 'items': []},
        identification=ident,
        user_id=5,
        note='Primeira conferência',
    )

    assert v1.version == 1
    assert v1.status == SNAPSHOT_STATUS_TEST
    assert len(v1.payload_sha256) == 64
    assert verify_snapshot_integrity(v1) is True

    # Alterar o perfil do produtor depois não modifica o snapshot v1.
    issuer.name = 'Produtor Alterado Depois'
    db.commit()
    v2 = freeze_draft_snapshot(
        db, draft=draft, issuer=issuer,
        details={'recipient_name': 'Comprador A', 'recipient_state': 'BA'},
        details_saved=True,
        technical={'emission_date': '2026-11-21', 'freight_mode': '9', 'items': []},
        identification=ident,
        user_id=5,
        note='Segunda conferência',
    )
    assert v2.version == 2
    assert v2.payload_sha256 != v1.payload_sha256
    assert json.loads(v1.payload_json)['produtor']['name'] == 'Produtor Original'
    assert json.loads(v2.payload_json)['produtor']['name'] == 'Produtor Alterado Depois'

    listed = list_draft_snapshots(db, draft_id=draft.id, organization_id=1, farm_id=10)
    assert [item['version'] for item in listed] == [2, 1]
    assert all(item['integrity_ok'] for item in listed)
    assert len(listed[0]['simulated_key']) == 44

    exported = json.loads(export_snapshot_json_bytes(v1).decode('utf-8'))
    assert exported['versao'] == 1
    assert exported['sha256_conteudo_canonico'] == v1.payload_sha256
    assert exported['conteudo']['produtor']['name'] == 'Produtor Original'

    events = list_draft_events(db, draft_id=draft.id, organization_id=1, farm_id=10)
    assert len(events) == 2
    assert all(ev.event_type == 'snapshot_congelado' for ev in events)
    db.close()


def test_unsaved_buyer_suggestions_are_excluded_from_frozen_snapshot():
    db = _new_db()
    draft = _sample_draft(db)
    v1 = freeze_draft_snapshot(
        db, draft=draft, issuer=None,
        details={'recipient_name': 'Sugestao Nao Salva'},
        details_saved=False,
        technical={},
        identification=None,
        user_id=5,
    )
    payload = json.loads(v1.payload_json)
    assert payload['destinatario_e_operacao'] == {}
    assert payload['identificacao_teste'] is None
    db.close()


def test_tampered_snapshot_is_detected_and_blocked_from_export():
    db = _new_db()
    draft = _sample_draft(db)
    v1 = freeze_draft_snapshot(
        db, draft=draft, issuer=None, details={}, details_saved=False,
        technical={}, identification=None, user_id=5,
    )
    # Simula adulteração direta no banco.
    v1.payload_json = v1.payload_json.replace('Comprador Ficticio', 'Adulterado')
    db.commit()

    assert verify_snapshot_integrity(v1) is False
    listed = list_draft_snapshots(db, draft_id=draft.id, organization_id=1, farm_id=10)
    assert listed[0]['integrity_ok'] is False
    with pytest.raises(ValueError):
        export_snapshot_json_bytes(v1)
    db.close()


def test_record_draft_event_validates_type_and_note_limit():
    db = _new_db()
    draft = _sample_draft(db)
    with pytest.raises(ValueError):
        record_draft_event(
            db, draft_id=draft.id, organization_id=1, farm_id=10,
            event_type='evento_invalido', summary='Teste', user_id=5,
        )
    with pytest.raises(ValueError):
        freeze_draft_snapshot(
            db, draft=draft, issuer=None, details={}, details_saved=False,
            technical={}, identification=None, user_id=5, note='x' * 241,
        )
    db.close()


def test_snapshot_routes_reject_other_organization(monkeypatch):
    from app.web import routes
    from tests.test_stock_assistant import _route_env
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    other_draft = _sample_draft(
        db, draft_id='z' * 32, organization_id=user.organization_id + 999,
        farm_id=scope['active_farm_id'], user_id=user.id,
    )

    class Request:
        async def form(self):
            return {'csrf_token': 'x', 'note': 'Teste'}

    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_launch_scope_or_redirect', lambda *args: (scope, None))

    with pytest.raises(HTTPException) as exc_freeze:
        asyncio.run(routes.fiscal_snapshot_freeze(other_draft.id, Request(), db, user))
    assert exc_freeze.value.status_code == 404
    assert db.query(NfeDraftSnapshot).count() == 0

    with pytest.raises(HTTPException) as exc_dl:
        routes.fiscal_snapshot_download(other_draft.id, 1, SimpleNamespace(), db, user)
    assert exc_dl.value.status_code == 404
    db.close()


def test_draft_template_renders_snapshots_and_local_history():
    from jinja2 import ChoiceLoader, DictLoader
    from app.web import routes
    env = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({'base.html': '{% block content %}{% endblock %}'}),
        routes.templates.env.loader,
    ]))
    snapshot = {'venda': 'V-0001', 'comprador': 'Teste', 'data_venda': '2026-11-21', 'itens': [], 'valor_bruto': '0'}
    html = env.get_template('nfe_draft.html').render(
        draft=SimpleNamespace(id='id'), issuer=None, snapshot=snapshot, details={},
        technical={}, technical_items={},
        snapshots=[{
            'version': 1, 'integrity_ok': True, 'short_hash': 'abcdef123456',
            'pending_count': 2, 'note': 'Conferência inicial', 'simulated_key': '',
        }],
        draft_events=[SimpleNamespace(summary='Versão de teste v1 congelada (SHA-256 abcdef123456…).', created_at=None)],
        csrf_token='x', can_write_page=True,
    )
    assert 'Versões congeladas e histórico local (teste)' in html
    assert 'abcdef123456' in html
    assert 'Conferência inicial' in html
    assert 'Versão de teste v1 congelada' in html
