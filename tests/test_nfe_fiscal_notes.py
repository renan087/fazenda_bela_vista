import json
from datetime import datetime, timedelta, timezone
from io import BytesIO

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db.base import Base
from app.models.nfe_draft import (
    NfeClientFiscalConfig,
    NfeDraft,
    NfeDraftDetails,
    NfeDraftTechnicalData,
    NfeFiscalOperation,
    NfeIssuerProfile,
    NfeTaxProfile,
)
from app.services import nfe_fiscal_setup as setup
from app.services import nfe_notes as notes
from tests.test_nfe_xml_builder import _sample_inputs

ORG, FARM, USER = 1, 10, 5


def _db():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    return Session(engine)


def _seed(db, *, env='teste_ficticio'):
    snapshot, issuer, details, technical, _ident, _extra = _sample_inputs(cst='40')
    snapshot = {**snapshot, 'origem_ids': [1, 2], 'payment_method': 'PIX', 'freight_value': '0'}
    db.add(NfeIssuerProfile(id='i' * 32, organization_id=ORG, farm_id=FARM, **vars(issuer)))
    db.add(NfeClientFiscalConfig(id='c' * 32, organization_id=ORG, farm_id=FARM, person_type='pf', document=issuer.cpf,
                                 state_registration='123456', crt='3', state='BA', environment=env, default_series=920,
                                 next_number=1, updated_by=USER))
    draft = NfeDraft(id='d' * 32, organization_id=ORG, farm_id=FARM, sale_code='V-0001', created_by=USER,
                     snapshot_json=json.dumps(snapshot))
    db.add(draft)
    db.add(NfeDraftDetails(draft_id=draft.id, organization_id=ORG, farm_id=FARM, updated_by=USER, data_json=json.dumps(details)))
    db.add(NfeDraftTechnicalData(draft_id=draft.id, organization_id=ORG, farm_id=FARM, updated_by=USER,
                                 data_json=json.dumps(technical)))
    db.commit()
    return draft


def test_operation_activation_requires_user_responsibility_and_versions():
    db = _db()
    op = setup.save_operation(db, organization_id=ORG, farm_id=FARM, user_id=USER,
                              data={'name': 'Venda de produção', 'nature': 'Venda de produção do estabelecimento',
                                    'cfop_in': '5.101', 'cfop_out': '6101', 'funrural': 'retido_comprador'})
    assert op.status == 'rascunho' and op.cfop_in == '5101'
    with pytest.raises(ValueError, match='confirmação'):
        setup.activate_operation(db, organization_id=ORG, farm_id=FARM, user_id=USER, operation_id=op.id, accepted=False)
    with pytest.raises(ValueError, match='CST do ICMS'):
        setup.activate_operation(db, organization_id=ORG, farm_id=FARM, user_id=USER, operation_id=op.id, accepted=True)
    op = setup.save_operation(db, organization_id=ORG, farm_id=FARM, user_id=USER, operation_id=op.id,
                              data={'name': 'Venda de produção', 'nature': 'Venda de produção do estabelecimento',
                                    'cfop_in': '5101', 'cfop_out': '6101', 'icms_cst': '41'})
    setup.activate_operation(db, organization_id=ORG, farm_id=FARM, user_id=USER, operation_id=op.id, accepted=True)
    assert op.status == 'ativa' and op.activated_by == USER and op.activated_at is not None

    # Editar uma ativa cria a versão 2 em rascunho; a v1 continua valendo até ativar a v2.
    v2 = setup.save_operation(db, organization_id=ORG, farm_id=FARM, user_id=USER, operation_id=op.id,
                              data={'name': 'Venda de produção', 'nature': 'Venda de produção do estabelecimento',
                                    'cfop_in': '5101', 'icms_cst': '51'})
    assert v2.id != op.id and v2.version == 2 and v2.status == 'rascunho' and op.icms_cst == '41'
    setup.activate_operation(db, organization_id=ORG, farm_id=FARM, user_id=USER, operation_id=v2.id, accepted=True)
    db.refresh(op)
    assert op.status == 'substituida' and len(setup.active_operations(db, organization_id=ORG, farm_id=FARM)) == 1
    assert setup.choose_cfop(op, buyer_state='SP', issuer_state='BA') == '6101'
    with pytest.raises(ValueError, match='começa com 5'):
        setup.clean_operation_input({'name': 'X', 'cfop_in': '6101'})
    # Isolamento: outra fazenda não enxerga a operação.
    assert setup.active_operations(db, organization_id=ORG, farm_id=FARM + 1) == []
    db.close()


def test_products_link_lot_stages_once_and_suggest_by_stage():
    db = _db()
    a = setup.create_product_from_template(db, organization_id=ORG, farm_id=FARM, user_id=USER, key='arabica')
    c = setup.create_product_from_template(db, organization_id=ORG, farm_id=FARM, user_id=USER, key='coco')
    assert a.is_default and not c.is_default and setup.product_is_complete(a) and not setup.product_is_complete(c)
    assert setup.suggest_product(db, organization_id=ORG, farm_id=FARM, lot_stage='tulha').id == c.id
    assert setup.suggest_product(db, organization_id=ORG, farm_id=FARM, lot_stage='outro').id == a.id
    with pytest.raises(ValueError, match='já está ligado'):
        setup.save_product(db, organization_id=ORG, farm_id=FARM, user_id=USER, product_id=c.id,
                           data={'name': 'Café em coco', 'ncm': '', 'lot_stages': ['beneficiado']})
    with pytest.raises(ValueError, match='8 dígitos'):
        setup.clean_product_input({'name': 'X', 'ncm': '0901'})
    db.close()


def test_apply_setup_converts_taxable_unit_and_fills_info():
    db = _db()
    draft = _seed(db)
    product = setup.create_product_from_template(db, organization_id=ORG, farm_id=FARM, user_id=USER, key='arabica')
    op = setup.save_operation(db, organization_id=ORG, farm_id=FARM, user_id=USER,
                              data={'name': 'Venda', 'nature': 'Venda de produção do estabelecimento', 'cfop_in': '5101',
                                    'icms_cst': '41', 'info': 'ICMS diferido.', 'funrural': 'retido_comprador'})
    setup.activate_operation(db, organization_id=ORG, farm_id=FARM, user_id=USER, operation_id=op.id, accepted=True)
    setup.apply_setup_to_draft(db, draft=draft, product=product, operation=op, buyer=None, issuer_state='BA', user_id=USER)
    tech = json.loads(db.query(NfeDraftTechnicalData).filter_by(draft_id=draft.id).first().data_json)
    item = tech['items'][0]
    assert item['commercial_unit'] == 'SC' and item['tax_unit'] == 'KG'
    assert item['tax_quantity'] == '60.0000' and item['icms_cst'] == '41'
    details = json.loads(db.query(NfeDraftDetails).filter_by(draft_id=draft.id).first().data_json)
    assert details['operation_cfop'] == '5101' and details['product_ncm'] == '09011110'
    sources = setup.draft_sources(db, draft=draft)
    assert sources['operation'].id == op.id and sources['product'].id == product.id
    from app.models.nfe_draft import NfeDraftExtraData
    extra = json.loads(db.query(NfeDraftExtraData).filter_by(draft_id=draft.id).first().data_json)
    assert 'ICMS diferido.' in extra['additional_info'] and 'Funrural' in extra['additional_info']
    db.close()


def test_emission_cancellation_and_status_flow_in_test_environment():
    db = _db()
    draft = _seed(db)
    note = notes.load_note(db, draft, issuer=db.query(NfeIssuerProfile).first())
    assert note['status'] == 'rascunho' and note['can_delete'] and note['title'] == 'Rascunho'

    notes.confirm_emission(db, draft=draft, user_id=USER)
    note = notes.load_note(db, draft, issuer=db.query(NfeIssuerProfile).first())
    assert note['status'] == 'autorizada' and note['number'] == '000001' and len(note['access_key']) == 44
    assert note['can_cancel'] and not note['can_delete']
    with pytest.raises(ValueError, match='já foi enviada'):
        notes.confirm_emission(db, draft=draft, user_id=USER)
    with pytest.raises(ValueError, match='15 a 255'):
        notes.cancel_note(db, draft=draft, user_id=USER, reason='curto')
    late = datetime.now(timezone.utc) + timedelta(hours=25)
    with pytest.raises(ValueError, match='24 horas'):
        notes.cancel_note(db, draft=draft, user_id=USER, reason='Quantidade informada errada', now=late)
    notes.cancel_note(db, draft=draft, user_id=USER, reason='Quantidade informada errada')
    note = notes.load_note(db, draft, issuer=db.query(NfeIssuerProfile).first())
    assert note['status'] == 'cancelada' and note['cancel_reason'] == 'Quantidade informada errada'
    titles = [h['title'] for h in notes.history(db, draft=draft)]
    assert titles[0] == 'Nota cancelada' and 'Enviada e autorizada (teste)' in titles
    summary = notes.summarize([note], month_prefix='2026-11')
    assert summary['cancelled_month'] == 1
    db.close()


def test_emission_is_blocked_outside_test_environment_and_pending_drafts():
    db = _db()
    draft = _seed(db, env='homologacao')
    with pytest.raises(ValueError, match='ambiente de teste'):
        notes.confirm_emission(db, draft=draft, user_id=USER)
    db2 = _db()
    d2 = _seed(db2)
    row = db2.query(NfeDraftDetails).first()
    row.data_json = json.dumps({**json.loads(row.data_json), 'product_ncm': ''})
    db2.commit()
    assert notes.load_note(db2, d2, issuer=db2.query(NfeIssuerProfile).first())['status'] == 'pendente'
    with pytest.raises(ValueError, match='pendências'):
        notes.confirm_emission(db2, draft=d2, user_id=USER)
    notes.delete_draft(db2, draft=d2)
    assert db2.query(NfeDraft).count() == 0
    db.close(); db2.close()


def test_logo_accepts_only_real_images_and_reencodes_png():
    from PIL import Image
    db = _db()
    with pytest.raises(ValueError, match='PNG ou JPG'):
        setup.save_logo(db, organization_id=ORG, farm_id=FARM, user_id=USER, raw=b'GIF89a....')
    with pytest.raises(ValueError, match='Não foi possível ler'):
        setup.save_logo(db, organization_id=ORG, farm_id=FARM, user_id=USER, raw=b'\x89PNG\r\n\x1a\nlixo')
    buf = BytesIO()
    Image.new('RGB', (1200, 400), (68, 106, 54)).save(buf, format='JPEG')
    logo = setup.save_logo(db, organization_id=ORG, farm_id=FARM, user_id=USER, raw=buf.getvalue())
    assert logo.data.startswith(b'\x89PNG') and logo.content_type == 'image/png'
    assert setup.get_logo(db, organization_id=ORG, farm_id=FARM + 1) is None
    db.close()


def test_legacy_profiles_convert_once_into_product_and_draft_operation():
    db = _db()
    db.add(NfeTaxProfile(id='p' * 32, organization_id=ORG, name='Café · BA', status='aprovado_teste', created_by=USER,
                         rules_json=json.dumps({'product_description': 'Café arábica', 'product_ncm': '09011110',
                                                'operation_nature': 'Venda de produção', 'operation_cfop': '5101',
                                                'icms_cst': '41', 'unit': 'SC'})))
    db.commit()
    assert setup.convert_legacy_profiles(db, organization_id=ORG, farm_id=FARM, user_id=USER) == 1
    assert setup.convert_legacy_profiles(db, organization_id=ORG, farm_id=FARM, user_id=USER) == 0
    op = db.query(NfeFiscalOperation).first()
    assert op.status == 'rascunho' and op.cfop_in == '5101' and op.icms_cst == '41'
    db.close()


# ---------------------------------------------------------------- rotas e telas

def _routes_env(monkeypatch):
    from app.web import routes
    from tests.test_stock_assistant import _route_env
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_launch_scope_or_redirect', lambda *args, **kw: (scope, None))
    flashes = []
    monkeypatch.setattr(routes, '_flash', lambda request, kind, msg: flashes.append((kind, msg)))
    farm_id = scope['active_farm_id']
    snapshot, issuer, details, technical, _i, _e = _sample_inputs(cst='40')
    snapshot = {**snapshot, 'origem_ids': [1, 2], 'payment_method': 'PIX', 'freight_value': '0'}
    db.add(NfeIssuerProfile(id='i' * 32, organization_id=user.organization_id, farm_id=farm_id, **vars(issuer)))
    db.add(NfeClientFiscalConfig(id='c' * 32, organization_id=user.organization_id, farm_id=farm_id, person_type='pf',
                                 document=issuer.cpf, crt='3', state='BA', environment='teste_ficticio', default_series=920,
                                 next_number=1, updated_by=user.id))
    draft = NfeDraft(id='r' * 32, organization_id=user.organization_id, farm_id=farm_id, sale_code='V-0001',
                     created_by=user.id, snapshot_json=json.dumps(snapshot))
    db.add(draft)
    db.add(NfeDraftDetails(draft_id=draft.id, organization_id=user.organization_id, farm_id=farm_id, updated_by=user.id,
                           data_json=json.dumps(details)))
    db.add(NfeDraftTechnicalData(draft_id=draft.id, organization_id=user.organization_id, farm_id=farm_id,
                                 updated_by=user.id, data_json=json.dumps(technical)))
    db.commit()
    return routes, db, user, scope, draft, flashes


def _render(name, **ctx):
    from jinja2 import ChoiceLoader, DictLoader
    from app.web import routes
    env = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({'base.html': '{% block content %}{% endblock %}'}), routes.templates.env.loader]))
    return env.get_template(name).render(csrf_token='x', can_write_page=True, **ctx)


def test_routes_emit_cancel_and_isolate_other_organizations(monkeypatch):
    from types import SimpleNamespace
    from fastapi import HTTPException
    routes, db, user, scope, draft, flashes = _routes_env(monkeypatch)
    req = SimpleNamespace(query_params={})

    resp = routes.fiscal_confirm_emission_action(draft.id, req, 'x', '', db, user)
    assert resp.status_code == 303 and 'etapa=conferir' in resp.headers['location'] and flashes[-1][0] == 'error'
    resp = routes.fiscal_confirm_emission_action(draft.id, req, 'x', '1', db, user)
    assert resp.status_code == 303 and resp.headers['location'].endswith('?etapa=enviada')
    assert notes.load_note(db, draft, issuer=db.query(NfeIssuerProfile).first())['status'] == 'autorizada'

    xml = routes.fiscal_note_xml(draft.id, req, db, user)
    assert xml.body.startswith(b'<') and b'nfeProc' in xml.body
    pdf = routes.fiscal_note_danfe(draft.id, req, db, user)
    assert pdf.body.startswith(b'%PDF-')

    resp = routes.fiscal_note_cancel(draft.id, req, 'x', 'curto', db, user)
    assert 'cancelar=1' in resp.headers['location'] and '15 a 255' in flashes[-1][1]
    routes.fiscal_note_cancel(draft.id, req, 'x', 'Quantidade informada errada', db, user)
    assert notes.load_note(db, draft, issuer=db.query(NfeIssuerProfile).first())['status'] == 'cancelada'
    cancel_xml = routes.fiscal_note_xml(draft.id, SimpleNamespace(query_params={'tipo': 'cancelamento'}), db, user)
    assert b'110111' in cancel_xml.body

    other = NfeDraft(id='z' * 32, organization_id=user.organization_id + 99, farm_id=scope['active_farm_id'],
                     sale_code='V-9', created_by=user.id, snapshot_json='{"itens": []}')
    db.add(other)
    db.commit()
    for call in (lambda: routes.fiscal_note_xml(other.id, req, db, user),
                 lambda: routes.fiscal_note_cancel(other.id, req, 'x', 'Motivo bem explicado aqui', db, user),
                 lambda: routes.fiscal_note_delete(other.id, req, 'x', db, user)):
        with pytest.raises(HTTPException) as exc:
            call()
        assert exc.value.status_code == 404
    db.close()


def test_list_detail_emission_and_config_templates_render(monkeypatch):
    routes, db, user, scope, draft, flashes = _routes_env(monkeypatch)
    notes.confirm_emission(db, draft=draft, user_id=user.id)
    issuer = db.query(NfeIssuerProfile).first()
    loaded = notes.load_note(db, draft, issuer=issuer)
    public = routes._nf_public(loaded)
    assert public['actions']['primary']['label'] == 'Baixar DANFE (PDF)'
    assert any(t.get('soon') for t in public['actions']['tiles'])
    summary = notes.summarize([loaded], month_prefix='2026-11')
    html = _render('notas_fiscais.html', notes=[loaded], notes_public=[public], summary=summary, month_name='novembro',
                   farm=SimpleFarm(), season_label='2026/27', env_test=True, buyers=['DESTINATARIO'], setup_ready=False,
                   today_iso='2026-11-22')
    assert 'Notas Fiscais' in html and 'data-nx-kpi="pendente,rejeitada"' in html and 'Falta configurar' in html
    assert 'class="dt-check"' in html and '000001' in html

    detail = _render('nota_fiscal.html', note=loaded, pub=public, issuer=issuer, buyer=None,
                     events=notes.history(db, draft=draft), sources={'operation': None, 'product': None},
                     open_cancel=True, freight_labels={'9': 'Sem transporte'})
    assert 'Cancelar a NF-e 000001?' in detail and 'data-nx-autoopen' in detail and 'Enviada e autorizada (teste)' in detail

    cfg_html = _render('nfe_farm_config.html', issuer=issuer, cfg=None, readiness={'config': None}, certificates=[],
                       master_key_configured=True, can_manage_cert=True, allowed_environments=[], allowed_credential_statuses=[],
                       allowed_crts=[], section='oper', item='novo', product_views=[], operations=[],
                       setup_steps=[{'key': 'emit', 'title': 'Emitente', 'ok': True, 'text': 'ok'}], finalities=setup.FINALITIES,
                       funrural_options=setup.FUNRURAL_OPTIONS, units=setup.UNITS, origins=setup.ORIGINS,
                       operation_missing=setup.operation_missing, operation_templates=setup.OPERATION_TEMPLATES)
    assert 'a responsabilidade pelas informações fiscais é minha' in cfg_html
    assert 'Contador responsável' not in cfg_html and 'data-nx-gated="activate"' in cfg_html
    db.close()


class SimpleFarm:
    name = 'Fazenda Bela Vista'


def test_config_routes_create_activate_and_apply_to_draft(monkeypatch):
    import asyncio
    from types import SimpleNamespace
    routes, db, user, scope, draft, flashes = _routes_env(monkeypatch)

    class Form(dict):
        def getlist(self, key):
            v = self.get(key)
            return v if isinstance(v, list) else ([v] if v else [])

    def req(data):
        class R:
            query_params = {}
            async def form(self):
                return Form(data)
        return R()

    asyncio.run(routes.nfe_config_product_save(req({'csrf_token': 'x', 'template': 'arabica'}), db, user))
    resp = asyncio.run(routes.nfe_config_operation_save(req({'csrf_token': 'x', 'template': 'venda'}), db, user))
    op = db.query(NfeFiscalOperation).first()
    assert 'secao=oper' in resp.headers['location'] and op.status == 'rascunho'
    asyncio.run(routes.nfe_config_operation_save(req({'csrf_token': 'x', 'operation_id': op.id, 'name': op.name,
                                                     'nature': op.nature, 'cfop_in': '5101', 'icms_cst': '41',
                                                     'activate': '1'}), db, user))
    db.refresh(op)
    assert op.status == 'rascunho' and 'confirmação' in flashes[-1][1]
    asyncio.run(routes.nfe_config_operation_save(req({'csrf_token': 'x', 'operation_id': op.id, 'name': op.name,
                                                     'nature': op.nature, 'cfop_in': '5101', 'icms_cst': '41',
                                                     'activate': '1', 'accept': '1'}), db, user))
    db.refresh(op)
    assert op.status == 'ativa' and op.activated_by == user.id
    from app.models.nfe_draft import NfeFiscalProduct
    product = db.query(NfeFiscalProduct).first()
    resp = routes.fiscal_apply_operation_action(draft.id, SimpleNamespace(query_params={}), 'x', op.id, product.id, db, user)
    assert resp.status_code == 303
    assert setup.draft_sources(db, draft=draft)['operation'].id == op.id
    db.close()
