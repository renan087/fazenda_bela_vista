import asyncio
from types import SimpleNamespace
import pytest
from fastapi import HTTPException

from app.models.nfe_draft import NfeCertificateVault, NfeClientFiscalConfig, NfeIssuerProfile
from app.services.nfe_client_config import (
    ALLOWED_CREDENTIAL_STATUSES,
    ALLOWED_CRTS,
    ALLOWED_ENVIRONMENTS,
    evaluate_pilot_homologation_readiness,
)
from tests.test_nfe_certificate_and_signature import TEST_MASTER_KEY, _generate_ephemeral_pfx


def test_farm_config_save_syncs_issuer_and_resolves_ibge(monkeypatch):
    from app.web import routes
    from tests.test_stock_assistant import _route_env
    _, db, user, scope, *_ = _route_env(monkeypatch, [])

    class Request:
        async def form(self):
            return {
                'csrf_token': 'x',
                'person_type': 'pf',
                'name': 'Produtor Rural Piloto',
                'document': '529.982.247-25',
                'state_registration': '123456',
                'crt': '3',
                'state': 'BA',
                'postal_code': '45000-000',
                'street': 'Fazenda Bela Vista',
                'number': 'S/N',
                'district': 'Zona Rural',
                'city': 'Vitória da Conquista',
                'municipality_code': '',
                'default_series': '920',
                'next_number': '1',
                'environment': 'homologacao',
                'sefaz_credential_status': 'em_analise',
                'pilot_homologacao_enabled': '1',
            }

    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_launch_scope_or_redirect', lambda *args: (scope, None))
    monkeypatch.setattr(routes, '_flash', lambda *args: None)

    resp = asyncio.run(routes.nfe_farm_config_save(Request(), db, user))
    assert resp.status_code == 303

    cfg = db.query(NfeClientFiscalConfig).filter_by(organization_id=user.organization_id, farm_id=scope['active_farm_id']).first()
    issuer = db.query(NfeIssuerProfile).filter_by(organization_id=user.organization_id, farm_id=scope['active_farm_id']).first()
    assert cfg is not None and cfg.document == '52998224725' and cfg.crt == '3'
    assert issuer is not None and issuer.name == 'Produtor Rural Piloto'
    assert issuer.municipality_code == '2933307'
    db.close()


def test_certificate_upload_enforces_permission_and_encrypts_in_vault(monkeypatch):
    from app.web import routes
    from tests.test_stock_assistant import _route_env
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    monkeypatch.setenv('NFE_CERT_MASTER_KEY', TEST_MASTER_KEY)
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_launch_scope_or_redirect', lambda *args: (scope, None))
    monkeypatch.setattr(routes, '_flash', lambda *args: None)

    pfx_bytes = _generate_ephemeral_pfx(password='senha-piloto-123')

    class FakeUpload:
        async def read(self):
            return pfx_bytes

    class Request:
        async def form(self):
            return {
                'csrf_token': 'x',
                'environment': 'homologacao',
                'certificate_password': 'senha-piloto-123',
                'certificate_file': FakeUpload(),
            }

    # Operador comum sem nfe.certificate.manage recebe 403.
    operator = SimpleNamespace(
        id=user.id, organization_id=user.organization_id, is_superuser=False, role='operator',
    )
    with pytest.raises(HTTPException) as exc:
        asyncio.run(routes.nfe_farm_certificate_upload(Request(), db, operator))
    assert exc.value.status_code == 403
    assert db.query(NfeCertificateVault).count() == 0

    # Proprietário consegue carregar, criptografar e depois desativar.
    owner = SimpleNamespace(
        id=user.id, organization_id=user.organization_id, is_superuser=False, role='owner',
    )
    resp = asyncio.run(routes.nfe_farm_certificate_upload(Request(), db, owner))
    assert resp.status_code == 303
    cert = db.query(NfeCertificateVault).filter_by(organization_id=user.organization_id, farm_id=scope['active_farm_id']).first()
    assert cert is not None and cert.is_active is True

    # Template renderiza sem jamais expor ciphertext ou senha.
    from jinja2 import ChoiceLoader, DictLoader
    env = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({'base.html': '{% block content %}{% endblock %}'}),
        routes.templates.env.loader,
    ]))
    readiness = evaluate_pilot_homologation_readiness(db, organization_id=user.organization_id, farm_id=scope['active_farm_id'])
    html = env.get_template('nfe_farm_config.html').render(
        issuer=None, cfg=readiness['config'], readiness=readiness, certificates=[cert],
        master_key_configured=True, can_manage_cert=True, can_write_page=True, csrf_token='x',
        allowed_environments=ALLOWED_ENVIRONMENTS,
        allowed_credential_statuses=ALLOWED_CREDENTIAL_STATUSES,
        allowed_crts=ALLOWED_CRTS,
    )
    assert 'Configurações fiscais' in html and 'Certificado digital A1' in html
    assert cert.subject_cn in html
    assert cert.ciphertext_b64 not in html
    assert 'senha-piloto-123' not in html

    class DeactivateReq:
        async def form(self):
            return {'csrf_token': 'x'}

    resp2 = asyncio.run(routes.nfe_farm_certificate_deactivate(cert.id, DeactivateReq(), db, owner))
    assert resp2.status_code == 303
    db.refresh(cert)
    assert cert.is_active is False
    db.close()


def test_tax_profile_create_approve_and_apply_to_draft(monkeypatch):
    import json
    from app.models.nfe_draft import NfeDraft, NfeDraftDetails, NfeDraftTechnicalData, NfeTaxProfile
    from app.web import routes
    from tests.test_stock_assistant import _route_env
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    monkeypatch.setattr(routes, 'validate_csrf', lambda *args: None)
    monkeypatch.setattr(routes, '_launch_scope_or_redirect', lambda *args: (scope, None))
    monkeypatch.setattr(routes, '_flash', lambda *args: None)

    class CreateProfileReq:
        async def form(self):
            return {
                'csrf_token': 'x',
                'name': 'Café Beneficiado · BA',
                'product_description': 'Café arábica beneficiado em grão',
                'product_ncm': '09011110',
                'operation_nature': 'Venda de produção do estabelecimento',
                'operation_cfop': '5101',
                'origin': '0',
                'icms_cst': '40',
                'unit': 'SC',
                'approved_by_name': 'Dr. Carlos Mendes CRC-BA',
                'reference_notes': 'Parecer interno BA',
            }

    resp = asyncio.run(routes.nfe_tax_profile_create(CreateProfileReq(), db, user))
    assert resp.status_code == 303
    profile = db.query(NfeTaxProfile).filter_by(organization_id=user.organization_id).first()
    assert profile is not None and profile.status == 'rascunho'

    class ApproveReq:
        async def form(self):
            return {'csrf_token': 'x', 'status': 'aprovado_teste'}

    resp2 = asyncio.run(routes.nfe_tax_profile_status_action(profile.id, ApproveReq(), db, user))
    assert resp2.status_code == 303
    db.refresh(profile)
    assert profile.status == 'aprovado_teste'

    # Cria um rascunho e aplica o perfil em 1 clique.
    draft = NfeDraft(
        id='k' * 32, organization_id=user.organization_id, farm_id=scope['active_farm_id'],
        sale_code='V-0018', created_by=user.id,
        snapshot_json=json.dumps({
            'versao': 1, 'venda': 'V-0018', 'origem_ids': [1],
            'comprador': 'João Batista', 'comprador_id': None, 'data_venda': '2026-11-22',
            'payment_method': 'PIX', 'payment_condition': 'a_vista', 'freight_value': '0',
            'itens': [{'lote': 'Lote 04', 'quantidade_sacas': '10', 'valor_bruto': '14200.00'}],
            'valor_bruto': '14200.00',
        }),
    )
    db.add(draft)
    db.commit()

    class ApplyReq:
        async def form(self):
            return {'csrf_token': 'x', 'profile_id': profile.id}

    resp3 = asyncio.run(routes.fiscal_apply_profile_action(draft.id, ApplyReq(), db, user))
    assert resp3.status_code == 303

    det = db.query(NfeDraftDetails).filter_by(draft_id=draft.id).first()
    tech = db.query(NfeDraftTechnicalData).filter_by(draft_id=draft.id).first()
    assert det is not None and json.loads(det.data_json)['product_ncm'] == '09011110'
    assert tech is not None and json.loads(tech.data_json)['items'][0]['icms_cst'] == '40'
    assert json.loads(tech.data_json)['items'][0]['commercial_unit'] == 'SC'
    db.close()
