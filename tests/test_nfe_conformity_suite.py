"""Suíte de conformidade offline SaaS NF-e.

Cobre: autorização, rejeição, consulta, cancelamento, duplicidade, timeout,
idempotência, isolamento multi-cliente, modo piloto e painel técnico.
Usa exclusivamente certificados efêmeros de teste, cópia temporária do XSD e
respostas simuladas da SEFAZ — sem transmissão real.
"""
import json
import shutil
from types import SimpleNamespace
import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db.base import Base
from app.models.nfe_draft import NfeStoredDocument, NfeTransmissionAttempt
from app.services.nfe_certificate_vault import (
    Pkcs12CertificateProvider,
    decrypt_certificate_from_vault,
    store_encrypted_certificate,
    verify_xml_signature,
)
from app.services.nfe_client_config import (
    build_technical_panel_context,
    evaluate_pilot_homologation_readiness,
    save_client_fiscal_config,
)
from app.services.nfe_cycle_simulator import run_offline_cycle_simulation
from app.services.nfe_events import build_cancellation_event, parse_event_response
from app.services.nfe_numbering import reserve_or_update_test_identification
from app.services.nfe_schema import SCHEMA_DIRECTORY, validate_xml
from app.services.nfe_sefaz_client import send_soap_request
from app.services.nfe_sefaz_protocol import (
    build_soap_consult_protocol,
    build_soap_status_service,
    record_simulated_attempt,
)
from app.services.nfe_sefaz_registry import SefazServiceConfig
from tests.test_nfe_certificate_and_signature import TEST_MASTER_KEY, _generate_ephemeral_pfx
from tests.test_nfe_xml_builder import _sample_inputs


def _new_db():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    return Session(engine)


def _temp_reviewed_xsd(tmp_path):
    temp_schema = tmp_path / 'schemas'
    shutil.copytree(SCHEMA_DIRECTORY, temp_schema)
    manifest_path = temp_schema / 'manifest.json'
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    manifest['reviewed'] = True
    manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
    return temp_schema


def test_conformity_authorization_signed_xsd_and_rejection(tmp_path):
    db = _new_db()
    temp_schema = _temp_reviewed_xsd(tmp_path)
    pfx = _generate_ephemeral_pfx(password='teste-cli-a')
    signer = Pkcs12CertificateProvider(pfx, 'teste-cli-a')
    snapshot, issuer, details, technical, identification, extra_data = _sample_inputs(cst='40')

    # 1. Autorização com assinatura XMLDSig real + verificação XSD + nfeProc travado.
    res_auth = run_offline_cycle_simulation(
        db, draft_id='a' * 32, organization_id=1, farm_id=10, user_id=5,
        snapshot=snapshot, issuer=issuer, details=details, technical=technical,
        identification=identification, issuer_crt='3', extra_data=extra_data,
        signer=signer, simulated_c_stat='100',
    )
    assert res_auth['ok'] is True and res_auth['signed'] is True
    assert res_auth['stored_document'] is not None
    # Extrai o <NFe> de dentro do nfeProc e valida assinatura + XSD 4.00.
    proc_xml = res_auth['stored_document'].xml_content.encode('utf-8')
    assert verify_xml_signature(proc_xml) is True
    # Valida a assinatura no XML gerado diretamente também.
    from lxml import etree
    nfe_el = etree.fromstring(proc_xml).xpath('.//*[local-name()="NFe"]')[0]
    nfe_bytes = etree.tostring(nfe_el, encoding='utf-8', xml_declaration=True)
    assert validate_xml(nfe_bytes, schema_directory=temp_schema)['valid'] is True

    # 2. Rejeição (cStat 225) registra tentativa sem salvar nfeProc.
    res_rej = run_offline_cycle_simulation(
        db, draft_id='a' * 32, organization_id=1, farm_id=10, user_id=5,
        snapshot=snapshot, issuer=issuer, details=details, technical=technical,
        identification=identification, issuer_crt='3', extra_data=extra_data,
        signer=signer, simulated_c_stat='225', simulated_x_motivo='Rejeicao: Falha no Schema XML',
    )
    assert res_rej['ok'] is False and res_rej['attempt'].status_category == 'rejeitada'
    assert res_rej['stored_document'] is None
    db.close()


def test_conformity_duplicity_idempotency_consultation_cancellation_and_timeout(monkeypatch):
    db = _new_db()
    monkeypatch.setenv('NFE_ENABLE_SEFAZ_NETWORK', 'true')
    pfx = _generate_ephemeral_pfx(password='teste-cli-a')
    signer = Pkcs12CertificateProvider(pfx, 'teste-cli-a')
    snapshot, issuer, details, technical, identification, extra_data = _sample_inputs(cst='40')

    # Autorização inicial.
    auth = run_offline_cycle_simulation(
        db, draft_id='b' * 32, organization_id=1, farm_id=10, user_id=5,
        snapshot=snapshot, issuer=issuer, details=details, technical=technical,
        identification=identification, issuer_crt='3', extra_data=extra_data,
        signer=signer, simulated_c_stat='100', idempotency_key='idem-auth-001',
    )
    key = auth['access_key']

    # Idempotência: mesma chave retorna a tentativa existente sem criar duplicata.
    auth_repeat = run_offline_cycle_simulation(
        db, draft_id='b' * 32, organization_id=1, farm_id=10, user_id=5,
        snapshot=snapshot, issuer=issuer, details=details, technical=technical,
        identification=identification, issuer_crt='3', extra_data=extra_data,
        signer=signer, simulated_c_stat='100', idempotency_key='idem-auth-001',
    )
    assert auth_repeat['attempt'].id == auth['attempt'].id

    # Duplicidade (cStat 204): classificada como rejeitada sem apagar o nfeProc já autorizado.
    dup = run_offline_cycle_simulation(
        db, draft_id='b' * 32, organization_id=1, farm_id=10, user_id=5,
        snapshot=snapshot, issuer=issuer, details=details, technical=technical,
        identification=identification, issuer_crt='3', extra_data=extra_data,
        signer=signer, simulated_c_stat='204', simulated_x_motivo='Rejeicao: Duplicidade de NF-e',
        idempotency_key='idem-dup-001',
    )
    assert dup['ok'] is False and dup['attempt'].c_stat == '204' and dup['attempt'].status_category == 'rejeitada'
    assert db.query(NfeStoredDocument).filter_by(access_key=key).count() == 1

    # Consulta de situação (consSitNFe -> retConsSitNFe cStat=100) via MockTransport.
    soap_consult = build_soap_consult_protocol(access_key=key, tp_amb='2')
    unlocked_cfg = SefazServiceConfig(
        uf='BA', uf_code='29', environment='homologacao', service='NfeConsultaProtocolo4',
        authorizer='SEFAZ-BA', verified=True, transmission_enabled=True,
    )
    consult_resp_xml = (
        f'<retConsSitNFe xmlns="http://www.portalfiscal.inf.br/nfe" versao="4.00">'
        f'<cStat>100</cStat><xMotivo>Autorizado o uso da NF-e</xMotivo>'
        f'<chNFe>{key}</chNFe><protNFe versao="4.00"><infProt>'
        f'<chNFe>{key}</chNFe><nProt>129260000000001</nProt><cStat>100</cStat>'
        f'<xMotivo>Autorizado o uso da NF-e</xMotivo></infProt></protNFe></retConsSitNFe>'
    ).encode('utf-8')
    consult_res = send_soap_request(
        config=unlocked_cfg,
        endpoint_url='https://hnfe.sefaz.ba.gov.br/webservices/NFeConsultaProtocolo4/NFeConsultaProtocolo4.asmx',
        soap_xml_bytes=soap_consult,
        provider=signer,
        transport=httpx.MockTransport(lambda req: httpx.Response(200, content=consult_resp_xml)),
    )
    assert consult_res['timeout'] is False and consult_res['parsed']['c_stat'] == '100'
    record_simulated_attempt(
        db, draft_id='b' * 32, organization_id=1, farm_id=10,
        idempotency_key='idem-consult-001', uf='BA', operation='consulta_situacao',
        request_bytes=soap_consult, response_bytes=consult_resp_xml, user_id=5,
    )

    # Cancelamento (110111) assinado com XMLDSig + parse de retorno cStat=135.
    cancel = build_cancellation_event(
        state_uf='BA', author_document='52998224725', access_key=key,
        protocol_number='129260000000001',
        justification='Cancelamento de teste em conformidade offline na Bahia.',
        event_datetime='2026-11-22T00:05:00-03:00',
    )
    signed_cancel = signer.sign_xml(cancel['xml_bytes'], cancel['event_id'])
    assert verify_xml_signature(signed_cancel) is True
    cancel_ret = (
        f'<retEnvEvento xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.00">'
        f'<retEvento versao="1.00"><infEvento><cStat>135</cStat>'
        f'<xMotivo>Evento registrado e vinculado a NF-e</xMotivo>'
        f'<tpEvento>110111</tpEvento><nSeqEvento>1</nSeqEvento>'
        f'<nProt>129260000000055</nProt><chNFe>{key}</chNFe></infEvento></retEvento></retEnvEvento>'
    ).encode('utf-8')
    assert parse_event_response(cancel_ret)['category'] == 'evento_registrado'

    # Timeout simulado no transporte HTTP: tratado com segurança sem estourar exceção não tratada.
    def timeout_handler(req):
        raise httpx.ReadTimeout('Tempo limite simulado')

    timeout_res = send_soap_request(
        config=unlocked_cfg,
        endpoint_url='https://hnfe.sefaz.ba.gov.br/webservices/NFeStatusServico4/NFeStatusServico4.asmx',
        soap_xml_bytes=build_soap_status_service(state_uf='BA', tp_amb='2'),
        provider=signer,
        transport=httpx.MockTransport(timeout_handler),
    )
    assert timeout_res['timeout'] is True
    assert timeout_res['parsed']['category'] == 'timeout'
    assert 'Consulte a situação' in timeout_res['parsed']['x_motivo']
    db.close()


def test_conformity_saas_multi_client_isolation_and_pilot_readiness(monkeypatch):
    db = _new_db()
    monkeypatch.setenv('NFE_CERT_MASTER_KEY', TEST_MASTER_KEY)
    monkeypatch.delenv('NFE_ENABLE_SEFAZ_NETWORK', raising=False)

    # Cliente A (PF na Bahia, piloto) e Cliente B (CNPJ em MG) completamente independentes.
    cfg_a = save_client_fiscal_config(
        db, organization_id=101, farm_id=1001, user_id=1,
        data={
            'person_type': 'pf', 'document': '52998224725', 'state_registration': '123456',
            'crt': '3', 'state': 'BA', 'environment': 'homologacao',
            'default_series': '920', 'next_number': '1',
            'sefaz_credential_status': 'credenciado_homologacao', 'pilot_homologacao_enabled': '1',
        },
    )
    cfg_b = save_client_fiscal_config(
        db, organization_id=202, farm_id=2002, user_id=2,
        data={
            'person_type': 'cnpj', 'document': '11222333000181', 'state_registration': '987654',
            'crt': '1', 'state': 'MG', 'environment': 'teste_ficticio',
            'default_series': '1', 'next_number': '1',
            'sefaz_credential_status': 'nao_credenciado',
        },
    )
    assert cfg_a.document != cfg_b.document and cfg_a.default_series == 920 and cfg_b.default_series == 1

    # Cada cliente tem seu próprio certificado A1 criptografado e isolado por AAD.
    cert_a = store_encrypted_certificate(
        db, organization_id=101, farm_id=1001, environment='homologacao',
        pfx_bytes=_generate_ephemeral_pfx(password='pwd-a'), password='pwd-a',
        user_id=1, master_key=TEST_MASTER_KEY,
    )
    cert_b = store_encrypted_certificate(
        db, organization_id=202, farm_id=2002, environment='homologacao',
        pfx_bytes=_generate_ephemeral_pfx(password='pwd-b'), password='pwd-b',
        user_id=2, master_key=TEST_MASTER_KEY,
    )
    assert cert_a.fingerprint_sha256 != cert_b.fingerprint_sha256
    # Se alguém tentar reutilizar o registro do Cliente A no contexto do Cliente B, o AES-GCM falha.
    cert_a.organization_id = 202
    with pytest.raises(ValueError):
        decrypt_certificate_from_vault(cert_a, master_key=TEST_MASTER_KEY)
    cert_a.organization_id = 101

    # Numeração fictícia independente por cliente (ambos começam no número 1).
    id_a = reserve_or_update_test_identification(
        db, organization_id=101, farm_id=1001, draft_id='1' * 32, user_id=1, data={'series': '920'},
    )
    id_b = reserve_or_update_test_identification(
        db, organization_id=202, farm_id=2002, draft_id='2' * 32, user_id=2, data={'series': '920'},
    )
    assert id_a.number == 1 and id_b.number == 1

    # Prontidão do Cliente A piloto: seus dados/certificado estão ok, mas transmissão REAL segue bloqueada
    # por XSD reviewed=false e trava de rede/SEFAZ!
    readiness_a = evaluate_pilot_homologation_readiness(db, organization_id=101, farm_id=1001)
    assert readiness_a['transmission_blocked'] is True
    assert readiness_a['ready_for_official_homologacao'] is False
    by_code = {c['code']: c['ok'] for c in readiness_a['checks']}
    assert by_code['client_config'] is True
    assert by_code['client_certificate'] is True
    assert by_code['client_credential'] is True
    assert by_code['xsd_official_review'] is False
    assert by_code['sefaz_transmission_gate'] is False

    # Painel técnico distingue validação local de homologação oficial e renderiza sem erro.
    panel = build_technical_panel_context(db, organization_id=101, farm_id=1001)
    assert panel['stats']['approved_local'] >= 7
    assert panel['stats']['pending_official'] >= 2

    from jinja2 import ChoiceLoader, DictLoader
    from app.web import routes
    env = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({'base.html': '{% block content %}{% endblock %}'}),
        routes.templates.env.loader,
    ]))
    html = env.get_template('nfe_technical_panel.html').render(
        panel=panel, csrf_token='x', can_write_page=True,
    )
    assert '1. Validação local (offline / fictícia)' in html
    assert '2. Homologação oficial SEFAZ-BA (cliente piloto)' in html
    assert 'Transmissão real bloqueada' in html
    db.close()
