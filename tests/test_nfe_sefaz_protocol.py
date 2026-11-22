import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db.base import Base
from app.services.nfe_access_key import build_access_key
from app.services.nfe_schema import UnsafeXML
from app.services.nfe_sefaz_protocol import (
    DisabledCertificateProvider,
    build_soap_authorize_batch,
    build_soap_consult_protocol,
    build_soap_status_service,
    classify_sefaz_status,
    parse_sefaz_response,
    record_simulated_attempt,
)
from app.services.nfe_sefaz_registry import assert_transmission_allowed, get_service_config


def _new_db():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    return Session(engine)


def test_sefaz_registry_has_bahia_and_blocks_real_transmission():
    cfg = get_service_config(state_uf='BA', environment='homologacao', service='NfeAutorizacao4')
    assert cfg.uf == 'BA' and cfg.uf_code == '29' and cfg.authorizer == 'SEFAZ-BA'
    assert cfg.verified is False and cfg.transmission_enabled is False
    with pytest.raises(PermissionError):
        assert_transmission_allowed(cfg)

    with pytest.raises(NotImplementedError):
        get_service_config(state_uf='SP', environment='homologacao', service='NfeAutorizacao4')
    with pytest.raises(ValueError):
        get_service_config(state_uf='XX', environment='homologacao', service='NfeAutorizacao4')
    with pytest.raises(ValueError):
        get_service_config(state_uf='BA', environment='homologacao', service='ServicoInexistente')


def test_certificate_provider_is_disabled_and_soap_builders_enforce_homologacao():
    provider = DisabledCertificateProvider()
    with pytest.raises(PermissionError):
        provider.sign_xml(b'<NFe/>', 'NFe000')

    soap_status = build_soap_status_service(state_uf='BA', tp_amb='2')
    assert b'consStatServ' in soap_status and b'<cUF>29</cUF>' in soap_status

    with pytest.raises(PermissionError):
        build_soap_status_service(state_uf='BA', tp_amb='1')

    key = build_access_key(
        state_uf='BA', emission_date='2026-11-21', issuer_document='52998224725',
        series=920, number=1, numeric_code='24681357',
    )['key']
    soap_consult = build_soap_consult_protocol(access_key=key, tp_amb='2')
    assert key.encode('utf-8') in soap_consult and b'consSitNFe' in soap_consult

    with pytest.raises(ValueError):
        build_soap_consult_protocol(access_key='123', tp_amb='2')

    soap_batch = build_soap_authorize_batch(
        nfe_xml_bytes=b'<NFe xmlns="http://www.portalfiscal.inf.br/nfe"><infNFe versao="4.00"/></NFe>',
        batch_id='1001',
        sync=True,
    )
    assert b'enviNFe' in soap_batch and b'<indSinc>1</indSinc>' in soap_batch

    with pytest.raises(UnsafeXML):
        build_soap_authorize_batch(
            nfe_xml_bytes=b'<!DOCTYPE foo [<!ENTITY xxe "bad">]><NFe/>',
            batch_id='1',
        )


def test_parse_sefaz_response_classifies_status_and_blocks_xxe():
    auth_xml = (
        b'<?xml version="1.0" encoding="utf-8"?>'
        b'<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope"><soap:Body>'
        b'<retEnviNFe xmlns="http://www.portalfiscal.inf.br/nfe" versao="4.00">'
        b'<cStat>104</cStat><xMotivo>Lote processado</xMotivo>'
        b'<protNFe versao="4.00"><infProt>'
        b'<chNFe>29261100052998224725559200000000011246813570</chNFe>'
        b'<dhRecbto>2026-11-21T12:00:05-03:00</dhRecbto>'
        b'<nProt>129260000000001</nProt>'
        b'<cStat>100</cStat><xMotivo>Autorizado o uso da NF-e</xMotivo>'
        b'</infProt></protNFe></retEnviNFe></soap:Body></soap:Envelope>'
    )
    parsed = parse_sefaz_response(auth_xml)
    assert parsed['c_stat'] == '100'
    assert parsed['category'] == 'autorizada'
    assert parsed['protocol_number'] == '129260000000001'

    rejected_xml = (
        b'<retEnviNFe xmlns="http://www.portalfiscal.inf.br/nfe">'
        b'<cStat>225</cStat><xMotivo>Rejeicao: Falha no Schema XML</xMotivo></retEnviNFe>'
    )
    rej = parse_sefaz_response(rejected_xml)
    assert rej['c_stat'] == '225' and rej['category'] == 'rejeitada'

    assert classify_sefaz_status('107') == 'servico_em_operacao'
    assert classify_sefaz_status('105') == 'em_processamento'
    assert classify_sefaz_status('108') == 'servico_indisponivel'
    assert classify_sefaz_status('302') == 'denegada'

    with pytest.raises(UnsafeXML):
        parse_sefaz_response(b'<!DOCTYPE a [<!ENTITY x SYSTEM "file:///etc/passwd">]><retEnviNFe/>')


def test_record_simulated_attempt_is_idempotent_and_isolated():
    db = _new_db()
    req = build_soap_status_service(state_uf='BA', tp_amb='2')
    resp = (
        b'<retConsStatServ xmlns="http://www.portalfiscal.inf.br/nfe">'
        b'<cStat>107</cStat><xMotivo>Servico em Operacao</xMotivo></retConsStatServ>'
    )
    a1 = record_simulated_attempt(
        db, draft_id='d' * 32, organization_id=1, farm_id=10,
        idempotency_key='status-ba-001', uf='BA', operation='status_servico',
        request_bytes=req, response_bytes=resp, user_id=5,
    )
    a2 = record_simulated_attempt(
        db, draft_id='d' * 32, organization_id=1, farm_id=10,
        idempotency_key='status-ba-001', uf='BA', operation='status_servico',
        request_bytes=req, response_bytes=resp, user_id=5,
    )
    assert a1.id == a2.id
    assert a1.environment == 'teste_ficticio'
    assert a1.status_category == 'servico_em_operacao'
    assert len(a1.request_sha256) == 64 and len(a1.response_sha256) == 64
    db.close()
