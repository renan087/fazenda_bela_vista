import httpx
import pytest

from app.services.nfe_certificate_vault import Pkcs12CertificateProvider
from app.services.nfe_sefaz_client import send_soap_request
from app.services.nfe_sefaz_protocol import build_soap_status_service
from app.services.nfe_sefaz_registry import SefazServiceConfig, get_service_config
from tests.test_nfe_certificate_and_signature import _generate_ephemeral_pfx


def test_sefaz_client_is_blocked_by_default_gates(monkeypatch):
    pfx = _generate_ephemeral_pfx(password='segredo123')
    provider = Pkcs12CertificateProvider(pfx, 'segredo123')
    soap = build_soap_status_service(state_uf='BA', tp_amb='2')

    # 1. Bloqueado pelo registro padrão (verified=False, transmission_enabled=False).
    default_cfg = get_service_config(state_uf='BA', environment='homologacao', service='NfeStatusServico4')
    with pytest.raises(PermissionError):
        send_soap_request(
            config=default_cfg,
            endpoint_url='https://hnfe.sefaz.ba.gov.br/webservices/NFeStatusServico4/NFeStatusServico4.asmx',
            soap_xml_bytes=soap,
            provider=provider,
        )

    # 2. Mesmo com config liberada em teste, bloqueia sem NFE_ENABLE_SEFAZ_NETWORK=true.
    monkeypatch.delenv('NFE_ENABLE_SEFAZ_NETWORK', raising=False)
    unlocked_cfg = SefazServiceConfig(
        uf='BA', uf_code='29', environment='homologacao', service='NfeStatusServico4',
        authorizer='SEFAZ-BA', verified=True, transmission_enabled=True,
    )
    with pytest.raises(PermissionError):
        send_soap_request(
            config=unlocked_cfg,
            endpoint_url='https://hnfe.sefaz.ba.gov.br/webservices/NFeStatusServico4/NFeStatusServico4.asmx',
            soap_xml_bytes=soap,
            provider=provider,
        )


def test_sefaz_client_sends_soap12_over_mock_transport_when_explicitly_unlocked(monkeypatch):
    monkeypatch.setenv('NFE_ENABLE_SEFAZ_NETWORK', 'true')
    pfx = _generate_ephemeral_pfx(password='segredo123')
    provider = Pkcs12CertificateProvider(pfx, 'segredo123')
    soap = build_soap_status_service(state_uf='BA', tp_amb='2')

    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured['content_type'] = request.headers.get('content-type')
        captured['body'] = request.content
        xml_resp = (
            b'<?xml version="1.0" encoding="utf-8"?>'
            b'<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope"><soap:Body>'
            b'<retConsStatServ xmlns="http://www.portalfiscal.inf.br/nfe" versao="4.00">'
            b'<cStat>107</cStat><xMotivo>Servico em Operacao</xMotivo>'
            b'</retConsStatServ></soap:Body></soap:Envelope>'
        )
        return httpx.Response(200, content=xml_resp, headers={'Content-Type': 'application/soap+xml; charset=utf-8'})

    unlocked_cfg = SefazServiceConfig(
        uf='BA', uf_code='29', environment='homologacao', service='NfeStatusServico4',
        authorizer='SEFAZ-BA', verified=True, transmission_enabled=True,
    )
    result = send_soap_request(
        config=unlocked_cfg,
        endpoint_url='https://hnfe.sefaz.ba.gov.br/webservices/NFeStatusServico4/NFeStatusServico4.asmx',
        soap_xml_bytes=soap,
        provider=provider,
        transport=httpx.MockTransport(handler),
    )
    assert result['http_status'] == 200
    assert 'application/soap+xml' in (captured['content_type'] or '')
    assert b'consStatServ' in captured['body']
    assert result['parsed']['c_stat'] == '107'
    assert result['parsed']['category'] == 'servico_em_operacao'
