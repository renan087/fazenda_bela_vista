"""Cliente HTTP SOAP 1.2 com mTLS para comunicação futura com a SEFAZ.

Trava dupla de segurança: nenhuma requisição de rede é disparada enquanto
service_config.verified=False, service_config.transmission_enabled=False ou
NFE_ENABLE_SEFAZ_NETWORK != 'true'.
"""
from contextlib import contextmanager
import os
import ssl
import tempfile

import httpx

from app.services.nfe_certificate_vault import Pkcs12CertificateProvider
from app.services.nfe_sefaz_protocol import parse_sefaz_response
from app.services.nfe_sefaz_registry import SefazServiceConfig, assert_transmission_allowed

SOAP12_CONTENT_TYPE = 'application/soap+xml; charset=utf-8'


def _assert_network_gate(config: SefazServiceConfig, endpoint_url: str) -> None:
    assert_transmission_allowed(config)
    if os.environ.get('NFE_ENABLE_SEFAZ_NETWORK', '').strip().lower() != 'true':
        raise PermissionError(
            'Comunicação de rede com a SEFAZ desativada (defina NFE_ENABLE_SEFAZ_NETWORK=true somente em homologação controlada).'
        )
    if not isinstance(endpoint_url, str) or not endpoint_url.startswith('https://'):
        raise ValueError('Endpoint SEFAZ deve usar HTTPS.')


@contextmanager
def _ephemeral_mtls_context(provider: Pkcs12CertificateProvider):
    """Cria SSLContext mTLS usando arquivos temporários 0600 removidos ao final."""
    cert_pem, key_pem = provider.export_pem_pair()
    cert_path = None
    key_path = None
    try:
        fd_cert, cert_path = tempfile.mkstemp(prefix='nfe_cert_', suffix='.pem')
        os.chmod(cert_path, 0o600)
        with os.fdopen(fd_cert, 'wb') as fh:
            fh.write(cert_pem)

        fd_key, key_path = tempfile.mkstemp(prefix='nfe_key_', suffix='.pem')
        os.chmod(key_path, 0o600)
        with os.fdopen(fd_key, 'wb') as fh:
            fh.write(key_pem)

        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
        yield ctx
    finally:
        for path in (key_path, cert_path):
            if path and os.path.exists(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass


def send_soap_request(
    *,
    config: SefazServiceConfig,
    endpoint_url: str,
    soap_xml_bytes: bytes,
    provider: Pkcs12CertificateProvider,
    timeout_seconds: float = 20.0,
    transport: httpx.BaseTransport | None = None,
) -> dict:
    """Envia envelope SOAP 1.2 via mTLS (somente quando todas as travas forem liberadas)."""
    _assert_network_gate(config, endpoint_url)
    headers = {
        'Content-Type': SOAP12_CONTENT_TYPE,
        'Accept': 'application/soap+xml, application/xml, text/xml',
    }
    with _ephemeral_mtls_context(provider) as ssl_ctx:
        try:
            if transport is not None:
                req = httpx.Request('POST', endpoint_url, headers=headers, content=soap_xml_bytes)
                resp = transport.handle_request(req)
            else:
                with httpx.Client(timeout=timeout_seconds, headers=headers, verify=ssl_ctx) as client:
                    resp = client.post(endpoint_url, content=soap_xml_bytes)
        except httpx.TimeoutException:
            return {
                'http_status': None,
                'raw_response': b'',
                'timeout': True,
                'parsed': {
                    'c_stat': '',
                    'x_motivo': 'Tempo limite esgotado na comunicação com a SEFAZ. Consulte a situação da NF-e antes de reenviar.',
                    'category': 'timeout',
                },
            }

    parsed = parse_sefaz_response(resp.content) if resp.content else {'c_stat': '', 'x_motivo': '', 'category': 'desconhecido'}
    return {
        'http_status': resp.status_code,
        'raw_response': resp.content,
        'timeout': False,
        'parsed': parsed,
    }
