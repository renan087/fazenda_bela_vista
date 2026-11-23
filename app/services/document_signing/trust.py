"""Official ICP-Brasil trust anchors and bounded fetching of CA evidence."""
import asyncio
import http.client
import ipaddress
import socket
from functools import lru_cache
from pathlib import Path
from threading import Lock
from types import SimpleNamespace
from urllib.parse import urlsplit

import requests
from pyhanko.keys import load_certs_from_pemder
from pyhanko_certvalidator import ValidationContext
from pyhanko_certvalidator.fetchers.api import Fetchers
from pyhanko_certvalidator.fetchers.requests_fetchers.cert_fetch_client import RequestsCertificateFetcher
from pyhanko_certvalidator.fetchers.requests_fetchers.crl_client import RequestsCRLFetcher
from pyhanko_certvalidator.fetchers.requests_fetchers.ocsp_client import RequestsOCSPFetcher

from app.core.config import get_settings

ASSETS = Path(__file__).parent / "assets"
# Additional endpoints are configured by the operator from the issuer's official repository.
OFFICIAL_CA_HOSTS = {
    "acraiz.icpbrasil.gov.br", "iti.gov.br", "www.iti.gov.br", "repositorio.iti.gov.br",
    "ac.certisign.com.br", "crl.certisign.com.br", "ocsp.certisign.com.br",
    "repositorio.serasaexperian.com.br", "ec.serasa.com.br",
    "repositorio.validcertificadora.com.br", "ocsp.validcertificadora.com.br",
    "repositorio.digital.serpro.gov.br", "ocsp.serpro.gov.br",
}
MAX_EVIDENCE_BYTES = 5 * 1024 * 1024


@lru_cache
def official_roots():
    return tuple(load_certs_from_pemder([str(ASSETS / "icp-brasil-roots.pem")]))


class EvidenceTransport:
    def __init__(self):
        self.allowed_hosts = OFFICIAL_CA_HOSTS | {
            value.strip().lower() for value in get_settings().digital_validation_hosts.split(",") if value.strip()
        }
        self.remaining = 20
        self.lock = Lock()

    def request(self, method, url, headers, data=None):
        try:
            return self._request(method, url, headers, data)
        except (OSError, ValueError, http.client.HTTPException) as exc:
            raise requests.RequestException("Não foi possível consultar a evidência da certificadora.") from exc

    def _request(self, method, url, headers, data):
        parts = urlsplit(url)
        scheme, host = parts.scheme, parts.hostname
        port = 443 if scheme == "https" else 80
        if (scheme not in {"https", "http"} or not host or parts.username is not None
                or parts.password is not None or parts.port not in (None, port)
                or not (host in self.allowed_hosts or host.endswith(".icpbrasil.gov.br"))):
            raise ValueError("Origem de validação não permitida.")
        with self.lock:
            if self.remaining <= 0:
                raise ValueError("Limite de consultas de validação atingido.")
            self.remaining -= 1
        destinations = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        addresses = {item[4][0] for item in destinations}
        if not addresses or any(not ipaddress.ip_address(address).is_global for address in addresses):
            raise ValueError("Endereço de validação não público.")

        # Pin the resolved address. HTTPSConnection still verifies the original hostname.
        connection_type = http.client.HTTPSConnection if scheme == "https" else http.client.HTTPConnection
        address = destinations[0][4][0]
        with_timeout = 4
        conn = connection_type(host, port, timeout=with_timeout)
        conn._create_connection = lambda _endpoint, timeout, source_address: socket.create_connection(
            (address, port), timeout, source_address
        )
        target = parts.path or "/"
        if parts.query:
            target += "?" + parts.query
        try:
            conn.request(method, target, body=data, headers=headers)
            response = conn.getresponse()
            if response.status != 200 or response.getheader("Content-Encoding", "identity") != "identity":
                raise ValueError("Resposta ou redirecionamento de validação não permitido.")
            body = response.read(MAX_EVIDENCE_BYTES + 1)
            if len(body) > MAX_EVIDENCE_BYTES:
                raise ValueError("Evidência de validação acima do limite.")
            return SimpleNamespace(content=body, headers=response.headers)
        finally:
            conn.close()


class _SafeFetch:
    def __init__(self, transport):
        super().__init__()
        self.transport = transport

    async def _get(self, url, *, acceptable_content_types):
        return await asyncio.to_thread(self.transport.request, "GET", url, {
            "Accept": ",".join(acceptable_content_types), "User-Agent": "SiSFarm/1.0",
        })

    async def _post(self, url, data, *, content_type, acceptable_content_types):
        return await asyncio.to_thread(self.transport.request, "POST", url, {
            "Content-Type": content_type, "Accept": ",".join(acceptable_content_types), "User-Agent": "SiSFarm/1.0",
        }, data)


class _CertificateFetcher(_SafeFetch, RequestsCertificateFetcher):
    pass


class _CrlFetcher(_SafeFetch, RequestsCRLFetcher):
    pass


class _OcspFetcher(_SafeFetch, RequestsOCSPFetcher):
    pass


def validation_context(*, dss=None, fetch=True, moment=None, other_certs=()):
    kwargs = {
        "trust_roots": official_roots(),
        "revocation_mode": "require",
        "allow_fetching": fetch,
        "other_certs": list(other_certs),
        "crls": [],
    }
    if moment is not None:
        kwargs["moment"] = moment
    if fetch:
        transport = EvidenceTransport()
        kwargs["fetchers"] = Fetchers(
            cert_fetcher=_CertificateFetcher(transport), crl_fetcher=_CrlFetcher(transport),
            ocsp_fetcher=_OcspFetcher(transport),
        )
    return dss.as_validation_context(kwargs) if dss is not None else ValidationContext(**kwargs)
