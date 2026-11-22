"""Catálogo de serviços SEFAZ por UF e ambiente (desativado para transmissão real).

As URLs e WSDLs reais exigem verificação direta no Portal Nacional e na SEFAZ-BA
antes da etapa de homologação. Enquanto transmission_enabled=False, nenhuma
chamada de rede à SEFAZ pode ser executada.
"""
from dataclasses import dataclass

from app.services.nfe_access_key import UF_IBGE_CODES

SEFAZ_SERVICES = (
    'NfeAutorizacao4',
    'NfeRetAutorizacao4',
    'NfeConsultaProtocolo4',
    'NfeStatusServico4',
    'RecepcaoEvento4',
    'NfeInutilizacao4',
)


@dataclass(frozen=True)
class SefazServiceConfig:
    uf: str
    uf_code: str
    environment: str
    service: str
    authorizer: str
    soap_version: str = '1.2'
    verified: bool = False
    transmission_enabled: bool = False
    reference_url: str = ''


# Mapeamento inicial focado na Bahia (autorizador próprio SEFAZ-BA); extensível para demais UFs.
_REGISTRY: dict[tuple[str, str], dict] = {
    ('BA', 'homologacao'): {
        'authorizer': 'SEFAZ-BA',
        'verified': False,
        'transmission_enabled': False,
        'reference_portal': 'https://hom.nfe.fazenda.gov.br/Portal/webServices.aspx',
    },
    ('BA', 'producao'): {
        'authorizer': 'SEFAZ-BA',
        'verified': False,
        'transmission_enabled': False,
        'reference_portal': 'https://www.nfe.fazenda.gov.br/portal/webServices.aspx',
    },
}


def get_service_config(*, state_uf: str, environment: str, service: str) -> SefazServiceConfig:
    """Retorna a configuração cadastrada para um serviço SEFAZ sem efetuar conexão."""
    uf = str(state_uf or '').strip().upper()
    env = str(environment or '').strip().lower()
    svc = str(service or '').strip()
    if uf not in UF_IBGE_CODES:
        raise ValueError(f'UF inválida: {uf!r}.')
    if env not in ('homologacao', 'producao'):
        raise ValueError('Ambiente SEFAZ deve ser homologacao ou producao.')
    if svc not in SEFAZ_SERVICES:
        raise ValueError(f'Serviço SEFAZ desconhecido: {svc!r}.')
    entry = _REGISTRY.get((uf, env))
    if not entry:
        raise NotImplementedError(f'Conector SEFAZ ainda não configurado para {uf} ({env}).')
    return SefazServiceConfig(
        uf=uf,
        uf_code=UF_IBGE_CODES[uf],
        environment=env,
        service=svc,
        authorizer=entry['authorizer'],
        verified=entry['verified'],
        transmission_enabled=entry['transmission_enabled'],
        reference_url=entry['reference_portal'],
    )


def assert_transmission_allowed(config: SefazServiceConfig) -> None:
    """Trava de segurança que impede qualquer transmissão real enquanto não homologado."""
    if not config.verified or not config.transmission_enabled:
        raise PermissionError(
            f'Transmissão à SEFAZ ({config.uf}/{config.environment}/{config.service}) está bloqueada nesta etapa.'
        )
