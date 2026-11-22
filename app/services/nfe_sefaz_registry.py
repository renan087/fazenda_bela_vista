"""Catálogo de serviços SEFAZ por UF e ambiente (transmissão real mantida bloqueada).

Endpoints de homologação da SEFAZ-BA conferidos no Portal Nacional da NF-e em 22/11/2026.
Enquanto transmission_enabled=False, nenhuma chamada de rede à SEFAZ pode ser executada.
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
    'CadConsultaCadastro4',
)

_SOAP_ACTIONS = {
    'NfeAutorizacao4': 'http://www.portalfiscal.inf.br/nfe/wsdl/NFeAutorizacao4/nfeAutorizacaoLote',
    'NfeRetAutorizacao4': 'http://www.portalfiscal.inf.br/nfe/wsdl/NFeRetAutorizacao4/nfeRetAutorizacaoLote',
    'NfeConsultaProtocolo4': 'http://www.portalfiscal.inf.br/nfe/wsdl/NFeConsultaProtocolo4/nfeConsultaNF',
    'NfeStatusServico4': 'http://www.portalfiscal.inf.br/nfe/wsdl/NFeStatusServico4/nfeStatusServicoNF',
    'RecepcaoEvento4': 'http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4/nfeRecepcaoEvento',
    'NfeInutilizacao4': 'http://www.portalfiscal.inf.br/nfe/wsdl/NFeInutilizacao4/nfeInutilizacaoNF',
    'CadConsultaCadastro4': 'http://www.portalfiscal.inf.br/nfe/wsdl/CadConsultaCadastro4/consultaCadastro',
}

_BA_HOMOLOGACAO_ENDPOINTS = {
    'NfeAutorizacao4': 'https://hnfe.sefaz.ba.gov.br/webservices/NFeAutorizacao4/NFeAutorizacao4.asmx',
    'NfeRetAutorizacao4': 'https://hnfe.sefaz.ba.gov.br/webservices/NFeRetAutorizacao4/NFeRetAutorizacao4.asmx',
    'NfeConsultaProtocolo4': 'https://hnfe.sefaz.ba.gov.br/webservices/NFeConsultaProtocolo4/NFeConsultaProtocolo4.asmx',
    'NfeStatusServico4': 'https://hnfe.sefaz.ba.gov.br/webservices/NFeStatusServico4/NFeStatusServico4.asmx',
    'RecepcaoEvento4': 'https://hnfe.sefaz.ba.gov.br/webservices/NFeRecepcaoEvento4/NFeRecepcaoEvento4.asmx',
    'NfeInutilizacao4': 'https://hnfe.sefaz.ba.gov.br/webservices/NFeInutilizacao4/NFeInutilizacao4.asmx',
    'CadConsultaCadastro4': 'https://hnfe.sefaz.ba.gov.br/webservices/CadConsultaCadastro4/CadConsultaCadastro4.asmx',
}


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
    endpoint_url: str = ''
    soap_action: str = ''
    reference_url: str = ''


# Mapeamento inicial focado na Bahia (autorizador próprio SEFAZ-BA); extensível para demais UFs.
_REGISTRY: dict[tuple[str, str], dict] = {
    ('BA', 'homologacao'): {
        'authorizer': 'SEFAZ-BA',
        'verified': True,
        'transmission_enabled': False,
        'endpoints': _BA_HOMOLOGACAO_ENDPOINTS,
        'reference_portal': 'https://hom.nfe.fazenda.gov.br/Portal/webServices.aspx?tipoConteudo=OUC%2FYVNWZfo%3D',
    },
    ('BA', 'producao'): {
        'authorizer': 'SEFAZ-BA',
        'verified': False,
        'transmission_enabled': False,
        'endpoints': {},
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
        endpoint_url=entry['endpoints'].get(svc, ''),
        soap_action=_SOAP_ACTIONS.get(svc, ''),
        reference_url=entry['reference_portal'],
    )


def assert_transmission_allowed(config: SefazServiceConfig) -> None:
    """Trava de segurança que impede qualquer transmissão real enquanto não liberado."""
    if not config.verified or not config.transmission_enabled:
        raise PermissionError(
            f'Transmissão à SEFAZ ({config.uf}/{config.environment}/{config.service}) está bloqueada nesta etapa.'
        )
