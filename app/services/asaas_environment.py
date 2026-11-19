"""Ambiente do Asaas usado pelas assinaturas das organizacoes (sandbox ou producao).

Toda chamada e toda consulta ao banco do modulo de assinaturas usa este ambiente. A integração de produção exige URL e credenciais próprias e habilitação explícita.
A contratação é limitada às organizações do piloto.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.core.config import get_settings


@dataclass(frozen=True)
class AsaasEnvironment:
    name: str
    api_base: str
    web_base: str
    label: str

    @property
    def is_production(self) -> bool:
        return self.name == "production"


SANDBOX = AsaasEnvironment("sandbox", "https://api-sandbox.asaas.com", "https://sandbox.asaas.com", "Ambiente de testes (Asaas sandbox)")
PRODUCTION = AsaasEnvironment("production", "https://api.asaas.com", "https://www.asaas.com", "Produção (cobranças reais)")
ENVIRONMENTS = {SANDBOX.name: SANDBOX, PRODUCTION.name: PRODUCTION}


def current_asaas_environment(settings=None) -> AsaasEnvironment:
    settings = settings or get_settings()
    name = (getattr(settings, "asaas_environment", None) or "sandbox").strip().lower()
    return ENVIRONMENTS.get(name, SANDBOX)


def asaas_api_base() -> str:
    return current_asaas_environment().api_base


def asaas_env_name() -> str:
    return current_asaas_environment().name


def asaas_configuration_error(settings=None, *, read_only: bool = False) -> str | None:
    """Motivo pelo qual o modulo nao pode chamar o Asaas, ou None se esta pronto."""
    settings = settings or get_settings()
    name = (getattr(settings, "asaas_environment", None) or "sandbox").strip().lower()
    if name not in ENVIRONMENTS:
        return "ASAAS_ENVIRONMENT deve ser 'sandbox' ou 'production'."
    environment = ENVIRONMENTS[name]
    base = (settings.asaas_api_base_url or "").strip().rstrip("/")
    if base.endswith("/v3"):
        base = base[:-3]
    if base != environment.api_base:
        return f"ASAAS_API_BASE_URL não corresponde ao ambiente {environment.name} (esperado {environment.api_base})."
    if not (settings.asaas_api_key or "").strip():
        return "Configure a chave da API do Asaas (ASAAS_API_KEY)."
    if environment.is_production and not read_only:
        if not getattr(settings, "asaas_live_enabled", False):
            return "Cobranças reais desativadas (ASAAS_LIVE_ENABLED)."
        webhook = (settings.asaas_webhook_token or "").strip()
        if len(webhook) < 32 or webhook == settings.asaas_api_key.strip():
            return "Configure um token exclusivo do webhook de produção com pelo menos 32 caracteres."
    return None


def asaas_pilot_allows(organization_id: int, settings=None) -> bool:
    settings = settings or get_settings()
    if not current_asaas_environment(settings).is_production:
        return True
    allowed = (getattr(settings, "asaas_pilot_organization_ids", "") or "").split(",")
    return str(organization_id) in {item.strip() for item in allowed}


def asaas_ready(settings=None) -> bool:
    return asaas_configuration_error(settings) is None


def asaas_api_key(settings=None) -> str:
    return ((settings or get_settings()).asaas_api_key or "").strip()


def asaas_webhook_configured() -> bool:
    return bool((get_settings().asaas_webhook_token or "").strip())


def app_public_base(settings=None) -> str:
    """Endereco publico do SiSFarm (retorno do checkout), sem barra final."""
    return (getattr(settings or get_settings(), "app_public_url", None) or "https://app.sisfarm.com.br").rstrip("/")
