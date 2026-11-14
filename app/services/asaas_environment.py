"""Ambiente do Asaas usado pelas assinaturas das organizacoes (sandbox ou producao).

Toda chamada e toda consulta ao banco do modulo de assinaturas usa este ambiente. Producao so e
ativada quando ASAAS_ENVIRONMENT=production E ASAAS_API_BASE_URL=https://api.asaas.com (duas chaves),
para que uma unica variavel errada nao gere cobrancas reais.
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


def asaas_configuration_error(settings=None) -> str | None:
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
    return None


def asaas_ready(settings=None) -> bool:
    return asaas_configuration_error(settings) is None


def asaas_api_key(settings=None) -> str:
    return ((settings or get_settings()).asaas_api_key or "").strip()


def asaas_webhook_configured() -> bool:
    return bool((get_settings().asaas_webhook_token or "").strip())


def app_public_base(settings=None) -> str:
    """Endereco publico do SiSFarm (retorno do checkout), sem barra final."""
    return (getattr(settings or get_settings(), "app_public_url", None) or "https://app.sisfarm.com.br").rstrip("/")
