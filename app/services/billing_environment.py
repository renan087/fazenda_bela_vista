from app.core.config import get_settings
from app.services.asaas_environment import asaas_env_name

MERCADOPAGO_TEST = "mercadopago_test"
MERCADOPAGO_LIVE = "mercadopago_live"
MERCADOPAGO_ENVIRONMENTS = {MERCADOPAGO_TEST, MERCADOPAGO_LIVE}


def mercadopago_environment(settings=None) -> str | None:
    settings = settings or get_settings()
    if getattr(settings, "mercadopago_live_enabled", False):
        return MERCADOPAGO_LIVE
    if getattr(settings, "mercadopago_sandbox", False):
        return MERCADOPAGO_TEST
    return None


def billing_environment() -> str:
    return mercadopago_environment() or asaas_env_name()
