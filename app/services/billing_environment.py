from app.core.config import get_settings
from app.services.asaas_environment import asaas_env_name

MERCADOPAGO_TEST = "mercadopago_test"


def billing_environment() -> str:
    return MERCADOPAGO_TEST if get_settings().mercadopago_sandbox else asaas_env_name()
