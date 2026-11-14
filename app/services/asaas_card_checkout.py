"""Checkout recorrente hospedado no Asaas (cartao de credito), restrito ao sandbox.

O SiSFarm nunca recebe dados de cartao: o cliente preenche o cartao na pagina do Asaas.
"""

from urllib.parse import urlparse
from uuid import UUID

import httpx
from app.core.config import get_settings
from app.services.asaas_environment import app_public_base, asaas_api_base, current_asaas_environment

CHECKOUT_MINUTES_TO_EXPIRE = 1440  # maximo permitido pelo Asaas (24 horas)
_HEADERS = {'User-Agent': 'SiSFarm/AsaasIntegration', 'Accept': 'application/json'}


def sandbox_checkout_link(checkout_id: str) -> str:
    return f'{current_asaas_environment().web_base}/checkoutSession/show/{checkout_id}'


def safe_asaas_link(value) -> str | None:
    """Aceita apenas links https do proprio Asaas, para exibir e copiar com seguranca."""
    if not isinstance(value, str) or len(value) > 255:
        return None
    parsed = urlparse(value.strip())
    host = (parsed.hostname or '').lower()
    if parsed.scheme != 'https' or parsed.username or parsed.password or not (host == 'asaas.com' or host.endswith('.asaas.com')):
        return None
    return value.strip()


def create_sandbox_card_checkout_session(*, api_key, customer_id, reference, plan_name, amount, cycle, next_due_date, callback_base, callback_path='/minha-assinatura'):
    """Retorna ({'id', 'link'}, None) ou (None, erro)."""
    if not api_key or not customer_id or not customer_id.startswith('cus_') or cycle not in {'MONTHLY', 'QUARTERLY', 'SEMIANNUALLY', 'YEARLY'}:
        return None, 'Dados de checkout inválidos.'
    expected_base = app_public_base()
    if not expected_base.startswith('https://') or callback_base != expected_base or not callback_path.startswith('/') or callback_path.startswith('//'):
        return None, 'Endereço de retorno não permitido.'
    callback = f'{callback_base}{callback_path}'
    payload = {
        'customer': customer_id,
        'externalReference': reference,
        'billingTypes': ['CREDIT_CARD'],
        'chargeTypes': ['RECURRENT'],
        'minutesToExpire': CHECKOUT_MINUTES_TO_EXPIRE,
        'callback': {'successUrl': callback, 'cancelUrl': callback, 'expiredUrl': callback},
        'items': [{'name': plan_name, 'quantity': 1, 'value': float(amount)}],
        'subscription': {'cycle': cycle, 'nextDueDate': next_due_date},
    }
    try:
        with httpx.Client(timeout=60.0) as client:
            response = client.post(f'{asaas_api_base()}/v3/checkouts', headers={'access_token': api_key, **_HEADERS}, json=payload)
        if response.status_code != 200:
            return None, f'Asaas retornou HTTP {response.status_code}; confira antes de tentar novamente.'
        data = response.json()
        checkout_id = str(UUID(str(data.get('id')))) if isinstance(data, dict) else None
        if not checkout_id:
            return None, 'O Asaas não confirmou o checkout.'
        return {'id': checkout_id, 'link': safe_asaas_link(data.get('link')) or sandbox_checkout_link(checkout_id)}, None
    except (httpx.HTTPError, ValueError, TypeError, AttributeError):
        return None, 'Resposta inconclusiva do Asaas; confira antes de tentar novamente.'


def create_sandbox_card_checkout(**kwargs):
    """Compatibilidade: retorna apenas o identificador do checkout."""
    session, error = create_sandbox_card_checkout_session(**kwargs)
    return (session['id'] if session else None), error


def cancel_sandbox_checkout(*, api_key: str, checkout_id: str) -> tuple[str | None, str | None]:
    """Cancela um checkout ainda nao pago. Retorna (status_confirmado, erro)."""
    try:
        checkout_id = str(UUID(str(checkout_id)))
    except (ValueError, TypeError):
        return None, 'Identificador de checkout inválido.'
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(f'{asaas_api_base()}/v3/checkouts/{checkout_id}/cancel', headers={'access_token': api_key, **_HEADERS}, json={})
        if response.status_code != 200:
            return None, f'O Asaas não confirmou o cancelamento (HTTP {response.status_code}).'
        data = response.json()
        status = data.get('status') if isinstance(data, dict) else None
        if status not in {'CANCELED', 'EXPIRED', 'PAID', 'ACTIVE'}:
            return None, 'Resposta inválida ao cancelar o checkout.'
        return status, None
    except (httpx.HTTPError, ValueError):
        return None, 'Não foi possível confirmar o cancelamento no Asaas.'
