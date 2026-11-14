"""Criacao de checkout recorrente hospedado, restrita ao sandbox."""

from uuid import UUID

import httpx


def create_sandbox_card_checkout(*, api_key, customer_id, reference, plan_name, amount, cycle, next_due_date, callback_base):
    if not api_key or not customer_id or not customer_id.startswith('cus_') or cycle not in {'MONTHLY', 'YEARLY'}:
        return None, 'Dados de checkout inválidos.'
    if callback_base != 'https://app.sisfarm.com.br':
        return None, 'Endereço de retorno não permitido.'
    callback = f'{callback_base}/organizacoes'
    payload = {
        'customer': customer_id,
        'externalReference': reference,
        'billingTypes': ['CREDIT_CARD'],
        'chargeTypes': ['RECURRENT'],
        'minutesToExpire': 60,
        'callback': {'successUrl': callback, 'cancelUrl': callback, 'expiredUrl': callback},
        'items': [{'name': plan_name, 'quantity': 1, 'value': float(amount)}],
        'subscription': {'cycle': cycle, 'nextDueDate': next_due_date},
    }
    try:
        with httpx.Client(timeout=60.0) as client:
            response = client.post('https://api-sandbox.asaas.com/v3/checkouts', headers={'access_token': api_key, 'User-Agent': 'SiSFarm/AsaasIntegration', 'Accept': 'application/json'}, json=payload)
        if response.status_code != 200:
            return None, f'Asaas retornou HTTP {response.status_code}; confira antes de tentar novamente.'
        data = response.json()
        checkout_id = str(UUID(str(data.get('id')))) if isinstance(data, dict) else None
        if not checkout_id:
            return None, 'O Asaas não confirmou o checkout.'
        return checkout_id, None
    except (httpx.HTTPError, ValueError, TypeError, AttributeError):
        return None, 'Resposta inconclusiva do Asaas; confira antes de tentar novamente.'
