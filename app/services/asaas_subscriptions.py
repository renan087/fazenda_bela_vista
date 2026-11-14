"""Sandbox subscription creation; failures never trigger an automatic retry."""

import httpx


def update_sandbox_subscription(*, api_key: str, subscription_id: str, customer_id: str, payload: dict) -> tuple[dict | None, str | None]:
    current, error = get_sandbox_subscription(api_key=api_key, subscription_id=subscription_id)
    if error:
        return None, error
    current_type = current.get('billingType')
    requested_type = payload.get('billingType', current_type)
    if current.get('customer') != customer_id or current.get('status') not in {'ACTIVE', 'INACTIVE'} or current_type not in {'PIX', 'BOLETO', 'CREDIT_CARD'} or requested_type not in {'PIX', 'BOLETO', 'CREDIT_CARD'} or (current_type != requested_type and 'CREDIT_CARD' in {current_type, requested_type}):
        return None, 'Assinatura ou cliente divergente; confira no Asaas antes de continuar.'
    headers = {'access_token': api_key, 'User-Agent': 'SiSFarm/AsaasIntegration', 'Accept': 'application/json'}
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.put(f'https://api-sandbox.asaas.com/v3/subscriptions/{subscription_id}', headers=headers, json={**payload, 'updatePendingPayments': False})
        if response.status_code != 200:
            return None, f'Asaas retornou HTTP {response.status_code}. Confira a assinatura antes de tentar novamente.'
        data = response.json()
        if not isinstance(data, dict) or data.get('id') != subscription_id or data.get('customer') != customer_id:
            return None, 'Resposta inconclusiva do Asaas; confira antes de tentar novamente.'
        return data, None
    except (httpx.HTTPError, ValueError):
        return None, 'Resposta inconclusiva do Asaas; confira antes de tentar novamente.'


def manage_sandbox_subscription(*, api_key: str, subscription_id: str, action: str, next_due_date: str | None = None) -> tuple[dict | None, str | None]:
    if not subscription_id or not subscription_id.startswith('sub_') or not subscription_id.replace('_', '').isalnum() or action not in {'suspender', 'reativar', 'cancelar'}:
        return None, 'Operação ou identificador inválido.'
    url = f'https://api-sandbox.asaas.com/v3/subscriptions/{subscription_id}'
    headers = {'access_token': api_key, 'User-Agent': 'SiSFarm/AsaasIntegration', 'Accept': 'application/json'}
    try:
        with httpx.Client(timeout=20.0) as client:
            if action == 'cancelar':
                response = client.delete(url, headers=headers)
            else:
                payload = {'status': 'INACTIVE' if action == 'suspender' else 'ACTIVE'}
                if action == 'reativar':
                    payload['nextDueDate'] = next_due_date
                response = client.put(url, headers=headers, json=payload)
        if response.status_code != 200:
            return None, f'Asaas retornou HTTP {response.status_code}. Verifique a assinatura antes de tentar novamente.'
        result = response.json()
        if not isinstance(result, dict) or result.get('id') != subscription_id:
            return None, 'O Asaas não confirmou a operação. Consulte a assinatura antes de tentar novamente.'
        if action == 'cancelar' and result.get('deleted') is not True:
            return None, 'O Asaas não confirmou o cancelamento. Consulte a assinatura antes de tentar novamente.'
        if action != 'cancelar' and result.get('status') != ('INACTIVE' if action == 'suspender' else 'ACTIVE'):
            return None, 'O Asaas não confirmou a situação solicitada. Consulte a assinatura antes de tentar novamente.'
        return result, None
    except (httpx.HTTPError, ValueError):
        return None, 'Resposta inconclusiva do Asaas. Consulte a assinatura antes de tentar novamente.'


def get_sandbox_subscription(*, api_key: str, subscription_id: str) -> tuple[dict | None, str | None]:
    if not subscription_id or not subscription_id.startswith('sub_') or not subscription_id.replace('_', '').isalnum():
        return None, 'Identificador de assinatura inválido.'
    try:
        with httpx.Client(timeout=20.0) as client:
            response = client.get(
                f'https://api-sandbox.asaas.com/v3/subscriptions/{subscription_id}',
                headers={'access_token': api_key, 'User-Agent': 'SiSFarm/AsaasIntegration', 'Accept': 'application/json'},
            )
        if response.status_code != 200:
            return None, f'Não foi possível consultar a assinatura (HTTP {response.status_code}).'
        data = response.json()
        if not isinstance(data, dict) or data.get('id') != subscription_id or not data.get('customer') or not data.get('status'):
            return None, 'O Asaas retornou dados incompletos da assinatura.'
        return data, None
    except (httpx.HTTPError, ValueError):
        return None, 'Não foi possível consultar a assinatura no Asaas.'


def list_sandbox_subscription_payments(*, api_key: str, subscription_id: str) -> tuple[list[dict] | None, str | None]:
    if not subscription_id or not subscription_id.startswith('sub_') or not subscription_id.replace('_', '').isalnum():
        return None, 'Identificador de assinatura inválido.'
    rows = []
    try:
        with httpx.Client(timeout=20.0) as client:
            for offset in range(0, 1000, 100):
                response = client.get(
                    f'https://api-sandbox.asaas.com/v3/subscriptions/{subscription_id}/payments',
                    headers={'access_token': api_key, 'User-Agent': 'SiSFarm/AsaasIntegration', 'Accept': 'application/json'},
                    params={'limit': 100, 'offset': offset},
                )
                if response.status_code != 200:
                    return None, f'Não foi possível consultar as cobranças (HTTP {response.status_code}).'
                data = response.json()
                if not isinstance(data, dict) or not isinstance(data.get('data'), list) or not isinstance(data.get('hasMore'), bool):
                    return None, 'O Asaas retornou uma lista de cobranças inválida.'
                rows.extend(data['data'])
                if not data['hasMore']:
                    return rows, None
    except (httpx.HTTPError, ValueError):
        return None, 'Não foi possível consultar as cobranças do Asaas.'
    return None, 'A consulta excedeu o limite seguro de cobranças.'


def create_sandbox_subscription(*, api_key: str, payload: dict) -> tuple[dict | None, str | None]:
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                "https://api-sandbox.asaas.com/v3/subscriptions",
                headers={"access_token": api_key, "User-Agent": "SiSFarm/AsaasIntegration", "Accept": "application/json"},
                json=payload,
            )
        if response.status_code not in (200, 201):
            return None, f"Asaas retornou HTTP {response.status_code}."
        data = response.json()
        if not isinstance(data, dict) or not isinstance(data.get("id"), str) or not data["id"].strip() or len(data["id"].strip()) > 80:
            return None, "O Asaas não confirmou o identificador da assinatura."
        return data, None
    except (httpx.HTTPError, ValueError):
        return None, "Não foi possível confirmar a resposta do Asaas."
