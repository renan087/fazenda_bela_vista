"""Sandbox subscription creation; failures never trigger an automatic retry."""

import httpx


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
