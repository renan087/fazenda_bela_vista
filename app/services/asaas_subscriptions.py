"""Sandbox subscription creation; failures never trigger an automatic retry."""

import httpx
from app.services.asaas_environment import asaas_api_base, current_asaas_environment


def list_customer_sandbox_subscriptions(*, api_key: str, customer_id: str) -> tuple[list[dict] | None, str | None]:
    """Lista tambem removidas e confirma o cliente na mesma conta autenticada."""
    if not api_key or not customer_id or not customer_id.startswith('cus_') or not customer_id.replace('_', '').isalnum():
        return None, 'Cliente Asaas inválido.'
    headers = {'access_token': api_key, 'User-Agent': 'SiSFarm/AsaasIntegration', 'Accept': 'application/json'}
    try:
        with httpx.Client(timeout=20.0) as client:
            customer = client.get(f'{asaas_api_base()}/v3/customers/{customer_id}', headers=headers)
            if customer.status_code != 200:
                return None, 'Não foi possível confirmar o cliente na conta Asaas configurada.'
            customer_data = customer.json()
            if not isinstance(customer_data, dict) or customer_data.get('id') != customer_id or customer_data.get('deleted') is True:
                return None, 'Cliente Asaas indisponível ou divergente.'
            rows = []
            for offset in range(0, 1000, 100):
                response = client.get(f'{asaas_api_base()}/v3/subscriptions', headers=headers, params={'customer': customer_id, 'includeDeleted': 'true', 'limit': 100, 'offset': offset})
                if response.status_code != 200:
                    return None, f'Não foi possível consultar as assinaturas (HTTP {response.status_code}).'
                data = response.json()
                if not isinstance(data, dict) or not isinstance(data.get('data'), list) or not isinstance(data.get('hasMore'), bool):
                    return None, 'Resposta inválida da consulta de assinaturas.'
                if any(not isinstance(item, dict) or item.get('customer') != customer_id or not item.get('id') or not isinstance(item.get('deleted'), bool) for item in data['data']):
                    return None, 'Assinatura retornada sem identificação ou situação de exclusão válida.'
                rows.extend(data['data'])
                if not data['hasMore']:
                    return rows, None
    except (httpx.HTTPError, ValueError):
        return None, 'Não foi possível confirmar as assinaturas no Asaas.'
    return None, 'A consulta excedeu o limite seguro de assinaturas.'


def sandbox_subscription_is_missing(*, api_key: str, subscription_id: str) -> tuple[bool | None, str | None]:
    if not subscription_id or not subscription_id.startswith('sub_') or not subscription_id.replace('_', '').isalnum():
        return None, 'Identificador de assinatura inválido.'
    try:
        with httpx.Client(timeout=20.0) as client:
            response = client.get(f'{asaas_api_base()}/v3/subscriptions/{subscription_id}', headers={'access_token': api_key, 'User-Agent': 'SiSFarm/AsaasIntegration', 'Accept': 'application/json'})
        if response.status_code == 404:
            return True, None
        if response.status_code == 200:
            return False, None
        return None, f'Não foi possível confirmar a assinatura (HTTP {response.status_code}).'
    except httpx.HTTPError:
        return None, 'Não foi possível confirmar a assinatura no Asaas.'


def update_sandbox_subscription(*, api_key: str, subscription_id: str, customer_id: str, payload: dict) -> tuple[dict | None, str | None]:
    current, error = get_sandbox_subscription(api_key=api_key, subscription_id=subscription_id)
    if error:
        return None, error
    current_type = current.get('billingType')
    requested_type = payload.get('billingType', current_type)
    if current.get('customer') != customer_id or current.get('status') not in {'ACTIVE', 'INACTIVE'} or current_type not in {'PIX', 'BOLETO', 'CREDIT_CARD'} or requested_type not in {'PIX', 'BOLETO', 'CREDIT_CARD'} or (requested_type == 'CREDIT_CARD' and current_type != 'CREDIT_CARD'):
        return None, 'Assinatura ou cliente divergente; confira no Asaas antes de continuar.'
    headers = {'access_token': api_key, 'User-Agent': 'SiSFarm/AsaasIntegration', 'Accept': 'application/json'}
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.put(f'{asaas_api_base()}/v3/subscriptions/{subscription_id}', headers=headers, json={**payload, 'updatePendingPayments': False})
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
    url = f'{asaas_api_base()}/v3/subscriptions/{subscription_id}'
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
                f'{asaas_api_base()}/v3/subscriptions/{subscription_id}',
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
                    f'{asaas_api_base()}/v3/subscriptions/{subscription_id}/payments',
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
                f"{asaas_api_base()}/v3/subscriptions",
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


def get_sandbox_payment(*, api_key: str, payment_id: str) -> tuple[dict | None, str | None]:
    """Consulta autoritativa de uma cobranca (inclui cobrancas removidas, com ``deleted``)."""
    if not payment_id or not payment_id.startswith('pay_') or not payment_id.replace('_', '').isalnum():
        return None, 'Identificador de cobrança inválido.'
    try:
        with httpx.Client(timeout=20.0) as client:
            response = client.get(
                f'{asaas_api_base()}/v3/payments/{payment_id}',
                headers={'access_token': api_key, 'User-Agent': 'SiSFarm/AsaasIntegration', 'Accept': 'application/json'},
            )
        if response.status_code == 404:
            return {'id': payment_id, 'deleted': True}, None
        if response.status_code != 200:
            return None, f'Não foi possível consultar a cobrança (HTTP {response.status_code}).'
        data = response.json()
        if not isinstance(data, dict) or data.get('id') != payment_id:
            return None, 'O Asaas retornou dados incompletos da cobrança.'
        return data, None
    except (httpx.HTTPError, ValueError):
        return None, 'Não foi possível consultar a cobrança no Asaas.'


def _card_token_from_payment(client, headers, *, payment_id: str, customer_id: str) -> tuple[str | None, str | None]:
    """Busca o token na cobranca paga no cartao. O token nunca e gravado no SiSFarm."""
    response = client.get(f'{asaas_api_base()}/v3/payments/{payment_id}', headers=headers)
    if response.status_code != 200:
        return None, f'Não foi possível consultar a cobrança do cartão (HTTP {response.status_code}).'
    payment = response.json()
    card = payment.get('creditCard') if isinstance(payment, dict) else None
    if not isinstance(payment, dict) or payment.get('id') != payment_id or payment.get('customer') != customer_id or payment.get('billingType') != 'CREDIT_CARD' or payment.get('status') not in {'CONFIRMED', 'RECEIVED'}:
        return None, 'A cobrança do cartão salvo não pertence a este cliente ou não foi paga.'
    token = card.get('creditCardToken') if isinstance(card, dict) else None
    if not isinstance(token, str) or not token.strip() or len(token) > 200:
        return None, 'O Asaas não disponibilizou o cartão salvo para reutilização (tokenização inativa).'
    return token.strip(), None


def switch_sandbox_subscription_to_saved_card(*, api_key: str, subscription_id: str, customer_id: str, card_payment_id: str, remote_ip: str) -> tuple[dict | None, str | None]:
    """Volta uma assinatura Pix/boleto para o cartao ja cadastrado pelo cliente, mantendo ciclo e proxima cobranca.

    1) troca a forma da assinatura para cartao; 2) vincula o cartao salvo (token) sem cobrar;
    se o passo 2 falhar, desfaz o passo 1 para nao deixar a assinatura no cartao sem cartao.
    """
    if not card_payment_id or not card_payment_id.startswith('pay_') or not card_payment_id.replace('_', '').isalnum():
        return None, 'Cobrança de referência do cartão inválida.'
    if not remote_ip:
        return None, 'Não foi possível identificar o IP da operação.'
    current, error = get_sandbox_subscription(api_key=api_key, subscription_id=subscription_id)
    if error:
        return None, error
    previous_type = current.get('billingType')
    if current.get('customer') != customer_id or current.get('status') != 'ACTIVE' or previous_type not in {'PIX', 'BOLETO'}:
        return None, 'A assinatura não está ativa em Pix ou boleto no Asaas.'
    headers = {'access_token': api_key, 'User-Agent': 'SiSFarm/AsaasIntegration', 'Accept': 'application/json'}
    base = f'{asaas_api_base()}/v3/subscriptions/{subscription_id}'
    try:
        with httpx.Client(timeout=30.0) as client:
            token, error = _card_token_from_payment(client, headers, payment_id=card_payment_id, customer_id=customer_id)
            if error:
                return None, error
            switched = client.put(base, headers=headers, json={'billingType': 'CREDIT_CARD', 'updatePendingPayments': False})
            if switched.status_code != 200:
                return None, f'O Asaas não aceitou a troca para cartão (HTTP {switched.status_code}). Nada foi alterado.'
            body = {'creditCardToken': token, 'remoteIp': remote_ip}
            # A documentacao do Asaas cita dois caminhos para trocar o cartao; o segundo so e tentado se o primeiro nao existir.
            card = client.post(f'{base}/updateCreditCard', headers=headers, json=body)
            if card.status_code in (404, 405):
                card = client.put(f'{base}/creditCard', headers=headers, json=body)
            if card.status_code != 200:
                reverted = client.put(base, headers=headers, json={'billingType': previous_type, 'updatePendingPayments': False})
                suffix = 'A forma anterior foi mantida.' if reverted.status_code == 200 else 'A situação será conferida automaticamente.'
                return None, f'O Asaas não confirmou o cartão salvo (HTTP {card.status_code}). {suffix}'
            data = card.json()
            if not isinstance(data, dict) or data.get('id') != subscription_id or data.get('customer') != customer_id or data.get('billingType') != 'CREDIT_CARD':
                return None, 'Resposta inconclusiva do Asaas; a situação será conferida automaticamente.'
            return data, None
    except (httpx.HTTPError, ValueError, AttributeError):
        return None, 'Resposta inconclusiva do Asaas; a situação será conferida automaticamente.'
