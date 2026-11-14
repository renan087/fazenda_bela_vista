"""Consultas de conciliacao do checkout de cartao no sandbox.

Este modulo nao cria checkout, altera assinaturas ou processa pagamentos.
"""

import uuid

import httpx


def _list_payments(client, headers, filters):
    rows = []
    for offset in range(0, 1000, 100):
        response = client.get('https://api-sandbox.asaas.com/v3/payments', headers=headers, params={**filters, 'limit': 100, 'offset': offset})
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, dict) or not isinstance(body.get('data'), list) or not isinstance(body.get('hasMore'), bool):
            raise ValueError('Resposta de cobranças inválida.')
        rows.extend(body['data'])
        if not body['hasMore']:
            return rows
    raise ValueError('A consulta excedeu o limite seguro.')


def find_paid_checkout_subscription(*, api_key: str, checkout_id: str, customer_id: str):
    """Retorna assinatura apenas quando existe uma unica cobranca confirmada e valida."""
    try:
        checkout_id = str(uuid.UUID(checkout_id))
    except (ValueError, TypeError, AttributeError):
        return None, 'Identificador de checkout inválido.'
    if not api_key or not customer_id or not customer_id.startswith('cus_'):
        return None, 'Credenciais ou cliente inválidos.'
    headers = {'access_token': api_key, 'User-Agent': 'SiSFarm/AsaasIntegration', 'Accept': 'application/json'}
    try:
        with httpx.Client(timeout=20.0) as client:
            rows = _list_payments(client, headers, {'checkoutSession': checkout_id})
            if not rows:
                customer_rows = _list_payments(client, headers, {'customer': customer_id})
                rows = [row for row in customer_rows if isinstance(row, dict) and row.get('checkoutSession') == checkout_id]
            if not rows:
                return None, 'O Asaas não retornou cobrança vinculada ao identificador deste checkout.'
            if any(not isinstance(row, dict) for row in rows):
                return None, 'O Asaas retornou uma cobrança inválida.'
            if any(row.get('customer') != customer_id for row in rows):
                actual = sorted({str(row.get('customer') or 'não informado')[:80] for row in rows})
                return None, f"Cliente divergente: esperado {customer_id[:80]}; retornado {', '.join(actual)[:100]}."
            if any(row.get('checkoutSession') not in (None, checkout_id) for row in rows):
                return None, 'O Asaas retornou uma cobrança vinculada a outro checkout.'
            candidates = {row.get('subscription') for row in rows if row.get('status') in {'CONFIRMED', 'RECEIVED'} and row.get('billingType') == 'CREDIT_CARD' and row.get('deleted') is not True}
            if len(candidates) != 1:
                return None, 'Não foi possível identificar uma assinatura paga única.'
            subscription_id = candidates.pop()
            if not isinstance(subscription_id, str) or not subscription_id.startswith('sub_') or not subscription_id.replace('_', '').isalnum():
                return None, 'Assinatura não identificada.'
            response = client.get(f'https://api-sandbox.asaas.com/v3/subscriptions/{subscription_id}', headers=headers)
            response.raise_for_status()
            subscription = response.json()
            if not isinstance(subscription, dict) or subscription.get('id') != subscription_id or subscription.get('customer') != customer_id or subscription.get('billingType') != 'CREDIT_CARD' or subscription.get('status') != 'ACTIVE' or subscription.get('deleted') is True:
                return None, 'A assinatura do cartão não foi confirmada.'
            return subscription, None
    except ValueError as exc:
        return None, str(exc)[:255] or 'Resposta inválida do Asaas.'
    except httpx.HTTPError:
        return None, 'Não foi possível confirmar o checkout no Asaas.'
