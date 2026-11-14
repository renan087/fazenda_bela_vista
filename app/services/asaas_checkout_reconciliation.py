"""Consultas de conciliacao do checkout de cartao no sandbox.

Este modulo nao cria checkout, altera assinaturas ou processa pagamentos.
"""

import uuid

import httpx


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
            rows = []
            for offset in range(0, 1000, 100):
                response = client.get('https://api-sandbox.asaas.com/v3/payments', headers=headers, params={'checkoutSession': checkout_id, 'limit': 100, 'offset': offset})
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, dict) or not isinstance(body.get('data'), list) or not isinstance(body.get('hasMore'), bool):
                    return None, 'Resposta de cobranças inválida.'
                rows.extend(body['data'])
                if not body['hasMore']:
                    break
            else:
                return None, 'A consulta excedeu o limite seguro.'
            if not rows or any(not isinstance(row, dict) or row.get('customer') != customer_id for row in rows):
                return None, 'Cobrança não encontrada ou cliente divergente.'
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
    except (httpx.HTTPError, ValueError):
        return None, 'Não foi possível confirmar o checkout no Asaas.'
