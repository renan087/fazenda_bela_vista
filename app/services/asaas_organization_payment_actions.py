"""Operações individuais no sandbox, sem retentativas automáticas."""

import httpx
from app.services.asaas_environment import asaas_api_base, current_asaas_environment


def execute_payment_action(*, api_key, payment_id, subscription_id, customer_id, action, payment_date=None, value=None):
    if not payment_id.startswith('pay_') or not payment_id.replace('_', '').isalnum():
        return None, 'Identificador de cobrança inválido.', False
    headers = {'access_token': api_key, 'User-Agent': 'SiSFarm/AsaasIntegration', 'Accept': 'application/json'}
    url = f'{asaas_api_base()}/v3/payments/{payment_id}'
    try:
        with httpx.Client(timeout=20.0) as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            payment = response.json()
            if not isinstance(payment, dict) or payment.get('id') != payment_id or payment.get('subscription') != subscription_id or payment.get('customer') != customer_id:
                return None, 'A cobrança não corresponde ao cliente e à assinatura.', False
            status = payment.get('status')
            billing = payment.get('billingType')
            if payment.get('deleted') or (action in {'cancelar', 'receber'} and status not in {'PENDING', 'OVERDUE'}):
                return None, 'A situação atual da cobrança não permite esta operação.', False
            if action == 'estornar' and not ((billing == 'PIX' and status == 'RECEIVED') or (billing == 'CREDIT_CARD' and status in {'RECEIVED', 'CONFIRMED'})):
                return None, 'Estorno disponível apenas para Pix recebido ou cartão confirmado/recebido; para boleto utilize o Asaas.', False
            if action not in {'cancelar', 'receber', 'estornar'}:
                return None, 'Operação inválida.', False
            try:
                if action == 'cancelar':
                    result = client.delete(url, headers=headers)
                elif action == 'receber':
                    result = client.post(url + '/receiveInCash', headers=headers, json={'paymentDate': payment_date, 'value': float(value), 'notifyCustomer': False})
                else:
                    result = client.post(url + '/refund', headers=headers, json={})
                if result.status_code != 200:
                    return None, f'Asaas retornou HTTP {result.status_code}; confira no Asaas antes de repetir.', True
                data = result.json()
                if not isinstance(data, dict) or data.get('id') != payment_id:
                    return None, 'Resposta inconclusiva; confira no Asaas antes de repetir.', True
                return data, None, True
            except (httpx.HTTPError, ValueError):
                return None, 'Resposta inconclusiva; confira no Asaas antes de repetir.', True
    except (httpx.HTTPError, ValueError):
        return None, 'Não foi possível verificar a cobrança no Asaas; nenhuma ação foi enviada.', False
