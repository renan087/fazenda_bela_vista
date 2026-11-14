from app.services import asaas_organization_payment_actions as actions


class Response:
    status_code = 200

    def __init__(self, body):
        self.body = body

    def json(self):
        return self.body

    def raise_for_status(self):
        pass


def test_receipt_uses_cash_endpoint_and_does_not_notify(monkeypatch):
    calls = []
    class Client:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def get(self, *args, **kwargs):
            return Response({'id': 'pay_123', 'customer': 'cus_1', 'subscription': 'sub_1', 'status': 'PENDING', 'billingType': 'PIX'})
        def post(self, url, **kwargs):
            calls.append((url, kwargs['json']))
            return Response({'id': 'pay_123', 'status': 'RECEIVED', 'billingType': 'RECEIVED_IN_CASH'})
    monkeypatch.setattr(actions.httpx, 'Client', Client)
    result, error, sent = actions.execute_payment_action(api_key='key', payment_id='pay_123', subscription_id='sub_1', customer_id='cus_1', action='receber', payment_date='2026-01-01', value=99.9)
    assert error is None and sent and result['id'] == 'pay_123'
    assert calls[0][0].endswith('/receiveInCash')
    assert calls[0][1] == {'paymentDate': '2026-01-01', 'value': 99.9, 'notifyCustomer': False}


def test_refund_rejects_wrong_customer_and_boleto(monkeypatch):
    class Client:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def get(self, *args, **kwargs):
            return Response({'id': 'pay_123', 'customer': 'cus_other', 'subscription': 'sub_1', 'status': 'RECEIVED', 'billingType': 'BOLETO'})
    monkeypatch.setattr(actions.httpx, 'Client', Client)
    result, error, sent = actions.execute_payment_action(api_key='key', payment_id='pay_123', subscription_id='sub_1', customer_id='cus_1', action='estornar')
    assert result is None and error and not sent
    result, error, sent = actions.execute_payment_action(api_key='key', payment_id='pay_123', subscription_id='sub_1', customer_id='cus_other', action='estornar')
    assert result is None and 'boleto' in error and not sent
