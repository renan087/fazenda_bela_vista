from app.services import asaas_checkout_reconciliation as reconciliation


CHECKOUT = 'c7b1c696-b27b-4d3d-80b9-d1c018e387f8'


def test_checkout_requires_confirmed_payment_and_unique_active_subscription(monkeypatch):
    state = {'status': 'PENDING', 'customer': 'cus_test'}
    class Response:
        def __init__(self, data): self.data = data
        def raise_for_status(self): pass
        def json(self): return self.data
    class Client:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def get(self, url, **kwargs):
            if url.endswith('/payments'):
                assert kwargs['params']['checkoutSession'] == CHECKOUT
                return Response({'data': [{'customer': state['customer'], 'subscription': 'sub_test', 'billingType': 'CREDIT_CARD', 'status': state['status']}], 'hasMore': False})
            return Response({'id': 'sub_test', 'customer': 'cus_test', 'billingType': 'CREDIT_CARD', 'status': 'ACTIVE'})
    monkeypatch.setattr(reconciliation.httpx, 'Client', Client)
    args = dict(api_key='sandbox', checkout_id=CHECKOUT, customer_id='cus_test')
    result, error = reconciliation.find_paid_checkout_subscription(**args)
    assert result is None and error
    state['status'] = 'CONFIRMED'
    result, error = reconciliation.find_paid_checkout_subscription(**args)
    assert result['id'] == 'sub_test' and error is None
    state['customer'] = 'cus_other'
    result, error = reconciliation.find_paid_checkout_subscription(**args)
    assert result is None and error
