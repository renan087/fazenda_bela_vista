from app.services.asaas_customers import find_asaas_customers_by_document


class Response:
    status_code = 200

    def __init__(self, data):
        self.data = data

    def json(self):
        return self.data


def test_lookup_paginates_and_filters_exact_document(monkeypatch):
    calls = []

    class Client:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def get(self, url, headers, params):
            calls.append(params['offset'])
            if params['offset'] == 0:
                return Response({'data': [{'id': 'cus_1', 'cpfCnpj': '123.456.789-01'}], 'hasMore': True})
            return Response({'data': [{'id': 'cus_2', 'cpfCnpj': '00000000000'}], 'hasMore': False})

    monkeypatch.setattr('app.services.asaas_customers.httpx.Client', Client)
    rows, error = find_asaas_customers_by_document(base_url='https://api-sandbox.asaas.com', api_key='test', cpf_cnpj='12345678901')
    assert error is None and [row['id'] for row in rows] == ['cus_1']
    assert calls == [0, 100]


def test_lookup_rejects_incomplete_response(monkeypatch):
    class Client:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def get(self, *args, **kwargs): return Response({'data': []})

    monkeypatch.setattr('app.services.asaas_customers.httpx.Client', Client)
    rows, error = find_asaas_customers_by_document(base_url='https://api-sandbox.asaas.com', api_key='test', cpf_cnpj='12345678901')
    assert rows is None and error
