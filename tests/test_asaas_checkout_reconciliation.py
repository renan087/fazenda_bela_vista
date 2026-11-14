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


def test_only_one_open_migration_per_organization():
    from datetime import date
    from decimal import Decimal
    import pytest
    from sqlalchemy import create_engine
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm import Session
    from app.db.base import Base
    from app.models import Organization, OrganizationCardMigration, OrganizationSubscription

    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        org = Organization(name='Teste', slug='cartao', is_active=True)
        db.add(org); db.flush()
        old = OrganizationSubscription(organization_id=org.id, plan_name='Plano', amount=Decimal('20.00'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox')
        db.add(old); db.flush()
        db.add(OrganizationCardMigration(organization_id=org.id, old_subscription_id=old.id))
        db.commit()
        db.add(OrganizationCardMigration(organization_id=org.id, old_subscription_id=old.id))
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()


def test_checkout_uses_hosted_card_without_card_data(monkeypatch):
    from app.services import asaas_card_checkout as checkout
    calls = []
    class Response:
        status_code = 200
        def json(self): return {'id': CHECKOUT}
    class Client:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def post(self, url, **kwargs):
            calls.append((url, kwargs['json']))
            return Response()
    monkeypatch.setattr(checkout.httpx, 'Client', Client)
    result, error = checkout.create_sandbox_card_checkout(api_key='key', customer_id='cus_test', reference='migration-1', plan_name='Plano', amount=20, cycle='MONTHLY', next_due_date='2099-01-01', callback_base='https://app.sisfarm.com.br')
    assert result == CHECKOUT and error is None
    assert calls[0][1]['billingTypes'] == ['CREDIT_CARD']
    assert calls[0][1]['chargeTypes'] == ['RECURRENT']
    assert 'creditCard' not in calls[0][1]


def test_paid_migration_preserves_history_and_suspends_only_after_confirmation(monkeypatch):
    from datetime import date
    from decimal import Decimal
    import pytest
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from app.db.base import Base
    from app.models import Organization, OrganizationBilling, OrganizationCardMigration, OrganizationSubscription, OrganizationSubscriptionPayment
    from app.services import asaas_card_migration_worker as worker
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    calls = []
    monkeypatch.setattr(worker, 'find_paid_checkout_subscription', lambda **kwargs: (None, 'Pagamento não confirmado'))
    monkeypatch.setattr(worker, 'get_sandbox_subscription', lambda **kwargs: ({'customer': 'cus_test', 'status': 'ACTIVE'}, None))
    monkeypatch.setattr(worker, 'manage_sandbox_subscription', lambda **kwargs: (calls.append(kwargs) or {'customer': 'cus_test', 'status': 'INACTIVE'}, None))
    monkeypatch.setattr(worker, 'list_sandbox_subscription_payments', lambda **kwargs: ([], None))
    with Session(engine) as db:
        org = Organization(name='Teste', slug='migrar', is_active=True)
        db.add(org); db.flush()
        db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email='a@example.com', asaas_customer_id='cus_test', asaas_environment='sandbox'))
        old = OrganizationSubscription(organization_id=org.id, plan_name='Plano', amount=Decimal('20.00'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox', status='migrando', asaas_subscription_id='sub_old', is_current=True)
        db.add(old); db.flush()
        db.add(OrganizationSubscriptionPayment(subscription_id=old.id, asaas_payment_id='pay_old', status='PENDING'))
        migration = OrganizationCardMigration(organization_id=org.id, old_subscription_id=old.id, checkout_id=CHECKOUT, first_due_date=date(2099, 1, 1), status='pago')
        db.add(migration); db.commit()
        with pytest.raises(ValueError):
            worker.process_paid_card_migration(db, migration, 'key')
        assert not calls and old.is_current
        monkeypatch.setattr(worker, 'find_paid_checkout_subscription', lambda **kwargs: ({'id': 'sub_new', 'value': 20, 'cycle': 'MONTHLY', 'nextDueDate': '2099-02-01'}, None))
        worker.process_paid_card_migration(db, migration, 'key')
        db.commit()
        assert len(calls) == 1 and not old.is_current
        assert migration.status == 'concluido'
        assert db.query(OrganizationSubscriptionPayment).one().subscription_id == old.id
        new = db.query(OrganizationSubscription).filter_by(is_current=True).one()
        assert new.billing_type == 'CREDIT_CARD' and new.asaas_subscription_id == 'sub_new'


def test_checkout_webhook_requires_paid_event_and_matching_customer(monkeypatch):
    import asyncio
    from datetime import date
    from decimal import Decimal
    from types import SimpleNamespace
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from app.db.base import Base
    from app.models import Organization, OrganizationBilling, OrganizationCardMigration, OrganizationSubscription
    from app.routers import asaas_webhook

    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    monkeypatch.setattr(asaas_webhook, 'get_settings', lambda: SimpleNamespace(asaas_webhook_token='secret'))
    class Request:
        headers = {'asaas-access-token': 'secret'}
        customer = 'cus_other'
        async def json(self): return {'event': 'CHECKOUT_PAID', 'checkout': {'id': CHECKOUT, 'customer': self.customer, 'status': 'PAID'}}
    with Session(engine) as db:
        org = Organization(name='Teste', slug='webhook-card', is_active=True)
        db.add(org); db.flush()
        db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email='a@example.com', asaas_customer_id='cus_test', asaas_environment='sandbox'))
        old = OrganizationSubscription(organization_id=org.id, plan_name='Plano', amount=Decimal('20.00'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox')
        db.add(old); db.flush()
        migration = OrganizationCardMigration(organization_id=org.id, old_subscription_id=old.id, checkout_id=CHECKOUT, status='aguardando')
        db.add(migration); db.commit()
        import pytest
        from fastapi import HTTPException
        request = Request()
        with pytest.raises(HTTPException):
            asyncio.run(asaas_webhook.asaas_webhook(request, db))
        assert migration.status == 'aguardando'
        request.customer = 'cus_test'
        assert asyncio.run(asaas_webhook.asaas_webhook(request, db)) == {'received': True}
        assert migration.status == 'pago'
