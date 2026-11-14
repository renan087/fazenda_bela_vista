from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.db.base import Base
from app.models import AsaasWebhookEvent, Organization, OrganizationBilling, OrganizationSubscription, OrganizationSubscriptionPayment
from app.services.organization_subscription_sync import persist_organization_webhook


def test_payment_events_are_idempotent_and_cannot_overwrite_newer_status():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        org = Organization(name='Teste', slug='teste', is_active=True)
        db.add(org)
        db.flush()
        db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email='a@example.com', asaas_customer_id='cus_test', asaas_environment='sandbox'))
        db.add(OrganizationSubscription(organization_id=org.id, plan_name='Teste', amount=Decimal('99.90'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox', asaas_subscription_id='sub_test'))
        db.commit()
        def event(identifier, timestamp, status, customer='cus_test'):
            return {'id': identifier, 'event': 'PAYMENT_UPDATED', 'dateCreated': timestamp, 'payment': {'id': 'pay_test', 'subscription': 'sub_test', 'customer': customer, 'status': status, 'value': 99.90, 'dueDate': '2026-01-01', 'paymentDate': '2026-01-02'}}
        paid = event('evt_paid', '2026-01-02T10:00:00', 'RECEIVED')
        assert persist_organization_webhook(db, paid)
        db.commit()
        assert persist_organization_webhook(db, paid)
        assert persist_organization_webhook(db, event('evt_old', '2026-01-01T10:00:00', 'PENDING'))
        db.commit()
        payment = db.query(OrganizationSubscriptionPayment).one()
        assert payment.status == 'RECEIVED'
        assert payment.paid_date == date(2026, 1, 2)
        assert db.query(AsaasWebhookEvent).count() == 2
        with pytest.raises(ValueError):
            persist_organization_webhook(db, event('evt_wrong', '2026-01-03T10:00:00', 'PENDING', 'cus_wrong'))
        db.rollback()
        persist_organization_webhook(db, event('evt_same', '2026-01-02T10:00:00', 'REFUNDED'))
        db.commit()
        assert payment.status == 'VERIFICAR'
