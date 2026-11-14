from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.db.base import Base
from app.models import Organization, OrganizationBilling, OrganizationSubscription, OrganizationSubscriptionPayment
from app.web import routes


def test_subscription_page_restricts_data_to_selected_organization(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, '_base_context', lambda *args, **kwargs: kwargs)
    monkeypatch.setattr(routes.templates, 'TemplateResponse', lambda name, context: (name, context))
    with Session(engine) as db:
        first = Organization(name='Primeira', slug='primeira', is_active=True)
        second = Organization(name='Segunda', slug='segunda', is_active=True)
        db.add_all([first, second]); db.flush()
        db.add(OrganizationBilling(organization_id=first.id, contact_name='Pessoa', contact_email='a@example.com'))
        def sub(org, remote):
            return OrganizationSubscription(organization_id=org.id, plan_name='Plano', amount=Decimal('10'), cycle='MONTHLY', billing_type='PIX', first_due_date=date(2026, 1, 1), environment='sandbox', asaas_subscription_id=remote)
        first_sub, second_sub = sub(first, 'sub_first'), sub(second, 'sub_second')
        db.add_all([first_sub, second_sub]); db.flush()
        db.add_all([OrganizationSubscriptionPayment(subscription_id=first_sub.id, asaas_payment_id='pay_first', status='PENDING'), OrganizationSubscriptionPayment(subscription_id=second_sub.id, asaas_payment_id='pay_second', status='PENDING')]); db.commit()
        monkeypatch.setattr(routes, '_repository', lambda _db: SimpleNamespace(list_organizations=lambda: [first, second]))
        name, context = routes.subscriptions_page(request=SimpleNamespace(), organizacao=first.id, db=db, user=SimpleNamespace(), csrf_token='token')
        assert name == 'subscriptions.html' and context['organization'].id == first.id
        assert set(context['subscription_payments']) == {first_sub.id}
        assert second.id not in context['subscriptions_by_organization']


def test_subscription_page_requires_super_admin(monkeypatch):
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: 'acesso negado')
    result = routes.subscriptions_page(request=SimpleNamespace(), organizacao=None, db=SimpleNamespace(), user=SimpleNamespace(), csrf_token='token')
    assert result == 'acesso negado'


def test_subscription_page_shows_portfolio_without_selection(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    monkeypatch.setattr(routes, '_require_super_admin', lambda *args: None)
    monkeypatch.setattr(routes, '_base_context', lambda *args, **kwargs: kwargs)
    monkeypatch.setattr(routes.templates, 'TemplateResponse', lambda name, context: (name, context))
    with Session(engine) as db:
        first = Organization(name='Primeira', slug='primeira', is_active=True)
        second = Organization(name='Segunda', slug='segunda', is_active=True)
        db.add_all([first, second]); db.commit()
        monkeypatch.setattr(routes, '_repository', lambda _db: SimpleNamespace(list_organizations=lambda: [first, second]))
        _, context = routes.subscriptions_page(request=SimpleNamespace(), organizacao=None, db=db, user=SimpleNamespace(), csrf_token='token')
        assert context['organization'] is None
        assert [row.organization.id for row in context['portfolio'].rows] == [first.id, second.id]
