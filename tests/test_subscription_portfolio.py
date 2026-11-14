from datetime import date
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.models  # noqa: F401
from app.db.base import Base
from app.models import (
    Organization,
    OrganizationBilling,
    OrganizationCardMigration,
    OrganizationSubscription,
    OrganizationSubscriptionPayment,
)
from app.services.subscription_portfolio import build_subscription_portfolio

TODAY = date(2026, 11, 14)


def _db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    return Session(engine)


def _org(db, name):
    org = Organization(name=name, slug=name.lower(), is_active=True)
    db.add(org)
    db.flush()
    db.add(OrganizationBilling(organization_id=org.id, contact_name='Pessoa', contact_email=f'{org.slug}@example.com', asaas_customer_id=f'cus_{org.slug}'))
    return org


def _sub(db, org, *, amount='100', cycle='MONTHLY', asaas_status='ACTIVE', status='criada', is_current=True):
    sub = OrganizationSubscription(
        organization_id=org.id, plan_name='Plano', amount=Decimal(amount), cycle=cycle, billing_type='PIX',
        first_due_date=date(2026, 1, 1), next_due_date=date(2026, 12, 1), status=status, asaas_status=asaas_status,
        environment='sandbox', is_current=is_current,
    )
    db.add(sub)
    db.flush()
    return sub


def _pay(db, sub, ref, status, due, amount='100', operation_state=None):
    db.add(OrganizationSubscriptionPayment(subscription_id=sub.id, asaas_payment_id=f'pay_{ref}', status=status, amount=Decimal(amount), due_date=due, operation_state=operation_state))


def _by_name(portfolio):
    return {row.organization.name: row for row in portfolio.rows}


def test_portfolio_totals_and_monthly_revenue():
    with _db() as db:
        monthly, yearly, suspended = _org(db, 'Mensal'), _org(db, 'Anual'), _org(db, 'Suspensa')
        _sub(db, monthly, amount='99.90')
        _sub(db, yearly, amount='1200', cycle='YEARLY')
        _sub(db, suspended, asaas_status='INACTIVE')
        db.commit()
        portfolio = build_subscription_portfolio(db, [monthly, yearly, suspended], TODAY)
    assert portfolio.monthly_recurring_revenue == Decimal('199.90')
    assert portfolio.active_count == 2
    assert _by_name(portfolio)['Suspensa'].situation == 'suspensa'
    assert _by_name(portfolio)['Mensal'].situation == 'ativa'


def test_overdue_includes_late_pending_and_previous_subscriptions():
    with _db() as db:
        org = _org(db, 'Devedora')
        old = _sub(db, org, is_current=False, asaas_status='DELETED', status='encerrada')
        current = _sub(db, org)
        _pay(db, old, 'old', 'OVERDUE', date(2026, 9, 10), amount='80')
        _pay(db, current, 'late', 'PENDING', date(2026, 11, 1))
        _pay(db, current, 'future', 'PENDING', date(2026, 12, 1))
        _pay(db, current, 'paid', 'RECEIVED', date(2026, 10, 1))
        db.commit()
        portfolio = build_subscription_portfolio(db, [org], TODAY)
    row = portfolio.rows[0]
    assert row.situation == 'inadimplente'
    assert row.overdue_count == 2 and row.overdue_amount == Decimal('180.00')
    assert row.oldest_overdue_due_date == date(2026, 9, 10) and row.days_overdue == 65
    assert portfolio.delinquent_count == 1 and portfolio.overdue_total == Decimal('180.00')


def test_review_situations_take_priority_and_sort_first():
    with _db() as db:
        ok, pending, migration, payment_check = _org(db, 'Aok'), _org(db, 'Revisao'), _org(db, 'Migracao'), _org(db, 'Cobranca')
        _sub(db, ok)
        _sub(db, pending, status='revisao')
        old = _sub(db, migration)
        db.add(OrganizationCardMigration(organization_id=migration.id, old_subscription_id=old.id, status='revisao', is_open=True))
        checked = _sub(db, payment_check)
        _pay(db, checked, 'chk', 'OVERDUE', date(2026, 10, 1), operation_state='verificar')
        db.commit()
        portfolio = build_subscription_portfolio(db, [ok, pending, migration, payment_check], TODAY)
    rows = _by_name(portfolio)
    assert {rows[name].situation for name in ('Revisao', 'Migracao', 'Cobranca')} == {'verificar'}
    assert portfolio.review_count == 3
    assert portfolio.rows[-1].organization.name == 'Aok'


def test_organizations_without_customer_or_subscription():
    with _db() as db:
        linked = _org(db, 'Vinculada')
        bare = Organization(name='Nova', slug='nova', is_active=True)
        db.add(bare)
        db.commit()
        portfolio = build_subscription_portfolio(db, [linked, bare], TODAY)
    rows = _by_name(portfolio)
    assert rows['Vinculada'].situation == 'sem_assinatura'
    assert rows['Nova'].situation == 'sem_cliente' and not rows['Nova'].has_customer
    assert portfolio.without_subscription_count == 2
    assert portfolio.monthly_recurring_revenue == Decimal('0.00')


def test_empty_organization_list():
    with _db() as db:
        portfolio = build_subscription_portfolio(db, [], TODAY)
    assert portfolio.rows == [] and portfolio.active_count == 0


def test_pending_card_contract_shows_awaiting_card():
    with _db() as db:
        waiting, broken = _org(db, 'Aguardando'), _org(db, 'Quebrado')
        db.add(OrganizationCardMigration(organization_id=waiting.id, old_subscription_id=None, status='aguardando', is_open=True))
        db.add(OrganizationCardMigration(organization_id=broken.id, old_subscription_id=None, status='revisao', is_open=True))
        db.commit()
        portfolio = build_subscription_portfolio(db, [waiting, broken], TODAY)
    rows = _by_name(portfolio)
    assert rows['Aguardando'].situation == 'aguardando_cartao' and rows['Aguardando'].situation_label == 'Aguardando cartão'
    assert rows['Quebrado'].situation == 'verificar'
