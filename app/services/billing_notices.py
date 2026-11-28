"""E-mails de atraso ao responsavel financeiro (os lembretes de vencimento ficam com o Asaas).

Envia, por cobranca vencida mais antiga da organizacao:
- "atraso": no 1o dia de atraso;
- "pre_restricao": 2 dias antes do modo somente leitura.
Cada tipo e registrado em SubscriptionNotice e nunca se repete. Organizacoes com liberacao manual,
isentas ou sem e-mail de cobranca nao recebem avisos. Envia apenas em horario comercial.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

from sqlalchemy.exc import IntegrityError

from app.core.timezone import app_now, today_in_app_timezone
from app.db.session import SessionLocal
from app.models import Organization, OrganizationBilling, OrganizationSubscription, OrganizationSubscriptionPayment, SubscriptionNotice
from app.services.asaas_environment import app_public_base
from app.services.billing_environment import MERCADOPAGO_LIVE, MERCADOPAGO_TEST, billing_environment
from app.services.email_service import (
    build_billing_notice_email,
    build_pix_renewal_reminder_email,
    build_plan_limits_notice_email,
    build_trial_notice_email,
    dispatch_email,
    send_plain_email,
)
from app.services.entitlements import compute_entitlements, grace_days
from app.services.plan_changes import PAID, latest_settled_subscription_payment

logger = logging.getLogger(__name__)
BUSINESS_HOURS = range(8, 20)


def _money(value) -> str:
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _oldest_overdue_charge(db, organization_id: int, today: date) -> OrganizationSubscriptionPayment | None:
    environment = billing_environment()
    subscriptions = db.query(OrganizationSubscription.id).filter_by(organization_id=organization_id, environment=environment)
    if environment == MERCADOPAGO_LIVE:
        subscriptions = subscriptions.filter_by(is_current=True)
    ids = [row[0] for row in subscriptions.all()]
    if not ids:
        return None
    return (
        db.query(OrganizationSubscriptionPayment)
        .filter(
            OrganizationSubscriptionPayment.subscription_id.in_(ids),
            (OrganizationSubscriptionPayment.status == "OVERDUE")
            | ((OrganizationSubscriptionPayment.status == "PENDING") & (OrganizationSubscriptionPayment.due_date < today)),
        )
        .order_by(OrganizationSubscriptionPayment.due_date)
        .first()
    )


def notices_due(days_overdue: int, grace: int, already_sent: set[str]) -> tuple[list[str], list[str]]:
    """(tipos a enviar, tipos a marcar como pulados). Nunca dois e-mails seguidos sobre a mesma fatura."""
    if days_overdue <= 0:
        return [], []
    in_pre_window = grace >= 2 and grace - 1 <= days_overdue <= grace
    if in_pre_window and "pre_restricao" not in already_sent:
        # O aviso de prazo ja informa o atraso; o aviso simples deixa de ser enviado.
        return ["pre_restricao"], ([] if "atraso" in already_sent else ["atraso"])
    if "atraso" not in already_sent:
        # Depois da tolerancia, o aviso de atraso informa que o modo somente leitura esta ativo.
        return ["atraso"], ([] if days_overdue < grace - 1 or "pre_restricao" in already_sent else ["pre_restricao"])
    return [], []


def _compose_notice_parts(kind: str, *, organization: Organization, charge: OrganizationSubscriptionPayment, entitlements, renewal_pending: bool = False) -> tuple[str, str, str]:
    link = f"{app_public_base()}/minha-assinatura"
    deadline = entitlements.regularize_until.strftime("%d/%m/%Y") if entitlements.regularize_until else "o fim do prazo"
    value = f"R$ {_money(entitlements.overdue_amount)}" if not renewal_pending else None
    due = (charge.due_date.strftime("%d/%m/%Y") if charge.due_date else "-") if not renewal_pending else None
    pay = (charge.invoice_url or link) if not renewal_pending else None
    return build_billing_notice_email(
        kind,
        organization_name=organization.name,
        deadline_str=deadline,
        link=link,
        renewal_pending=renewal_pending,
        read_only=bool(entitlements.read_only),
        value_str=value,
        due_str=due,
        pay_url=pay,
    )


def compose_notice(kind: str, *, organization: Organization, charge: OrganizationSubscriptionPayment, entitlements, renewal_pending: bool = False) -> tuple[str, str]:
    subject, text_body, _ = _compose_notice_parts(
        kind, organization=organization, charge=charge, entitlements=entitlements, renewal_pending=renewal_pending
    )
    return subject, text_body


def process_organization(db, organization: Organization, today: date, *, send=send_plain_email) -> list[str]:
    entitlements = compute_entitlements(db, organization.id, today)
    if entitlements.exempt or entitlements.billing_state not in {"aviso", "restrito"}:
        return []
    charge = _oldest_overdue_charge(db, organization.id, today)
    renewal_pending = False
    if not charge and billing_environment() == MERCADOPAGO_LIVE and entitlements.days_overdue > 0:
        current = db.query(OrganizationSubscription).filter_by(
            organization_id=organization.id, environment=MERCADOPAGO_LIVE, is_current=True,
        ).first()
        settled = latest_settled_subscription_payment(db, current.id) if current else None
        if settled and settled.status in PAID:
            charge = settled
            renewal_pending = True
    if not charge:
        return []
    if charge.subscription.environment == MERCADOPAGO_LIVE:
        recipient = charge.subscription.mercadopago_payer_email
    else:
        billing = db.query(OrganizationBilling).filter_by(organization_id=organization.id).first()
        recipient = billing.contact_email if billing else None
    if not recipient:
        return []
    prefix = "renovacao_" if renewal_pending else ""
    sent = {row.kind.removeprefix(prefix) for row in db.query(SubscriptionNotice.kind).filter_by(payment_id=charge.id).all()
            if (not prefix and not row.kind.startswith("renovacao_")) or (prefix and row.kind.startswith(prefix))}
    delivered = []
    to_send, to_skip = notices_due(entitlements.days_overdue, grace_days(), sent)
    for kind in to_skip:
        db.add(SubscriptionNotice(organization_id=organization.id, payment_id=charge.id, kind=prefix + kind, recipient=recipient, outcome="pulado"))
    for kind in to_send:
        stored_kind = prefix + kind
        subject, body, html_body = _compose_notice_parts(stored_kind, organization=organization, charge=charge, entitlements=entitlements, renewal_pending=renewal_pending)
        notice = SubscriptionNotice(organization_id=organization.id, payment_id=charge.id, kind=stored_kind, recipient=recipient, outcome="enviando")
        db.add(notice)
        try:
            db.flush()  # reserva o envio: a restricao unica impede duplicidade entre processos
        except IntegrityError:
            db.rollback()
            continue
        try:
            dispatch_email(send, recipient, subject, body, "aviso de cobrança", html_body=html_body)
            notice.outcome = "enviado"
            delivered.append(stored_kind)
        except Exception:
            logger.exception("Falha ao enviar aviso de cobrança %s da organização %s", stored_kind, organization.id)
            db.delete(notice)  # permite nova tentativa no proximo ciclo
        db.commit()
    return delivered


def _compose_trial_notice_parts(kind: str, *, organization: Organization, ends_on: date) -> tuple[str, str, str]:
    link = f"{app_public_base()}/planos-disponiveis"
    return build_trial_notice_email(
        kind,
        organization_name=organization.name,
        ends_on_str=ends_on.strftime("%d/%m/%Y"),
        link=link,
    )


def compose_trial_notice(kind: str, *, organization: Organization, ends_on: date) -> tuple[str, str]:
    subject, text_body, _ = _compose_trial_notice_parts(kind, organization=organization, ends_on=ends_on)
    return subject, text_body


def trial_notice_kind(ends_on: date, today: date, already_sent: str) -> str | None:
    """Um aviso 3 dias antes e outro no fim do teste (ate 3 dias depois, para cobrir quedas do servico)."""
    sent = set(filter(None, (already_sent or "").split(",")))
    days_left = (ends_on - today).days
    if days_left < 0 and days_left >= -3 and "teste_fim" not in sent:
        return "teste_fim"
    if 0 <= days_left <= 3 and "teste_3_dias" not in sent:
        return "teste_3_dias"
    return None


def process_trial_notices(db, today: date, *, send=send_plain_email) -> list[tuple[int, str]]:
    from app.models import OrganizationAccessGrant, User
    from app.services.access_grants import TRIAL_GRANT_REASON
    from app.services.plan_usage import confirmed_plan_for_usage, plan_usage

    delivered = []
    grants = db.query(OrganizationAccessGrant).filter(
        OrganizationAccessGrant.reason == TRIAL_GRANT_REASON, OrganizationAccessGrant.revoked_at.is_(None),
        OrganizationAccessGrant.ends_on >= today - timedelta(days=3), OrganizationAccessGrant.ends_on <= today + timedelta(days=3),
    ).all()
    for grant in grants:
        kind = trial_notice_kind(grant.ends_on, today, grant.notices_sent)
        if not kind:
            continue
        organization = db.get(Organization, grant.organization_id)
        recipients = [row[0] for row in db.query(User.email).filter_by(organization_id=grant.organization_id, is_admin=True, is_active=True)]
        if not organization or not recipients:
            continue
        paid_plan = confirmed_plan_for_usage(db, grant.organization_id, today)
        if paid_plan:
            usage = plan_usage(db, grant.organization_id, paid_plan)
            if not usage.exceeds:
                continue
            transition = (f"O teste termina em {grant.ends_on.strftime('%d/%m/%Y')}."
                          if today <= grant.ends_on else "Os limites do plano contratado já estão em vigor.")
            subject, body, html_body = build_plan_limits_notice_email(
                organization_name=organization.name,
                plan_name=paid_plan.name,
                transition_text=transition,
                link=f"{app_public_base()}/minha-assinatura#regularizacao-plano",
            )
        elif db.query(OrganizationSubscription.id).filter_by(
            organization_id=grant.organization_id, environment=billing_environment(), is_current=True,
        ).first():
            continue  # Contratação pendente: o fluxo de cobrança já informa a situação.
        else:
            subject, body, html_body = _compose_trial_notice_parts(kind, organization=organization, ends_on=grant.ends_on)
        try:
            for email in recipients:
                dispatch_email(send, email, subject, body, "aviso de teste grátis", html_body=html_body)
        except Exception:
            logger.exception("Falha ao enviar aviso de teste %s da organização %s", kind, grant.organization_id)
            continue
        grant.notices_sent = ",".join(filter(None, [grant.notices_sent, kind]))
        db.commit()
        delivered.append((grant.organization_id, kind))
    return delivered


def pix_renewal_reminder_kind(next_due: date, today: date, already_sent: set[str]) -> str | None:
    """Lembretes de renovação para assinaturas Pix (sem recorrência automática): 7d, 3d e no dia."""
    days_left = (next_due - today).days
    if days_left == 0 and "pix_renovacao_hoje" not in already_sent:
        return "pix_renovacao_hoje"
    if 1 <= days_left <= 3 and "pix_renovacao_3d" not in already_sent:
        return "pix_renovacao_3d"
    if 4 <= days_left <= 7 and "pix_renovacao_7d" not in already_sent:
        return "pix_renovacao_7d"
    return None


def _compose_pix_renewal_reminder_parts(kind: str, *, organization: Organization, subscription: OrganizationSubscription) -> tuple[str, str, str]:
    link = f"{app_public_base()}/minha-assinatura"
    due = subscription.next_due_date.strftime("%d/%m/%Y") if subscription.next_due_date else "-"
    value = f"R$ {_money(subscription.amount)}"
    return build_pix_renewal_reminder_email(
        kind,
        organization_name=organization.name,
        plan_name=subscription.plan_name,
        due_str=due,
        value_str=value,
        link=link,
    )


def compose_pix_renewal_reminder(kind: str, *, organization: Organization, subscription: OrganizationSubscription) -> tuple[str, str]:
    subject, text_body, _ = _compose_pix_renewal_reminder_parts(kind, organization=organization, subscription=subscription)
    return subject, text_body


def process_pix_renewal_reminders(db, today: date, *, send=send_plain_email) -> list[tuple[int, str]]:
    """Envia lembretes antecipados de renovação para assinaturas Pix ativas em produção."""
    if billing_environment() != MERCADOPAGO_LIVE:
        return []
    subscriptions = (
        db.query(OrganizationSubscription)
        .filter(
            OrganizationSubscription.environment == MERCADOPAGO_LIVE,
            OrganizationSubscription.is_current.is_(True),
            OrganizationSubscription.mercadopago_status == "authorized",
            OrganizationSubscription.mercadopago_reference.like("sisfarm-pix-live-%"),
            OrganizationSubscription.next_due_date.isnot(None),
            OrganizationSubscription.next_due_date >= today,
            OrganizationSubscription.next_due_date <= today + timedelta(days=7),
        )
        .all()
    )
    delivered: list[tuple[int, str]] = []
    for subscription in subscriptions:
        recipient = (subscription.mercadopago_payer_email or "").strip()
        if not recipient:
            continue
        organization = db.get(Organization, subscription.organization_id)
        if not organization or organization.archived_at or not organization.is_active:
            continue
        settled = latest_settled_subscription_payment(db, subscription.id)
        if not settled or settled.status not in PAID:
            continue
        sent = {row.kind for row in db.query(SubscriptionNotice.kind).filter_by(payment_id=settled.id).all()}
        kind = pix_renewal_reminder_kind(subscription.next_due_date, today, sent)
        if not kind:
            continue
        subject, body, html_body = _compose_pix_renewal_reminder_parts(kind, organization=organization, subscription=subscription)
        notice = SubscriptionNotice(
            organization_id=organization.id, payment_id=settled.id, kind=kind, recipient=recipient, outcome="enviando",
        )
        db.add(notice)
        try:
            db.flush()
        except IntegrityError:
            db.rollback()
            continue
        try:
            dispatch_email(send, recipient, subject, body, "lembrete de renovação Pix", html_body=html_body)
            notice.outcome = "enviado"
            delivered.append((organization.id, kind))
        except Exception:
            logger.exception("Falha ao enviar lembrete Pix %s da organização %s", kind, organization.id)
            db.delete(notice)
        db.commit()
    return delivered


def _tick() -> None:
    if app_now().hour not in BUSINESS_HOURS:
        return
    today = today_in_app_timezone()
    with SessionLocal() as db:
        try:
            process_trial_notices(db, today)
        except Exception:
            db.rollback()
            logger.exception("Falha ao processar avisos de teste grátis")
    environment = billing_environment()
    if environment == MERCADOPAGO_TEST:
        return
    if environment == MERCADOPAGO_LIVE:
        with SessionLocal() as db:
            try:
                process_pix_renewal_reminders(db, today)
            except Exception:
                db.rollback()
                logger.exception("Falha ao processar lembretes de renovação Pix")
    with SessionLocal() as db:
        organization_ids = [row[0] for row in db.query(OrganizationSubscription.organization_id).filter_by(environment=environment).distinct().all()]
    for organization_id in organization_ids:
        with SessionLocal() as db:
            organization = db.get(Organization, organization_id)
            if not organization:
                continue
            try:
                process_organization(db, organization, today)
            except Exception:
                db.rollback()
                logger.exception("Falha ao processar avisos de cobrança da organização %s", organization_id)


async def run_billing_notice_loop(interval_seconds: int = 3600) -> None:
    while True:
        try:
            await asyncio.to_thread(_tick)
        except Exception:
            logger.exception("Falha no ciclo de avisos de cobrança")
        await asyncio.sleep(interval_seconds)
