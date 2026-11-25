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
from app.services.asaas_environment import app_public_base, asaas_env_name
from app.services.billing_environment import MERCADOPAGO_TEST, billing_environment
from app.services.email_service import send_plain_email
from app.services.entitlements import compute_entitlements, grace_days

logger = logging.getLogger(__name__)
BUSINESS_HOURS = range(8, 20)


def _money(value) -> str:
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _oldest_overdue_charge(db, organization_id: int, today: date) -> OrganizationSubscriptionPayment | None:
    ids = [row[0] for row in db.query(OrganizationSubscription.id).filter_by(organization_id=organization_id, environment=asaas_env_name()).all()]
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


def compose_notice(kind: str, *, organization: Organization, charge: OrganizationSubscriptionPayment, entitlements) -> tuple[str, str]:
    link = f"{app_public_base()}/minha-assinatura"
    value = f"R$ {_money(entitlements.overdue_amount)}"
    due = charge.due_date.strftime("%d/%m/%Y") if charge.due_date else "-"
    pay = charge.invoice_url or link
    if kind == "pre_restricao":
        subject = f"SiSFarm: regularize até {entitlements.regularize_until.strftime('%d/%m/%Y')} para manter o acesso completo"
        body = (f"Olá,\n\nA assinatura SiSFarm de {organization.name} tem {value} em aberto desde {due}.\n"
                f"Se o pagamento não for identificado até {entitlements.regularize_until.strftime('%d/%m/%Y')}, o sistema passa para o modo somente leitura: "
                "os dados continuam disponíveis para consulta, mas não será possível cadastrar ou editar.\n\n"
                f"Pagar agora: {pay}\nSua assinatura: {link}\n\nSe você já pagou, desconsidere esta mensagem; a confirmação é automática.\n\nEquipe SiSFarm")
    else:
        restricted = entitlements.read_only
        subject = "SiSFarm: fatura em atraso" + (" (modo somente leitura ativo)" if restricted else "")
        body = (f"Olá,\n\nNão identificamos o pagamento da fatura SiSFarm de {organization.name}, com vencimento em {due} ({value} em aberto).\n"
                + ("O sistema está em modo somente leitura até a regularização.\n" if restricted else
                   f"Para evitar o modo somente leitura, regularize até {entitlements.regularize_until.strftime('%d/%m/%Y')}.\n")
                + f"\nPagar agora: {pay}\nSua assinatura: {link}\n\nSe você já pagou, desconsidere esta mensagem; a confirmação é automática.\n\nEquipe SiSFarm")
    return subject, body


def process_organization(db, organization: Organization, today: date, *, send=send_plain_email) -> list[str]:
    entitlements = compute_entitlements(db, organization.id, today)
    if entitlements.exempt or entitlements.billing_state not in {"aviso", "restrito"}:
        return []
    charge = _oldest_overdue_charge(db, organization.id, today)
    billing = db.query(OrganizationBilling).filter_by(organization_id=organization.id).first()
    if not charge or not billing or not billing.contact_email:
        return []
    sent = {row.kind for row in db.query(SubscriptionNotice.kind).filter_by(payment_id=charge.id).all()}
    delivered = []
    to_send, to_skip = notices_due(entitlements.days_overdue, grace_days(), sent)
    for kind in to_skip:
        db.add(SubscriptionNotice(organization_id=organization.id, payment_id=charge.id, kind=kind, recipient=billing.contact_email, outcome="pulado"))
    for kind in to_send:
        subject, body = compose_notice(kind, organization=organization, charge=charge, entitlements=entitlements)
        notice = SubscriptionNotice(organization_id=organization.id, payment_id=charge.id, kind=kind, recipient=billing.contact_email, outcome="enviando")
        db.add(notice)
        try:
            db.flush()  # reserva o envio: a restricao unica impede duplicidade entre processos
        except IntegrityError:
            db.rollback()
            continue
        try:
            send(billing.contact_email, subject, body, "aviso de cobrança")
            notice.outcome = "enviado"
            delivered.append(kind)
        except Exception:
            logger.exception("Falha ao enviar aviso de cobrança %s da organização %s", kind, organization.id)
            db.delete(notice)  # permite nova tentativa no proximo ciclo
        db.commit()
    return delivered


def compose_trial_notice(kind: str, *, organization: Organization, ends_on: date) -> tuple[str, str]:
    link = f"{app_public_base()}/planos-disponiveis"
    if kind == "teste_3_dias":
        return (f"Seu teste grátis do SiSFarm termina em {ends_on.strftime('%d/%m/%Y')}",
                f"Olá!\n\nO teste grátis da {organization.name} no SiSFarm termina em {ends_on.strftime('%d/%m/%Y')}.\n"
                f"Para continuar usando sem interrupção, contrate seu plano:\n{link}\n\n"
                "Seus cadastros continuam guardados. Qualquer dúvida, é só responder este e-mail.\n")
    return ("Seu teste grátis do SiSFarm terminou",
            f"Olá!\n\nO teste grátis da {organization.name} no SiSFarm terminou em {ends_on.strftime('%d/%m/%Y')}.\n"
            "O sistema segue disponível para consulta e nenhum dado foi apagado.\n"
            f"Para voltar a cadastrar e editar, contrate seu plano:\n{link}\n")


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
            subject = f"Confira os limites do plano contratado pela {organization.name}"
            transition = (f"O teste termina em {grant.ends_on.strftime('%d/%m/%Y')}."
                          if today <= grant.ends_on else "Os limites do plano contratado já estão em vigor.")
            body = (
                f"Olá!\n\n{transition} Há cadastros acima dos limites do plano {paid_plan.name}. "
                "Consulte os excedentes em Minha assinatura para ajustar seus cadastros ou escolher outro plano.\n\n"
                f"{app_public_base()}/minha-assinatura#regularizacao-plano"
            )
        elif db.query(OrganizationSubscription.id).filter_by(organization_id=grant.organization_id, is_current=True).first():
            continue  # Contratação pendente: o fluxo de cobrança já informa a situação.
        else:
            subject, body = compose_trial_notice(kind, organization=organization, ends_on=grant.ends_on)
        try:
            for email in recipients:
                send(email, subject, body, "aviso de teste grátis")
        except Exception:
            logger.exception("Falha ao enviar aviso de teste %s da organização %s", kind, grant.organization_id)
            continue
        grant.notices_sent = ",".join(filter(None, [grant.notices_sent, kind]))
        db.commit()
        delivered.append((grant.organization_id, kind))
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
    if billing_environment() == MERCADOPAGO_TEST:
        return
    with SessionLocal() as db:
        organization_ids = [row[0] for row in db.query(OrganizationSubscription.organization_id).filter_by(environment=asaas_env_name()).distinct().all()]
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
