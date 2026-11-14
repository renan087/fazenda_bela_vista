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
from datetime import date

from sqlalchemy.exc import IntegrityError

from app.core.timezone import app_now, today_in_app_timezone
from app.db.session import SessionLocal
from app.models import Organization, OrganizationBilling, OrganizationSubscription, OrganizationSubscriptionPayment, SubscriptionNotice
from app.services.asaas_environment import app_public_base, asaas_env_name
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


def _tick() -> None:
    if app_now().hour not in BUSINESS_HOURS:
        return
    today = today_in_app_timezone()
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
