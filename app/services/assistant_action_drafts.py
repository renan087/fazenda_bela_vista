"""Ciclo transacional para ações do assistente; não executa alterações por conta própria."""
import json
from datetime import date
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from uuid import uuid4

from app.models.assistant_action_draft import AssistantActionDraft


def prepare_action(db, *, user, farm, season, thread_id, kind, payload):
    if not isinstance(payload, dict) or kind not in {"rainfall", "pest", "maintenance", "operation"}:
        raise ValueError("Ação ou dados inválidos.")
    if not isinstance(thread_id, str) or len(thread_id) != 32:
        raise ValueError("Conversa inválida.")
    now = datetime.now(timezone.utc)
    draft = AssistantActionDraft(id=uuid4().hex, user_id=user.id, organization_id=user.organization_id,
                                 farm_id=farm.id, season_id=season.id, thread_id=thread_id, kind=kind,
                                 payload_json=json.dumps(payload, ensure_ascii=False, allow_nan=False),
                                 status="pending", created_at=now, expires_at=now + timedelta(minutes=30))
    db.add(draft)
    db.flush()
    return draft


def claim_action(db, *, draft_id, user, farm, season, thread_id, status):
    """O chamador valida permissões e dados e confirma a transação junto com o lançamento."""
    if status not in {"confirmed", "discarded"}:
        raise ValueError("Estado inválido.")
    return db.query(AssistantActionDraft).filter_by(
        id=draft_id, user_id=user.id, organization_id=user.organization_id,
        farm_id=farm.id, season_id=season.id, thread_id=thread_id, status="pending",
    ).filter(AssistantActionDraft.expires_at > datetime.now(timezone.utc)).update(
        {"status": status}, synchronize_session=False) == 1


def validate_rainfall(data, season, today):
    try:
        day = date.fromisoformat(data["date"])
        amount = Decimal(str(data["millimeters"]))
    except (KeyError, TypeError, ValueError, InvalidOperation):
        raise ValueError("Informe uma data válida e a quantidade de chuva em milímetros.") from None
    if day > today or not season.start_date <= day <= season.end_date:
        raise ValueError("A data precisa estar na safra ativa e não pode ser futura.")
    if not amount.is_finite() or amount < 0 or amount > 9999 or amount.as_tuple().exponent < -2:
        raise ValueError("Informe uma quantidade entre 0 e 9.999 mm, com até duas casas decimais.")
    return day, amount
