"""Visao consolidada de organizacoes, usuarios e fazendas para a tela /organizacoes."""

from __future__ import annotations

import unicodedata
from collections import Counter
from typing import Any

from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.timezone import format_app_datetime, today_in_app_timezone
from app.models import Plot
from app.services.entitlements import compute_entitlements


def normalize_search_text(value: str | None) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""
    decomposed = unicodedata.normalize("NFD", raw)
    return "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")


def _safe_plan_summary(db: Session, org: Any, today) -> dict[str, Any]:
    if getattr(org, "slug", None) == "sisfarm":
        return {
            "plan_label": "Interno",
            "plan_tone": "brand",
            "billing_state": "ok",
            "exempt": True,
            "max_users": None,
            "max_farms": None,
            "max_hectares": None,
        }
    try:
        ent = compute_entitlements(db, org.id, today)
    except SQLAlchemyError:
        return {
            "plan_label": "Sem assinatura",
            "plan_tone": "muted",
            "billing_state": "ok",
            "exempt": False,
            "max_users": None,
            "max_farms": None,
            "max_hectares": None,
        }

    plan_label = ent.plan_name or "Sem assinatura"
    plan_tone = "muted"
    if ent.exempt:
        plan_label = "Interno"
        plan_tone = "brand"
    elif ent.billing_state == "teste":
        days_left = max((ent.trial_ends_on - today).days, 0) if ent.trial_ends_on else None
        plan_label = f"Teste grátis · {days_left}d" if days_left is not None else "Teste grátis"
        plan_tone = "info"
    elif ent.billing_state == "liberado":
        plan_label = ent.plan_name or "Liberação manual"
        plan_tone = "info"
    elif ent.billing_state == "restrito":
        plan_label = f"{ent.plan_name} · Restrita" if ent.plan_name else "Sem assinatura"
        plan_tone = "err" if ent.plan_name else "muted"
    elif ent.billing_state == "aviso":
        plan_label = f"{ent.plan_name or 'Assinatura'} · Atraso"
        plan_tone = "warn"
    elif ent.plan_name:
        plan_tone = "ok"

    return {
        "plan_label": plan_label,
        "plan_tone": plan_tone,
        "billing_state": ent.billing_state,
        "exempt": bool(ent.exempt),
        "max_users": ent.max_users,
        "max_farms": ent.max_farms,
        "max_hectares": float(ent.max_hectares) if ent.max_hectares is not None else None,
    }


def _safe_plot_counts(db: Session) -> dict[int, int]:
    try:
        rows = db.query(Plot.farm_id, func.count(Plot.id)).group_by(Plot.farm_id).all()
        return {int(farm_id): int(count) for farm_id, count in rows if farm_id is not None}
    except SQLAlchemyError:
        return {}


def _format_last_login(dt: Any) -> str:
    formatted = format_app_datetime(dt, "%d/%m/%Y %H:%M")
    return formatted if formatted else "Nunca acessou"


def build_organizations_overview(
    db: Session,
    *,
    organizations: list[Any],
    users: list[Any],
    farms: list[Any],
    primary_users: dict[int, Any],
    billing_by_organization: dict[int, Any],
    signup_suggestions: dict[int, Any],
    responsible_history: dict[int, list[dict[str, Any]]],
    blocked_user_ids: set[int],
    current_user_id: int | None,
) -> dict[str, Any]:
    today = today_in_app_timezone()
    plot_counts_by_farm = _safe_plot_counts(db)
    entitlements_by_organization = {
        org.id: _safe_plan_summary(db, org, today)
        for org in organizations
    }

    name_counter = Counter(normalize_search_text(farm.name) for farm in farms if getattr(farm, "name", None))
    duplicate_farm_names = {key: count for key, count in name_counter.items() if key and count > 1}

    orgs_payload: list[dict[str, Any]] = []
    for org in organizations:
        primary = primary_users.get(org.id)
        billing = billing_by_organization.get(org.id)
        suggestion = signup_suggestions.get(org.id)
        plan = entitlements_by_organization.get(org.id, {})
        history_items = []
        for ev in responsible_history.get(org.id, []):
            old_info = ev.get("old")
            new_info = ev.get("new")
            history_items.append({
                "date": format_app_datetime(ev.get("date"), "%d/%m/%Y %H:%M") or "Data não informada",
                "actor": ev.get("actor") or "Sistema",
                "oldLabel": f"{old_info.get('nome')} ({old_info.get('email')})" if isinstance(old_info, dict) and old_info.get("nome") else "Não definido",
                "newLabel": f"{new_info.get('nome')} ({new_info.get('email')})" if isinstance(new_info, dict) and new_info.get("nome") else "Não definido",
            })

        orgs_payload.append({
            "id": org.id,
            "name": org.name,
            "slug": org.slug,
            "isActive": bool(org.is_active),
            "isProtected": org.slug == "sisfarm",
            "primaryUserId": primary.id if primary else None,
            "suggestedPrimary": (
                {"id": suggestion.id, "name": suggestion.name, "email": suggestion.email}
                if suggestion and not primary
                else None
            ),
            "planLabel": plan.get("plan_label", "Sem assinatura"),
            "planTone": plan.get("plan_tone", "muted"),
            "billingState": plan.get("billing_state", "ok"),
            "exempt": bool(plan.get("exempt", False)),
            "maxUsers": plan.get("max_users"),
            "maxFarms": plan.get("max_farms"),
            "maxHectares": plan.get("max_hectares"),
            "billing": (
                {
                    "contactName": billing.contact_name or "",
                    "contactEmail": billing.contact_email or "",
                    "contactPhone": billing.contact_phone or "",
                    "cpfCnpj": billing.cpf_cnpj or "",
                    "postalCode": billing.postal_code or "",
                    "address": billing.address or "",
                    "addressNumber": billing.address_number or "",
                    "addressComplement": billing.address_complement or "",
                    "province": billing.province or "",
                    "city": billing.city or "",
                    "state": billing.state or "",
                    "asaasCustomerId": billing.asaas_customer_id or "",
                    "asaasEnvironment": billing.asaas_environment or "",
                }
                if billing
                else None
            ),
            "history": history_items,
        })

    users_payload: list[dict[str, Any]] = []
    for item in users:
        users_payload.append({
            "id": item.id,
            "name": item.name,
            "email": item.email,
            "orgId": item.organization_id,
            "isActive": bool(item.is_active),
            "isAdmin": bool(item.is_admin),
            "isBlocked": item.id in blocked_user_ids,
            "jobTitle": (item.job_title or "").strip(),
            "lastLogin": _format_last_login(getattr(item, "last_login_at", None)),
            "activeFarmId": getattr(item, "active_farm_id", None),
        })

    farms_payload: list[dict[str, Any]] = []
    for farm in farms:
        norm_name = normalize_search_text(farm.name)
        farms_payload.append({
            "id": farm.id,
            "name": farm.name,
            "location": farm.location or "",
            "area": float(farm.total_area or 0),
            "plots": plot_counts_by_farm.get(farm.id, 0),
            "orgId": farm.organization_id,
            "paused": bool(getattr(farm, "operations_paused", False)),
            "car": (getattr(farm, "car_registration_number", None) or "").strip(),
            "duplicateCount": duplicate_farm_names.get(norm_name, 1),
        })

    return {
        "plot_counts_by_farm": plot_counts_by_farm,
        "entitlements_by_organization": entitlements_by_organization,
        "duplicate_farm_names": duplicate_farm_names,
        "payload": {
            "currentUserId": current_user_id,
            "organizations": orgs_payload,
            "users": users_payload,
            "farms": farms_payload,
        },
    }
