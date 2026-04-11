"""Criação de cobranças na API Asaas (POST /v3/payments)."""

from __future__ import annotations

from datetime import date
from typing import Any

import httpx


def create_asaas_payment(
    *,
    base_url: str,
    api_key: str,
    customer_id: str,
    billing_type: str,
    value: float,
    due_date: date,
    description: str | None = None,
    external_reference: str | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    url = base_url.rstrip("/") + "/v3/payments"
    payload: dict[str, Any] = {
        "customer": customer_id.strip(),
        "billingType": billing_type.strip().upper(),
        "value": round(float(value), 2),
        "dueDate": due_date.isoformat(),
    }
    if description and description.strip():
        payload["description"] = description.strip()[:500]
    if external_reference and str(external_reference).strip():
        payload["externalReference"] = str(external_reference).strip()[:200]

    headers = {
        "access_token": api_key,
        "Content-Type": "application/json",
        "User-Agent": "SiSFarm/AsaasIntegration",
    }
    try:
        with httpx.Client(timeout=60.0) as client:
            response = client.post(url, headers=headers, json=payload)
    except httpx.HTTPError as exc:
        return None, f"Erro de rede ao contatar o Asaas: {exc}"

    try:
        data = response.json()
    except Exception:
        return None, f"Resposta inválida do Asaas (HTTP {response.status_code})."

    if response.status_code >= 400:
        errors = data.get("errors") if isinstance(data, dict) else None
        if isinstance(errors, list) and errors:
            parts: list[str] = []
            for item in errors[:8]:
                if isinstance(item, dict):
                    parts.append(str(item.get("description") or item.get("code") or item))
                else:
                    parts.append(str(item))
            return None, "; ".join(parts) if parts else f"Erro Asaas (HTTP {response.status_code})."
        return None, f"Erro Asaas (HTTP {response.status_code})."

    if not isinstance(data, dict):
        return None, "Resposta inesperada do Asaas."
    return data, None


def get_asaas_payment(
    *,
    base_url: str,
    api_key: str,
    payment_id: str,
) -> tuple[dict[str, Any] | None, str | None]:
    """GET /v3/payments/{id} — usado para polling quando o webhook ainda não sincronizou."""
    pid = (payment_id or "").strip()
    if not pid:
        return None, "ID da cobrança inválido."
    url = base_url.rstrip("/") + f"/v3/payments/{pid}"
    headers = {
        "access_token": api_key,
        "User-Agent": "SiSFarm/AsaasIntegration",
    }
    try:
        with httpx.Client(timeout=45.0) as client:
            response = client.get(url, headers=headers)
    except httpx.HTTPError as exc:
        return None, f"Erro de rede ao contatar o Asaas: {exc}"

    try:
        data = response.json()
    except Exception:
        return None, f"Resposta inválida do Asaas (HTTP {response.status_code})."

    if response.status_code >= 400:
        return None, f"Erro Asaas (HTTP {response.status_code})."

    if not isinstance(data, dict):
        return None, "Resposta inesperada do Asaas."
    return data, None


def is_asaas_status_paid(status: str | None) -> bool:
    if not status:
        return False
    return status.strip().upper() in {"RECEIVED", "CONFIRMED"}
