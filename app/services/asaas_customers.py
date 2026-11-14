"""Validação e montagem de payload de cliente para API Asaas (POST /v3/customers)."""

from __future__ import annotations

import re
from typing import Any

import httpx
from email_validator import EmailNotValidError, validate_email


def asaas_digits(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\D", "", value)


def validate_asaas_customer_form(
    *,
    name: str,
    cpf_cnpj: str,
    email: str,
    mobile_phone: str,
    postal_code: str,
    address_number: str,
    person_type: str,
    phone: str | None = None,
) -> list[str]:
    errors: list[str] = []
    name = (name or "").strip()
    if len(name) < 2:
        errors.append("Informe o nome completo ou a razão social (mínimo 2 caracteres).")

    doc = asaas_digits(cpf_cnpj)
    pt = (person_type or "FISICA").strip().upper()
    if pt == "JURIDICA":
        if len(doc) != 14:
            errors.append("CNPJ deve conter 14 dígitos.")
    else:
        if len(doc) != 11:
            errors.append("CPF deve conter 11 dígitos.")

    email_clean = (email or "").strip()
    if not email_clean:
        errors.append("E-mail é obrigatório para notificações de cobrança.")
    else:
        try:
            validate_email(email_clean, check_deliverability=False)
        except EmailNotValidError:
            errors.append("E-mail inválido.")

    mobile = asaas_digits(mobile_phone)
    if len(mobile) not in (10, 11):
        errors.append("Celular deve ter DDD + número (10 ou 11 dígitos).")

    cep = asaas_digits(postal_code)
    if len(cep) != 8:
        errors.append("CEP deve ter 8 dígitos.")

    if not (address_number or "").strip():
        errors.append("Número do endereço é obrigatório.")

    if phone and asaas_digits(phone) and len(asaas_digits(phone)) not in (10, 11):
        errors.append("Telefone fixo: informe DDD + número (10 ou 11 dígitos) ou deixe em branco.")

    return errors


def build_asaas_customer_payload(
    *,
    name: str,
    cpf_cnpj_digits: str,
    email: str,
    mobile_phone_digits: str,
    postal_code_digits: str,
    address_number: str,
    complement: str | None = None,
    province: str | None = None,
    address: str | None = None,
    phone_digits: str | None = None,
    company: str | None = None,
    external_reference: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": (name or "").strip(),
        "cpfCnpj": cpf_cnpj_digits,
        "email": (email or "").strip(),
        "mobilePhone": mobile_phone_digits,
        "postalCode": postal_code_digits,
        "addressNumber": (address_number or "").strip(),
    }
    if complement and complement.strip():
        payload["complement"] = complement.strip()[:255]
    if province and province.strip():
        payload["province"] = province.strip()
    if address and address.strip():
        payload["address"] = address.strip()
    if phone_digits and len(phone_digits) >= 10:
        payload["phone"] = phone_digits
    if company and company.strip():
        payload["company"] = company.strip()
    if external_reference and str(external_reference).strip():
        payload["externalReference"] = str(external_reference).strip()[:200]
    return payload


def create_asaas_customer(
    *,
    base_url: str,
    api_key: str,
    payload: dict[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    url = base_url.rstrip("/") + "/v3/customers"
    headers = {
        "access_token": api_key,
        "Content-Type": "application/json",
        "User-Agent": "SiSFarm/AsaasIntegration",
    }
    try:
        with httpx.Client(timeout=45.0) as client:
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


def find_asaas_customers_by_document(*, base_url: str, api_key: str, cpf_cnpj: str) -> tuple[list[dict] | None, str | None]:
    """Fail closed when a search is incomplete, so callers never create from partial results."""
    customers: list[dict] = []
    try:
        with httpx.Client(timeout=20.0) as client:
            for offset in range(0, 1000, 100):
                response = client.get(
                    base_url.rstrip('/') + '/v3/customers',
                    headers={"access_token": api_key, "User-Agent": "SiSFarm/AsaasIntegration", "Accept": "application/json"},
                    params={"cpfCnpj": cpf_cnpj, "limit": 100, "offset": offset},
                )
                if response.status_code != 200:
                    return None, f"Não foi possível consultar clientes no Asaas (HTTP {response.status_code})."
                data = response.json()
                if not isinstance(data, dict) or not isinstance(data.get('data'), list) or not isinstance(data.get('hasMore'), bool):
                    return None, "O Asaas retornou uma lista de clientes inválida."
                customers.extend(item for item in data['data'] if isinstance(item, dict) and asaas_digits(item.get('cpfCnpj')) == cpf_cnpj and item.get('id') and not item.get('deleted'))
                if not data['hasMore']:
                    return customers, None
    except (httpx.HTTPError, ValueError):
        return None, "Não foi possível consultar o Asaas. Tente novamente antes de criar ou vincular um cliente."
    return None, "A consulta ao Asaas excedeu o limite seguro de resultados."
