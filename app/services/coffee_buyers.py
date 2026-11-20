"""Cadastro de compradores de café: validação de CPF/CNPJ, pendências para NF-e e consultas públicas."""

from __future__ import annotations

import re
import unicodedata

import httpx

from app.models.coffee_buyer import CoffeeBuyer

UF_CODES = {
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB",
    "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
}
LOOKUP_TIMEOUT = 6.0
BRASILAPI = "https://brasilapi.com.br/api"

TEXT_FIELDS = {
    "name": 180, "trade_name": 180, "state_registration": 30, "street": 180, "number": 20,
    "complement": 120, "district": 120, "city": 120, "phone": 40, "email": 180, "contact_name": 120,
}


def digits(value: object) -> str:
    return re.sub(r"\D", "", str(value or ""))


def _check_digits(base: str, weights: list[int]) -> str:
    total = sum(int(d) * w for d, w in zip(base, weights))
    rest = total % 11
    return "0" if rest < 2 else str(11 - rest)


def valid_cpf(value: str) -> bool:
    cpf = digits(value)
    if len(cpf) != 11 or cpf == cpf[0] * 11:
        return False
    first = _check_digits(cpf[:9], list(range(10, 1, -1)))
    second = _check_digits(cpf[:9] + first, list(range(11, 1, -1)))
    return cpf[-2:] == first + second


def valid_cnpj(value: str) -> bool:
    cnpj = digits(value)
    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False
    w1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    first = _check_digits(cnpj[:12], w1)
    second = _check_digits(cnpj[:12] + first, [6] + w1)
    return cnpj[-2:] == first + second


def format_document(value: str | None, person_type: str | None = None) -> str:
    d = digits(value)
    if len(d) == 14:
        return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"
    if len(d) == 11:
        return f"{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}"
    return d


def format_postal_code(value: str | None) -> str:
    d = digits(value)
    return f"{d[:5]}-{d[5:]}" if len(d) == 8 else d


def _fold(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", text).strip().lower()


def missing_for_invoice(buyer: CoffeeBuyer) -> list[str]:
    """Itens que faltam para usar o comprador como destinatário de NF-e."""
    missing = []
    pj = buyer.person_type == "pj"
    if not buyer.document:
        missing.append("CNPJ" if pj else "CPF")
    if pj and not buyer.state_registration and not buyer.ie_exempt:
        missing.append("inscrição estadual")
    if not pj and buyer.rural_producer and not buyer.state_registration:
        missing.append("IE de produtor rural")
    if not all([buyer.postal_code, buyer.street, buyer.number, buyer.district, buyer.city, buyer.state]):
        missing.append("endereço completo")
    return missing


def serialize(buyer: CoffeeBuyer, last_sale=None) -> dict:
    missing = missing_for_invoice(buyer)
    return {
        "id": buyer.id, "person_type": buyer.person_type, "name": buyer.name, "trade_name": buyer.trade_name or "",
        "document": format_document(buyer.document), "state_registration": buyer.state_registration or "",
        "ie_exempt": bool(buyer.ie_exempt), "rural_producer": bool(buyer.rural_producer),
        "postal_code": format_postal_code(buyer.postal_code), "street": buyer.street or "", "number": buyer.number or "",
        "complement": buyer.complement or "", "district": buyer.district or "", "city": buyer.city or "",
        "state": buyer.state or "", "phone": buyer.phone or "", "email": buyer.email or "",
        "contact_name": buyer.contact_name or "", "missing": missing, "invoice_ready": not missing,
        "last_sale": last_sale.strftime("%d/%m/%Y") if last_sale else "",
    }


def clean_payload(data: dict) -> tuple[dict, list[str]]:
    """Normaliza o formulário do modal. Devolve (valores, erros em linguagem simples)."""
    errors = []
    person_type = "pf" if str(data.get("person_type") or "").lower() == "pf" else "pj"
    values: dict = {"person_type": person_type}
    for field, size in TEXT_FIELDS.items():
        values[field] = (str(data.get(field) or "").strip()[:size]) or None
    values["ie_exempt"] = person_type == "pj" and str(data.get("ie_exempt") or "").lower() in {"1", "true", "on", "sim"}
    values["rural_producer"] = person_type == "pf" and str(data.get("rural_producer") or "").lower() in {"1", "true", "on", "sim"}
    if values["ie_exempt"] or (person_type == "pf" and not values["rural_producer"]):
        values["state_registration"] = None
    if person_type == "pf":
        values["trade_name"] = None
    document = digits(data.get("document"))
    if document:
        if person_type == "pj" and not valid_cnpj(document):
            errors.append("CNPJ inválido. Confira os 14 números.")
        if person_type == "pf" and not valid_cpf(document):
            errors.append("CPF inválido. Confira os 11 números.")
    values["document"] = document or None
    postal = digits(data.get("postal_code"))
    if postal and len(postal) != 8:
        errors.append("O CEP precisa ter 8 números.")
    values["postal_code"] = postal or None
    state = str(data.get("state") or "").strip().upper()
    if state and state not in UF_CODES:
        errors.append("UF inválida. Use a sigla do estado, como MG.")
    values["state"] = state or None
    if values["email"] and not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", values["email"]):
        errors.append("E-mail inválido.")
    if not values["name"]:
        errors.append("Informe o nome ou a razão social do comprador.")
    return values, errors


def find_duplicate(db, organization_id: int | None, values: dict, exclude_id: int | None = None) -> CoffeeBuyer | None:
    query = db.query(CoffeeBuyer).filter(CoffeeBuyer.organization_id == organization_id, CoffeeBuyer.active.is_(True))
    if exclude_id:
        query = query.filter(CoffeeBuyer.id != exclude_id)
    if values.get("document"):
        return query.filter(CoffeeBuyer.document == values["document"]).first()
    name = _fold(values.get("name"))
    if not name:
        return None
    return next((row for row in query.filter(CoffeeBuyer.document.is_(None)).all() if _fold(row.name) == name), None)


def search(db, organization_id: int | None, term: str | None = None, limit: int = 30) -> list[CoffeeBuyer]:
    rows = (db.query(CoffeeBuyer)
            .filter(CoffeeBuyer.organization_id == organization_id, CoffeeBuyer.active.is_(True))
            .order_by(CoffeeBuyer.name).all())
    folded, numbers = _fold(term), digits(term)
    if folded:
        rows = [r for r in rows if folded in _fold(r.name) or folded in _fold(r.trade_name)
                or (len(numbers) >= 3 and numbers in (r.document or ""))]
    return rows[:limit]


def _get_json(url: str) -> dict | None:
    try:
        response = httpx.get(url, timeout=LOOKUP_TIMEOUT, headers={"User-Agent": "SiSFarm"})
    except httpx.HTTPError:
        return None
    if response.status_code != 200:
        return None
    try:
        return response.json()
    except ValueError:
        return None


def _title(value: str | None) -> str:
    small = {"de", "da", "do", "das", "dos", "e"}
    words = (value or "").strip().lower().split()
    return " ".join(w if i and w in small else w.capitalize() for i, w in enumerate(words))


def lookup_cnpj(cnpj: str) -> dict | None:
    data = _get_json(f"{BRASILAPI}/cnpj/v1/{digits(cnpj)}")
    if not data:
        return None
    street = " ".join(filter(None, [_title(data.get("descricao_tipo_de_logradouro")), _title(data.get("logradouro"))]))
    return {
        "name": (data.get("razao_social") or "").strip(), "trade_name": (data.get("nome_fantasia") or "").strip(),
        "postal_code": format_postal_code(data.get("cep")), "street": street, "number": (data.get("numero") or "").strip(),
        "complement": _title(data.get("complemento")), "district": _title(data.get("bairro")),
        "city": _title(data.get("municipio")), "state": (data.get("uf") or "").upper(),
        "phone": (data.get("ddd_telefone_1") or "").strip(), "email": (data.get("email") or "").strip().lower(),
        "active": str(data.get("descricao_situacao_cadastral") or "").upper() in {"", "ATIVA"},
    }


def lookup_cep(cep: str) -> dict | None:
    data = _get_json(f"{BRASILAPI}/cep/v1/{digits(cep)}")
    if not data:
        return None
    return {"street": data.get("street") or "", "district": data.get("neighborhood") or "",
            "city": data.get("city") or "", "state": (data.get("state") or "").upper()}
