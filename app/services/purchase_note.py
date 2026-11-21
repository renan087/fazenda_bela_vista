"""Compra em 4 etapas: grava uma nota com vários itens numa transação só.

Cada item vira uma entrada no estoque (PurchasedInput, com lote e validade quando
faz sentido). O pagamento é da nota inteira: um único lançamento no Financeiro,
com as parcelas (que o usuário pode ajustar, desde que a soma bata com a nota).
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date

from app.core.timezone import app_now
from app.models.finance_transaction import FinanceTransaction
from app.models.finance_transaction_installment import FinanceTransactionInstallment
from app.models.purchased_input import PurchasedInput
from app.services.forms import (
    _build_installments,
    _lot_tracking_fields,
    _resolve_input_catalog,
)
from app.services.inputs_hub import parse_br_number

NOTE_PREFIX = "Nota fiscal"
CARD_METHOD = "Cartão de crédito"
MAX_ITEMS = 40
TRACEABLE_TYPES = {"insumo_agricola"}
UNTRACEABLE_CATEGORIES = {"Outros Custos de Insumos"}


def note_marker(invoice: str | None) -> str | None:
    invoice = (invoice or "").strip()
    return f"{NOTE_PREFIX} {invoice}" if invoice else None


def _plain(value: str | None) -> str:
    text = unicodedata.normalize("NFD", str(value or "")).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", text).strip().lower()


def find_duplicate_note(entries, supplier: str | None, invoice: str | None) -> date | None:
    """Data de uma compra já lançada com o mesmo fornecedor e número de nota."""
    marker = note_marker(invoice)
    if not marker:
        return None
    for entry in entries:
        if _plain(entry.supplier_name) == _plain(supplier) and marker.lower() in (entry.notes or "").lower():
            return entry.purchase_date
    return None


def _date(value, label: str) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        raise ValueError(f"Confira a data: {label}.")


def _money(value, label: str) -> float:
    try:
        number = parse_br_number(str(value or "").replace("R$", ""))
    except ValueError:
        number = None
    if number is None or number < 0:
        raise ValueError(f"Confira o valor: {label}.")
    return round(number, 2)


def register_purchase_note(
    repo,
    *,
    farm_id: int,
    supplier: str,
    invoice: str | None,
    purchase_date: date,
    items: list[dict],
    payment: dict,
    finance_account=None,
    credit_card=None,
    notes: str | None = None,
) -> tuple[list[PurchasedInput], FinanceTransaction | None]:
    """`items` já chegam validados pela rota (tipo, categoria e unidade resolvidos)."""
    supplier = (supplier or "").strip()
    if not supplier:
        raise ValueError("Informe de quem você comprou.")
    if not items:
        raise ValueError("Adicione pelo menos um item da nota.")
    if len(items) > MAX_ITEMS:
        raise ValueError(f"Uma nota pode ter no máximo {MAX_ITEMS} itens.")

    mode = (payment.get("mode") or "vista").strip()
    method = (payment.get("method") or "").strip() or None
    if mode == "cartao":
        method = CARD_METHOD
        if not credit_card:
            raise ValueError("Escolha o cartão de crédito usado na compra.")
    elif mode == "prazo" and not finance_account:
        raise ValueError("Escolha a conta que vai pagar as parcelas.")
    count = int(payment.get("count") or 1)
    if count < 1 or count > 60:
        raise ValueError("Informe entre 1 e 60 parcelas.")
    payment_condition = "a_prazo" if (mode == "prazo" or (mode == "cartao" and count > 1)) else "a_vista"
    first_due = _date(payment.get("first_due"), "primeiro vencimento") if mode == "prazo" else None
    if mode == "prazo" and not first_due:
        raise ValueError("Informe a data do primeiro vencimento.")
    frequency = "mensal" if payment_condition == "a_prazo" else None

    marker = note_marker(invoice)
    note_text = " · ".join(part for part in (marker, (notes or "").strip() or None) if part) or None
    created: list[PurchasedInput] = []
    db = repo.db
    try:
        for number, line in enumerate(items, start=1):
            prefix = f"Item {number}: " if len(items) > 1 else ""
            try:
                packs = parse_br_number(line.get("packs"))
                size = parse_br_number(line.get("size"))
                price = parse_br_number(str(line.get("price") or "").replace("R$", ""))
                minimum = parse_br_number(line.get("minimum")) or 0.0
            except ValueError:
                raise ValueError(prefix + "confira quantidade, tamanho da embalagem e preço.")
            if not packs or packs <= 0 or not size or size <= 0:
                raise ValueError(prefix + "informe quantas embalagens e quanto tem cada uma.")
            if price is None or price < 0:
                raise ValueError(prefix + "informe o preço por embalagem.")
            item_type = line["item_type"]
            category = line["category"]
            traceable = item_type in TRACEABLE_TYPES and category not in UNTRACEABLE_CATEGORIES
            try:
                lot = _lot_tracking_fields({
                    "supplier_name": supplier,
                    "batch_number": line.get("batch") if traceable else None,
                    "manufacture_date": line.get("manufacture_date") if traceable else None,
                    "expiration_date": line.get("expiration_date") if traceable else None,
                })
            except ValueError as exc:
                raise ValueError(prefix + str(exc))
            catalog = _resolve_input_catalog(
                repo, line["name"], line["package_unit"], item_type, category,
                minimum if minimum > 0 else None, override_primary_unit=bool(line.get("override")),
            )
            total_quantity = round(packs * size, 2)
            entry = PurchasedInput(
                input_id=catalog.id, farm_id=farm_id,
                finance_account_id=finance_account.id if finance_account and mode != "cartao" else None,
                credit_card_id=credit_card.id if credit_card and mode == "cartao" else None,
                name=catalog.name, normalized_name=catalog.normalized_name,
                quantity_purchased=packs, package_size=size, package_unit=catalog.default_unit, unit_price=price,
                purchase_date=purchase_date, **lot,
                total_quantity=total_quantity, available_quantity=total_quantity, total_cost=round(packs * price, 2),
                low_stock_threshold=minimum, notes=note_text,
                payment_condition=payment_condition, payment_method=method, installment_count=count,
                installment_frequency=frequency, first_installment_date=first_due,
            )
            db.add(entry)
            created.append(entry)
        db.flush()

        total = round(sum(float(e.total_cost or 0) for e in created), 2)
        tx = None
        if finance_account or credit_card:
            biggest = max(created, key=lambda e: float(e.total_cost or 0))
            label = f"{marker} · {supplier}" if marker else f"Compra · {supplier}"
            tx = FinanceTransaction(
                farm_id=farm_id,
                finance_account_id=finance_account.id if finance_account and mode != "cartao" else None,
                credit_card_id=credit_card.id if credit_card and mode == "cartao" else None,
                operation_type="despesa", launch_date=purchase_date, amount=total,
                category=items[created.index(biggest)]["category"],
                product_service=f"{label} ({len(created)} {'item' if len(created) == 1 else 'itens'})"[:200],
                description=note_text or "", payment_condition=payment_condition, payment_method=method,
                installment_count=count, installment_frequency=frequency, first_installment_date=first_due,
                source="Insumos", created_at=app_now(),
            )
            db.add(tx)
            db.flush()
            for entry in created:
                entry.finance_transaction_id = tx.id
            rows = _build_installments(
                amount=total, payment_condition=payment_condition, payment_method=method, credit_card=credit_card,
                reference_date=purchase_date, installment_count=count, installment_frequency=frequency,
                first_installment_date=first_due,
            )
            custom = payment.get("installments") or []
            if custom and mode == "prazo":
                if len(custom) != len(rows):
                    raise ValueError("O número de parcelas não bate com o plano escolhido.")
                for row, edited in zip(rows, custom):
                    row["due_date"] = _date(edited.get("due_date"), f"vencimento da parcela {row['installment_number']}") or row["due_date"]
                    row["amount"] = _money(edited.get("amount"), f"parcela {row['installment_number']}")
                if abs(sum(r["amount"] for r in rows) - total) > 0.01:
                    raise ValueError("A soma das parcelas não bate com o total da nota. Ajuste os valores.")
            for row in rows:
                db.add(FinanceTransactionInstallment(
                    finance_transaction_id=tx.id, installment_number=row["installment_number"],
                    due_date=row["due_date"], amount=row["amount"], status=row.get("status") or "pendente",
                ))
        db.commit()
    except Exception:
        db.rollback()
        raise
    return created, tx
