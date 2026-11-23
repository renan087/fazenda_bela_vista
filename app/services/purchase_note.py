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
from app.models.stock_output import StockOutput
from app.services.forms import (
    _build_installments,
    _lot_tracking_fields,
    _resolve_input_catalog,
    _resolve_organization_id_for_farm,
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


def _payment_plan(payment: dict, finance_account, credit_card) -> dict:
    mode = (payment.get("mode") or "vista").strip()
    method = (payment.get("method") or "").strip() or None
    if mode == "cartao":
        method = CARD_METHOD
        if not credit_card:
            raise ValueError("Escolha o cartão de crédito usado na compra.")
    elif mode == "prazo" and not finance_account:
        raise ValueError("Escolha a conta que vai pagar as parcelas.")
    try:
        count = int(payment.get("count") or 1)
    except (TypeError, ValueError):
        count = 0
    if count < 1 or count > 60:
        raise ValueError("Informe entre 1 e 60 parcelas.")
    condition = "a_prazo" if (mode == "prazo" or (mode == "cartao" and count > 1)) else "a_vista"
    first_due = _date(payment.get("first_due"), "primeiro vencimento") if mode == "prazo" else None
    if mode == "prazo" and not first_due:
        raise ValueError("Informe a data do primeiro vencimento.")
    account_id = (
        finance_account.id
        if finance_account
        else (getattr(credit_card, "payment_account_id", None) if mode == "cartao" else None)
    )
    if mode == "cartao" and not account_id:
        raise ValueError("Cadastre ou vincule uma conta bancária ao cartão antes de lançar a compra no cartão.")
    return {
        "mode": mode, "method": method, "count": count, "condition": condition, "first_due": first_due,
        "frequency": "mensal" if condition == "a_prazo" else None,
        "account_id": account_id,
        "card_id": credit_card.id if credit_card and mode == "cartao" else None,
        "custom": payment.get("installments") or [],
    }


def _line_numbers(line: dict, prefix: str) -> tuple[float, float, float, float]:
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
    return packs, size, price, minimum


def _lot(line: dict, supplier: str, item_type: str, category: str, prefix: str) -> dict:
    traceable = item_type in TRACEABLE_TYPES and category not in UNTRACEABLE_CATEGORIES
    try:
        return _lot_tracking_fields({
            "supplier_name": supplier,
            "batch_number": line.get("batch") if traceable else None,
            "manufacture_date": line.get("manufacture_date") if traceable else None,
            "expiration_date": line.get("expiration_date") if traceable else None,
        })
    except ValueError as exc:
        raise ValueError(prefix + str(exc))


def _payment_fields(plan: dict) -> dict:
    return {
        "finance_account_id": plan["account_id"], "credit_card_id": plan["card_id"],
        "payment_condition": plan["condition"], "payment_method": plan["method"], "installment_count": plan["count"],
        "installment_frequency": plan["frequency"], "first_installment_date": plan["first_due"],
    }


def _new_entry(repo, line: dict, prefix: str, *, farm_id, supplier, purchase_date, note_text, plan) -> PurchasedInput:
    packs, size, price, minimum = _line_numbers(line, prefix)
    org_id = _resolve_organization_id_for_farm(repo, farm_id)
    catalog = _resolve_input_catalog(
        repo, line["name"], line["package_unit"], line["item_type"], line["category"],
        minimum if minimum > 0 else None, override_primary_unit=bool(line.get("override")),
        organization_id=org_id,
    )
    total_quantity = round(packs * size, 2)
    entry = PurchasedInput(
        input_id=catalog.id, farm_id=farm_id, name=catalog.name, normalized_name=catalog.normalized_name,
        quantity_purchased=packs, package_size=size, package_unit=catalog.default_unit, unit_price=price,
        purchase_date=purchase_date, **_lot(line, supplier, line["item_type"], line["category"], prefix),
        total_quantity=total_quantity, available_quantity=total_quantity, total_cost=round(packs * price, 2),
        low_stock_threshold=minimum, notes=note_text, **_payment_fields(plan),
    )
    repo.db.add(entry)
    return entry


def _paid(tx) -> list:
    return [i for i in (tx.installments or []) if (i.status or "").strip().lower() == "pago"] if tx is not None else []


def _write_finance(repo, entries: list, tx, *, farm_id, supplier, marker, note_text, purchase_date, plan, credit_card, category):
    """Cria, atualiza ou remove o lançamento único da nota. Não grava (commit fica com quem chamou)."""
    db = repo.db
    total = round(sum(float(e.total_cost or 0) for e in entries), 2)
    wants_finance = bool(plan["account_id"] or plan["card_id"])
    paid = _paid(tx)
    if paid:
        same = (abs(float(tx.amount or 0) - total) <= 0.01 and (tx.payment_condition or "") == plan["condition"]
                and int(tx.installment_count or 1) == plan["count"] and tx.finance_account_id == plan["account_id"]
                and tx.credit_card_id == plan["card_id"])
        if not same:
            raise ValueError("Esta nota já tem parcela paga. Para mudar valores ou pagamento, estorne o pagamento no Financeiro antes.")
        tx.launch_date = purchase_date
        tx.description = note_text or ""
        return tx
    if not wants_finance:
        if tx is not None:
            for entry in entries:
                entry.finance_transaction_id = None
            db.delete(tx)
        return None
    label = f"{marker} · {supplier}" if marker else f"Compra · {supplier}"
    if tx is None:
        tx = FinanceTransaction(farm_id=farm_id, operation_type="despesa", source="Insumos", created_at=app_now())
        db.add(tx)
    tx.finance_account_id = plan["account_id"]
    tx.credit_card_id = plan["card_id"]
    tx.launch_date = purchase_date
    tx.amount = total
    tx.category = category
    tx.product_service = f"{label} ({len(entries)} {'item' if len(entries) == 1 else 'itens'})"[:200]
    tx.description = note_text or ""
    tx.payment_condition = plan["condition"]
    tx.payment_method = plan["method"]
    tx.installment_count = plan["count"]
    tx.installment_frequency = plan["frequency"]
    tx.first_installment_date = plan["first_due"]
    db.flush()
    for entry in entries:
        entry.finance_transaction_id = tx.id
    rows = _build_installments(
        amount=total, payment_condition=plan["condition"], payment_method=plan["method"], credit_card=credit_card,
        reference_date=purchase_date, installment_count=plan["count"], installment_frequency=plan["frequency"],
        first_installment_date=plan["first_due"],
    )
    if plan["custom"] and plan["mode"] == "prazo":
        if len(plan["custom"]) != len(rows):
            raise ValueError("O número de parcelas não bate com o plano escolhido.")
        for row, edited in zip(rows, plan["custom"]):
            row["due_date"] = _date(edited.get("due_date"), f"vencimento da parcela {row['installment_number']}") or row["due_date"]
            row["amount"] = _money(edited.get("amount"), f"parcela {row['installment_number']}")
        if abs(sum(r["amount"] for r in rows) - total) > 0.01:
            raise ValueError("A soma das parcelas não bate com o total da nota. Ajuste os valores.")
    for old in list(tx.installments or []):
        db.delete(old)
    db.flush()
    for row in rows:
        db.add(FinanceTransactionInstallment(
            finance_transaction_id=tx.id, installment_number=row["installment_number"],
            due_date=row["due_date"], amount=row["amount"], status="pendente",
        ))
    return tx


def _check_header(supplier, items):
    supplier = (supplier or "").strip()
    if not supplier:
        raise ValueError("Informe de quem você comprou.")
    if not items:
        raise ValueError("Adicione pelo menos um item da nota.")
    if len(items) > MAX_ITEMS:
        raise ValueError(f"Uma nota pode ter no máximo {MAX_ITEMS} itens.")
    return supplier


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
    supplier = _check_header(supplier, items)
    plan = _payment_plan(payment, finance_account, credit_card)
    marker = note_marker(invoice)
    note_text = " · ".join(part for part in (marker, (notes or "").strip() or None) if part) or None
    db = repo.db
    try:
        created = [
            _new_entry(repo, line, f"Item {n}: " if len(items) > 1 else "", farm_id=farm_id, supplier=supplier,
                       purchase_date=purchase_date, note_text=note_text, plan=plan)
            for n, line in enumerate(items, start=1)
        ]
        db.flush()
        biggest = max(range(len(created)), key=lambda k: float(created[k].total_cost or 0))
        tx = _write_finance(repo, created, None, farm_id=farm_id, supplier=supplier, marker=marker, note_text=note_text,
                            purchase_date=purchase_date, plan=plan, credit_card=credit_card, category=items[biggest]["category"])
        db.commit()
    except Exception:
        db.rollback()
        raise
    return created, tx


# ---------------------------------------------------------------- nota já lançada
def note_entries(repo, entry) -> list[PurchasedInput]:
    """Todos os itens da mesma nota (mesmo lançamento no Financeiro) ou só o item, se for compra avulsa."""
    if entry is None:
        return []
    if entry.finance_transaction_id and entry.finance_transaction is not None:
        items = [e for e in entry.finance_transaction.purchased_inputs if (e.entry_origin or "purchase") != "stock_adjustment_entry"]
        return sorted(items, key=lambda e: e.id) or [entry]
    return [entry]


def consumed(entry) -> float:
    return round(max(float(entry.total_quantity or 0) - float(entry.available_quantity or 0), 0), 2)


def note_uses(db, entries) -> list[dict]:
    """O que já saiu do estoque a partir desta nota (impede excluir)."""
    ids = [e.id for e in entries]
    if not ids:
        return []
    rows = (
        db.query(StockOutput)
        .filter(StockOutput.purchased_input_id.in_(ids))
        .order_by(StockOutput.movement_date.desc(), StockOutput.id.desc())
        .all()
    )
    names = {e.id: e.name for e in entries}
    units = {e.id: e.package_unit for e in entries}
    uses = []
    for row in rows:
        uses.append({
            "name": names.get(row.purchased_input_id, "item"), "quantity": float(row.quantity or 0),
            "unit": row.unit or units.get(row.purchased_input_id), "plot": row.plot.name if row.plot else None,
            "date": row.movement_date, "output_id": row.reference_id if row.reference_type == "manual_stock_output_allocation" else row.id,
            "adjustment": (row.reference_type or "").startswith("stock_adjustment"),
        })
    return uses


def delete_blockers(db, entries) -> dict:
    uses = note_uses(db, entries)
    linked = sum(len(e.recommendations or []) + len(e.recommendation_items or []) + len(e.schedule_items or []) + len(e.stock_allocations or []) for e in entries)
    tx = entries[0].finance_transaction if entries and entries[0].finance_transaction_id else None
    paid = _paid(tx)
    return {"uses": uses, "linked": linked, "paid": len(paid), "blocked": bool(uses or linked or paid or any(consumed(e) > 0 for e in entries))}


def delete_purchase_note(repo, entry) -> int:
    entries = note_entries(repo, entry)
    if not entries:
        raise ValueError("Compra não encontrada.")
    info = delete_blockers(repo.db, entries)
    if info["blocked"]:
        if info["paid"]:
            raise ValueError("Esta compra tem parcela paga. Estorne o pagamento no Financeiro antes de excluir.")
        raise ValueError("Parte desta compra já foi usada. Desfaça os usos em Movimentações ou corrija a compra.")
    db = repo.db
    tx = entries[0].finance_transaction if entries[0].finance_transaction_id else None
    try:
        for e in entries:
            db.delete(e)
        if tx is not None:
            db.flush()
            db.refresh(tx)
            if not tx.purchased_inputs:
                db.delete(tx)
        db.commit()
    except Exception:
        db.rollback()
        raise
    return len(entries)


def update_purchase_note(
    repo,
    entry,
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
    """Corrige uma nota já lançada sem perder o que já foi usado do estoque."""
    supplier = _check_header(supplier, items)
    plan = _payment_plan(payment, finance_account, credit_card)
    current = note_entries(repo, entry)
    if not current:
        raise ValueError("Compra não encontrada.")
    by_id = {e.id: e for e in current}
    tx = current[0].finance_transaction if current[0].finance_transaction_id else None
    marker = note_marker(invoice)
    note_text = " · ".join(part for part in (marker, (notes or "").strip() or None) if part) or None
    db = repo.db
    kept_ids = set()
    result: list[PurchasedInput] = []
    try:
        for n, line in enumerate(items, start=1):
            prefix = f"Item {n}: " if len(items) > 1 else ""
            raw_id = line.get("entry_id")
            existing = by_id.get(int(raw_id)) if str(raw_id or "").isdigit() else None
            if raw_id and existing is None:
                raise ValueError(prefix + "este item não pertence a esta compra.")
            if existing is None:
                result.append(_new_entry(repo, line, prefix, farm_id=farm_id, supplier=supplier,
                                         purchase_date=purchase_date, note_text=note_text, plan=plan))
                continue
            kept_ids.add(existing.id)
            packs, size, price, minimum = _line_numbers(line, prefix)
            total_quantity = round(packs * size, 2)
            used = consumed(existing)
            if total_quantity + 1e-9 < used:
                raise ValueError(prefix + f"já saíram {used:g} {existing.package_unit} de {existing.name} desta compra; "
                                 f"a quantidade não pode ficar menor que isso.")
            catalog = existing.input_catalog
            existing.quantity_purchased = packs
            existing.package_size = size
            existing.unit_price = price
            existing.total_quantity = total_quantity
            existing.available_quantity = round(total_quantity - used, 2)
            existing.total_cost = round(packs * price, 2)
            existing.low_stock_threshold = minimum
            existing.purchase_date = purchase_date
            existing.notes = note_text
            for key, value in _lot(line, supplier, catalog.item_type if catalog else "insumo_agricola",
                                   catalog.category if catalog else "", prefix).items():
                setattr(existing, key, value)
            for key, value in _payment_fields(plan).items():
                setattr(existing, key, value)
            if catalog and minimum > 0:
                catalog.low_stock_threshold = minimum
            result.append(existing)
        for removed in [e for e in current if e.id not in kept_ids]:
            info = delete_blockers(db, [removed])
            if info["uses"] or info["linked"] or consumed(removed) > 0:
                raise ValueError(f"{removed.name} já foi usado e não pode sair da nota. Corrija a quantidade em vez de remover.")
            removed.finance_transaction_id = None
            db.delete(removed)
        db.flush()
        biggest = max(result, key=lambda e: float(e.total_cost or 0))
        category = biggest.input_catalog.category if biggest.input_catalog else "Insumos"
        tx = _write_finance(repo, result, tx, farm_id=farm_id, supplier=supplier, marker=marker, note_text=note_text,
                            purchase_date=purchase_date, plan=plan, credit_card=credit_card, category=category)
        db.commit()
    except Exception:
        db.rollback()
        raise
    return result, tx
