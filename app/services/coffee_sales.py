"""Vendas de café: uma venda (código V-0001) pode sair de vários lotes.

Cada lote vira um registro em ``coffee_commercialization_records`` com o mesmo ``sale_code``;
assim o saldo por lote, o painel e o vínculo com o Financeiro continuam por registro.
"""

from __future__ import annotations

import re
from collections import OrderedDict
from datetime import date

from app.core.timezone import app_now, today_in_app_timezone
from app.models.coffee_commercialization import CoffeeCommercializationRecord
from app.models.finance_transaction import FinanceTransaction

SALE_STATUS_OPTIONS = [("negociado", "Negociada"), ("entregue", "Entregue"), ("recebido", "Recebida")]
SALE_STATUS_LABELS = {"negociado": "Negociada", "faturado": "Faturada", "entregue": "Entregue", "recebido": "Recebida"}
SALE_STATUS_STEPS = ["negociado", "entregue", "recebido"]
EPS = 0.0001


def _num(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def parse_decimal(value) -> float | None:
    """Aceita '1.900,50', '1900.5' ou número."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("R$", "").replace("%", "").replace(" ", "")
    if not text:
        return None
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    elif re.fullmatch(r"-?\d{1,3}(\.\d{3})+", text):
        text = text.replace(".", "")
    try:
        return float(text)
    except ValueError:
        return None


def next_sale_code(db, farm_id: int) -> str:
    codes = [row[0] for row in db.query(CoffeeCommercializationRecord.sale_code)
             .filter(CoffeeCommercializationRecord.farm_id == farm_id, CoffeeCommercializationRecord.sale_code.isnot(None))
             .distinct().all()]
    numbers = [int(m.group(1)) for code in codes if (m := re.fullmatch(r"V-(\d+)", code or ""))]
    return f"V-{(max(numbers) + 1) if numbers else 1:04d}"


def compute_values(lines: list[dict], unit_price: float, funrural_percent: float, brokerage_percent: float,
                   freight_value: float) -> dict:
    """Valor bruto por lote, rateio dos descontos pelo volume e líquido."""
    total_sacks = sum(line["sacks"] for line in lines)
    gross_total = round(total_sacks * unit_price, 2)
    percent_discount = round(gross_total * (funrural_percent + brokerage_percent) / 100, 2)
    discount_total = round(percent_discount + freight_value, 2)
    if discount_total > gross_total:
        raise ValueError("Os descontos não podem ser maiores que o valor da venda.")
    rows, gross_acc, disc_acc = [], 0.0, 0.0
    for index, line in enumerate(lines):
        last = index == len(lines) - 1
        gross = round(gross_total - gross_acc, 2) if last else round(line["sacks"] * unit_price, 2)
        share = line["sacks"] / total_sacks if total_sacks else 0
        disc = round(discount_total - disc_acc, 2) if last else round(discount_total * share, 2)
        gross_acc += gross
        disc_acc += disc
        rows.append({**line, "gross": gross, "discount": disc, "net": round(gross - disc, 2)})
    return {"lines": rows, "sacks": round(total_sacks, 4), "gross": gross_total, "discount": discount_total,
            "net": round(gross_total - discount_total, 2)}


def save_sale(repo, *, farm_id: int, lines: list[dict], data: dict, existing: list[CoffeeCommercializationRecord] | None = None) -> str:
    """Cria ou substitui uma venda.

    ``lines``: [{"harvest", "sacks", "available", "label"}]; ``available`` já desconta outras vendas
    (e não esta, quando for edição).
    """
    from app.services.forms import _build_installments, _normalize_finance_schedule_fields, _replace_installments

    existing = existing or []
    if not lines:
        raise ValueError("Escolha pelo menos um lote para a venda.")
    seen = set()
    for line in lines:
        if line["harvest"].id in seen:
            raise ValueError("O mesmo lote apareceu duas vezes na venda.")
        seen.add(line["harvest"].id)
        if line["sacks"] <= 0:
            raise ValueError("Informe quantas sacas saem de cada lote.")
        if line["sacks"] > round(_num(line["available"]), 4) + EPS:
            raise ValueError(f"A quantidade do lote {line['label']} ultrapassa o saldo disponível.")
    unit_price = parse_decimal(data.get("unit_price")) or 0
    if unit_price <= 0:
        raise ValueError("Informe o preço por saca.")
    funrural = parse_decimal(data.get("funrural_percent")) or 0
    brokerage = parse_decimal(data.get("brokerage_percent")) or 0
    freight = parse_decimal(data.get("freight_value")) or 0
    if min(funrural, brokerage, freight) < 0 or funrural > 100 or brokerage > 100:
        raise ValueError("Confira os descontos: use valores entre 0 e 100%.")
    buyer_name = (data.get("buyer_name") or "").strip()
    if not buyer_name:
        raise ValueError("Escolha ou cadastre o comprador.")
    status = data.get("status") if data.get("status") in SALE_STATUS_LABELS else "negociado"
    sale_date = data.get("sale_date") or today_in_app_timezone()
    values = compute_values(lines, unit_price, funrural, brokerage, freight)
    payment_condition, payment_method, installment_count, installment_frequency, first_installment_date = (
        _normalize_finance_schedule_fields(data))
    code = existing[0].sale_code if existing and existing[0].sale_code else next_sale_code(repo.db, farm_id)

    tx = None
    old_tx_id = next((r.finance_transaction_id for r in existing if r.finance_transaction_id), None)
    if old_tx_id:
        tx = repo.get_finance_transaction(old_tx_id)
    mode = data.get("recebimento")  # "nao" | "sim" | "parc" (Nova venda); ausente = formulário antigo
    if mode == "nao" and tx is not None and any(not _open(i) for i in (tx.installments or [])):
        raise ValueError("Esta venda já tem parcelas recebidas. Desfaça o recebimento pela lista antes de mudar para “Ainda não”.")
    for record in existing:
        repo.db.delete(record)
    repo.db.flush()

    account_id = data.get("finance_account_id")
    if account_id:
        lot_count = len(values["lines"])
        description = f"Venda de café {code} — {lot_count} lote" + ("s" if lot_count > 1 else "")
        if tx is None:
            tx = FinanceTransaction(farm_id=farm_id, operation_type="receita", created_at=app_now())
            repo.db.add(tx)
        tx.finance_account_id = account_id
        tx.launch_date = sale_date
        tx.amount = values["net"]
        tx.category = "Venda Agrícola"
        tx.product_service = description
        tx.description = data.get("notes") or ""
        tx.counterparty_name = buyer_name
        tx.payment_condition = payment_condition
        tx.payment_method = payment_method
        tx.installment_count = installment_count
        tx.installment_frequency = installment_frequency
        tx.first_installment_date = first_installment_date
        tx.source = "Comercialização"
        if mode:
            tx.credit_card_id = data.get("credit_card_id") or None
        repo.db.flush()
        _replace_installments(repo, tx, _build_installments(
            amount=values["net"], payment_condition=payment_condition, installment_count=installment_count,
            installment_frequency=installment_frequency, first_installment_date=first_installment_date))
        if mode and data.get("paid_count") is not None:
            # As primeiras N parcelas já foram recebidas; as demais ficam pendentes.
            repo.db.flush()
            repo.db.refresh(tx)
            today = today_in_app_timezone()
            for inst in sorted(tx.installments or [], key=lambda i: i.installment_number):
                if inst.installment_number <= int(data["paid_count"]):
                    if _open(inst):
                        inst.status = "pago"
                        inst.paid_at = data.get("received_on") if mode == "sim" else min(inst.due_date, today)
                elif not _open(inst):
                    inst.status, inst.paid_at, inst.payment_notes = "pendente", None, None
    elif tx is not None:
        repo.db.delete(tx)
        tx = None

    for line in values["lines"]:
        harvest = line["harvest"]
        plot = harvest.plot
        repo.db.add(CoffeeCommercializationRecord(
            farm_id=farm_id, harvest_id=harvest.id, buyer_id=data.get("buyer_id"),
            finance_account_id=account_id or None, finance_transaction_id=tx.id if tx else None,
            sale_code=code, sale_date=sale_date, buyer_name=buyer_name[:180], lot_label=line["label"][:220],
            plot_name=plot.name if plot else None,
            variety_name=plot.variety.name if plot and plot.variety else None,
            harvest_date_snapshot=harvest.harvest_date, coffee_type=data.get("coffee_type") or None,
            sale_unit="sc_60", quantity_sold=line["sacks"], equivalent_sacks=line["sacks"], unit_price=unit_price,
            total_value=line["gross"], discount_value=line["discount"], net_value=line["net"],
            funrural_percent=funrural or None, brokerage_percent=brokerage or None, freight_value=freight or None,
            contract_number=(data.get("contract_number") or "").strip()[:60] or None,
            delivery_due_date=data.get("delivery_due_date"), status="negociado" if mode else status,
            delivered_on=data.get("delivered_on") if (mode or status in {"entregue", "recebido"}) else None,
            received_on=data.get("received_on") if (mode or status == "recebido") else None,
            payment_method=payment_method, payment_condition=payment_condition, installment_count=installment_count,
            installment_frequency=installment_frequency, first_installment_date=first_installment_date,
            notes=(data.get("notes") or "").strip() or None,
        ))
    repo.db.flush()
    if mode:
        records = sale_records(repo.db, farm_id, code)
        if mode == "parc" and tx is not None:
            _sync_received(repo.db, records, list(tx.installments or []))
        refresh_status(records)
    repo.db.commit()
    return code


def sale_records(db, farm_id: int, code: str) -> list[CoffeeCommercializationRecord]:
    return (db.query(CoffeeCommercializationRecord)
            .filter(CoffeeCommercializationRecord.farm_id == farm_id, CoffeeCommercializationRecord.sale_code == code)
            .order_by(CoffeeCommercializationRecord.id).all())


def group_sales(records: list[CoffeeCommercializationRecord]) -> list[dict]:
    """Agrupa registros por venda, mantendo a ordem recebida."""
    groups: "OrderedDict[str, list]" = OrderedDict()
    for record in records:
        groups.setdefault(record.sale_code or f"V-{record.id:04d}", []).append(record)
    result = []
    for code, rows in groups.items():
        first = rows[0]
        gross = round(sum(_num(r.total_value) for r in rows), 2)
        net = round(sum(_num(r.net_value if r.net_value is not None else r.total_value) for r in rows), 2)
        status = (first.status or "negociado").lower()
        # Faturada = nota fiscal autorizada; o café ainda não foi entregue (passo 0).
        step = SALE_STATUS_STEPS.index(status) if status in SALE_STATUS_STEPS else 0
        result.append({
            "code": code, "records": rows, "first": first, "sale_date": first.sale_date, "buyer_name": first.buyer_name,
            "buyer_id": first.buyer_id, "sacks": round(sum(_num(r.equivalent_sacks) for r in rows), 4),
            "gross": gross, "net": net, "discount": round(gross - net, 2), "unit_price": _num(first.unit_price),
            "status": status, "status_label": SALE_STATUS_LABELS.get(status, status.capitalize()), "step": step,
            "lots": [{"harvest_id": r.harvest_id, "label": r.lot_label, "sacks": _num(r.equivalent_sacks)} for r in rows],
            "delivery_due_date": first.delivery_due_date, "delivered_on": first.delivered_on,
            "received_on": first.received_on, "contract_number": first.contract_number,
            "finance_transaction_id": first.finance_transaction_id, "coffee_type": first.coffee_type,
            "delivered": first.delivered_on is not None, "received": first.received_on is not None,
        })
    return result


def set_status(db, records: list[CoffeeCommercializationRecord], status: str, on_date: date | None) -> None:
    if status not in SALE_STATUS_STEPS:
        raise ValueError("Situação inválida.")
    on_date = on_date or today_in_app_timezone()
    for record in records:
        record.status = status
        if status == "negociado":
            record.delivered_on = record.received_on = None
        if status in {"entregue", "recebido"} and not record.delivered_on:
            record.delivered_on = on_date
        if status == "entregue":
            record.received_on = None
        if status == "recebido":
            record.received_on = on_date
    db.commit()


def refresh_status(records: list[CoffeeCommercializationRecord]) -> None:
    """Entrega e recebimento são independentes; a situação gravada só resume as duas datas."""
    for record in records:
        current = (record.status or "negociado").lower()
        if record.received_on:
            record.status = "recebido"
        elif record.delivered_on:
            record.status = "entregue"
        else:
            record.status = "faturado" if current == "faturado" else "negociado"


def set_delivery(db, records: list[CoffeeCommercializationRecord], delivered: bool, on_date: date | None = None) -> None:
    on_date = on_date or today_in_app_timezone()
    for record in records:
        record.delivered_on = on_date if delivered else None
    refresh_status(records)
    db.commit()


def set_delivery_due(db, records: list[CoffeeCommercializationRecord], due: date | None) -> None:
    for record in records:
        record.delivery_due_date = due
    db.commit()


def _open(inst) -> bool:
    return (inst.status or "pendente").strip().lower() != "pago"


def sale_installments(db, transaction_id: int | None) -> list:
    from app.models.finance_transaction_installment import FinanceTransactionInstallment
    if not transaction_id:
        return []
    return (db.query(FinanceTransactionInstallment)
            .filter(FinanceTransactionInstallment.finance_transaction_id == transaction_id)
            .order_by(FinanceTransactionInstallment.installment_number, FinanceTransactionInstallment.id).all())


def receipt_info(group: dict, installments: list, today: date | None = None) -> dict:
    """Situação do recebimento: pelas parcelas do Financeiro quando existem; senão pela data da venda."""
    today = today or today_in_app_timezone()
    rows = []
    for inst in installments:
        paid = not _open(inst)
        rows.append({"id": inst.id, "n": inst.installment_number, "due": inst.due_date, "amount": round(_num(inst.amount), 2),
                     "paid": paid, "paid_at": inst.paid_at, "late": (not paid) and inst.due_date is not None and inst.due_date < today})
    if rows:
        total = round(sum(r["amount"] for r in rows), 2) or 0.0
        paid_amount = round(sum(r["amount"] for r in rows if r["paid"]), 2)
        open_rows = [r for r in rows if not r["paid"]]
        received = not open_rows
        pct = 100 if received else (round(paid_amount * 100 / total) if total else 0)
    else:
        total, paid_amount = group["net"], (group["net"] if group.get("received") else 0.0)
        open_rows, received = [], bool(group.get("received"))
        pct = 100 if received else 0
    return {"rows": rows, "open": open_rows, "total": total, "paid_amount": paid_amount, "open_amount": round(total - paid_amount, 2),
            "received": received, "pct": pct, "partial": 0 < pct < 100, "late": [r for r in open_rows if r["late"]],
            "count": len(rows), "paid_count": len(rows) - len(open_rows)}


def _sync_received(db, records, installments) -> None:
    all_paid = bool(installments) and not any(_open(i) for i in installments)
    stamp = max((i.paid_at for i in installments if i.paid_at), default=None) or today_in_app_timezone()
    for record in records:
        record.received_on = stamp if all_paid else None
    refresh_status(records)


def receive(db, records: list[CoffeeCommercializationRecord], installment_ids: list[int] | None = None,
            on_date: date | None = None, account_id: int | None = None, payment_method: str | None = None) -> list[int]:
    """Marca como recebidas as parcelas escolhidas (todas as abertas se nenhuma for indicada).

    Sem lançamento no Financeiro, a venda inteira fica recebida; com uma conta escolhida, o recebimento
    entra no Financeiro como à vista na data. Devolve as parcelas pagas agora."""
    on_date = on_date or today_in_app_timezone()
    tx_id = records[0].finance_transaction_id if records else None
    installments = sale_installments(db, tx_id)
    if not installments:
        if not tx_id and account_id and records:
            first = records[0]
            net = round(sum(_num(r.net_value if r.net_value is not None else r.total_value) for r in records), 2)
            tx = FinanceTransaction(
                farm_id=first.farm_id, operation_type="receita", created_at=app_now(), finance_account_id=account_id,
                launch_date=on_date, amount=net, category="Venda Agrícola",
                product_service=f"Venda de café {first.sale_code} — {len(records)} lote" + ("s" if len(records) > 1 else ""),
                description=first.notes or "", counterparty_name=first.buyer_name, payment_condition="a_vista",
                payment_method=(payment_method or "").strip() or None, installment_count=1, source="Comercialização")
            db.add(tx)
            db.flush()
            for record in records:
                record.finance_transaction_id, record.finance_account_id = tx.id, account_id
                record.payment_method = tx.payment_method
        for record in records:
            record.received_on = on_date
        refresh_status(records)
        db.commit()
        return []
    wanted = set(installment_ids or [])
    paid_now = []
    for inst in installments:
        if _open(inst) and (not wanted or inst.id in wanted):
            inst.status, inst.paid_at = "pago", on_date
            paid_now.append(inst.id)
    if wanted and not paid_now:
        raise ValueError("Escolha parcelas em aberto desta venda.")
    _sync_received(db, records, installments)
    db.commit()
    return paid_now


def undo_receive(db, records: list[CoffeeCommercializationRecord], installment_ids: list[int] | None = None) -> None:
    """Desfaz o recebimento: das parcelas indicadas, ou de tudo."""
    installments = sale_installments(db, records[0].finance_transaction_id if records else None)
    wanted = set(installment_ids or [])
    for inst in installments:
        if not _open(inst) and (not wanted or inst.id in wanted):
            inst.status, inst.paid_at, inst.payment_notes = "pendente", None, None
    if installments:
        _sync_received(db, records, installments)
    else:
        tx_id = records[0].finance_transaction_id if records else None
        tx = db.get(FinanceTransaction, tx_id) if tx_id else None
        if tx is not None and (tx.payment_condition or "a_vista") == "a_vista":
            # À vista = já recebido no Financeiro; desfazer tira o lançamento (volta a "a receber").
            for record in records:
                record.finance_transaction_id = None
            db.delete(tx)
        for record in records:
            record.received_on = None
        refresh_status(records)
    db.commit()


def sync_status_with_invoice(db, *, farm_id: int, sale_code: str, authorized: bool) -> bool:
    """Nota autorizada fatura a venda negociada; nota cancelada devolve a venda faturada para negociada."""
    records = sale_records(db, farm_id, sale_code)
    changed = False
    for record in records:
        current = (record.status or "negociado").lower()
        if authorized and current == "negociado":
            record.status, changed = "faturado", True
        elif not authorized and current == "faturado":
            record.status, changed = "negociado", True
    if changed:
        db.commit()
    return changed


def sold_sacks_by_harvest(records) -> dict[int, float]:
    totals: dict[int, float] = {}
    for record in records:
        if record.harvest_id:
            totals[record.harvest_id] = totals.get(record.harvest_id, 0.0) + _num(record.equivalent_sacks)
    return totals

