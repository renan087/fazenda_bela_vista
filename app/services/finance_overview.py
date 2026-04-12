"""Resumo financeiro para a página de gestão (valores derivados dos módulos existentes)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.models import CropSeason, FinanceAccount, FinanceTransaction, PurchasedInput
from app.repositories.farm import FarmRepository

EXTRACT_MAX_ROWS = 500


def _f(value: object) -> float:
    return float(value or 0)


def _entry_inventory_value(entry: PurchasedInput) -> float:
    avail = _f(entry.available_quantity)
    if avail <= 0:
        return 0.0
    tot_q = _f(entry.total_quantity) or 1.0
    tot_c = _f(entry.total_cost)
    return avail * (tot_c / tot_q)


def _filter_entries_by_farm(entries: list[PurchasedInput], farm_id: int | None) -> list[PurchasedInput]:
    if not farm_id:
        return entries
    return [e for e in entries if e.farm_id in (None, farm_id)]


def _item_type(entry: PurchasedInput) -> str:
    if entry.input_catalog and entry.input_catalog.item_type:
        return str(entry.input_catalog.item_type)
    return "insumo"


def _season_bounds(season: CropSeason | None) -> tuple:
    if not season:
        return None, None
    return season.start_date, season.end_date


def _in_date_range(d, start, end) -> bool:
    if d is None:
        return False
    if start and d < start:
        return False
    if end and d > end:
        return False
    return True


def _in_extract_period(d: date | None, period_start: date | None, period_end: date | None) -> bool:
    """Filtro de datas do extrato; sem início e fim, aceita qualquer data (exceto None quando há filtro)."""
    if period_start is None and period_end is None:
        return True
    if d is None:
        return False
    if period_start and d < period_start:
        return False
    if period_end and d > period_end:
        return False
    return True


def build_finance_overview_context(
    repo: FarmRepository,
    *,
    farm_id: int | None,
    active_season: CropSeason | None,
) -> dict:
    """Agrega totais ligados a compras, estoque, saídas, fertilização e patrimônio."""
    scope_ready = bool(farm_id and active_season)
    plots = (
        repo.list_plots(
            farm_ids=[farm_id] if farm_id else None,
            variety_ids=[active_season.variety_id] if active_season and active_season.variety_id else None,
        )
        if farm_id
        else []
    )
    plot_ids = {p.id for p in plots}
    season_start, season_end = _season_bounds(active_season)

    all_entries = repo.list_purchased_inputs()
    entries = _filter_entries_by_farm(all_entries, farm_id) if farm_id else []

    inv_insumo = 0.0
    inv_suprimento = 0.0
    cost_purchased_insumo = 0.0
    cost_purchased_suprimento = 0.0

    for entry in entries:
        it = _item_type(entry)
        val = _entry_inventory_value(entry)
        tc = _f(entry.total_cost)
        if it == "suprimento":
            inv_suprimento += val
            cost_purchased_suprimento += tc
        else:
            inv_insumo += val
            cost_purchased_insumo += tc

    stock_out_cost_season = 0.0
    if scope_ready and farm_id and active_season:
        sid = active_season.id
        for output in repo.list_stock_outputs(farm_id=farm_id):
            if output.plot_id:
                if output.plot_id not in plot_ids:
                    continue
            elif output.season_id and output.season_id != sid:
                continue
            if not _in_date_range(output.movement_date, season_start, season_end):
                continue
            stock_out_cost_season += _f(output.total_cost)

    fertilization_cost_season = 0.0
    if scope_ready and plot_ids:
        for rec in repo.list_fertilizations():
            if rec.plot_id not in plot_ids:
                continue
            if not _in_date_range(rec.application_date, season_start, season_end):
                continue
            fertilization_cost_season += _f(rec.cost)

    assets_total = 0.0
    if farm_id:
        for asset in repo.list_equipment_assets(farm_id=farm_id):
            assets_total += _f(asset.acquisition_value)

    inventory_total = round(inv_insumo + inv_suprimento, 2)

    return {
        "finance_scope_ready": scope_ready,
        "farm_id": farm_id,
        "plot_count": len(plot_ids),
        "inventory_value_insumo": round(inv_insumo, 2),
        "inventory_value_suprimento": round(inv_suprimento, 2),
        "inventory_value_total": inventory_total,
        "historical_purchase_cost_insumo": round(cost_purchased_insumo, 2),
        "historical_purchase_cost_suprimento": round(cost_purchased_suprimento, 2),
        "historical_purchase_cost_total": round(cost_purchased_insumo + cost_purchased_suprimento, 2),
        "stock_output_cost_season": round(stock_out_cost_season, 2),
        "fertilization_cost_season": round(fertilization_cost_season, 2),
        "assets_acquisition_total": round(assets_total, 2),
        "operational_cost_season": round(stock_out_cost_season + fertilization_cost_season, 2),
    }


def _collect_finance_revenue_rows(
    repo: FarmRepository,
    *,
    farm_id: int,
    period_start: date | None,
    period_end: date | None,
    finance_account_id: int | None = None,
) -> list[dict]:
    """
    Receitas no extrato (coluna Crédito).

    Quando existir modelo ou lançamento de receita (ex.: venda de sacas, outros),
    retornar linhas no formato:
      { date, sort_group, ref_id, module, description, detail, debit: None, credit: float }

    Filtrar cada lançamento com `_in_extract_period(date, period_start, period_end)`.

    Por enquanto retorna lista vazia — mantém o extrato preparado para créditos.
    """
    raw: list[dict] = []

    for account in repo.list_finance_accounts(farm_id=farm_id):
        if finance_account_id and account.id != finance_account_id:
            continue
        balance_date = account.initial_balance_date
        initial_balance = _f(account.initial_balance)
        if initial_balance == 0:
            continue
        if not _in_extract_period(balance_date, period_start, period_end):
            continue
        raw.append(
            {
                "date": balance_date,
                "sort_group": 0,
                "ref_id": account.id,
                "module": "Contas",
                "description": f"Saldo inicial — {account.account_name}",
                "detail": f"({account.bank_code}) {account.bank_name}",
                "debit": abs(initial_balance) if initial_balance < 0 else None,
                "credit": initial_balance if initial_balance > 0 else None,
            }
        )

    return raw


def _collect_finance_transaction_rows(
    repo: FarmRepository,
    *,
    farm_id: int,
    period_start: date | None,
    period_end: date | None,
    finance_account_id: int | None = None,
) -> list[dict]:
    raw: list[dict] = []
    for transaction in repo.list_finance_transactions(farm_id=farm_id):
        if finance_account_id and transaction.finance_account_id != finance_account_id:
            continue
        operation_type = (transaction.operation_type or "").lower()
        is_revenue = operation_type == "receita"
        detail_parts = []
        if transaction.category:
            detail_parts.append(transaction.category)
        if transaction.finance_account:
            detail_parts.append(transaction.finance_account.account_name)
        if transaction.counterparty_name:
            detail_parts.append(transaction.counterparty_name)
        if transaction.document_number:
            detail_parts.append(f"Doc. {transaction.document_number}")
        if transaction.payment_method:
            detail_parts.append(transaction.payment_method)
        detail = " • ".join(detail_parts)
        source_label = _finance_transaction_module_label(transaction)

        payment_condition = (transaction.payment_condition or "").strip().lower()
        if payment_condition == "a_prazo":
            installments = sorted(
                transaction.installments or [],
                key=lambda inst: (inst.installment_number or 0, inst.id),
            )
            total_parts = int(transaction.installment_count or 0) or len(installments) or 1
            for inst in installments:
                if (inst.status or "").strip().lower() != "pago":
                    continue
                paid_day = inst.paid_at
                if paid_day is None:
                    continue
                if not _in_extract_period(paid_day, period_start, period_end):
                    continue
                amt = abs(_f(inst.amount))
                if amt <= 0:
                    continue
                n = int(inst.installment_number or 0)
                parcel_label = f"Parcela {n}/{total_parts}"
                raw.append(
                    {
                        "date": paid_day,
                        "sort_group": 1,
                        "ref_id": inst.id,
                        "module": source_label,
                        "description": f"{'Receita' if is_revenue else 'Despesa'} — {transaction.product_service} ({parcel_label})",
                        "detail": detail,
                        "debit": amt if not is_revenue else None,
                        "credit": amt if is_revenue else None,
                    }
                )
            continue

        if not _in_extract_period(transaction.launch_date, period_start, period_end):
            continue
        amount = abs(_f(transaction.amount))
        if amount <= 0:
            continue
        raw.append(
            {
                "date": transaction.launch_date,
                "sort_group": 0,
                "ref_id": transaction.id,
                "module": source_label,
                "description": f"{'Receita' if is_revenue else 'Despesa'} — {transaction.product_service}",
                "detail": detail,
                "debit": amount if not is_revenue else None,
                "credit": amount if is_revenue else None,
            }
        )
    return raw


def _finance_transaction_module_label(transaction: FinanceTransaction) -> str:
    source = (transaction.source or "").strip().lower()
    if source == "insumos":
        return "Compra de Insumos"
    if source == "suprimentos":
        return "Suprimentos"
    if source == "patrimônio" or source == "patrimonio":
        return "Patrimônio"
    if source == "comercialização" or source == "comercializacao":
        return "Comercialização"
    return "Contas"


def build_finance_extract_rows(
    repo: FarmRepository,
    *,
    farm_id: int | None,
    period_start: date | None = None,
    period_end: date | None = None,
    finance_account_id: int | None = None,
    limit: int = EXTRACT_MAX_ROWS,
) -> tuple[list[dict], bool]:
    """Extrato: despesas = entradas em Gestão de compras, entradas em Suprimentos, patrimônio adquirido.

    `period_start` / `period_end`: filtro opcional (independente da safra ativa). Sem ambos vazios, inclui todo o histórico da fazenda.

    Não inclui saídas de estoque nem fertilizações (custos operacionais fora do extrato).
    Receitas: ver `_collect_finance_revenue_rows`.
    Saldo acumulado por linha: soma de (crédito − débito) até a linha.
    """
    if not farm_id:
        return [], False

    raw: list[dict] = []

    for entry in _filter_entries_by_farm(repo.list_purchased_inputs(), farm_id):
        if entry.finance_transaction_id:
            continue
        if finance_account_id and entry.finance_account_id != finance_account_id:
            continue
        pd = entry.purchase_date
        if not _in_extract_period(pd, period_start, period_end):
            continue
        it = _item_type(entry)
        is_suprimento = it == "suprimento"
        raw.append(
            {
                "date": pd,
                "sort_group": 2 if is_suprimento else 1,
                "ref_id": entry.id,
                "module": "Suprimentos" if is_suprimento else "Gestão de compras",
                "description": f"Entrada — {entry.name}",
                "detail": " • ".join(part for part in [
                    "Registro de compra / entrada de estoque",
                    entry.finance_account.account_name if getattr(entry, "finance_account", None) else None,
                ] if part),
                "debit": _f(entry.total_cost),
                "credit": None,
            }
        )

    for asset in repo.list_equipment_assets(farm_id=farm_id):
        if asset.finance_transaction_id:
            continue
        if finance_account_id and asset.finance_account_id != finance_account_id:
            continue
        ad = asset.acquisition_date
        if not _in_extract_period(ad, period_start, period_end):
            continue
        av = _f(asset.acquisition_value)
        if av <= 0:
            continue
        raw.append(
            {
                "date": ad,
                "sort_group": 3,
                "ref_id": asset.id,
                "module": "Patrimônio",
                "description": f"Aquisição — {asset.name}",
                "detail": " • ".join(part for part in [
                    asset.category or "Bem adquirido",
                    asset.finance_account.account_name if getattr(asset, "finance_account", None) else None,
                ] if part),
                "debit": av,
                "credit": None,
            }
        )

    raw.extend(
        _collect_finance_revenue_rows(
            repo,
            farm_id=farm_id,
            period_start=period_start,
            period_end=period_end,
            finance_account_id=finance_account_id,
        )
    )
    raw.extend(
        _collect_finance_transaction_rows(
            repo,
            farm_id=farm_id,
            period_start=period_start,
            period_end=period_end,
            finance_account_id=finance_account_id,
        )
    )

    raw.sort(
        key=lambda r: (
            r["date"],
            r["sort_group"],
            r["ref_id"],
        )
    )

    truncated = len(raw) > limit
    raw = raw[:limit]

    # Saldo acumulado = créditos − débitos (despesas reduzem o saldo; só débitos → saldo negativo).
    balance = 0.0
    out: list[dict] = []
    for r in raw:
        deb = float(r["debit"] or 0)
        cre = float(r["credit"] or 0)
        balance += cre - deb
        out.append(
            {
                **r,
                "balance": round(balance, 2),
            }
        )

    return out, truncated


def finance_transaction_balance_amount_chunks(transaction: FinanceTransaction) -> list[float]:
    """
    Montantes absolutos a aplicar no saldo do card da conta: uma parcela paga por vez se a prazo;
    valor integral do lançamento se à vista. Alinhado ao extrato (contas).
    """
    if (transaction.payment_condition or "").strip().lower() == "a_prazo":
        chunks: list[float] = []
        for inst in sorted(
            transaction.installments or [],
            key=lambda x: (x.installment_number or 0, x.id),
        ):
            if (inst.status or "").strip().lower() != "pago":
                continue
            amt = abs(float(inst.amount or 0))
            if amt > 0:
                chunks.append(round(amt, 2))
        return chunks
    amt = abs(float(transaction.amount or 0))
    if amt <= 0:
        return []
    return [round(amt, 2)]


def _account_initial_balance_float(account: FinanceAccount) -> float:
    """Converte saldo inicial (Numeric no ORM) de forma estável para float."""
    raw = account.initial_balance
    if raw is None:
        return 0.0
    return float(Decimal(str(raw)).quantize(Decimal("0.01")))


def compute_finance_account_card_balances(
    accounts: list[FinanceAccount],
    transactions: list[FinanceTransaction],
) -> tuple[dict[int, float], dict[int, int]]:
    """
    Saldo atual por conta = saldo inicial + movimentos (à vista integral; a prazo só parcelas pagas).
    Retorna também a quantidade de lançamentos por conta (mesma base usada no saldo).
    """
    balances: dict[int, float] = {}
    for acc in accounts:
        aid = int(acc.id)
        balances[aid] = round(_account_initial_balance_float(acc), 2)
    counts: dict[int, int] = {int(a.id): 0 for a in accounts}
    for tx in transactions:
        acc_id = tx.finance_account_id
        if acc_id is None:
            continue
        acc_id = int(acc_id)
        if acc_id not in balances:
            balances[acc_id] = 0.0
        if acc_id not in counts:
            counts[acc_id] = 0
        counts[acc_id] += 1
        is_revenue = (tx.operation_type or "").strip().lower() == "receita"
        for chunk in finance_transaction_balance_amount_chunks(tx):
            if is_revenue:
                balances[acc_id] = round(balances[acc_id] + chunk, 2)
            else:
                balances[acc_id] = round(balances[acc_id] - chunk, 2)
    return balances, counts
