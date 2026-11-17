"""Ferramentas de CONSULTA do Assistente SiSFarm (somente leitura).

Cada ferramenta so e oferecida a IA quando o usuario tem permissao para o modulo, e todas
ficam presas a fazenda e a safra ativas. Os resultados sao resumidos para caber no contexto.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta

from app.core.permissions_catalog import (
    PAGE_DASHBOARD, PAGE_FINANCE, PAGE_INPUTS, PAGE_OPERATIONS, PAGE_PRODUCTION, PAGE_RAINFALL,
)

MAX_ROWS = 15
PERIOD_PROPS = {
    "inicio": {"type": ["string", "null"], "description": "Data inicial AAAA-MM-DD (null = padrão da ferramenta)."},
    "fim": {"type": ["string", "null"], "description": "Data final AAAA-MM-DD (null = hoje)."},
}


def _tool(name, description, properties=None):
    properties = properties or {}
    return {"type": "function", "function": {
        "name": name, "description": description,
        "parameters": {"type": "object", "additionalProperties": False,
                       "required": list(properties), "properties": properties},
    }}


def _num(value, places=2):
    return round(float(value or 0), places)


def _norm(value):
    from app.services.stock_assistant import normalized
    return normalized(str(value or ""))


class AssistantQueries:
    def __init__(self, *, repo, farm, season, plots, today, allowed):
        self.repo = repo
        self.farm = farm
        self.season = season
        self.plots = list(plots)
        self.plot_ids = {plot.id for plot in self.plots}
        self.today = today
        self.allowed = allowed  # funcao(page_code) -> bool

    # ------------------------------------------------------------ utilitarios
    def _period(self, inicio, fim, default_days=None):
        end = self._date(fim) or self.today
        if inicio:
            start = self._date(inicio)
        elif default_days:
            start = end - timedelta(days=default_days)
        else:
            start = self.season.start_date if self.season else end - timedelta(days=30)
        if not start or start > end:
            raise ValueError("Período inválido.")
        return start, end

    @staticmethod
    def _date(value):
        if not value:
            return None
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            raise ValueError(f"Data inválida: {value}. Use AAAA-MM-DD.") from None

    def _plot_filter(self, setor):
        if not setor:
            return None
        wanted = _norm(setor)
        matches = [plot for plot in self.plots if _norm(plot.name) == wanted] or \
                  [plot for plot in self.plots if wanted in _norm(plot.name)]
        if len(matches) != 1:
            raise ValueError(f"Setor '{setor}' não encontrado ou ambíguo.")
        return matches[0]

    # ------------------------------------------------------------ consultas
    def resumo_safra(self):
        season = self.season
        return {
            "fazenda": self.farm.name,
            "area_total_fazenda_ha": _num(self.farm.total_area),
            "safra": season.name if season else None,
            "periodo": f"{season.start_date:%d/%m/%Y} a {season.end_date:%d/%m/%Y}" if season else None,
            "cultura": getattr(season, "culture", None),
            "status": getattr(season, "status", None),
            "setores": [{"nome": plot.name, "area_ha": _num(plot.area_hectares),
                         "plantas": plot.plant_count, "plantio": plot.planting_date.isoformat() if plot.planting_date else None}
                        for plot in self.plots],
        }

    def consultar_chuva(self, inicio=None, fim=None):
        start, end = self._period(inicio, fim, default_days=30)
        records = self.repo.list_rainfalls(farm_id=self.farm.id, start_date=start, end_date=end)
        by_day = defaultdict(float)
        for record in records:
            by_day[record.rainfall_date] += float(record.millimeters or 0)
        rainy = [(day, mm) for day, mm in by_day.items() if mm > 0]
        last = max(rainy, default=None)
        biggest = max(rainy, key=lambda item: item[1], default=None)
        return {
            "periodo": f"{start:%d/%m/%Y} a {end:%d/%m/%Y}",
            "total_mm": _num(sum(by_day.values()), 1),
            "dias_com_chuva": len(rainy),
            "ultima_chuva": {"data": last[0].strftime("%d/%m/%Y"), "mm": _num(last[1], 1)} if last else None,
            "maior_chuva": {"data": biggest[0].strftime("%d/%m/%Y"), "mm": _num(biggest[1], 1)} if biggest else None,
            "observacao": "Chuva registrada nos pluviômetros da fazenda.",
        }

    def consultar_previsao_tempo(self):
        from app.services.weather_forecast import farm_coordinates, get_farm_forecast, weather_condition

        coords = farm_coordinates(self.farm)
        if not coords:
            return {"erro": "A fazenda não tem contorno nem setores no mapa; a previsão precisa da localização."}
        data = get_farm_forecast(coords[0], coords[1])
        if data.get("status") not in ("ok", "stale"):
            return {"erro": "Previsão do tempo indisponível no momento."}
        return {
            "fonte": "Open-Meteo (modelo ECMWF)",
            "desatualizada": data["status"] == "stale",
            "chuva_total_7_dias_mm": data.get("total_precip"),
            "dias": [{"data": day["label"], "dia": day["weekday"], "icone": icon, "condicao": condition,
                      "max_c": day["tmax"], "min_c": day["tmin"], "chuva_mm": day["precip"], "chance_chuva_pct": day["prob"]}
                     for day in data.get("days", [])
                     for icon, condition in [weather_condition(day.get("code"), day.get("precip"), day.get("prob"))]],
            "comparacao_modelos_mm_7_dias": {m["label"]: m["total"] for m in data.get("models", [])},
        }

    def consultar_aplicacoes(self, inicio=None, fim=None, setor=None):
        start, end = self._period(inicio, fim)
        plot = self._plot_filter(setor)
        rows = []
        for record in self.repo.list_fertilizations():
            if record.plot_id not in self.plot_ids or not record.application_date:
                continue
            if not start <= record.application_date <= end or (plot and record.plot_id != plot.id):
                continue
            rows.append({
                "registro_id": getattr(record, "id", None),
                "data": record.application_date.strftime("%d/%m/%Y"),
                "setor": record.plot.name if record.plot else None,
                "metodo": record.application_method,
                "produtos": [f"{item.name}: {_num(item.total_quantity)} {item.unit or ''}".strip() for item in record.items]
                            or ([record.product] if record.product else []),
                "custo": _num(record.cost),
            })
        return {"periodo": f"{start:%d/%m/%Y} a {end:%d/%m/%Y}", "quantidade": len(rows),
                "custo_total": _num(sum(row["custo"] for row in rows)), "aplicacoes": rows[:MAX_ROWS],
                "observacao": "Mostrando as mais recentes." if len(rows) > MAX_ROWS else None}

    def consultar_colheitas(self, setor=None):
        plot = self._plot_filter(setor)
        per_plot = defaultdict(float)
        rows = []
        for record in self.repo.list_harvests():
            if record.plot_id not in self.plot_ids or (self.season and record.season_id not in (None, self.season.id)):
                continue
            if self.season and record.harvest_date and not self.season.start_date <= record.harvest_date <= self.season.end_date:
                continue
            if plot and record.plot_id != plot.id:
                continue
            name = record.plot.name if record.plot else "Sem setor"
            per_plot[name] += float(record.sacks_produced or 0)
            rows.append({"registro_id": getattr(record, "id", None), "data": record.harvest_date.strftime("%d/%m/%Y") if record.harvest_date else None,
                         "setor": name, "sacas": _num(record.sacks_produced), "tipo": record.harvest_type})
        return {"safra": self.season.name if self.season else None, "total_sacas": _num(sum(per_plot.values())),
                "sacas_por_setor": {name: _num(value) for name, value in per_plot.items()},
                "ultimas_colheitas": rows[:MAX_ROWS]}

    def consultar_consumos(self, inicio=None, fim=None, item=None, setor=None):
        start, end = self._period(inicio, fim, default_days=30)
        plot = self._plot_filter(setor)
        wanted = _norm(item) if item else ""
        totals = defaultdict(float)
        rows = []
        for output in self.repo.list_stock_outputs(farm_id=self.farm.id):
            if not output.movement_date or not start <= output.movement_date <= end:
                continue
            name = output.input_catalog.name if output.input_catalog else "Item"
            if wanted and wanted not in _norm(name):
                continue
            if plot and output.plot_id != plot.id:
                continue
            totals[(name, output.unit)] += float(output.quantity or 0)
            rows.append({"registro_id": getattr(output, "id", None), "data": output.movement_date.strftime("%d/%m/%Y"), "item": name,
                         "quantidade": _num(output.quantity), "unidade": output.unit,
                         "setor": output.plot.name if getattr(output, "plot", None) else "Geral", "custo": _num(output.total_cost)})
        rows.sort(key=lambda row: row["data"][6:] + row["data"][3:5] + row["data"][:2], reverse=True)
        return {"periodo": f"{start:%d/%m/%Y} a {end:%d/%m/%Y}",
                "totais": [{"item": name, "quantidade": _num(qty), "unidade": unit} for (name, unit), qty in totals.items()],
                "custo_total": _num(sum(row["custo"] for row in rows)), "ultimas_saidas": rows[:MAX_ROWS]}

    def consultar_compras(self, inicio=None, fim=None, item=None):
        start, end = self._period(inicio, fim, default_days=30)
        wanted = _norm(item) if item else ""
        rows = []
        for purchase in self.repo.list_purchased_inputs():
            if purchase.farm_id != self.farm.id or not purchase.purchase_date or not start <= purchase.purchase_date <= end:
                continue
            name = purchase.input_catalog.name if purchase.input_catalog else purchase.name
            if wanted and wanted not in _norm(name):
                continue
            rows.append({"registro_id": purchase.id, "data": purchase.purchase_date.strftime("%d/%m/%Y"),
                         "item": name, "quantidade": _num(purchase.quantity_purchased)})
        rows.sort(key=lambda row: row["data"][6:] + row["data"][3:5] + row["data"][:2], reverse=True)
        return {"periodo": f"{start:%d/%m/%Y} a {end:%d/%m/%Y}", "quantidade": len(rows), "compras": rows[:MAX_ROWS]}

    def consultar_saldos_estoque(self, item=None):
        wanted = _norm(item) if item else ""
        balances = defaultdict(float)
        units = {}
        for lot in self.repo.list_purchased_inputs():
            if lot.farm_id != self.farm.id or not lot.input_catalog:
                continue
            name = lot.input_catalog.name
            if wanted and wanted not in _norm(name):
                continue
            balances[name] += float(lot.available_quantity or 0)
            units[name] = lot.input_catalog.default_unit
        rows = sorted(({"item": name, "saldo": _num(qty), "unidade": units[name]} for name, qty in balances.items()),
                      key=lambda row: row["item"])
        return {"itens": rows[:40], "observacao": "Saldo dos lotes desta fazenda."}

    def consultar_financeiro(self, inicio=None, fim=None):
        start, end = self._period(inicio, fim, default_days=30)
        revenue = expense = 0.0
        categories = defaultdict(float)
        for tx in self.repo.list_finance_transactions(farm_id=self.farm.id):
            if not tx.launch_date or not start <= tx.launch_date <= end:
                continue
            amount = float(tx.amount or 0)
            if tx.operation_type == "receita":
                revenue += amount
            else:
                expense += amount
                categories[tx.category or "Sem categoria"] += amount
        top = sorted(categories.items(), key=lambda item: item[1], reverse=True)[:8]
        return {"periodo": f"{start:%d/%m/%Y} a {end:%d/%m/%Y}", "receitas": _num(revenue), "despesas": _num(expense),
                "saldo": _num(revenue - expense), "maiores_despesas_por_categoria": {name: _num(value) for name, value in top},
                "moeda": "BRL"}

    def consultar_cotacao_cafe(self):
        from app.services.coffee_quotes import get_dashboard_coffee_quotes

        quotes = get_dashboard_coffee_quotes(self.repo)
        result = {}
        for kind, quote in (quotes or {}).items():
            if quote:
                result[kind] = {"preco_saca_60kg_brl": _num(quote.price_brl), "data": quote.quote_date.strftime("%d/%m/%Y"),
                                "variacao_dia_pct": _num(quote.variation_day) if quote.variation_day is not None else None}
        return {"fonte": "CEPEA/ESALQ", "cotacoes": result} if result else {"erro": "Cotação indisponível no momento."}

    # ------------------------------------------------------------ catalogo
    def tools(self):
        """[(schema, handler)] das consultas permitidas a este usuario."""
        setor = {"setor": {"type": ["string", "null"], "description": "Nome do setor ou null para todos."}}
        item = {"item": {"type": ["string", "null"], "description": "Nome (ou parte) do item ou null para todos."}}
        catalog = [
            (None, _tool("resumo_safra", "Dados da fazenda e da safra ativa: período, cultura, setores com área, plantas e data de plantio."), self.resumo_safra),
            (PAGE_RAINFALL, _tool("consultar_chuva", "Chuva registrada nos pluviômetros da fazenda num período (padrão: últimos 30 dias).", PERIOD_PROPS), self.consultar_chuva),
            (PAGE_RAINFALL, _tool("consultar_previsao_tempo", "Previsão do tempo dos próximos 7 dias na fazenda: temperatura, chuva e chance de chuva."), self.consultar_previsao_tempo),
            (PAGE_OPERATIONS, _tool("consultar_aplicacoes", "Aplicações/adubações feitas na safra ativa, por período e setor.", {**PERIOD_PROPS, **setor}), self.consultar_aplicacoes),
            (PAGE_PRODUCTION, _tool("consultar_colheitas", "Colheitas da safra ativa: total de sacas e sacas por setor.", setor), self.consultar_colheitas),
            (PAGE_INPUTS, _tool("consultar_consumos", "Saídas de estoque (consumos) da fazenda por período, item e setor (padrão: últimos 30 dias).", {**PERIOD_PROPS, **item, **setor}), self.consultar_consumos),
            (PAGE_INPUTS, _tool("consultar_compras", "Compras de insumos e combustíveis da fazenda por período e item (padrão: últimos 30 dias).", {**PERIOD_PROPS, **item}), self.consultar_compras),
            (PAGE_INPUTS, _tool("consultar_saldos_estoque", "Saldo atual em estoque dos insumos e combustíveis desta fazenda.", item), self.consultar_saldos_estoque),
            (PAGE_FINANCE, _tool("consultar_financeiro", "Receitas, despesas e saldo da fazenda num período (padrão: últimos 30 dias), com as maiores categorias de despesa.", PERIOD_PROPS), self.consultar_financeiro),
            (PAGE_DASHBOARD, _tool("consultar_cotacao_cafe", "Cotação mais recente do café arábica e robusta (CEPEA), por saca de 60 kg."), self.consultar_cotacao_cafe),
        ]
        return [(schema, handler) for page, schema, handler in catalog if page is None or self.allowed(page)]
