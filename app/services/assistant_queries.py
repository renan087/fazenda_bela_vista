"""Ferramentas de CONSULTA do Assistente SiSFarm (somente leitura).

Cada ferramenta so e oferecida a IA quando o usuario tem permissao para o modulo, e todas
ficam presas a fazenda e a safra ativas. Os resultados sao resumidos para caber no contexto.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta

from app.core.permissions_catalog import (
    PAGE_AGENDA, PAGE_AGRONOMIC, PAGE_ASSETS, PAGE_DASHBOARD, PAGE_FINANCE, PAGE_INPUTS,
    PAGE_IRRIGATION, PAGE_MAP, PAGE_OPERATIONS, PAGE_PESTS, PAGE_PRODUCTION, PAGE_PRODUCTIVE_UNIT, PAGE_RAINFALL,
    PAGE_SOIL, PAGE_VARIETIES, BACKUPS_MANAGE, USERS_MANAGE,
)
from sqlalchemy import or_
from sqlalchemy.orm import defer, joinedload, selectinload
from app.models.agricultural_operation import AgriculturalOperation
from app.models.equipment_asset import EquipmentAsset
from app.models.equipment_maintenance import EquipmentMaintenance
from app.models.equipment_maintenance_plan import EquipmentMaintenancePlan
from app.models.fertilization_schedule import FertilizationSchedule
from app.models.irrigation import IrrigationRecord
from app.models.organization_subscription import OrganizationSubscription
from app.models.pesticide import PestIncident
from app.models.soil_analysis import SoilAnalysis

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
    def __init__(self, *, repo, farm, season, plots, today, allowed, allow_system_backup=False, allow_org_admin=False):
        self.repo = repo
        self.farm = farm
        self.season = season
        self.plots = list(plots)
        self.plot_ids = {plot.id for plot in self.plots}
        self.today = today
        self.allowed = allowed  # funcao(page_code) -> bool
        self.allow_system_backup = allow_system_backup
        self.allow_org_admin = allow_org_admin

    # ------------------------------------------------------------ utilitarios
    def _period(self, inicio, fim, default_days=None):
        end = self._date(fim) or (min(self.today, self.season.end_date) if self.season else self.today)
        if inicio:
            start = self._date(inicio)
        elif default_days:
            start = end - timedelta(days=default_days)
        else:
            start = self.season.start_date if self.season else end - timedelta(days=30)
        if self.season:
            if (inicio and start < self.season.start_date) or (fim and end > self.season.end_date):
                raise ValueError("O período solicitado está fora da safra ativa.")
            start = max(start, self.season.start_date)
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

    def _plot_history(self, model, date_field, inicio=None, fim=None, setor=None, search=None, search_fields=()):
        start, end = self._period(inicio, fim)
        plot = self._plot_filter(setor)
        ids = {plot.id} if plot else self.plot_ids
        if not ids:
            return start, end, []
        query = self.repo.db.query(model).filter(model.plot_id.in_(ids), date_field >= start, date_field <= end)
        if model is SoilAnalysis:
            query = query.options(defer(SoilAnalysis.pdf_data))
        if hasattr(model, "farm_id"):
            query = query.filter(model.farm_id == self.farm.id)
        if self.season and hasattr(model, "season_id"):
            query = query.filter(or_(model.season_id.is_(None), model.season_id == self.season.id))
        if search and search_fields:
            term = str(search).strip()[:100].replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            query = query.filter(or_(*(field.ilike(f"%{term}%", escape="\\") for field in search_fields)))
        return start, end, query.order_by(date_field.desc(), model.id.desc()).limit(MAX_ROWS + 1).all()

    @staticmethod
    def _history(rows, start, end):
        return {"periodo": f"{start:%d/%m/%Y} a {end:%d/%m/%Y}", "registros": rows[:MAX_ROWS],
                "mais_resultados": len(rows) > MAX_ROWS}

    def _plot_name(self, plot_id):
        return next((plot.name for plot in self.plots if plot.id == plot_id), None)

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
                        for plot in self.plots[:MAX_ROWS]],
            "mais_setores": len(self.plots) > MAX_ROWS,
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
            if self.season and getattr(record, "season_id", None) not in (None, self.season.id):
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
        rows.sort(key=lambda row: row["data"][6:] + row["data"][3:5] + row["data"][:2], reverse=True)
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
        rows.sort(key=lambda row: (row["data"] or "")[6:] + (row["data"] or "")[3:5] + (row["data"] or "")[:2], reverse=True)
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
            if self.season and getattr(output, "season_id", None) not in (None, self.season.id):
                continue
            if output.plot_id is not None and output.plot_id not in self.plot_ids:
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
                         "item": name, "quantidade_embalagens": _num(purchase.quantity_purchased),
                         "tamanho_embalagem": _num(getattr(purchase, "package_size", None)) if getattr(purchase, "package_size", None) is not None else None,
                         "unidade": getattr(purchase, "package_unit", None),
                         "quantidade_total": _num(getattr(purchase, "total_quantity", None)) if getattr(purchase, "total_quantity", None) is not None else None,
                         "preco_embalagem": _num(getattr(purchase, "unit_price", None)) if getattr(purchase, "unit_price", None) is not None else None,
                         "valor_total": _num(getattr(purchase, "total_cost", None)) if getattr(purchase, "total_cost", None) is not None else None,
                         "fornecedor": getattr(purchase, "supplier_name", None)})
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
            if getattr(tx, "is_reversed", False):
                continue
            if self.season and getattr(tx, "season_id", None) not in (None, self.season.id):
                continue
            if getattr(tx, "plot_id", None) is not None and tx.plot_id not in self.plot_ids:
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

    def consultar_irrigacoes(self, inicio=None, fim=None, setor=None):
        start, end, rows = self._plot_history(IrrigationRecord, IrrigationRecord.irrigation_date, inicio, fim, setor)
        return self._history([{"data": r.irrigation_date.isoformat(), "setor": self._plot_name(r.plot_id),
                               "volume_litros": _num(r.volume_liters), "duracao_minutos": r.duration_minutes,
                               "observacao": (r.notes or "")[:500]} for r in rows], start, end)

    def consultar_pragas(self, inicio=None, fim=None, setor=None, termo=None):
        start, end, rows = self._plot_history(PestIncident, PestIncident.occurrence_date, inicio, fim, setor, termo,
                                              (PestIncident.name, PestIncident.category))
        return self._history([{"data": r.occurrence_date.isoformat(), "setor": self._plot_name(r.plot_id),
                               "nome": r.name, "categoria": r.category, "severidade_registrada": r.severity,
                               "tratamento_registrado": r.treatment, "observacao": (r.notes or "")[:500]}
                              for r in rows], start, end)

    def consultar_analises_solo(self, inicio=None, fim=None, setor=None):
        start, end, rows = self._plot_history(SoilAnalysis, SoilAnalysis.analysis_date, inicio, fim, setor)
        def value(number):
            return _num(number) if number is not None else None
        result = self._history([{"data": r.analysis_date.isoformat(), "setor": self._plot_name(r.plot_id),
                                "laboratorio": r.laboratory, "ph": value(r.ph), "materia_organica": value(r.organic_matter),
                                "fosforo": value(r.phosphorus), "potassio": value(r.potassium), "calcio": value(r.calcium),
                                "magnesio": value(r.magnesium), "aluminio": value(r.aluminum), "ctc": value(r.ctc),
                                "h_mais_al": value(r.h_al), "necessidade_calagem_t_ha": value(r.liming_need_t_ha),
                                "saturacao_bases": value(r.base_saturation),
                                "recomendacao_npk": r.npk_recommendation, "recomendacao_micronutrientes": r.micronutrient_recommendation,
                                "recomendacao_ia": (r.ai_recommendation or "")[:500] or None,
                                "recomendacao_truncada": len(r.ai_recommendation or "") > 500,
                                "data_recomendacao_ia": r.ai_generated_at.isoformat() if r.ai_generated_at else None}
                               for r in rows], start, end)
        result["aviso"] = "Valores cadastrados; consulte o laudo para unidades e validação técnica. Não infira unidades."
        return result

    def consultar_perfil_agronomico(self):
        profile = self.repo.get_agronomic_profile_by_farm(self.farm.id)
        if not profile:
            return {"disponivel": False, "mensagem": "Não há perfil agronômico cadastrado para esta fazenda."}
        return {"disponivel": True, "cultura": profile.culture, "regiao": profile.region, "clima": profile.climate,
                "solo": profile.soil_type, "irrigacao": profile.irrigation_system, "cultivar": profile.cultivar,
                "espacamento_plantio": profile.plant_spacing, "espacamento_gotejo": profile.drip_spacing,
                "sistema_producao": profile.production_system, "altitude_m": profile.altitude_meters,
                "declividade": profile.terrain_slope, "exposicao_solar": profile.sun_exposure,
                "meta_sacas_ha": _num(profile.target_productivity_sacks_per_ha) if profile.target_productivity_sacks_per_ha is not None else None,
                "estagio_cultura": (profile.crop_stage or "")[:500], "adubos_utilizados": (profile.fertilizers_used or "")[:500],
                "pragas_comuns": (profile.common_pests or "")[:500],
                "ultima_calagem": profile.last_liming_date.isoformat() if profile.last_liming_date else None,
                "ultima_gessagem": profile.last_gypsum_date.isoformat() if profile.last_gypsum_date else None,
                "limitacoes_registradas": (profile.current_limitations or "")[:500]}

    def consultar_operacoes_agricolas(self, inicio=None, fim=None, setor=None, termo=None):
        start, end, rows = self._plot_history(AgriculturalOperation, AgriculturalOperation.operation_date, inicio, fim, setor,
                                              termo, (AgriculturalOperation.operation_type,))
        return self._history([{"data": r.operation_date.isoformat(), "setor": self._plot_name(r.plot_id),
                               "operacao": r.operation_type, "responsavel": r.responsible_name,
                               "observacao": (r.notes or "")[:500]} for r in rows], start, end)

    def consultar_agenda(self, inicio=None, fim=None, setor=None):
        if not self.season:
            return {"erro": "Selecione uma safra para consultar os agendamentos."}
        start = self._date(inicio) or max(self.today, self.season.start_date)
        end = self._date(fim) or self.season.end_date
        if start < self.season.start_date or end > self.season.end_date or start > end:
            raise ValueError("O período solicitado está fora da safra ativa.")
        plot = self._plot_filter(setor)
        ids = {plot.id} if plot else self.plot_ids
        rows = (self.repo.db.query(FertilizationSchedule).options(selectinload(FertilizationSchedule.items)).filter(
            FertilizationSchedule.plot_id.in_(ids), FertilizationSchedule.scheduled_date >= start,
            FertilizationSchedule.scheduled_date <= end,
            or_(FertilizationSchedule.season_id.is_(None), FertilizationSchedule.season_id == self.season.id),
        ).order_by(FertilizationSchedule.scheduled_date.asc(), FertilizationSchedule.id.asc()).limit(MAX_ROWS + 1).all()) if ids else []
        return self._history([{"data": r.scheduled_date.isoformat(), "setor": self._plot_name(r.plot_id),
                               "status": r.status, "metodo": r.application_method,
                               "insumos": [{"nome": i.name, "quantidade": _num(i.quantity), "unidade": i.unit} for i in r.items[:10]],
                               "mais_insumos": len(r.items) > 10,
                               "observacao": (r.notes or "")[:500]} for r in rows[:MAX_ROWS + 1]], start, end)

    def consultar_patrimonio(self, termo=None):
        wanted = str(termo or "").strip()
        query = self.repo.db.query(EquipmentAsset).filter(EquipmentAsset.farm_id == self.farm.id)
        if wanted:
            term = wanted[:100].replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            query = query.filter(or_(EquipmentAsset.name.ilike(f"%{term}%", escape="\\"),
                                     EquipmentAsset.category.ilike(f"%{term}%", escape="\\")))
        rows = query.order_by(EquipmentAsset.name.asc(), EquipmentAsset.id.asc()).limit(MAX_ROWS + 1).all()
        return {"registros": [{"nome": r.name, "categoria": r.category, "modelo": r.brand_model,
                               "codigo": r.asset_code, "status": r.status} for r in rows[:MAX_ROWS]],
                "mais_resultados": len(rows) > MAX_ROWS}

    def consultar_manutencoes(self, inicio=None, fim=None, termo=None):
        start, end = self._period(inicio, fim)
        wanted = str(termo or "").strip()
        query = (self.repo.db.query(EquipmentMaintenance).join(EquipmentAsset)
                 .options(joinedload(EquipmentMaintenance.asset))
                 .filter(EquipmentAsset.farm_id == self.farm.id, EquipmentMaintenance.maintenance_date >= start,
                         EquipmentMaintenance.maintenance_date <= end))
        if wanted:
            term = wanted[:100].replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            query = query.filter(or_(EquipmentAsset.name.ilike(f"%{term}%", escape="\\"),
                                     EquipmentMaintenance.description.ilike(f"%{term}%", escape="\\")))
        rows = query.order_by(EquipmentMaintenance.maintenance_date.desc(), EquipmentMaintenance.id.desc()).limit(MAX_ROWS + 1).all()
        return self._history([{"data": r.maintenance_date.isoformat(), "patrimonio": r.asset.name,
                               "tipo": r.maintenance_type, "status": r.status, "descricao": r.description,
                               "proximo_vencimento": r.next_due_date.isoformat() if r.next_due_date else None}
                              for r in rows[:MAX_ROWS + 1]], start, end)

    def consultar_contas(self, inicio=None, fim=None, termo=None):
        start, end = self._period(inicio, fim)
        wanted = _norm(termo) if termo else ""
        rows = []
        for tx in self.repo.list_finance_transactions(farm_id=self.farm.id):
            if tx.farm_id != self.farm.id or tx.is_reversed or (self.season and tx.season_id not in (None, self.season.id)):
                continue
            if tx.plot_id is not None and tx.plot_id not in self.plot_ids:
                continue
            if wanted and wanted not in _norm(tx.product_service) and wanted not in _norm(tx.category):
                continue
            for part in tx.installments or []:
                if part.due_date and start <= part.due_date <= end:
                    rows.append({"vencimento": part.due_date.isoformat(), "descricao": tx.product_service,
                                 "categoria": tx.category, "tipo": tx.operation_type, "parcela": part.installment_number,
                                 "valor": _num(part.amount), "status": part.status,
                                 "forma_pagamento": tx.payment_method, "contraparte": tx.counterparty_name,
                                 "pago_em": part.paid_at.isoformat() if part.paid_at else None})
        rows.sort(key=lambda row: row["vencimento"], reverse=True)
        return self._history(rows, start, end)

    def consultar_comercializacao(self, inicio=None, fim=None, termo=None):
        start, end = self._period(inicio, fim)
        wanted = _norm(termo) if termo else ""
        rows = []
        for sale in self.repo.list_coffee_commercializations(farm_id=self.farm.id):
            if sale.farm_id != self.farm.id or not sale.sale_date or not start <= sale.sale_date <= end:
                continue
            if sale.harvest and (sale.harvest.plot_id not in self.plot_ids or
                                 (self.season and sale.harvest.season_id not in (None, self.season.id))):
                continue
            if not sale.harvest and sale.plot_name and _norm(sale.plot_name) not in {_norm(plot.name) for plot in self.plots}:
                continue
            if wanted and wanted not in _norm(sale.buyer_name) and wanted not in _norm(sale.lot_label):
                continue
            rows.append({"data": sale.sale_date.isoformat(), "comprador": sale.buyer_name, "lote": sale.lot_label,
                         "setor": sale.plot_name, "sacas_equivalentes": _num(sale.equivalent_sacks),
                         "valor_total": _num(sale.total_value), "status": sale.status})
        rows.sort(key=lambda row: row["data"], reverse=True)
        return self._history(rows, start, end)

    def consultar_recomendacoes_insumos(self, setor=None, termo=None):
        plot = self._plot_filter(setor)
        wanted = _norm(termo) if termo else ""
        rows = []
        for rec in self.repo.list_input_recommendations(farm_id=self.farm.id):
            if rec.farm_id != self.farm.id or (rec.plot_id is not None and rec.plot_id not in self.plot_ids):
                continue
            if plot and rec.plot_id != plot.id:
                continue
            if wanted and wanted not in _norm(rec.application_name):
                continue
            rows.append({"nome": rec.application_name, "setor": self._plot_name(rec.plot_id) if rec.plot_id else "Geral",
                         "observacao": (rec.notes or "")[:500],
                         "itens": [{"nome": item.input_catalog.name if item.input_catalog else None,
                                    "quantidade": _num(item.quantity), "unidade": item.unit} for item in rec.items[:15]]})
        return {"registros": rows[:MAX_ROWS], "mais_resultados": len(rows) > MAX_ROWS,
                "observacao": "Recomendações cadastradas na fazenda; o cadastro não possui vínculo direto com safra."}

    def consultar_manutencoes_previstas(self, termo=None):
        wanted = str(termo or "").strip()
        query = (self.repo.db.query(EquipmentMaintenancePlan).join(EquipmentAsset)
                 .options(joinedload(EquipmentMaintenancePlan.asset))
                 .filter(EquipmentAsset.farm_id == self.farm.id, EquipmentMaintenancePlan.is_active.is_(True)))
        if wanted:
            term = wanted[:100].replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            query = query.filter(or_(EquipmentAsset.name.ilike(f"%{term}%", escape="\\"),
                                     EquipmentMaintenancePlan.description.ilike(f"%{term}%", escape="\\")))
        rows = query.order_by(EquipmentMaintenancePlan.next_due_date.asc().nullslast(), EquipmentMaintenancePlan.id.asc()).limit(MAX_ROWS + 1).all()
        return {"registros": [{"patrimonio": r.asset.name, "descricao": r.description,
                               "proximo_vencimento": r.next_due_date.isoformat() if r.next_due_date else None,
                               "proximas_horas": _num(r.next_due_hours) if r.next_due_hours is not None else None}
                              for r in rows[:MAX_ROWS]], "mais_resultados": len(rows) > MAX_ROWS}

    def consultar_variedades_fazenda(self):
        rows = [{"setor": plot.name, "variedade": plot.variety.name if plot.variety else None,
                 "especie": plot.variety.species if plot.variety else None} for plot in self.plots[:MAX_ROWS]]
        return {"registros": rows, "mais_resultados": len(self.plots) > MAX_ROWS,
                "observacao": "Variedade atualmente cadastrada em cada setor da safra ativa."}

    def consultar_fazenda(self):
        return {"nome": self.farm.name, "localizacao": self.farm.location,
                "area_total_ha": _num(self.farm.total_area) if self.farm.total_area is not None else None,
                "car": self.farm.car_registration_number, "ccir": self.farm.ccir_number,
                "itr": self.farm.itr_registration_number, "matricula": self.farm.property_registration_number,
                "cartorio": self.farm.registry_office,
                "safra": self.season.name if self.season else None,
                "setores_da_safra": [{"nome": plot.name, "area_ha": _num(plot.area_hectares)} for plot in self.plots[:MAX_ROWS]],
                "mais_setores": len(self.plots) > MAX_ROWS}

    def consultar_mapa_fazenda(self):
        return {"fazenda": self.farm.name, "perimetro_cadastrado": bool(self.farm.boundary_geojson),
                "setores": [{"nome": plot.name, "perimetro_cadastrado": bool(plot.boundary_geojson),
                             "latitude_centro": float(plot.centroid_lat) if plot.centroid_lat is not None else None,
                             "longitude_centro": float(plot.centroid_lng) if plot.centroid_lng is not None else None}
                            for plot in self.plots[:MAX_ROWS]], "mais_setores": len(self.plots) > MAX_ROWS,
                "observacao": "Coordenadas são centros cadastrados dos setores, não os limites do imóvel."}

    def consultar_inventarios(self, inicio=None, fim=None):
        start, end = self._period(inicio, fim)
        rows = [r for r in self.repo.list_stock_inventory_sessions(farm_id=self.farm.id)
                if r.farm_id == self.farm.id and r.inventory_date and start <= r.inventory_date <= end]
        return self._history([{"data": r.inventory_date.isoformat(), "status": r.status,
                               "responsavel": r.responsible_name, "itens": len(r.items),
                               "observacao": (r.notes or "")[:500]} for r in rows[:MAX_ROWS + 1]], start, end)

    def consultar_usuarios_organizacao(self, termo=None):
        if not self.farm.organization_id:
            return {"erro": "A fazenda não possui organização vinculada."}
        wanted = _norm(termo) if termo else ""
        users = self.repo.list_users(organization_id=self.farm.organization_id)
        users = [u for u in users if u.organization_id == self.farm.organization_id
                 and (not wanted or wanted in _norm(u.name))]
        return {"registros": [{"nome": user.name, "ativo": user.is_active} for user in users[:MAX_ROWS]],
                "mais_resultados": len(users) > MAX_ROWS}

    def consultar_backups_sistema(self):
        runs = self.repo.list_backup_runs(limit=MAX_ROWS + 1)
        return {"escopo": "Backup do sistema inteiro, não apenas da fazenda ativa.",
                "registros": [{"inicio": r.started_at.isoformat() if r.started_at else None,
                               "termino": r.finished_at.isoformat() if r.finished_at else None,
                               "status": r.status, "origem": r.trigger_source,
                               "registro_indica_arquivo_no_storage": bool(r.database_object_path) and r.deleted_from_storage_at is None}
                              for r in runs[:MAX_ROWS]], "mais_resultados": len(runs) > MAX_ROWS,
                "observacao": "A disponibilidade reflete o histórico local e não confirma a presença atual no Storage."}

    def consultar_organizacao_atual(self):
        organization = self.repo.get_organization(self.farm.organization_id) if self.farm.organization_id else None
        if not organization:
            return {"erro": "A organização da fazenda não está disponível."}
        return {"nome": organization.name, "ativa": organization.is_active,
                "observacao": "Somente a organização da fazenda ativa."}

    def consultar_assinatura(self):
        from app.services.asaas_environment import asaas_env_name
        if not self.farm.organization_id:
            return {"erro": "A fazenda não possui organização vinculada."}
        subscription = (self.repo.db.query(OrganizationSubscription).filter_by(
            organization_id=self.farm.organization_id, environment=asaas_env_name(), is_current=True).first())
        if not subscription:
            return {"disponivel": False, "mensagem": "Nenhuma assinatura atual cadastrada para a organização."}
        return {"disponivel": True, "plano": subscription.plan_name, "valor": _num(subscription.amount),
                "periodicidade": subscription.cycle, "forma_pagamento": subscription.billing_type,
                "status": subscription.status, "status_asaas": subscription.asaas_status,
                "proximo_vencimento": subscription.next_due_date.isoformat() if subscription.next_due_date else None,
                "observacao": "Dados da última sincronização; assinatura ativa não confirma pagamento."}

    # ------------------------------------------------------------ catalogo
    def tools(self):
        """[(schema, handler)] das consultas permitidas a este usuario."""
        setor = {"setor": {"type": ["string", "null"], "description": "Nome do setor ou null para todos."}}
        item = {"item": {"type": ["string", "null"], "description": "Nome (ou parte) do item ou null para todos."}}
        termo = {"termo": {"type": ["string", "null"], "description": "Nome ou trecho para buscar, ou null para todos."}}
        catalog = [
            (PAGE_PRODUCTIVE_UNIT, _tool("resumo_safra", "Dados da fazenda e da safra ativa: período, cultura, setores com área, plantas e data de plantio."), self.resumo_safra),
            (PAGE_RAINFALL, _tool("consultar_chuva", "Chuva registrada nos pluviômetros da fazenda num período (padrão: últimos 30 dias).", PERIOD_PROPS), self.consultar_chuva),
            (PAGE_RAINFALL, _tool("consultar_previsao_tempo", "Previsão do tempo dos próximos 7 dias na fazenda: temperatura, chuva e chance de chuva."), self.consultar_previsao_tempo),
            (PAGE_OPERATIONS, _tool("consultar_aplicacoes", "Aplicações/adubações feitas na safra ativa, por período e setor.", {**PERIOD_PROPS, **setor}), self.consultar_aplicacoes),
            (PAGE_PRODUCTION, _tool("consultar_colheitas", "Colheitas da safra ativa: total de sacas e sacas por setor.", setor), self.consultar_colheitas),
            (PAGE_INPUTS, _tool("consultar_consumos", "Saídas de estoque (consumos) da fazenda por período, item e setor (padrão: últimos 30 dias).", {**PERIOD_PROPS, **item, **setor}), self.consultar_consumos),
            (PAGE_INPUTS, _tool("consultar_compras", "Compras de insumos e combustíveis da fazenda por período e item (padrão: últimos 30 dias).", {**PERIOD_PROPS, **item}), self.consultar_compras),
            (PAGE_INPUTS, _tool("consultar_saldos_estoque", "Saldo atual em estoque dos insumos e combustíveis desta fazenda.", item), self.consultar_saldos_estoque),
            (PAGE_FINANCE, _tool("consultar_financeiro", "Receitas, despesas e saldo da fazenda num período (padrão: últimos 30 dias), com as maiores categorias de despesa.", PERIOD_PROPS), self.consultar_financeiro),
            (PAGE_DASHBOARD, _tool("consultar_cotacao_cafe", "Cotação mais recente do café arábica e robusta (CEPEA), por saca de 60 kg."), self.consultar_cotacao_cafe),
            (PAGE_IRRIGATION, _tool("consultar_irrigacoes", "Histórico de irrigações, volumes e duração na safra ativa.", {**PERIOD_PROPS, **setor}), self.consultar_irrigacoes),
            (PAGE_PESTS, _tool("consultar_pragas", "Histórico de pragas e doenças registradas na safra ativa.", {**PERIOD_PROPS, **setor, **termo}), self.consultar_pragas),
            (PAGE_SOIL, _tool("consultar_analises_solo", "Análises de solo cadastradas na safra ativa, resultados e recomendação gravada.", {**PERIOD_PROPS, **setor}), self.consultar_analises_solo),
            (PAGE_AGRONOMIC, _tool("consultar_perfil_agronomico", "Dados do perfil agronômico cadastrado da fazenda ativa."), self.consultar_perfil_agronomico),
            (PAGE_OPERATIONS, _tool("consultar_operacoes_agricolas", "Histórico de operações agrícolas da safra ativa por setor ou tipo.", {**PERIOD_PROPS, **setor, **termo}), self.consultar_operacoes_agricolas),
            (PAGE_AGENDA, _tool("consultar_agenda", "Agendamentos de fertilização por período e setor na safra ativa; padrão: hoje até o fim da safra.", {**PERIOD_PROPS, **setor}), self.consultar_agenda),
            (PAGE_ASSETS, _tool("consultar_patrimonio", "Patrimônio cadastrado na fazenda ativa, com busca por nome ou categoria.", termo), self.consultar_patrimonio),
            (PAGE_ASSETS, _tool("consultar_manutencoes", "Histórico de manutenções do patrimônio da fazenda na safra ativa.", {**PERIOD_PROPS, **termo}), self.consultar_manutencoes),
            (PAGE_ASSETS, _tool("consultar_manutencoes_previstas", "Planos ativos e próximos vencimentos de manutenção do patrimônio da fazenda.", termo), self.consultar_manutencoes_previstas),
            (PAGE_FINANCE, _tool("consultar_contas", "Parcelas de contas da fazenda por período de vencimento e descrição, na safra ativa.", {**PERIOD_PROPS, **termo}), self.consultar_contas),
            (PAGE_FINANCE, _tool("consultar_assinatura", "Plano, situação e próximo vencimento da assinatura da organização da fazenda ativa."), self.consultar_assinatura),
            (PAGE_PRODUCTION, _tool("consultar_comercializacao", "Vendas de café registradas na safra ativa, com busca por comprador ou lote.", {**PERIOD_PROPS, **termo}), self.consultar_comercializacao),
            (PAGE_OPERATIONS, _tool("consultar_recomendacoes_insumos", "Recomendações de insumos cadastradas para a fazenda e setores da safra.", {**setor, **termo}), self.consultar_recomendacoes_insumos),
            (PAGE_VARIETIES, _tool("consultar_variedades_fazenda", "Variedades atualmente cadastradas nos setores da safra ativa."), self.consultar_variedades_fazenda),
            (PAGE_PRODUCTIVE_UNIT, _tool("consultar_fazenda", "Cadastro e área da fazenda e setores da safra ativa."), self.consultar_fazenda),
            (PAGE_MAP, _tool("consultar_mapa_fazenda", "Existência de perímetros e coordenadas centrais cadastradas dos setores da safra."), self.consultar_mapa_fazenda),
            (PAGE_INPUTS, _tool("consultar_inventarios", "Histórico das contagens de estoque da fazenda na safra ativa.", PERIOD_PROPS), self.consultar_inventarios),
            (USERS_MANAGE, _tool("consultar_usuarios_organizacao", "Usuários da organização da fazenda ativa, com busca por nome.", termo), self.consultar_usuarios_organizacao),
        ]
        if self.allow_system_backup:
            catalog.append((BACKUPS_MANAGE, _tool("consultar_backups_sistema", "Últimas execuções de backup do sistema; consulta administrativa global."), self.consultar_backups_sistema))
        if self.allow_org_admin:
            catalog.append((None, _tool("consultar_organizacao_atual", "Organização da fazenda ativa, para administrador geral."), self.consultar_organizacao_atual))
        return [(schema, handler) for page, schema, handler in catalog if page is None or self.allowed(page)]
