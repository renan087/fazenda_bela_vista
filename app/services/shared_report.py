"""Componentes tipados para compartilhar resultados verificados de consultas do Pezinho.

O conteúdo da IA nunca é interpretado como HTML nem determina campos ou URLs.
"""
from __future__ import annotations

import math
import base64
import hashlib
import json
import secrets
from datetime import datetime, timedelta, timezone

from app.core.permissions_catalog import (
    PAGE_AGENDA, PAGE_AGRONOMIC, PAGE_ASSETS, PAGE_DASHBOARD, PAGE_FINANCE, PAGE_INPUTS, PAGE_IRRIGATION,
    PAGE_MAP, PAGE_OPERATIONS, PAGE_PESTS, PAGE_PRODUCTION, PAGE_PRODUCTIVE_UNIT, PAGE_RAINFALL,
    PAGE_SOIL, PAGE_VARIETIES,
)
from app.models.shared_report import SharedReport

# Colunas e permissões são controladas pelo servidor, não pelo modelo.
REPORTS = {
    "consultar_compras": (PAGE_INPUTS, "Compras de insumos", "compras", (
        ("data", "Data", "text"), ("item", "Item", "text"), ("quantidade_total", "Quantidade", "number"),
        ("unidade", "Unidade", "text"), ("valor_total", "Valor", "currency"), ("fornecedor", "Fornecedor", "text"))),
    "consultar_consumos": (PAGE_INPUTS, "Consumos de insumos", "ultimas_saidas", (
        ("data", "Data", "text"), ("item", "Item", "text"), ("quantidade", "Quantidade", "number"),
        ("unidade", "Unidade", "text"), ("setor", "Setor", "text"), ("custo", "Custo", "currency"))),
    "consultar_saldos_estoque": (PAGE_INPUTS, "Saldo de estoque", "itens", (
        ("item", "Item", "text"), ("saldo", "Saldo", "number"), ("unidade", "Unidade", "text"))),
    "consultar_aplicacoes": (PAGE_OPERATIONS, "Aplicações", "aplicacoes", (
        ("data", "Data", "text"), ("setor", "Setor", "text"), ("metodo", "Método", "text"),
        ("produtos", "Produtos", "text_list"), ("custo", "Custo", "currency"))),
    "consultar_colheitas": (PAGE_PRODUCTION, "Colheitas", "ultimas_colheitas", (
        ("data", "Data", "text"), ("setor", "Setor", "text"), ("sacas", "Sacas", "number"), ("tipo", "Tipo", "text"))),
    "consultar_irrigacoes": (PAGE_IRRIGATION, "Irrigações", "registros", (
        ("data", "Data", "text"), ("setor", "Setor", "text"), ("volume_litros", "Volume (L)", "number"),
        ("duracao_minutos", "Duração (min)", "number"))),
    "consultar_pragas": (PAGE_PESTS, "Pragas e doenças", "registros", (
        ("data", "Data", "text"), ("setor", "Setor", "text"), ("nome", "Ocorrência", "text"),
        ("categoria", "Categoria", "text"), ("severidade_registrada", "Severidade", "number"))),
    "consultar_analises_solo": (PAGE_SOIL, "Análises de solo", "registros", (
        ("data", "Data", "text"), ("setor", "Setor", "text"), ("laboratorio", "Laboratório", "text"),
        ("ph", "pH", "number"), ("fosforo", "Fósforo", "number"), ("potassio", "Potássio", "number"),
        ("calcio", "Cálcio", "number"), ("magnesio", "Magnésio", "number"),
        ("saturacao_bases", "Saturação de bases", "number"))),
    "consultar_agenda": (PAGE_AGENDA, "Agenda de fertilização", "registros", (
        ("data", "Data", "text"), ("setor", "Setor", "text"), ("status", "Status", "text"), ("metodo", "Método", "text"))),
    "consultar_patrimonio": (PAGE_ASSETS, "Patrimônio", "registros", (
        ("nome", "Nome", "text"), ("categoria", "Categoria", "text"), ("modelo", "Modelo", "text"), ("status", "Status", "text"))),
    "consultar_contas": (PAGE_FINANCE, "Contas", "registros", (
        ("vencimento", "Vencimento", "text"), ("descricao", "Descrição", "text"), ("tipo", "Tipo", "text"),
        ("parcela", "Parcela", "number"), ("valor", "Valor", "currency"), ("status", "Status", "text"))),
    "consultar_comercializacao": (PAGE_PRODUCTION, "Vendas de café", "registros", (
        ("data", "Data", "text"), ("comprador", "Comprador", "text"), ("lote", "Lote", "text"),
        ("sacas_equivalentes", "Sacas", "number"), ("valor_total", "Valor", "currency"), ("status", "Status", "text"))),
    "consultar_operacoes_agricolas": (PAGE_OPERATIONS, "Operações agrícolas", "registros", (
        ("data", "Data", "text"), ("setor", "Setor", "text"), ("operacao", "Operação", "text"), ("responsavel", "Responsável", "text"))),
    "consultar_manutencoes": (PAGE_ASSETS, "Manutenções", "registros", (
        ("data", "Data", "text"), ("patrimonio", "Patrimônio", "text"), ("descricao", "Descrição", "text"), ("status", "Status", "text"))),
    "consultar_manutencoes_previstas": (PAGE_ASSETS, "Manutenção preventiva", "registros", (
        ("patrimonio", "Patrimônio", "text"), ("descricao", "Descrição", "text"), ("proximo_vencimento", "Vencimento", "text"))),
    "consultar_inventarios": (PAGE_INPUTS, "Inventários", "registros", (
        ("data", "Data", "text"), ("responsavel", "Responsável", "text"), ("itens", "Itens", "number"), ("status", "Status", "text"))),
    "consultar_recomendacoes_insumos": (PAGE_OPERATIONS, "Recomendações de insumos", "registros", (
        ("nome", "Recomendação", "text"), ("setor", "Setor", "text"), ("observacao", "Observação", "text"))),
    "consultar_variedades_fazenda": (PAGE_VARIETIES, "Variedades por setor", "registros", (
        ("setor", "Setor", "text"), ("variedade", "Variedade", "text"), ("especie", "Espécie", "text"))),
    "consultar_mapa_fazenda": (PAGE_MAP, "Setores no mapa", "setores", (
        ("nome", "Setor", "text"), ("latitude_centro", "Latitude do centro", "number"),
        ("longitude_centro", "Longitude do centro", "number"))),
    "consultar_previsao_tempo": (PAGE_RAINFALL, "Previsão do tempo", "dias", (
        ("data", "Data", "text"), ("condicao", "Condição", "text"), ("min_c", "Mínima (°C)", "number"),
        ("max_c", "Máxima (°C)", "number"), ("chuva_mm", "Chuva (mm)", "number"),
        ("chance_chuva_pct", "Chance de chuva (%)", "number"))),
    "consultar_chuva": (PAGE_RAINFALL, "Chuva registrada", None, (
        ("total_mm", "Total (mm)", "number"), ("dias_com_chuva", "Dias com chuva", "number"))),
    "consultar_fazenda": (PAGE_PRODUCTIVE_UNIT, "Fazenda", None, (
        ("nome", "Fazenda", "text"), ("localizacao", "Localização", "text"), ("area_total_ha", "Área (ha)", "number"))),
    "consultar_perfil_agronomico": (PAGE_AGRONOMIC, "Perfil agronômico", None, (
        ("cultura", "Cultura", "text"), ("regiao", "Região", "text"), ("solo", "Solo", "text"),
        ("cultivar", "Cultivar", "text"), ("sistema_producao", "Sistema", "text"), ("altitude_m", "Altitude (m)", "number"))),
    "consultar_assinatura": (PAGE_FINANCE, "Assinatura", None, (
        ("plano", "Plano", "text"), ("valor", "Valor", "currency"), ("periodicidade", "Periodicidade", "text"),
        ("status", "Status", "text"), ("proximo_vencimento", "Próximo vencimento", "text"))),
    "resumo_safra": (PAGE_PRODUCTIVE_UNIT, "Setores da safra", "setores", (
        ("nome", "Setor", "text"), ("area_ha", "Área (ha)", "number"), ("plantas", "Plantas", "number"), ("plantio", "Plantio", "text"))),
    "consultar_cotacao_cafe": (PAGE_DASHBOARD, "Cotação do café", "cotacoes", (
        ("tipo", "Tipo", "text"), ("data", "Data", "text"), ("preco_saca_60kg_brl", "Preço por saca", "currency"),
        ("variacao_dia_pct", "Variação (%)", "number"))),
}


def _text(value, max_length=160):
    return str(value or "").strip()[:max_length]


def _url_cipher():
    from cryptography.fernet import Fernet
    from app.core.config import get_settings
    key = hashlib.sha256(("assistant-shared-url:" + get_settings().secret_key).encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(key))


def seal_shared_url(url):
    return _url_cipher().encrypt(url.encode("utf-8")).decode("ascii")


def unseal_shared_url(value):
    from cryptography.fernet import InvalidToken
    try:
        return _url_cipher().decrypt(value.encode("ascii"), ttl=300).decode("utf-8")
    except (InvalidToken, ValueError, UnicodeError, AttributeError):
        return None


def _number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and abs(number) <= 10**15 else None


def _cell(value, kind):
    if kind in {"number", "currency"}:
        return _number(value)
    if kind == "text_list":
        return "; ".join(_text(item, 80) for item in value[:10])[:400] if isinstance(value, list) else ""
    return _text(value)


def build_report(tool_name, result, *, farm_name, season_name):
    """Monta dados limitados e reutilizáveis a partir de uma resposta de ferramenta."""
    if tool_name not in REPORTS or not isinstance(result, dict) or result.get("erro"):
        raise ValueError("Esta consulta não está disponível para compartilhamento.")
    permission, title, row_key, columns = REPORTS[tool_name]
    raw = ([] if result.get("disponivel") is False else [result]) if row_key is None else result.get(row_key)
    if tool_name == "consultar_cotacao_cafe" and isinstance(raw, dict):
        raw = [{**value, "tipo": key} for key, value in raw.items() if isinstance(value, dict)]
    if not isinstance(raw, list):
        raise ValueError("A consulta não retornou registros compartilháveis.")
    rows = [[_cell(row.get(key), kind) for key, _, kind in columns]
            for row in raw[:15] if isinstance(row, dict)]
    chart_field = {"consultar_irrigacoes": ("volume_litros", "Volume registrado (L)"),
                   "consultar_colheitas": ("sacas", "Produção registrada (sacas)")}.get(tool_name)
    chart = None
    if chart_field:
        points = []
        for row in raw[:8]:
            if not isinstance(row, dict):
                continue
            value = _number(row.get(chart_field[0]))
            if value is not None and value >= 0:
                points.append({"label": _text(" · ".join(str(row.get(key) or "") for key in ("data", "setor")).strip(" ·"), 80), "value": value})
        maximum = max((point["value"] for point in points), default=0)
        if points:
            chart = {"title": chart_field[1], "bars": [{**point, "width": round(point["value"] / maximum * 100) if maximum else 0} for point in points]}
    return {"version": 1, "tool": tool_name, "permission": permission, "title": title,
            "farm": _text(farm_name), "season": _text(season_name), "period": _text(result.get("periodo")),
            "columns": [{"label": label, "kind": kind} for _, label, kind in columns], "rows": rows,
            "chart": chart,
            "note": "Previsão desatualizada." if result.get("desatualizada") else _text(result.get("aviso") or result.get("observacao"), 300),
            "truncated": len(raw) > 15 or bool(result.get("mais_resultados")) or bool(result.get("mais_setores")) or
                         (isinstance(result.get("quantidade"), int) and result["quantidade"] > len(rows))}


def build_financial_report(result, *, farm_name, season_name):
    if not isinstance(result, dict) or result.get("erro"):
        raise ValueError("O resumo financeiro não está disponível.")
    income, expense, balance = (_number(result.get(key)) for key in ("receitas", "despesas", "saldo"))
    if any(value is None for value in (income, expense, balance)) or income < 0 or expense < 0:
        raise ValueError("O resumo financeiro está incompleto.")
    maximum = max(income, expense)
    return {"version": 1, "tool": "consultar_financeiro", "permission": PAGE_FINANCE,
            "title": "Resumo financeiro", "farm": _text(farm_name), "season": _text(season_name),
            "period": _text(result.get("periodo")), "balance": balance,
            "bars": [{"label": label, "value": value, "width": round(value / maximum * 100) if maximum else 0}
                     for label, value in (("Receitas", income), ("Despesas", expense))]}


def create_shared_report(db, *, user, farm, season, tool_name, result, whatsapp_link=None):
    """Persiste apenas uma projeção permitida; não aceita HTML ou instruções de layout."""
    from app.services.whatsapp_record_access import link_binding

    if farm.organization_id != user.organization_id or season.farm_id != farm.id or not user.organization_id:
        raise ValueError("Contexto indisponível para compartilhamento.")
    if tool_name == "consultar_financeiro":
        spec = build_financial_report(result, farm_name=farm.name, season_name=season.name)
    else:
        spec = build_report(tool_name, result, farm_name=farm.name, season_name=season.name)
    binding = link_binding(whatsapp_link) if whatsapp_link else None
    if whatsapp_link and (not binding or whatsapp_link.user_id != user.id or whatsapp_link.farm_id != farm.id
                          or whatsapp_link.season_id != season.id or whatsapp_link.organization_id != user.organization_id):
        raise ValueError("Vínculo do WhatsApp indisponível.")
    raw = json.dumps(spec, ensure_ascii=False, separators=(",", ":"))
    if len(raw.encode("utf-8")) > 32768:
        raise ValueError("O relatório excede o limite de compartilhamento.")
    now = datetime.now(timezone.utc)
    token = secrets.token_urlsafe(32)
    row = SharedReport(token_hash=hashlib.sha256(token.encode("ascii")).hexdigest(), user_id=user.id,
                       organization_id=user.organization_id, farm_id=farm.id, season_id=season.id,
                       whatsapp_link_id=whatsapp_link.id if whatsapp_link else None, whatsapp_binding=binding,
                       tool_name=tool_name, data_json=raw, created_at=now, expires_at=now + timedelta(hours=48))
    db.add(row)
    db.flush()
    return token, row


def find_shared_report(db, token, *, lock=False):
    if not isinstance(token, str) or len(token) != 43 or not all(c.isascii() and (c.isalnum() or c in "-_") for c in token):
        return None
    digest = hashlib.sha256(token.encode("ascii")).hexdigest()
    query = db.query(SharedReport).filter(SharedReport.token_hash == digest, SharedReport.revoked_at.is_(None),
                                          SharedReport.expires_at > datetime.now(timezone.utc))
    return (query.with_for_update() if lock else query).first()


def authorize_shared_report(db, report):
    """Revalida o proprietário e o módulo a cada abertura, sem criar sessão de usuário."""
    from app.models import CropSeason, Farm, User
    from app.models.whatsapp import WhatsAppLink
    from app.services.whatsapp_record_access import link_binding
    from app.web import routes

    user, farm, season = db.get(User, report.user_id), db.get(Farm, report.farm_id), db.get(CropSeason, report.season_id)
    if (not user or not user.is_active or user.organization_id != report.organization_id or not user.organization
            or not user.organization.is_active or not farm or farm.organization_id != report.organization_id
            or not season or season.farm_id != farm.id):
        return False
    if not routes._can_use_stock_assistant(db, user):
        return False
    permission = PAGE_FINANCE if report.tool_name == "consultar_financeiro" else REPORTS.get(report.tool_name, (None,))[0]
    if not permission or permission not in routes.permission_codes_for_user(db, user) or not routes._entitlements_for(db, user).module_allowed(permission):
        return False
    if report.whatsapp_link_id:
        link = db.get(WhatsAppLink, report.whatsapp_link_id)
        if (not link or (link.user_id, link.organization_id, link.farm_id, link.season_id) !=
                (report.user_id, report.organization_id, report.farm_id, report.season_id)
                or not report.whatsapp_binding or not secrets.compare_digest(report.whatsapp_binding, link_binding(link))):
            return False
    return True
