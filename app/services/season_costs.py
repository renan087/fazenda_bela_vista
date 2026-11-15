"""Custo da safra detalhado: classificacao inteligente dos lancamentos.

Usa as mesmas fontes e filtros de ``build_sector_cost_context`` (saidas de
estoque e despesas de Contas/Operacoes vinculadas a setores da safra), para que
o total bata com o custo exibido no Dashboard. Cada lancamento recebe uma
categoria em tres niveis (grupo > subgrupo > item), uma explicacao e um grau de
certeza. Correcoes do usuario ficam em ``CostClassification`` e tambem ensinam o
sistema a classificar lancamentos parecidos.
"""

from __future__ import annotations

import re
import unicodedata
from collections import Counter, defaultdict
from datetime import date

GROUPS: list[dict] = [
    {"code": "insumos", "name": "Insumos", "color": "#446a36", "subs": [
        ("fertilizantes", "Fertilizantes"), ("defensivos", "Defensivos"), ("corretivos", "Corretivos de solo"),
        ("sementes", "Sementes e mudas"), ("outros_insumos", "Outros insumos")]},
    {"code": "mao_obra", "name": "Mão de obra", "color": "#7fa86a", "subs": [
        ("colheita", "Colheita"), ("diarias", "Diárias e empreitas"), ("salarios", "Salários"), ("encargos", "Encargos")]},
    {"code": "maquinas", "name": "Máquinas e combustível", "color": "#a9bf9e", "subs": [
        ("combustivel", "Combustível"), ("lubrificantes", "Lubrificantes"), ("manutencao", "Manutenção e peças"),
        ("aluguel", "Aluguel de máquinas"), ("outros_maquinas", "Outros custos de máquinas")]},
    {"code": "outros", "name": "Outros", "color": "#5e7558", "subs": [
        ("energia", "Energia e irrigação"), ("frete", "Fretes e transporte"), ("servicos", "Serviços terceirizados"),
        ("assistencia", "Assistência técnica"), ("secagem", "Secagem e beneficiamento"),
        ("administrativo", "Administrativo e taxas"), ("nao_identificado", "Não identificados")]},
]
SUB_INDEX = {sub: (group["code"], name) for group in GROUPS for sub, name in group["subs"]}
GROUP_INDEX = {group["code"]: group for group in GROUPS}
UNIDENTIFIED = ("outros", "nao_identificado")


def normalize(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9%/\-\. ]+", " ", text)).strip()


def _number(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


# Categorias ja usadas no cadastro (lancamentos e catalogo de insumos).
CATEGORY_MAP = {
    "fertilizantes": "fertilizantes", "feritlizantes": "fertilizantes",
    "fungicida": "defensivos", "herbicida": "defensivos", "inseticida": "defensivos", "acaricida": "defensivos",
    "outros defensivos": "defensivos", "adjuvante": "defensivos",
    "corretivos de solo": "corretivos", "sementes e mudas": "sementes", "outros custos de insumos": "outros_insumos",
    "combustiveis": "combustivel", "lubrificantes": "lubrificantes",
    "manutencao de maquinas e equipamentos": "manutencao", "pecas de maquinas e equipamentos": "manutencao",
    "outros custos de maquinas": "outros_maquinas",
    "preparo do solo": "servicos", "servicos terceirizados": "servicos",
    "fretes": "frete", "transportes internos": "frete",
    "irrigacao": "energia", "energia eletrica": "energia", "compra de agua": "energia",
    "secagem de graos": "secagem", "encargos sociais": "encargos",
    "seguros": "administrativo", "taxas": "administrativo", "impostos iptu itr etc.": "administrativo",
    "comunicacao telefone intenert": "administrativo", "outras despesas administrativas": "administrativo",
    "despesas com viagens": "administrativo", "arrendamento de terra": "administrativo",
    "manutencao de benfeitorias": "manutencao", "comercializacao": "administrativo",
}
GENERIC_CATEGORIES = {"outros itens", "despesas diversas", "geral", ""}

# Palavras-chave: (regex, subgrupo). A ordem importa: a primeira que casar vence.
KEYWORD_RULES: list[tuple[re.Pattern, str]] = [(re.compile(p), s) for p, s in [
    (r"\b(inss|fgts|encargo|gps)\b", "encargos"),
    (r"salari|folha de pag|\bferias\b|13o|decimo terceiro|rescis|holerite", "salarios"),
    (r"(diari|diarista|mao de obra|empreit|boia.?fria|trabalhador|apanha|panha|derri[cç]a|catacao|colhedor\b|colheita manual)", "_labor"),
    (r"diesel|gasolina|etanol|combustiv|\bs-?10\b|\barla\b", "combustivel"),
    (r"lubrific|graxa|oleo (de )?motor|oleo hidraul", "lubrificantes"),
    (r"(aluguel|locac|hora.?maquina).*(maquin|trator|colhedora|recolhedora|implement|retro|pulveriz)|hora.?maquina", "aluguel"),
    (r"oficina|revis[aã]o|\bpecas?\b|pneu|manuten|conserto|mecanic|retifica|solda|rolamento|filtro", "manutencao"),
    (r"frete|transport|carreto|\bct-?e\b", "frete"),
    (r"energia|cemig|copel|cpfl|enel|eletric|conta de luz|irrigac|gotejo|aspersor", "energia"),
    (r"agronom|consultor|assistencia tecnica|\bart\b|laudo|analise de solo|analise foliar", "assistencia"),
    (r"secag|secador|benefici|lenha|cavaco|armazenag", "secagem"),
    (r"ureia|\bkcl\b|cloreto de potassio|\bmap\b|\bnpk\b|adubo|aduba|fertiliz|foliar|\bboro\b|zinco|sulfato de amonio|nitrato|superfosfato|\b\d{1,2}-\d{1,2}-\d{1,2}\b|micronutri", "fertilizantes"),
    (r"fungicid|inseticid|herbicid|acaricid|nematicid|glifosato|cupric|cobre|triazol|estrobil|defensiv|adjuvante|espalhante", "defensivos"),
    (r"calcario|gesso|corretivo|calagem", "corretivos"),
    (r"semente|muda", "sementes"),
    (r"seguro|imposto|\bitr\b|\btaxa\b|cartorio|contador|contabil|telefone|internet", "administrativo"),
]]
HARVEST_WORDS = re.compile(r"colheit|apanha|panha|derri|catacao|colhedor")
LOW_CONFIDENCE = 70


def _keyword_sub(text: str) -> tuple[str | None, str | None]:
    for pattern, sub in KEYWORD_RULES:
        match = pattern.search(text)
        if match:
            if sub == "_labor":
                sub = "colheita" if HARVEST_WORDS.search(text) else "diarias"
            return sub, match.group(0).strip()
    return None, None


def _within(value: date | None, start: date | None, end: date | None) -> bool:
    if value is None:
        return False
    if start and value < start:
        return False
    if end and value > end:
        return False
    return True


def _fmt_qty(value, unit) -> str:
    qty = _number(value)
    text = f"{qty:,.2f}".rstrip("0").rstrip(".").replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{text} {unit or ''}".strip()


def collect_entries(repository, *, farm_id: int | None, season, plots: list) -> list[dict]:
    """Lancamentos que compoem o custo da safra, nos mesmos criterios do custo por setor."""
    if not farm_id or season is None:
        return []
    plot_names = {plot.id: plot.name for plot in plots}
    start, end = season.start_date, season.end_date
    entries: list[dict] = []
    for output in repository.list_stock_outputs(farm_id=farm_id):
        if output.season_id != season.id or not _within(output.movement_date, start, end):
            continue
        if "ajuste" in normalize(output.origin) or output.plot_id not in plot_names:
            continue
        amount = _number(output.total_cost)
        if amount <= 0:
            continue
        catalog = output.input_catalog
        purchase = output.purchased_input
        tx = getattr(purchase, "finance_transaction", None) if purchase else None
        name = (getattr(catalog, "name", None) or "Insumo").strip()
        entries.append({
            "key": f"stock:{output.id}", "kind": "stock", "date": output.movement_date, "amount": round(amount, 2),
            "plot_id": output.plot_id, "plot": plot_names.get(output.plot_id),
            "item": name, "title": f"{name} · {_fmt_qty(output.quantity, output.unit)}",
            "origin": output.origin or "Saída de estoque",
            "supplier": (getattr(purchase, "supplier_name", None) or getattr(tx, "counterparty_name", None) or "").strip(),
            "document": (getattr(tx, "document_number", None) or "").strip(),
            "payment": (getattr(tx, "payment_method", None) or "").strip(),
            "base_category": getattr(catalog, "category", None) or "",
            "item_type": normalize(getattr(catalog, "item_type", None)),
            "text": normalize(" ".join(filter(None, [name, getattr(catalog, "category", None), output.notes]))),
            "learn_key": normalize(name)[:200],
            "unit": output.unit, "quantity": _number(output.quantity), "input_id": output.input_id,
            "link": f"/insumos/estoque/saidas/{output.id}/editar",
        })
    for tx in repository.list_finance_transactions(farm_id=farm_id):
        if normalize(tx.operation_type) != "despesa" or normalize(tx.source) not in {"contas", "operacoes agricolas"}:
            continue
        if tx.season_id != season.id or not _within(tx.launch_date, start, end) or tx.plot_id not in plot_names:
            continue
        if getattr(tx, "is_reversed", False):
            continue
        product = (tx.product_service or tx.category or "Despesa").strip()
        entries.append({
            "key": f"fin:{tx.id}", "kind": "fin", "date": tx.launch_date, "amount": round(_number(tx.amount), 2),
            "plot_id": tx.plot_id, "plot": plot_names.get(tx.plot_id),
            "item": product, "title": product if not tx.description else f"{product} · {tx.description.strip()[:80]}",
            "origin": tx.source or "Contas",
            "supplier": (tx.counterparty_name or "").strip(), "document": (tx.document_number or "").strip(),
            "payment": (tx.payment_method or "").strip(), "base_category": tx.category or "", "item_type": "",
            "text": normalize(" ".join(filter(None, [tx.product_service, tx.description, tx.counterparty_name, tx.notes]))),
            "learn_key": normalize(product)[:200],
            "unit": None, "quantity": 0.0, "input_id": None,
            "link": f"/gestao-financeira/contas?transaction_edit_id={tx.id}",
        })
    return entries


def load_overrides(db, farm_id: int | None) -> list:
    if not farm_id or db is None:
        return []
    from app.models.cost_classification import CostClassification

    return db.query(CostClassification).filter(CostClassification.farm_id == farm_id).all()


def classify_entries(entries: list[dict], overrides: list) -> None:
    """Atribui ``group``, ``sub``, ``confidence`` e ``why`` a cada lancamento (in place)."""
    by_key = {row.entry_key: row for row in overrides}
    learned: dict[str, Counter] = defaultdict(Counter)
    for row in overrides:
        if row.learn_key and row.subgroup_code in SUB_INDEX:
            learned[row.learn_key][row.subgroup_code] += 1

    for entry in entries:
        sub, conf, why, source = None, 0, "", "rule"
        row = by_key.get(entry["key"])
        if row is not None and row.subgroup_code in SUB_INDEX:
            sub, conf, source = row.subgroup_code, 100, "user"
            why = "Você confirmou esta categoria."
        elif entry["learn_key"] in learned:
            sub = learned[entry["learn_key"]].most_common(1)[0][0]
            conf, source = 92, "learned"
            why = f"Você classificou “{entry['item']}” como {SUB_INDEX[sub][1].lower()} em outro lançamento, e o sistema aplicou o mesmo aqui."
        else:
            base = normalize(entry["base_category"])
            base_sub = CATEGORY_MAP.get(base)
            kw_sub, word = _keyword_sub(entry["text"])
            label = entry["base_category"].strip()
            if entry["kind"] == "stock" and entry["item_type"] == "combustivel":
                sub, conf = ("lubrificantes" if base_sub == "lubrificantes" or kw_sub == "lubrificantes" else "combustivel"), 98
                why = f"“{entry['item']}” está cadastrado no estoque como combustível."
            elif entry["kind"] == "stock" and base_sub:
                sub, conf = base_sub, 97
                why = f"“{entry['item']}” está cadastrado no catálogo de insumos como {label}."
            elif base_sub and kw_sub and SUB_INDEX[kw_sub][0] != SUB_INDEX[base_sub][0] and SUB_INDEX[kw_sub][0] == "mao_obra":
                sub, conf = kw_sub, 84
                why = f"O lançamento está como “{label}”, mas a descrição cita “{word}”, o que indica {SUB_INDEX[kw_sub][1].lower()} de mão de obra."
            elif base_sub:
                sub = base_sub
                if kw_sub == base_sub:
                    conf, why = 97, f"Categoria lançada “{label}” e a descrição, que cita “{word}”, apontam para o mesmo grupo."
                else:
                    conf, why = 93, f"Lançado na categoria “{label}”."
            elif kw_sub:
                sub, conf = kw_sub, 80 if entry["kind"] == "fin" else 85
                why = f"A descrição cita “{word}”, típico de {SUB_INDEX[kw_sub][1].lower()}."
                if entry["kind"] == "stock" and entry["item_type"] != "combustivel":
                    sub = kw_sub if SUB_INDEX[kw_sub][0] == "insumos" else "outros_insumos"
        if sub is None and entry["kind"] == "stock":
            sub, conf = "outros_insumos", 72
            why = f"Saída de estoque de “{entry['item']}” sem categoria específica no catálogo."
        entry.update(sub=sub, confidence=conf, why=why, source=source)

    # Segunda passada: fornecedor que sempre vende o mesmo tipo de coisa.
    by_supplier: dict[str, Counter] = defaultdict(Counter)
    for entry in entries:
        if entry["sub"] and entry["confidence"] >= 90 and entry["supplier"]:
            by_supplier[normalize(entry["supplier"])][entry["sub"]] += 1
    for entry in entries:
        if entry["sub"] is None:
            counts = by_supplier.get(normalize(entry["supplier"])) if entry["supplier"] else None
            if counts:
                top, n = counts.most_common(1)[0]
                if n >= 2 and n / sum(counts.values()) >= 0.75:
                    entry.update(sub=top, confidence=65, source="supplier",
                                 why=f"A descrição não é clara, mas {entry['supplier']} costuma aparecer em {SUB_INDEX[top][1].lower()} ({n} lançamentos).")
                    continue
            entry.update(sub=UNIDENTIFIED[1], confidence=30, source="none",
                         why="A descrição é genérica e o sistema não encontrou pistas suficientes para decidir a categoria.")
    for entry in entries:
        entry["group"] = SUB_INDEX[entry["sub"]][0]


def _months(start: date, end: date) -> list[tuple[int, int]]:
    months = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        months.append((y, m))
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return months[-12:]


_MONTH_ABBR = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]


def _pct(part: float, whole: float) -> float:
    return round(part / whole * 100, 1) if whole else 0.0


def _delta(current: float, previous: float | None) -> float | None:
    if not previous:
        return None
    return round((current / previous - 1) * 100, 1)


def _brl_short(value: float) -> str:
    if abs(value) >= 1000:
        return f"R$ {value / 1000:,.1f} mil".replace(",", "X").replace(".", ",").replace("X", ".").replace(",0 mil", " mil")
    return f"R$ {value:,.0f}".replace(",", ".")


def _previous_season(repository, farm_id: int, season):
    candidates = [s for s in repository.list_crop_seasons(farm_id=farm_id)
                  if s.id != season.id and s.start_date and season.start_date and s.start_date < season.start_date]
    return max(candidates, key=lambda s: s.start_date) if candidates else None


def build_cost_tree(repository, *, farm_id: int | None, season, plots: list, today: date, with_previous: bool = True) -> dict:
    entries = collect_entries(repository, farm_id=farm_id, season=season, plots=plots)
    classify_entries(entries, load_overrides(getattr(repository, "db", None), farm_id))
    total = round(sum(e["amount"] for e in entries), 2)
    area = sum(_number(p.area_hectares) for p in plots)
    plantings = getattr(season, "planting_by_plot", None) or {}
    sacks = sum(_number((plantings.get(p.id) or p).estimated_yield_sacks) for p in plots) if plots else 0

    prev_entries: list[dict] = []
    prev_season = None
    if with_previous and season is not None and farm_id:
        prev_season = _previous_season(repository, farm_id, season)
        if prev_season is not None:
            prev_plots = list(getattr(prev_season, "plots", None) or plots)
            prev_entries = collect_entries(repository, farm_id=farm_id, season=prev_season, plots=prev_plots)
            classify_entries(prev_entries, load_overrides(getattr(repository, "db", None), farm_id))
    prev_by = defaultdict(float)
    prev_unit = defaultdict(list)
    for e in prev_entries:
        prev_by["*"] += e["amount"]
        prev_by[e["group"]] += e["amount"]
        prev_by[f"{e['group']}/{e['sub']}"] += e["amount"]
        prev_by[f"{e['group']}/{e['sub']}/{normalize(e['item'])}"] += e["amount"]
        if e["kind"] == "stock" and e["quantity"]:
            prev_unit[e["input_id"]].append((e["amount"], e["quantity"]))

    span_end = min(today, season.end_date) if season and season.end_date else today
    span_start = season.start_date if season and season.start_date else span_end
    months = _months(span_start, max(span_start, span_end))

    def monthly(items: list[dict]) -> list[dict]:
        sums = defaultdict(float)
        for e in items:
            sums[(e["date"].year, e["date"].month)] += e["amount"]
        return [{"m": _MONTH_ABBR[m - 1], "y": y, "v": round(sums.get((y, m), 0.0), 2)} for y, m in months]

    def node(name: str, path: str, items: list[dict], color: str | None = None, sub: str | None = None) -> dict:
        value = round(sum(e["amount"] for e in items), 2)
        return {"name": name, "path": path, "value": value, "color": color, "sub": sub,
                "prev": round(prev_by.get(path, 0.0), 2) or None, "count": len(items),
                "low": sum(1 for e in items if e["confidence"] < LOW_CONFIDENCE), "monthly": monthly(items)}

    root = node("Custo da safra", "*", entries)
    root["children"] = []
    for group in GROUPS:
        g_items = [e for e in entries if e["group"] == group["code"]]
        if not g_items:
            continue
        g = node(group["name"], group["code"], g_items, group["color"])
        g["children"] = []
        for sub_code, sub_name in group["subs"]:
            s_items = [e for e in g_items if e["sub"] == sub_code]
            if not s_items:
                continue
            s = node(sub_name, f"{group['code']}/{sub_code}", s_items, sub=sub_code)
            by_item: dict[str, list[dict]] = defaultdict(list)
            labels: dict[str, str] = {}
            for e in s_items:
                k = normalize(e["item"]) or "item"
                by_item[k].append(e)
                labels.setdefault(k, e["item"])
            s["children"] = []
            for k, i_items in sorted(by_item.items(), key=lambda kv: -sum(e["amount"] for e in kv[1])):
                i = node(labels[k], f"{group['code']}/{sub_code}/{k}", i_items)
                i["entries"] = [_entry_json(e) for e in sorted(i_items, key=lambda e: e["date"], reverse=True)]
                i["insights"] = _item_insights(i_items, prev_unit)
                s["children"].append(i)
            s["children"].sort(key=lambda c: -c["value"])
            s["insights"] = _children_insights(s, "item")
            g["children"].append(s)
        g["children"].sort(key=lambda c: -c["value"])
        g["insights"] = _children_insights(g, "grupo")
        root["children"].append(g)
    root["children"].sort(key=lambda c: -c["value"])
    root["insights"] = _root_insights(root, entries, plots)

    return {
        "status": "ok" if entries else "empty",
        "season": season.name if season else None,
        "previous_season": prev_season.name if prev_season else None,
        "area": round(area, 2), "sacks": round(sacks, 2), "total": total,
        "tree": root,
        "groups": [{"code": g["code"], "name": g["name"], "color": g["color"]} for g in GROUPS],
        "subgroups": [{"code": s, "name": n, "group": g["code"]} for g in GROUPS for s, n in g["subs"]],
        "period": f"{_MONTH_ABBR[months[0][1] - 1]}/{str(months[0][0])[2:]} a {_MONTH_ABBR[months[-1][1] - 1]}/{str(months[-1][0])[2:]}" if months else "",
    }


def _entry_json(e: dict) -> dict:
    return {
        "key": e["key"], "date": e["date"].strftime("%d/%m/%Y"), "amount": e["amount"], "title": e["title"],
        "item": e["item"], "plot": e["plot"], "origin": e["origin"], "supplier": e["supplier"],
        "document": e["document"], "payment": e["payment"], "category": e["base_category"],
        "group": e["group"], "sub": e["sub"], "confidence": e["confidence"], "why": e["why"],
        "source": e["source"], "link": e["link"],
    }


def _children_insights(node: dict, kind: str) -> list[list[str]]:
    out: list[list[str]] = []
    kids = node.get("children") or []
    if not kids or not node["value"]:
        return out
    top = kids[0]
    share = _pct(top["value"], node["value"])
    if len(kids) > 1 and share >= 40:
        out.append(["spark", f"{top['name']} responde por {share:.0f}% de {node['name'].lower()}.".replace(".0%", "%")])
    moves = [(c, _delta(c["value"], c["prev"])) for c in kids]
    moves = [(c, d) for c, d in moves if d is not None and abs(d) >= 10 and c["value"] >= node["value"] * 0.05]
    moves.sort(key=lambda cd: -abs(cd[1]))
    for c, d in moves[:2]:
        verb = "subiu" if d > 0 else "caiu"
        out.append(["up" if d > 0 else "down", f"{c['name']} {verb} {abs(d):.0f}% em relação à safra anterior."])
    if node["low"]:
        out.append(["spark", f"{node['low']} lançamento(s) aqui foram classificados com pouca certeza. Vale revisar."])
    return out[:3]


def _item_insights(items: list[dict], prev_unit: dict) -> list[list[str]]:
    out: list[list[str]] = []
    stock = [e for e in items if e["kind"] == "stock" and e["quantity"]]
    if stock:
        input_id = stock[0]["input_id"]
        prev = prev_unit.get(input_id)
        if prev:
            cur_price = sum(e["amount"] for e in stock) / sum(e["quantity"] for e in stock)
            prev_price = sum(a for a, _ in prev) / sum(q for _, q in prev)
            d = _delta(cur_price, prev_price)
            if d is not None and abs(d) >= 5:
                out.append(["up" if d > 0 else "down", f"O custo por {stock[0]['unit'] or 'unidade'} ficou {abs(d):.0f}% {'mais caro' if d > 0 else 'mais barato'} que na safra anterior."])
    plots = Counter()
    for e in items:
        plots[e["plot"]] += e["amount"]
    if len(plots) > 1:
        name, value = plots.most_common(1)[0]
        out.append(["spark", f"{name} concentra {_pct(value, sum(plots.values())):.0f}% deste gasto."])
    low = sum(1 for e in items if e["confidence"] < LOW_CONFIDENCE)
    if low:
        out.append(["spark", f"{low} lançamento(s) com pouca certeza na classificação. Confira abaixo."])
    return out[:3]


def _root_insights(root: dict, entries: list[dict], plots: list) -> list[list[str]]:
    out = _children_insights(root, "categoria")
    out = [i for i in out if not i[1].startswith(root["children"][0]["name"] + " responde")] if root["children"] else out
    by_plot = defaultdict(float)
    for e in entries:
        by_plot[e["plot_id"]] += e["amount"]
    areas = {p.id: _number(p.area_hectares) for p in plots}
    per_ha = {pid: v / areas[pid] for pid, v in by_plot.items() if areas.get(pid)}
    if len(per_ha) > 1:
        avg = sum(by_plot.values()) / sum(areas[pid] for pid in per_ha)
        pid, value = max(per_ha.items(), key=lambda kv: kv[1])
        diff = _pct(value - avg, avg)
        if diff >= 10:
            name = next((p.name for p in plots if p.id == pid), "Um setor")
            out.insert(0, ["spark", f"{name} tem o maior custo por hectare ({_brl_short(value)}/ha), {diff:.0f}% acima da média da fazenda."])
    return out[:3]


def cost_group_summary(tree: dict) -> list[dict]:
    root = tree.get("tree") or {}
    total = root.get("value") or 0
    return [{"name": g["name"], "color": g["color"], "value": g["value"], "pct": round(g["value"] / total * 100) if total else 0}
            for g in root.get("children", [])]
