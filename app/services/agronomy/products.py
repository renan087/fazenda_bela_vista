"""Fontes de nutrientes, leitura de garantias e conversão de nutriente em produto comercial."""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field

NUTRIENTS = ("N", "P2O5", "K2O", "Ca", "Mg", "S", "B", "Zn", "Cu", "Mn")
NUTRIENT_LABELS = {"N": "N", "P2O5": "P₂O₅", "K2O": "K₂O", "Ca": "Ca", "Mg": "Mg", "S": "S", "B": "B",
                   "Zn": "Zn", "Cu": "Cu", "Mn": "Mn"}


@dataclass
class Source:
    key: str
    name: str
    guarantee: dict            # nutriente -> % (m/m)
    fertigation: bool = False  # solúvel o bastante para fertirrigação
    foliar: bool = False
    note: str = ""


# Garantias típicas de mercado (valores usuais de rótulo). Quando o lote em estoque tem garantia
# registrada no nome ou nas observações, ela substitui o valor típico.
GENERIC = {
    "ureia": Source("ureia", "Ureia", {"N": 45}, fertigation=True,
                    note="Aplicar com solo úmido para reduzir perdas por volatilização."),
    "sulfato_amonio": Source("sulfato_amonio", "Sulfato de amônio", {"N": 20, "S": 22}, fertigation=True),
    "nitrato_calcio": Source("nitrato_calcio", "Nitrato de cálcio", {"N": 15, "Ca": 19}, fertigation=True),
    "nitrato_potassio": Source("nitrato_potassio", "Nitrato de potássio", {"N": 13, "K2O": 44}, fertigation=True),
    "kcl": Source("kcl", "Cloreto de potássio", {"K2O": 60}, fertigation=True,
                  note="Na fertirrigação, usar cloreto de potássio branco (solúvel)."),
    "sulfato_potassio": Source("sulfato_potassio", "Sulfato de potássio", {"K2O": 48, "S": 17}),
    "ssp": Source("ssp", "Superfosfato simples", {"P2O5": 18, "Ca": 16, "S": 10}),
    "tsp": Source("tsp", "Superfosfato triplo", {"P2O5": 41, "Ca": 10}),
    "map": Source("map", "MAP (fosfato monoamônico)", {"N": 11, "P2O5": 52}),
    "gesso": Source("gesso", "Gesso agrícola", {"Ca": 16, "S": 13}),
    "acido_borico": Source("acido_borico", "Ácido bórico", {"B": 17}, fertigation=True, foliar=True),
    "sulfato_zinco": Source("sulfato_zinco", "Sulfato de zinco", {"Zn": 20}, foliar=True),
    "sulfato_cobre": Source("sulfato_cobre", "Sulfato de cobre", {"Cu": 25}, foliar=True),
    "sulfato_manganes": Source("sulfato_manganes", "Sulfato de manganês", {"Mn": 26}, foliar=True),
}

NAME_PATTERNS = (  # nome normalizado -> fonte genérica
    (r"\bureia\b", "ureia"), (r"sulfato de amonio", "sulfato_amonio"), (r"nitrato de calcio", "nitrato_calcio"),
    (r"nitrato de potassio", "nitrato_potassio"), (r"cloreto de potassio|\bkcl\b", "kcl"),
    (r"sulfato de potassio", "sulfato_potassio"), (r"superfosfato simples|\bssp\b", "ssp"),
    (r"superfosfato triplo|\btsp\b", "tsp"), (r"\bmap\b|fosfato monoamonico", "map"), (r"\bgesso\b", "gesso"),
    (r"acido borico", "acido_borico"), (r"sulfato de zinco", "sulfato_zinco"), (r"sulfato de cobre", "sulfato_cobre"),
    (r"sulfato de manganes", "sulfato_manganes"),
)


def normalize(text: str | None) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", (text or "").casefold())
                   if not unicodedata.combining(c)).strip()


NPK = re.compile(r"\b(\d{1,2})\s*[-–.]\s*(\d{1,2})\s*[-–.]\s*(\d{1,2})\b")
PERCENT = re.compile(r"(\d+(?:[.,]\d+)?)\s*%\s*(?:de\s+)?(n|p2o5|k2o|ca|mg|s|b|zn|cu|mn)\b"
                     r"|\b(n|p2o5|k2o|ca|mg|s|b|zn|cu|mn)\s*[:=]?\s*(\d+(?:[.,]\d+)?)\s*%")
PRNT = re.compile(r"\bprnt\b\D{0,12}(\d+(?:[.,]\d+)?)\s*%?")
CANONICAL = {"n": "N", "p2o5": "P2O5", "k2o": "K2O", "ca": "Ca", "mg": "Mg", "s": "S", "b": "B", "zn": "Zn",
             "cu": "Cu", "mn": "Mn"}


def parse_guarantee(*texts: str | None) -> tuple[dict, str]:
    """Lê a garantia de um rótulo: fórmula NPK (20-05-20) e/ou teores em %. Retorna (garantia, origem)."""
    joined = " ".join(normalize(text) for text in texts if text)
    guarantee = {}
    match = NPK.search(joined)
    if match and re.search(r"\bnpk\b|formul|adubo|\d{2}-\d{2}-\d{2}", joined):
        guarantee.update({k: float(v) for k, v in zip(("N", "P2O5", "K2O"), match.groups()) if float(v) > 0})
    for found in PERCENT.finditer(joined):
        value = found.group(1) or found.group(4)
        name = found.group(2) or found.group(3)
        guarantee.setdefault(CANONICAL[name], float(value.replace(",", ".")))
    if guarantee and all(0 < value <= 100 for value in guarantee.values()) and sum(guarantee.values()) <= 100:
        return guarantee, "rótulo registrado"
    for pattern, key in NAME_PATTERNS:
        if re.search(pattern, joined):
            return dict(GENERIC[key].guarantee), "garantia típica da fonte (confira o rótulo)"
    return {}, ""


def parse_prnt(*texts: str | None) -> float | None:
    match = PRNT.search(" ".join(normalize(text) for text in texts if text))
    if not match:
        return None
    value = float(match.group(1).replace(",", "."))
    return value if 40 <= value <= 150 else None


def is_lime(name: str | None) -> bool:
    return "calcario" in normalize(name)


@dataclass
class StockLot:
    ref: str
    name: str
    balance: float
    unit: str
    guarantee: dict
    origin: str
    prnt: float | None = None
    notes: str = ""


MASS_TO_KG = {"kg": 1.0, "t": 1000.0, "ton": 1000.0, "g": 0.001}


def to_kg(quantity: float | None, unit: str | None) -> float | None:
    factor = MASS_TO_KG.get((unit or "").strip().casefold())
    return quantity * factor if quantity is not None and factor else None


@dataclass
class Allocation:
    source: Source
    kg_ha: float
    stock: StockLot | None = None
    roles: list = field(default_factory=list)

    def supplies(self) -> dict:
        return {n: self.kg_ha * pct / 100 for n, pct in self.source.guarantee.items()}


def _source_from_stock(lot: StockLot) -> Source:
    return Source(f"estoque:{lot.ref}", lot.name, lot.guarantee)


def _stock_for(stock: list[StockLot], predicate) -> StockLot | None:
    matches = [lot for lot in stock if lot.guarantee and predicate(lot.guarantee)]
    return max(matches, key=lambda lot: lot.balance, default=None)


def fit_npk(needs: dict, stock: list[StockLot], *, fertigation: bool) -> tuple[list[Allocation], list[str]]:
    """Converte necessidades (kg/ha de N, P2O5, K2O, S) em produtos sem ultrapassar nenhuma delas.

    1. Uma fórmula do estoque é usada até o limite do nutriente que primeiro se esgota.
    2. O restante vem de fontes simples, preferindo lotes do estoque com garantia conhecida.
    """
    remaining = {n: max(0.0, needs.get(n, 0.0)) for n in ("N", "P2O5", "K2O", "S")}
    plan, notes = [], []

    def add(source, kg_ha, lot=None, role=""):
        if kg_ha <= 0.05:
            return
        allocation = Allocation(source, kg_ha, lot, [role] if role else [])
        for nutrient, amount in allocation.supplies().items():
            if nutrient in remaining:
                remaining[nutrient] = max(0.0, remaining[nutrient] - amount)
        plan.append(allocation)

    primary = [n for n in ("N", "P2O5", "K2O") if remaining[n] > 0]
    if not fertigation and len(primary) >= 2:
        formulas = [lot for lot in stock if len({"N", "P2O5", "K2O"} & lot.guarantee.keys()) >= 2
                    and all(remaining.get(n, 0) > 0 for n in lot.guarantee if n in ("N", "P2O5", "K2O"))]
        lot = max(formulas, key=lambda item: item.balance, default=None)
        if lot:
            dose = min(remaining[n] / (lot.guarantee[n] / 100) for n in lot.guarantee if n in ("N", "P2O5", "K2O"))
            add(_source_from_stock(lot), dose, lot, "Fórmula do estoque")
            notes.append(f"A fórmula {lot.name} do estoque cobre parte da necessidade; o restante vem de fontes simples.")

    if remaining["P2O5"] > 0:
        lot = _stock_for(stock, lambda g: g.get("P2O5", 0) > 0 and len({"N", "K2O"} & g.keys()) == 0)
        if lot:
            add(_source_from_stock(lot), remaining["P2O5"] / (lot.guarantee["P2O5"] / 100), lot, "Fósforo")
        else:
            source = GENERIC["ssp"] if remaining["S"] > 0 else GENERIC["map"] if remaining["N"] > 0 else GENERIC["tsp"]
            add(source, remaining["P2O5"] / (source.guarantee["P2O5"] / 100), role="Fósforo")
    if remaining["S"] > 0 and remaining["N"] > 0:
        source = GENERIC["sulfato_amonio"]
        dose = min(remaining["S"] / (source.guarantee["S"] / 100), remaining["N"] / (source.guarantee["N"] / 100))
        add(source, dose, role="Nitrogênio e enxofre")
    if remaining["N"] > 0:
        lot = _stock_for(stock, lambda g: g.get("N", 0) > 0 and set(g) <= {"N", "S"})
        source = _source_from_stock(lot) if lot else GENERIC["ureia"]
        add(source, remaining["N"] / (source.guarantee["N"] / 100), lot, "Nitrogênio")
    if remaining["K2O"] > 0:
        lot = _stock_for(stock, lambda g: g.get("K2O", 0) > 0 and set(g) <= {"K2O", "S"})
        source = _source_from_stock(lot) if lot else GENERIC["kcl"]
        add(source, remaining["K2O"] / (source.guarantee["K2O"] / 100), lot, "Potássio")
    if remaining["S"] > 0.5:
        notes.append("O enxofre restante pode vir do gesso agrícola ou do sulfato de amônio.")
    return plan, notes
