"""Tabela local de códigos IBGE de municípios (7 dígitos) para apoio ao cadastro NF-e.

Permite sugerir/preencher automaticamente o código cMun sem depender de consulta externa,
priorizando a Bahia (regiões cafeeiras e polos) e principais polos cafeeiros nacionais.
"""
from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path
import re
import unicodedata

BRAZIL_STATES: tuple[dict[str, str], ...] = (
    {"uf": "AC", "name": "Acre"},
    {"uf": "AL", "name": "Alagoas"},
    {"uf": "AP", "name": "Amapá"},
    {"uf": "AM", "name": "Amazonas"},
    {"uf": "BA", "name": "Bahia"},
    {"uf": "CE", "name": "Ceará"},
    {"uf": "DF", "name": "Distrito Federal"},
    {"uf": "ES", "name": "Espírito Santo"},
    {"uf": "GO", "name": "Goiás"},
    {"uf": "MA", "name": "Maranhão"},
    {"uf": "MT", "name": "Mato Grosso"},
    {"uf": "MS", "name": "Mato Grosso do Sul"},
    {"uf": "MG", "name": "Minas Gerais"},
    {"uf": "PA", "name": "Pará"},
    {"uf": "PB", "name": "Paraíba"},
    {"uf": "PR", "name": "Paraná"},
    {"uf": "PE", "name": "Pernambuco"},
    {"uf": "PI", "name": "Piauí"},
    {"uf": "RJ", "name": "Rio de Janeiro"},
    {"uf": "RN", "name": "Rio Grande do Norte"},
    {"uf": "RS", "name": "Rio Grande do Sul"},
    {"uf": "RO", "name": "Rondônia"},
    {"uf": "RR", "name": "Roraima"},
    {"uf": "SC", "name": "Santa Catarina"},
    {"uf": "SP", "name": "São Paulo"},
    {"uf": "SE", "name": "Sergipe"},
    {"uf": "TO", "name": "Tocantins"},
)
_VALID_UFS: frozenset[str] = frozenset(item["uf"] for item in BRAZIL_STATES)
_UF_NAMES: dict[str, str] = {item["uf"]: item["name"] for item in BRAZIL_STATES}
_IBGE_DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "ibge_municipios.json"
_IBGE_COORDS_FILE = Path(__file__).resolve().parent.parent / "data" / "ibge_municipios_coords.json"


@lru_cache(maxsize=1)
def _load_municipality_coords() -> dict[str, tuple[float, float]]:
    try:
        raw = json.loads(_IBGE_COORDS_FILE.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            out: dict[str, tuple[float, float]] = {}
            for key, pair in raw.items():
                if isinstance(pair, list) and len(pair) == 2:
                    out[str(key)] = (float(pair[0]), float(pair[1]))
            return out
    except (OSError, ValueError, TypeError):
        pass
    return {}


def get_state_name(uf: str | None) -> str:
    """Retorna o nome completo do estado para a UF informada, ou ''."""
    return _UF_NAMES.get(str(uf or "").strip().upper(), "")


def resolve_municipality_coords(location: str | None) -> tuple[float, float] | None:
    """Retorna (latitude, longitude) oficiais do IBGE para o município informado."""
    uf, city = parse_location_uf_city(location)
    coords = _load_municipality_coords()
    if not coords:
        return None
    norm_city = _norm(city)
    if uf and norm_city:
        hit = coords.get(f"{uf}|{norm_city}")
        if hit:
            return hit
    if not uf and norm_city:
        matches = [val for key, val in coords.items() if key.endswith(f"|{norm_city}")]
        if len(matches) == 1:
            return matches[0]
    return None


@lru_cache(maxsize=1)
def _load_municipalities_by_uf() -> dict[str, tuple[str, ...]]:
    try:
        raw = json.loads(_IBGE_DATA_FILE.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            return {
                str(uf).upper(): tuple(str(name) for name in names if name)
                for uf, names in raw.items()
                if str(uf).upper() in _VALID_UFS and isinstance(names, list)
            }
    except (OSError, ValueError):
        pass
    return {}


def list_brazil_states() -> list[dict[str, str]]:
    """Retorna os 27 estados brasileiros ordenados por nome."""
    return [dict(item) for item in BRAZIL_STATES]


def list_municipalities_by_uf(uf: str | None) -> list[str]:
    """Retorna todos os municípios oficiais do IBGE para a UF informada."""
    code = str(uf or "").strip().upper()
    if code not in _VALID_UFS:
        return []
    return list(_load_municipalities_by_uf().get(code, ()))


def parse_location_uf_city(location: str | None) -> tuple[str, str]:
    """Extrai (UF, Município) de strings como 'Vitória da Conquista, BA' ou 'Patrocínio - MG'."""
    text = re.sub(r"\s+", " ", str(location or "").strip())
    if not text:
        return ("", "")
    uf = ""
    city = text
    match = re.search(r"(?:[,\-/|]|\()\s*([A-Za-z]{2})\s*\)?$", text)
    if match:
        candidate = match.group(1).upper()
        if candidate in _VALID_UFS:
            uf = candidate
            city = text[: match.start()].strip(" ,-/|()")
    elif len(text) == 2 and text.upper() in _VALID_UFS:
        return (text.upper(), "")

    if uf and city:
        norm_city = _norm(city)
        for official in list_municipalities_by_uf(uf):
            if _norm(official) == norm_city:
                return (uf, official)
    return (uf, city)


def _norm(text: str | None) -> str:
    folded = unicodedata.normalize('NFKD', text or '').encode('ascii', 'ignore').decode('ascii')
    return re.sub(r'\s+', ' ', folded).strip().lower()


# Chave: (UF_MAIUSCULA, nome_normalizado) -> código IBGE de 7 dígitos.
_MUNICIPALITIES: dict[tuple[str, str], str] = {
    # Bahia (29) — polos cafeeiros (Planalto, Chapada Diamantina, Oeste, Extremo Sul) e principais cidades
    ('BA', 'salvador'): '2927408',
    ('BA', 'vitoria da conquista'): '2933307',
    ('BA', 'barra do choca'): '2902906',
    ('BA', 'planalto'): '2925006',
    ('BA', 'pocoes'): '2925105',
    ('BA', 'encruzilhada'): '2910404',
    ('BA', 'candido sales'): '2906709',
    ('BA', 'anage'): '2901205',
    ('BA', 'belo campo'): '2903508',
    ('BA', 'piata'): '2924306',
    ('BA', 'mucuge'): '2921906',
    ('BA', 'ibicoara'): '2912202',
    ('BA', 'barra da estiva'): '2902807',
    ('BA', 'seabra'): '2929909',
    ('BA', 'boninal'): '2904001',
    ('BA', 'rio de contas'): '2926707',
    ('BA', 'abaíra'): '2900108',
    ('BA', 'abaira'): '2900108',
    ('BA', 'jussiape'): '2918605',
    ('BA', 'jaguaquara'): '2917607',
    ('BA', 'brejoes'): '2904308',
    ('BA', 'itirucu'): '2916906',
    ('BA', 'maracas'): '2920502',
    ('BA', 'jequie'): '2918001',
    ('BA', 'barreiras'): '2903201',
    ('BA', 'luis eduardo magalhaes'): '2919553',
    ('BA', 'sao desiderio'): '2928901',
    ('BA', 'cocorobo'): '2908200',
    ('BA', 'eunapolis'): '2910727',
    ('BA', 'itabela'): '2914653',
    ('BA', 'guaratinga'): '2911808',
    ('BA', 'teixeira de freitas'): '2931350',
    ('BA', 'itamaraju'): '2915601',
    ('BA', 'porto seguro'): '2925303',
    ('BA', 'ilheus'): '2913606',
    ('BA', 'itabuna'): '2914802',
    ('BA', 'feira de santana'): '2910800',
    ('BA', 'brumado'): '2904605',
    ('BA', 'guanambi'): '2911709',
    ('BA', 'juazeiro'): '2918407',
    ('BA', 'alagoinhas'): '2900702',
    ('BA', 'camacari'): '2905701',
    ('BA', 'lauro de freitas'): '2919207',
    ('BA', 'itapetinga'): '2916401',
    ('BA', 'caetite'): '2905206',
    ('BA', 'livramento de nossa senhora'): '2919504',
    # Minas Gerais (31) — expansão futura
    ('MG', 'belo horizonte'): '3106200',
    ('MG', 'varginha'): '3170701',
    ('MG', 'guaxupe'): '3128709',
    ('MG', 'patrocinio'): '3148103',
    ('MG', 'manhuacu'): '3139409',
    ('MG', 'caratinga'): '3113404',
    ('MG', 'tres pontas'): '3169406',
    ('MG', 'alfenas'): '3101607',
    ('MG', 'uberlandia'): '3170206',
    # Espírito Santo (32) — expansão futura
    ('ES', 'vitoria'): '3205309',
    ('ES', 'linhares'): '3203205',
    ('ES', 'colatina'): '3201506',
    ('ES', 'venda nova do imigrante'): '3205069',
    # São Paulo (35) — expansão futura
    ('SP', 'sao paulo'): '3550308',
    ('SP', 'santos'): '3548500',
    ('SP', 'franca'): '3516200',
    ('SP', 'garca'): '3516705',
    ('SP', 'campinas'): '3509502',
}


def resolve_municipality_code(city: str | None, state: str | None) -> str:
    """Retorna o código IBGE de 7 dígitos se o município estiver na tabela local, ou ''."""
    uf = str(state or '').strip().upper()
    name = _norm(city)
    if not uf or not name:
        return ''
    return _MUNICIPALITIES.get((uf, name), '')
