import json
from copy import deepcopy
from typing import Any

NOTIFICATION_CATEGORIES: tuple[dict[str, Any], ...] = (
    {
        "key": "stock",
        "title": "Estoque abaixo do mínimo",
        "description": "Avisa quando insumos ou defensivos atingem o ponto de reposição.",
        "defaults": {"system": True, "email": True, "whatsapp": True},
    },
    {
        "key": "finance",
        "title": "Contas a pagar e a receber",
        "description": "Lembretes de vencimentos próximos e recebimentos pendentes.",
        "defaults": {"system": True, "email": True, "whatsapp": False},
    },
    {
        "key": "schedule",
        "title": "Agenda de adubação e manejo",
        "description": "Atividades programadas para os talhões da safra ativa.",
        "defaults": {"system": True, "email": False, "whatsapp": True},
    },
    {
        "key": "maintenance",
        "title": "Manutenção de máquinas",
        "description": "Revisões preventivas por horímetro ou data.",
        "defaults": {"system": True, "email": False, "whatsapp": False},
    },
    {
        "key": "rain",
        "title": "Alertas de chuva e clima",
        "description": "Previsão de chuva relevante para planejamento de campo.",
        "defaults": {"system": True, "email": False, "whatsapp": False},
    },
    {
        "key": "digest",
        "title": "Resumo semanal da fazenda",
        "description": "Síntese de produção, estoque e caixa toda segunda-feira.",
        "defaults": {"system": False, "email": True, "whatsapp": False},
    },
)

START_PAGE_OPTIONS: tuple[dict[str, str], ...] = (
    {"path": "/dashboard", "label": "Visão geral (Início)", "permission": ""},
    {"path": "/producao", "label": "Colheita e venda (Produção)", "permission": "page.harvests"},
    {"path": "/lavoura", "label": "Lavoura e talhões", "permission": "page.plots"},
    {"path": "/gestao-financeira", "label": "Financeiro", "permission": "page.finance"},
)

VALID_START_PATHS = {item["path"] for item in START_PAGE_OPTIONS}
VALID_QUIET_START = ("19:00", "20:00", "21:00", "22:00", "23:00")
VALID_QUIET_END = ("05:00", "06:00", "07:00", "08:00", "09:00")
VALID_DENSITIES = ("comfortable", "compact")
VALID_CONTEXT_MODES = ("last", "fixed")


def default_user_preferences() -> dict[str, Any]:
    return {
        "notifications": {
            cat["key"]: dict(cat["defaults"])
            for cat in NOTIFICATION_CATEGORIES
        },
        "quiet_hours": {
            "enabled": True,
            "start": "21:00",
            "end": "07:00",
        },
        "start_page": "/dashboard",
        "context_mode": "last",
        "default_farm_id": None,
        "density": "comfortable",
        "google_login_enabled": True,
    }


def _normalize_preferences(raw: dict[str, Any] | None) -> dict[str, Any]:
    prefs = default_user_preferences()
    if not isinstance(raw, dict):
        return prefs

    raw_notifs = raw.get("notifications")
    if isinstance(raw_notifs, dict):
        for cat in NOTIFICATION_CATEGORIES:
            key = cat["key"]
            entry = raw_notifs.get(key)
            if isinstance(entry, dict):
                for channel in ("system", "email", "whatsapp"):
                    if channel in entry:
                        prefs["notifications"][key][channel] = bool(entry[channel])

    raw_quiet = raw.get("quiet_hours")
    if isinstance(raw_quiet, dict):
        if "enabled" in raw_quiet:
            prefs["quiet_hours"]["enabled"] = bool(raw_quiet["enabled"])
        if raw_quiet.get("start") in VALID_QUIET_START:
            prefs["quiet_hours"]["start"] = str(raw_quiet["start"])
        if raw_quiet.get("end") in VALID_QUIET_END:
            prefs["quiet_hours"]["end"] = str(raw_quiet["end"])

    if raw.get("start_page") in VALID_START_PATHS:
        prefs["start_page"] = str(raw["start_page"])

    if raw.get("context_mode") in VALID_CONTEXT_MODES:
        prefs["context_mode"] = str(raw["context_mode"])

    farm_id = raw.get("default_farm_id")
    if farm_id not in (None, ""):
        try:
            parsed_farm_id = int(farm_id)
            prefs["default_farm_id"] = parsed_farm_id if parsed_farm_id > 0 else None
        except (TypeError, ValueError):
            prefs["default_farm_id"] = None

    if raw.get("density") in VALID_DENSITIES:
        prefs["density"] = str(raw["density"])

    if "google_login_enabled" in raw:
        prefs["google_login_enabled"] = bool(raw["google_login_enabled"])

    return prefs


def get_user_preferences(user: Any) -> dict[str, Any]:
    raw_text = getattr(user, "preferences_json", None) if user is not None else None
    if not raw_text:
        return default_user_preferences()
    try:
        parsed = json.loads(raw_text)
    except (TypeError, ValueError):
        return default_user_preferences()
    return _normalize_preferences(parsed)


def merge_user_preferences(user: Any, patch: dict[str, Any]) -> tuple[dict[str, Any], str]:
    current = deepcopy(get_user_preferences(user))
    if isinstance(patch.get("notifications"), dict):
        for key, entry in patch["notifications"].items():
            if key in current["notifications"] and isinstance(entry, dict):
                for channel in ("system", "email", "whatsapp"):
                    if channel in entry:
                        current["notifications"][key][channel] = bool(entry[channel])
    if isinstance(patch.get("quiet_hours"), dict):
        current["quiet_hours"].update(patch["quiet_hours"])
    for scalar in ("start_page", "context_mode", "default_farm_id", "density", "google_login_enabled"):
        if scalar in patch:
            current[scalar] = patch[scalar]

    normalized = _normalize_preferences(current)
    return normalized, json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))


def parse_user_agent_label(user_agent: str | None) -> str:
    ua = (user_agent or "").strip()
    if not ua:
        return "Navegador autorizado"
    low = ua.lower()

    if "edg/" in low or "edge/" in low:
        browser = "Edge"
    elif "opr/" in low or "opera" in low:
        browser = "Opera"
    elif "firefox/" in low or "fxios/" in low:
        browser = "Firefox"
    elif "crios/" in low or ("chrome/" in low and "chromium" not in low):
        browser = "Chrome"
    elif "safari/" in low and "chrome/" not in low:
        browser = "Safari"
    else:
        browser = "Navegador"

    if "iphone" in low:
        platform = "iPhone"
    elif "ipad" in low:
        platform = "iPad"
    elif "android" in low:
        platform = "Android"
    elif "mac os x" in low or "macintosh" in low:
        platform = "macOS"
    elif "windows" in low:
        platform = "Windows"
    elif "linux" in low:
        platform = "Linux"
    else:
        platform = ""

    return f"{browser} · {platform}" if platform else browser
