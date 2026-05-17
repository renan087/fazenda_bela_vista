import logging
import re
from html import unescape
from datetime import date

import httpx

from app.core.timezone import app_now, today_in_app_timezone
from app.models import CoffeeQuote
from app.repositories.farm import FarmRepository

logger = logging.getLogger(__name__)

CEPEA_COFFEE_URL = "https://www.cepea.org.br/br/indicador/cafe.aspx?mobile="
CEPEA_SOURCE = "CEPEA/ESALQ"


def _parse_brazilian_decimal(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = value.strip().replace(".", "").replace(",", ".").replace("%", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_brazilian_date(value: str) -> date | None:
    try:
        day, month, year = value.strip().split("/")
        return date(int(year), int(month), int(day))
    except (ValueError, TypeError):
        return None


def _extract_section(text: str, start_marker: str, end_marker: str | None = None) -> str:
    start = text.find(start_marker)
    if start < 0:
        return ""
    end = text.find(end_marker, start + len(start_marker)) if end_marker else -1
    return text[start:end if end > start else len(text)]


def parse_cepea_coffee_quotes(html: str) -> list[CoffeeQuote]:
    text = re.sub(r"<[^>]+>", " ", html)
    normalized = re.sub(r"\s+", " ", unescape(text))
    sections = {
        "arabica": _extract_section(
            normalized,
            "INDICADOR DO CAFÉ ARÁBICA CEPEA/ESALQ",
            "INDICADOR DO CAFÉ ROBUSTA CEPEA/ESALQ",
        ),
        "robusta": _extract_section(
            normalized,
            "INDICADOR DO CAFÉ ROBUSTA CEPEA/ESALQ",
            "Séries de Preços",
        ),
    }
    row_pattern = re.compile(
        r"(?P<date>\d{2}/\d{2}/\d{4})\s+"
        r"(?P<brl>[-+]?\d{1,3}(?:\.\d{3})*,\d{2})\s+"
        r"(?P<day>[-+]?\d{1,3},\d{2})%\s+"
        r"(?P<month>[-+]?\d{1,3},\d{2})%\s+"
        r"(?P<usd>[-+]?\d{1,3}(?:\.\d{3})*,\d{2})"
    )
    fetched_at = app_now()
    quotes: list[CoffeeQuote] = []
    for quote_type, section in sections.items():
        if not section:
            continue
        for match in row_pattern.finditer(section):
            quote_date = _parse_brazilian_date(match.group("date"))
            price_brl = _parse_brazilian_decimal(match.group("brl"))
            if not quote_date or price_brl is None:
                continue
            quotes.append(
                CoffeeQuote(
                    quote_type=quote_type,
                    quote_date=quote_date,
                    price_brl=price_brl,
                    variation_day=_parse_brazilian_decimal(match.group("day")),
                    variation_month=_parse_brazilian_decimal(match.group("month")),
                    price_usd=_parse_brazilian_decimal(match.group("usd")),
                    source=CEPEA_SOURCE,
                    source_url=CEPEA_COFFEE_URL,
                    fetched_at=fetched_at,
                )
            )
    return quotes


def refresh_cepea_coffee_quotes(repository: FarmRepository, *, force: bool = False) -> bool:
    today = today_in_app_timezone()
    latest_quotes = [
        repository.get_latest_coffee_quote("arabica"),
        repository.get_latest_coffee_quote("robusta"),
    ]
    if (
        not force
        and all(latest_quotes)
        and all(quote.fetched_at and quote.fetched_at.date() >= today for quote in latest_quotes if quote)
    ):
        return False
    try:
        with httpx.Client(timeout=4.0, follow_redirects=True) as client:
            response = client.get(CEPEA_COFFEE_URL)
            response.raise_for_status()
    except Exception as exc:
        logger.warning("Nao foi possivel atualizar cotacao de cafe CEPEA: %s", exc)
        return False

    quotes = parse_cepea_coffee_quotes(response.text)
    if not quotes:
        logger.warning("CEPEA nao retornou cotacoes de cafe interpretaveis.")
        return False
    for quote in quotes:
        repository.upsert_coffee_quote(quote)
    return True


def latest_coffee_quote_context(repository: FarmRepository) -> dict:
    refresh_cepea_coffee_quotes(repository)
    quotes = {
        "arabica": repository.get_latest_coffee_quote("arabica"),
        "robusta": repository.get_latest_coffee_quote("robusta"),
    }
    history = {}
    for quote_type in quotes:
        rows = list(reversed(repository.list_coffee_quotes(quote_type=quote_type, limit=30)))
        history[quote_type] = {
            "labels": [row.quote_date.strftime("%d/%m") for row in rows],
            "values": [float(row.price_brl or 0) for row in rows],
        }
    return {
        "quotes": quotes,
        "history": history,
        "source": CEPEA_SOURCE,
        "source_url": CEPEA_COFFEE_URL,
    }
