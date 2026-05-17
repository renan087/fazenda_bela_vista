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
NOTICIAS_AGRICOLAS_CEPEA_URLS = {
    "arabica": "https://www.noticiasagricolas.com.br/cotacoes/cafe/indicador-cepea-esalq-cafe-arabica",
    "robusta": "https://www.noticiasagricolas.com.br/cotacoes/cafe/indicador-cepea-esalq-cafe-conillon",
}


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


def parse_noticias_agricolas_cepea_quotes(html: str, quote_type: str) -> list[CoffeeQuote]:
    row_pattern = re.compile(
        r"<tr>\s*"
        r"<td>\s*(?P<date>\d{2}/\d{2}/\d{4})\s*</td>\s*"
        r"<td>\s*(?P<brl>[-+]?\d{1,3}(?:\.\d{3})*,\d{2})\s*</td>\s*"
        r"<td>\s*(?P<day>[-+]?\d{1,3},\d{2})\s*</td>\s*"
        r"</tr>",
        re.IGNORECASE,
    )
    fetched_at = app_now()
    quotes: list[CoffeeQuote] = []
    for match in row_pattern.finditer(html):
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
                variation_month=None,
                price_usd=None,
                source=CEPEA_SOURCE,
                source_url=CEPEA_COFFEE_URL,
                fetched_at=fetched_at,
            )
        )
    _fill_month_variation_from_history(quotes)
    return quotes


def _fill_month_variation_from_history(quotes: list[CoffeeQuote]) -> None:
    if not quotes:
        return
    oldest_price_by_month: dict[tuple[int, int], float] = {}
    for quote in sorted(quotes, key=lambda item: item.quote_date):
        if quote.price_brl is None or quote.price_brl == 0:
            continue
        month_key = (quote.quote_date.year, quote.quote_date.month)
        oldest_price_by_month.setdefault(month_key, float(quote.price_brl))
    for quote in quotes:
        month_key = (quote.quote_date.year, quote.quote_date.month)
        month_base = oldest_price_by_month.get(month_key)
        if not month_base or quote.price_brl is None:
            continue
        quote.variation_month = round(((float(quote.price_brl) - month_base) / month_base) * 100, 2)


def fetch_noticias_agricolas_cepea_quotes(client: httpx.Client) -> list[CoffeeQuote]:
    quotes: list[CoffeeQuote] = []
    for quote_type, url in NOTICIAS_AGRICOLAS_CEPEA_URLS.items():
        try:
            response = client.get(url)
            response.raise_for_status()
        except Exception as exc:
            logger.warning("Nao foi possivel buscar espelho CEPEA %s: %s", quote_type, exc)
            continue
        quotes.extend(parse_noticias_agricolas_cepea_quotes(response.text, quote_type))
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
        and all(quote.variation_month is not None for quote in latest_quotes if quote)
    ):
        return False
    quotes: list[CoffeeQuote] = []
    headers = {"User-Agent": "SiSFarm/1.0 (+https://app.sisfarm.com.br)"}
    with httpx.Client(timeout=6.0, follow_redirects=True, headers=headers) as client:
        try:
            response = client.get(CEPEA_COFFEE_URL)
            response.raise_for_status()
            quotes = parse_cepea_coffee_quotes(response.text)
        except Exception as exc:
            logger.warning("Nao foi possivel atualizar cotacao de cafe CEPEA diretamente: %s", exc)
        if not quotes:
            quotes = fetch_noticias_agricolas_cepea_quotes(client)
    if not quotes:
        logger.warning("Cotacoes de cafe CEPEA nao retornaram dados interpretaveis.")
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
