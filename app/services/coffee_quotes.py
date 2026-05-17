import logging
import re
from html import unescape
from datetime import date, datetime
from pathlib import Path

import httpx

from app.core.timezone import app_now, today_in_app_timezone
from app.models import CoffeeQuote
from app.repositories.farm import FarmRepository

logger = logging.getLogger(__name__)

CEPEA_COFFEE_URL = "https://www.cepea.org.br/br/indicador/cafe.aspx?mobile="
CEPEA_WIDGET_URL = (
    "https://www.cepea.org.br/br/widgetproduto.js.php?"
    "fonte=arial&tamanho=10&largura=400px&corfundo=dbd6b2&cortexto=333333&corlinha=ede7bf"
    "&id_indicador%5B%5D=23&id_indicador%5B%5D=24"
)
CEPEA_SOURCE = "CEPEA/ESALQ"
CECAFE_CEPEA_URL = "https://www.cecafe.com.br/en/market-indicators/cepea-esalq-prices/"
CECAFE_CEPEA_AJAX_URL = "https://www.cecafe.com.br/site/wp-admin/admin-ajax.php?action=get_wdtable&table_id=103"
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


def _parse_cepea_xls_date(value: object, datemode: int) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        try:
            import xlrd

            return xlrd.xldate.xldate_as_datetime(value, datemode).date()
        except Exception:
            return None
    return _parse_brazilian_date(str(value))


def _parse_cepea_xls_decimal(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return _parse_brazilian_decimal(str(value))


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
    _calculate_variations_from_series(quotes)
    return quotes


def parse_cepea_widget_quotes(script: str) -> list[CoffeeQuote]:
    row_pattern = re.compile(
        r"<tr>\s*"
        r"<td>\s*(?P<date>\d{2}/\d{2}/\d{4})\s*</td>\s*"
        r"<td>\s*(?P<product>.*?)</td>\s*"
        r"<td>\s*R\$\s*<span[^>]*>\s*(?P<brl>[-+]?\d{1,3}(?:\.\d{3})*,\d{2})\s*</span>\s*</td>\s*"
        r"</tr>",
        re.IGNORECASE | re.DOTALL,
    )
    fetched_at = app_now()
    quotes: list[CoffeeQuote] = []
    for match in row_pattern.finditer(script):
        product_text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", unescape(match.group("product")))).strip().lower()
        quote_type = "robusta" if any(token in product_text for token in ("robusta", "conillon")) else "arabica"
        quote_date = _parse_brazilian_date(match.group("date"))
        price_brl = _parse_brazilian_decimal(match.group("brl"))
        if not quote_date or price_brl is None:
            continue
        quotes.append(
            CoffeeQuote(
                quote_type=quote_type,
                quote_date=quote_date,
                price_brl=price_brl,
                variation_day=None,
                variation_month=None,
                price_usd=None,
                source=CEPEA_SOURCE,
                source_url=CECAFE_CEPEA_URL,
                fetched_at=fetched_at,
            )
        )
    return quotes


def parse_cecafe_cepea_rows(rows: list[list[str]]) -> list[CoffeeQuote]:
    fetched_at = app_now()
    quotes: list[CoffeeQuote] = []
    for row in rows:
        if len(row) < 5:
            continue
        quote_date = _parse_brazilian_date(str(row[0]))
        if not quote_date:
            continue
        for quote_type, brl_index, usd_index in (("arabica", 1, 2), ("robusta", 3, 4)):
            price_brl = _parse_brazilian_decimal(str(row[brl_index]))
            if price_brl is None:
                continue
            quotes.append(
                CoffeeQuote(
                    quote_type=quote_type,
                    quote_date=quote_date,
                    price_brl=price_brl,
                    variation_day=None,
                    variation_month=None,
                    price_usd=_parse_brazilian_decimal(str(row[usd_index])),
                    source=CEPEA_SOURCE,
                    source_url=CECAFE_CEPEA_URL,
                    fetched_at=fetched_at,
                )
            )
    return quotes


def parse_cecafe_rendered_quotes(html: str) -> list[CoffeeQuote]:
    rows: list[list[str]] = []
    for match in re.finditer(r'<tr id="table_103_row_\d+".*?</tr>', html, re.IGNORECASE | re.DOTALL):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", match.group(0), re.IGNORECASE | re.DOTALL)
        clean_cells = [
            re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", unescape(cell))).strip()
            for cell in cells
        ]
        if clean_cells:
            rows.append(clean_cells)
    return parse_cecafe_cepea_rows(rows)


def parse_cepea_xls_quotes(file_path: str | Path, quote_type: str | None = None) -> list[CoffeeQuote]:
    try:
        import xlrd
    except ImportError as exc:
        raise RuntimeError("A dependencia xlrd e necessaria para importar arquivos .xls do CEPEA.") from exc

    workbook = xlrd.open_workbook(str(file_path), ignore_workbook_corruption=True)
    sheet = workbook.sheet_by_index(0)
    if sheet.nrows < 5 or sheet.ncols < 3:
        return []

    title = str(sheet.cell_value(0, 0) or "").upper()
    inferred_type = quote_type
    if inferred_type is None:
        if "ROBUSTA" in title or "CONILLON" in title:
            inferred_type = "robusta"
        elif "ARABICA" in title or "ARÁBICA" in title:
            inferred_type = "arabica"
    if inferred_type not in {"arabica", "robusta"}:
        raise ValueError("Nao foi possivel identificar se o arquivo CEPEA e de arabica ou robusta.")

    fetched_at = app_now()
    quotes: list[CoffeeQuote] = []
    for row_index in range(4, sheet.nrows):
        quote_date = _parse_cepea_xls_date(sheet.cell_value(row_index, 0), workbook.datemode)
        price_brl = _parse_cepea_xls_decimal(sheet.cell_value(row_index, 1))
        if not quote_date or price_brl is None or price_brl <= 0:
            continue
        quotes.append(
            CoffeeQuote(
                quote_type=inferred_type,
                quote_date=quote_date,
                price_brl=round(price_brl, 2),
                variation_day=None,
                variation_month=None,
                price_usd=_parse_cepea_xls_decimal(sheet.cell_value(row_index, 2)),
                source=CEPEA_SOURCE,
                source_url=CEPEA_COFFEE_URL,
                fetched_at=fetched_at,
            )
        )
    _calculate_variations_from_series(quotes)
    return quotes


def _calculate_variations_from_series(quotes: list[CoffeeQuote]) -> None:
    by_type: dict[str, list[CoffeeQuote]] = {}
    for quote in quotes:
        by_type.setdefault(quote.quote_type, []).append(quote)
    for quote_type_rows in by_type.values():
        rows = sorted(quote_type_rows, key=lambda item: item.quote_date)
        previous_by_date: dict[date, CoffeeQuote] = {}
        latest_before_month: dict[tuple[int, int], CoffeeQuote] = {}
        previous: CoffeeQuote | None = None
        for quote in rows:
            if previous:
                previous_by_date[quote.quote_date] = previous
            month_key = (quote.quote_date.year, quote.quote_date.month)
            if month_key not in latest_before_month:
                latest_before_month[month_key] = previous
            previous = quote

        for quote in rows:
            previous_quote = previous_by_date.get(quote.quote_date)
            if previous_quote and previous_quote.price_brl:
                quote.variation_day = round(((float(quote.price_brl) - float(previous_quote.price_brl)) / float(previous_quote.price_brl)) * 100, 2)
            month_base = latest_before_month.get((quote.quote_date.year, quote.quote_date.month))
            if month_base and month_base.price_brl:
                quote.variation_month = round(((float(quote.price_brl) - float(month_base.price_brl)) / float(month_base.price_brl)) * 100, 2)


def _has_previous_month_base(quotes: list[CoffeeQuote], quote_type: str) -> bool:
    rows = sorted((quote for quote in quotes if quote.quote_type == quote_type), key=lambda item: item.quote_date)
    if not rows:
        return False
    latest = rows[-1]
    return any(row.quote_date < latest.quote_date.replace(day=1) for row in rows)


def _merge_quotes_by_type_and_date(*quote_groups: list[CoffeeQuote]) -> list[CoffeeQuote]:
    merged: dict[tuple[str, date], CoffeeQuote] = {}
    for group in quote_groups:
        for quote in group:
            key = (quote.quote_type, quote.quote_date)
            existing = merged.get(key)
            if existing is None:
                merged[key] = quote
                continue
            existing.price_brl = quote.price_brl if quote.price_brl is not None else existing.price_brl
            existing.price_usd = quote.price_usd if quote.price_usd is not None else existing.price_usd
            existing.variation_day = quote.variation_day if quote.variation_day is not None else existing.variation_day
            existing.variation_month = quote.variation_month if quote.variation_month is not None else existing.variation_month
            existing.source_url = quote.source_url or existing.source_url
            existing.fetched_at = quote.fetched_at or existing.fetched_at
    return list(merged.values())


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


def fetch_cecafe_cepea_quotes(client: httpx.Client, *, length: int = 120) -> list[CoffeeQuote]:
    response = client.get(CECAFE_CEPEA_URL)
    response.raise_for_status()
    nonce_match = re.search(r'id="wdtNonceFrontendServerSide_103"[^>]*value="([^"]+)"', response.text)
    rendered_quotes = parse_cecafe_rendered_quotes(response.text)
    if not nonce_match:
        return rendered_quotes

    nonce = nonce_match.group(1)
    data: dict[str, str] = {
        "draw": "1",
        "start": "0",
        "length": str(length),
        "wdtNonce": nonce,
        "wdtNonceFrontendServerSide_103": nonce,
        "order[0][column]": "0",
        "order[0][dir]": "desc",
        "search[value]": "",
        "search[regex]": "false",
    }
    columns = ("data", "arabica_rs", "arabica_usd", "conillon_rs", "conillon_usd", "id")
    for index, name in enumerate(columns):
        data[f"columns[{index}][data]"] = name
        data[f"columns[{index}][name]"] = name
        data[f"columns[{index}][searchable]"] = "true"
        data[f"columns[{index}][orderable]"] = "true"
        data[f"columns[{index}][search][value]"] = ""
        data[f"columns[{index}][search][regex]"] = "false"
    ajax_response = client.post(
        CECAFE_CEPEA_AJAX_URL,
        data=data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": CECAFE_CEPEA_URL,
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    ajax_response.raise_for_status()
    try:
        payload = ajax_response.json()
    except ValueError:
        return rendered_quotes
    rows = payload.get("data")
    if not isinstance(rows, list):
        return rendered_quotes
    quotes = parse_cecafe_cepea_rows(rows)
    return quotes or rendered_quotes


def fetch_cepea_widget_with_cecafe_history_quotes(client: httpx.Client) -> list[CoffeeQuote]:
    widget_response = client.get(
        CEPEA_WIDGET_URL,
        headers={
            "Accept": "application/javascript,text/javascript,*/*;q=0.8",
            "Referer": CEPEA_COFFEE_URL,
        },
    )
    widget_response.raise_for_status()
    widget_quotes = parse_cepea_widget_quotes(widget_response.text)
    if not widget_quotes:
        return []
    try:
        history_quotes = fetch_cecafe_cepea_quotes(client, length=120)
    except Exception as exc:
        logger.warning("Nao foi possivel buscar serie Cecafe/CEPEA: %s", exc)
        history_quotes = []
        try:
            history_quotes = fetch_noticias_agricolas_cepea_quotes(client)
        except Exception as fallback_exc:
            logger.warning("Nao foi possivel buscar espelho Noticias Agricolas: %s", fallback_exc)
    official_quotes: list[CoffeeQuote] = []
    try:
        official_response = client.get(CEPEA_COFFEE_URL)
        official_response.raise_for_status()
        if _cepea_page_html_is_blocked(official_response.text):
            logger.warning(
                "Pagina CEPEA bloqueada por protecao anti-bot; usando widget + historico Cecafe para variacao mensal."
            )
        else:
            official_quotes = parse_cepea_coffee_quotes(official_response.text)
    except Exception as exc:
        logger.warning("Nao foi possivel buscar variacao oficial CEPEA: %s", exc)
        official_quotes = []
    quotes = _merge_quotes_by_type_and_date(history_quotes, widget_quotes)
    _calculate_variations_from_series(quotes)
    quotes = _merge_quotes_by_type_and_date(quotes, official_quotes)
    trusted_quote_types = {
        quote_type
        for quote_type in ("arabica", "robusta")
        if any(quote.quote_type == quote_type for quote in official_quotes) or _has_previous_month_base(quotes, quote_type)
    }
    for quote in quotes:
        if quote.quote_type in trusted_quote_types:
            quote.source_url = CEPEA_COFFEE_URL
    return quotes


def _http_client_headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 (compatible; SiSFarm/1.0; +https://app.sisfarm.com.br)",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    }


def _cepea_page_html_is_blocked(html: str) -> bool:
    lowered = html.lower()
    return (
        "just a moment" in lowered
        or "challenge-platform" in lowered
        or "cf_chl" in lowered
        or "enable javascript and cookies" in lowered
    )


def _fetch_remote_coffee_quotes(client: httpx.Client) -> list[CoffeeQuote]:
    quotes = fetch_cepea_widget_with_cecafe_history_quotes(client)
    if quotes:
        return quotes
    raise RuntimeError("Widget CEPEA/Cecafe nao retornou cotacoes interpretaveis.")


def _latest_coffee_quotes_from_remote_series(remote_quotes: list[CoffeeQuote]) -> dict[str, CoffeeQuote | None]:
    result: dict[str, CoffeeQuote | None] = {"arabica": None, "robusta": None}
    if not remote_quotes:
        return result
    _calculate_variations_from_series(remote_quotes)
    for quote_type in ("arabica", "robusta"):
        type_rows = [quote for quote in remote_quotes if quote.quote_type == quote_type]
        if not type_rows:
            continue
        latest = max(type_rows, key=lambda item: item.quote_date)
        if not _has_previous_month_base(remote_quotes, quote_type):
            latest.variation_month = None
        result[quote_type] = latest
    return result


def _build_latest_coffee_quotes_for_dashboard(repository: FarmRepository) -> dict[str, CoffeeQuote | None]:
    remote_quotes: list[CoffeeQuote] = []
    try:
        with httpx.Client(timeout=10.0, follow_redirects=True, headers=_http_client_headers()) as client:
            remote_quotes = _fetch_remote_coffee_quotes(client)
    except Exception as exc:
        logger.warning("Nao foi possivel buscar cotacoes remotas de cafe para o dashboard: %s", exc)

    result = _latest_coffee_quotes_from_remote_series(remote_quotes)
    for quote_type in ("arabica", "robusta"):
        latest = result.get(quote_type)
        if latest:
            repository.upsert_coffee_quote(latest)
            continue
        result[quote_type] = _resolve_latest_coffee_quote(repository, quote_type)
    if remote_quotes:
        for quote in remote_quotes:
            repository.upsert_coffee_quote(quote)
    return result


def refresh_cepea_coffee_quotes(repository: FarmRepository, *, force: bool = False) -> bool:
    today = today_in_app_timezone()
    latest_quotes = [
        repository.get_latest_coffee_quote("arabica"),
        repository.get_latest_coffee_quote("robusta"),
    ]
    headers = _http_client_headers()
    if (
        not force
        and all(latest_quotes)
        and all(quote.fetched_at and quote.fetched_at.date() >= today for quote in latest_quotes if quote)
    ):
        return False
    quotes: list[CoffeeQuote] = []
    with httpx.Client(timeout=10.0, follow_redirects=True, headers=headers) as client:
        quotes = _fetch_remote_coffee_quotes(client)
    if not quotes:
        logger.warning("Cotacoes de cafe CEPEA nao retornaram dados interpretaveis.")
        return False
    for quote in quotes:
        repository.upsert_coffee_quote(quote)
    return True


def _resolve_latest_coffee_quote(repository: FarmRepository, quote_type: str) -> CoffeeQuote | None:
    recent = repository.list_coffee_quotes(quote_type=quote_type, limit=12)
    if not recent:
        return None
    latest_date = recent[0].quote_date
    candidates = [row for row in recent if row.quote_date == latest_date]
    for row in candidates:
        if row.source_url == CEPEA_COFFEE_URL and row.variation_month is not None:
            return row
    for row in candidates:
        if row.variation_month is not None:
            return row
    return candidates[0]


def _coffee_quote_history_rows(repository: FarmRepository, quote_type: str, *, limit: int = 90) -> list[CoffeeQuote]:
    return list(reversed(repository.list_coffee_quotes(quote_type=quote_type, limit=limit)))


def latest_coffee_quote_context(repository: FarmRepository) -> dict:
    quotes = _build_latest_coffee_quotes_for_dashboard(repository)
    refresh_cepea_coffee_quotes(repository)
    history = {}
    for quote_type in ("arabica", "robusta"):
        chart_rows = _coffee_quote_history_rows(repository, quote_type)[-30:]
        history[quote_type] = {
            "labels": [row.quote_date.strftime("%d/%m") for row in chart_rows],
            "values": [float(row.price_brl or 0) for row in chart_rows],
        }
    return {
        "quotes": quotes,
        "history": history,
        "source": CEPEA_SOURCE,
        "source_url": CEPEA_COFFEE_URL,
    }
