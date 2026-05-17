from datetime import date

from app.models import CoffeeQuote
from app.services.coffee_quotes import (
    _calculate_variations_from_series,
    _cepea_page_html_is_blocked,
    _latest_coffee_quotes_from_remote_series,
    parse_cepea_coffee_quotes,
)


SAMPLE_CEPEA_HTML = """
<html><body>
INDICADOR DO CAFÉ ARÁBICA CEPEA/ESALQ
15/05/2026 1.637,88 -0,55% -7,02% 7,12
14/05/2026 1.646,95 -0,12% -6,80% 7,16
INDICADOR DO CAFÉ ROBUSTA CEPEA/ESALQ
15/05/2026 930,15 -2,40% 0,53% 4,04
14/05/2026 953,00 -1,10% 0,20% 4,14
</body></html>
"""


def test_parse_cepea_coffee_quotes_reads_official_month_variation() -> None:
    quotes = parse_cepea_coffee_quotes(SAMPLE_CEPEA_HTML)
    by_type = {quote.quote_type: quote for quote in quotes if quote.quote_date == date(2026, 5, 15)}

    assert round(float(by_type["arabica"].variation_month), 2) == -7.02
    assert round(float(by_type["robusta"].variation_month), 2) == 0.53


def test_cepea_cloudflare_page_is_detected() -> None:
    assert _cepea_page_html_is_blocked('<html><title>Just a moment...</title></html>') is True
    assert _cepea_page_html_is_blocked(SAMPLE_CEPEA_HTML) is False


def test_month_variation_uses_previous_month_close() -> None:
    quotes = [
        CoffeeQuote(quote_type="arabica", quote_date=date(2026, 4, 30), price_brl=1761.57, source="CEPEA/ESALQ"),
        CoffeeQuote(quote_type="arabica", quote_date=date(2026, 5, 15), price_brl=1637.88, source="CEPEA/ESALQ"),
        CoffeeQuote(quote_type="robusta", quote_date=date(2026, 4, 30), price_brl=925.26, source="CEPEA/ESALQ"),
        CoffeeQuote(quote_type="robusta", quote_date=date(2026, 5, 15), price_brl=930.15, source="CEPEA/ESALQ"),
    ]
    _calculate_variations_from_series(quotes)
    arabica = next(quote for quote in quotes if quote.quote_type == "arabica" and quote.quote_date == date(2026, 5, 15))
    robusta = next(quote for quote in quotes if quote.quote_type == "robusta" and quote.quote_date == date(2026, 5, 15))
    assert round(float(arabica.variation_month), 2) == -7.02
    assert round(float(robusta.variation_month), 2) == 0.53


def test_latest_from_remote_series_ignores_stale_month_calculation() -> None:
    quotes = [
        CoffeeQuote(quote_type="arabica", quote_date=date(2026, 4, 30), price_brl=1761.57, source="CEPEA/ESALQ"),
        CoffeeQuote(quote_type="arabica", quote_date=date(2026, 5, 4), price_brl=1759.75, source="CEPEA/ESALQ"),
        CoffeeQuote(quote_type="arabica", quote_date=date(2026, 5, 15), price_brl=1637.88, source="CEPEA/ESALQ"),
        CoffeeQuote(quote_type="robusta", quote_date=date(2026, 4, 30), price_brl=925.26, source="CEPEA/ESALQ"),
        CoffeeQuote(quote_type="robusta", quote_date=date(2026, 5, 15), price_brl=930.15, source="CEPEA/ESALQ"),
    ]
    latest = _latest_coffee_quotes_from_remote_series(quotes)
    arabica = latest["arabica"]
    robusta = latest["robusta"]
    assert arabica is not None and arabica.quote_date == date(2026, 5, 15)
    assert robusta is not None and robusta.quote_date == date(2026, 5, 15)
    assert round(float(arabica.variation_month), 2) == -7.02
    assert round(float(robusta.variation_month), 2) == 0.53
    assert round(float(arabica.variation_month), 2) != -6.93


def test_latest_from_remote_series_clears_month_without_previous_month_close() -> None:
    quotes = [
        CoffeeQuote(quote_type="arabica", quote_date=date(2026, 5, 4), price_brl=1759.75, source="CEPEA/ESALQ"),
        CoffeeQuote(quote_type="arabica", quote_date=date(2026, 5, 15), price_brl=1637.88, variation_month=-6.93, source="CEPEA/ESALQ"),
    ]
    latest = _latest_coffee_quotes_from_remote_series(quotes)
    assert latest["arabica"] is not None
    assert latest["arabica"].variation_month is None
