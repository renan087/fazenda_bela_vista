from datetime import date

from app.services.coffee_quotes import parse_cepea_coffee_quotes


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
