from pathlib import Path


def test_pest_header_uses_official_page_shell():
    root = Path(__file__).resolve().parents[1]
    template = (root / "app/templates/pests.html").read_text()
    assert "sf-page pd-page ft-page pest-page" in template
    assert "data-pests-page" in template
    assert "Pragas e pulverizações" in template
