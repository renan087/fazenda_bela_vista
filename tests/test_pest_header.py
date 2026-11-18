from pathlib import Path


def test_pest_header_covers_scrolling_content():
    root = Path(__file__).resolve().parents[1]
    template = (root / "app/templates/pests.html").read_text()
    css = (root / "app/static/css/styles.css").read_text()
    assert "stock-page-shell sf-page pest-page" in template
    assert "body:has(.pest-page) .hd { background: #fff; }" in css
    assert "body .pest-page .stock-page-header { top: 69px; background: #f1f5f9; }" in css
