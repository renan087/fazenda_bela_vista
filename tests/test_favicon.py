from pathlib import Path

from PIL import Image

from app.main import favicon


def test_favicon_is_multisize_and_publicly_served():
    response = favicon()
    assert response.media_type == "image/x-icon"
    with Image.open(response.path) as icon:
        assert icon.format == "ICO"
        assert {(16, 16), (32, 32), (48, 48)} <= set(icon.ico.sizes())


def test_all_page_layouts_use_versioned_favicon():
    templates = Path(__file__).resolve().parents[1] / "app" / "templates"
    for name in ("base.html", "landing.html", "legal_base.html", "maintenance.html"):
        html = (templates / name).read_text(encoding="utf-8")
        assert '/favicon.ico?v=20261116' in html
        assert 'favicon.png' in html
