from pathlib import Path


def test_mobile_profile_menu_can_open_and_stays_inside_viewport():
    root = Path(__file__).resolve().parents[1]
    css = (root / "app/static/css/styles.css").read_text()
    mobile = css.split("@media (max-width: 1023px) {\n    #erp-header-session-flyout", 1)[1].split("\n}\n", 1)[0]
    assert "display: none" not in mobile
    assert "position: fixed" in mobile
    assert "calc(100vw - 1rem)" in mobile
    assert "overflow-y: auto" in mobile
    assert "#erp-header-session-flyout.is-open" in mobile
    assert "visibility: visible" in mobile
    assert "pointer-events: auto" in mobile
