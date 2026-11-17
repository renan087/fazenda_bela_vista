from types import SimpleNamespace as NS
from xml.dom import minidom

from app.services import mascot
from app.services.entitlements import UNRESTRICTED
from app.core.permissions_catalog import PAGE_INPUTS, PAGE_MOBILE, WRITE_INPUTS, WRITE_MOBILE
from app.web.routes import _pezinho_context


def test_every_state_is_a_valid_animated_svg(tmp_path):
    written = mascot.build(tmp_path)
    assert {p.stem for p in written} >= set(mascot.STATES)
    for path in written:
        doc = minidom.parse(str(path))
        assert doc.documentElement.tagName == "svg"
        assert "@keyframes" in path.read_text(encoding="utf-8")


def test_static_files_match_the_generator():
    folder = mascot.Path(mascot.__file__).resolve().parents[1] / "static" / "img" / "pezinho"
    for state in mascot.STATES:
        assert (folder / f"{state}.svg").read_text(encoding="utf-8") == mascot.mascot_svg(state)


def _ctx(**extra):
    base = {"permission_codes": {PAGE_MOBILE, WRITE_MOBILE, PAGE_INPUTS, WRITE_INPUTS}, "plan_entitlements": UNRESTRICTED,
            "active_farm": None, "active_season": None, "modal_mode": False}
    base.update(extra)
    return base


def test_pezinho_context_needs_assistant_permission():
    request = NS(url=NS(path="/dashboard"))
    user = NS(id=1, display_name="Renan Correia", name="Renan")
    assert _pezinho_context(request, None, user, _ctx(permission_codes={PAGE_MOBILE})) is None
    data = _pezinho_context(request, None, user, _ctx())
    assert data["name"] == "Renan" and data["ready"] is False and data["pending"] is None
    assert [item[0] for item in data["suggestions"]] == ["Estoque", "Safra"]


def test_pezinho_hidden_on_assistant_page_and_modals():
    user = NS(display_name="Renan", name="Renan")
    assert _pezinho_context(NS(url=NS(path="/mobile/assistente")), None, user, _ctx()) is None
    assert _pezinho_context(NS(url=NS(path="/dashboard")), None, user, _ctx(modal_mode=True)) is None


def test_peek_svg_is_valid_and_keeps_hand_apart_from_moving_head():
    svg = mascot.peek_svg(92)
    doc = minidom.parseString(svg)
    classes = {node.getAttribute("class") for node in doc.getElementsByTagName("g")}
    assert {"pk-move", "pk-hand", "pk-eyes", "pk-pupil"} <= classes
    assert 'width="92"' in svg and 'aria-hidden="true"' in svg
