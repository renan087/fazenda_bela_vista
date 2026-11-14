from types import SimpleNamespace

from app.web.routes import _plot_allowed_for_season_launch, _season_launch_plots


def test_linked_season_filters_new_plot_choices():
    linked = SimpleNamespace(id=1, farm_id=10)
    other = SimpleNamespace(id=2, farm_id=10)
    scope = {"active_farm_id": 10, "active_season": SimpleNamespace(plots=[linked])}
    assert _season_launch_plots([linked, other], scope) == [linked]
    assert _plot_allowed_for_season_launch(linked, scope)
    assert not _plot_allowed_for_season_launch(other, scope)


def test_legacy_season_without_links_keeps_existing_choices():
    plot = SimpleNamespace(id=1, farm_id=10)
    scope = {"active_farm_id": 10, "active_season": SimpleNamespace(plots=[])}
    assert _season_launch_plots([plot], scope) == [plot]
    assert _plot_allowed_for_season_launch(plot, scope)
