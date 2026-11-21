from types import SimpleNamespace

from jinja2 import ChoiceLoader, DictLoader

from app.services import help_center
from app.core.permissions_catalog import PAGE_PRODUCTION
from app.web import routes
from tests.test_stock_assistant import _route_env


def test_documentation_search_respects_permissions_and_unknown_topics():
    entries = help_center.visible_articles(lambda _: True)
    assert len(entries) >= 10 and all(a['answer'] and a['path'].startswith('/') for a in entries)
    result = help_center.search_articles('classificar lote café', lambda _: True)
    assert any(a['path'] == '/producao/classificacao' for a in result['artigos'])
    denied = help_center.search_articles('classificar lote café', lambda code: code != PAGE_PRODUCTION)
    assert all(not a['path'].startswith('/producao') for a in denied['artigos'])
    assert not help_center.search_articles('xyzabc123', lambda _: True)['artigos']


def test_help_tool_is_available_in_both_channels(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    calls = []
    def fake_run(history, assistant, **kwargs):
        calls.append(assistant.call_tool('consultar_ajuda_sisfarm', '{"assunto":"colheita"}'))
        return {'resposta': 'Confira a Central de ajuda.'}
    monkeypatch.setattr(routes, 'run_farm_assistant', fake_run)
    routes._assistant_turn(db, user, routes._repository(db), scope, None, 'onde registro colheita?')
    routes._assistant_turn(db, user, routes._repository(db), scope, None, 'onde registro colheita?', title_prefix='WhatsApp · ')
    assert len(calls) == 2 and all('artigos' in result for result in calls)
    db.close()


def test_page_renders_documentation_without_raw_html():
    environment = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({'base.html': '{% block content %}{% endblock %}'}), routes.templates.env.loader]))
    entries = help_center.visible_articles(lambda _: True)
    page = environment.get_template('help_center.html').render(help_articles=entries)
    assert 'Perguntas frequentes' in page and 'Como registrar uma colheita?' in page
    assert 'href="/producao"' in page and '<!-- path:' not in page
