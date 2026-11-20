from datetime import datetime, timezone, date
from types import SimpleNamespace

from app.models.whatsapp import WhatsAppClosingRequest, WhatsAppLink, WhatsAppSurvey
from app.web import routes
from tests.test_stock_assistant import _route_env


def test_dashboard_aggregates_filters_and_requires_super_admin(monkeypatch):
    _, db, user, scope, *_ = _route_env(monkeypatch, [])
    request = SimpleNamespace(url=SimpleNamespace(path='/organizacoes/avaliacoes-pezinho'), method='GET', session={})
    monkeypatch.setattr(routes, 'is_super_admin_email', lambda email: False)
    monkeypatch.setattr(routes, '_flash', lambda *args: None)
    assert routes.whatsapp_surveys_page(request, None, None, None, 1, db, user, 'x').status_code in (302, 303, 307)
    monkeypatch.setattr(routes, 'is_super_admin_email', lambda email: True)
    monkeypatch.setattr(routes, 'current_asaas_environment', lambda settings: SimpleNamespace(is_production=False))
    monkeypatch.setattr(routes, '_base_context', lambda request, user, csrf, page, **kwargs: kwargs)
    monkeypatch.setattr(routes.templates, 'TemplateResponse', lambda name, context: context)
    link = WhatsAppLink(user_id=user.id, organization_id=user.organization_id, farm_id=scope['active_farm_id'],
                        season_id=scope['active_season_id'], created_at=datetime(2026, 11, 1, tzinfo=timezone.utc))
    db.add(link)
    db.flush()
    for number, status, rating, when in [(1, 'answered', 'good', 3), (2, 'unrated', None, 4), (3, 'unavailable', None, 8)]:
        created = datetime(2026, 11, when, 15, tzinfo=timezone.utc)
        req = WhatsAppClosingRequest(id=str(number) * 32, link_id=link.id, status='closed', created_at=created)
        db.add(req)
        db.flush()
        db.add(WhatsAppSurvey(id=str(number + 3) * 32, closing_id=req.id, link_id=link.id,
                              status=status, rating=rating, created_at=created))
    db.commit()
    result = routes.whatsapp_surveys_page(request, date(2026, 11, 1), date(2026, 11, 5), user.organization_id, 1, db, user, 'x')
    assert result['total'] == 2
    assert result['counts']['good'] == 1 and result['counts']['unrated'] == 1
    assert result['counts']['unavailable'] == 0
    assert len(result['rows']) == 2
    db.close()


def test_dashboard_template_displays_current_organization_and_status():
    from jinja2 import ChoiceLoader, DictLoader
    environment = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({'base.html': '{% block content %}{% endblock %}'}), routes.templates.env.loader,
    ]))
    survey = SimpleNamespace(created_at=datetime(2026, 11, 4, tzinfo=timezone.utc), status='answered', rating='good', resolved_at=None)
    organization = SimpleNamespace(id=7, name='Fazenda Exemplo')
    page = environment.get_template('whatsapp_surveys.html').render(
        counts={'good': 1, 'fair': 0, 'bad': 0, 'unrated': 0, 'pending': 0, 'sending': 0, 'unavailable': 0},
        total=1, rows=[(survey, None, organization)], filters={}, organizations=[organization], pagina=1, pages=1,
        query_string='')
    assert 'Fazenda Exemplo' in page and '#7' in page and 'Bom' in page
    assert 'vínculo atual' in page
