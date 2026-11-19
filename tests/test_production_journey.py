import asyncio
import re
from types import SimpleNamespace
from urllib.parse import urlsplit

from jinja2 import ChoiceLoader, DictLoader
from starlette.datastructures import FormData, QueryParams

from app.models import HarvestRecord, Plot
from app.services.entitlements import UNRESTRICTED
from tests.test_stock_assistant import _route_env


def test_lot_journey_from_harvest_through_classification_and_partial_sales(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    plot = Plot(name="Setor 1", farm_id=scope["active_farm_id"], area_hectares=1, plant_count=1)
    db.add(plot)
    db.commit()

    monkeypatch.setattr(routes, "_scoped_plot_filters", lambda *args: ([scope["active_farm_id"]], None))
    monkeypatch.setattr(routes, "_launch_scope_or_redirect", lambda *args: (scope, None))

    def page_context(request, user, csrf_token, page, **kwargs):
        kwargs.pop("_repo")
        return {
            "request": request, "user": user, "csrf_token": csrf_token, "can_write_page": True,
            "plan_entitlements": UNRESTRICTED, "flash": request.session.pop("flash", None), **scope, **kwargs,
        }

    monkeypatch.setattr(routes, "_base_context", page_context)
    environment = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({"base.html": "{% block content %}{% endblock %}{% block scripts %}{% endblock %}"}),
        routes.templates.env.loader,
    ]))
    monkeypatch.setattr(routes.templates, "env", environment)

    session = {}

    def request(url):
        parsed = urlsplit(url)
        return SimpleNamespace(
            url=parsed, query_params=QueryParams(parsed.query), session=session,
            url_for=lambda name, path: f"/{name}/{path}",
        )

    def get(url):
        req = request(url)
        if req.url.path.endswith("/classificacao"):
            return routes.coffee_classification_page(req, db=db, user=user, csrf_token="x").body.decode()
        if req.url.path.endswith("/comercializacao"):
            return routes.commercialization_page(
                req, harvest_id=req.query_params.get("harvest_id"), db=db, user=user, csrf_token="x",
            ).body.decode()
        return routes.production_page(req, db=db, user=user, csrf_token="x").body.decode()

    def sell(quantity):
        return routes.create_commercialization_action(
            request("/producao/comercializacao"), csrf_token="x", harvest_id=lot_id, sale_date="2026-11-05",
            buyer_name="Comprador", quantity_sold=quantity, sale_unit="sc_60", unit_price=100,
            status="negociado", coffee_type=None, finance_account_id=None, payment_method=None,
            payment_condition="a_vista", installment_count=None, installment_frequency=None,
            first_installment_date=None, notes=None, db=db, user=user, origem="producao",
        )

    response = routes.create_harvest_action(
        request("/producao"), csrf_token="x", plot_id=plot.id, harvest_date="2026-11-01", lot_code=None,
        sacks_produced=10, harvest_type="Manual", coffee_stage="Cereja", initial_destination="Terreiro",
        responsible_name=None, work_shift=None, maturation_percentage=None, impurity_percentage=None,
        input_moisture_percentage=None, volume_count=None, notes=None, db=db, user=user,
    )
    lot_id = db.query(HarvestRecord).one().id
    lot_url = f"/producao?lote={lot_id}"
    assert response.headers["location"] == lot_url
    page = get(lot_url)
    assert 'data-production-focused-lot' in page
    assert 'Sem classificação' in page

    url = f"/producao/classificacao?harvest_id={lot_id}&novo=1&origem=producao"
    assert f'href="{url.replace("&", "&amp;")}"' in page
    classification = get(url)
    assert f'<option value="{lot_id}" selected>' in classification
    assert 'name="origem" value="producao"' in classification
    async def analysis_form():
        return FormData({
            "csrf_token": "x", "harvest_id": str(lot_id), "origem": "producao",
            "analysis_date": "2026-11-02", "source": "informado", "beverage": "Mole", "sensory_score": "85",
        })

    analysis_request = request("/producao/classificacao")
    analysis_request.form = analysis_form
    response = asyncio.run(routes.create_coffee_classification(analysis_request, db, user))
    assert response.headers["location"] == lot_url
    page = get(lot_url)
    assert 'Classificado' in page and 'Mole' in page

    sale_url = f"/producao/comercializacao?harvest_id={lot_id}&origem=producao&launch=1"
    assert f'href="{sale_url.replace("&", "&amp;")}"' in page
    sale = get(sale_url)
    assert re.search(rf'<option value="{lot_id}" data-available-sacks="10\.0"[^>]+ selected>', sale)
    assert 'name="origem" value="producao"' in sale

    assert sell(4).headers["location"] == lot_url
    page = get(lot_url)
    assert 'Parcialmente vendido' in page and '6,00 sc de saldo estimado' in page

    response = sell(7)
    retry = get(response.headers["location"])
    assert 'ultrapassa o saldo disponível do lote' in retry
    assert 'name="origem" value="producao"' in retry

    assert sell(6).headers["location"] == lot_url
    page = get(lot_url)
    assert 'Sem saldo para venda' in page and 'Ver vendas' in page
    assert 'Registrar venda' not in page
    assert 'O lote está indisponível para uma nova venda' in get(sale_url)
