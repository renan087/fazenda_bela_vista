import asyncio
import re
from datetime import date
from io import BytesIO
from types import SimpleNamespace
from urllib.parse import urlsplit

from jinja2 import ChoiceLoader, DictLoader
from starlette.datastructures import FormData, QueryParams, UploadFile

from app.models import HarvestRecord, Plot
from app.models.coffee_classification import CoffeeClassification
from app.models.coffee_commercialization import CoffeeCommercializationRecord
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
            response = routes.coffee_classification_page(req, db=db, user=user, csrf_token="x")
        elif req.url.path == "/producao/classificacao/nova":
            response = routes.coffee_classification_new_page(req, db=db, user=user, csrf_token="x")
        elif re.fullmatch(r"/producao/classificacao/\d+", req.url.path):
            response = routes.coffee_classification_detail_page(int(req.url.path.rsplit("/", 1)[1]), req, db=db, user=user, csrf_token="x")
        elif req.url.path.endswith("/comercializacao"):
            response = routes.commercialization_page(
                req, harvest_id=req.query_params.get("harvest_id"), status=req.query_params.get("status"),
                search=req.query_params.get("search"), db=db, user=user, csrf_token="x",
            )
        elif req.url.path == "/producao/vendas/nova":
            response = routes.sale_new_page(req, db=db, user=user, csrf_token="x")
        elif req.url.path.startswith("/producao/lotes/"):
            response = routes.lot_detail_page(int(req.url.path.rsplit("/", 1)[1]), req, db=db, user=user, csrf_token="x")
        else:
            response = routes.production_page(req, db=db, user=user, csrf_token="x")
        if response.status_code in (302, 303, 307):
            return get(response.headers["location"])
        return response.body.decode()

    def sell(quantity):
        async def form():
            return FormData([("csrf_token", "x"), ("lot_id", str(lot_id)), (f"lot_sacks_{lot_id}", str(quantity)),
                             ("origem_lote", str(lot_id)), ("buyer_name", "Comprador"), ("sale_date", "2026-11-05"),
                             ("unit_price", "100"), ("status", "negociado"), ("payment_condition", "a_vista")])
        req = request("/producao/vendas")
        req.form = form
        return asyncio.run(routes.sale_create_action(req, db=db, user=user))

    response = routes.create_harvest_action(
        request("/producao"), csrf_token="x", plot_id=plot.id, harvest_date="2026-11-01", lot_code=None,
        sacks_produced=10, harvest_type="Manual", coffee_stage="Cereja", initial_destination="Terreiro",
        responsible_name=None, work_shift=None, maturation_percentage=None, impurity_percentage=None,
        input_moisture_percentage=None, volume_count=None, notes=None, db=db, user=user,
    )
    lot_id = db.query(HarvestRecord).one().id
    other_lot = HarvestRecord(plot_id=plot.id, season_id=scope["active_season_id"],
                              harvest_date=date(2026, 11, 1), lot_code="Outro lote", sacks_produced=5)
    db.add(other_lot)
    db.commit()
    lot_url = f"/producao/lotes/{lot_id}"
    assert response.headers["location"] == lot_url
    page = get(lot_url)
    assert 'data-production-focused-lot' in page
    assert 'Sem classificação' in page

    url = f"/producao/classificacao?harvest_id={lot_id}&novo=1&origem=producao"
    assert f'href="{url.replace("&", "&amp;")}"' in page
    classification = get(url)
    assert f'name="harvest_id" value="{lot_id}" data-code="2026-L001" checked' in classification
    assert 'name="origem" value="producao"' in classification
    legacy_analysis = CoffeeClassification(
        harvest_id=lot_id, analysis_date=date(2026, 11, 1), source="laudo", beverage="Dura",
        report_filename="laudo-antigo.pdf", report_data=b"%PDF-1.4\nantigo",
    )
    db.add(legacy_analysis)
    db.commit()

    async def analysis_form():
        return FormData([
            ("csrf_token", "x"), ("harvest_id", str(lot_id)), ("origem", "producao"),
            ("analysis_date", "2026-11-02"), ("source", "laudo"), ("beverage", "Mole"), ("sensory_score", "85"),
            ("report", UploadFile(filename="laudo-a.pdf", file=BytesIO(b"%PDF-1.4\na"))),
            ("report", UploadFile(filename="laudo-b.pdf", file=BytesIO(b"%PDF-1.4\nb"))),
        ])

    analysis_request = request("/producao/classificacao")
    analysis_request.form = analysis_form
    response = asyncio.run(routes.create_coffee_classification(analysis_request, db, user))
    assert response.headers["location"] == lot_url
    page = get(lot_url)
    assert 'Classificado' in page and 'Mole' in page
    newest_analysis = db.query(CoffeeClassification).order_by(CoffeeClassification.id.desc()).first()
    quality = page[page.index('lt-quality'):]
    assert quality.index('datetime="2026-11-02"') < quality.index('datetime="2026-11-01"')
    assert f'/producao/classificacao/{legacy_analysis.id}/laudo"' in page
    for attachment in newest_analysis.attachments:
        assert f'/producao/classificacao/{newest_analysis.id}/laudo?attachment_id={attachment.id}' in page
    detail = get(f"/producao/classificacao?harvest_id={lot_id}&origem=producao&analise={newest_analysis.id}")
    assert f'/producao/classificacao/{newest_analysis.id}/editar' in detail and 'Mole' in detail
    assert 'Análises deste lote' in detail and f'href="/producao/classificacao/{legacy_analysis.id}"' in detail
    listing = get("/producao/classificacao")
    assert f'href="/producao/classificacao/{newest_analysis.id}"' in listing and 'Qualidade por lote' in listing

    sale_url = f"/producao/vendas/nova?lote={lot_id}"
    assert f'href="{sale_url}"' in page
    sale = get(f"/producao/comercializacao?harvest_id={lot_id}&origem=producao&launch=1")
    assert f'name="lot_id" value="{lot_id}" id="vw-lot-{lot_id}" checked' in sale
    assert f'name="origem_lote" value="{lot_id}"' in sale

    assert sell(4).headers["location"] == lot_url + "?venda_nova=V-0001"
    page = get(lot_url + "?venda_nova=V-0001")
    assert 'Venda V-0001 registrada' in page and 'Emitir NF' in page and 'Marcar entregue' in page
    assert 'Sem nota fiscal' in page
    assert '6,00 sc <small>disponíveis de 10,00</small>' in page
    assert 'Comprador' in page and 'V-0001 · 4,00 sc' in page and 'Venda V-0001 negociada' in page
    sale_details = get("/producao/comercializacao?venda=V-0001")
    assert 'id="venda-V-0001"' in sale_details and 'is-focus' in sale_details
    assert f'href="/producao?lote={lot_id}"' in sale_details
    other_sales = get(f"/producao/comercializacao?harvest_id={other_lot.id}")
    assert 'id="venda-V-0001"' not in other_sales

    retry = sell(7)
    assert retry.status_code == 422 and "ultrapassa o saldo" in retry.body.decode()

    assert sell(6).headers["location"] == lot_url + "?venda_nova=V-0002"
    page = get(lot_url)
    assert 'Sem saldo para venda' in page and 'Ver vendas' in page
    assert 'Vender deste lote' not in page
    assert 'O lote escolhido está indisponível para uma nova venda' in get(sale_url)
