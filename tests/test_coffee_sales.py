import asyncio
from datetime import date, datetime
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest
from jinja2 import ChoiceLoader, DictLoader
from starlette.datastructures import FormData, QueryParams

from app.models import FinanceAccount, HarvestRecord, Plot
from app.models.coffee_commercialization import CoffeeCommercializationRecord
from app.models.finance_transaction import FinanceTransaction
from app.services.coffee_sales import compute_values, group_sales, parse_decimal
from app.services.entitlements import UNRESTRICTED
from tests.test_stock_assistant import _route_env


def test_compute_values_splits_discounts_by_volume():
    lines = [{"sacks": 60.0}, {"sacks": 64.0}]
    v = compute_values(lines, 1900, 1.5, 0, 0)
    assert v["gross"] == 235600.0 and v["discount"] == 3534.0 and v["net"] == 232066.0
    assert sum(line["gross"] for line in v["lines"]) == v["gross"]
    assert sum(line["discount"] for line in v["lines"]) == v["discount"]
    with pytest.raises(ValueError):
        compute_values([{"sacks": 1.0}], 10, 0, 0, 50)


def test_parse_decimal_accepts_brazilian_format():
    assert parse_decimal("1.900,50") == 1900.5
    assert parse_decimal("R$ 2.041") == 2041.0 or parse_decimal("2041") == 2041.0
    assert parse_decimal("1,5%") == 1.5
    assert parse_decimal("") is None


@pytest.fixture
def env(monkeypatch):
    routes, db, user, scope, *_ = _route_env(monkeypatch, [])
    plot = Plot(name="Talhão 07", farm_id=scope["active_farm_id"], area_hectares=6.8, plant_count=1)
    db.add(plot)
    db.commit()
    lots = []
    for code, sacks in (("L-016", 90), ("L-011", 64)):
        lot = HarvestRecord(plot_id=plot.id, season_id=scope["active_season_id"], harvest_date=date(2026, 8, 21),
                            lot_code=code, sacks_produced=sacks)
        db.add(lot)
        lots.append(lot)
    account = FinanceAccount(farm_id=scope["active_farm_id"], account_name="Banco", bank_code="001", bank_name="Banco",
                             initial_balance_date=date(2026, 1, 1), initial_balance=0, created_at=datetime(2026, 1, 1))
    db.add(account)
    db.commit()
    monkeypatch.setattr(routes, "_scoped_plot_filters", lambda *args: ([scope["active_farm_id"]], None))
    monkeypatch.setattr(routes, "_launch_scope_or_redirect", lambda *args, **kw: (scope, None))
    monkeypatch.setattr(routes, "validate_csrf", lambda request, token: None)

    def page_context(request, user, csrf_token, page, **kwargs):
        kwargs.pop("_repo", None)
        return {"request": request, "user": user, "csrf_token": csrf_token, "can_write_page": True,
                "plan_entitlements": UNRESTRICTED, "flash": request.session.pop("flash", None), **scope, **kwargs}

    monkeypatch.setattr(routes, "_base_context", page_context)
    environment = routes.templates.env.overlay(loader=ChoiceLoader([
        DictLoader({"base.html": "{% block content %}{% endblock %}{% block scripts %}{% endblock %}"}),
        routes.templates.env.loader,
    ]))
    monkeypatch.setattr(routes.templates, "env", environment)
    session = {}

    def request(url, form=None):
        parsed = urlsplit(url)
        req = SimpleNamespace(url=parsed, query_params=QueryParams(parsed.query), session=session,
                              url_for=lambda name, path: f"/{name}/{path}")
        if form is not None:
            async def get_form():
                return FormData([("csrf_token", "x"), *form])
            req.form = get_form
        return req

    return SimpleNamespace(routes=routes, db=db, user=user, scope=scope, lots=lots, account=account, request=request, session=session)


def _sale_form(env, lines, **extra):
    fields = [("lot_id", str(lot.id)) for lot, _ in lines]
    fields += [(f"lot_sacks_{lot.id}", qty) for lot, qty in lines]
    base = {"buyer_name": "Exportadora Sul", "sale_date": "2026-11-20", "unit_price": "1.900,00", "status": "negociado",
            "funrural_percent": "1,5", "payment_condition": "a_vista", "finance_account_id": str(env.account.id),
            "coffee_type": "beneficiado", "delivery_due_date": "2026-12-10"}
    base.update(extra)
    return fields + list(base.items())


def test_multi_lot_sale_creates_one_sale_with_one_finance_entry(env):
    r = env.routes
    lot_a, lot_b = env.lots
    response = asyncio.run(r.sale_create_action(env.request("/producao/vendas", _sale_form(env, [(lot_a, "60"), (lot_b, "64")])),
                                                db=env.db, user=env.user))
    assert response.status_code == 303 and "venda=V-0001" in response.headers["location"]
    records = env.db.query(CoffeeCommercializationRecord).order_by(CoffeeCommercializationRecord.id).all()
    assert [r.sale_code for r in records] == ["V-0001", "V-0001"]
    assert [float(r.equivalent_sacks) for r in records] == [60.0, 64.0]
    assert records[0].finance_transaction_id == records[1].finance_transaction_id
    tx = env.db.get(FinanceTransaction, records[0].finance_transaction_id)
    assert float(tx.amount) == 232066.0
    group = group_sales(records)[0]
    assert group["sacks"] == 124 and group["net"] == 232066.0 and group["status_label"] == "Negociada"

    # Edição: tira um lote, mantém o código e a mesma entrada no Financeiro.
    response = asyncio.run(r.sale_update_action("V-0001", env.request("/producao/vendas/V-0001/editar",
                                                _sale_form(env, [(lot_a, "90")], funrural_percent="0")),
                                                db=env.db, user=env.user))
    assert response.status_code == 303
    records = env.db.query(CoffeeCommercializationRecord).all()
    assert len(records) == 1 and records[0].sale_code == "V-0001" and float(records[0].equivalent_sacks) == 90
    assert records[0].finance_transaction_id == tx.id
    env.db.refresh(tx)
    assert float(tx.amount) == 171000.0

    # Mais do que o saldo: volta para a tela com o erro e os dados preenchidos.
    page = asyncio.run(r.sale_create_action(env.request("/producao/vendas", _sale_form(env, [(lot_a, "1")])),
                                            db=env.db, user=env.user))
    assert page.status_code == 422
    assert "ultrapassa o saldo" in page.body.decode()

    r.sale_status_action("V-0001", env.request("/x"), csrf_token="x", status="entregue", status_date="2026-11-28",
                         db=env.db, user=env.user)
    env.db.refresh(records[0])
    assert records[0].status == "entregue" and records[0].delivered_on == date(2026, 11, 28)

    r.sale_delete_action("V-0001", env.request("/x"), csrf_token="x", db=env.db, user=env.user)
    assert env.db.query(CoffeeCommercializationRecord).count() == 0
    assert env.db.get(FinanceTransaction, tx.id) is None


def test_sale_form_page_lists_lots_and_preselects_origin_lot(env):
    lot_a, _ = env.lots
    html = env.routes.sale_new_page(env.request(f"/producao/vendas/nova?lote={lot_a.id}"), db=env.db, user=env.user,
                                    csrf_token="x").body.decode()
    assert 'data-sale-wizard' in html and "L-016" in html and "L-011" in html
    assert f'name="lot_id" value="{lot_a.id}" id="vw-lot-{lot_a.id}" checked' in html
    assert f'name="origem_lote" value="{lot_a.id}"' in html
    assert "Conferir" in html and "Funrural / Senar" in html and "data-buyer-picker" in html
