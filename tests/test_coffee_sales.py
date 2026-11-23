import asyncio
import json
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


def _installments(env, records, amounts, first=date(2026, 1, 10)):
    from app.models.finance_transaction_installment import FinanceTransactionInstallment
    tx_id = records[0].finance_transaction_id
    env.db.query(FinanceTransactionInstallment).filter_by(finance_transaction_id=tx_id).delete()
    for n, amount in enumerate(amounts, start=1):
        env.db.add(FinanceTransactionInstallment(finance_transaction_id=tx_id, installment_number=n, amount=amount,
                                                 due_date=date(first.year + (first.month + n - 2) // 12, (first.month + n - 2) % 12 + 1, first.day),
                                                 status="pendente"))
    env.db.commit()
    return sorted(env.db.query(FinanceTransactionInstallment).filter_by(finance_transaction_id=tx_id).all(), key=lambda i: i.installment_number)


def _receipt_form(**fields):
    return list(fields.items())


def test_delivery_and_receipt_are_independent_and_receipt_pays_finance_installments(env):
    from app.services.coffee_sales import sale_records
    r = env.routes
    lot_a, _ = env.lots
    asyncio.run(r.sale_create_action(env.request("/producao/vendas", _sale_form(env, [(lot_a, "30")])), db=env.db, user=env.user))
    records = sale_records(env.db, env.scope["active_farm_id"], "V-0001")
    parcels = _installments(env, records, [100, 100, 100])

    # Pagamento antecipado: recebe duas parcelas antes de entregar.
    req = env.request("/x", _receipt_form(acao="receber", parcelas=f"{parcels[0].id},{parcels[1].id}"))
    asyncio.run(r.sale_receipt_action("V-0001", req, db=env.db, user=env.user))
    for item in parcels:
        env.db.refresh(item)
    assert [i.status for i in parcels] == ["pago", "pago", "pendente"]
    assert env.session["sale_toast"]["text"].startswith("2 parcelas recebidas")
    group = group_sales(sale_records(env.db, env.scope["active_farm_id"], "V-0001"))[0]
    assert not group["delivered"] and not group["received"]

    page = r.commercialization_page(env.request("/producao/comercializacao"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert 'class="sv-ic is-part"' in page and "--p: 67%" in page and "Recebido 67%" in page
    assert "2 parcelas recebidas" in page and ">Desfazer</button>" in page  # aviso rápido com desfazer

    # A última parcela fecha a venda como Recebida, ainda sem entrega.
    asyncio.run(r.sale_receipt_action("V-0001", env.request("/x", _receipt_form(acao="receber")), db=env.db, user=env.user))
    records = sale_records(env.db, env.scope["active_farm_id"], "V-0001")
    assert records[0].status == "recebido" and records[0].received_on and records[0].delivered_on is None

    asyncio.run(r.sale_delivery_action("V-0001", env.request("/x"), csrf_token="x", acao="entregar", data="2026-11-30",
                                       voltar=None, db=env.db, user=env.user))
    env.db.refresh(records[0])
    assert records[0].delivered_on == date(2026, 11, 30) and records[0].status == "recebido"

    # Desfazer o recebimento devolve as parcelas para pendente; a entrega continua.
    asyncio.run(r.sale_receipt_action("V-0001", env.request("/x", _receipt_form(acao="desfazer")), db=env.db, user=env.user))
    env.db.refresh(records[0])
    assert records[0].status == "entregue" and records[0].received_on is None
    for item in parcels:
        env.db.refresh(item)
    assert all(i.status == "pendente" and i.paid_at is None for i in parcels)

    page = r.commercialization_page(env.request("/producao/comercializacao?filtro=a_receber"), db=env.db, user=env.user,
                                    csrf_token="x").body.decode()
    assert 'id="venda-V-0001"' in page and "Selecione as parcelas" in page
    page = r.commercialization_page(env.request("/producao/comercializacao?filtro=a_entregar"), db=env.db, user=env.user,
                                    csrf_token="x").body.decode()
    assert 'id="venda-V-0001"' not in page


def test_many_open_installments_use_stepper_and_year_drawer(env):
    from app.services.coffee_sales import sale_records
    r = env.routes
    lot_a, _ = env.lots
    asyncio.run(r.sale_create_action(env.request("/producao/vendas", _sale_form(env, [(lot_a, "30")])), db=env.db, user=env.user))
    records = sale_records(env.db, env.scope["active_farm_id"], "V-0001")
    _installments(env, records, [50] * 14, first=date(2025, 11, 10))
    page = r.commercialization_page(env.request("/producao/comercializacao?por=30"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Receber as próximas" in page and "Escolher parcelas específicas…" in page
    assert 'id="sv-dw-V-0001"' in page and "<b>2025</b>" in page and "<b>2026</b>" in page
    assert "vencida" in page and '<option value="30" selected>' in page
    assert env.session["sales_per_page"] == 30


def test_sales_search_finds_sale_by_invoice_number(env, monkeypatch):
    r = env.routes
    lot_a, _ = env.lots
    asyncio.run(r.sale_create_action(env.request("/producao/vendas", _sale_form(env, [(lot_a, "30")])), db=env.db, user=env.user))
    monkeypatch.setattr(r, "_nf_notes_by_sale", lambda *a: {"V-0001": {"id": "d" * 32, "status": "autorizada", "label": "Autorizada",
                                                                       "tone": "green", "number": "000127", "issues": 0}})
    page = r.commercialization_page(env.request("/producao/comercializacao?search=127"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert 'id="venda-V-0001"' in page and "NF 000127" in page and "Ver nota" in page
    page = r.commercialization_page(env.request("/producao/comercializacao?filtro=sem_nota"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert 'id="venda-V-0001"' not in page


def test_quick_harvest_inside_sale_includes_plot_and_selects_new_lot(env, monkeypatch):
    r = env.routes
    season = env.scope["active_season"]
    # o ambiente de teste libera todos os talhões; aqui vale a regra real da safra
    monkeypatch.setattr(r, "_season_launch_plots", lambda plots, scope: list(scope["active_season"].plots) or plots)
    in_plot = env.db.query(Plot).filter_by(name="Talhão 07").one()
    season.plots.append(in_plot)
    out_plot = Plot(name="Setor 3", farm_id=env.scope["active_farm_id"], area_hectares=2, plant_count=1)
    env.db.add(out_plot)
    env.db.commit()
    monkeypatch.setattr(r, "_resolve_plot_in_scope", lambda request, repo, plot_id, path: (env.db.get(Plot, plot_id), env.scope, None))

    html = r.sale_new_page(env.request("/producao/vendas/nova"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "data-qh-open" in html and "Setor 3 · fora da safra" in html and "Salvar e usar na venda" in html

    def harvest(plot, include=""):
        return r.create_harvest_action(env.request("/producao"), csrf_token="x", plot_id=plot.id,
                                       harvest_date=season.start_date.isoformat(), measure_unit="sc_60", measured_quantity="12",
                                       situacao="tulha", incluir_safra=include, apos="venda", db=env.db, user=env.user,
                                       **{k: None for k in ("lot_code", "sacks_produced", "harvest_type", "coffee_stage",
                                                            "initial_destination", "responsible_name", "work_shift",
                                                            "maturation_percentage", "impurity_percentage",
                                                            "input_moisture_percentage", "volume_count", "notes", "yield_factor")})

    refused = harvest(out_plot)
    assert refused.headers["location"] == "/producao/vendas/nova"
    assert env.session.pop("flash")["message"] == "O setor selecionado não está vinculado à safra ativa."
    done = harvest(out_plot, "1")
    assert env.session.get("flash") is None, env.session.get("flash")
    lot = env.db.query(HarvestRecord).filter_by(plot_id=out_plot.id).one()
    assert done.headers["location"] == f"/producao/vendas/nova?lote={lot.id}"
    assert lot.lot_stage == "tulha" and float(lot.sacks_produced) == 12
    assert out_plot in season.plots
    assert env.session["sale_toast"]["text"].endswith(f"Lote {lot.lot_code} criado e selecionado")


def test_receipt_choice_defines_sale_situation_and_finance(env):
    from app.models.finance_transaction_installment import FinanceTransactionInstallment
    from app.services.coffee_sales import sale_records
    r = env.routes
    lot_a, lot_b = env.lots
    farm = env.scope["active_farm_id"]

    # Ainda não: nada no Financeiro, venda a receber; entrega pelo switch.
    asyncio.run(r.sale_create_action(env.request("/producao/vendas", _sale_form(
        env, [(lot_a, "10")], recebimento="nao", delivered="1", delivered_on="2026-11-21")), db=env.db, user=env.user))
    rec = sale_records(env.db, farm, "V-0001")[0]
    assert rec.finance_transaction_id is None and rec.received_on is None
    assert rec.delivered_on == date(2026, 11, 21) and rec.status == "entregue"

    # Já recebi: à vista pago na data, venda recebida.
    asyncio.run(r.sale_create_action(env.request("/producao/vendas", _sale_form(
        env, [(lot_a, "10")], recebimento="sim", received_on="2026-11-20", payment_method="PIX")), db=env.db, user=env.user))
    rec = sale_records(env.db, farm, "V-0002")[0]
    tx = env.db.get(FinanceTransaction, rec.finance_transaction_id)
    assert tx.payment_condition == "a_vista" and rec.received_on == date(2026, 11, 20) and rec.status == "recebido"

    # Parcelado trimestral em 4×, as 2 primeiras já recebidas.
    asyncio.run(r.sale_create_action(env.request("/producao/vendas", _sale_form(
        env, [(lot_b, "20")], recebimento="parc", installment_count="4", installment_frequency="trimestral",
        first_installment_date="2026-06-10", paid_count="2", payment_method="Boleto")), db=env.db, user=env.user))
    rec = sale_records(env.db, farm, "V-0003")[0]
    insts = sorted(env.db.query(FinanceTransactionInstallment).filter_by(finance_transaction_id=rec.finance_transaction_id),
                   key=lambda i: i.installment_number)
    assert [i.due_date for i in insts] == [date(2026, 6, 10), date(2026, 9, 10), date(2026, 12, 10), date(2027, 3, 10)]
    assert [i.status for i in insts] == ["pago", "pago", "pendente", "pendente"]
    assert rec.status == "negociado" and rec.received_on is None

    # Edição mostra a escolha gravada.
    html = r.sale_edit_page("V-0003", env.request("/producao/vendas/V-0003/editar"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert 'value="parc" checked' in html and 'name="paid_count" inputmode="numeric" value="2"' in html
    assert 'name="status"' not in html and "Café já entregue" in html

    # Na lista, "Ainda não" pede conta e forma; receber cria o lançamento à vista e desfazer o retira.
    page = r.commercialization_page(env.request("/producao/comercializacao"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert 'class="sv-rcv"' in page and 'name="conta"' in page
    req = env.request("/x", [("acao", "receber"), ("conta", str(env.account.id)), ("forma", "PIX"), ("data", "2026-11-22")])
    asyncio.run(r.sale_receipt_action("V-0001", req, db=env.db, user=env.user))
    rec = sale_records(env.db, farm, "V-0001")[0]
    tx = env.db.get(FinanceTransaction, rec.finance_transaction_id)
    assert tx.payment_method == "PIX" and float(tx.amount) == float(rec.net_value) and rec.status == "recebido"
    asyncio.run(r.sale_receipt_action("V-0001", env.request("/x", [("acao", "desfazer")]), db=env.db, user=env.user))
    rec = sale_records(env.db, farm, "V-0001")[0]
    assert rec.finance_transaction_id is None and env.db.get(FinanceTransaction, tx.id) is None
    assert env.session["sale_toast"]["undo"]["fields"]["conta"] == str(env.account.id)


def test_quick_account_and_card_from_sale_form(env):
    from app.models import FinanceAccount
    from app.models.finance_credit_card import FinanceCreditCard
    r = env.routes
    html = r.sale_new_page(env.request("/producao/vendas/nova"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert '+ Cadastrar outra conta…' in html and 'data-vr-open="cartao"' in html and 'Salvar e usar' in html
    res = asyncio.run(r.sale_quick_account_action(env.request("/x", [("account_name", "Sicredi"), ("bank_code", "748"),
                                                                     ("initial_balance", "1.234,56")]), db=env.db, user=env.user))
    data = json.loads(res.body)
    acc = env.db.get(FinanceAccount, data["id"])
    assert data["name"] == "Sicredi" and float(acc.initial_balance) == 1234.56
    bad = asyncio.run(r.sale_quick_card_action(env.request("/x", [("card_name", "Visa"), ("issuer", "Nenhum")]), db=env.db, user=env.user))
    assert bad.status_code == 422 and "emissor" in json.loads(bad.body)["error"]
    issuer = r.FINANCE_CREDIT_CARD_ISSUER_OPTIONS[0]
    res = asyncio.run(r.sale_quick_card_action(env.request("/x", [("card_name", "Visa Sicredi"), ("issuer", issuer), ("brand", "Visa"),
                                                                  ("closing_day", "5"), ("due_day", "12"), ("payment_account_id", str(acc.id))]),
                                               db=env.db, user=env.user))
    card = env.db.get(FinanceCreditCard, json.loads(res.body)["id"])
    assert card.card_name == "Visa Sicredi" and card.payment_account_id == acc.id and card.is_default


def test_sale_with_authorized_invoice_cannot_be_deleted(env, monkeypatch):
    from app.services.coffee_sales import sale_records
    r = env.routes
    asyncio.run(r.sale_create_action(env.request("/producao/vendas", _sale_form(env, [(env.lots[0], "10")])), db=env.db, user=env.user))
    note = {"id": "d" * 32, "status": "autorizada", "label": "Autorizada", "tone": "green", "number": "000127", "issues": 0}
    monkeypatch.setattr(r, "_nf_notes_by_sale", lambda *a: {"V-0001": note})
    page = r.commercialization_page(env.request("/producao/comercializacao"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Cancelar a nota</a> antes de excluir" in page
    r.sale_delete_action("V-0001", env.request("/x"), csrf_token="x", db=env.db, user=env.user)
    assert sale_records(env.db, env.scope["active_farm_id"], "V-0001")
    assert "Cancele a nota" in env.session.pop("flash")["message"]
    note["status"] = "cancelada"
    r.sale_delete_action("V-0001", env.request("/x"), csrf_token="x", db=env.db, user=env.user)
    assert not sale_records(env.db, env.scope["active_farm_id"], "V-0001")


def test_harvest_delete_removes_analyses_and_is_blocked_by_sales(env, monkeypatch):
    from app.models.coffee_classification import CoffeeClassification
    r = env.routes
    lot_a, lot_b = env.lots
    env.db.add(CoffeeClassification(harvest_id=lot_b.id, analysis_date=date(2026, 9, 1), source="laudo", beverage="Mole"))
    env.db.commit()
    monkeypatch.setattr(r, "_global_scope_context", lambda *a, **k: env.scope)
    monkeypatch.setattr(r, "_plot_matches_scope", lambda *a: True)
    asyncio.run(r.sale_create_action(env.request("/producao/vendas", _sale_form(env, [(lot_a, "10")])), db=env.db, user=env.user))
    r.delete_harvest_action(lot_a.id, env.request("/x"), csrf_token="x", db=env.db, user=env.user)
    assert env.db.get(HarvestRecord, lot_a.id) is not None
    env.session.pop("flash", None)
    r.delete_harvest_action(lot_b.id, env.request("/x"), csrf_token="x", db=env.db, user=env.user)
    assert env.db.get(HarvestRecord, lot_b.id) is None and env.db.query(CoffeeClassification).count() == 0
    assert env.session["flash"]["message"] == "Colheita do lote L-011 excluída."
