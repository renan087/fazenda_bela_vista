import json

import pytest

from app.models.farm import Farm
from app.models.finance_account import FinanceAccount
from app.models.finance_transaction import FinanceTransaction
from app.models.purchased_input import PurchasedInput
from app.models.stock_output import StockOutput
from app.services.forms import update_purchased_input
from tests.test_inputs_hub import env, hub  # noqa: F401


@pytest.fixture
def wiz(hub, monkeypatch):
    env, x = hub
    farm = env.db.get(Farm, env.scope["active_farm_id"])
    monkeypatch.setattr(env.routes, "_launch_scope_or_redirect", lambda *a, **k: ({**env.scope, "active_farm": farm}, None))
    account = env.db.query(FinanceAccount).filter(FinanceAccount.farm_id == farm.id).first()
    return env, x, account


def _payload(account, *, parcels=None, mode="prazo"):
    return {
        "note": {"supplier": "Agro Cerrado Insumos", "invoice": "18.322", "date": "2026-11-20"},
        "payment": {"mode": mode, "account_id": str(account.id), "method": "Boleto", "count": "3", "first_due": "2026-12-20",
                    "installments": parcels or [{"due_date": "2026-12-20", "amount": "5.000,00"}, {"due_date": "2027-01-20", "amount": "4.790,00"},
                                                {"due_date": "2027-02-20", "amount": "4.790,00"}]},
        "items": [
            {"name": "Cloreto de potássio", "item_type": "insumo_agricola", "category": "Fertilizantes", "unit": "sc", "packs": "40", "size": "1", "price": "190"},
            {"name": "Fungicida Novo XR", "item_type": "insumo_agricola", "category": "Fungicida", "unit": "L", "packs": "6", "size": "5", "price": "1.100,00",
             "batch": "2412-F", "manufacture_date": "2025-11-01", "expiration_date": "2027-11-01", "minimum": "10"},
            {"name": "Bota de borracha", "item_type": "suprimento", "category": "EPIs", "unit": "par", "packs": "20", "size": "1", "price": "19", "batch": "X"},
        ],
    }


def _post(env, payload):
    req = env.request("/insumos/comprados/nota", form=[("payload", json.dumps(payload))])
    import asyncio
    return asyncio.run(env.routes.register_purchase_note_action(req, db=env.db, user=env.user))


def test_note_with_three_items_shares_one_finance_entry_with_edited_parcels(wiz):
    env, x, account = wiz
    resp = _post(env, _payload(account))
    assert resp.status_code == 303 and resp.headers["location"] == "/insumos/comprados/nova?registrada=1", env.session.get("flash")
    entries = env.db.query(PurchasedInput).filter(PurchasedInput.notes.like("Nota fiscal 18.322%")).all()
    assert len(entries) == 3
    tx = env.db.get(FinanceTransaction, entries[0].finance_transaction_id)
    assert {e.finance_transaction_id for e in entries} == {tx.id}
    assert float(tx.amount) == 14580.0 and "3 itens" in tx.product_service
    assert [float(i.amount) for i in sorted(tx.installments, key=lambda i: i.installment_number)] == [5000.0, 4790.0, 4790.0]
    by_name = {e.name: e for e in entries}
    assert by_name["Fungicida Novo XR"].batch_number == "2412-F" and float(by_name["Fungicida Novo XR"].total_quantity) == 30
    assert by_name["Bota de borracha"].batch_number is None  # suprimento não guarda lote
    assert by_name["Bota de borracha"].input_catalog.item_type == "suprimento"
    assert env.session["purchase_note_done"]["count"] == 3 and env.session["purchase_note_done"]["installments"] == 3


def test_note_rejects_parcels_that_do_not_add_up_and_saves_nothing(wiz):
    env, x, account = wiz
    bad = [{"due_date": "2026-12-20", "amount": "100"}, {"due_date": "2027-01-20", "amount": "100"}, {"due_date": "2027-02-20", "amount": "100"}]
    before = env.db.query(PurchasedInput).count()
    _post(env, _payload(account, parcels=bad))
    assert env.session["flash"]["kind"] == "error" and "soma das parcelas" in env.session["flash"]["message"]
    assert env.db.query(PurchasedInput).count() == before


def test_new_item_without_type_is_rejected(wiz):
    env, x, account = wiz
    payload = _payload(account, mode="vista")
    payload["items"] = [{"name": "Coisa nova", "item_type": "", "category": "", "unit": "kg", "packs": "1", "size": "1", "price": "1"}]
    _post(env, payload)
    assert env.session["flash"]["kind"] == "error"


def test_editing_or_deleting_one_item_keeps_shared_entry_consistent(wiz):
    env, x, account = wiz
    _post(env, _payload(account))
    entries = env.db.query(PurchasedInput).filter(PurchasedInput.notes.like("Nota fiscal 18.322%")).all()
    by_name = {e.name: e for e in entries}
    tx_id = entries[0].finance_transaction_id
    repo = env.routes._repository(env.db)
    bota = by_name["Bota de borracha"]
    update_purchased_input(repo, bota, {
        "quantity_purchased": 10, "package_size": 1, "unit_price": 19, "name": bota.name, "package_unit": "par", "item_type": "suprimento",
        "category": "EPIs", "farm_id": bota.farm_id, "finance_account_id": None, "payment_condition": "a_vista",
    })
    tx = env.db.get(FinanceTransaction, tx_id)
    assert tx is not None and float(tx.amount) == 14390.0
    assert round(sum(float(i.amount) for i in tx.installments), 2) == 14390.0

    env.routes.delete_purchased_input_action(by_name["Cloreto de potássio"].id, env.request("/insumos/comprados"), csrf_token="x", db=env.db, user=env.user)
    env.db.expire_all()
    tx = env.db.get(FinanceTransaction, tx_id)
    assert tx is not None and float(tx.amount) == 6790.0
    assert env.db.get(PurchasedInput, by_name["Fungicida Novo XR"].id) is not None


def test_wizard_page_lists_catalog_suppliers_and_done_screen(wiz):
    env, x, account = wiz
    _post(env, _payload(account))
    resp = env.routes.purchase_wizard_page(env.request("/insumos/comprados/nova?registrada=1"), db=env.db, user=env.user, csrf_token="x")
    html = resp.body.decode()
    assert "Compra registrada" in html and "3 itens entraram" in html and "As 3 parcelas" in html
    resp = env.routes.purchase_wizard_page(env.request("/insumos/comprados/nova"), db=env.db, user=env.user, csrf_token="x")
    html = resp.body.decode()
    data = json.loads(html.split("data-cw-data>")[1].split("</script>")[0])
    assert "Agro Cerrado Insumos" in data["suppliers"]
    assert any(n["invoice"] == "18.322" for n in data["notes"])
    assert any(c["name"] == "Cloreto de potássio" and c["available"] == 48 for c in data["catalog"])
    assert {t["key"] for t in data["types"]} == {"def", "fert", "comb", "sup", "outros"}
    assert 'data-cw-step="4"' in html


def _note(env):
    return env.db.query(PurchasedInput).filter(PurchasedInput.notes.like("Nota fiscal 18.322%")).order_by(PurchasedInput.id).all()


def test_movement_detail_shows_note_items_parcels_and_delete_modal(wiz):
    env, x, account = wiz
    _post(env, _payload(account))
    first = _note(env)[0]
    html = env.routes.stock_page(env.request("/insumos/estoque"), aba="movimentacoes", periodo="safra", tipo="compra",
                                 db=env.db, user=env.user, csrf_token="x").body.decode()
    assert f'data-io-mov="c-{first.id}"' in html and "Parcelas no Financeiro" in html and "A pagar" in html
    assert f"/insumos/comprados/nova?editar={first.id}" in html and "Excluir esta compra?" in html
    assert f'data-io-upload-open="{first.id}"' in html and "Relatório de compras em PDF" in html


def test_delete_note_blocked_when_used_and_allowed_otherwise(wiz):
    env, x, account = wiz
    _post(env, _payload(account))
    entries = _note(env)
    repo = env.routes._repository(env.db)
    from app.services import inputs_hub as ih
    ih.register_uses(repo, [{"input_id": entries[1].input_id, "quantity": "5"}], farm_id=env.scope["active_farm_id"],
                     season_id=env.scope["active_season_id"], plots={})
    env.routes.delete_purchase_note_action(entries[0].id, env.request("/x"), csrf_token="x", db=env.db, user=env.user)
    assert env.session["flash"]["kind"] == "error" and len(_note(env)) == 3
    html = env.routes.stock_page(env.request("/insumos/estoque"), aba="movimentacoes", periodo="safra", tipo="compra",
                                 db=env.db, user=env.user, csrf_token="x").body.decode()
    assert "Esta compra ainda não pode ser excluída" in html and "5 L de Fungicida Novo XR" in html
    # desfaz o uso e aí pode excluir: itens e lançamento somem
    out = env.db.query(StockOutput).filter(StockOutput.reference_type == "manual_stock_output").one()
    ih.undo_movement(repo, env.db, out, env.scope["active_farm_id"])
    tx_id = _note(env)[0].finance_transaction_id
    env.routes.delete_purchase_note_action(_note(env)[0].id, env.request("/x"), csrf_token="x", db=env.db, user=env.user)
    assert env.session["flash"]["kind"] == "success" and _note(env) == []
    assert env.db.get(FinanceTransaction, tx_id) is None


def test_correct_note_keeps_used_amounts_and_rebuilds_parcels(wiz):
    env, x, account = wiz
    _post(env, _payload(account))
    entries = _note(env)
    repo = env.routes._repository(env.db)
    from app.services import inputs_hub as ih
    ih.register_uses(repo, [{"input_id": entries[0].input_id, "quantity": "30"}], farm_id=env.scope["active_farm_id"],
                     season_id=env.scope["active_season_id"], plots={})
    state = env.routes._purchase_edit_state(repo, entries[0])
    # 8 sacos saíram de uma compra mais antiga (usa primeiro o que chegou antes) e 22 desta nota
    assert [i["used"] for i in state["state"]["items"]] == [22.0, 0, 0]
    payload = _payload(account)
    payload["edit_of"] = entries[0].id
    payload["payment"]["installments"] = None
    for item, e in zip(payload["items"], entries):
        item["entry_id"] = e.id
    payload["items"][0]["packs"] = "20"  # menos do que já saiu (22): não pode
    _post(env, payload)
    assert "não pode ficar menor" in env.session["flash"]["message"]
    payload["items"][0]["packs"] = "35"
    payload["items"].pop()  # tira a bota (nunca usada)
    _post(env, payload)
    assert env.session["flash"]["message"].startswith("Compra corrigida")
    env.db.expire_all()
    entries = _note(env)
    assert len(entries) == 2 and float(entries[0].available_quantity) == 13.0
    tx = env.db.get(FinanceTransaction, entries[0].finance_transaction_id)
    assert float(tx.amount) == 35 * 190 + 6600 and len(tx.installments) == 3
    assert round(sum(float(i.amount) for i in tx.installments), 2) == float(tx.amount)


def test_correct_note_with_paid_parcel_cannot_change_total(wiz):
    env, x, account = wiz
    _post(env, _payload(account))
    entries = _note(env)
    tx = env.db.get(FinanceTransaction, entries[0].finance_transaction_id)
    tx.installments[0].status = "pago"
    env.db.commit()
    payload = _payload(account)
    payload["edit_of"] = entries[0].id
    for item, e in zip(payload["items"], entries):
        item["entry_id"] = e.id
    payload["items"][2]["packs"] = "30"
    _post(env, payload)
    assert "parcela paga" in env.session["flash"]["message"]


def test_attachments_upload_dedupe_and_remove(wiz):
    import asyncio as aio
    from io import BytesIO
    from starlette.datastructures import UploadFile, Headers
    from app.models.purchased_input_attachment import PurchasedInputAttachment
    env, x, account = wiz
    _post(env, _payload(account))
    first = _note(env)[0]
    upload = UploadFile(file=BytesIO(b"%PDF-1.4 nota"), filename="nota-18322.pdf", headers=Headers({"content-type": "application/pdf"}))
    req = env.request("/x", form=[("attachments", upload)])
    aio.run(env.routes.upload_purchase_note_attachments_action(first.id, req, db=env.db, user=env.user))
    assert env.session["flash"]["message"] == "1 anexo salvo na compra."
    att = env.db.query(PurchasedInputAttachment).one()
    env.routes.delete_purchase_note_attachment_action(first.id, att.id, env.request("/x"), csrf_token="x", db=env.db, user=env.user)
    assert env.db.query(PurchasedInputAttachment).count() == 0


def test_old_pages_redirect_to_stock_and_purchase_export_still_works(wiz):
    env, x, account = wiz
    for fn in (env.routes.purchased_inputs_page, env.routes.supplies_page):
        assert fn(env.request("/insumos/comprados")).headers["location"] == "/insumos/estoque?aba=movimentacoes&tipo=compra&periodo=safra"
    from pathlib import Path
    block = Path("app/templates/partials/sidebar.html").read_text()
    assert "'label': 'Compras'" not in block


def test_purchase_report_export_still_works(wiz):
    env, x, account = wiz
    _post(env, _payload(account))
    resp = env.routes.export_purchased_inputs_xlsx(env.request("/insumos/comprados/exportar.xlsx"), farm_id=None, item_type=None, input_id=None,
                                                   start_date=None, end_date=None, db=env.db, user=env.user)
    assert resp.status_code == 200 and "spreadsheet" in resp.media_type


def test_item_step_has_search_above_list_and_missing_summary(wiz):
    env, x, account = wiz
    html = env.routes.purchase_wizard_page(env.request("/insumos/comprados/nova"), db=env.db, user=env.user, csrf_token="x").body.decode()
    step2 = html[html.index('data-cw-step="2"'):html.index('data-cw-step="3"')]
    assert step2.index("data-cw-search") < step2.index("data-cw-items")
    assert "data-cw-todo" in step2 and "data-cw-count" in step2


def test_purchase_wizard_quick_account_and_card_creation(hub):
    import asyncio as aio
    from app.models.finance_account import FinanceAccount
    from app.models.finance_credit_card import FinanceCreditCard
    env, x = hub
    # Sem contas nem cartões: modo cartão liberado, botões "+ Cadastrar conta/cartão" e gavetas rápidas
    env.db.query(FinanceCreditCard).delete()
    env.db.query(FinanceAccount).delete()
    env.db.commit()
    html = env.routes.purchase_wizard_page(env.request("/insumos/comprados/nova"), db=env.db, user=env.user, csrf_token="x").body.decode()
    assert 'data-cw-mode="cartao"' in html and 'data-cw-mode="cartao" disabled' not in html
    assert 'data-vr-open="conta"' in html and 'data-vr-open="cartao"' in html
    assert '/insumos/comprados/conta-rapida' in html and '/insumos/comprados/cartao-rapido' in html
    assert '+ Cadastrar outra conta…' in html and '+ Cadastrar outro cartão…' in html

    # Cadastra conta rápida via endpoint JSON
    acc_req = env.request("/insumos/comprados/conta-rapida", form=[
        ("csrf_token", "x"), ("nome", "Sicoob Fazenda"), ("bank_code", "756"), ("saldo", "0,00"), ("saldo_em", "2026-11-22"),
    ])
    acc_resp = aio.run(env.routes.purchase_quick_account_action(acc_req, db=env.db, user=env.user))
    assert acc_resp.status_code == 200
    acc_data = json.loads(acc_resp.body.decode())
    assert acc_data["id"] and acc_data["name"] == "Sicoob Fazenda"

    # Cadastra cartão rápido via endpoint JSON (com closing_day e due_day)
    card_req = env.request("/insumos/comprados/cartao-rapido", form=[
        ("csrf_token", "x"), ("nome_cartao", "Visa Ourocard"), ("issuer", "Banco do Brasil / Ourocard"), ("brand", "Visa"),
        ("closing_day", "5"), ("due_day", "12"), ("payment_account_id", str(acc_data["id"])),
    ])
    card_resp = aio.run(env.routes.purchase_quick_card_action(card_req, db=env.db, user=env.user))
    assert card_resp.status_code == 200
    card_data = json.loads(card_resp.body.decode())
    assert card_data["id"] and "Visa Ourocard" in card_data["name"]
    assert card_data["closing_day"] == 5 and card_data["due_day"] == 12
