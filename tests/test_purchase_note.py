import json

import pytest

from app.models.farm import Farm
from app.models.finance_account import FinanceAccount
from app.models.finance_transaction import FinanceTransaction
from app.models.purchased_input import PurchasedInput
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
