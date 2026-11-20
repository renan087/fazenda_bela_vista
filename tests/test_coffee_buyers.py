import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from starlette.datastructures import FormData

from app.models.coffee_buyer import CoffeeBuyer
from app.services import coffee_buyers as svc
from tests.test_stock_assistant import _route_env

ROOT = Path(__file__).resolve().parents[1]


def test_document_validation_and_format():
    assert svc.valid_cnpj("11.222.333/0001-81")
    assert not svc.valid_cnpj("11.222.333/0001-82")
    assert not svc.valid_cnpj("11111111111111")
    assert svc.valid_cpf("529.982.247-25")
    assert not svc.valid_cpf("529.982.247-26")
    assert svc.format_document("11222333000181") == "11.222.333/0001-81"
    assert svc.format_document("52998224725") == "529.982.247-25"


def test_clean_payload_rules_for_pj_and_pf():
    values, errors = svc.clean_payload({"person_type": "pj", "name": " Exportadora ", "document": "11.222.333/0001-81",
                                        "ie_exempt": "1", "state_registration": "123", "postal_code": "37010-000", "state": "mg"})
    assert not errors
    assert values["document"] == "11222333000181" and values["postal_code"] == "37010000"
    assert values["state"] == "MG" and values["state_registration"] is None and values["name"] == "Exportadora"

    values, errors = svc.clean_payload({"person_type": "pf", "name": "", "document": "123", "postal_code": "1", "state": "XX",
                                        "email": "x"})
    assert len(errors) == 5
    assert values["trade_name"] is None and values["state_registration"] is None


def test_missing_for_invoice_lists_only_what_nf_needs():
    pj = CoffeeBuyer(person_type="pj", name="A", document="11222333000181", ie_exempt=False)
    assert svc.missing_for_invoice(pj) == ["inscrição estadual", "endereço completo"]
    pj.ie_exempt = True
    pj.postal_code, pj.street, pj.number, pj.district, pj.city, pj.state = "37010000", "Rua", "1", "Centro", "Varginha", "MG"
    assert svc.missing_for_invoice(pj) == []
    pf = CoffeeBuyer(person_type="pf", name="José", rural_producer=True)
    assert svc.missing_for_invoice(pf) == ["CPF", "IE de produtor rural", "endereço completo"]


def test_lookup_cnpj_maps_public_data(monkeypatch):
    payload = {"razao_social": "CAFE BOM LTDA", "nome_fantasia": "BOM", "cep": "37130000", "descricao_tipo_de_logradouro": "RUA",
               "logradouro": "DR PAULO", "numero": "410", "bairro": "CENTRO", "municipio": "ALFENAS", "uf": "mg",
               "descricao_situacao_cadastral": "ATIVA"}
    monkeypatch.setattr(svc, "_get_json", lambda url: payload if "/cnpj/" in url else {"street": "Rua X", "city": "Alfenas", "state": "mg"})
    data = svc.lookup_cnpj("11222333000181")
    assert data["name"] == "CAFE BOM LTDA" and data["street"] == "Rua Dr Paulo" and data["postal_code"] == "37130-000"
    assert data["city"] == "Alfenas" and data["state"] == "MG" and data["active"] is True
    assert svc.lookup_cep("37130000")["state"] == "MG"


def _call(handler, form, **kwargs):
    request = SimpleNamespace(session={"csrf_token": "x"})

    async def get_form():
        return FormData({"csrf_token": "x", **form})

    request.form = get_form
    return asyncio.run(handler(request=request, **kwargs))


def test_buyer_routes_create_dedupe_update_and_list(monkeypatch):
    routes, db, user, *_ = _route_env(monkeypatch, [])
    monkeypatch.setattr(routes, "validate_csrf", lambda request, token: None)
    response = _call(routes.coffee_buyer_create, {"person_type": "pj", "name": "Exportadora Sul", "document": "11.222.333/0001-81"},
                     db=db, user=user)
    assert response.status_code == 201
    created = json.loads(response.body)["buyer"]
    assert created["document"] == "11.222.333/0001-81" and created["invoice_ready"] is False

    dup = _call(routes.coffee_buyer_create, {"person_type": "pj", "name": "Outro nome", "document": "11222333000181"}, db=db, user=user)
    assert dup.status_code == 409 and json.loads(dup.body)["duplicate"]["id"] == created["id"]

    bad = _call(routes.coffee_buyer_create, {"person_type": "pf", "name": "Fulano", "document": "11111111111"}, db=db, user=user)
    assert bad.status_code == 422

    updated = _call(routes.coffee_buyer_update, {"person_type": "pj", "name": "Exportadora Sul Ltda", "document": "11222333000181",
                                                 "ie_exempt": "1", "postal_code": "37010000", "street": "Av. Café", "number": "1",
                                                 "district": "Centro", "city": "Varginha", "state": "MG"},
                    buyer_id=created["id"], db=db, user=user)
    assert json.loads(updated.body)["buyer"]["invoice_ready"] is True

    listing = json.loads(routes.coffee_buyers_list(SimpleNamespace(), q="sul", db=db, user=user).body)["items"]
    assert [item["name"] for item in listing] == ["Exportadora Sul Ltda"]
    by_doc = json.loads(routes.coffee_buyers_list(SimpleNamespace(), q="222.333", db=db, user=user).body)["items"]
    assert len(by_doc) == 1


def test_resolve_buyer_prefers_registered_buyer(monkeypatch):
    routes, db, user, *_ = _route_env(monkeypatch, [])
    buyer = CoffeeBuyer(organization_id=user.organization_id, person_type="pj", name="Cadastrado")
    other = CoffeeBuyer(organization_id=(user.organization_id or 0) + 99, person_type="pj", name="De outra empresa")
    db.add_all([buyer, other])
    db.commit()
    assert routes._resolve_coffee_buyer(db, user, str(buyer.id), "digitado") == (buyer.id, "Cadastrado")
    assert routes._resolve_coffee_buyer(db, user, str(other.id), "digitado") == (None, "digitado")
    assert routes._resolve_coffee_buyer(db, user, None, "  livre ") == (None, "livre")


def test_sale_form_uses_buyer_picker_and_modal():
    template = (ROOT / "app/templates/sale_form.html").read_text()
    partial = (ROOT / "app/templates/partials/buyer_picker.html").read_text()
    assert "buyer_field(sale_buyer" in template and "buyer_modal(csrf_token)" in template
    assert "js/buyer_picker.js" in template
    for text in ["Vou emitir nota fiscal", "Buscar dados", "Isento / não contribuinte de ICMS", "É produtor rural com inscrição estadual",
                 "E-mail para receber a nota", 'aria-label="Fechar"']:
        assert text in partial
