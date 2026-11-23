import asyncio
import json

from app.models.coffee_commercialization import CoffeeCommercializationRecord
from app.models.nfe_draft import (
    NfeClientFiscalConfig,
    NfeDraft,
    NfeDraftDetails,
    NfeDraftIdentification,
    NfeDraftTechnicalData,
    NfeIssuerProfile,
)
from app.services.coffee_sales import next_sale_code, sale_records
from app.services.nfe_notes import cancel_note, confirm_emission, inspect_sale_link, load_note
from tests.test_coffee_sales import _sale_form, env  # noqa: F401
from tests.test_nfe_xml_builder import _sample_inputs


def _seed_issuer_and_details(db, draft, user_id):
    from app.core.timezone import today_in_app_timezone
    _snap, issuer_obj, details_data, technical_data, _ident, _extra = _sample_inputs(cst="40")
    technical_data = {**technical_data, "emission_date": today_in_app_timezone().isoformat()}
    issuer = db.query(NfeIssuerProfile).filter_by(organization_id=draft.organization_id, farm_id=draft.farm_id).first()
    if not issuer:
        issuer = NfeIssuerProfile(
            id="iss-" + draft.id[:20],
            organization_id=draft.organization_id,
            farm_id=draft.farm_id,
            **vars(issuer_obj),
        )
        db.add(issuer)
    cfg = db.query(NfeClientFiscalConfig).filter_by(organization_id=draft.organization_id, farm_id=draft.farm_id).first()
    if not cfg:
        db.add(NfeClientFiscalConfig(
            id="cfg-" + draft.id[:20],
            organization_id=draft.organization_id,
            farm_id=draft.farm_id,
            person_type="pf",
            document=issuer_obj.cpf,
            state_registration="123456",
            crt="3",
            state="BA",
            environment="teste_ficticio",
            default_series=920,
            next_number=1,
            updated_by=user_id,
        ))
    details = db.query(NfeDraftDetails).filter_by(draft_id=draft.id).first()
    if details:
        details.data_json = json.dumps(details_data)
    else:
        db.add(NfeDraftDetails(
            draft_id=draft.id,
            organization_id=draft.organization_id,
            farm_id=draft.farm_id,
            data_json=json.dumps(details_data),
            updated_by=user_id,
        ))
    tech = db.query(NfeDraftTechnicalData).filter_by(draft_id=draft.id).first()
    if tech:
        tech.data_json = json.dumps(technical_data)
    else:
        db.add(NfeDraftTechnicalData(
            draft_id=draft.id,
            organization_id=draft.organization_id,
            farm_id=draft.farm_id,
            data_json=json.dumps(technical_data),
            updated_by=user_id,
        ))
    db.commit()
    return issuer


def test_cancel_sale_never_reuses_code_and_blocks_duplicating_cancelled_invoice(env):
    r = env.routes
    db = env.db
    farm_id = env.scope["active_farm_id"]
    lot_a, _ = env.lots

    # 1. Cadastra venda V-0001 (10 sc de 100 sc)
    asyncio.run(r.sale_create_action(env.request("/producao/vendas", _sale_form(env, [(lot_a, "10")])), db=db, user=env.user))
    assert sale_records(db, farm_id, "V-0001")

    # 2. Cria rascunho de NF-e para V-0001: em rascunho, Duplicar deve estar DESABILITADO
    draft_resp = r.create_fiscal_draft_action("V-0001", env.request("/producao/vendas/V-0001/rascunho-fiscal"), csrf_token="x", db=db, user=env.user)
    draft_id = draft_resp.headers["location"].rsplit("/", 1)[-1]
    draft = db.get(NfeDraft, draft_id)
    issuer = _seed_issuer_and_details(db, draft, env.user.id)
    draft_note = load_note(db, draft, issuer=issuer)
    assert draft_note["status"] == "rascunho" and draft_note["can_duplicate"] is False
    dup_draft_resp = r.fiscal_note_duplicate(draft.id, env.request(f"/notas-fiscais/{draft.id}/duplicar"), csrf_token="x", db=db, user=env.user)
    assert dup_draft_resp.status_code == 303 and dup_draft_resp.headers["location"] == f"/notas-fiscais/{draft.id}"
    assert "cancelada" in env.session.pop("flash")["message"].lower()
    assert db.query(NfeDraft).count() == 1

    confirm_emission(db, draft=draft, user_id=env.user.id)
    assert load_note(db, draft, issuer=issuer)["status"] == "autorizada"

    # Enquanto NF está autorizada, não permite duplicar nem cancelar a venda
    assert load_note(db, draft, issuer=issuer)["can_duplicate"] is False
    r.sale_delete_action("V-0001", env.request("/x"), csrf_token="x", db=db, user=env.user)
    assert sale_records(db, farm_id, "V-0001")[0].status != "cancelado"

    # Cancela a NF-e
    cancel_note(db, draft=draft, user_id=env.user.id, reason="Cancelamento para teste de integridade fiscal")
    note_after_nf_cancel = load_note(db, draft, issuer=issuer)
    assert note_after_nf_cancel["status"] == "cancelada"
    assert note_after_nf_cancel["sale_state"] == "ativa"
    assert note_after_nf_cancel["can_duplicate"] is True

    # 3. Cancela a venda V-0001 (mantém no histórico como Cancelada e devolve saldo)
    r.sale_delete_action("V-0001", env.request("/x"), csrf_token="x", db=db, user=env.user)
    recs_v1 = sale_records(db, farm_id, "V-0001")
    assert len(recs_v1) == 1 and recs_v1[0].status == "cancelado" and recs_v1[0].cancelled_at is not None

    # Venda cancelada não pode ser editada nem emitir nova NF
    edit_resp = r.sale_edit_page("V-0001", env.request("/producao/vendas/V-0001/editar"), db=db, user=env.user, csrf_token="x")
    assert edit_resp.status_code == 303 and "cancelada" in env.session.pop("flash")["message"].lower()

    # 4. Cadastra nova venda → código DEVE ser V-0002 (nunca V-0001!) e saldo total do lote está disponível
    full_sacks = str(int(lot_a.sacks_produced))
    resp_v2 = asyncio.run(r.sale_create_action(env.request("/producao/vendas", _sale_form(env, [(lot_a, full_sacks)])), db=db, user=env.user))
    assert resp_v2.status_code == 303
    assert sale_records(db, farm_id, "V-0002")
    assert len(sale_records(db, farm_id, "V-0001")) == 1  # continua sendo apenas a antiga cancelada

    # 5. NF cancelada da V-0001 sabe que V-0001 foi cancelada e BLOQUEIA duplicação sem erro 404
    note_after_sale_cancel = load_note(db, draft, issuer=issuer)
    assert note_after_sale_cancel["sale_state"] == "cancelada"
    assert note_after_sale_cancel["can_duplicate"] is False
    assert note_after_sale_cancel["duplicate_reason"] == "venda cancelada"

    dup_resp = r.fiscal_note_duplicate(draft.id, env.request(f"/notas-fiscais/{draft.id}/duplicar"), csrf_token="x", db=db, user=env.user)
    assert dup_resp.status_code == 303 and dup_resp.headers["location"] == f"/notas-fiscais/{draft.id}"
    assert "cancelada" in env.session.pop("flash")["message"].lower()
    assert db.query(NfeDraft).count() == 1  # nenhum rascunho extra criado

    # 6. Página de Comercialização mantém V-0001 no histórico como Cancelada e KPIs contam apenas V-0002
    html = r.commercialization_page(env.request("/producao/comercializacao"), db=db, user=env.user, csrf_token="x").body.decode()
    assert "V-0001" in html and "V-0002" in html and "Cancelada" in html


def test_duplicate_blocked_on_legacy_hard_deleted_sale_and_allowed_on_active_sale(env):
    r = env.routes
    db = env.db
    farm_id = env.scope["active_farm_id"]
    lot_a, _ = env.lots

    # Cria V-0001, emite e cancela NF-e
    asyncio.run(r.sale_create_action(env.request("/producao/vendas", _sale_form(env, [(lot_a, "15")])), db=db, user=env.user))
    draft_resp = r.create_fiscal_draft_action("V-0001", env.request("/producao/vendas/V-0001/rascunho-fiscal"), csrf_token="x", db=db, user=env.user)
    draft_v1 = db.get(NfeDraft, draft_resp.headers["location"].rsplit("/", 1)[-1])
    issuer = _seed_issuer_and_details(db, draft_v1, env.user.id)
    confirm_emission(db, draft=draft_v1, user_id=env.user.id)
    cancel_note(db, draft=draft_v1, user_id=env.user.id, reason="Nota cancelada antes de simular legado")

    # Simula dado legado: venda V-0001 excluída fisicamente no banco antes desta correção
    db.query(CoffeeCommercializationRecord).filter_by(farm_id=farm_id, sale_code="V-0001").delete()
    db.commit()

    # next_sale_code ainda respeita o histórico de nfe_drafts e gera V-0002
    assert next_sale_code(db, farm_id) == "V-0002"

    # NF órfã marca venda como excluída e bloqueia duplicação amigavelmente (sem 404!)
    assert inspect_sale_link(db, draft_v1)["state"] == "excluida"
    dup_resp = r.fiscal_note_duplicate(draft_v1.id, env.request(f"/notas-fiscais/{draft_v1.id}/duplicar"), csrf_token="x", db=db, user=env.user)
    assert dup_resp.status_code == 303 and dup_resp.headers["location"] == f"/notas-fiscais/{draft_v1.id}"
    assert "não existe mais" in env.session.pop("flash")["message"].lower()

    # Agora testa o caminho feliz: venda ativa + NF cancelada → Duplicar funciona e copia preparação sem copiar número!
    asyncio.run(r.sale_create_action(env.request("/producao/vendas", _sale_form(env, [(lot_a, "20")])), db=db, user=env.user))
    active_code = sale_records(db, farm_id, "V-0003")[0].sale_code
    d2_resp = r.create_fiscal_draft_action(active_code, env.request(f"/producao/vendas/{active_code}/rascunho-fiscal"), csrf_token="x", db=db, user=env.user)
    draft_v3 = db.get(NfeDraft, d2_resp.headers["location"].rsplit("/", 1)[-1])
    _seed_issuer_and_details(db, draft_v3, env.user.id)
    confirm_emission(db, draft=draft_v3, user_id=env.user.id)
    cancel_note(db, draft=draft_v3, user_id=env.user.id, reason="Erro de CFOP, vou duplicar para reemitir")

    ok_dup = r.fiscal_note_duplicate(draft_v3.id, env.request(f"/notas-fiscais/{draft_v3.id}/duplicar"), csrf_token="x", db=db, user=env.user)
    assert ok_dup.status_code == 303 and ok_dup.headers["location"].startswith("/producao/rascunhos-fiscais/")
    new_draft_id = ok_dup.headers["location"].rsplit("/", 1)[-1]
    new_draft = db.get(NfeDraft, new_draft_id)
    assert new_draft.sale_code == active_code
    # Novo rascunho não herda número fiscal nem status cancelado, mas herda os dados fiscais preenchidos
    assert db.query(NfeDraftIdentification).filter_by(draft_id=new_draft.id).first() is None
    assert load_note(db, new_draft, issuer=issuer)["status"] == "rascunho"
    new_details = json.loads(db.query(NfeDraftDetails).filter_by(draft_id=new_draft.id).one().data_json)
    assert new_details.get("cfop") or new_details.get("recipient_name")
    # Após duplicar 1 vez, tentar duplicar novamente a mesma NF cancelada é bloqueado (já há rascunho aberto)
    assert load_note(db, draft_v3, issuer=issuer)["can_duplicate"] is False
    second_dup = r.fiscal_note_duplicate(draft_v3.id, env.request(f"/notas-fiscais/{draft_v3.id}/duplicar"), csrf_token="x", db=db, user=env.user)
    assert second_dup.status_code == 303 and second_dup.headers["location"] == f"/notas-fiscais/{draft_v3.id}"
    assert "rascunho em aberto" in env.session.pop("flash")["message"].lower()
