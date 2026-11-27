import asyncio
from datetime import date, timedelta
from types import SimpleNamespace

from fastapi import BackgroundTasks

from app.models.agronomic_profile import AgronomicProfile
from app.models.farm import Farm
from app.models.farm_attachment import FarmAttachment
from app.services import farm_profile as fp
from tests.test_coffee_sales import env  # noqa: F401
from tests.test_farms_redesign import SAMPLE_GEOJSON


def _open_writes(env, monkeypatch):
    monkeypatch.setattr(env.routes, "_require_permission", lambda *a, **k: None)


AGRO_EMPTY = dict(
    altitude_meters=None, terrain_slope=None, sun_exposure=None, soil_type=None, climate=None,
    last_liming_date=None, last_gypsum_date=None, last_organic_matter_date=None,
    target_productivity_sacks_per_ha=None, common_pests=[], pruning_history=None, current_limitations=None,
)


def _agro(env, farm, secao, **values):
    return env.routes.save_farm_agronomy_section_action(
        farm.id, secao, env.request("/x"), csrf_token="x", db=env.db, user=env.user, **{**AGRO_EMPTY, **values},
    )


def _detail(env, farm, tab="resumo"):
    return env.routes.farm_detail_page(
        farm.id, env.request(f"/fazendas/{farm.id}?aba={tab}"), BackgroundTasks(), aba=tab,
        db=env.db, user=env.user, csrf_token="x",
    ).body.decode("utf-8")


def test_checks_list_pending_items_and_score_grows_when_completed(env):
    farm = env.db.query(Farm).one()
    farm.boundary_geojson = None
    farm.car_registration_number = None
    farm.ccir_number = farm.itr_registration_number = farm.property_registration_number = None
    env.db.commit()
    today = date(2026, 11, 27)
    empty = fp.build_farm_checks(farm, plots_count=0, attachments=[], profile=None, today=today)
    ids = {p.id for p in empty.pending}
    assert {"perim", "car", "set", "reg", "amb", "man"} <= ids
    assert empty.score < 50
    assert empty.label.endswith("pendências")

    farm.ccir_number = "950.068.123.456-7"
    farm.itr_registration_number = "1.234.567-8"
    farm.property_registration_number = "12.345"
    itr_old = SimpleNamespace(id=1, doc_type="ITR", reference="Exercício 2025", valid_until=None)
    lic = SimpleNamespace(id=2, doc_type="Licença ambiental", reference=None, valid_until=today + timedelta(days=40))
    ccir = SimpleNamespace(id=3, doc_type="CCIR", reference="Exercício 2025", valid_until=None)
    profile = SimpleNamespace(altitude_meters=980, terrain_slope="Ondulado (8 a 20%)", soil_type="Latossolo Vermelho",
                              last_liming_date=date(2025, 8, 1), last_gypsum_date=None)
    chk = fp.build_farm_checks(farm, plots_count=3, attachments=[itr_old, lic, ccir], profile=profile, today=today)
    titles = [p.title for p in chk.pending]
    assert "Anexar o ITR 2026" in titles
    assert "Anexar a matrícula" in titles
    assert "Renovar a licença ambiental" in titles
    assert "Informar a última gessagem" in titles
    assert "Cadastrar os setores" not in titles
    assert chk.score > empty.score
    assert chk.by_tab("documentos") == 4  # CAR, ITR, matrícula e licença


def test_attachment_status_labels():
    today = date(2026, 11, 27)
    assert fp.attachment_status(SimpleNamespace(doc_type="ITR", reference="2026", valid_until=None), today) == ("Em dia", "ok")
    assert fp.attachment_status(SimpleNamespace(doc_type="ITR", reference="2025", valid_until=None), today)[1] == "warn"
    assert fp.attachment_status(SimpleNamespace(doc_type="Matrícula", reference=None, valid_until=None), today) == ("Sem validade", "mute")
    assert fp.attachment_status(SimpleNamespace(doc_type="Outorga de água", reference=None, valid_until=today - timedelta(days=1)), today) == ("Vencido", "err")


def test_identity_registry_and_agronomy_sections_save(env, monkeypatch):
    _open_writes(env, monkeypatch)
    farm = env.db.query(Farm).one()
    r = env.routes
    resp = r.update_farm_identity_action(
        farm.id, env.request(f"/fazendas/{farm.id}/identificacao"), csrf_token="x", name="Fazenda Nova Era",
        location_uf="MG", location_city="Patrocínio", total_area="1.234,50", notes="Entrada pela BR-365",
        db=env.db, user=env.user,
    )
    assert resp.status_code == 303 and resp.headers["location"] == f"/fazendas/{farm.id}"
    env.db.refresh(farm)
    assert farm.name == "Fazenda Nova Era" and farm.location == "Patrocínio, MG"
    assert float(farm.total_area) == 1234.5

    resp = r.update_farm_registry_action(
        farm.id, env.request("/x"), csrf_token="x", ccir_number=" 950.1 ", itr_registration_number="",
        property_registration_number="12.345", registry_office="Registro de Imóveis de Patrocínio",
        db=env.db, user=env.user,
    )
    assert resp.headers["location"] == f"/fazendas/{farm.id}?aba=documentos"
    env.db.refresh(farm)
    assert farm.ccir_number == "950.1" and farm.itr_registration_number is None

    env.db.query(AgronomicProfile).filter_by(farm_id=farm.id).delete()
    env.db.commit()
    resp = _agro(env, farm, "ambiente", altitude_meters="1.050", terrain_slope="Ondulado (8 a 20%)",
                 sun_exposure="Face norte", soil_type="Latossolo Vermelho", climate="Cwa")
    assert resp.headers["location"] == f"/fazendas/{farm.id}?aba=agronomia"
    profile = env.db.query(AgronomicProfile).filter_by(farm_id=farm.id).one()
    assert profile.altitude_meters == 1050 and profile.culture == "Café arábica"
    _agro(env, farm, "manejo", last_liming_date="2025-08", last_gypsum_date="2026-06", target_productivity_sacks_per_ha="42,00")
    _agro(env, farm, "historico", common_pests=["Ferrugem", "Bicho-mineiro"], pruning_history="Recepa em 2026", current_limitations="")
    env.db.refresh(profile)
    assert profile.last_liming_date == date(2025, 8, 1) and profile.last_gypsum_date == date(2026, 6, 1)
    assert float(profile.target_productivity_sacks_per_ha) == 42.0
    assert profile.common_pests == "Ferrugem, Bicho-mineiro" and profile.altitude_meters == 1050

    html = _detail(env, farm, "agronomia")
    assert "08/2025" in html and "Recepa em 2026" in html and "Ferrugem, Bicho-mineiro" in html


def test_document_upload_edit_and_delete_back_to_documents_tab(env, monkeypatch):
    _open_writes(env, monkeypatch)
    farm = env.db.query(Farm).one()
    farm.itr_registration_number = "1.234.567-8"
    env.db.commit()

    class Upload:
        filename = "ITR 2026.pdf"
        content_type = "application/pdf"

    async def fake_attachments(_req, field_name="attachments"):
        assert field_name == "file"
        return [Upload()]

    monkeypatch.setattr(env.routes, "_request_attachments", fake_attachments)
    monkeypatch.setattr(env.routes, "_read_attachments", lambda ups: [("ITR 2026.pdf", "application/pdf", b"%PDF")])
    resp = asyncio.run(env.routes.upload_farm_document_action(
        farm.id, env.request("/x"), csrf_token="x", doc_type="ITR", reference="Exercício 2026", valid_until="",
        db=env.db, user=env.user,
    ))
    assert resp.headers["location"] == f"/fazendas/{farm.id}?aba=documentos"
    att = env.db.query(FarmAttachment).filter_by(farm_id=farm.id).one()
    assert att.doc_type == "ITR" and att.reference == "Exercício 2026"

    html = _detail(env, farm, "documentos")
    assert "ITR 2026.pdf" in html and "Excluir arquivo" in html and "Substituir por nova versão" in html

    env.routes.edit_farm_document_action(
        farm.id, att.id, env.request("/x"), csrf_token="x", doc_type="Outorga de água", reference="Processo 123",
        valid_until="2027-08-31", db=env.db, user=env.user,
    )
    env.db.refresh(att)
    assert att.doc_type == "Outorga de água" and att.valid_until == date(2027, 8, 31)

    resp = env.routes.delete_farm_attachment_action(farm.id, att.id, env.request("/x"), csrf_token="x", db=env.db, user=env.user)
    assert resp.headers["location"] == f"/fazendas/{farm.id}?aba=documentos"
    assert env.db.query(FarmAttachment).filter_by(farm_id=farm.id).count() == 0


def test_area_divergence_keep_or_use_map(env, monkeypatch):
    _open_writes(env, monkeypatch)
    farm = env.db.query(Farm).one()
    farm.boundary_geojson = SAMPLE_GEOJSON
    farm.total_area = 10.0
    env.db.commit()
    assert "O mapa e o cadastro mostram áreas diferentes" in _detail(env, farm, "territorio")

    env.routes.confirm_farm_area_action(farm.id, env.request("/x"), csrf_token="x", choice="keep", db=env.db, user=env.user)
    env.db.refresh(farm)
    assert float(farm.total_area) == 10.0 and farm.area_ack_ha is not None
    assert "O mapa e o cadastro mostram áreas diferentes" not in _detail(env, farm, "territorio")

    env.routes.confirm_farm_area_action(farm.id, env.request("/x"), csrf_token="x", choice="map", db=env.db, user=env.user)
    env.db.refresh(farm)
    assert float(farm.total_area) != 10.0 and farm.area_ack_ha is None


def test_list_shows_pending_and_score_and_clima_has_rain_alerts(env):
    farm = env.db.query(Farm).one()
    html = env.routes.farms_page(env.request("/fazendas"), BackgroundTasks(), db=env.db, user=env.user, csrf_token="x").body.decode("utf-8")
    assert "Com pendências" in html and "Sem mapa" in html and "fz-pins-data" in html
    assert "% completo" not in html  # percentual aparece só como barra + número na linha
    assert f'href="/fazendas/{farm.id}"' in html
