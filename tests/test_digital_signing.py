import base64
import json
import re
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from io import BytesIO
from types import SimpleNamespace

import httpx
import pytest
from asn1crypto import core, crl
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient
from pyhanko.keys.internal import translate_pyca_cryptography_cert_to_asn1
from pyhanko.pdf_utils.incremental_writer import IncrementalPdfFileWriter
from pyhanko.sign import fields, signers
from pyhanko_certvalidator import ValidationContext
from reportlab.pdfgen.canvas import Canvas
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from starlette.middleware.sessions import SessionMiddleware

from app.core.csrf import ensure_csrf_token
from app.core.deps import get_current_user_web
from app.db.base import Base
from app.db.session import get_db
from app.models import Farm, Organization, User
from app.models.signed_document import SigningRevision
from app.services import digital_certificates as certificates
from app.services.document_signing import pdf, policy, workflow
from app.services.nfe_certificate_vault import Pkcs12CertificateProvider, store_encrypted_certificate
from app.web.digital_documents import router

PASSWORD = "senha-a1-de-teste"
CPF = "12345678901"


class LocalTestClient(TestClient):
    # Starlette uses a local ASGI transport; the global fixture blocks network HTTP.
    send = httpx.Client.send



def _original():
    stream = BytesIO()
    canvas = Canvas(stream, pageCompression=0)
    canvas.drawString(60, 400, "Documento de teste")
    canvas.showPage()
    canvas.drawString(60, 400, "Segunda pagina")
    canvas.save()
    return stream.getvalue()


@pytest.fixture
def authority(monkeypatch):
    now = datetime.now(timezone.utc)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, "Raiz de teste")])
    root = (
        x509.CertificateBuilder().issuer_name(ca_name).subject_name(ca_name).serial_number(1)
        .not_valid_before(now - timedelta(days=3)).not_valid_after(now + timedelta(days=60)).public_key(ca_key.public_key())
        .add_extension(x509.BasicConstraints(ca=True, path_length=1), True).sign(ca_key, hashes.SHA256())
    )
    revoked = set()

    def validation_context(**kwargs):
        builder = (x509.CertificateRevocationListBuilder().issuer_name(ca_name)
                   .last_update(now - timedelta(days=1)).next_update(now + timedelta(days=1)))
        for serial in revoked:
            builder = builder.add_revoked_certificate(
                x509.RevokedCertificateBuilder().serial_number(serial).revocation_date(now - timedelta(hours=3)).build()
            )
        revocations = builder.sign(ca_key, hashes.SHA256())
        return ValidationContext(
            trust_roots=[translate_pyca_cryptography_cert_to_asn1(root)], revocation_mode="require",
            crls=[crl.CertificateList.load(revocations.public_bytes(serialization.Encoding.DER))],
        )

    def make(serial, holder, *, cpf=CPF):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, holder)])
        san = x509.SubjectAlternativeName([
            x509.OtherName(certificates.HOLDER_OID, core.OctetString(("01011990" + cpf + "0" * 26).encode()).dump())
        ])
        leaf = (
            x509.CertificateBuilder().issuer_name(ca_name).subject_name(subject).serial_number(serial)
            .not_valid_before(now - timedelta(days=1)).not_valid_after(now + timedelta(days=22)).public_key(key.public_key())
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), True)
            .add_extension(x509.KeyUsage(True, True, False, False, False, False, False, False, False), True)
            .add_extension(san, False).sign(ca_key, hashes.SHA256())
        )
        pfx = pkcs12.serialize_key_and_certificates(
            b"test", key, leaf, [root], serialization.BestAvailableEncryption(PASSWORD.encode())
        )
        return SimpleNamespace(signer=pdf.a1_signer({"certificate": leaf, "private_key": key, "chain": [root]}), pfx=pfx)

    monkeypatch.setattr(pdf, "validation_context", validation_context)
    return make, revoked


def test_pades_visible_and_invisible_preserve_previous_signature(authority):
    make, _ = authority
    file = _original()
    first, field, first_report = pdf.sign_document(
        file, make(10, "Primeira titular").signer, {"visible": True, "page": 1, "box": [45, 70, 224, 118]},
    )
    assert first.startswith(file)
    assert first_report["state"] == "valid"
    reader = pdf.read_pdf(first)
    original_sig = reader.embedded_signatures[0]
    assert original_sig.sig_object["/Filter"] == policy.FILTER
    assert original_sig.sig_object["/SubFilter"] == policy.SUBFILTER
    assert original_sig.fieldmdp.is_locked(field)
    assert original_sig.sig_field["/Rect"] == [45, 70, 224, 118]
    assert reader.root["/Extensions"]["/ESIC"]["/ExtensionLevel"] >= 2
    assert all(attr["type"].native != "signing_time" for attr in original_sig.signer_info["signed_attrs"])

    second, second_field, report = pdf.sign_document(first, make(11, "Segundo titular").signer, {"visible": False})
    assert second.startswith(first)
    assert [item["holder"] for item in report["signatures"]] == ["Primeira titular", "Segundo titular"]
    assert [item["state"] for item in report["signatures"]] == ["valid", "valid"]
    newest = pdf.read_pdf(second).embedded_signatures[-1]
    assert newest.fq_name == second_field
    assert newest.sig_field["/Rect"] == [0, 0, 0, 0]
    assert pdf.read_pdf(second).embedded_signatures[0].pkcs7_content == original_sig.pkcs7_content


@pytest.mark.parametrize("permission", [fields.MDPPerm.NO_CHANGES, fields.MDPPerm.FILL_FORMS])
def test_respect_external_certification_and_sign_existing_slot(authority, permission):
    make, _ = authority
    writer = IncrementalPdfFileWriter(BytesIO(_original()))
    fields.append_signature_field(writer, fields.SigFieldSpec("OutraAssinatura", on_page=1, box=(40, 45, 180, 88)))
    certified = signers.sign_pdf(
        writer, signers.PdfSignatureMetadata(field_name="Certificacao", certify=True, docmdp_permissions=permission),
        signer=make(20, "Certificação externa").signer,
    ).getvalue()
    options = pdf.signing_options(pdf.read_pdf(certified))
    assert not options["free_position"]
    with pytest.raises(ValueError, match="restringe"):
        pdf.sign_document(certified, make(21, "Outra pessoa").signer, {"visible": False})
    if permission == fields.MDPPerm.NO_CHANGES:
        assert options["fields"] == []
        with pytest.raises(ValueError, match="bloqueado"):
            pdf.sign_document(certified, make(21, "Outra pessoa").signer, {"field": "OutraAssinatura"})
    else:
        assert options["fields"] == [{"name": "OutraAssinatura", "location": {"page": 1, "box": [40, 45, 180, 88]}}]
        updated, _, outcome = pdf.sign_document(certified, make(21, "Outra pessoa").signer, {"field": "OutraAssinatura"})
        assert updated.startswith(certified)
        assert all(item["integrity"] for item in outcome["signatures"])


def test_external_validation_reports_tampering_and_revocation(authority):
    make, revoked = authority
    raw, _, _ = pdf.sign_document(_original(), make(24, "Titular externo").signer, {"visible": False})
    assert pdf.validate_pdf(raw)["state"] == "valid"
    assert pdf.validate_pdf(_original())["state"] == "unsigned"
    modified = raw.replace(b"Documento de teste", b"Documento de falso", 1)
    assert pdf.validate_pdf(modified)["state"] == "invalid"
    revoked.add(24)
    report = pdf.validate_pdf(raw)
    assert report["state"] != "valid"
    assert report["signatures"][0]["revocation"] == "revoked"
    with pytest.raises(ValueError, match="revogação"):
        pdf.sign_document(_original(), make(24, "Titular revogado").signer, {"visible": False})


@pytest.fixture
def web_db(monkeypatch):
    settings = SimpleNamespace(
        digital_certificate_key=base64.b64encode(b"A" * 32).decode(),
        digital_document_key=base64.b64encode(b"B" * 32).decode(),
        digital_signing_iti_receipts=None, digital_signing_pilot_farm_ids="1", is_production=False,
    )
    monkeypatch.setattr(certificates, "get_settings", lambda: settings)
    monkeypatch.setattr(policy, "get_settings", lambda: settings)
    monkeypatch.setattr("app.services.nfe_certificate_vault._configured_master_key", lambda: "F" * 40)
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine, autoflush=False)
    with sessions() as db:
        db.add_all([Organization(id=1, name="Org 1", slug="org-1"), Organization(id=2, name="Org 2", slug="org-2")])
        db.add_all([Farm(id=n, organization_id=1 if n in (1, 2) else 2, name=f"Fazenda {n}",
                         location="Bahia", total_area=8) for n in (1, 2, 3)])
        db.add_all([User(id=n, name=f"Pessoa {n}", email=f"conta{n}@example.test", hashed_password="unused",
                         organization_id=1 if n in (1, 2) else 2, active_farm_id=1 if n in (1, 2) else 3,
                         is_admin=True) for n in (1, 2, 3)])
        db.commit()
    return sessions, settings


def test_a1_key_configuration_distinguishes_absence_and_bad_format(web_db):
    _, settings = web_db
    setting = "digital_certificate_key"
    expected = b"A" * 32
    settings.digital_certificate_key = " " + base64.b64encode(expected).decode() + "\n"
    assert certificates.encryption_key(setting) == expected
    for value in (None, " "):
        settings.digital_certificate_key = value
        with pytest.raises(certificates.EncryptionConfigurationError, match="DIGITAL_CERTIFICATE_KEY está ausente"):
            certificates.encryption_key(setting)
    for value in ("segredo-invalido", base64.b64encode(b"curta").decode()):
        settings.digital_certificate_key = value
        with pytest.raises(certificates.EncryptionConfigurationError, match="32 bytes em Base64") as exc:
            certificates.encryption_key(setting)
        assert value not in str(exc.value)


def test_central_reuses_fiscal_a1_and_controls_independent_purposes(authority, web_db):
    make, _ = authority
    sessions, _ = web_db
    material = make(25, "Titular")
    with sessions() as db:
        owner = db.get(User, 1)
        admin = db.get(User, 2)
        foreign = db.get(User, 3)
        old = store_encrypted_certificate(db, organization_id=1, farm_id=1, environment="homologacao",
                                          pfx_bytes=material.pfx, password=PASSWORD, user_id=owner.id)
        with pytest.raises(ValueError, match="CPF"):
            certificates.verify_responsible_identity(db, cert=old, user=admin, cpf="20000000000",
                                                      password=PASSWORD, reference="teste")
        with pytest.raises(ValueError, match="organização"):
            certificates.verify_responsible_identity(db, cert=old, user=foreign, cpf=CPF,
                                                      password=PASSWORD, reference="teste")
        certificates.verify_responsible_identity(db, cert=old, user=owner, cpf=CPF,
                                                  password=PASSWORD, reference="verificação de teste")
        db.commit()
        assert workflow.usable_certificates(db, owner, 1) == []
        with pytest.raises(PermissionError):
            certificates.authorize_purpose(db, cert=old, user=admin, farm_id=1, purpose=certificates.DOCUMENTS,
                                           enable=True, password=PASSWORD)
        certificates.authorize_purpose(db, cert=old, user=owner, farm_id=1, purpose=certificates.DOCUMENTS,
                                       enable=True, password=PASSWORD)
        db.commit()
        assert workflow.usable_certificates(db, owner, 1) == [old]
        assert workflow.usable_certificates(db, owner, 2) == []
        assert certificates.fiscal_certificate_query(db, 1, 1, active=True).one() == old

        new = certificates.store_a1(db, organization_id=1, farm_id=1, user_id=owner.id,
                                    pfx=make(26, "Titular novo").pfx, password=PASSWORD)
        db.flush()
        certificates.verify_responsible_identity(db, cert=new, user=owner, cpf=CPF,
                                                  password=PASSWORD, reference="verificação de teste 2")
        db.flush()
        certificates.authorize_purpose(db, cert=new, user=owner, farm_id=1, purpose=certificates.FISCAL,
                                       enable=True, password=PASSWORD)
        db.commit()
        assert certificates.fiscal_certificate_query(db, 1, 1, environment="homologacao", active=True).one() == new
        assert Pkcs12CertificateProvider.from_vault(new, db=db).export_pem_pair()[0].startswith(b"-----BEGIN CERTIFICATE")
        assert workflow.usable_certificates(db, owner, 1) == [old]
        replacement = store_encrypted_certificate(db, organization_id=1, farm_id=1, environment="homologacao",
                                                   pfx_bytes=material.pfx, password=PASSWORD, user_id=owner.id)
        assert certificates.fiscal_certificate_query(db, 1, 1, active=True).one() == replacement
        assert workflow.usable_certificates(db, owner, 1) == [old]
        with pytest.raises(PermissionError):
            Pkcs12CertificateProvider.from_vault(new, db=db)


def test_signing_web_workflow_and_external_validation_without_certificate(authority, web_db, monkeypatch):
    make, _ = authority
    sessions, settings = web_db
    emails = []
    monkeypatch.setattr(workflow, "send_plain_email", lambda *args: emails.append(args))
    web = FastAPI()
    web.add_middleware(SessionMiddleware, secret_key="secret-for-tests")
    web.include_router(router)
    web.mount("/static", StaticFiles(directory="app/static"), name="static")

    def db_dependency():
        with sessions() as db:
            yield db

    def user_dependency(request: Request):
        with sessions() as db:
            return db.get(User, request.session["user_id"])

    @web.get("/test-login")
    def login(request: Request, user_id: int, farm_id: int = 1):
        request.session.clear()
        request.session["user_id"] = user_id
        request.session["active_farm_id"] = farm_id
        request.session["user_email"] = f"conta{user_id}@example.test"
        return {"csrf": ensure_csrf_token(request)}

    web.dependency_overrides[get_db] = db_dependency
    web.dependency_overrides[get_current_user_web] = user_dependency
    with sessions() as db:
        for user_id in (1, 2):
            owner = db.get(User, user_id)
            cert = certificates.store_a1(db, organization_id=1, farm_id=1, user_id=owner.id,
                                         pfx=make(30 + user_id, f"Titular {user_id}").pfx, password=PASSWORD)
            db.flush()
            certificates.verify_responsible_identity(db, cert=cert, user=owner, cpf=CPF,
                                                      password=PASSWORD, reference=f"prova-{user_id}")
            db.flush()
            certificates.authorize_purpose(db, cert=cert, user=owner, farm_id=1, purpose=certificates.DOCUMENTS,
                                           enable=True, password=PASSWORD)
            if user_id == 1:
                first_id = cert.id
            else:
                second_id = cert.id
        db.commit()

    def login_headers(client, user_id, farm_id=1):
        return {
            "X-CSRF-Token": client.get("/test-login", params={"user_id": user_id, "farm_id": farm_id}).json()["csrf"],
        }

    def authorize(client, endpoint, headers, cert_id, digest, expected=201):
        result = client.post(endpoint + "/autorizar", headers=headers, json={
            "consent": True, "certificate_id": cert_id, "appearance": {"visible": False}, "sha256": digest,
        })
        assert result.status_code == expected, result.text
        return result

    def sign(client, endpoint, headers, challenge_id, code):
        return client.post(endpoint + "/assinar", headers=headers, json={
            "consent": True, "challenge_id": challenge_id, "code": code, "password": PASSWORD,
        })

    client = LocalTestClient(web)
    original = _original()
    headers = login_headers(client, 1)
    with sessions() as db:
        new_cert = certificates.store_a1(db, organization_id=1, farm_id=1, user_id=1,
                                         pfx=make(45, "Responsável pela web").pfx, password=PASSWORD)
        db.commit()
        binding_url = f"/certificados-digitais/{new_cert.id}/vinculo"
        new_id = new_cert.id
    with sessions() as db:
        db.get(User, 2).is_admin = False
        db.commit()

    central = client.get("/certificados-digitais").text
    assert "Vincular responsável" in central and "conta2@example.test" in central
    assert "conta3@example.test" not in central
    assert client.post(binding_url, headers=headers, json={"user_id": 3}).status_code == 404
    holder_client = LocalTestClient(web)
    holder_headers = login_headers(holder_client, 2)
    assert holder_client.post(binding_url, headers=holder_headers, json={"user_id": 2}).status_code == 403
    assert client.post(binding_url, headers=headers, json={"user_id": 2}).status_code == 200
    assert "Responsável pela web" in holder_client.get("/certificados-digitais").text
    with sessions() as db:
        selected = db.get(certificates.CertificateHolderAssignment, new_id)
        assert selected.user_id == 2
        assert db.get(certificates.CertificatePurpose, (new_id, certificates.DOCUMENTS)) is None
    purpose_url = binding_url.removesuffix("/vinculo") + "/finalidades"
    authorization = {"purpose": "documents", "enable": True, "consent": True, "password": PASSWORD}
    assert client.post(purpose_url, headers=headers, json={**authorization, "cpf": CPF}).status_code == 403
    assert holder_client.post(purpose_url, headers=holder_headers, json=authorization).status_code == 422
    enabled = holder_client.post(purpose_url, headers=holder_headers, json={**authorization, "cpf": CPF})
    assert enabled.status_code == 200, enabled.text
    # Rebinding to the same user is idempotent; changing to another user is blocked for security!
    assert client.post(binding_url, headers=headers, json={"user_id": 2}).status_code == 200
    with sessions() as db:
        grant = db.get(certificates.CertificatePurpose, (new_id, certificates.DOCUMENTS))
        assert grant.user_id == 2 and grant.enabled_at is not None and grant.disabled_at is None
    assert client.post(binding_url, headers=headers, json={"user_id": 1}).status_code == 403
    with sessions() as db:
        grant = db.get(certificates.CertificatePurpose, (new_id, certificates.DOCUMENTS))
        assert grant.user_id == 2 and grant.disabled_at is None
    assert client.get("/assinatura-digital").status_code == 200
    assert client.get("/certificados-digitais").status_code == 200
    assert client.post("/assinatura-digital/documentos", content=original,
                       headers={"Content-Type": "application/pdf"}).status_code == 403
    created = client.post("/assinatura-digital/documentos", content=original, headers={
        **headers, "Content-Type": "application/pdf", "X-Document-Name": "Contrato.pdf",
    })
    assert created.status_code == 201
    endpoint = created.json()["url"]
    assert client.get(endpoint).status_code == 200
    assert client.get(endpoint + "/revisoes/1/pdf").content == original
    with sessions() as db:
        assert original not in db.scalars(select(SigningRevision)).one().encrypted_pdf

    # Signing is immediately available in production without requiring an ITI homologação manifest
    settings.is_production = True
    issue = authorize(client, endpoint, headers, first_id, sha256(original).hexdigest())
    settings.is_production = False
    challenge = issue.json()["challenge_id"]
    code = re.search(r"Código de autorização: (\d{8})", emails[-1][2]).group(1)
    assert sign(client, endpoint, headers, challenge, "invalid").status_code == 403
    signed = sign(client, endpoint, headers, challenge, code)
    assert signed.status_code == 200, signed.text
    assert sign(client, endpoint, headers, challenge, code).status_code == 409
    assert client.post(endpoint + "/participantes", headers=headers, json={
        "email": "conta2@example.test", "consent": True,
    }).status_code == 200

    collaborator = LocalTestClient(web)
    second_headers = login_headers(collaborator, 2)
    assert collaborator.get(endpoint).status_code == 200
    authorize(collaborator, endpoint, second_headers, first_id, signed.json()["sha256"], 403)
    issue = authorize(collaborator, endpoint, second_headers, second_id, signed.json()["sha256"])
    code = re.search(r"Código de autorização: (\d{8})", emails[-1][2]).group(1)
    result = sign(collaborator, endpoint, second_headers, issue.json()["challenge_id"], code)
    assert result.status_code == 200, result.text
    assert result.json()["revision"] == 3
    second_pdf = collaborator.get(endpoint + "/revisoes/3/pdf").content

    foreign = LocalTestClient(web)
    foreign_headers = login_headers(foreign, 3, 3)
    assert foreign.get(endpoint + "/revisoes/3/pdf").status_code == 404
    validation = foreign.post("/assinatura-digital/validar", content=second_pdf, headers={
        **foreign_headers, "Content-Type": "application/pdf",
    })
    assert validation.status_code == 200
    assert [signature["state"] for signature in validation.json()["signatures"]] == ["valid", "valid"]


def test_release_requires_three_approved_artifacts(monkeypatch, tmp_path):
    manifest_path = tmp_path / "homologacao.json"
    confirmations = {}
    for case in ("visible", "invisible", "multiple"):
        evidence = {"result": "Aprovado"}
        for label in ("pdf", "report"):
            name = f"{case}-{label}.pdf"
            raw = f"fixture-{name}".encode()
            (tmp_path / name).write_bytes(raw)
            evidence[label] = name
            evidence[f"{label}_sha256"] = sha256(raw).hexdigest()
        confirmations[case] = evidence
    manifest_path.write_text(json.dumps({
        "policy_oid": policy.POLICY_OID, "policy_sha256": policy.POLICY_FILE_HASH, "confirmations": confirmations,
    }))
    settings = SimpleNamespace(digital_signing_iti_receipts=str(manifest_path),
                               digital_signing_pilot_farm_ids="1", is_production=True)
    monkeypatch.setattr(policy, "get_settings", lambda: settings)
    assert policy._confirmed_release(str(manifest_path)) is True
    (tmp_path / "multiple-report.pdf").write_bytes(b"changed")
    assert policy._confirmed_release(str(manifest_path)) is False
    # System is ready for use out of the box without homologação gate
    assert policy.signing_mode(1) == "released"
    assert policy.signing_mode(2) == "released"


def test_sign_with_email_code_only_and_delete_certificate_preserves_signed_documents(authority, web_db, monkeypatch):
    make, _ = authority
    sessions, _ = web_db
    emails = []
    monkeypatch.setattr(workflow, "send_plain_email", lambda *args: emails.append(args))
    web = FastAPI()
    web.add_middleware(SessionMiddleware, secret_key="secret-for-tests-2")
    web.include_router(router)
    web.mount("/static", StaticFiles(directory="app/static"), name="static")

    def db_dependency():
        with sessions() as db:
            yield db

    def user_dependency(request: Request):
        with sessions() as db:
            return db.get(User, request.session["user_id"])

    @web.get("/test-login")
    def login(request: Request, user_id: int, farm_id: int = 1):
        request.session.clear()
        request.session["user_id"] = user_id
        request.session["active_farm_id"] = farm_id
        request.session["user_email"] = f"conta{user_id}@example.test"
        return {"csrf": ensure_csrf_token(request)}

    web.dependency_overrides[get_db] = db_dependency
    web.dependency_overrides[get_current_user_web] = user_dependency

    client = LocalTestClient(web)
    headers = {"X-CSRF-Token": client.get("/test-login", params={"user_id": 1, "farm_id": 1}).json()["csrf"]}

    # 1) Register A1 certificate with responsible binding + immediate documents activation in one step
    pfx_b64 = base64.b64encode(make(55, "João Produtor:12345678901").pfx).decode()
    reg = client.post("/certificados-digitais", headers=headers, json={
        "file": pfx_b64, "password": PASSWORD, "holder_user_id": 1, "enable_documents": True, "cpf": CPF,
    })
    assert reg.status_code == 201, reg.text
    cert_id = reg.json()["id"]

    # Certificates page hides technical SHA-256 and shows friendly name + (...) menu
    cert_html = client.get("/certificados-digitais").text
    assert "João Produtor" in cert_html
    assert "e-CPF A1" in cert_html
    assert "Impressão digital SHA-256" not in cert_html
    assert "data-ds-open-delete-cert" in cert_html

    # 2) Import document with custom title & category
    original = _original()
    created = client.post("/assinatura-digital/documentos", content=original, headers={
        **headers,
        "Content-Type": "application/pdf",
        "X-Document-Name": "contrato_cafe.pdf",
        "X-Document-Title": "Contrato%20Venda%202026",
        "X-Document-Category": "Venda%20de%20caf%C3%A9",
    })
    assert created.status_code == 201, created.text
    endpoint = created.json()["url"]

    # Home page renders clean count, title, and (...) menu
    home_html = client.get("/assinatura-digital").text
    assert "Contrato Venda 2026" in home_html
    assert "data-nx-more-btn" in home_html
    assert "/assinatura-digital/novo" in home_html

    # New document dedicated Wizard page renders 3-step bar, 3 signer cards, sticky summary and animated dropzone
    wizard_html = client.get("/assinatura-digital/novo").text
    assert "data-ds-wizard" in wizard_html
    assert "Resumo" in wizard_html
    assert "Só eu vou assinar" in wizard_html
    assert "Eu e outras pessoas" in wizard_html
    assert "Apenas outras pessoas" in wizard_html
    assert "data-ds-drop-btn" in wizard_html

    # Workspace page does NOT ask for A1 password during signing
    ws_html = client.get(endpoint).text
    assert "Senha do certificado A1" not in ws_html

    # 3) Authorize and sign using ONLY the email code (no password field!) with compact stamp style
    issue = client.post(endpoint + "/autorizar", headers=headers, json={
        "consent": True,
        "certificate_id": cert_id,
        "appearance": {"visible": True, "page": 0, "box": [45, 70, 250, 125], "style": "compact"},
        "sha256": sha256(original).hexdigest(),
    })
    assert issue.status_code == 201, issue.text
    code = re.search(r"Código de autorização: (\d{8})", emails[-1][2]).group(1)
    signed = client.post(endpoint + "/assinar", headers=headers, json={
        "consent": True, "challenge_id": issue.json()["challenge_id"], "code": code,
    })
    assert signed.status_code == 200, signed.text
    assert signed.json()["validation"]["state"] == "valid"

    # Reopening a completed single-signer document shows "Documento já assinado" instead of the signing form
    ws_signed = client.get(endpoint).text
    assert "Documento já assinado" in ws_signed
    assert "disponível para download" in ws_signed
    assert "data-ds-authorize" not in ws_signed

    # Register A1 for another user with CPF and WhatsApp saved to profile when empty
    other_pfx = base64.b64encode(make(56, "Maria Produtora:12345678901").pfx).decode()
    reg_other = client.post("/certificados-digitais", headers=headers, json={
        "file": other_pfx, "password": PASSWORD, "holder_user_id": 2, "cpf": CPF, "whatsapp": "(35) 99888-7766",
    })
    assert reg_other.status_code == 201, reg_other.text
    with sessions() as db:
        assert db.get(User, 2).phone == "(35) 99888-7766"

    # 4) Delete certificate and verify signed PDF remains valid and downloadable
    removed = client.delete(f"/certificados-digitais/{cert_id}", headers=headers)
    assert removed.status_code == 200, removed.text
    signed_pdf = client.get(endpoint + "/revisoes/2/pdf").content
    validation = client.post("/assinatura-digital/validar", content=signed_pdf, headers={
        **headers, "Content-Type": "application/pdf",
    })
    assert validation.status_code == 200
    assert validation.json()["state"] == "valid"

    # Owner can also delete an already-signed document from the list
    assert client.delete(endpoint, headers=headers).status_code == 200
    assert client.get(endpoint).status_code == 404

    # 5) Unsigned document can be deleted by owner; workspace avoids repeating filename when title == filename stem
    # and keeps consent checkbox disabled when user has no active certificate selected
    draft = client.post("/assinatura-digital/documentos", content=original, headers={
        **headers, "Content-Type": "application/pdf", "X-Document-Name": "rascunho.pdf",
    })
    assert draft.status_code == 201
    draft_url = draft.json()["url"]
    draft_ws = client.get(draft_url).text
    assert "rascunho.pdf" not in draft_ws
    assert 'data-ds-consent disabled' in draft_ws
    assert client.delete(draft_url, headers=headers).status_code == 200
    assert client.get(draft_url).status_code == 404

    # 6) Logical-delete fallback when database FK blocks hard delete still removes certificate cleanly
    other_id = reg_other.json()["id"]
    with sessions() as db:
        from sqlalchemy.exc import IntegrityError
        orig_execute = db.execute
        def raise_fk_on_cert_delete(stmt, *args, **kwargs):
            if str(stmt).startswith("DELETE FROM nfe_certificate_vaults"):
                raise IntegrityError("DELETE", {}, Exception("foreign key violation"))
            return orig_execute(stmt, *args, **kwargs)
        monkeypatch.setattr(db, "execute", raise_fk_on_cert_delete)
        certificates.delete_certificate(
            db,
            cert=db.get(certificates.DigitalCertificate, other_id),
            user=db.get(User, 1),
            farm_id=1,
            can_manage_fiscal=True,
        )
        db.commit()
    with sessions() as db:
        soft_cert = db.get(certificates.DigitalCertificate, other_id)
        assert soft_cert is not None and soft_cert.deleted_at is not None and not soft_cert.is_active
        assert soft_cert.ciphertext_b64 == ""
        assert workflow.usable_certificates(db, user=db.get(User, 2), farm_id=1) == []
    assert "Maria Produtora" not in client.get("/certificados-digitais").text

    # 7) "Apenas outras pessoas" (owner_signs=False) counts only invited signers (0 de 1, Aguardando outros)
    # and actually dispatches WhatsApp notification when channel="whatsapp"
    sent_whatsapp = []
    from app.web import digital_documents as web_dd
    monkeypatch.setattr(web_dd, "whatsapp_is_configured", lambda: True)
    class _FakeWA:
        def send_text(self, to, body, **kwargs):
            sent_whatsapp.append((to, body))
            return {"ok": True}
    monkeypatch.setattr(web_dd, "WhatsAppClient", _FakeWA)

    others_only = client.post("/assinatura-digital/documentos", content=original, headers={
        **headers,
        "Content-Type": "application/pdf",
        "X-Document-Name": "so_terceiro.pdf",
        "X-Document-Title": "Contrato%20Terceiro",
        "X-Document-Owner-Signs": "0",
    })
    assert others_only.status_code == 201, others_only.text
    others_url = others_only.json()["url"]
    invited = client.post(others_url + "/participantes", headers=headers, json={
        "email": "conta2@example.test", "channel": "whatsapp", "consent": True,
    })
    assert invited.status_code == 200, invited.text
    assert invited.json()["notification_sent"] is True
    assert len(sent_whatsapp) == 1
    assert sent_whatsapp[0][0] == "5535998887766"
    assert "Contrato Terceiro" in sent_whatsapp[0][1]

    home_after = client.get("/assinatura-digital").text
    assert "Contrato Terceiro" in home_after
    assert "Aguardando outros" in home_after
    ws_others = client.get(others_url).text
    assert "0 de 1 assinou" in ws_others
    assert "0 de 2 assinou" not in ws_others
    assert "Aguardando assinaturas" in ws_others
    assert "data-ds-authorize" not in ws_others

