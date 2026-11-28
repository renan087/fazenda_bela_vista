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
from app.models.signed_document import SigningDocument, SigningRevision
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


@pytest.mark.parametrize("perm", [fields.MDPPerm.FILL_FORMS, fields.MDPPerm.ANNOTATE])
def test_cosign_govbr_style_docmdp_pdf_without_empty_slots(authority, perm):
    make, _ = authority
    writer = IncrementalPdfFileWriter(BytesIO(_original()))
    govbr_pdf = signers.sign_pdf(
        writer,
        signers.PdfSignatureMetadata(field_name="AssinaturaGovBr", certify=True, docmdp_permissions=perm),
        signer=make(25, "Assinatura Gov.br").signer,
    ).getvalue()
    options = pdf.signing_options(pdf.read_pdf(govbr_pdf))
    assert options["free_position"] is True
    cosigned, _, report = pdf.sign_document(
        govbr_pdf, make(26, "Produtor ICP-Brasil").signer, {"visible": True, "page": 0, "box": [45, 70, 245, 150]},
    )
    assert cosigned.startswith(govbr_pdf)
    assert [item["state"] for item in report["signatures"]] == ["valid", "valid"]


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


def test_document_list_paginates_and_filters_beyond_the_first_hundred(web_db):
    sessions, _ = web_db
    now = datetime.now(timezone.utc)
    category = workflow.DOCUMENT_CATEGORIES[0]
    with sessions() as db:
        db.add_all(
            SigningDocument(
                id=f"doc-{n:03}", organization_id=1, farm_id=1, owner_id=1,
                title=f"Contrato {n:03}" if n < 104 else "Cédula antiga",
                filename=f"arquivo-{n:03}.pdf", category=category, validation={},
                updated_at=now - timedelta(minutes=n),
            )
            for n in range(105)
        )
        db.add(SigningDocument(
            id="unshared", organization_id=1, farm_id=1, owner_id=2,
            title="Cédula de outra pessoa", filename="outra.pdf", category=category,
            validation={}, updated_at=now,
        ))
        db.commit()

    app = FastAPI()
    app.add_middleware(SessionMiddleware, secret_key="pagination-test")
    app.include_router(router)
    app.mount("/static", StaticFiles(directory="app/static"), name="static")

    def db_dependency():
        with sessions() as db:
            yield db

    def signing_user(request: Request):
        request.session["active_farm_id"] = 1
        with sessions() as db:
            return db.get(User, 1)

    app.dependency_overrides[get_db] = db_dependency
    app.dependency_overrides[get_current_user_web] = signing_user
    with LocalTestClient(app) as client:
        first = client.get("/assinatura-digital")
        assert first.status_code == 200
        assert first.text.count("data-ds-list-row") == 10
        assert "Página 1 de 11" in first.text
        assert re.findall(r'<option value="(\d+)"(?: selected)?>\d+</option>', first.text) == ["10", "30", "50", "100"]

        for size in (30, 50, 100):
            page = client.get("/assinatura-digital", params={"por": size})
            assert page.text.count("data-ds-list-row") == size
            assert f'<option value="{size}" selected>' in page.text

        last = client.get("/assinatura-digital", params={"por": 100, "pagina": 999})
        assert last.text.count("data-ds-list-row") == 5
        assert "Página 2 de 2" in last.text
        assert "Cédula antiga" in last.text

        filtered = client.get("/assinatura-digital", params={
            "por": 30, "pagina": 2, "q": "cedula", "categoria": category, "situacao": "your_turn",
        })
        assert filtered.text.count("data-ds-list-row") == 1
        assert "Cédula antiga" in filtered.text
        assert "Cédula de outra pessoa" not in filtered.text
        assert "Página 1 de 1" in filtered.text


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

    # New document dedicated Wizard page renders segmented control, scalable search, sticky summary and animated dropzone
    wizard_html = client.get("/assinatura-digital/novo").text
    assert "data-ds-wizard" in wizard_html
    assert "Resumo" in wizard_html
    assert "Só eu" in wizard_html
    assert "Eu + outros" in wizard_html
    assert "Só outras pessoas" in wizard_html
    assert "Buscar na equipe, por nome, cargo ou email..." in wizard_html
    assert "Selecionar da equipe" in wizard_html
    assert "data-ds-colleagues-data" in wizard_html
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
    assert "0 de 1</strong> assinou" in ws_others
    assert "0 de 2</strong>" not in ws_others
    assert "Aguardando 1 assinatura" in ws_others
    assert "Aguardando assinaturas" in ws_others
    assert "data-ds-authorize" not in ws_others

    # 8) Importing an already-signed external PDF still opens for the user's signature and preserves BOTH signatures
    pfx_b64_re = base64.b64encode(make(57, "João Produtor:12345678901").pfx).decode()
    reg_re = client.post("/certificados-digitais", headers=headers, json={
        "file": pfx_b64_re, "password": PASSWORD, "holder_user_id": 1, "enable_documents": True, "cpf": CPF,
    })
    assert reg_re.status_code == 201, reg_re.text
    re_cert_id = reg_re.json()["id"]

    pre_signed_upload = client.post("/assinatura-digital/documentos", content=signed_pdf, headers={
        **headers,
        "Content-Type": "application/pdf",
        "X-Document-Name": "contrato_ja_assinado.pdf",
        "X-Document-Title": "Contrato%20Externo%20Assinado",
    })
    assert pre_signed_upload.status_code == 201, pre_signed_upload.text
    pre_url = pre_signed_upload.json()["url"]
    ws_pre = client.get(pre_url).text
    assert "data-ds-authorize" in ws_pre
    assert "1 de 2" in ws_pre
    assert "Documento já assinado" not in ws_pre

    issue_pre = client.post(pre_url + "/autorizar", headers=headers, json={
        "consent": True,
        "certificate_id": re_cert_id,
        "appearance": {"visible": True, "page": 0, "box": [45, 160, 250, 240], "style": "full"},
        "sha256": sha256(signed_pdf).hexdigest(),
    })
    assert issue_pre.status_code == 201, issue_pre.text
    code_pre = re.search(r"Código de autorização: (\d{8})", emails[-1][2]).group(1)
    cosigned = client.post(pre_url + "/assinar", headers=headers, json={
        "consent": True, "challenge_id": issue_pre.json()["challenge_id"], "code": code_pre,
    })
    assert cosigned.status_code == 200, cosigned.text
    assert [s["state"] for s in cosigned.json()["validation"]["signatures"]] == ["valid", "valid"]
    assert "Documento já assinado" in client.get(pre_url).text



def _make_signing_app(sessions):
    web = FastAPI()
    web.add_middleware(SessionMiddleware, secret_key="secret-for-electronic-tests")
    web.include_router(router)
    web.mount("/static", StaticFiles(directory="app/static"), name="static")

    def db_dependency():
        with sessions() as db:
            yield db

    def user_dependency(request: Request):
        with sessions() as db:
            uid = request.session.get("user_id")
            if not uid:
                raise HTTPException(401, "Não autenticado.")
            return db.get(User, uid)

    @web.get("/test-login")
    def login(request: Request, user_id: int, farm_id: int = 1):
        request.session.clear()
        request.session["user_id"] = user_id
        request.session["active_farm_id"] = farm_id
        request.session["user_email"] = f"conta{user_id}@example.test"
        return {"csrf": ensure_csrf_token(request)}

    web.dependency_overrides[get_db] = db_dependency
    web.dependency_overrides[get_current_user_web] = user_dependency
    return web


def test_electronic_signing_manifesto_and_public_validation(web_db, monkeypatch):
    from app.services.document_signing import electronic

    sessions, _ = web_db
    emails = []
    monkeypatch.setattr(electronic, "send_plain_email", lambda *args: emails.append(args))

    web = _make_signing_app(sessions)
    client = LocalTestClient(web)
    headers = {"X-CSRF-Token": client.get("/test-login", params={"user_id": 1, "farm_id": 1}).json()["csrf"]}

    original = _original()
    created = client.post("/assinatura-digital/documentos", content=original, headers={
        **headers,
        "Content-Type": "application/pdf",
        "X-Document-Name": "termo_parceria.pdf",
        "X-Document-Title": "Termo%20de%20Parceria%20Agr%C3%ADcola",
        "X-Document-Signing-Policy": "electronic",
    })
    assert created.status_code == 201, created.text
    endpoint = created.json()["url"]

    # Workspace renders electronic signing form without needing an A1 certificate
    ws_html = client.get(endpoint).text
    assert "Assinatura Eletrônica SiSFarm" in ws_html
    assert "data-ds-el-cpf" in ws_html

    # Invalid CPF is rejected with clear validation error
    bad_cpf = client.post(endpoint + "/eletronica/autorizar", headers=headers, json={
        "signer_name": "João Produtor",
        "cpf": "111.111.111-11",
        "channel": "email",
        "appearance": {"visible": True, "page": 0, "box": [45, 70, 260, 140], "style": "full"},
        "consent": True,
    })
    assert bad_cpf.status_code == 422

    # Valid CPF issues 8-digit OTP via email
    issued = client.post(endpoint + "/eletronica/autorizar", headers=headers, json={
        "signer_name": "João Produtor",
        "cpf": "529.982.247-25",
        "channel": "email",
        "appearance": {"visible": True, "page": 0, "box": [45, 70, 260, 140], "style": "full"},
        "consent": True,
    })
    assert issued.status_code == 201, issued.text
    otp = re.search(r"Código de autorização: (\d{8})", emails[-1][2]).group(1)

    confirmed = client.post(endpoint + "/eletronica/assinar", headers=headers, json={
        "consent": True,
        "challenge_id": issued.json()["challenge_id"],
        "code": otp,
    })
    assert confirmed.status_code == 200, confirmed.text
    body = confirmed.json()
    ver_code = body["verification_code"]
    assert ver_code.startswith("SF-")
    assert body["sealed"] is True
    assert body["seal_mode"] == "internal_hmac"
    assert body["remaining"] == 0

    # Sealed PDF has automatic Manifesto appended (original pages + 1), remains compact,
    # and ALWAYS embeds official domain app.sisfarm.com.br (never testserver or onrender.com)
    from app.services.document_signing.pdf import read_pdf, signing_options
    sealed_pdf = client.get(f"{endpoint}/revisoes/{body['revision']}/pdf").content
    reader_sealed = read_pdf(sealed_pdf)
    assert signing_options(reader_sealed)["pages"] == signing_options(read_pdf(original))["pages"] + 1
    assert len(sealed_pdf) - len(original) < 35_000
    last_page = reader_sealed.root["/Pages"]["/Kids"][-1].get_object()
    manifesto_stream = last_page["/Resources"]["/XObject"]["/SFManifesto"].get_object().data
    assert b"https://app.sisfarm.com.br/validar/" in manifesto_stream
    assert b"testserver" not in manifesto_stream
    assert b"onrender.com" not in manifesto_stream

    # Public validation works WITHOUT login, with or without hyphens, masks CPF (LGPD), and checks PDF integrity
    anon = LocalTestClient(web)
    val_home = anon.get("/validar")
    assert val_home.status_code == 200
    assert "data-ds-code-mask" in val_home.text

    # Query without hyphens redirects cleanly to canonical SF-XXXX-XXXX-XXXX URL
    compact_code = ver_code.replace("-", "")
    redir = anon.get(f"/validar?codigo={compact_code}", follow_redirects=False)
    assert redir.status_code == 303
    assert redir.headers["location"] == f"/validar/{ver_code}"

    val_page = anon.get(f"/validar/{ver_code}")
    assert val_page.status_code == 200
    assert "Termo de Parceria Agrícola" in val_page.text
    assert "Eletrônica SiSFarm (Avançada)" in val_page.text
    assert "•••.982.247-••" in val_page.text
    assert "52998224725" not in val_page.text
    assert "529.982.247-25" not in val_page.text
    assert "não equivale à assinatura qualificada ICP-Brasil" in val_page.text

    pub_csrf = re.search(r'data-csrf="([^"]+)"', val_page.text).group(1)

    # Intact sealed PDF passes integrity check
    ok_check = anon.post(f"/validar/{ver_code}/conferir", content=sealed_pdf, headers={
        "X-CSRF-Token": pub_csrf, "Content-Type": "application/pdf",
    })
    assert ok_check.status_code == 200, ok_check.text
    assert ok_check.json()["matches"] is True
    assert ok_check.json()["headline"] == "Documento original confirmado"

    # Tampered PDF returns plain-language non-technical alert
    tampered_pdf = sealed_pdf + b"\n%tampered"
    bad_check = anon.post(f"/validar/{ver_code}/conferir", content=tampered_pdf, headers={
        "X-CSRF-Token": pub_csrf, "Content-Type": "application/pdf",
    })
    assert bad_check.status_code == 200, bad_check.text
    assert bad_check.json()["matches"] is False
    assert bad_check.json()["headline"] == "Este arquivo não é o mesmo que foi assinado"
    assert "SHA-256" not in bad_check.json()["message"]


def test_external_signer_and_mixed_icp_electronic_document(authority, web_db, monkeypatch):
    from app.services.document_signing import electronic

    make, _ = authority
    sessions, _ = web_db
    emails = []
    wa_sent = []
    monkeypatch.setattr(workflow, "send_plain_email", lambda *args: emails.append(args))
    monkeypatch.setattr(electronic, "send_plain_email", lambda *args: emails.append(args))
    monkeypatch.setattr(electronic, "whatsapp_is_configured", lambda: True)
    monkeypatch.setattr(electronic, "get_settings", lambda: SimpleNamespace(
        whatsapp_signing_invite_template="",
        whatsapp_signing_otp_template="",
        whatsapp_template_language="pt_BR",
    ))

    class _FakeWA:
        def send_text(self, to, body, **kwargs):
            wa_sent.append((to, body))
            return {"ok": True}

    monkeypatch.setattr(electronic, "WhatsAppClient", _FakeWA)

    web = _make_signing_app(sessions)
    owner_client = LocalTestClient(web)
    headers = {"X-CSRF-Token": owner_client.get("/test-login", params={"user_id": 1, "farm_id": 1}).json()["csrf"]}

    # Owner registers A1 certificate
    pfx_b64 = base64.b64encode(make(71, "João Produtor:12345678901").pfx).decode()
    reg = owner_client.post("/certificados-digitais", headers=headers, json={
        "file": pfx_b64, "password": PASSWORD, "holder_user_id": 1, "enable_documents": True, "cpf": CPF,
    })
    assert reg.status_code == 201, reg.text
    cert_id = reg.json()["id"]

    # Create flexible document and invite an external buyer via WhatsApp
    original = _original()
    created = owner_client.post("/assinatura-digital/documentos", content=original, headers={
        **headers,
        "Content-Type": "application/pdf",
        "X-Document-Name": "contrato_misto.pdf",
        "X-Document-Title": "Contrato%20Misto%20Caf%C3%A9",
        "X-Document-Signing-Policy": "flexible",
    })
    assert created.status_code == 201, created.text
    endpoint = created.json()["url"]

    # Without an active 24h Meta window (and no approved template), WhatsApp does not silently drop:
    # returns a clear warning + ready-to-click wa.me share URL!
    ext_res = owner_client.post(endpoint + "/externos", headers=headers, json={
        "name": "Carlos Comprador",
        "role_label": "Comprador",
        "channel": "whatsapp",
        "phone": "(35) 99123-4567",
    })
    assert ext_res.status_code == 201, ext_res.text
    ext_json = ext_res.json()
    assert ext_json["notification_sent"] is False
    assert "24h" in (ext_json["notification_error"] or "")
    assert ext_json["whatsapp_share_url"].startswith("https://wa.me/5535991234567?text=")
    assert len(wa_sent) == 0

    # Once the recipient has an active 24h inbound window, resending dispatches automatically via WhatsApp Cloud API!
    from app.models.whatsapp import WhatsAppInboundMessage
    with sessions() as db:
        db.add(WhatsAppInboundMessage(
            wamid="wamid.test.24h",
            wa_id="5535991234567",
            msg_type="text",
            received_at=datetime.now(timezone.utc),
        ))
        db.commit()
    resend_res = owner_client.post(f"{endpoint}/externos/{ext_json['signer_id']}/reenviar", headers=headers)
    assert resend_res.status_code == 200, resend_res.text
    assert resend_res.json()["notification_sent"] is True
    sign_url = resend_res.json()["sign_url"]
    token = sign_url.rsplit("/assinar/", 1)[1]
    assert len(wa_sent) == 1 and "Carlos Comprador" in wa_sent[0][1]

    # 1) Owner signs with Qualified ICP-Brasil A1
    issue_icp = owner_client.post(endpoint + "/autorizar", headers=headers, json={
        "consent": True,
        "certificate_id": cert_id,
        "appearance": {"visible": True, "page": 0, "box": [40, 70, 240, 140], "style": "compact"},
        "sha256": sha256(original).hexdigest(),
    })
    assert issue_icp.status_code == 201, issue_icp.text
    icp_otp = re.search(r"Código de autorização: (\d{8})", emails[-1][2]).group(1)
    icp_signed = owner_client.post(endpoint + "/assinar", headers=headers, json={
        "consent": True, "challenge_id": issue_icp.json()["challenge_id"], "code": icp_otp,
    })
    assert icp_signed.status_code == 200, icp_signed.text

    # 2) External buyer signs via /assinar/{token} WITHOUT logging in
    ext_client = LocalTestClient(web)
    ext_page = ext_client.get(f"/assinar/{token}")
    assert ext_page.status_code == 200
    assert "Carlos Comprador" in ext_page.text
    assert "Contrato Misto Café" in ext_page.text
    ext_csrf = re.search(r'data-csrf="([^"]+)"', ext_page.text).group(1)

    ext_pdf_resp = ext_client.get(f"/assinar/{token}/pdf")
    assert ext_pdf_resp.status_code == 200
    assert ext_pdf_resp.content.startswith(b"%PDF")

    ext_auth = ext_client.post(f"/assinar/{token}/autorizar", headers={"X-CSRF-Token": ext_csrf}, json={
        "signer_name": "Carlos Comprador",
        "cpf": "111.444.777-35",
        "channel": "whatsapp",
        "appearance": {"visible": True, "page": 0, "box": [300, 70, 520, 140], "style": "compact"},
        "consent": True,
    })
    assert ext_auth.status_code == 201, ext_auth.text
    wa_otp = re.search(r"\*(\d{8})\*", wa_sent[-1][1]).group(1)

    ext_done = ext_client.post(f"/assinar/{token}/assinar", headers={"X-CSRF-Token": ext_csrf}, json={
        "consent": True, "challenge_id": ext_auth.json()["challenge_id"], "code": wa_otp,
    })
    assert ext_done.status_code == 200, ext_done.text
    ext_body = ext_done.json()
    assert ext_body["sealed"] is True
    assert ext_body["remaining"] == 0

    # Reopening /assinar/{token} now shows already-signed screen
    reopened = ext_client.get(f"/assinar/{token}")
    assert reopened.status_code == 200
    assert "Você já assinou este documento" in reopened.text

    # Public validation shows BOTH distinct modalities side by side!
    val_html = ext_client.get(f"/validar/{ext_body['verification_code']}").text
    assert "Qualificada ICP-Brasil" in val_html
    assert "Eletrônica SiSFarm (Avançada)" in val_html
    assert "João Produtor" in val_html
    assert "Carlos Comprador" in val_html

    # Final sealed PDF has Manifesto page appended
    from app.services.document_signing.pdf import read_pdf, signing_options
    final_pdf = owner_client.get(f"{endpoint}/revisoes/{ext_body['revision']}/pdf").content
    assert signing_options(read_pdf(final_pdf))["pages"] == signing_options(read_pdf(original))["pages"] + 1


def test_superadmin_institutional_ecnpj_seal_lifecycle(authority, web_db, monkeypatch):
    from app.services.document_signing import electronic
    from app.web import digital_documents as web_dd

    make, _ = authority
    sessions, _ = web_db
    emails = []
    monkeypatch.setattr(electronic, "send_plain_email", lambda *args: emails.append(args))

    web = _make_signing_app(sessions)
    client = LocalTestClient(web)
    headers = {"X-CSRF-Token": client.get("/test-login", params={"user_id": 1, "farm_id": 1}).json()["csrf"]}

    # Regular user is blocked from /admin/selo-institucional
    monkeypatch.setattr(web_dd, "is_super_admin_email", lambda email: False)
    assert client.get("/admin/selo-institucional").status_code == 403
    assert client.post("/admin/selo-institucional/desativar", headers=headers, json={}).status_code == 403

    # Superadmin accesses /admin/selo-institucional and sees internal seal mode active
    monkeypatch.setattr(web_dd, "is_super_admin_email", lambda email: True)
    page_before = client.get("/admin/selo-institucional")
    assert page_before.status_code == 200
    assert "Selo Criptográfico Interno ativo" in page_before.text

    # Upload institutional e-CNPJ A1 certificate
    ecnpj_pfx = base64.b64encode(make(88, "SISFARM TECNOLOGIA LTDA:12345678000199").pfx).decode()
    uploaded = client.post("/admin/selo-institucional", headers=headers, json={
        "file": ecnpj_pfx,
        "password": PASSWORD,
        "consent": True,
    })
    assert uploaded.status_code == 201, uploaded.text

    page_active = client.get("/admin/selo-institucional")
    assert page_active.status_code == 200
    assert "e-CNPJ A1 Institucional ativo" in page_active.text
    assert "SISFARM TECNOLOGIA LTDA" in page_active.text

    # Signing an electronic document now seals it with institutional_ecnpj!
    original = _original()
    created = client.post("/assinatura-digital/documentos", content=original, headers={
        **headers,
        "Content-Type": "application/pdf",
        "X-Document-Name": "recibo_selado.pdf",
        "X-Document-Signing-Policy": "electronic",
    })
    assert created.status_code == 201, created.text
    endpoint = created.json()["url"]

    issued = client.post(endpoint + "/eletronica/autorizar", headers=headers, json={
        "signer_name": "João Produtor",
        "cpf": "529.982.247-25",
        "channel": "email",
        "appearance": {"visible": True, "page": 0, "box": [45, 70, 260, 140], "style": "compact"},
        "consent": True,
    })
    assert issued.status_code == 201, issued.text
    otp = re.search(r"Código de autorização: (\d{8})", emails[-1][2]).group(1)
    confirmed = client.post(endpoint + "/eletronica/assinar", headers=headers, json={
        "consent": True, "challenge_id": issued.json()["challenge_id"], "code": otp,
    })
    assert confirmed.status_code == 200, confirmed.text
    body = confirmed.json()
    assert body["sealed"] is True
    assert body["seal_mode"] == "institutional_ecnpj"

    # Public validation page shows institutional e-CNPJ seal AND keeps legal separation explicit
    val_html = client.get(f"/validar/{body['verification_code']}").text
    assert "e-CNPJ A1 Institucional SiSFarm" in val_html
    assert "Eletrônica SiSFarm (Avançada)" in val_html
    assert "não substitui nem equivale à assinatura qualificada ICP-Brasil" in val_html

    # Deactivating institutional seal returns cleanly to internal cryptographic seal
    deact = client.post("/admin/selo-institucional/desativar", headers=headers, json={})
    assert deact.status_code == 200
    page_after = client.get("/admin/selo-institucional")
    assert "Selo Criptográfico Interno ativo" in page_after.text


def test_external_signer_email_and_whatsapp_template_dispatch(web_db, monkeypatch):
    from app.services.document_signing import electronic

    sessions, settings = web_db
    emails = []
    wa_templates = []
    monkeypatch.setattr(electronic, "send_plain_email", lambda *args: emails.append(args))
    monkeypatch.setattr(electronic, "whatsapp_is_configured", lambda: True)

    class _FakeTemplateWA:
        def send_template(self, to, template_name, *, language="pt_BR", body_params=None, button_url_param=None, button_index="0"):
            wa_templates.append((to, template_name, language, body_params, button_url_param, str(button_index)))
            return {"messages": [{"id": "wamid.template.001"}]}

    monkeypatch.setattr(electronic, "WhatsAppClient", _FakeTemplateWA)

    web = _make_signing_app(sessions)
    client = LocalTestClient(web)
    headers = {"X-CSRF-Token": client.get("/test-login", params={"user_id": 1, "farm_id": 1}).json()["csrf"]}

    original = _original()
    created = client.post("/assinatura-digital/documentos", content=original, headers={
        **headers,
        "Content-Type": "application/pdf",
        "X-Document-Name": "termo_externo.pdf",
        "X-Document-Owner-Signs": "0",
        "X-Document-Signing-Policy": "electronic",
    })
    assert created.status_code == 201, created.text
    endpoint = created.json()["url"]

    # 1) External invite via email dispatches send_plain_email cleanly
    ext_email = client.post(endpoint + "/externos", headers=headers, json={
        "name": "Ana Compradora",
        "role_label": "Compradora",
        "channel": "email",
        "email": "ana@compradora.test",
    })
    assert ext_email.status_code == 201, ext_email.text
    assert ext_email.json()["notification_sent"] is True
    assert len(emails) == 1
    assert emails[0][0] == "ana@compradora.test"
    assert "/assinar/" in emails[0][2]

    # 2) External invite via WhatsApp with approved Meta template works WITHOUT 24h window
    monkeypatch.setattr(electronic, "get_settings", lambda: SimpleNamespace(
        whatsapp_signing_invite_template="sisfarm_convite_assinatura",
        whatsapp_signing_otp_template="sisfarm_codigo_assinatura",
        whatsapp_template_language="pt_BR",
    ))
    ext_wa = client.post(endpoint + "/externos", headers=headers, json={
        "name": "Pedro Fornecedor",
        "role_label": "Fornecedor",
        "channel": "whatsapp",
        "phone": "(35) 99222-3344",
    })
    assert ext_wa.status_code == 201, ext_wa.text
    assert ext_wa.json()["notification_sent"] is True
    assert len(wa_templates) == 1
    assert wa_templates[0][0] == "5535992223344"
    assert wa_templates[0][1] == "sisfarm_convite_assinatura"
    assert wa_templates[0][2] == "pt_BR"
    assert wa_templates[0][3] == ["Pedro Fornecedor", "Pessoa 1", "termo_externo"]
    token_only = ext_wa.json()["sign_url"].rsplit("/assinar/", 1)[1]
    assert wa_templates[0][4] == token_only
    assert not wa_templates[0][4].startswith("http")
    assert wa_templates[0][5] == "0"



def test_external_signer_preset_cpf_locked_channel_and_stamp_position(web_db, monkeypatch):
    from app.services.document_signing import electronic

    sessions, _ = web_db
    emails = []
    monkeypatch.setattr(electronic, "send_plain_email", lambda *args: emails.append(args))

    web = FastAPI()
    web.add_middleware(SessionMiddleware, secret_key="secret-preset-ext")
    web.include_router(router)
    web.mount("/static", StaticFiles(directory="app/static"), name="static")

    def db_dep():
        with sessions() as db:
            yield db

    def user_dep(request: Request):
        with sessions() as db:
            return db.get(User, request.session["user_id"])

    @web.get("/test-login")
    def login(request: Request, user_id: int, farm_id: int = 1):
        request.session.clear()
        request.session["user_id"] = user_id
        request.session["active_farm_id"] = farm_id
        return {"csrf": ensure_csrf_token(request)}

    web.dependency_overrides[get_db] = db_dep
    web.dependency_overrides[get_current_user_web] = user_dep

    client = LocalTestClient(web)
    headers = {"X-CSRF-Token": client.get("/test-login", params={"user_id": 1, "farm_id": 1}).json()["csrf"]}

    original = _original()
    created = client.post("/assinatura-digital/documentos", content=original, headers={
        **headers,
        "Content-Type": "application/pdf",
        "X-Document-Name": "contrato_travado.pdf",
        "X-Document-Owner-Signs": "0",
        "X-Document-Signing-Policy": "electronic",
    })
    assert created.status_code == 201, created.text
    endpoint = created.json()["url"]

    preset_box = [240.0, 80.0, 480.0, 145.0]
    ext_res = client.post(endpoint + "/externos", headers=headers, json={
        "name": "Carlos Eduardo Mendes",
        "cpf": "111.444.777-35",
        "role_label": "Comprador",
        "channel": "email",
        "email": "carlos@comprador.test",
        "appearance": {"visible": True, "style": "compact", "page": 0, "box": preset_box},
    })
    assert ext_res.status_code == 201, ext_res.text
    body = ext_res.json()
    assert body["cpf_masked"] == "•••.444.777-••"
    assert body["appearance"]["visible"] is True
    assert body["appearance"]["style"] == "compact"
    token = body["sign_url"].rsplit("/assinar/", 1)[1]

    # Public signing page renders read-only data, locked channel, read-only stamp and ZERO stamp/channel controls
    ext_client = LocalTestClient(web)
    page = ext_client.get(f"/assinar/{token}")
    assert page.status_code == 200
    assert "Carlos Eduardo Mendes" in page.text
    assert "111.444.777-35" in page.text
    assert "Somente conferência" in page.text
    assert "data-ds-ro-stamp" in page.text
    assert "data-ds-visible" not in page.text
    assert "data-ds-el-channel" not in page.text
    assert "data-ds-el-cpf" not in page.text
    ext_csrf = re.search(r'data-csrf="([^"]+)"', page.text).group(1)

    # Authorize works with ONLY consent=True and strictly ignores tampered name/cpf/channel/appearance
    auth = ext_client.post(f"/assinar/{token}/autorizar", headers={"X-CSRF-Token": ext_csrf}, json={
        "consent": True,
        "signer_name": "Outro Nome Tentativa",
        "cpf": "999.999.999-99",
        "channel": "whatsapp",
        "appearance": {"visible": False},
    })
    assert auth.status_code == 201, auth.text
    # OTP must have been sent via emitter's locked channel (email)
    otp_email = emails[-1]
    assert otp_email[0] == "carlos@comprador.test"
    otp = re.search(r"\b(\d{8})\b", otp_email[2]).group(1)

    done = ext_client.post(f"/assinar/{token}/assinar", headers={"X-CSRF-Token": ext_csrf}, json={
        "consent": True,
        "challenge_id": auth.json()["challenge_id"],
        "code": otp,
    })
    assert done.status_code == 200, done.text
    assert done.json()["sealed"] is True



def test_public_validate_direct_pdf_upload_and_styled_receipt_pdf(web_db, monkeypatch):
    from fastapi import Request
    from app.core.csrf import ensure_csrf_token
    from app.core.deps import get_current_user_web
    from app.db.session import get_db
    from app.models import User
    from app.services.document_signing import electronic
    from app.services.document_signing.pdf import read_pdf, signing_options

    sessions, _ = web_db
    emails = []
    monkeypatch.setattr(workflow, "send_plain_email", lambda *args: emails.append(args))
    monkeypatch.setattr(electronic, "send_plain_email", lambda *args: emails.append(args))

    web = _make_signing_app(sessions)

    def db_dep():
        with sessions() as db:
            yield db

    def user_dep(request: Request):
        with sessions() as db:
            return db.get(User, request.session["user_id"])

    @web.get("/test-login")
    def login(request: Request, user_id: int, farm_id: int = 1):
        request.session.clear()
        request.session["user_id"] = user_id
        request.session["active_farm_id"] = farm_id
        return {"csrf": ensure_csrf_token(request)}

    web.dependency_overrides[get_db] = db_dep
    web.dependency_overrides[get_current_user_web] = user_dep

    owner = LocalTestClient(web)
    headers = {"X-CSRF-Token": owner.get("/test-login", params={"user_id": 1, "farm_id": 1}).json()["csrf"]}

    # Cria e assina eletronicamente um documento do SiSFarm
    pdf_bytes = _original()
    created = owner.post("/assinatura-digital/documentos", content=pdf_bytes, headers={
        **headers,
        "Content-Type": "application/pdf",
        "X-Document-Name": "contrato_cafe_lote42.pdf",
        "X-Document-Title": "Contrato%20de%20Venda%20-%20Lote%20042",
        "X-Document-Signing-Policy": "electronic",
    })
    assert created.status_code == 201, created.text
    endpoint = created.json()["url"]

    issued = owner.post(endpoint + "/eletronica/autorizar", headers=headers, json={
        "signer_name": "João Produtor",
        "cpf": "529.982.247-25",
        "channel": "email",
        "appearance": {"visible": True, "page": 0, "box": [45, 70, 260, 140], "style": "full"},
        "consent": True,
    })
    assert issued.status_code == 201, issued.text
    otp = re.search(r"Código de autorização: (\d{8})", emails[-1][2]).group(1)
    confirmed = owner.post(endpoint + "/eletronica/assinar", headers=headers, json={
        "consent": True,
        "challenge_id": issued.json()["challenge_id"],
        "code": otp,
    })
    assert confirmed.status_code == 200, confirmed.text
    ver_code = confirmed.json()["verification_code"]
    sealed_pdf = owner.get(f"{endpoint}/revisoes/{confirmed.json()['revision']}/pdf").content

    # 1. Acesso público à home /validar (sem login) e upload direto do PDF selado
    anon = LocalTestClient(web)
    home = anon.get("/validar")
    assert home.status_code == 200
    assert "data-sv-upload-drop" in home.text
    pub_csrf = re.search(r'data-csrf="([^"]+)"', home.text).group(1)

    upload_ok = anon.post("/validar/arquivo", content=sealed_pdf, headers={
        "X-CSRF-Token": pub_csrf,
        "Content-Type": "application/pdf",
        "X-Document-Name": "contrato_cafe_lote42.pdf",
    })
    assert upload_ok.status_code == 200, upload_ok.text
    ok_data = upload_ok.json()
    assert ok_data["tone"] == "valid"
    assert ok_data["code"] == ver_code
    assert ok_data["headline"] == "Documento autêntico"
    assert ok_data["receipt_url"] == f"/validar/{ver_code}/comprovante.pdf"
    assert len(ok_data["signers"]) == 1

    # 2. Upload direto de um PDF comum sem assinatura
    upload_unsigned = anon.post("/validar/arquivo", content=pdf_bytes, headers={
        "X-CSRF-Token": pub_csrf,
        "Content-Type": "application/pdf",
        "X-Document-Name": "rascunho_sem_assinatura.pdf",
    })
    assert upload_unsigned.status_code == 200, upload_unsigned.text
    uns_data = upload_unsigned.json()
    assert uns_data["tone"] == "unsigned"
    assert uns_data["signers"] == []

    # 3. Download do Comprovante PDF estilizado via código (/validar/{code}/comprovante.pdf)
    receipt_by_code = anon.get(f"/validar/{ver_code}/comprovante.pdf")
    assert receipt_by_code.status_code == 200
    assert receipt_by_code.headers["content-type"].startswith("application/pdf")
    assert receipt_by_code.content.startswith(b"%PDF-")
    assert signing_options(read_pdf(receipt_by_code.content))["pages"] == 1

    # 4. Download do Comprovante PDF estilizado via POST (/validar/comprovante.pdf)
    receipt_by_payload = anon.post("/validar/comprovante.pdf", headers={
        "X-CSRF-Token": pub_csrf,
    }, json=uns_data)
    assert receipt_by_payload.status_code == 200
    assert receipt_by_payload.headers["content-type"].startswith("application/pdf")
    assert receipt_by_payload.content.startswith(b"%PDF-")
    assert signing_options(read_pdf(receipt_by_payload.content))["pages"] == 1



def test_whatsapp_invite_webhook_statuses_bot_filtering_and_duplicate_prevention(web_db, monkeypatch):
    import hashlib
    import hmac
    import json
    from app.models.signed_document import SigningExternalSigner
    from app.routers import whatsapp_webhook
    from app.services import whatsapp as wa_svc
    from app.services.document_signing import electronic

    sessions, _ = web_db
    sent_payloads = []

    class _FakeHttp:
        def post(self, url, json=None, headers=None):
            sent_payloads.append((url, json))
            return SimpleNamespace(
                status_code=200,
                json=lambda: {"messaging_product": "whatsapp", "messages": [{"id": "wamid.meta.invite.123"}]},
            )

    monkeypatch.setattr(electronic, "whatsapp_is_configured", lambda: True)
    monkeypatch.setattr(
        electronic,
        "WhatsAppClient",
        lambda: wa_svc.WhatsAppClient.__new__(wa_svc.WhatsAppClient) if False else _build_real_wa_client(_FakeHttp()),
    )

    def _build_real_wa_client(http):
        c = wa_svc.WhatsAppClient(http=http)
        c.token = "tok-test"
        c.phone_number_id = "100200300"
        return c

    web = _make_signing_app(sessions)
    web.include_router(whatsapp_webhook.router)
    monkeypatch.setattr(whatsapp_webhook, "SESSION_FACTORY", sessions)
    monkeypatch.setattr(
        whatsapp_webhook,
        "get_settings",
        lambda: SimpleNamespace(whatsapp_app_secret="sec-webhook-123", whatsapp_phone_number_id="100200300"),
    )
    monkeypatch.setattr(
        wa_svc,
        "get_settings",
        lambda: SimpleNamespace(
            whatsapp_access_token="tok-test",
            whatsapp_phone_number_id="100200300",
            whatsapp_graph_version="v23.0",
            whatsapp_app_secret="sec-webhook-123",
        ),
    )

    client = LocalTestClient(web)
    headers = {"X-CSRF-Token": client.get("/test-login", params={"user_id": 1, "farm_id": 1}).json()["csrf"]}

    created = client.post("/assinatura-digital/documentos", content=_original(), headers={
        **headers,
        "Content-Type": "application/pdf",
        "X-Document-Name": "contrato_wa.pdf",
        "X-Document-Title": "Contrato%20Safra%202026",
        "X-Document-Owner-Signs": "0",
        "X-Document-Signing-Policy": "electronic",
    })
    assert created.status_code == 201, created.text
    endpoint = created.json()["url"]

    # 1) Create external signer with default approved template (outside 24h window!)
    ext = client.post(endpoint + "/externos", headers=headers, json={
        "name": "Marcos Comprador",
        "cpf": "111.444.777-35",
        "role_label": "Comprador",
        "channel": "whatsapp",
        "phone": "(35) 99876-5432",
    })
    assert ext.status_code == 201, ext.text
    ext_data = ext.json()
    assert ext_data["notification_sent"] is True
    assert ext_data["whatsapp_share_url"].startswith("https://wa.me/5535998765432")
    signer_id = ext_data["signer_id"]
    token = ext_data["sign_url"].rsplit("/assinar/", 1)[1]

    # Verify exact Meta Cloud API payload (3 body variables + URL button index 0 with token only)
    assert len(sent_payloads) == 1
    _, body = sent_payloads[0]
    assert body["type"] == "template"
    assert body["template"]["name"] == "sisfarm_convite_assinatura"
    assert body["template"]["language"]["code"] == "pt_BR"
    comps = body["template"]["components"]
    assert comps[0] == {
        "type": "body",
        "parameters": [
            {"type": "text", "text": "Marcos Comprador"},
            {"type": "text", "text": "Pessoa 1"},
            {"type": "text", "text": "Contrato Safra 2026"},
        ],
    }
    assert comps[1] == {
        "type": "button",
        "sub_type": "url",
        "index": "0",
        "parameters": [{"type": "text", "text": token}],
    }

    # 2) Duplicate prevention: creating the same pending recipient again is blocked; immediate resend within 30s is blocked
    dup = client.post(endpoint + "/externos", headers=headers, json={
        "name": "Marcos Comprador",
        "cpf": "111.444.777-35",
        "role_label": "Comprador",
        "channel": "whatsapp",
        "phone": "35998765432",
    })
    assert dup.status_code == 422
    assert "já recebeu um convite" in dup.text

    fast_resend = client.post(f"{endpoint}/externos/{signer_id}/reenviar", headers=headers)
    assert fast_resend.status_code == 422
    assert "poucos segundos" in fast_resend.text

    # 3) Webhook records sent -> delivered -> read (and ignores out-of-order sent after read)
    def _post_wh(payload_dict):
        raw = json.dumps(payload_dict).encode("utf-8")
        sig = "sha256=" + hmac.new(b"sec-webhook-123", raw, hashlib.sha256).hexdigest()
        return client.post("/api/webhooks/whatsapp", content=raw, headers={"X-Hub-Signature-256": sig, "Content-Type": "application/json"})

    wh_res = _post_wh({
        "object": "whatsapp_business_account",
        "entry": [{
            "changes": [{
                "field": "messages",
                "value": {
                    "metadata": {"phone_number_id": "100200300"},
                    "statuses": [
                        {"id": "wamid.meta.invite.123", "status": "delivered", "timestamp": "1732700000", "recipient_id": "5535998765432"},
                        {"id": "wamid.meta.invite.123", "status": "read", "timestamp": "1732700120", "recipient_id": "5535998765432"},
                        {"id": "wamid.meta.invite.123", "status": "sent", "timestamp": "1732699900", "recipient_id": "5535998765432"},
                    ],
                },
            }],
        }],
    })
    assert wh_res.status_code == 200

    with sessions() as db:
        row = db.get(SigningExternalSigner, signer_id)
        assert row.whatsapp_status == "read"
        assert row.whatsapp_sent_at is not None
        assert row.whatsapp_delivered_at is not None
        assert row.whatsapp_read_at is not None

    # 4) Bot accesses (WhatsApp previewer, Facebook crawler, prefetch) MUST NOT count as document access
    pub = LocalTestClient(web)
    for ua, extra in [
        ("WhatsApp/2.23.20.0 A", {}),
        ("facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)", {}),
        ("Mozilla/5.0", {"Sec-Purpose": "prefetch"}),
        ("", {}),
    ]:
        r = pub.get(f"/assinar/{token}", headers={"User-Agent": ua, **extra})
        assert r.status_code == 200

    with sessions() as db:
        row = db.get(SigningExternalSigner, signer_id)
        assert row.first_accessed_at is None
        assert row.last_accessed_at is None
        assert row.access_count == 0

    # 5) Real browser accesses DO record first_accessed_at, last_accessed_at and access_count
    real_ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15"
    assert pub.get(f"/assinar/{token}", headers={"User-Agent": real_ua}).status_code == 200
    assert pub.get(f"/assinar/{token}", headers={"User-Agent": real_ua}).status_code == 200
    with sessions() as db:
        row = db.get(SigningExternalSigner, signer_id)
        assert row.first_accessed_at is not None
        assert row.last_accessed_at is not None
        assert row.access_count == 2


def test_workspace_signers_tab_remind_change_channel_and_copy_link(web_db, monkeypatch):
    from app.services.document_signing import electronic
    from app.web import digital_documents as dd_web

    sessions, _settings = web_db
    ext_emails, team_emails = [], []
    monkeypatch.setattr(electronic, "send_plain_email", lambda *args: ext_emails.append(args))
    monkeypatch.setattr(dd_web, "send_plain_email", lambda *args: team_emails.append(args))
    monkeypatch.setattr(electronic, "whatsapp_is_configured", lambda: False)

    client = LocalTestClient(_make_signing_app(sessions))
    headers = {"X-CSRF-Token": client.get("/test-login", params={"user_id": 1, "farm_id": 1}).json()["csrf"]}
    created = client.post("/assinatura-digital/documentos", content=_original(), headers={
        **headers, "Content-Type": "application/pdf", "X-Document-Name": "contrato_cafe.pdf",
        "X-Document-Owner-Signs": "0", "X-Document-Signing-Policy": "electronic",
    })
    assert created.status_code == 201, created.text
    endpoint = created.json()["url"]

    assert client.post(endpoint + "/participantes", headers=headers, json={
        "email": "conta2@example.test", "consent": True,
    }).status_code == 200
    ext = client.post(endpoint + "/externos", headers=headers, json={
        "name": "João Batista", "role_label": "Comprador", "channel": "email", "email": "joao@comprador.test",
    })
    assert ext.status_code == 201, ext.text
    signer_id = ext.json()["signer_id"]
    first_url = ext.json()["sign_url"]

    page = client.get(endpoint + "?aba=detalhes").text
    assert "Assinaturas e histórico" in page
    assert "0 de 2</strong> assinou" in page
    assert "João Batista" in page and "Comprador · de fora" in page
    assert "Trocar para WhatsApp" in page
    assert "data-dd-remind=\"team\"" in page and "data-dd-remind=\"external\"" in page
    assert "Convite enviado para João Batista por e-mail" in page
    assert "Detalhes, participantes e versões" not in page

    # Lembrar alguém da equipe
    team_emails.clear()
    remind = client.post(endpoint + "/participantes/2/lembrar", headers=headers)
    assert remind.status_code == 200, remind.text
    assert remind.json()["notification_sent"] is True
    assert team_emails and team_emails[0][0] == "conta2@example.test"
    assert client.post(endpoint + "/participantes/99/lembrar", headers=headers).status_code == 404

    # Copiar link: gera link novo sem mandar mensagem
    ext_emails.clear()
    only_link = client.post(f"{endpoint}/externos/{signer_id}/reenviar?somente_link=1", headers=headers)
    assert only_link.status_code == 200, only_link.text
    assert only_link.json()["notification_sent"] is False
    assert "/assinar/" in only_link.json()["sign_url"] and only_link.json()["sign_url"] != first_url
    assert ext_emails == []

    # Trocar canal: exige dado válido e reenvia pelo novo canal
    bad = client.post(f"{endpoint}/externos/{signer_id}/canal", headers=headers, json={"channel": "email", "email": "x"})
    assert bad.status_code == 400 or bad.status_code == 422
    changed = client.post(f"{endpoint}/externos/{signer_id}/canal", headers=headers, json={
        "channel": "email", "email": "Novo@Comprador.test",
    })
    assert changed.status_code == 200, changed.text
    assert changed.json()["notification_sent"] is True
    assert ext_emails[-1][0] == "novo@comprador.test"

    # Quem não é dono não consegue lembrar nem trocar canal
    headers2 = {"X-CSRF-Token": client.get("/test-login", params={"user_id": 2, "farm_id": 1}).json()["csrf"]}
    assert client.post(endpoint + "/participantes/2/lembrar", headers=headers2).status_code in (403, 404)
    other_view = client.get(endpoint + "?aba=detalhes").text
    assert "data-dd-remind" not in other_view and "data-dd-open-add" not in other_view


def test_whatsapp_otp_auth_template_countdown_resend_invalidation_abuse_and_fallback(web_db, monkeypatch, caplog):
    import logging
    from datetime import datetime, timedelta, timezone
    from sqlalchemy import update
    from app.models.signed_document import SigningElectronicChallenge
    from app.services import whatsapp as wa_svc
    from app.services.document_signing import electronic

    sessions, _settings = web_db
    emails = []
    sent_payloads = []
    fail_whatsapp = {"value": False}

    monkeypatch.setattr(electronic, "send_plain_email", lambda *args: emails.append(args))
    monkeypatch.setattr(electronic, "whatsapp_is_configured", lambda: True)

    def _fake_post(self, payload):
        if fail_whatsapp["value"]:
            raise wa_svc.WhatsAppError("Meta temporary error", code=131000)
        sent_payloads.append(payload)
        return {"messages": [{"id": f"wamid.otp.{len(sent_payloads)}"}]}

    monkeypatch.setattr(wa_svc.WhatsAppClient, "_post", _fake_post)

    client = LocalTestClient(_make_signing_app(sessions))
    headers = {"X-CSRF-Token": client.get("/test-login", params={"user_id": 1, "farm_id": 1}).json()["csrf"]}

    created = client.post("/assinatura-digital/documentos", content=_original(), headers={
        **headers,
        "Content-Type": "application/pdf",
        "X-Document-Name": "termo_otp_wa.pdf",
        "X-Document-Owner-Signs": "0",
        "X-Document-Signing-Policy": "electronic",
    })
    assert created.status_code == 201, created.text
    endpoint = created.json()["url"]

    # Cria signatário externo via WhatsApp (sem janela 24h) com e-mail cadastrado como alternativa
    ext = client.post(endpoint + "/externos", headers=headers, json={
        "name": "Carlos Produtor",
        "role_label": "Produtor",
        "channel": "whatsapp",
        "phone": "(35) 99222-3344",
        "email": "carlos@produtor.test",
        "cpf": "529.982.247-25",
    })
    assert ext.status_code == 201, ext.text
    token = ext.json()["sign_url"].rsplit("/assinar/", 1)[1]

    # 1) Página pública NÃO exibe mais instrução de enviar "Oi" e exibe controles de validade/reenvio
    pub_client = LocalTestClient(_make_signing_app(sessions))
    page = pub_client.get(f"/assinar/{token}")
    assert page.status_code == 200
    assert 'envie primeiro um "Oi"' not in page.text
    assert "Abrir WhatsApp do SiSFarm" not in page.text
    assert "data-ds-otp-expiry" in page.text
    assert "data-ds-otp-resend" in page.text
    assert "data-ds-otp-sent-banner" in page.text
    assert "(35) 9****-3344" in page.text
    pub_csrf = re.search(r'data-csrf="([^"]+)"', page.text).group(1)
    pub_headers = {"X-CSRF-Token": pub_csrf}

    # 2) Dispara OTP via template oficial Authentication (sisfarm_codigo_assinatura) fora da janela 24h
    sent_payloads.clear()
    with caplog.at_level(logging.DEBUG):
        auth1 = pub_client.post(f"/assinar/{token}/autorizar", headers=pub_headers, json={"consent": True})
    assert auth1.status_code == 201, auth1.text
    a1 = auth1.json()
    assert a1["channel"] == "whatsapp"
    assert a1["destination_masked"] == "(35) 9****-3344"
    assert a1["expires_minutes"] == 10
    assert a1["expires_seconds"] == 600
    assert a1["resend_after_seconds"] == 60
    assert a1["fallback_email_available"] is True
    assert len(sent_payloads) == 1
    tpl = sent_payloads[0]["template"]
    assert tpl["name"] == "sisfarm_codigo_assinatura"
    assert tpl["language"] == {"code": "pt_BR"}
    assert len(tpl["components"]) == 2
    assert tpl["components"][0]["type"] == "body"
    code1 = tpl["components"][0]["parameters"][0]["text"]
    assert re.fullmatch(r"\d{8}", code1)
    assert tpl["components"][1] == {
        "type": "button",
        "sub_type": "url",
        "index": "0",
        "parameters": [{"type": "text", "text": code1}],
    }
    # Garante que o código NUNCA aparece nos logs
    assert code1 not in caplog.text

    # 3) Reenvio antes de 60s é bloqueado e NÃO invalida o código ativo
    too_soon = pub_client.post(f"/assinar/{token}/autorizar", headers=pub_headers, json={"consent": True})
    assert too_soon.status_code == 422
    assert "segundos" in too_soon.json()["detail"]

    # 4) Após 60s, reenvio funciona e INVALIDA o código anterior
    with sessions() as db:
        db.execute(
            update(SigningElectronicChallenge)
            .where(SigningElectronicChallenge.id == a1["challenge_id"])
            .values(created_at=datetime.now(timezone.utc) - timedelta(seconds=65))
        )
        db.commit()

    auth2 = pub_client.post(f"/assinar/{token}/autorizar", headers=pub_headers, json={"consent": True})
    assert auth2.status_code == 201, auth2.text
    a2 = auth2.json()
    code2 = sent_payloads[-1]["template"]["components"][0]["parameters"][0]["text"]
    assert code2 != code1 or a2["challenge_id"] != a1["challenge_id"]

    # Código anterior está invalidado
    old_try = pub_client.post(f"/assinar/{token}/assinar", headers=pub_headers, json={
        "consent": True, "challenge_id": a1["challenge_id"], "code": code1,
    })
    assert old_try.status_code == 422

    # 5) Limite de tentativas incorretas (5 tentativas) bloqueia o challenge
    for attempt in range(5):
        wrong = pub_client.post(f"/assinar/{token}/assinar", headers=pub_headers, json={
            "consent": True, "challenge_id": a2["challenge_id"], "code": "00000000" if code2 != "00000000" else "99999999",
        })
        assert wrong.status_code == 403
        if attempt < 4:
            assert "tentativa" in wrong.json()["detail"]
        else:
            assert "Limite de tentativas" in wrong.json()["detail"]

    # Mesmo com código correto depois de 5 erros, não aceita mais
    after_lock = pub_client.post(f"/assinar/{token}/assinar", headers=pub_headers, json={
        "consent": True, "challenge_id": a2["challenge_id"], "code": code2,
    })
    assert after_lock.status_code == 422

    # 6) Falha de entrega no WhatsApp -> mensagem amigável sem vazar código + fallback por e-mail funciona!
    with sessions() as db:
        db.execute(
            update(SigningElectronicChallenge)
            .where(SigningElectronicChallenge.id == a2["challenge_id"])
            .values(created_at=datetime.now(timezone.utc) - timedelta(seconds=65))
        )
        db.commit()

    fail_whatsapp["value"] = True
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        failed_wa = pub_client.post(f"/assinar/{token}/autorizar", headers=pub_headers, json={"consent": True})
    assert failed_wa.status_code == 422
    assert "e-mail" in failed_wa.json()["detail"].lower()
    assert "Meta temporary error" not in failed_wa.json()["detail"]

    # Avança cooldown e usa alternativa de e-mail
    with sessions() as db:
        db.execute(
            update(SigningElectronicChallenge)
            .values(created_at=datetime.now(timezone.utc) - timedelta(seconds=65))
        )
        db.commit()

    emails.clear()
    fallback = pub_client.post(f"/assinar/{token}/autorizar", headers=pub_headers, json={"consent": True, "channel": "email"})
    assert fallback.status_code == 201, fallback.text
    assert fallback.json()["channel"] == "email"
    assert len(emails) == 1
    email_code = re.search(r"Código de autorização: (\d{8})", emails[0][2]).group(1)

    # 7) Proteção contra envios abusivos por hora (MAX_CHALLENGES_PER_HOUR = 6)
    with sessions() as db:
        now_utc = datetime.now(timezone.utc)
        db.execute(
            update(SigningElectronicChallenge)
            .values(created_at=now_utc - timedelta(minutes=2))
        )
        # Completa até 6 challenges na última hora
        existing_cnt = len(db.scalars(select(SigningElectronicChallenge.id)).all())
        base_ch = db.scalar(select(SigningElectronicChallenge).limit(1))
        for i in range(max(0, electronic.MAX_CHALLENGES_PER_HOUR - existing_cnt)):
            db.add(SigningElectronicChallenge(
                id=f"abuse-ch-{i}",
                document_id=base_ch.document_id,
                revision_id=base_ch.revision_id,
                revision_sha256=base_ch.revision_sha256,
                external_signer_id=base_ch.external_signer_id,
                signer_name=base_ch.signer_name,
                cpf_masked=base_ch.cpf_masked,
                cpf_encrypted=base_ch.cpf_encrypted,
                cpf_sha256=base_ch.cpf_sha256,
                auth_channel="whatsapp",
                auth_destination_masked="(35) 9****-3344",
                appearance={"visible": False},
                consent_version="v1",
                consent_text_sha256=base_ch.consent_text_sha256,
                code_hash=base_ch.code_hash,
                expires_at=now_utc + timedelta(minutes=10),
                created_at=now_utc - timedelta(minutes=5),
                consumed_at=now_utc - timedelta(minutes=4),
            ))
        db.commit()

    abuse_blocked = pub_client.post(f"/assinar/{token}/autorizar", headers=pub_headers, json={"consent": True, "channel": "email"})
    assert abuse_blocked.status_code == 422
    assert "Limite de envios" in abuse_blocked.json()["detail"]

    # 8) Conclui assinatura com o código válido recebido via fallback por e-mail
    signed = pub_client.post(f"/assinar/{token}/assinar", headers=pub_headers, json={
        "consent": True, "challenge_id": fallback.json()["challenge_id"], "code": email_code,
    })
    assert signed.status_code == 200, signed.text
    assert signed.json()["sealed"] is True
