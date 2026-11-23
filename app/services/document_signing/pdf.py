"""PDF inspection, incremental PAdES signing and independently verifiable results."""
import asyncio
import math
from dataclasses import replace
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
from uuid import uuid4

from pyhanko.keys.internal import translate_pyca_cryptography_cert_to_asn1, translate_pyca_cryptography_key_to_asn1
from pyhanko.pdf_utils import generic
from pyhanko.pdf_utils.extensions import DeveloperExtension, DevExtensionMultivalued
from pyhanko.pdf_utils.incremental_writer import IncrementalPdfFileWriter
from pyhanko.pdf_utils.reader import PdfFileReader
from pyhanko.sign import fields, signers
from pyhanko.sign.ades.api import CAdESSignedAttrSpec
from pyhanko.sign.general import SigningError
from pyhanko.sign.validation.settings import KeyUsageConstraints
from pyhanko.sign.signers.cms_embedder import SigObjSetup
from pyhanko.sign.validation import DocumentSecurityStore, PdfSignatureStatus, async_validate_pdf_signature
from pyhanko.sign.validation.generic_cms import cms_basic_validation, collect_timing_info
from pyhanko.sign.validation.pdf_embedded import EmbeddedPdfSignature, report_seed_value_validation
from pyhanko.sign.validation.status import SignatureCoverageLevel
from pyhanko.stamp import TextStampStyle
from pyhanko_certvalidator.errors import RevokedError
from pyhanko_certvalidator.registry import SimpleCertificateStore

from app.services.document_signing.policy import FILTER, SUBFILTER, POLICY_OID, policy_identifier
from app.services.document_signing.trust import validation_context

MAX_UPLOAD_BYTES = 12 * 1024 * 1024
MAX_PDF_BYTES = 16 * 1024 * 1024
MAX_PAGES = 120
KEY_USAGE = KeyUsageConstraints(key_usage={"digital_signature", "non_repudiation"})
STAMP = TextStampStyle(
    stamp_text="Assinado digitalmente\n%(signer)s",
    border_width=1,
)
PAdES_POLICY_EXTENSION = DeveloperExtension(
    prefix_name=generic.pdf_name("/ESIC"), base_version=generic.pdf_name("/1.7"), extension_level=2,
    compare_by_level=True, multivalued=DevExtensionMultivalued.NEVER,
)
COMPLETE_COVERAGE = {SignatureCoverageLevel.ENTIRE_FILE, SignatureCoverageLevel.ENTIRE_REVISION}


def read_pdf(data: bytes) -> PdfFileReader:
    if not data.startswith(b"%PDF-") or len(data) > MAX_PDF_BYTES:
        raise ValueError("Envie um arquivo PDF válido dentro do limite de tamanho.")
    try:
        reader = PdfFileReader(BytesIO(data), strict=True)
        if reader.security_handler is not None:
            raise ValueError("PDF protegido por senha ou criptografia.")
        page_count = int(reader.root["/Pages"]["/Count"])
        if not 0 < page_count <= MAX_PAGES:
            raise ValueError(f"O PDF precisa ter entre 1 e {MAX_PAGES} páginas.")
        if reader.total_revisions > 120:
            raise ValueError("O PDF tem revisões demais para ser processado.")
        return reader
    except (ValueError, TypeError, KeyError, OverflowError, RecursionError) as exc:
        if isinstance(exc, ValueError) and str(exc).startswith(("PDF protegido", "O PDF")):
            raise
        raise ValueError("Não foi possível ler a estrutura do PDF.") from exc
    except Exception as exc:
        raise ValueError("Não foi possível ler a estrutura do PDF.") from exc


def _signatures(reader):
    for name, _, ref in fields.enumerate_sig_fields(reader, filled_status=True):
        yield name, ref


def _field_lock(signature):
    lock = signature.fieldmdp
    if lock is None:
        field = signature.reader.get_historical_resolver(signature.signed_revision)
        # The signature's /Reference is authoritative; a signed /Lock is also a restriction.
        matches = fields.enumerate_sig_fields(field, filled_status=True, with_name=signature.fq_name)
        item = next(matches, None)
        if item is not None:
            lock_dict = item[2].get_object().get("/Lock")
            if lock_dict is not None:
                lock = fields.FieldMDPSpec.from_pdf_object(lock_dict.get_object())
    return lock


def _appearance_for_field(reader, ref):
    obj = ref.get_object()
    kids = obj.get("/Kids")
    widget = kids[0].get_object() if kids and len(kids) == 1 else obj
    if kids and len(kids) != 1:
        raise ValueError("O campo de assinatura possui mais de uma área visual.")
    rect = widget.get("/Rect", [0, 0, 0, 0])
    box = _rect(rect)
    if box[2] == box[0] or box[3] == box[1]:
        return None
    page = widget.raw_get("/P") if "/P" in widget else None
    for index in range(int(reader.root["/Pages"]["/Count"])):
        page_ref, _ = reader.find_page_for_modification(index)
        page_obj = page_ref.get_object()
        if page is not None and getattr(page, "reference", None) == page_ref.reference:
            return {"page": index, "box": box}
        if any(getattr(item, "reference", None) == widget.container_ref for item in page_obj.get("/Annots", ())):
            return {"page": index, "box": box}
    raise ValueError("O campo de assinatura tem uma posição que não pôde ser verificada.")


def _eligible(reader, signatures):
    acroform = reader.root.get("/AcroForm")
    names = reader.root.get("/Names")
    if (acroform is not None and "/XFA" in acroform.get_object()
            or names is not None and "/JavaScript" in names.get_object()):
        return False, []
    if any(sig.docmdp_level == fields.MDPPerm.NO_CHANGES for sig in signatures):
        return False, []
    locks = [lock for sig in signatures if (lock := _field_lock(sig)) is not None]
    free_position = not any(sig.docmdp_level is not None for sig in signatures)
    free_position = free_position and not any(lock.is_locked("SiSFarm#" + "0" * 32) for lock in locks)
    eligible = []
    for name, _, ref in fields.enumerate_sig_fields(reader, filled_status=False):
        if any(lock.is_locked(name) for lock in locks):
            continue
        try:
            _check_seed(ref.get_object())
            appearance = _appearance_for_field(reader, ref)
        except (ValueError, TypeError):
            continue
        eligible.append({"name": name, "location": appearance})
    return free_position, eligible


def signing_options(reader):
    try:
        signatures = [EmbeddedPdfSignature(reader, ref, name) for name, ref in _signatures(reader)]
        free, eligible = _eligible(reader, signatures)
    except Exception:
        free, eligible = False, []
    return {"pages": int(reader.root["/Pages"]["/Count"]), "free_position": free, "fields": eligible}


def _rect(values):
    x1, y1, x2, y2 = [float(number) for number in values]
    if any(not math.isfinite(v) or abs(v) > 1_000_000 for v in (x1, y1, x2, y2)):
        raise ValueError("Posição da assinatura inválida.")
    return [min(x1, x2), min(y1, y2), max(x1, x2), max(y1, y2)]


def _page_box(reader, page):
    ref, _ = reader.find_page_for_modification(page)
    media = crop = None
    visited = set()
    while ref is not None and ref.reference not in visited:
        visited.add(ref.reference)
        current = ref.get_object()
        if crop is None:
            crop = current.get("/CropBox")
        if media is None:
            media = current.get("/MediaBox")
        ref = current.raw_get("/Parent") if "/Parent" in current else None
    if media is None:
        raise ValueError("A página não possui dimensões válidas.")
    bounds = _rect(media)
    if crop is not None:
        visible = _rect(crop)
        bounds = [max(bounds[0], visible[0]), max(bounds[1], visible[1]),
                  min(bounds[2], visible[2]), min(bounds[3], visible[3])]
    return bounds


def _check_seed(field):
    seed_ref = field.get("/SV")
    if seed_ref is None:
        return
    seed = seed_ref.get_object()
    timestamp = seed.get("/TimeStamp")
    if timestamp is not None and int(timestamp.get_object().get("/Ff", 0)) & 1:
        raise ValueError("O campo exige carimbo de tempo.")
    flags = fields.SigSeedValFlags(seed.get("/Ff", 0))
    if (flags & fields.SigSeedValFlags.FILTER and seed.get("/Filter") != FILTER
            or flags & fields.SigSeedValFlags.SUBFILTER and SUBFILTER not in seed.get("/SubFilter", ())):
        raise ValueError("O campo exige um formato de assinatura incompatível com AD-RB.")
    # pyHanko's seed parser supports standard PAdES names. They are not a substitute
    # for a mandatory ICP-Brasil PBAD seed value.
    if flags & fields.SigSeedValFlags.SUBFILTER:
        raise ValueError("O campo possui um perfil obrigatório que o assinador ainda não pode aplicar.")


def check_appearance(reader, appearance):
    info = signing_options(reader)
    field_name = appearance.get("field")
    if field_name:
        if not any(field["name"] == field_name for field in info["fields"]):
            raise ValueError("O campo de assinatura está bloqueado ou é incompatível.")
        return {"field": field_name}
    if not info["free_position"]:
        raise ValueError("O PDF restringe novos campos de assinatura. Use um campo permitido do documento.")
    if not appearance.get("visible"):
        return {"visible": False}
    page = appearance.get("page")
    if type(page) is not int or not 0 <= page < info["pages"]:
        raise ValueError("Escolha uma página válida.")
    try:
        rect = [round(value, 2) for value in _rect(appearance["box"])]
    except (KeyError, TypeError, ValueError):
        raise ValueError("Posição da assinatura inválida.") from None
    left, bottom, right, top = _page_box(reader, page)
    x1, y1, x2, y2 = rect
    if (x2 - x1 < 28 or y2 - y1 < 20
            or not (left - .02 <= x1 < x2 <= right + .02 and bottom - .02 <= y1 < y2 <= top + .02)):
        raise ValueError("Posicione a assinatura dentro dos limites visíveis da página.")
    return {"visible": True, "page": page, "box": rect}


class _PbadCmsWriter:
    """Translates the PDF dictionary before pyHanko hashes the revision."""

    def __init__(self, writer, field_name):
        self.writer = writer
        self.field_name = field_name

    def send(self, value):
        if isinstance(value, SigObjSetup):
            placeholder = value.sig_placeholder
            placeholder[generic.pdf_name("/Filter")] = FILTER
            placeholder[generic.pdf_name("/SubFilter")] = SUBFILTER
            if value.mdp_setup.field_lock is None:
                lock = fields.FieldMDPSpec(fields.FieldMDPAction.INCLUDE, [self.field_name])
                value = replace(value, mdp_setup=replace(value.mdp_setup, field_lock=lock))
        return self.writer.send(value)


class _PbadSigner(signers.PdfSigner):
    def register_extensions(self, pdf_out, *, md_algorithm):
        super().register_extensions(pdf_out, md_algorithm=md_algorithm)
        pdf_out.register_extension(PAdES_POLICY_EXTENSION)

    def init_signing_session(self, pdf_out, existing_fields_only=False):
        session = super().init_signing_session(pdf_out, existing_fields_only)
        session.cms_writer = _PbadCmsWriter(session.cms_writer, self.signature_meta.field_name)
        return session


def a1_signer(material):
    store = SimpleCertificateStore()
    store.register_multiple(map(translate_pyca_cryptography_cert_to_asn1, material["chain"]))
    return signers.SimpleSigner(
        translate_pyca_cryptography_cert_to_asn1(material["certificate"]),
        translate_pyca_cryptography_key_to_asn1(material["private_key"]),
        cert_registry=store,
        embed_roots=False,
    )


async def _validate_signature(sig, context, timestamp_context=None):
    if sig.sig_object.get("/SubFilter") != SUBFILTER:
        return await async_validate_pdf_signature(sig, signer_validation_context=context,
                                                  ts_validation_context=timestamp_context,
                                                  key_usage_settings=KEY_USAGE)

    sig.compute_integrity_info()
    status = sig.summarise_integrity_info()
    status.update(await collect_timing_info(sig.signer_info, timestamp_context or context, raw_digest=sig.compute_digest))
    status = await cms_basic_validation(
        sig.signed_data, raw_digest=sig.compute_digest(), validation_context=context,
        status_kwargs=status, key_usage_settings=KEY_USAGE,
    )
    timestamp = status.get("timestamp_validity")
    status.update(report_seed_value_validation(sig, status["validation_path"], bool(timestamp and timestamp.trusted)))
    return PdfSignatureStatus(**status)


def _status_attributes(sig):
    try:
        cert = sig.signer_cert
        return {
            "holder": cert.subject.native.get("common_name") or cert.subject.human_friendly,
            "issuer": cert.issuer.native.get("common_name") or cert.issuer.human_friendly,
            "fingerprint": sha256(cert.dump()).hexdigest(),
            "valid_from": cert.not_valid_before.isoformat(),
            "valid_until": cert.not_valid_after.isoformat(),
        }
    except Exception:
        return {}


def _policy_oid(sig):
    for attr in sig.signer_info["signed_attrs"]:
        if attr["type"].dotted == "1.2.840.113549.1.9.16.2.15":
            identifier = attr["values"][0]
            if identifier.name == "signature_policy_id":
                return identifier.chosen["sig_policy_id"].dotted
    return None


def _proof_of_integrity(status):
    return status.valid and status.intact and status.coverage in COMPLETE_COVERAGE and status.docmdp_ok is not False


async def _validate_one(sig, context, dss):
    status = await _validate_signature(sig, context)
    timestamp = status.timestamp_validity
    certified_time = timestamp.timestamp if timestamp and timestamp.valid and timestamp.intact and timestamp.trusted else None
    if certified_time and not status.trusted:
        historical_context = validation_context(dss=dss, fetch=False, moment=certified_time)
        status = await _validate_signature(sig, historical_context, context)
    # A later revocation alone does not establish the state at a proven earlier time.
    problem = getattr(status.trust_problem_indic, "name", "")
    if not _proof_of_integrity(status):
        state, label = "invalid", "Integridade ou restrições do PDF violadas."
    elif status.seed_value_constraint_error:
        state, label = "invalid", "A assinatura viola as exigências do campo do PDF."
    elif status.trusted:
        state, label = "valid", "Integridade e cadeia ICP-Brasil verificadas."
    elif problem in {"REVOKED", "REVOKED_NO_POE", "REVOKED_CA_NO_POE"}:
        state = "indeterminate" if certified_time or problem.endswith("_NO_POE") else "invalid"
        label = "Há evidência de revogação no caminho de certificação."
    elif problem.startswith("OUT_OF_BOUNDS"):
        state, label = "indeterminate", "Vigência do certificado sem evidência temporal suficiente."
    else:
        state, label = "indeterminate", "Não foi possível comprovar a cadeia ou a revogação."
    return {
        "state": state, "message": label, "field": sig.fq_name, "revision": sig.signed_revision + 1,
        "integrity": _proof_of_integrity(status), "chain": bool(status.validation_path),
        "revocation": "checked" if status.trusted else "revoked" if problem.startswith("REVOKED") else "unknown",
        "coverage": status.coverage.name if status.coverage else None,
        "signed_at": certified_time.isoformat() if certified_time else None,
        "policy": _policy_oid(sig), **_status_attributes(sig),
    }


async def _report(reader, *, network=True, context=None):
    try:
        dss = DocumentSecurityStore.read_dss(reader)
    except Exception:
        dss = None
    if context is None:
        try:
            context = validation_context(dss=dss, fetch=network)
        except Exception:
            dss = None
            context = validation_context(fetch=network)
    signatures = []
    try:
        for name, ref in _signatures(reader):
            try:
                if ref.get_object()["/V"].get("/Type") == "/DocTimeStamp":
                    continue
                sig = EmbeddedPdfSignature(reader, ref, name)
                item = await _validate_one(sig, context, dss)
            except Exception:
                item = {"field": name, "state": "indeterminate",
                        "message": "Não foi possível interpretar ou verificar esta assinatura.",
                        "integrity": False, "chain": False, "revocation": "unknown"}
            signatures.append(item)
    except Exception as exc:
        raise ValueError("Não foi possível ler os campos de assinatura do PDF.") from exc
    states = {item["state"] for item in signatures}
    state = ("invalid" if "invalid" in states else "indeterminate" if "indeterminate" in states
             else "valid" if signatures else "unsigned")
    return {"state": state, "checked_at": datetime.now(timezone.utc).isoformat(), "signatures": signatures}


def validate_pdf(data, *, network=True):
    reader = read_pdf(data)
    try:
        return asyncio.run(asyncio.wait_for(_report(reader, network=network), timeout=24))
    except TimeoutError:
        return {"state": "indeterminate", "checked_at": datetime.now(timezone.utc).isoformat(),
                "signatures": [], "message": "A validação excedeu o tempo de consulta às certificadoras."}


async def _assert_preserved(reader, previous):
    current = dict(_signatures(reader))
    context = validation_context(fetch=False)
    for name, original in previous.items():
        if original.sig_object_type == "/DocTimeStamp":
            new_sig = EmbeddedPdfSignature(reader, current[name], name)
            new_sig.compute_integrity_info()
            if (new_sig.pkcs7_content != original.pkcs7_content or new_sig.coverage not in COMPLETE_COVERAGE
                    or new_sig.summarise_integrity_info()["docmdp_ok"] is False):
                raise ValueError("A nova revisão interferiria no carimbo de tempo existente.")
            continue
        ref = current.get(name)
        if ref is None:
            raise ValueError("Uma assinatura existente desapareceria na nova revisão.")
        new_sig = EmbeddedPdfSignature(reader, ref, name)
        status = await _validate_signature(new_sig, context)
        if new_sig.pkcs7_content != original.pkcs7_content or not _proof_of_integrity(status):
            raise ValueError("A nova revisão violaria uma assinatura ou restrição existente.")


async def _sign(data, signer, appearance, field_name):
    reader = read_pdf(data)
    approved = check_appearance(reader, appearance)
    previous = {name: EmbeddedPdfSignature(reader, ref, name) for name, ref in _signatures(reader)}
    await _assert_preserved(reader, previous)
    existing_field = approved.get("field")
    spec = None if existing_field else fields.SigFieldSpec(
        field_name, on_page=approved.get("page", 0), box=tuple(approved["box"]) if "box" in approved else None,
    )
    name = existing_field or field_name
    context = validation_context()
    metadata = signers.PdfSignatureMetadata(
        field_name=name, md_algorithm="sha256", subfilter=fields.SigSeedSubFilter.PADES,
        validation_context=context, signer_key_usage={"non_repudiation"},
        cades_signed_attr_spec=CAdESSignedAttrSpec(signature_policy_identifier=policy_identifier()),
    )
    writer = IncrementalPdfFileWriter(BytesIO(data), prev=reader)
    result = await _PbadSigner(metadata, signer, stamp_style=STAMP, new_field_spec=spec).async_sign_pdf(
        writer, existing_fields_only=bool(existing_field),
    )
    signed = result.getvalue()
    if not signed.startswith(data):
        raise ValueError("O assinador não conseguiu preservar os bytes originais do PDF.")
    final_reader = read_pdf(signed)
    await _assert_preserved(final_reader, previous)
    sig = EmbeddedPdfSignature(final_reader, dict(_signatures(final_reader))[name], name)
    status = await _validate_signature(sig, context)
    if not status.trusted or not _proof_of_integrity(status) or _policy_oid(sig) != POLICY_OID:
        raise ValueError("A nova assinatura não passou na verificação local de integridade, política e confiança.")
    report = await _report(final_reader, context=context)
    return signed, name, report


def sign_document(data, signer, appearance):
    try:
        return asyncio.run(asyncio.wait_for(_sign(data, signer, appearance, "SiSFarm#" + uuid4().hex), timeout=30))
    except SigningError as exc:
        if isinstance(exc.__cause__, RevokedError):
            raise ValueError("A revogação do certificado impede esta assinatura.") from exc
        raise ValueError("Não foi possível usar o certificado neste PDF. Confira a cadeia, a revogação e as restrições.") from exc
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("Não foi possível concluir a assinatura. Verifique a cadeia, a revogação e as restrições do PDF.") from exc
