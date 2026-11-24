"""PDF inspection, incremental PAdES signing and independently verifiable results."""
import asyncio
import binascii
import logging
import math
from dataclasses import replace
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from hashlib import sha256
from io import BytesIO
from uuid import uuid4

from pyhanko.keys.internal import translate_pyca_cryptography_cert_to_asn1, translate_pyca_cryptography_key_to_asn1
from pyhanko.pdf_utils import content as pdf_content, generic, layout as pdf_layout
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
from pyhanko.stamp import StaticStampStyle, TextStampStyle
from pyhanko_certvalidator.errors import RevokedError
from pyhanko_certvalidator.registry import SimpleCertificateStore
from reportlab.lib.colors import HexColor
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas as rl_canvas

from app.services.document_signing.policy import FILTER, SUBFILTER, POLICY_OID, policy_identifier
from app.services.document_signing.trust import validation_context

logger = logging.getLogger(__name__)

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
    style = "compact" if appearance.get("style") == "compact" else "full"
    return {"visible": True, "page": page, "box": rect, "style": style}


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


class _MemoryPdfPage(pdf_content.PdfContent):
    """Imports a single-page vector PDF from memory as an XObject."""

    def __init__(self, pdf_bytes: bytes):
        self._pdf_bytes = pdf_bytes
        super().__init__()

    def render(self) -> bytes:
        w = self._ensure_writer
        r = PdfFileReader(BytesIO(self._pdf_bytes))
        xobj = w.import_page_as_xobject(r, page_ix=0)
        resource_name = b"/SiSFarmStamp" + binascii.hexlify(uuid4().bytes)
        self.resources.xobject[resource_name.decode("ascii")] = xobj
        x1, y1, x2, y2 = xobj.get_object()["/BBox"]
        self.box = pdf_layout.BoxConstraints(width=abs(x1 - x2), height=abs(y1 - y2))
        return resource_name + b" Do"


def _fit_font_size(text: str, font_name: str, max_size: float, max_width: float, min_size: float = 4.2) -> float:
    size = max_size
    if not text or max_width <= 0:
        return size
    measured = stringWidth(text, font_name, size)
    if measured > max_width:
        size = max(min_size, size * (max_width / measured))
    return size


def _draw_shield_icon(c: rl_canvas.Canvas, x: float, y: float, size: float, color: HexColor) -> None:
    """Draws a crisp vector shield + checkmark icon matching the SiSFarm UI stamp."""
    c.saveState()
    c.setStrokeColor(color)
    c.setLineWidth(max(0.65, size * 0.13))
    c.setLineCap(1)
    c.setLineJoin(1)
    # Shield outline
    p = c.beginPath()
    p.moveTo(x + size * 0.5, y + size * 0.96)
    p.lineTo(x + size * 0.12, y + size * 0.78)
    p.lineTo(x + size * 0.12, y + size * 0.45)
    p.curveTo(x + size * 0.12, y + size * 0.22, x + size * 0.28, y + size * 0.09, x + size * 0.5, y + size * 0.03)
    p.curveTo(x + size * 0.72, y + size * 0.09, x + size * 0.88, y + size * 0.22, x + size * 0.88, y + size * 0.45)
    p.lineTo(x + size * 0.88, y + size * 0.78)
    p.close()
    c.drawPath(p, stroke=1, fill=0)
    # Checkmark inside shield
    chk = c.beginPath()
    chk.moveTo(x + size * 0.30, y + size * 0.49)
    chk.lineTo(x + size * 0.45, y + size * 0.34)
    chk.lineTo(x + size * 0.72, y + size * 0.62)
    c.drawPath(chk, stroke=1, fill=0)
    c.restoreState()


def _render_stamp_pdf(
    width: float,
    height: float,
    *,
    signer_name: str,
    doc_label: str,
    cert_type: str,
    now_br: datetime,
    style: str = "full",
) -> bytes:
    """Renders a crisp vector PDF stamp identical to the SiSFarm visual preview."""
    w = max(80.0, float(width))
    h = max(28.0, float(height))
    is_compact = style == "compact"

    buf = BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=(w, h))

    bg_color = HexColor("#EEF3EB")
    border_color = HexColor("#446A36")
    divider_color = HexColor("#C7D6C0")
    ok_color = HexColor("#1F7A3A")
    ink_color = HexColor("#16201A")
    meta_color = HexColor("#354A39")
    brand_color = HexColor("#446A36")

    stroke_w = max(0.8, min(1.4, min(w, h) * 0.02))
    inset = stroke_w / 2.0
    radius = min(8.0, h * 0.14, w * 0.05)

    # Soft green rounded card + crisp green border
    c.setFillColor(bg_color)
    c.setStrokeColor(border_color)
    c.setLineWidth(stroke_w)
    c.roundRect(inset, inset, w - stroke_w, h - stroke_w, radius, stroke=1, fill=1)

    pad_x = max(6.0, min(11.0, w * 0.045))
    pad_y = max(4.0, min(8.0, h * (0.09 if is_compact else 0.10)))
    inner_w = max(20.0, w - pad_x * 2)
    inner_h = max(16.0, h - pad_y * 2)

    head_text = "ASSINADO DIGITALMENTE - ICP-BRASIL"
    if is_compact:
        meta_text = " · ".join(part for part in (doc_label, now_br.strftime("%d/%m/%Y %H:%M")) if part)
        url_text = "validar.iti.gov.br"
        rows = [
            ("head", head_text, "Helvetica-Bold", ok_color, 0.175),
            ("name", signer_name, "Helvetica-Bold", ink_color, 0.255),
            ("meta", meta_text, "Helvetica", meta_color, 0.205),
            ("url", url_text, "Helvetica-Bold", brand_color, 0.195),
        ]
    else:
        doc_line = " · ".join(part for part in (doc_label, cert_type) if part)
        date_line = f"Data: {now_br.strftime('%d/%m/%Y, %H:%M')} (BRT)"
        url_text = "Verifique em validar.iti.gov.br"
        rows = [
            ("head", head_text, "Helvetica-Bold", ok_color, 0.135),
            ("name", signer_name, "Helvetica-Bold", ink_color, 0.185),
        ]
        if doc_line:
            rows.append(("meta", doc_line, "Helvetica", meta_color, 0.150))
        rows.append(("meta", date_line, "Helvetica", meta_color, 0.150))
        rows.append(("url", url_text, "Helvetica-Bold", brand_color, 0.145))

    # Scale row heights so the entire block fits cleanly inside inner_h and centers vertically
    total_ratio = sum(r[4] for r in rows)
    gap_ratio = 0.038 if is_compact else 0.042
    divider_extra = 0.055 if not is_compact else 0.0
    total_units = total_ratio + gap_ratio * (len(rows) - 1) + divider_extra
    scale = inner_h / total_units

    block_h = total_units * scale
    cursor_top = pad_y + (inner_h + block_h) / 2.0

    for idx, (kind, text, font_name, color, ratio) in enumerate(rows):
        line_h = ratio * scale
        target_size = min(line_h * 0.92, 9.5 if kind == "name" else 7.8)
        if kind == "head":
            icon_size = max(5.2, min(9.0, target_size * 1.15))
            icon_gap = max(2.5, icon_size * 0.4)
            avail_w = max(12.0, inner_w - icon_size - icon_gap)
            font_size = _fit_font_size(text, font_name, target_size, avail_w, min_size=3.8)
            baseline = cursor_top - line_h * 0.80
            _draw_shield_icon(c, pad_x, baseline - icon_size * 0.16, icon_size, color)
            c.setFillColor(color)
            c.setFont(font_name, font_size)
            c.drawString(pad_x + icon_size + icon_gap, baseline, text)
            cursor_top -= line_h
            if not is_compact:
                div_y = cursor_top - (divider_extra * scale) * 0.45
                c.setStrokeColor(divider_color)
                c.setLineWidth(0.55)
                c.line(pad_x, div_y, w - pad_x, div_y)
                cursor_top -= divider_extra * scale
        else:
            font_size = _fit_font_size(text, font_name, target_size, inner_w, min_size=4.0)
            baseline = cursor_top - line_h * 0.80
            c.setFillColor(color)
            c.setFont(font_name, font_size)
            c.drawString(pad_x, baseline, text)
            cursor_top -= line_h

        if idx < len(rows) - 1:
            cursor_top -= gap_ratio * scale

    c.showPage()
    c.save()
    return buf.getvalue()


def _build_stamp_style(signer, style: str = "full", box: tuple | list | None = None):
    cn = ""
    try:
        cn = signer.signing_cert.subject.native.get("common_name") or ""
    except Exception:
        cn = ""
    display_name = cn.strip()
    doc_label = ""
    cert_type = "Certificado A1"
    if ":" in display_name:
        left, right = display_name.rsplit(":", 1)
        digits = "".join(ch for ch in right if ch.isdigit())
        if len(digits) == 11:
            display_name = left.strip() or display_name
            doc_label = f"CPF •••.{digits[3:6]}.{digits[6:9]}-••"
            cert_type = "e-CPF A1"
        elif len(digits) == 14:
            display_name = left.strip() or display_name
            doc_label = f"CNPJ {digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"
            cert_type = "e-CNPJ A1"
    now_br = datetime.now(ZoneInfo("America/Sao_Paulo"))
    signer_line = display_name.upper() if display_name else "ASSINATURA DIGITAL"

    if box and len(box) == 4:
        width = abs(float(box[2]) - float(box[0]))
        height = abs(float(box[3]) - float(box[1]))
    else:
        width, height = (190.0, 48.0) if style == "compact" else (220.0, 76.0)

    stamp_pdf = _render_stamp_pdf(
        width,
        height,
        signer_name=signer_line,
        doc_label=doc_label,
        cert_type=cert_type,
        now_br=now_br,
        style=style,
    )
    return StaticStampStyle(
        background=_MemoryPdfPage(stamp_pdf),
        border_width=0,
        background_opacity=1.0,
        background_layout=pdf_layout.SimpleBoxLayoutRule(
            x_align=pdf_layout.AxisAlignment.ALIGN_MID,
            y_align=pdf_layout.AxisAlignment.ALIGN_MID,
            margins=pdf_layout.Margins.uniform(0),
        ),
    )


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
    embedded_chain = list(signer.cert_registry) if getattr(signer, "cert_registry", None) else []
    context = validation_context(other_certs=embedded_chain)
    metadata = signers.PdfSignatureMetadata(
        field_name=name, md_algorithm="sha256", subfilter=fields.SigSeedSubFilter.PADES,
        validation_context=context, signer_key_usage={"non_repudiation"},
        cades_signed_attr_spec=CAdESSignedAttrSpec(signature_policy_identifier=policy_identifier()),
    )
    writer = IncrementalPdfFileWriter(BytesIO(data), prev=reader)
    stamp_style = _build_stamp_style(signer, approved.get("style", "full"), approved.get("box"))
    result = await _PbadSigner(metadata, signer, stamp_style=stamp_style, new_field_spec=spec).async_sign_pdf(
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
        logger.warning(
            "Assinatura local nao aprovada: trusted=%s intact=%s valid=%s problem=%s policy=%s",
            status.trusted, status.intact, status.valid,
            getattr(status, "trust_problem_indic", None), _policy_oid(sig),
        )
        raise ValueError("A nova assinatura não passou na verificação local de integridade, política e confiança.")
    report = await _report(final_reader, context=context)
    return signed, name, report


def sign_document(data, signer, appearance):
    try:
        return asyncio.run(asyncio.wait_for(_sign(data, signer, appearance, "SiSFarm#" + uuid4().hex), timeout=60))
    except SigningError as exc:
        logger.warning("SigningError ao assinar PDF: %s | cause=%r", exc, exc.__cause__)
        if isinstance(exc.__cause__, RevokedError):
            raise ValueError("A revogação do certificado impede esta assinatura.") from exc
        raise ValueError("Não foi possível usar o certificado neste PDF. Confira a cadeia, a revogação e as restrições.") from exc
    except ValueError:
        raise
    except Exception as exc:
        logger.warning("Erro inesperado ao assinar PDF (%s): %s", type(exc).__name__, exc)
        raise ValueError("Não foi possível concluir a assinatura. Verifique a cadeia, a revogação e as restrições do PDF.") from exc
