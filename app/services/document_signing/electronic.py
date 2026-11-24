"""Assinatura Eletrônica SiSFarm (Avançada — MP 2.200-2/2001 e Lei 14.063/2020):
selo visual vetorial com QR Code, manifesto automático de evidências, signatários externos
e selamento criptográfico (selo interno HMAC + e-CNPJ A1 institucional do SiSFarm).
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import re
import secrets
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from io import BytesIO
from urllib.parse import quote, urlsplit
from uuid import uuid4
from zoneinfo import ZoneInfo

import segno
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import pkcs12
from pyhanko.pdf_utils import generic
from pyhanko.pdf_utils.incremental_writer import IncrementalPdfFileWriter
from pyhanko.pdf_utils.reader import PdfFileReader
from pyhanko.pdf_utils.writer import PageObject
from reportlab import rl_config
from reportlab.lib.colors import HexColor
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas as rl_canvas
from sqlalchemy import select, update

from app.core.config import get_settings
from app.core.security import hash_persistent_token, hash_verification_code
from app.models import Farm, User
from app.models.audit_log import AuditLog
from app.models.signed_document import (
    InstitutionalSealingCertificate,
    SigningDocument,
    SigningElectronicChallenge,
    SigningEvidence,
    SigningExternalSigner,
    SigningParticipant,
    SigningRevision,
)
from app.models.whatsapp import WhatsAppInboundMessage, WhatsAppLink
from app.services.digital_certificates import encryption_key, seal, unseal, utc
from app.services.document_signing.pdf import a1_signer, check_appearance, read_pdf, sign_document, validate_pdf
from app.services.email_service import send_plain_email
from app.services.whatsapp import (
    SERVICE_WINDOW_SECONDS,
    WhatsAppClient,
    WhatsAppError,
    is_configured as whatsapp_is_configured,
    wa_id_variants,
)

rl_config.useA85 = 0
logger = logging.getLogger("uvicorn.error.electronic_signing")

CODE_ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
CONSENT_VERSION = "v1"
CHALLENGE_MINUTES = 5
MAX_ATTEMPTS = 5
EXTERNAL_TOKEN_DAYS = 15
SP_TZ = ZoneInfo("America/Sao_Paulo")


# ---------------------------------------------------------------------------
# Identificadores, validação de CPF e mascaramento LGPD
# ---------------------------------------------------------------------------


def generate_verification_code() -> str:
    raw = "".join(secrets.choice(CODE_ALPHABET) for _ in range(12))
    return f"SF-{raw[0:4]}-{raw[4:8]}-{raw[8:12]}"


def normalize_verification_code(value: str | None) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]", "", (value or "").upper())
    if cleaned.startswith("SF") and len(cleaned) == 14:
        cleaned = cleaned[2:]
    if len(cleaned) != 12:
        return ""
    return f"SF-{cleaned[0:4]}-{cleaned[4:8]}-{cleaned[8:12]}"


def generate_evidence_code() -> str:
    return "EV-" + secrets.token_hex(6).upper()


def validate_cpf(raw: str | None) -> str:
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) != 11 or len(set(digits)) == 1:
        raise ValueError("CPF inválido. Confira os 11 dígitos digitados.")
    for pos in (9, 10):
        total = sum(int(digits[idx]) * ((pos + 1) - idx) for idx in range(pos))
        rem = (total * 10) % 11
        check = 0 if rem == 10 else rem
        if check != int(digits[pos]):
            raise ValueError("CPF inválido. Confira os 11 dígitos digitados.")
    return digits


def mask_cpf(digits_or_raw: str | None) -> str:
    d = re.sub(r"\D", "", digits_or_raw or "")
    if len(d) == 11:
        return f"•••.{d[3:6]}.{d[6:9]}-••"
    if len(d) == 14:
        return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"
    return ""


def mask_email(email: str | None) -> str:
    value = (email or "").strip()
    if "@" not in value:
        return ""
    local, domain = value.split("@", 1)
    prefix = local[:2] if len(local) > 2 else local[:1]
    return f"{prefix}***@{domain}"


def mask_phone(phone: str | None) -> str:
    d = re.sub(r"\D", "", phone or "")
    if d.startswith("55") and len(d) in (12, 13):
        d = d[2:]
    if len(d) >= 10:
        return f"({d[:2]}) {d[2]}****-{d[-4:]}"
    if len(d) >= 4:
        return f"****-{d[-4:]}"
    return ""


def normalize_br_whatsapp(phone: str | None) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if not digits:
        return ""
    if len(digits) in (10, 11) and not digits.startswith("55"):
        return "55" + digits
    return digits


def mask_ip(ip: str | None) -> str:
    val = (ip or "").strip()
    if not val:
        return ""
    if "." in val:
        parts = val.split(".")
        if len(parts) == 4:
            return f"{parts[0]}.{parts[1]}.***.*"
    if ":" in val:
        parts = [p for p in val.split(":") if p]
        return ":".join(parts[:2]) + ":***" if parts else "***"
    return "***"


def summarize_device(user_agent: str | None) -> str:
    ua = (user_agent or "").strip()
    if not ua:
        return "Navegador web"
    low = ua.lower()
    if "edg/" in low or "edge/" in low:
        browser = "Edge"
    elif "firefox/" in low:
        browser = "Firefox"
    elif "chrome/" in low or "crios/" in low:
        browser = "Chrome"
    elif "safari/" in low:
        browser = "Safari"
    else:
        browser = "Navegador"
    if "iphone" in low:
        device = "iPhone (iOS)"
    elif "ipad" in low:
        device = "iPad (iPadOS)"
    elif "android" in low:
        device = "Android"
    elif "mac os x" in low or "macintosh" in low:
        device = "macOS"
    elif "windows" in low:
        device = "Windows"
    elif "linux" in low:
        device = "Linux"
    else:
        device = "Dispositivo"
    return f"{browser} · {device}"


def consent_text_for(doc_title: str, sha256_hex: str) -> str:
    return (
        f"Declaro que li integralmente o documento \"{doc_title}\" (SHA-256: {sha256_hex}), "
        "concordo expressamente em assiná-lo eletronicamente pela plataforma SiSFarm nos termos da "
        "Medida Provisória nº 2.200-2/2001 (art. 10, §2º) e da Lei nº 14.063/2020 (art. 4º, II), "
        "e autorizo o registro das evidências técnicas de assinatura (CPF, IP, dispositivo, canal verificado, "
        "data e hora) conforme a LGPD (Lei nº 13.709/2018)."
    )


def ensure_document_verification_code(db, doc: SigningDocument) -> str:
    if doc.verification_code:
        return doc.verification_code
    for _ in range(10):
        candidate = generate_verification_code()
        exists = db.scalar(select(SigningDocument.id).filter_by(verification_code=candidate))
        if exists is None:
            doc.verification_code = candidate
            return candidate
    raise RuntimeError("Não foi possível gerar código único de verificação.")


# ---------------------------------------------------------------------------
# Desenho vetorial: QR Code (segno), Selo Eletrônico SiSFarm e Manifesto
# ---------------------------------------------------------------------------


def _fit_font(text: str, font_name: str, max_size: float, max_width: float, min_size: float = 4.0) -> float:
    if not text or max_width <= 0:
        return max_size
    measured = stringWidth(text, font_name, max_size)
    if measured > max_width:
        return max(min_size, max_size * (max_width / measured))
    return max_size


def _draw_vector_qr(c: rl_canvas.Canvas, data: str, x: float, y: float, size: float) -> None:
    qr = segno.make(data, error="m")
    matrix = list(qr.matrix)
    n = len(matrix)
    if n <= 0:
        return
    pad = size * 0.06
    inner = size - pad * 2
    cell = inner / n
    c.saveState()
    c.setFillColor(HexColor("#FFFFFF"))
    c.setStrokeColor(HexColor("#D3E0CE"))
    c.setLineWidth(0.5)
    c.roundRect(x, y, size, size, min(4.0, size * 0.10), stroke=1, fill=1)
    c.setFillColor(HexColor("#16201A"))
    for r_idx, row in enumerate(matrix):
        py = y + pad + (n - 1 - r_idx) * cell
        for c_idx, val in enumerate(row):
            if val:
                px = x + pad + c_idx * cell
                c.rect(px, py, cell + 0.08, cell + 0.08, stroke=0, fill=1)
    c.restoreState()


def _draw_shield_check(c: rl_canvas.Canvas, x: float, y: float, size: float, color: HexColor) -> None:
    c.saveState()
    c.setStrokeColor(color)
    c.setLineWidth(max(0.7, size * 0.14))
    c.setLineCap(1)
    c.setLineJoin(1)
    p = c.beginPath()
    p.moveTo(x + size * 0.5, y + size * 0.96)
    p.lineTo(x + size * 0.12, y + size * 0.78)
    p.lineTo(x + size * 0.12, y + size * 0.45)
    p.curveTo(x + size * 0.12, y + size * 0.22, x + size * 0.28, y + size * 0.09, x + size * 0.5, y + size * 0.03)
    p.curveTo(x + size * 0.72, y + size * 0.09, x + size * 0.88, y + size * 0.22, x + size * 0.88, y + size * 0.45)
    p.lineTo(x + size * 0.88, y + size * 0.78)
    p.close()
    c.drawPath(p, stroke=1, fill=0)
    chk = c.beginPath()
    chk.moveTo(x + size * 0.30, y + size * 0.49)
    chk.lineTo(x + size * 0.45, y + size * 0.34)
    chk.lineTo(x + size * 0.72, y + size * 0.62)
    c.drawPath(chk, stroke=1, fill=0)
    c.restoreState()


def render_electronic_stamp_pdf(
    width: float,
    height: float,
    *,
    signer_name: str,
    cpf_masked: str,
    channel_label: str,
    verification_code: str,
    verify_url: str,
    now_br: datetime,
    style: str = "full",
) -> bytes:
    """Gera o selo visual próprio do SiSFarm 100% vetorial com QR Code e código único."""
    w = max(120.0, float(width))
    h = max(44.0, float(height))
    is_compact = style == "compact"

    buf = BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=(w, h), pageCompression=1)

    bg_main = HexColor("#F5F9F3")
    bg_foot = HexColor("#ECF3E8")
    brand = HexColor("#446A36")
    brand_dark = HexColor("#294127")
    ink = HexColor("#16201A")
    meta = HexColor("#3A4B40")
    line_col = HexColor("#D2DFCD")
    white = HexColor("#FFFFFF")

    radius = min(6.5, h * 0.12)
    stroke_w = 1.0
    inset = stroke_w / 2.0

    # Card base com clip arredondado
    c.saveState()
    clip = c.beginPath()
    clip.roundRect(inset, inset, w - stroke_w, h - stroke_w, radius)
    c.clipPath(clip, stroke=0, fill=0)

    c.setFillColor(bg_main)
    c.rect(0, 0, w, h, stroke=0, fill=1)

    # Faixa superior verde escuro SiSFarm
    bar_h = max(11.0, min(17.0, h * (0.22 if is_compact else 0.19)))
    c.setFillColor(brand_dark)
    c.rect(0, h - bar_h, w, bar_h, stroke=0, fill=1)

    # Faixa de rodapé com URL e código único
    foot_h = max(10.0, min(14.5, h * (0.19 if is_compact else 0.16)))
    c.setFillColor(bg_foot)
    c.rect(0, 0, w, foot_h, stroke=0, fill=1)
    c.setStrokeColor(line_col)
    c.setLineWidth(0.5)
    c.line(0, foot_h, w, foot_h)

    pad_x = max(6.0, min(10.0, w * 0.035))

    # Conteúdo da faixa superior
    ic_sz = bar_h * 0.56
    _draw_shield_check(c, pad_x, h - bar_h + (bar_h - ic_sz) / 2.0, ic_sz, HexColor("#DCF3C6"))
    head_txt = "ASSINATURA ELETRÔNICA · SISFARM"
    badge_w = 0.0 if is_compact else min(42.0, w * 0.18)
    head_avail = max(30.0, w - pad_x * 2 - ic_sz - 4 - badge_w)
    head_fs = _fit_font(head_txt, "Helvetica-Bold", min(6.6, bar_h * 0.45), head_avail, min_size=3.8)
    c.setFillColor(white)
    c.setFont("Helvetica-Bold", head_fs)
    c.drawString(pad_x + ic_sz + 3.5, h - bar_h + (bar_h - head_fs) / 2.0 + 0.8, head_txt)

    if not is_compact:
        pill_h = bar_h * 0.64
        pill_y = h - bar_h + (bar_h - pill_h) / 2.0
        pill_x = w - pad_x - badge_w
        c.setFillColor(brand)
        c.roundRect(pill_x, pill_y, badge_w, pill_h, pill_h / 2.0, stroke=0, fill=1)
        c.setFillColor(HexColor("#E7F7D8"))
        b_fs = _fit_font("AVANÇADA", "Helvetica-Bold", min(5.2, pill_h * 0.56), badge_w - 6, min_size=3.5)
        bw = stringWidth("AVANÇADA", "Helvetica-Bold", b_fs)
        c.setFont("Helvetica-Bold", b_fs)
        c.drawString(pill_x + (badge_w - bw) / 2.0, pill_y + (pill_h - b_fs) / 2.0 + 0.6, "AVANÇADA")

    # Miolo: texto à esquerda + QR Code vetorial à direita
    body_y = foot_h
    body_h = max(18.0, h - bar_h - foot_h)
    qr_size = min(body_h - 6.0, w * (0.24 if is_compact else 0.25), 54.0)
    qr_x = w - pad_x - qr_size
    qr_y = body_y + (body_h - qr_size) / 2.0
    _draw_vector_qr(c, verify_url, qr_x, qr_y, qr_size)

    text_avail = max(40.0, qr_x - pad_x - 6.0)
    name_upper = (signer_name or "SIGNATÁRIO").strip().upper()
    if is_compact:
        rows = [
            (name_upper, "Helvetica-Bold", ink, 0.44),
            (f"CPF {cpf_masked} · {now_br.strftime('%d/%m/%Y %H:%M')}", "Helvetica", meta, 0.34),
        ]
    else:
        line2 = f"CPF {cpf_masked} · {channel_label}" if cpf_masked else channel_label
        line3 = f"Data: {now_br.strftime('%d/%m/%Y, %H:%M')} (BRT)"
        line4 = "Autenticação OTP · MP 2.200-2/2001"
        rows = [
            (name_upper, "Helvetica-Bold", ink, 0.28),
            (line2, "Helvetica", meta, 0.21),
            (line3, "Helvetica", meta, 0.21),
            (line4, "Helvetica", HexColor("#4E6354"), 0.18),
        ]

    total_r = sum(r[3] for r in rows) + 0.05 * (len(rows) - 1)
    unit = (body_h - 6.0) / total_r
    block_h = total_r * unit
    top_cursor = body_y + (body_h + block_h) / 2.0
    for idx, (txt, font_name, col, ratio) in enumerate(rows):
        lh = ratio * unit
        fs = _fit_font(txt, font_name, min(lh * 0.84, 8.6 if idx == 0 else 6.6), text_avail, min_size=3.8)
        c.setFillColor(col)
        c.setFont(font_name, fs)
        c.drawString(pad_x, top_cursor - lh * 0.78, txt)
        top_cursor -= lh + (0.05 * unit if idx < len(rows) - 1 else 0)

    # Rodapé: URL curta à esquerda + código único à direita
    host_display = urlsplit(verify_url).netloc or "sisfarm"
    left_foot = f"{host_display}/validar"
    code_fs = _fit_font(verification_code, "Courier-Bold", min(6.2, foot_h * 0.54), w * 0.48, min_size=4.0)
    code_w = stringWidth(verification_code, "Courier-Bold", code_fs)
    left_avail = max(24.0, w - pad_x * 2 - code_w - 6.0)
    left_fs = _fit_font(left_foot, "Helvetica-Bold", min(5.8, foot_h * 0.50), left_avail, min_size=3.6)

    c.setFillColor(brand)
    c.setFont("Helvetica-Bold", left_fs)
    c.drawString(pad_x, (foot_h - left_fs) / 2.0 + 0.6, left_foot)

    c.setFillColor(brand_dark)
    c.setFont("Courier-Bold", code_fs)
    c.drawString(w - pad_x - code_w, (foot_h - code_fs) / 2.0 + 0.6, verification_code)

    c.restoreState()

    # Borda externa arredondada nítida
    c.setStrokeColor(brand)
    c.setLineWidth(stroke_w)
    c.roundRect(inset, inset, w - stroke_w, h - stroke_w, radius, stroke=1, fill=0)

    c.showPage()
    c.save()
    return buf.getvalue()


def apply_electronic_stamp_to_pdf(
    pdf_bytes: bytes,
    *,
    page: int,
    box: list[float] | tuple[float, ...],
    stamp_pdf_bytes: bytes,
    signer_name: str,
    verification_code: str,
) -> bytes:
    """Aplica o carimbo vetorial de forma incremental (/Stamp com /AP /N), preservando assinaturas existentes."""
    w = IncrementalPdfFileWriter(BytesIO(pdf_bytes), strict=False)
    xobj = w.import_page_as_xobject(PdfFileReader(BytesIO(stamp_pdf_bytes)), page_ix=0)
    page_ref, _ = w.find_page_for_modification(page)
    annot = generic.DictionaryObject({
        generic.pdf_name("/Type"): generic.pdf_name("/Annot"),
        generic.pdf_name("/Subtype"): generic.pdf_name("/Stamp"),
        generic.pdf_name("/Rect"): generic.ArrayObject([generic.FloatObject(float(v)) for v in box]),
        generic.pdf_name("/F"): generic.NumberObject(4),
        generic.pdf_name("/AP"): generic.DictionaryObject({generic.pdf_name("/N"): xobj}),
        generic.pdf_name("/P"): page_ref,
        generic.pdf_name("/T"): generic.pdf_string(f"Assinatura Eletronica SiSFarm - {signer_name}"),
        generic.pdf_name("/Contents"): generic.pdf_string(f"Assinatura Eletronica SiSFarm ({verification_code})"),
    })
    annot_ref = w.add_object(annot)
    page_obj = page_ref.get_object()
    annots = page_obj.get("/Annots")
    if annots is None:
        page_obj[generic.pdf_name("/Annots")] = generic.ArrayObject([annot_ref])
        w.mark_update(page_ref)
    else:
        arr = annots.get_object() if hasattr(annots, "get_object") else annots
        arr.append(annot_ref)
        w.update_container(arr)
    out = BytesIO()
    w.write(out)
    return out.getvalue()


def _wrap_text(text: str, font_name: str, font_size: float, max_width: float) -> list[str]:
    words = (text or "").split()
    lines, cur = [], ""
    for word in words:
        trial = f"{cur} {word}".strip()
        if not cur or stringWidth(trial, font_name, font_size) <= max_width:
            cur = trial
        else:
            lines.append(cur)
            cur = word
    if cur:
        lines.append(cur)
    return lines or [""]


def render_manifesto_pdf(
    *,
    doc_title: str,
    farm_name: str,
    verification_code: str,
    verify_url: str,
    original_sha256: str,
    evidences: list[SigningEvidence],
    seal_mode: str,
    institutional_cn: str | None,
    now_br: datetime,
) -> bytes:
    """Gera a página final de Manifesto de Assinaturas e Evidências (A4, 100% vetorial, LGPD-safe)."""
    page_w, page_h = 595.0, 842.0
    margin = 40.0
    inner_w = page_w - margin * 2

    buf = BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=(page_w, page_h), pageCompression=1)

    brand = HexColor("#446A36")
    brand_dark = HexColor("#294127")
    ink = HexColor("#16201A")
    muted = HexColor("#5B6660")
    line_col = HexColor("#D8E2D5")
    soft_bg = HexColor("#F6F8F5")
    ok_bg = HexColor("#E6F4E2")
    ok_fg = HexColor("#1F7A3A")
    info_bg = HexColor("#E8F1FB")
    info_fg = HexColor("#1F5FA8")

    # Faixa superior fina
    c.setFillColor(brand)
    c.rect(0, page_h - 8, page_w, 8, stroke=0, fill=1)

    y = page_h - 38
    # QR Code no canto superior direito
    qr_sz = 72.0
    _draw_vector_qr(c, verify_url, page_w - margin - qr_sz, y - qr_sz + 8, qr_sz)

    c.setFillColor(brand)
    c.setFont("Helvetica-Bold", 8.5)
    c.drawString(margin, y, "SISFARM · MANIFESTO DE ASSINATURAS E EVIDÊNCIAS (LGPD)")

    y -= 20
    c.setFillColor(ink)
    title_fs = _fit_font("Manifesto de Assinaturas", "Helvetica-Bold", 17.0, inner_w - qr_sz - 16)
    c.setFont("Helvetica-Bold", title_fs)
    c.drawString(margin, y, "Manifesto de Assinaturas")

    y -= 16
    c.setFillColor(muted)
    c.setFont("Helvetica", 9.5)
    doc_line = f"Documento: {doc_title[:70]} · {farm_name}"
    c.drawString(margin, y, doc_line)

    y -= 14
    c.drawString(margin, y, f"Concluído em: {now_br.strftime('%d/%m/%Y às %H:%M:%S')} (BRT) · Código: {verification_code}")

    y -= 13
    c.setFillColor(brand_dark)
    c.setFont("Helvetica-Bold", 8.5)
    c.drawString(margin, y, f"Validação pública: {verify_url}")

    y -= 16
    c.setStrokeColor(brand)
    c.setLineWidth(1.2)
    c.line(margin, y, page_w - margin, y)
    y -= 18

    # Lista de assinaturas e evidências
    c.setFillColor(ink)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(margin, y, f"Assinaturas registradas ({len(evidences)})")
    y -= 14

    for idx, ev in enumerate(evidences[:8], 1):
        if y < 185:
            break
        is_icp = ev.modality == "qualified_icp_brasil"
        card_h = 68.0 if is_icp else 80.0
        card_y = y - card_h

        c.setFillColor(HexColor("#FCFDFC"))
        c.setStrokeColor(line_col)
        c.setLineWidth(0.8)
        c.roundRect(margin, card_y, inner_w, card_h, 7, stroke=1, fill=1)

        # Nome + papel
        c.setFillColor(ink)
        c.setFont("Helvetica-Bold", 10)
        header_name = f"{idx}. {ev.signer_name}"
        if ev.role_label:
            header_name += f" ({ev.role_label})"
        c.drawString(margin + 10, y - 16, header_name[:64])

        # Selo de modalidade
        badge_txt = "Qualificada ICP-Brasil · Lei 14.063 art. 4º III" if is_icp else "Eletrônica SiSFarm (Avançada) · Lei 14.063 art. 4º II"
        b_w = stringWidth(badge_txt, "Helvetica-Bold", 7.2) + 12
        c.setFillColor(ok_bg if is_icp else info_bg)
        c.roundRect(page_w - margin - b_w - 8, y - 19.5, b_w, 13, 6.5, stroke=0, fill=1)
        c.setFillColor(ok_fg if is_icp else info_fg)
        c.setFont("Helvetica-Bold", 7.2)
        c.drawString(page_w - margin - b_w - 2, y - 15.2, badge_txt)

        # Detalhes mascarados (LGPD)
        signed_br = utc(ev.signed_at).astimezone(SP_TZ) if ev.signed_at else now_br
        signed_utc = utc(ev.signed_at) if ev.signed_at else now_br.astimezone(timezone.utc)
        c.setFillColor(muted)
        c.setFont("Helvetica", 8.2)
        if is_icp:
            c.drawString(margin + 10, y - 33, f"CPF (LGPD): {ev.cpf_masked or 'Conforme certificado'}   ·   Certificado: {(ev.cert_issuer or 'ICP-Brasil A1')[:54]}")
            c.drawString(margin + 10, y - 46, f"Data/hora: {signed_br.strftime('%d/%m/%Y %H:%M:%S')} (BRT) / {signed_utc.strftime('%H:%M:%S')} UTC")
            c.drawString(margin + 10, y - 59, f"Validação oficial: PAdES AD-RB · validar.iti.gov.br   ·   Registro: {ev.evidence_code}")
        else:
            chan_desc = "WhatsApp" if "whatsapp" in (ev.auth_channel or "") else "E-mail"
            c.drawString(margin + 10, y - 33, f"CPF (LGPD): {ev.cpf_masked}   ·   Autenticação: OTP 8 dígitos via {chan_desc} ({ev.auth_destination_masked})")
            c.drawString(margin + 10, y - 46, f"Data/hora: {signed_br.strftime('%d/%m/%Y %H:%M:%S')} (BRT) / {signed_utc.strftime('%H:%M:%S')} UTC   ·   IP (LGPD): {ev.ip_masked}")
            c.drawString(margin + 10, y - 59, f"Dispositivo: {(ev.device_summary or 'Navegador web')[:48]}   ·   Evidência: {ev.evidence_code}")
            c.drawString(margin + 10, y - 72, "Consentimento expresso confirmado (MP 2.200-2/2001 art. 10 §2º · Lei 14.063/2020 · LGPD)")

        y = card_y - 10

    # Bloco de Selo Criptográfico de Integridade SiSFarm (separação jurídica explícita)
    seal_h = 112.0
    seal_y = max(48.0, min(y - seal_h, 175.0))
    c.setFillColor(HexColor("#F3F8F1"))
    c.setStrokeColor(HexColor("#B7D2AE"))
    c.setLineWidth(1.0)
    c.roundRect(margin, seal_y, inner_w, seal_h, 8, stroke=1, fill=1)

    c.setFillColor(brand_dark)
    c.setFont("Helvetica-Bold", 9.5)
    if seal_mode == "institutional_ecnpj" and institutional_cn:
        seal_heading = f"Selo Criptográfico Institucional — e-CNPJ A1 SiSFarm ({institutional_cn[:42]})"
        seal_note = (
            "Nota de transparência jurídica: Este PDF recebe selamento criptográfico institucional pelo e-CNPJ A1 "
            "do SiSFarm exclusivamente para garantir a integridade binária do arquivo após a coleta das assinaturas "
            "eletrônicas. Este selo institucional não equivale à assinatura qualificada ICP-Brasil dos signatários eletrônicos."
        )
    else:
        seal_heading = "Selo Criptográfico de Integridade — SiSFarm"
        seal_note = (
            "Nota de transparência jurídica: Este PDF foi protegido criptograficamente pelo SiSFarm para comprovar "
            "que o documento não sofreu alterações após a conclusão das assinaturas eletrônicas. Este selo protege a "
            "integridade do arquivo e não equivale à assinatura qualificada ICP-Brasil dos signatários eletrônicos."
        )
    c.drawString(margin + 12, seal_y + seal_h - 18, seal_heading)

    c.setFillColor(HexColor("#2E4332"))
    c.setFont("Helvetica", 7.8)
    ty = seal_y + seal_h - 32
    for ln in _wrap_text(seal_note, "Helvetica", 7.8, inner_w - 24):
        c.drawString(margin + 12, ty, ln)
        ty -= 10.5

    # Hash original
    c.setFillColor(soft_bg)
    c.setStrokeColor(line_col)
    c.setLineWidth(0.5)
    c.roundRect(margin + 12, seal_y + 12, inner_w - 24, 24, 5, stroke=1, fill=1)
    c.setFillColor(muted)
    c.setFont("Helvetica-Bold", 7.0)
    c.drawString(margin + 18, seal_y + 26, "SHA-256 DO DOCUMENTO ORIGINAL (ANTES DAS ASSINATURAS):")
    c.setFillColor(ink)
    c.setFont("Courier", 7.5)
    c.drawString(margin + 18, seal_y + 16, original_sha256 or "-")

    c.setFillColor(muted)
    c.setFont("Helvetica", 7.5)
    c.drawString(margin, 24, f"Gerado automaticamente pelo SiSFarm · Dados pessoais mascarados conforme LGPD (Lei 13.709/2018) · Código {verification_code}")

    c.showPage()
    c.save()
    return buf.getvalue()


def append_manifesto_page(pdf_bytes: bytes, manifesto_pdf_bytes: bytes) -> bytes:
    """Anexa a página de Manifesto ao final do PDF de forma incremental (sem reescrever bytes anteriores)."""
    w = IncrementalPdfFileWriter(BytesIO(pdf_bytes), strict=False)
    mxobj = w.import_page_as_xobject(PdfFileReader(BytesIO(manifesto_pdf_bytes)), page_ix=0)
    m_stream = w.add_object(generic.StreamObject(stream_data=b"q /SFManifesto Do Q"))
    res = generic.DictionaryObject({
        generic.pdf_name("/XObject"): generic.DictionaryObject({generic.pdf_name("/SFManifesto"): mxobj})
    })
    page_obj = PageObject(contents=m_stream, media_box=(0, 0, 595, 842), resources=res)
    w.insert_page(page_obj)
    out = BytesIO()
    w.write(out)
    return out.getvalue()


def compute_internal_seal_hmac(doc_id: str, verification_code: str, original_sha256: str, evidence_hashes: list[str]) -> str:
    key = encryption_key("digital_document_key")
    payload = "|".join([doc_id, verification_code, original_sha256, *sorted(evidence_hashes)]).encode("utf-8")
    return hmac.new(key, payload, hashlib.sha256).hexdigest()


def apply_internal_metadata_seal(
    pdf_bytes: bytes,
    *,
    doc_id: str,
    verification_code: str,
    original_sha256: str,
    evidence_hashes: list[str],
) -> tuple[bytes, str]:
    """Aplica o selo criptográfico interno (HMAC-SHA256 em metadados PDF incrementais) e retorna (bytes, sha256)."""
    seal_hmac = compute_internal_seal_hmac(doc_id, verification_code, original_sha256, evidence_hashes)
    w = IncrementalPdfFileWriter(BytesIO(pdf_bytes), strict=False)
    info_ref = w.prev.trailer.raw_get("/Info") if "/Info" in w.prev.trailer else None
    if info_ref is not None and hasattr(info_ref, "get_object"):
        info_dict = info_ref.get_object()
        w.mark_update(info_ref)
    else:
        info_dict = generic.DictionaryObject()
        w.set_info(w.add_object(info_dict))
    info_dict[generic.pdf_name("/SiSFarmVerificationCode")] = generic.pdf_string(verification_code)
    info_dict[generic.pdf_name("/SiSFarmOriginalSHA256")] = generic.pdf_string(original_sha256)
    info_dict[generic.pdf_name("/SiSFarmSealHMAC")] = generic.pdf_string(seal_hmac)
    info_dict[generic.pdf_name("/SiSFarmSealMode")] = generic.pdf_string("internal_hmac")
    out = BytesIO()
    w.write(out)
    sealed = out.getvalue()
    return sealed, sha256(sealed).hexdigest()


# ---------------------------------------------------------------------------
# Certificado Institucional e-CNPJ A1 (Painel Administrativo Geral)
# ---------------------------------------------------------------------------


def _inst_context(cert_id: str, label: str) -> bytes:
    return f"sisfarm:institutional-seal:{cert_id}:{label}".encode("utf-8")


def get_active_institutional_certificate(db) -> InstitutionalSealingCertificate | None:
    now = datetime.now(timezone.utc)
    cert = db.scalar(
        select(InstitutionalSealingCertificate)
        .where(InstitutionalSealingCertificate.is_active.is_(True))
        .order_by(InstitutionalSealingCertificate.created_at.desc())
        .limit(1)
    )
    if cert is None:
        return None
    if not (utc(cert.valid_from) <= now <= utc(cert.valid_until)):
        return None
    return cert


def store_institutional_certificate(db, *, pfx_bytes: bytes, password: str, user: User) -> InstitutionalSealingCertificate:
    if not pfx_bytes or len(pfx_bytes) > 512 * 1024:
        raise ValueError("Envie um arquivo .pfx ou .p12 válido (até 512 KB).")
    if not password:
        raise ValueError("Informe a senha do certificado institucional.")
    try:
        key, cert, chain = pkcs12.load_key_and_certificates(pfx_bytes, password.encode("utf-8"))
    except ValueError as exc:
        raise ValueError("Não foi possível abrir o certificado institucional. Confira o arquivo e a senha.") from exc
    if key is None or cert is None:
        raise ValueError("O arquivo enviado não contém chave privada e certificado digital válidos.")
    now = datetime.now(timezone.utc)
    valid_from = cert.not_valid_before_utc
    valid_until = cert.not_valid_after_utc
    if valid_until <= now:
        raise ValueError("Este certificado institucional já está vencido.")

    subject_cn = ""
    for attr in cert.subject:
        if attr.oid._name == "commonName":
            subject_cn = str(attr.value)
            break
    issuer_cn = ""
    for attr in cert.issuer:
        if attr.oid._name == "commonName":
            issuer_cn = str(attr.value)
            break
    cnpj_masked = ""
    if ":" in subject_cn:
        digits = re.sub(r"\D", "", subject_cn.rsplit(":", 1)[1])
        if len(digits) == 14:
            cnpj_masked = f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"
    fingerprint = sha256(cert.public_bytes(serialization.Encoding.DER)).hexdigest()

    # Desativa certificados institucionais anteriores
    db.execute(
        update(InstitutionalSealingCertificate)
        .where(InstitutionalSealingCertificate.is_active.is_(True))
        .values(is_active=False, deactivated_at=now)
    )

    cert_id = uuid4().hex
    master_key = encryption_key("digital_certificate_key")
    pfx_enc = base64.b64encode(seal(pfx_bytes, master_key, _inst_context(cert_id, "pfx"))).decode("ascii")
    pw_enc = base64.b64encode(seal(password.encode("utf-8"), master_key, _inst_context(cert_id, "pw"))).decode("ascii")

    row = InstitutionalSealingCertificate(
        id=cert_id,
        subject_cn=(subject_cn or "Certificado Institucional SiSFarm")[:255],
        cnpj_masked=cnpj_masked,
        issuer_cn=issuer_cn[:255],
        fingerprint_sha256=fingerprint,
        valid_from=valid_from,
        valid_until=valid_until,
        pfx_encrypted=pfx_enc,
        password_encrypted=pw_enc,
        is_active=True,
        uploaded_by=user.id,
    )
    db.add(row)
    db.add(
        AuditLog(
            actor_user_id=user.id,
            actor_email=user.email,
            organization_id=user.organization_id,
            event_type="institutional_seal.certificate.activated",
            outcome="success",
            metadata_json=json.dumps({"cert_id": cert_id, "subject_cn": row.subject_cn, "valid_until": valid_until.isoformat()}),
        )
    )
    db.commit()
    return row


def deactivate_institutional_certificate(db, *, user: User) -> int:
    now = datetime.now(timezone.utc)
    result = db.execute(
        update(InstitutionalSealingCertificate)
        .where(InstitutionalSealingCertificate.is_active.is_(True))
        .values(is_active=False, deactivated_at=now)
    )
    db.add(
        AuditLog(
            actor_user_id=user.id,
            actor_email=user.email,
            organization_id=user.organization_id,
            event_type="institutional_seal.certificate.deactivated",
            outcome="success",
            metadata_json=json.dumps({"deactivated": int(result.rowcount or 0)}),
        )
    )
    db.commit()
    return int(result.rowcount or 0)


def _open_institutional_material(cert_row: InstitutionalSealingCertificate) -> dict:
    master_key = encryption_key("digital_certificate_key")
    pfx_bytes = unseal(base64.b64decode(cert_row.pfx_encrypted), master_key, _inst_context(cert_row.id, "pfx"))
    password = unseal(base64.b64decode(cert_row.password_encrypted), master_key, _inst_context(cert_row.id, "pw")).decode("utf-8")
    key, cert, chain = pkcs12.load_key_and_certificates(pfx_bytes, password.encode("utf-8"))
    if key is None or cert is None:
        raise ValueError("Certificado institucional inválido.")
    return {"private_key": key, "certificate": cert, "chain": list(chain or ())}


def apply_institutional_ecnpj_seal(pdf_bytes: bytes, *, cert_row: InstitutionalSealingCertificate) -> tuple[bytes, str]:
    material = _open_institutional_material(cert_row)
    signed, _, _ = sign_document(pdf_bytes, a1_signer(material), {"visible": False})
    return signed, sha256(signed).hexdigest()


# ---------------------------------------------------------------------------
# Signatários externos (sem conta no SiSFarm — link exclusivo)
# ---------------------------------------------------------------------------


def has_active_whatsapp_window(db, phone_or_wa_id: str | None) -> bool:
    """Verifica se o destinatário enviou mensagem ao WhatsApp do SiSFarm nas últimas 24h (janela ativa da Meta)."""
    wa_to = normalize_br_whatsapp(phone_or_wa_id)
    if not wa_to:
        return False
    variants = wa_id_variants(wa_to)
    if not variants:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=SERVICE_WINDOW_SECONDS)
    link_at = db.scalar(
        select(WhatsAppLink.last_inbound_at)
        .where(WhatsAppLink.wa_id.in_(variants), WhatsAppLink.last_inbound_at.is_not(None))
        .order_by(WhatsAppLink.last_inbound_at.desc())
        .limit(1)
    )
    if link_at and utc(link_at) >= cutoff:
        return True
    inbound_at = db.scalar(
        select(WhatsAppInboundMessage.received_at)
        .where(WhatsAppInboundMessage.wa_id.in_(variants))
        .order_by(WhatsAppInboundMessage.received_at.desc())
        .limit(1)
    )
    return bool(inbound_at and utc(inbound_at) >= cutoff)


def build_whatsapp_share_url(phone: str | None, text: str) -> str:
    wa_to = normalize_br_whatsapp(phone)
    if not wa_to:
        return ""
    return f"https://wa.me/{wa_to}?text={quote(text, safe='')}"


def _dispatch_external_invite(
    db,
    *,
    doc: SigningDocument,
    farm: Farm,
    sender: User,
    signer: SigningExternalSigner,
    sign_url: str,
) -> tuple[bool, str | None, str]:
    doc_title = doc.title or doc.filename
    sender_name = sender.display_name or sender.name
    invite_text = (
        f"Olá, {signer.name}!\n\n"
        f"*{sender_name}* ({farm.name}) enviou o documento *{doc_title}* para sua assinatura eletrônica no SiSFarm.\n"
        f"Não precisa criar conta nem ter certificado digital.\n\n"
        f"Acesse seu link exclusivo para ler e assinar:\n{sign_url}"
    )
    whatsapp_share_url = build_whatsapp_share_url(signer.phone, invite_text)
    sent, err = False, None
    settings = get_settings()

    if signer.channel == "whatsapp":
        wa_to = normalize_br_whatsapp(signer.phone)
        template_name = (getattr(settings, "whatsapp_signing_invite_template", None) or "").strip()
        template_lang = (getattr(settings, "whatsapp_template_language", None) or "pt_BR").strip()
        if not whatsapp_is_configured():
            err = "Envio automático por WhatsApp não está configurado no servidor. Clique em 'Enviar pelo meu WhatsApp' ou copie o link."
        elif template_name:
            try:
                WhatsAppClient().send_template(
                    wa_to,
                    template_name,
                    language=template_lang,
                    body_params=[signer.name, sender_name, farm.name, doc_title, sign_url],
                )
                sent = True
            except WhatsAppError as exc:
                logger.warning("Falha ao enviar template WhatsApp externo (doc=%s): %s", doc.id, exc)
                err = f"Não foi possível enviar o WhatsApp automático para {signer.name} ({exc}). Use 'Enviar pelo meu WhatsApp' ou copie o link."
        elif has_active_whatsapp_window(db, wa_to):
            try:
                WhatsAppClient().send_text(wa_to, invite_text)
                sent = True
            except WhatsAppError as exc:
                logger.warning("Falha ao enviar link externo via WhatsApp (doc=%s): %s", doc.id, exc)
                err = f"Não foi possível enviar o WhatsApp para {signer.name} ({exc}). Use 'Enviar pelo meu WhatsApp' ou copie o link."
        else:
            logger.info(
                "WhatsApp externo sem janela de 24h nem template aprovado (doc=%s, signer=%s); retornando link wa.me",
                doc.id,
                signer.id,
            )
            err = (
                f"O WhatsApp Business (Meta) não entrega mensagens automáticas para quem não conversou com o SiSFarm nas últimas 24h "
                f"(sem modelo aprovado). Use o botão 'Enviar pelo meu WhatsApp' para mandar o convite para {signer.name} na hora."
            )
    else:
        try:
            send_plain_email(
                signer.email,
                f"Convite para assinar documento no SiSFarm: {doc_title}",
                f"Olá, {signer.name}.\n\n"
                f"{sender_name} ({farm.name}) enviou o documento \"{doc_title}\" para sua assinatura eletrônica no SiSFarm.\n"
                "Você não precisa criar conta nem ter certificado digital.\n\n"
                f"Acesse seu link exclusivo para revisar e assinar:\n{sign_url}\n\n"
                f"Link válido por {EXTERNAL_TOKEN_DAYS} dias.",
                "document_signing_external_invite",
            )
            sent = True
        except RuntimeError as exc:
            logger.warning("Falha ao enviar link externo via e-mail (doc=%s, to=%s): %s", doc.id, signer.email, exc)
            err = f"Não foi possível enviar o e-mail para {signer.email}: {exc}"
    return sent, err, whatsapp_share_url


def create_external_signer(
    db,
    *,
    doc: SigningDocument,
    farm: Farm,
    sender: User,
    name: str,
    email: str | None,
    phone: str | None,
    channel: str,
    role_label: str,
    base_url: str,
) -> tuple[SigningExternalSigner, str, bool, str | None, str]:
    clean_name = (name or "").strip()[:160]
    if len(clean_name) < 2:
        raise ValueError("Informe o nome completo do signatário externo.")
    clean_email = (email or "").strip().lower()[:180] or None
    clean_phone = (phone or "").strip()[:40] or None
    chan = "whatsapp" if (channel or "").strip().lower() == "whatsapp" else "email"
    if chan == "whatsapp" and not normalize_br_whatsapp(clean_phone):
        raise ValueError("Informe o número de WhatsApp do signatário externo.")
    if chan == "email" and (not clean_email or "@" not in clean_email):
        raise ValueError("Informe um e-mail válido para o signatário externo.")
    clean_role = (role_label or "Signatário").strip()[:64] or "Signatário"

    raw_token = secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc)
    signer = SigningExternalSigner(
        id=uuid4().hex,
        document_id=doc.id,
        name=clean_name,
        email=clean_email,
        phone=clean_phone,
        channel=chan,
        role_label=clean_role,
        access_token_hash=hash_persistent_token(raw_token),
        token_expires_at=now + timedelta(days=EXTERNAL_TOKEN_DAYS),
        status="pending",
    )
    ensure_document_verification_code(db, doc)
    db.add(signer)
    db.commit()

    sign_url = f"{base_url.rstrip('/')}/assinar/{raw_token}"
    sent, err, whatsapp_share_url = _dispatch_external_invite(
        db, doc=doc, farm=farm, sender=sender, signer=signer, sign_url=sign_url,
    )
    return signer, sign_url, sent, err, whatsapp_share_url


def resend_external_signer(
    db,
    *,
    doc: SigningDocument,
    farm: Farm,
    sender: User,
    signer: SigningExternalSigner,
    base_url: str = "",
) -> tuple[str, bool, str | None, str]:
    if signer.status == "signed":
        raise ValueError("Este signatário já assinou o documento.")
    raw_token = secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc)
    signer.access_token_hash = hash_persistent_token(raw_token)
    signer.token_expires_at = now + timedelta(days=EXTERNAL_TOKEN_DAYS)
    signer.status = "pending"
    db.commit()
    sign_url = f"{base_url.rstrip('/')}/assinar/{raw_token}"
    sent, err, whatsapp_share_url = _dispatch_external_invite(
        db, doc=doc, farm=farm, sender=sender, signer=signer, sign_url=sign_url,
    )
    return sign_url, sent, err, whatsapp_share_url


def get_external_signer_by_token(db, raw_token: str) -> tuple[SigningExternalSigner, SigningDocument, Farm] | None:
    if not raw_token or len(raw_token) < 16:
        return None
    token_hash = hash_persistent_token(raw_token.strip())
    signer = db.scalar(select(SigningExternalSigner).filter_by(access_token_hash=token_hash))
    if signer is None or signer.status == "revoked":
        return None
    if utc(signer.token_expires_at) <= datetime.now(timezone.utc) and signer.status != "signed":
        return None
    doc = db.get(SigningDocument, signer.document_id)
    if doc is None:
        return None
    farm = db.get(Farm, doc.farm_id)
    if farm is None:
        return None
    return signer, doc, farm


# ---------------------------------------------------------------------------
# Cerimônia de Assinatura Eletrônica (OTP + Evidências + Selamento Final)
# ---------------------------------------------------------------------------


def _encrypt_cpf(cpf_digits: str, doc_id: str) -> str:
    key = encryption_key("digital_document_key")
    sealed = seal(cpf_digits.encode("utf-8"), key, f"sisfarm:cpf:{doc_id}".encode("utf-8"))
    return base64.b64encode(sealed).decode("ascii")


def _compute_evidence_hash(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return sha256(canonical).hexdigest()


def issue_electronic_challenge(
    db,
    *,
    doc: SigningDocument,
    revision: SigningRevision,
    pdf_bytes: bytes,
    user: User | None,
    external_signer: SigningExternalSigner | None,
    signer_name: str,
    cpf_raw: str,
    auth_channel: str,
    appearance: dict,
    consent: bool,
    override_email: str | None = None,
) -> dict:
    if not consent:
        raise ValueError("Confirme que leu o documento e autoriza a assinatura eletrônica.")
    clean_name = (signer_name or "").strip()[:160]
    if len(clean_name) < 3:
        raise ValueError("Informe seu nome completo para assinar o documento.")
    cpf_digits = validate_cpf(cpf_raw)
    chan = "whatsapp" if (auth_channel or "").strip().lower() == "whatsapp" else "email"

    # Valida destino do canal de OTP
    if external_signer is not None:
        if external_signer.status == "signed":
            raise ValueError("Você já assinou este documento.")
        dest_email = external_signer.email
        if chan == "email" and (not dest_email or "@" not in dest_email):
            clean_override = (override_email or "").strip().lower()[:180]
            if clean_override and "@" in clean_override:
                dest_email = clean_override
                external_signer.email = clean_override
        dest_phone = external_signer.phone
    elif user is not None:
        dest_email = user.email
        link = db.scalar(select(WhatsAppLink).where(WhatsAppLink.user_id == user.id, WhatsAppLink.wa_id.is_not(None)))
        dest_phone = (link.wa_id if link and link.wa_id else None) or user.phone
    else:
        raise ValueError("Signatário inválido.")

    settings = get_settings()
    otp_template = (getattr(settings, "whatsapp_signing_otp_template", None) or "").strip()
    otp_lang = (getattr(settings, "whatsapp_template_language", None) or "pt_BR").strip()

    if chan == "whatsapp":
        wa_target = normalize_br_whatsapp(dest_phone)
        if not wa_target:
            raise ValueError("Não há número de WhatsApp cadastrado para receber o código. Escolha E-mail.")
        if not whatsapp_is_configured():
            raise ValueError("O envio de código por WhatsApp não está disponível no momento. Escolha receber por E-mail.")
        if not otp_template and not has_active_whatsapp_window(db, wa_target):
            raise ValueError(
                "Para receber o código pelo WhatsApp do SiSFarm, envie primeiro um 'Oi' no nosso WhatsApp ou escolha receber o código por E-mail."
            )
        dest_masked = mask_phone(wa_target)
    else:
        if not dest_email or "@" not in dest_email:
            raise ValueError("Informe um e-mail válido para receber o código de confirmação.")
        wa_target = ""
        dest_masked = mask_email(dest_email)

    # Valida posição do carimbo (ou invisível)
    reader = read_pdf(pdf_bytes)
    if appearance.get("visible"):
        approved = check_appearance(reader, appearance)
    else:
        approved = {"visible": False, "style": "compact" if appearance.get("style") == "compact" else "full"}

    now = datetime.now(timezone.utc)
    # Rate limit de 30s por signatário
    if external_signer is not None:
        latest = db.scalar(
            select(SigningElectronicChallenge.created_at)
            .filter_by(external_signer_id=external_signer.id)
            .order_by(SigningElectronicChallenge.created_at.desc())
            .limit(1)
        )
        db.execute(
            update(SigningElectronicChallenge)
            .where(SigningElectronicChallenge.external_signer_id == external_signer.id, SigningElectronicChallenge.consumed_at.is_(None))
            .values(consumed_at=now)
        )
    else:
        latest = db.scalar(
            select(SigningElectronicChallenge.created_at)
            .filter_by(user_id=user.id)
            .order_by(SigningElectronicChallenge.created_at.desc())
            .limit(1)
        )
        db.execute(
            update(SigningElectronicChallenge)
            .where(SigningElectronicChallenge.user_id == user.id, SigningElectronicChallenge.consumed_at.is_(None))
            .values(consumed_at=now)
        )
    if latest and now - utc(latest) < timedelta(seconds=30):
        raise ValueError("Aguarde 30 segundos antes de solicitar outro código.")

    code = f"{secrets.randbelow(100_000_000):08d}"
    challenge_id = uuid4().hex
    doc_title = doc.title or doc.filename
    consent_txt = consent_text_for(doc_title, revision.sha256)

    challenge = SigningElectronicChallenge(
        id=challenge_id,
        document_id=doc.id,
        revision_id=revision.id,
        revision_sha256=revision.sha256,
        user_id=user.id if user is not None else None,
        external_signer_id=external_signer.id if external_signer is not None else None,
        signer_name=clean_name,
        cpf_masked=mask_cpf(cpf_digits),
        cpf_encrypted=_encrypt_cpf(cpf_digits, doc.id),
        cpf_sha256=sha256(cpf_digits.encode("utf-8")).hexdigest(),
        auth_channel=chan,
        auth_destination_masked=dest_masked,
        appearance=approved,
        consent_version=CONSENT_VERSION,
        consent_text_sha256=sha256(consent_txt.encode("utf-8")).hexdigest(),
        code_hash=hash_verification_code(challenge_id + code),
        expires_at=now + timedelta(minutes=CHALLENGE_MINUTES),
    )
    ensure_document_verification_code(db, doc)
    if not doc.original_sha256:
        first_rev = db.scalar(select(SigningRevision).filter_by(document_id=doc.id, number=1))
        if first_rev:
            doc.original_sha256 = first_rev.sha256
    db.add(challenge)
    db.commit()

    try:
        if chan == "whatsapp":
            if otp_template:
                WhatsAppClient().send_template(
                    wa_target,
                    otp_template,
                    language=otp_lang,
                    body_params=[code, doc_title],
                )
            else:
                WhatsAppClient().send_text(
                    wa_target,
                    f"SiSFarm · Código de confirmação para assinar *{doc_title}*:\n\n"
                    f"*{code}*\n\n"
                    f"Válido por {CHALLENGE_MINUTES} minutos. Informe este código apenas na tela onde você iniciou a assinatura.",
                )
        else:
            send_plain_email(
                dest_email,
                f"Código para assinar documento no SiSFarm: {doc_title}",
                f"Documento: {doc_title}\n"
                f"Signatário: {clean_name} (CPF {mask_cpf(cpf_digits)})\n"
                f"SHA-256: {revision.sha256}\n\n"
                f"Código de autorização: {code}\n"
                f"Válido por {CHALLENGE_MINUTES} minutos exclusivamente para esta versão do documento.\n"
                "Informe o código somente na tela do SiSFarm onde você iniciou a assinatura.",
                "document_signing_electronic_otp",
            )
    except (RuntimeError, WhatsAppError) as exc:
        db.execute(update(SigningElectronicChallenge).filter_by(id=challenge_id).values(consumed_at=now))
        db.commit()
        raise ValueError(f"Não foi possível enviar o código de confirmação ({exc}). Tente outro canal ou aguarde.") from exc

    return {
        "challenge_id": challenge_id,
        "channel": chan,
        "destination_masked": dest_masked,
        "expires_minutes": CHALLENGE_MINUTES,
        "sha256": revision.sha256,
    }


def _pending_signer_count(db, doc: SigningDocument) -> int:
    evidences = db.scalars(select(SigningEvidence).filter_by(document_id=doc.id)).all()
    signed_user_ids = {ev.user_id for ev in evidences if ev.user_id is not None}
    # Também considera revisões ICP-Brasil anteriores que ainda não tinham SigningEvidence
    for uid, sig in db.execute(
        select(SigningRevision.user_id, SigningRevision.signature).filter_by(document_id=doc.id)
    ).all():
        if uid is not None and sig:
            signed_user_ids.add(uid)

    participant_ids = db.scalars(select(SigningParticipant.user_id).filter_by(document_id=doc.id)).all()
    owner_must_sign = bool(getattr(doc, "owner_signs", True))
    expected_internal = {uid for uid in (([doc.owner_id] if owner_must_sign else []) + list(participant_ids)) if uid is not None}
    internal_pending = len(expected_internal - signed_user_ids)

    ext_pending = db.scalars(
        select(SigningExternalSigner.id).where(
            SigningExternalSigner.document_id == doc.id,
            SigningExternalSigner.status == "pending",
        )
    ).all()
    return internal_pending + len(ext_pending)


def record_icp_evidence(
    db,
    *,
    doc: SigningDocument,
    revision_number: int,
    user: User,
    signer_name: str,
    cpf_masked: str,
    cert_issuer: str,
    sha_before: str,
    sha_after: str,
    ip_address: str,
    user_agent: str,
) -> SigningEvidence:
    ensure_document_verification_code(db, doc)
    now = datetime.now(timezone.utc)
    ev_code = generate_evidence_code()
    ev_payload = {
        "evidence_code": ev_code,
        "document_id": doc.id,
        "revision_number": revision_number,
        "modality": "qualified_icp_brasil",
        "signer_name": signer_name,
        "cpf_masked": cpf_masked,
        "cert_issuer": cert_issuer,
        "document_sha256_before": sha_before,
        "document_sha256_after": sha_after,
        "signed_at": now.isoformat(),
    }
    ev = SigningEvidence(
        id=uuid4().hex,
        evidence_code=ev_code,
        document_id=doc.id,
        revision_number=revision_number,
        user_id=user.id,
        modality="qualified_icp_brasil",
        signer_name=signer_name[:160],
        role_label="Responsável" if doc.owner_id == user.id else "Participante",
        cpf_masked=cpf_masked[:24],
        auth_channel="icp_brasil_a1",
        auth_destination_masked=mask_email(user.email),
        ip_address=(ip_address or "")[:64],
        ip_masked=mask_ip(ip_address),
        user_agent=(user_agent or "")[:300],
        device_summary=summarize_device(user_agent),
        consent_version=CONSENT_VERSION,
        document_sha256_before=sha_before,
        document_sha256_after=sha_after,
        evidence_hash=_compute_evidence_hash(ev_payload),
        cert_issuer=(cert_issuer or "")[:180],
        signed_at=now,
    )
    db.add(ev)
    return ev


def finalize_electronic_document_if_complete(
    db,
    *,
    doc: SigningDocument,
    farm_name: str,
    base_url: str,
    current_pdf_bytes: bytes,
    store_revision_fn,
    actor_user: User | None,
) -> tuple[bytes, str] | None:
    """Se todos os signatários concluíram e há assinatura eletrônica, anexa Manifesto e aplica o selamento final."""
    if _pending_signer_count(db, doc) > 0:
        return None
    evidences = db.scalars(
        select(SigningEvidence).filter_by(document_id=doc.id).order_by(SigningEvidence.signed_at.asc(), SigningEvidence.revision_number.asc())
    ).all()
    if not any(ev.modality == "electronic_sisfarm" for ev in evidences):
        return None
    if doc.sealed_at is not None:
        return None

    code = ensure_document_verification_code(db, doc)
    verify_url = f"{base_url.rstrip('/')}/validar/{code}"
    now_utc = datetime.now(timezone.utc)
    now_br = now_utc.astimezone(SP_TZ)
    original_sha = doc.original_sha256 or (evidences[0].document_sha256_before if evidences else sha256(current_pdf_bytes).hexdigest())
    doc.original_sha256 = original_sha

    inst_cert = get_active_institutional_certificate(db)
    planned_mode = "institutional_ecnpj" if inst_cert is not None else "internal_hmac"

    manifesto_bytes = render_manifesto_pdf(
        doc_title=doc.title or doc.filename,
        farm_name=farm_name,
        verification_code=code,
        verify_url=verify_url,
        original_sha256=original_sha,
        evidences=evidences,
        seal_mode=planned_mode,
        institutional_cn=inst_cert.subject_cn if inst_cert else None,
        now_br=now_br,
    )
    with_manifesto = append_manifesto_page(current_pdf_bytes, manifesto_bytes)

    final_mode = "internal_hmac"
    sealed_bytes = with_manifesto
    sealed_sha = ""
    if inst_cert is not None:
        try:
            sealed_bytes, sealed_sha = apply_institutional_ecnpj_seal(with_manifesto, cert_row=inst_cert)
            final_mode = "institutional_ecnpj"
        except Exception as exc:
            logger.warning("Falha ao aplicar selo e-CNPJ institucional (doc=%s); aplicando selo interno: %s", doc.id, exc)
            sealed_bytes, sealed_sha = apply_internal_metadata_seal(
                with_manifesto,
                doc_id=doc.id,
                verification_code=code,
                original_sha256=original_sha,
                evidence_hashes=[ev.evidence_hash for ev in evidences],
            )
    else:
        sealed_bytes, sealed_sha = apply_internal_metadata_seal(
            with_manifesto,
            doc_id=doc.id,
            verification_code=code,
            original_sha256=original_sha,
            evidence_hashes=[ev.evidence_hash for ev in evidences],
        )

    doc.current_revision += 1
    doc.sealed_sha256 = sealed_sha
    doc.seal_mode = final_mode
    doc.sealed_at = now_utc
    store_revision_fn(
        db,
        doc,
        actor_user,
        sealed_bytes,
        signature={
            "modality": "institutional_seal",
            "seal_mode": final_mode,
            "verification_code": code,
            "sealed_at": now_utc.isoformat(),
        },
    )
    return sealed_bytes, sealed_sha


def confirm_electronic_signature(
    db,
    *,
    doc: SigningDocument,
    revision: SigningRevision,
    pdf_bytes: bytes,
    user: User | None,
    external_signer: SigningExternalSigner | None,
    challenge_id: str,
    code: str,
    ip_address: str,
    user_agent: str,
    farm_name: str,
    base_url: str,
    store_revision_fn,
) -> dict:
    q = select(SigningElectronicChallenge).where(
        SigningElectronicChallenge.id == challenge_id,
        SigningElectronicChallenge.document_id == doc.id,
    )
    if external_signer is not None:
        q = q.where(SigningElectronicChallenge.external_signer_id == external_signer.id)
    elif user is not None:
        q = q.where(SigningElectronicChallenge.user_id == user.id)
    challenge = db.scalar(q.with_for_update().execution_options(populate_existing=True))

    now = datetime.now(timezone.utc)
    if challenge is None or challenge.consumed_at or challenge.attempts >= MAX_ATTEMPTS or utc(challenge.expires_at) <= now:
        raise ValueError("Código de autorização expirado ou indisponível. Solicite um novo código.")
    if revision.id != challenge.revision_id or revision.sha256 != challenge.revision_sha256:
        raise ValueError("O documento foi atualizado. Confira a nova versão e solicite um novo código.")

    challenge.attempts += 1
    if not hmac.compare_digest(challenge.code_hash, hash_verification_code(challenge.id + (code or "").strip())):
        if challenge.attempts >= MAX_ATTEMPTS:
            challenge.consumed_at = now
        db.commit()
        raise PermissionError("Código de autorização incorreto. Confira o código recebido ou solicite um novo.")

    challenge.consumed_at = now
    ver_code = ensure_document_verification_code(db, doc)
    verify_url = f"{base_url.rstrip('/')}/validar/{ver_code}"
    now_br = now.astimezone(SP_TZ)
    appearance = challenge.appearance or {}
    chan_label = "Via WhatsApp" if challenge.auth_channel == "whatsapp" else "Via E-mail"

    new_pdf = pdf_bytes
    if appearance.get("visible") and appearance.get("box"):
        box = appearance["box"]
        width = abs(float(box[2]) - float(box[0]))
        height = abs(float(box[3]) - float(box[1]))
        stamp_pdf = render_electronic_stamp_pdf(
            width,
            height,
            signer_name=challenge.signer_name,
            cpf_masked=challenge.cpf_masked,
            channel_label=chan_label,
            verification_code=ver_code,
            verify_url=verify_url,
            now_br=now_br,
            style=appearance.get("style", "full"),
        )
        new_pdf = apply_electronic_stamp_to_pdf(
            pdf_bytes,
            page=int(appearance.get("page", 0)),
            box=box,
            stamp_pdf_bytes=stamp_pdf,
            signer_name=challenge.signer_name,
            verification_code=ver_code,
        )

    sha_after = sha256(new_pdf).hexdigest()
    doc.current_revision += 1
    ev_code = generate_evidence_code()
    role_label = (
        external_signer.role_label
        if external_signer is not None
        else ("Responsável" if user is not None and doc.owner_id == user.id else "Participante")
    )
    ev_payload = {
        "evidence_code": ev_code,
        "document_id": doc.id,
        "revision_number": doc.current_revision,
        "modality": "electronic_sisfarm",
        "signer_name": challenge.signer_name,
        "cpf_masked": challenge.cpf_masked,
        "cpf_sha256": challenge.cpf_sha256,
        "auth_channel": f"{challenge.auth_channel}_otp",
        "auth_destination_masked": challenge.auth_destination_masked,
        "ip_address": ip_address,
        "user_agent": user_agent,
        "consent_version": challenge.consent_version,
        "consent_text_sha256": challenge.consent_text_sha256,
        "document_sha256_before": revision.sha256,
        "document_sha256_after": sha_after,
        "signed_at": now.isoformat(),
    }
    ev = SigningEvidence(
        id=uuid4().hex,
        evidence_code=ev_code,
        document_id=doc.id,
        revision_number=doc.current_revision,
        user_id=user.id if user is not None else None,
        external_signer_id=external_signer.id if external_signer is not None else None,
        modality="electronic_sisfarm",
        signer_name=challenge.signer_name,
        role_label=role_label,
        cpf_masked=challenge.cpf_masked,
        cpf_encrypted=challenge.cpf_encrypted,
        cpf_sha256=challenge.cpf_sha256,
        auth_channel=f"{challenge.auth_channel}_otp",
        auth_destination_masked=challenge.auth_destination_masked,
        ip_address=(ip_address or "")[:64],
        ip_masked=mask_ip(ip_address),
        user_agent=(user_agent or "")[:300],
        device_summary=summarize_device(user_agent),
        consent_version=challenge.consent_version,
        consent_text_sha256=challenge.consent_text_sha256,
        document_sha256_before=revision.sha256,
        document_sha256_after=sha_after,
        evidence_hash=_compute_evidence_hash(ev_payload),
        signed_at=now,
    )
    db.add(ev)

    store_revision_fn(
        db,
        doc,
        user,
        new_pdf,
        signature={
            "modality": "electronic_sisfarm",
            "holder": challenge.signer_name,
            "cpf_masked": challenge.cpf_masked,
            "evidence_code": ev_code,
            "verification_code": ver_code,
            "auth_channel": f"{challenge.auth_channel}_otp",
            "authorized_at": now.isoformat(),
            "input_sha256": revision.sha256,
            "appearance": appearance,
        },
    )

    if external_signer is not None:
        external_signer.status = "signed"
        external_signer.signed_at = now

    db.flush()

    sealed = finalize_electronic_document_if_complete(
        db,
        doc=doc,
        farm_name=farm_name,
        base_url=base_url,
        current_pdf_bytes=new_pdf,
        store_revision_fn=store_revision_fn,
        actor_user=user,
    )
    final_sha = sealed[1] if sealed else sha_after
    final_rev = doc.current_revision
    remaining = _pending_signer_count(db, doc)

    db.commit()
    return {
        "revision": final_rev,
        "sha256": final_sha,
        "verification_code": ver_code,
        "verify_url": f"/validar/{ver_code}",
        "evidence_code": ev_code,
        "sealed": bool(sealed),
        "seal_mode": doc.seal_mode,
        "remaining": remaining,
    }


def public_validation_snapshot(db, raw_code: str) -> dict | None:
    code = normalize_verification_code(raw_code)
    if not code:
        return None
    doc = db.scalar(select(SigningDocument).filter_by(verification_code=code))
    if doc is None:
        return None
    farm = db.get(Farm, doc.farm_id)
    evidences = db.scalars(
        select(SigningEvidence).filter_by(document_id=doc.id).order_by(SigningEvidence.signed_at.asc(), SigningEvidence.revision_number.asc())
    ).all()
    latest_rev = db.scalar(
        select(SigningRevision).filter_by(document_id=doc.id, number=doc.current_revision)
    )
    pending = _pending_signer_count(db, doc)
    # Permite conferir integridade tanto contra a versão selada final quanto contra a última revisão atual
    accepted_hashes = {h for h in (doc.sealed_sha256, latest_rev.sha256 if latest_rev else None) if h}
    return {
        "doc": doc,
        "farm": farm,
        "code": code,
        "title": doc.title or (doc.filename[:-4] if doc.filename.lower().endswith(".pdf") else doc.filename),
        "evidences": evidences,
        "pending_count": pending,
        "completed": pending == 0 and len(evidences) > 0,
        "original_sha256": doc.original_sha256 or "",
        "sealed_sha256": doc.sealed_sha256 or (latest_rev.sha256 if latest_rev else ""),
        "accepted_hashes": accepted_hashes,
        "seal_mode": doc.seal_mode,
        "sealed_at": doc.sealed_at,
    }
