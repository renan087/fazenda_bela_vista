"""WhatsApp Cloud API (Meta): assinatura do webhook, leitura do payload e envio de mensagens.

As credenciais vem so de variaveis de ambiente e nunca aparecem em logs nem no frontend.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import re
from dataclasses import dataclass

import httpx

from app.core.config import get_settings

# Filho do logger do uvicorn: aparece no log do Render tambem em nivel INFO.
logger = logging.getLogger("uvicorn.error.whatsapp")

GRAPH_URL = "https://graph.facebook.com"
TEXT_LIMIT = 4096
INTERACTIVE_BODY_LIMIT = 1024
MEDIA_MAX_BYTES = 5 * 1024 * 1024
MEDIA_TYPES = {"image/jpeg", "image/png", "image/webp", "application/pdf"}
SERVICE_WINDOW_SECONDS = 24 * 3600


def mask_phone(wa_id: str | None) -> str:
    digits = re.sub(r"\D", "", wa_id or "")
    return f"***{digits[-4:]}" if digits else "?"


def is_configured() -> bool:
    s = get_settings()
    return all((s.whatsapp_access_token, s.whatsapp_phone_number_id, s.whatsapp_app_secret, s.whatsapp_verify_token))


def verify_subscription(mode: str | None, token: str | None, challenge: str | None) -> str | None:
    """Verificacao GET da Meta: devolve o challenge so se o token conferir."""
    expected = (get_settings().whatsapp_verify_token or "").strip()
    if not expected or mode != "subscribe" or not token or challenge is None:
        return None
    return challenge if hmac.compare_digest(token.strip().encode(), expected.encode()) else None


def valid_signature(raw_body: bytes, header: str | None) -> bool:
    """X-Hub-Signature-256 = 'sha256=' + HMAC-SHA256(corpo bruto, App Secret)."""
    secret = (get_settings().whatsapp_app_secret or "").strip()
    if not secret or not header or not header.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(header[len("sha256="):].strip().lower().encode(), expected.encode())


@dataclass
class InboundMessage:
    wamid: str
    wa_id: str
    msg_type: str
    timestamp: int
    raw: dict


def extract_messages(payload: dict) -> list[InboundMessage]:
    """Mensagens de usuarios para o nosso numero (status de entrega e outros numeros sao ignorados)."""
    phone_number_id = (get_settings().whatsapp_phone_number_id or "").strip()
    found = []
    if not isinstance(payload, dict) or payload.get("object") != "whatsapp_business_account":
        return found
    for entry in payload.get("entry") or []:
        for change in (entry or {}).get("changes") or []:
            if (change or {}).get("field") != "messages":
                continue
            value = change.get("value") or {}
            metadata = value.get("metadata") or {}
            if phone_number_id and str(metadata.get("phone_number_id") or "") != phone_number_id:
                continue
            for message in value.get("messages") or []:
                wamid, wa_id = str(message.get("id") or ""), re.sub(r"\D", "", str(message.get("from") or ""))
                if not wamid or not wa_id or len(wamid) > 160 or len(wa_id) > 32:
                    continue
                try:
                    timestamp = int(message.get("timestamp") or 0)
                except (TypeError, ValueError):
                    timestamp = 0
                found.append(InboundMessage(wamid, wa_id, str(message.get("type") or "unknown")[:24], timestamp, message))
    return found


def summarize(payload: dict) -> str:
    """Resumo do evento para o log, sem conteudo das mensagens nem numeros completos."""
    phone_number_id = (get_settings().whatsapp_phone_number_id or "").strip()
    fields, messages, statuses, other_numbers, errors = [], 0, 0, 0, []
    for entry in (payload.get("entry") or []) if isinstance(payload, dict) else []:
        for change in (entry or {}).get("changes") or []:
            fields.append(str((change or {}).get("field")))
            value = (change or {}).get("value") or {}
            if str((value.get("metadata") or {}).get("phone_number_id") or "") not in ("", phone_number_id):
                other_numbers += 1
            messages += len(value.get("messages") or [])
            statuses += len(value.get("statuses") or [])
            for status in value.get("statuses") or []:
                for error in status.get("errors") or []:
                    errors.append(f"{status.get('status')}:{error.get('code')}")
    return (f"objeto={payload.get('object') if isinstance(payload, dict) else '?'} campos={','.join(fields) or '-'} "
            f"mensagens={messages} status={statuses} outro_phone_number_id={other_numbers}"
            + (f" erros_entrega={','.join(errors[:5])}" if errors else ""))


class WhatsAppError(RuntimeError):
    def __init__(self, message: str, code: int | None = None):
        super().__init__(message)
        self.code = code


RECIPIENT_NOT_ALLOWED = 131030


def br_ninth_digit(wa_id: str) -> str | None:
    """A Meta entrega celulares do Brasil sem o 9 (55 + DDD + 8 digitos); a lista de teste costuma ter o 9."""
    digits = re.sub(r"\D", "", wa_id or "")
    if len(digits) == 12 and digits.startswith("55") and digits[4] in "6789":
        return f"{digits[:4]}9{digits[4:]}"
    return None


class WhatsAppClient:
    """Cliente minimo da Cloud API. Erros viram WhatsAppError sem expor o token."""

    def __init__(self, *, http: httpx.Client | None = None):
        s = get_settings()
        self.token = (s.whatsapp_access_token or "").strip()
        self.phone_number_id = (s.whatsapp_phone_number_id or "").strip()
        self.base = f"{GRAPH_URL}/{s.whatsapp_graph_version}"
        self.http = http or httpx.Client(timeout=20)

    @property
    def _headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def _post(self, payload: dict) -> dict:
        if not self.token or not self.phone_number_id:
            raise WhatsAppError("WhatsApp não configurado no servidor.")
        try:
            response = self.http.post(f"{self.base}/{self.phone_number_id}/messages", json=payload, headers=self._headers)
        except httpx.HTTPError as exc:
            raise WhatsAppError(f"Falha de rede ao enviar mensagem: {type(exc).__name__}") from None
        if response.status_code >= 400:
            try:
                error = (response.json() or {}).get("error") or {}
            except ValueError:
                error = {}
            raise WhatsAppError(f"Meta recusou o envio (HTTP {response.status_code}, código {error.get('code')}): "
                                f"{str(error.get('message') or '')[:150]}", code=error.get("code"))
        return response.json()

    def _send(self, payload: dict) -> dict:
        try:
            return self._post(payload)
        except WhatsAppError as exc:
            alternative = br_ninth_digit(payload.get("to", ""))
            if exc.code != RECIPIENT_NOT_ALLOWED or not alternative:
                raise
            logger.info("WhatsApp: reenviando para %s com o nono digito", mask_phone(alternative))
            return self._post({**payload, "to": alternative})

    def send_text(self, to: str, body: str) -> dict:
        return self._send({"messaging_product": "whatsapp", "recipient_type": "individual", "to": to, "type": "text",
                           "text": {"body": (body or "").strip()[:TEXT_LIMIT] or "…", "preview_url": False}})

    def send_buttons(self, to: str, body: str, buttons: list[tuple[str, str]]) -> dict:
        """Ate 3 botoes de resposta rapida (titulo ate 20 caracteres)."""
        return self._send({
            "messaging_product": "whatsapp", "recipient_type": "individual", "to": to, "type": "interactive",
            "interactive": {"type": "button", "body": {"text": body[:INTERACTIVE_BODY_LIMIT]}, "action": {"buttons": [
                {"type": "reply", "reply": {"id": button_id[:256], "title": title[:20]}} for button_id, title in buttons[:3]
            ]}},
        })

    def mark_read(self, wamid: str) -> None:
        """Marca como lida e mostra 'digitando...' enquanto o assistente pensa (melhor esforco)."""
        try:
            self._post({"messaging_product": "whatsapp", "status": "read", "message_id": wamid,
                        "typing_indicator": {"type": "text"}})
        except WhatsAppError as exc:
            logger.info("WhatsApp: nao foi possivel marcar como lida: %s", exc)

    def download_media(self, media_id: str) -> tuple[bytes, str]:
        if not self.token:
            raise WhatsAppError("WhatsApp não configurado no servidor.")
        try:
            meta = self.http.get(f"{self.base}/{media_id}", headers=self._headers)
            meta.raise_for_status()
            info = meta.json()
            mime = str(info.get("mime_type") or "").split(";")[0].strip()
            if mime not in MEDIA_TYPES:
                raise WhatsAppError("Envie um PDF ou uma foto JPG, PNG ou WEBP.")
            if int(info.get("file_size") or 0) > MEDIA_MAX_BYTES:
                raise WhatsAppError("O arquivo precisa ter até 5 MB.")
            media = self.http.get(info["url"], headers=self._headers)
            media.raise_for_status()
        except (httpx.HTTPError, KeyError, ValueError):
            raise WhatsAppError("Não consegui baixar o arquivo enviado.") from None
        if len(media.content) > MEDIA_MAX_BYTES:
            raise WhatsAppError("O arquivo precisa ter até 5 MB.")
        if mime == "application/pdf" and not media.content.startswith(b"%PDF-"):
            raise WhatsAppError("O arquivo enviado não é um PDF válido.")
        return media.content, mime
