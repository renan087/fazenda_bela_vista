"""Transcrição de mensagens de voz recebidas pelo assistente."""

from io import BytesIO
import asyncio
import logging
from datetime import datetime, timezone

from app.core.config import get_settings

AUDIO_TYPES = {"audio/ogg": "voz.ogg", "audio/mpeg": "voz.mp3", "audio/mp4": "voz.m4a",
               "audio/webm": "voz.webm", "audio/wav": "voz.wav"}
MAX_AUDIO_BYTES = 5 * 1024 * 1024
AUDIO_RETENTION_DAYS = 30


def delete_expired_audio(db):
    from app.models.assistant_audio import AssistantAudio
    count = db.query(AssistantAudio).filter(AssistantAudio.expires_at <= datetime.now(timezone.utc)).delete(synchronize_session=False)
    db.commit()
    return count


async def audio_retention_loop():
    from app.db.session import SessionLocal
    def cleanup():
        with SessionLocal() as db:
            delete_expired_audio(db)
    while True:
        try:
            await asyncio.to_thread(cleanup)
        except Exception:
            logging.getLogger(__name__).exception("Falha ao remover áudios expirados")
        await asyncio.sleep(3600)


def transcribe_audio(data: bytes, mime: str) -> str:
    if mime not in AUDIO_TYPES or not data or len(data) > MAX_AUDIO_BYTES:
        raise ValueError("Envie um áudio válido de até 5 MB.")
    settings = get_settings()
    if not settings.openai_api_key:
        raise ValueError("A transcrição de áudio não está configurada.")
    from openai import OpenAI
    audio = BytesIO(data)
    audio.name = AUDIO_TYPES[mime]
    try:
        result = OpenAI(api_key=settings.openai_api_key, timeout=settings.openai_timeout_seconds, max_retries=1).audio.transcriptions.create(
            model="whisper-1", file=audio, language="pt", response_format="text"
        )
    except Exception:
        raise ValueError("Não consegui transcrever o áudio agora. Tente novamente ou envie uma mensagem de texto.") from None
    text = str(result).strip()
    if not text:
        raise ValueError("Não consegui entender o áudio. Tente gravar novamente.")
    if len(text) > 600:
        raise ValueError("O áudio ficou longo demais. Envie uma mensagem mais curta.")
    return text
