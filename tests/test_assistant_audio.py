from types import SimpleNamespace

import pytest

from app.services import assistant_audio


def test_transcription_uses_audio_and_returns_text(monkeypatch):
    import openai
    calls = []
    monkeypatch.setattr(assistant_audio, "get_settings", lambda: SimpleNamespace(openai_api_key="test", openai_timeout_seconds=10))
    monkeypatch.setattr(openai, "OpenAI", lambda **kwargs: SimpleNamespace(audio=SimpleNamespace(
        transcriptions=SimpleNamespace(create=lambda **args: calls.append(args) or "Comprei gasolina")
    )))
    assert assistant_audio.transcribe_audio(b"audio", "audio/ogg") == "Comprei gasolina"
    assert calls[0]["file"].name == "voz.ogg" and calls[0]["language"] == "pt"


@pytest.mark.parametrize("data,mime", [(b"", "audio/ogg"), (b"x", "application/pdf"),
                                     (b"x" * (assistant_audio.MAX_AUDIO_BYTES + 1), "audio/ogg")])
def test_rejects_invalid_audio(data, mime):
    with pytest.raises(ValueError):
        assistant_audio.transcribe_audio(data, mime)
