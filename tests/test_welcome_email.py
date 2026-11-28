from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.db.base import Base
from app.models import Organization, User
from app.services import email_service
from app.web import routes


def _make_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


def _make_request(path: str, query_string: str = "") -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": query_string.encode("utf-8"),
        "headers": [],
        "session": {},
        "client": ("127.0.0.1", 12345),
        "scheme": "https",
        "server": ("app.sisfarm.com.br", 443),
    }
    return Request(scope)


def test_welcome_email_static_assets_exist():
    assert Path("app/static/images/logo-wordmark.png").is_file()
    assert Path("app/static/img/pezinho/icone-cabecalho.png").is_file()
    email_dir = Path("app/static/images/email")
    for filename in (
        "icon-whatsapp.png",
        "icon-mail.png",
    ):
        asset = email_dir / filename
        assert asset.is_file() and asset.stat().st_size > 500, f"Asset ausente ou vazio: {asset}"


def test_build_welcome_email_content_personalizes_and_escapes():
    subject, text_body, html_body = email_service.build_welcome_email_content(
        display_name="João <script>alert(1)</script>",
        full_name="João Silva",
        organization_name='Fazenda "Bela Vista" & Cia',
        trial_days=14,
        base_url="https://app.sisfarm.com.br/",
        support_email="suporte@sisfarm.com.br",
        whatsapp_display="(35) 99999-0000",
    )

    assert subject == "Bem-vindo ao SiSFarm, João! Sua conta está pronta"
    assert "Olá, João!" in text_body
    assert "Pezinho" in text_body
    assert "Gestão agrícola inteligente" in text_body
    assert "https://app.sisfarm.com.br/primeiros-passos" in text_body
    assert "https://app.sisfarm.com.br/mobile/assistente" in text_body
    assert "https://app.sisfarm.com.br/fazendas" in text_body
    assert "https://app.sisfarm.com.br/producao" in text_body
    assert "https://app.sisfarm.com.br/insumos/estoque" in text_body
    assert "https://app.sisfarm.com.br/gestao-financeira" in text_body

    assert "<script>" not in html_body
    assert "Fazenda &#34;Bela Vista&#34; &amp; Cia" in html_body
    assert "https://app.sisfarm.com.br/static/images/logo-wordmark.png" in html_body
    assert "https://app.sisfarm.com.br/static/img/pezinho/icone-cabecalho.png" in html_body
    assert "https://app.sisfarm.com.br/static/images/email/icon-whatsapp.png" in html_body
    assert "https://app.sisfarm.com.br/static/images/email/icon-mail.png" in html_body
    assert "Gestão agrícola inteligente" in html_body
    assert "Este é um e-mail automático do SiSFarm" in html_body
    assert "suporte@sisfarm.com.br" in html_body
    assert "(35) 99999-0000" in html_body


def test_send_welcome_email_sends_multipart_html_and_reply_to(monkeypatch):
    sent = {}

    class DummySMTP:
        def __init__(self, host, port, timeout=20):
            sent["host"] = host
            sent["port"] = port

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def ehlo(self):
            pass

        def starttls(self):
            pass

        def login(self, user, password):
            pass

        def send_message(self, message, from_addr=None, to_addrs=None):
            sent["message"] = message
            sent["from_addr"] = from_addr
            sent["to_addrs"] = to_addrs

    fake_settings = SimpleNamespace(
        smtp_host="smtp.exemplo.local",
        smtp_port=587,
        smtp_use_tls=True,
        smtp_username="mailer",
        smtp_password="secret",
        smtp_from_email="nao-responda@sisfarm.com.br",
        smtp_from_name="SiSFarm",
        app_public_url="https://app.sisfarm.com.br",
        support_email="suporte@sisfarm.com.br",
        whatsapp_display_number=None,
    )
    monkeypatch.setattr(email_service, "get_settings", lambda: fake_settings)
    monkeypatch.setattr(email_service.smtplib, "SMTP", DummySMTP)

    email_service.send_welcome_email(
        recipient_email="joao@fazendabelavista.com.br",
        display_name="João",
        full_name="João Silva",
        organization_name="Fazenda Bela Vista",
    )

    msg = sent["message"]
    assert msg["To"] == "joao@fazendabelavista.com.br"
    assert msg["Reply-To"] == "suporte@sisfarm.com.br"
    assert "João" in msg["Subject"]
    assert msg.is_multipart()
    parts = {part.get_content_type(): part.get_content() for part in msg.walk() if not part.is_multipart()}
    assert "text/plain" in parts
    assert "text/html" in parts
    assert "Fazenda Bela Vista" in parts["text/html"]
    assert "Conheça o Pezinho" in parts["text/html"]


def test_onboarding_page_triggers_welcome_email_once_on_cadastro_concluido(monkeypatch):
    db = _make_session()
    org = Organization(name="Fazenda Bela Vista", slug="fazenda-bela-vista", is_active=True)
    db.add(org)
    db.flush()
    user = User(
        name="João Silva",
        display_name="João",
        email="joao@fazendabelavista.com.br",
        hashed_password="hash",
        is_active=True,
        is_admin=True,
        organization_id=org.id,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    sent_calls = []
    monkeypatch.setattr(
        email_service,
        "send_welcome_email",
        lambda **kwargs: sent_calls.append(kwargs),
    )
    monkeypatch.setattr(routes, "_render_onboarding", lambda request, db, user, csrf_token: HTMLResponse("ok"))

    # 1. Visita sem cadastro=concluido -> nao envia
    resp0 = routes.onboarding_page(_make_request("/primeiros-passos"), db=db, user=user, csrf_token="tok")
    assert resp0.background is None
    assert user.welcome_email_sent_at is None

    # 2. Visita com cadastro=concluido -> agenda envio em background e grava welcome_email_sent_at
    resp1 = routes.onboarding_page(
        _make_request("/primeiros-passos", "cadastro=concluido"),
        db=db,
        user=user,
        csrf_token="tok",
    )
    assert resp1.background is not None
    assert user.welcome_email_sent_at is not None
    # Executa a task em background
    resp1.background.func(*resp1.background.args, **resp1.background.kwargs)
    assert len(sent_calls) == 1
    assert sent_calls[0]["recipient_email"] == "joao@fazendabelavista.com.br"
    assert sent_calls[0]["display_name"] == "João"
    assert sent_calls[0]["organization_name"] == "Fazenda Bela Vista"

    # 3. F5 / nova visita com cadastro=concluido -> nao reenvia
    resp2 = routes.onboarding_page(
        _make_request("/primeiros-passos", "cadastro=concluido"),
        db=db,
        user=user,
        csrf_token="tok",
    )
    assert resp2.background is None
    assert len(sent_calls) == 1


def test_welcome_email_safe_swallows_smtp_errors(monkeypatch):
    def boom(**kwargs):
        raise RuntimeError("SMTP fora do ar")

    monkeypatch.setattr(email_service, "send_welcome_email", boom)
    # Nao deve lancar excecao
    routes._send_welcome_email_safe(
        "joao@fazendabelavista.com.br",
        "João",
        "João Silva",
        "Fazenda Bela Vista",
        14,
    )
