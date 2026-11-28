from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from app.core.config import Settings
from app.services import email_service


def _assert_sisfarm_shell(html: str) -> None:
    assert "logo-wordmark.png" in html
    assert "Gestão agrícola inteligente" in html
    assert "Este é um e-mail automático do SiSFarm" in html
    assert "@media only screen and (max-width: 600px)" in html
    assert "-apple-system" in html


def test_default_smtp_from_name_is_sisfarm():
    assert Settings.model_fields["smtp_from_name"].default == "SiSFarm"


def test_all_otp_emails_support_apple_autofill_and_sisfarm_shell():
    otp_builders = [
        ("482916", email_service.build_access_code_email("482916", expires_in_minutes=10)),
        ("739204", email_service.build_signup_code_email("739204", first_name="Ana", expires_in_minutes=15)),
        ("615830", email_service.build_password_change_code_email("615830", expires_in_minutes=10)),
        (
            "84920173",
            email_service.build_document_signing_electronic_otp_email(
                code="84920173",
                doc_title="Contrato de Safra",
                signer_name="Fernanda Alves",
                masked_cpf="***.456.789-**",
                sha256="a" * 64,
                expires_minutes=10,
            ),
        ),
        (
            "52048196",
            email_service.build_document_signing_icp_otp_email(
                code="52048196",
                filename="laudo.pdf",
                holder="CARLOS SILVA",
                sha256="b" * 64,
                expires_minutes=10,
            ),
        ),
    ]

    for code, (subject, text_body, html_body) in otp_builders:
        assert subject.startswith(code), f"Assunto deve começar com o código OTP: {subject}"
        assert f"#{code}" in text_body
        assert f">{code}<" in html_body
        _assert_sisfarm_shell(html_body)


def test_all_transactional_and_team_emails_render_clean_html():
    samples = [
        email_service.build_password_reset_email("https://app.sisfarm.com.br/login/redefinir-senha?token=abc", 60),
        email_service.build_bootstrap_admin_password_email("admin@sisfarm.com.br", "Tmp#12345"),
        email_service.build_document_signing_internal_invite_email(
            collaborator_name="Rafael",
            sender_name="Carlos",
            doc_title="Contrato 2026",
            farm_name="Fazenda Bela Vista",
            doc_url="https://app.sisfarm.com.br/assinatura-digital/documentos/1",
        ),
        email_service.build_document_signing_external_invite_email(
            signer_name="Fernanda",
            sender_name="Carlos",
            farm_name="Fazenda Bela Vista",
            doc_title="Contrato 2026",
            sign_url="https://app.sisfarm.com.br/assinar/tok",
            expires_days=14,
        ),
        email_service.build_trial_notice_email(
            "teste_3_dias",
            organization_name="Sítio Serra Alta",
            ends_on_str="30/11/2026",
            link="https://app.sisfarm.com.br/planos-disponiveis",
        ),
        email_service.build_trial_notice_email(
            "teste_fim",
            organization_name="Sítio Serra Alta",
            ends_on_str="30/11/2026",
            link="https://app.sisfarm.com.br/planos-disponiveis",
        ),
        email_service.build_plan_limits_notice_email(
            organization_name="Fazenda Bela Vista",
            plan_name="Essencial",
            transition_text="O teste termina em 30/11/2026.",
            link="https://app.sisfarm.com.br/minha-assinatura#regularizacao-plano",
        ),
        email_service.build_pix_renewal_reminder_email(
            "pix_renovacao_hoje",
            organization_name="Fazenda Bela Vista",
            plan_name="Profissional",
            due_str="27/11/2026",
            value_str="R$ 199,00",
            link="https://app.sisfarm.com.br/minha-assinatura",
        ),
        email_service.build_billing_notice_email(
            "pre_restricao",
            organization_name="Fazenda Bela Vista",
            deadline_str="07/12/2026",
            link="https://app.sisfarm.com.br/minha-assinatura",
            value_str="R$ 199,00",
            due_str="27/11/2026",
            pay_url="https://pagar.exemplo/123",
        ),
        email_service.build_billing_notice_email(
            "atraso",
            organization_name="Fazenda Bela Vista",
            deadline_str="07/12/2026",
            link="https://app.sisfarm.com.br/minha-assinatura",
            read_only=True,
            value_str="R$ 199,00",
            due_str="27/11/2026",
        ),
        email_service.build_new_signup_team_email(
            organization_name="Sítio Serra Alta",
            user_name="Mariana Costa",
            user_email="mariana@sitioserraalta.com.br",
            user_phone="(35) 99876-5432",
        ),
        email_service.build_plan_upgrade_request_email(
            organization_name="Fazenda Bela Vista",
            plan_name="Profissional",
            requester_email="carlos@fazendabelavista.com.br",
            module_label="Assinatura Digital",
        ),
    ]

    for subject, text_body, html_body in samples:
        assert subject.strip()
        assert text_body.strip()
        _assert_sisfarm_shell(html_body)


def test_send_access_code_email_dispatches_multipart_html(monkeypatch):
    sent = {}

    class DummySMTP:
        def __init__(self, host, port, timeout=20):
            pass

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
        whatsapp_display_number="(35) 99999-0000",
        two_factor_code_minutes=10,
    )
    monkeypatch.setattr(email_service, "get_settings", lambda: fake_settings)
    monkeypatch.setattr(email_service.smtplib, "SMTP", DummySMTP)

    email_service.send_access_code_email("produtor@fazenda.com.br", "482916")
    msg = sent["message"]
    assert msg["From"] == "SiSFarm <nao-responda@sisfarm.com.br>"
    assert msg["Subject"].startswith("482916")
    assert msg.is_multipart()
    parts = {part.get_content_type(): part.get_content() for part in msg.walk() if not part.is_multipart()}
    assert "text/plain" in parts
    assert "text/html" in parts
    assert "@app.sisfarm.com.br #482916" in parts["text/plain"]
    _assert_sisfarm_shell(parts["text/html"])


def test_otp_inputs_have_one_time_code_autocomplete():
    for template_path in (
        "app/templates/login_verify.html",
        "app/templates/profile.html",
        "app/templates/signup.html",
        "app/templates/users.html",
        "app/templates/digital_signing_workspace.html",
        "app/templates/digital_signing_external.html",
    ):
        content = Path(template_path).read_text(encoding="utf-8")
        assert 'autocomplete="one-time-code"' in content, f"Faltando autocomplete=one-time-code em {template_path}"
