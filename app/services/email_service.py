from __future__ import annotations

import logging
import re
import smtplib
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from jinja2 import Environment, FileSystemLoader, select_autoescape

from app.core.config import get_settings
from app.core.timezone import format_app_datetime

logger = logging.getLogger(__name__)

_EMAIL_TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates" / "emails"
_email_jinja_env = Environment(
    loader=FileSystemLoader(str(_EMAIL_TEMPLATES_DIR)),
    autoescape=select_autoescape(["html", "xml"]),
)

_BADGE_TONES: dict[str, dict[str, str]] = {
    "brand": {"bg": "#EEF3EB", "text": "#294127", "border": "#D5E3CE"},
    "success": {"bg": "#E6F4E2", "text": "#1F7A3A", "border": "#C4E6BC"},
    "info": {"bg": "#E8F1FB", "text": "#1F5FA8", "border": "#C6DCF5"},
    "warning": {"bg": "#FFF6E5", "text": "#7A4F00", "border": "#F0D9A6"},
    "danger": {"bg": "#FDECEA", "text": "#B42318", "border": "#F5C2BE"},
    "neutral": {"bg": "#F0F3EF", "text": "#435048", "border": "#DCE2DB"},
}

_DETAIL_TONES: dict[str, dict[str, str]] = {
    "soft": {"bg": "#F6F8F5", "border": "#E3E8E2"},
    "warning": {"bg": "#FFFBF2", "border": "#F0D9A6"},
    "danger": {"bg": "#FEF6F5", "border": "#F5C2BE"},
}

_CALLOUT_TONES: dict[str, dict[str, str]] = {
    "brand": {"bg": "#EEF3EB", "border": "#D5E3CE", "text": "#294127"},
    "warning": {"bg": "#FFF6E5", "border": "#F0D9A6", "text": "#7A4F00"},
    "danger": {"bg": "#FDECEA", "border": "#F5C2BE", "text": "#912018"},
    "info": {"bg": "#E8F1FB", "border": "#C6DCF5", "text": "#1F5FA8"},
}

_CTA_TONES: dict[str, dict[str, str]] = {
    "brand": {"bg": "#446A36", "text": "#FFFFFF"},
    "danger": {"bg": "#B42318", "text": "#FFFFFF"},
    "dark": {"bg": "#16201A", "text": "#FFFFFF"},
}


def _generic_error_message(error_context: str) -> str:
    if error_context == "password_reset":
        return "Nao foi possivel enviar o email de redefinicao."
    if error_context == "welcome":
        return "Nao foi possivel enviar o email de boas-vindas."
    if error_context == "backup_failure":
        return "Não foi possível enviar o alerta de falha no backup."
    return "Nao foi possivel enviar o email de verificacao."


def _resolve_email_env(
    *,
    base_url: str | None = None,
    support_email: str | None = None,
    whatsapp_display: str | None = None,
) -> dict[str, str]:
    settings = get_settings()
    public_url = (base_url or getattr(settings, "app_public_url", None) or "https://app.sisfarm.com.br").rstrip("/")
    parsed = urlparse(public_url)
    public_host = parsed.netloc or public_url.removeprefix("https://").removeprefix("http://")
    resolved_support = (
        support_email if support_email is not None else (getattr(settings, "support_email", None) or "")
    ).strip()
    resolved_whatsapp = (
        whatsapp_display if whatsapp_display is not None else (getattr(settings, "whatsapp_display_number", None) or "")
    ).strip()
    digits = re.sub(r"\D", "", resolved_whatsapp)
    if digits:
        wa_digits = digits if digits.startswith("55") else (f"55{digits}" if len(digits) in (10, 11) else digits)
        whatsapp_href = f"https://wa.me/{wa_digits}"
    else:
        whatsapp_href = ""
    return {
        "base_url": public_url,
        "public_host": public_host,
        "support_email": resolved_support,
        "whatsapp_display": resolved_whatsapp,
        "whatsapp_href": whatsapp_href,
    }


def _render_transactional_html(
    *,
    subject: str,
    preheader: str = "",
    badge_label: str = "SiSFarm",
    badge_tone: str = "brand",
    eyebrow: str = "",
    title: str,
    intro_paragraphs: list[str],
    otp: dict[str, str] | None = None,
    details: list[dict[str, Any]] | None = None,
    details_tone: str = "soft",
    callout: dict[str, str] | None = None,
    callout_tone: str = "brand",
    cta: dict[str, str] | None = None,
    cta_tone: str = "brand",
    secondary_cta: dict[str, str] | None = None,
    fallback_url: str = "",
    footnotes: list[str] | None = None,
    context_line: str = "",
    show_support: bool = False,
    is_internal: bool = False,
    base_url: str | None = None,
    support_email: str | None = None,
    whatsapp_display: str | None = None,
) -> str:
    env = _resolve_email_env(
        base_url=base_url,
        support_email=support_email,
        whatsapp_display=whatsapp_display,
    )
    return _email_jinja_env.get_template("transactional.html").render(
        subject=subject,
        preheader=preheader,
        badge_label=badge_label,
        badge=_BADGE_TONES.get(badge_tone, _BADGE_TONES["brand"]),
        eyebrow=eyebrow,
        title=title,
        intro_paragraphs=intro_paragraphs,
        otp=otp,
        details=details or [],
        details_style=_DETAIL_TONES.get(details_tone, _DETAIL_TONES["soft"]),
        callout=callout,
        callout_style=_CALLOUT_TONES.get(callout_tone, _CALLOUT_TONES["brand"]),
        cta=cta,
        cta_style=_CTA_TONES.get(cta_tone, _CTA_TONES["brand"]),
        secondary_cta=secondary_cta,
        fallback_url=fallback_url,
        footnotes=footnotes or [],
        context_line=context_line,
        show_support=show_support,
        is_internal=is_internal,
        **env,
    )


def _send_email(
    recipient_email: str,
    subject: str,
    body: str,
    error_context: str,
    *,
    html_body: str | None = None,
    reply_to: str | None = None,
) -> None:
    settings = get_settings()
    if not settings.smtp_host or not settings.smtp_from_email:
        raise RuntimeError("Servico de email nao configurado.")

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = f"{settings.smtp_from_name} <{settings.smtp_from_email}>"
    message["To"] = recipient_email
    if reply_to:
        message["Reply-To"] = reply_to
    message.set_content(body)
    if html_body:
        message.add_alternative(html_body, subtype="html")

    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=20) as server:
            server.ehlo()
            if settings.smtp_use_tls:
                server.starttls()
                server.ehlo()
            if settings.smtp_username and settings.smtp_password:
                server.login(settings.smtp_username, settings.smtp_password)
            server.send_message(
                message,
                from_addr=settings.smtp_from_email,
                to_addrs=[recipient_email],
            )
    except smtplib.SMTPAuthenticationError as exc:
        logger.exception(
            "Falha de autenticacao SMTP ao enviar email",
            extra={
                "context": error_context,
                "smtp_host": settings.smtp_host,
                "smtp_port": settings.smtp_port,
                "smtp_username": settings.smtp_username,
                "smtp_from_email": settings.smtp_from_email,
                "recipient_email": recipient_email,
            },
        )
        raise RuntimeError("Falha de autenticacao no servico de email.") from exc
    except smtplib.SMTPRecipientsRefused as exc:
        logger.exception(
            "Destinatario recusado pelo SMTP ao enviar email",
            extra={
                "context": error_context,
                "smtp_host": settings.smtp_host,
                "smtp_port": settings.smtp_port,
                "smtp_from_email": settings.smtp_from_email,
                "recipient_email": recipient_email,
            },
        )
        raise RuntimeError(_generic_error_message(error_context)) from exc
    except smtplib.SMTPSenderRefused as exc:
        logger.exception(
            "Remetente recusado pelo SMTP ao enviar email",
            extra={
                "context": error_context,
                "smtp_host": settings.smtp_host,
                "smtp_port": settings.smtp_port,
                "smtp_username": settings.smtp_username,
                "smtp_from_email": settings.smtp_from_email,
                "recipient_email": recipient_email,
            },
        )
        raise RuntimeError("Remetente de email invalido ou nao autorizado.") from exc
    except smtplib.SMTPDataError as exc:
        logger.exception(
            "Servico SMTP recusou a mensagem enviada",
            extra={
                "context": error_context,
                "smtp_host": settings.smtp_host,
                "smtp_port": settings.smtp_port,
                "smtp_username": settings.smtp_username,
                "smtp_from_email": settings.smtp_from_email,
                "recipient_email": recipient_email,
            },
        )
        raise RuntimeError("Servico de email recusou a mensagem enviada.") from exc
    except (smtplib.SMTPConnectError, smtplib.SMTPServerDisconnected, TimeoutError, OSError) as exc:
        logger.exception(
            "Falha de conexao SMTP ao enviar email",
            extra={
                "context": error_context,
                "smtp_host": settings.smtp_host,
                "smtp_port": settings.smtp_port,
                "smtp_username": settings.smtp_username,
                "smtp_from_email": settings.smtp_from_email,
                "recipient_email": recipient_email,
                "smtp_use_tls": settings.smtp_use_tls,
            },
        )
        raise RuntimeError("Nao foi possivel conectar ao servico de email.") from exc
    except smtplib.SMTPException as exc:
        logger.exception(
            "Falha SMTP ao enviar email",
            extra={
                "context": error_context,
                "smtp_host": settings.smtp_host,
                "smtp_port": settings.smtp_port,
                "smtp_username": settings.smtp_username,
                "smtp_from_email": settings.smtp_from_email,
                "recipient_email": recipient_email,
                "smtp_use_tls": settings.smtp_use_tls,
            },
        )
        raise RuntimeError(_generic_error_message(error_context)) from exc


# =============================================================================
# 1. ACESSO, CADASTRO E SEGURANÇA
# =============================================================================


def build_access_code_email(
    code: str,
    *,
    expires_in_minutes: int | None = None,
    base_url: str | None = None,
) -> tuple[str, str, str]:
    settings = get_settings()
    ttl = expires_in_minutes if expires_in_minutes is not None else getattr(settings, "two_factor_code_minutes", 10)
    env = _resolve_email_env(base_url=base_url)
    clean_code = str(code).strip()
    subject = f"{clean_code} é o seu código de verificação do SiSFarm"
    text_body = (
        f"{clean_code} é o seu código de verificação do SiSFarm.\n\n"
        f"Use este código para confirmar seu acesso ao SiSFarm:\n{clean_code}\n\n"
        f"Este código vale por {ttl} minutos e só pode ser usado uma vez.\n"
        "Se você não tentou entrar na sua conta agora, ignore este e-mail.\n\n"
        f"@{env['public_host']} #{clean_code}\n\n"
        "SiSFarm · Gestão agrícola inteligente\n"
        f"{env['base_url']}\n"
    )
    html_body = _render_transactional_html(
        subject=subject,
        preheader=f"Use o código {clean_code} para confirmar seu acesso. Ele vale por {ttl} minutos.",
        badge_label="Segurança",
        badge_tone="brand",
        eyebrow="Verificação em duas etapas",
        title="Seu código de acesso",
        intro_paragraphs=["Use o código de verificação abaixo para confirmar sua entrada no SiSFarm."],
        otp={
            "code": clean_code,
            "label": "Código de verificação",
            "expiry": f"Válido por {ttl} minutos · Uso único",
        },
        footnotes=[
            "Se você não tentou entrar na sua conta agora, ignore este e-mail. Nunca compartilhe este código com outras pessoas."
        ],
        show_support=False,
        base_url=base_url,
    )
    return subject, text_body, html_body


def send_access_code_email(recipient_email: str, code: str, expires_in_minutes: int | None = None) -> None:
    subject, text_body, html_body = build_access_code_email(code, expires_in_minutes=expires_in_minutes)
    _send_email(
        recipient_email=recipient_email,
        subject=subject,
        body=text_body,
        html_body=html_body,
        error_context="two_factor",
    )


def build_signup_code_email(
    code: str,
    *,
    first_name: str | None = None,
    is_resend: bool = False,
    expires_in_minutes: int = 15,
    base_url: str | None = None,
) -> tuple[str, str, str]:
    env = _resolve_email_env(base_url=base_url)
    clean_code = str(code).strip()
    greeting = f"Olá, {first_name.strip()}! " if (first_name or "").strip() else "Olá! "
    subject = f"{clean_code} é o seu código de verificação do SiSFarm"
    lead = (
        f"{greeting}Enviamos um novo código de verificação para você concluir a criação da sua conta no SiSFarm:"
        if is_resend
        else f"{greeting}Falta só um passo para criar sua conta no SiSFarm. Digite o código de verificação abaixo na tela de cadastro:"
    )
    text_body = (
        f"{greeting}\n\n"
        f"{clean_code} é o seu código de verificação para criar sua conta no SiSFarm.\n\n"
        f"Código de verificação: {clean_code}\n"
        f"Este código vale por {expires_in_minutes} minutos.\n\n"
        "Se você não pediu este cadastro, ignore este e-mail.\n\n"
        f"@{env['public_host']} #{clean_code}\n\n"
        "SiSFarm · Gestão agrícola inteligente\n"
        f"{env['base_url']}\n"
    )
    html_body = _render_transactional_html(
        subject=subject,
        preheader=f"Informe o código {clean_code} para concluir o cadastro da sua conta no SiSFarm.",
        badge_label="Cadastro",
        badge_tone="brand",
        eyebrow="Confirmação de e-mail",
        title="Confirme seu e-mail para começar",
        intro_paragraphs=[lead],
        otp={
            "code": clean_code,
            "label": "Código de verificação",
            "expiry": f"Válido por {expires_in_minutes} minutos",
        },
        footnotes=["Se você não solicitou a criação de uma conta no SiSFarm, pode ignorar este e-mail com segurança."],
        context_line=f"E-mail enviado para confirmar o cadastro iniciado em {env['public_host']}.",
        show_support=False,
        base_url=base_url,
    )
    return subject, text_body, html_body


def build_password_reset_email(
    reset_link: str,
    expires_in_minutes: int,
    *,
    base_url: str | None = None,
) -> tuple[str, str, str]:
    env = _resolve_email_env(base_url=base_url)
    subject = "Redefinição de senha da sua conta SiSFarm"
    text_body = (
        "Redefinição de senha no SiSFarm\n\n"
        "Recebemos um pedido para redefinir a senha da sua conta no SiSFarm.\n"
        f"Acesse o link abaixo em até {expires_in_minutes} minutos para escolher uma nova senha:\n\n"
        f"{reset_link}\n\n"
        "Se você não solicitou esta redefinição, ignore este e-mail — sua senha atual permanece inalterada.\n\n"
        "SiSFarm · Gestão agrícola inteligente\n"
        f"{env['base_url']}\n"
    )
    html_body = _render_transactional_html(
        subject=subject,
        preheader=f"Use o link seguro para criar uma nova senha. Válido por {expires_in_minutes} minutos.",
        badge_label="Segurança",
        badge_tone="brand",
        eyebrow="Recuperação de acesso",
        title="Redefinir sua senha",
        intro_paragraphs=[
            "Recebemos um pedido para redefinir a senha da sua conta no SiSFarm. Clique no botão abaixo para escolher uma nova senha."
        ],
        callout={
            "title": "Atenção:",
            "text": f"Este link é pessoal e expira em {expires_in_minutes} minutos.",
        },
        callout_tone="info",
        cta={"label": "Criar nova senha", "url": reset_link},
        cta_tone="brand",
        fallback_url=reset_link,
        footnotes=["Se você não pediu essa alteração, basta ignorar este e-mail — sua senha atual continua valendo."],
        show_support=False,
        base_url=base_url,
    )
    return subject, text_body, html_body


def send_password_reset_email(recipient_email: str, reset_link: str, expires_in_minutes: int) -> None:
    subject, text_body, html_body = build_password_reset_email(reset_link, expires_in_minutes)
    _send_email(
        recipient_email=recipient_email,
        subject=subject,
        body=text_body,
        html_body=html_body,
        error_context="password_reset",
    )


def build_password_change_code_email(
    code: str,
    expires_in_minutes: int,
    *,
    base_url: str | None = None,
) -> tuple[str, str, str]:
    env = _resolve_email_env(base_url=base_url)
    clean_code = str(code).strip()
    subject = f"{clean_code} é o seu código para alterar a senha no SiSFarm"
    text_body = (
        f"{clean_code} é o seu código de verificação para alterar a senha no SiSFarm.\n\n"
        "Recebemos uma solicitação para alterar a senha da sua conta.\n"
        f"Código de verificação: {clean_code}\n"
        f"Este código expira em {expires_in_minutes} minutos.\n\n"
        "Se você não iniciou essa alteração, ignore este e-mail.\n\n"
        f"@{env['public_host']} #{clean_code}\n\n"
        "SiSFarm · Gestão agrícola inteligente\n"
        f"{env['base_url']}\n"
    )
    html_body = _render_transactional_html(
        subject=subject,
        preheader=f"Confirme a alteração de senha informando o código {clean_code}. Válido por {expires_in_minutes} minutos.",
        badge_label="Segurança",
        badge_tone="warning",
        eyebrow="Alteração de senha",
        title="Confirme a troca da sua senha",
        intro_paragraphs=[
            "Recebemos uma solicitação para alterar a senha da sua conta no SiSFarm. Informe o código de verificação abaixo na tela de segurança do seu perfil:"
        ],
        otp={
            "code": clean_code,
            "label": "Código de verificação",
            "expiry": f"Válido por {expires_in_minutes} minutos · Uso único",
        },
        footnotes=["Se você não iniciou essa troca de senha, ignore este e-mail e verifique quem tem acesso à sua conta."],
        show_support=False,
        base_url=base_url,
    )
    return subject, text_body, html_body


def send_password_change_code_email(recipient_email: str, code: str, expires_in_minutes: int) -> None:
    subject, text_body, html_body = build_password_change_code_email(code, expires_in_minutes)
    _send_email(
        recipient_email=recipient_email,
        subject=subject,
        body=text_body,
        html_body=html_body,
        error_context="two_factor",
    )


def build_bootstrap_admin_password_email(
    recipient_email: str,
    password: str,
    *,
    base_url: str | None = None,
) -> tuple[str, str, str]:
    env = _resolve_email_env(base_url=base_url)
    subject = "SiSFarm: acesso administrativo de contingência criado"
    text_body = (
        "Acesso administrativo de contingência — SiSFarm\n\n"
        "Uma conta administrativa de contingência foi criada no SiSFarm.\n\n"
        f"Login: {recipient_email}\n"
        f"Senha temporária: {password}\n\n"
        "Ao acessar, mantenha o 2FA ativo e altere a senha imediatamente em Meu Perfil > Segurança.\n"
        f"{env['base_url']}/login\n"
    )
    html_body = _render_transactional_html(
        subject=subject,
        preheader="Credenciais temporárias da conta administrativa inicial. Altere a senha no primeiro acesso.",
        badge_label="Infraestrutura",
        badge_tone="danger",
        eyebrow="Segurança do sistema",
        title="Conta administrativa de contingência",
        intro_paragraphs=[
            "Uma conta administrativa de contingência foi provisionada automaticamente na inicialização do ambiente SiSFarm."
        ],
        details=[
            {"label": "Login", "value": recipient_email, "bold": True},
            {"label": "Senha temporária", "value": password, "mono": True, "bold": True},
            {"label": "2FA por e-mail", "value": "Ativo por padrão", "color": "#1F7A3A"},
        ],
        details_tone="danger",
        callout={
            "title": "Ação obrigatória:",
            "text": "Faça login imediatamente, mantenha a verificação em duas etapas (2FA) ativa e substitua esta senha temporária em Meu Perfil > Segurança.",
        },
        callout_tone="warning",
        cta={"label": "Acessar o SiSFarm →", "url": f"{env['base_url']}/login"},
        cta_tone="dark",
        context_line="E-mail restrito de provisionamento técnico do ambiente SiSFarm.",
        show_support=False,
        is_internal=True,
        base_url=base_url,
    )
    return subject, text_body, html_body


def send_bootstrap_admin_password_email(recipient_email: str, password: str) -> None:
    subject, text_body, html_body = build_bootstrap_admin_password_email(recipient_email, password)
    _send_email(
        recipient_email=recipient_email,
        subject=subject,
        body=text_body,
        html_body=html_body,
        error_context="two_factor",
    )


# =============================================================================
# 2. ASSINATURA DIGITAL DE DOCUMENTOS
# =============================================================================


def build_document_signing_internal_invite_email(
    *,
    collaborator_name: str,
    sender_name: str,
    doc_title: str,
    farm_name: str,
    doc_url: str,
    base_url: str | None = None,
) -> tuple[str, str, str]:
    env = _resolve_email_env(base_url=base_url)
    subject = f"Documento aguardando sua assinatura: {doc_title}"
    text_body = (
        f"Olá, {collaborator_name}.\n\n"
        f"{sender_name} compartilhou o documento \"{doc_title}\" ({farm_name}) para sua assinatura digital no SiSFarm.\n\n"
        f"Acesse o link abaixo para revisar e assinar:\n{doc_url}\n\n"
        "SiSFarm · Gestão agrícola inteligente\n"
        f"{env['base_url']}\n"
    )
    html_body = _render_transactional_html(
        subject=subject,
        preheader=f"{sender_name} compartilhou um documento de {farm_name} para sua assinatura.",
        badge_label="Assinatura digital",
        badge_tone="info",
        eyebrow="Documento compartilhado",
        title="Você tem um documento para assinar",
        intro_paragraphs=[
            f"Olá, {collaborator_name}. {sender_name} compartilhou um documento para sua revisão e assinatura digital no SiSFarm."
        ],
        details=[
            {"label": "Documento", "value": doc_title, "bold": True},
            {"label": "Fazenda", "value": farm_name},
            {"label": "Enviado por", "value": sender_name},
        ],
        cta={"label": "Revisar e assinar documento →", "url": doc_url},
        cta_tone="brand",
        fallback_url=doc_url,
        footnotes=["Você precisará estar conectado à sua conta SiSFarm para abrir e assinar este documento."],
        context_line=f"Você recebeu este e-mail por estar vinculado a {farm_name} no SiSFarm.",
        show_support=True,
        base_url=base_url,
    )
    return subject, text_body, html_body


def build_document_signing_external_invite_email(
    *,
    signer_name: str,
    sender_name: str,
    farm_name: str,
    doc_title: str,
    sign_url: str,
    expires_days: int,
    base_url: str | None = None,
) -> tuple[str, str, str]:
    env = _resolve_email_env(base_url=base_url)
    subject = f"Convite para assinar documento: {doc_title}"
    text_body = (
        f"Olá, {signer_name}.\n\n"
        f"{sender_name} ({farm_name}) enviou o documento \"{doc_title}\" para sua assinatura eletrônica no SiSFarm.\n"
        "Você não precisa criar conta nem ter certificado digital.\n\n"
        f"Acesse seu link exclusivo para revisar e assinar:\n{sign_url}\n\n"
        f"Link válido por {expires_days} dias.\n\n"
        "SiSFarm · Gestão agrícola inteligente\n"
        f"{env['base_url']}\n"
    )
    html_body = _render_transactional_html(
        subject=subject,
        preheader=f"{sender_name} ({farm_name}) enviou um documento para sua assinatura eletrônica.",
        badge_label="Assinatura eletrônica",
        badge_tone="info",
        eyebrow="Convite de assinatura",
        title="Documento enviado para sua assinatura",
        intro_paragraphs=[
            f"Olá, {signer_name}. {sender_name} ({farm_name}) enviou o documento abaixo para sua assinatura eletrônica pelo SiSFarm."
        ],
        details=[
            {"label": "Documento", "value": doc_title, "bold": True},
            {"label": "Remetente", "value": f"{sender_name} · {farm_name}"},
            {"label": "Validade do link", "value": f"{expires_days} dias"},
        ],
        callout={
            "title": "Simples e seguro:",
            "text": "Você não precisa criar conta nem ter certificado digital. Na hora de assinar, enviaremos um código de confirmação para validar sua assinatura.",
        },
        callout_tone="brand",
        cta={"label": "Abrir documento para assinar →", "url": sign_url},
        cta_tone="brand",
        fallback_url=sign_url,
        footnotes=[f"Este link é exclusivo para você e vale por {expires_days} dias. Não encaminhe para terceiros."],
        context_line=f"Convite de assinatura eletrônica enviado por {farm_name} através do SiSFarm.",
        show_support=True,
        base_url=base_url,
    )
    return subject, text_body, html_body


def build_document_signing_electronic_otp_email(
    *,
    code: str,
    doc_title: str,
    signer_name: str,
    masked_cpf: str,
    sha256: str,
    expires_minutes: int,
    base_url: str | None = None,
) -> tuple[str, str, str]:
    env = _resolve_email_env(base_url=base_url)
    clean_code = str(code).strip()
    subject = f"{clean_code} é o código para assinar seu documento no SiSFarm"
    text_body = (
        f"{clean_code} é o seu código de verificação para assinar documento no SiSFarm.\n\n"
        f"Código de autorização: {clean_code}\n"
        f"Válido por {expires_minutes} minutos exclusivamente para esta versão do documento.\n\n"
        f"Documento: {doc_title}\n"
        f"Signatário: {signer_name} (CPF {masked_cpf})\n"
        f"SHA-256: {sha256}\n\n"
        "Informe o código somente na tela do SiSFarm onde você iniciou a assinatura.\n\n"
        f"@{env['public_host']} #{clean_code}\n"
    )
    html_body = _render_transactional_html(
        subject=subject,
        preheader=f"Informe o código {clean_code} na tela de assinatura do documento {doc_title}.",
        badge_label="Código de assinatura",
        badge_tone="info",
        eyebrow="Autorização de assinatura eletrônica",
        title="Confirme sua assinatura",
        intro_paragraphs=[
            "Use o código de verificação abaixo somente na tela do SiSFarm onde você iniciou a assinatura deste documento."
        ],
        otp={
            "code": clean_code,
            "label": "Código de verificação (8 dígitos)",
            "expiry": f"Válido por {expires_minutes} minutos · Exclusivo para esta versão",
        },
        details=[
            {"label": "Documento", "value": doc_title, "bold": True},
            {"label": "Signatário", "value": f"{signer_name} (CPF {masked_cpf})"},
            {"label": "Integridade (SHA-256)", "value": sha256, "mono": True},
        ],
        footnotes=[
            "Se você não solicitou este código para assinar o documento acima, ignore esta mensagem e não compartilhe o número."
        ],
        context_line="Código de validação gerado para assinatura eletrônica no SiSFarm.",
        show_support=False,
        base_url=base_url,
    )
    return subject, text_body, html_body


def build_document_signing_icp_otp_email(
    *,
    code: str,
    filename: str,
    holder: str,
    sha256: str,
    expires_minutes: int,
    base_url: str | None = None,
) -> tuple[str, str, str]:
    env = _resolve_email_env(base_url=base_url)
    clean_code = str(code).strip()
    subject = f"{clean_code} é o código para assinar seu PDF no SiSFarm"
    text_body = (
        f"{clean_code} é o seu código de verificação para assinar PDF no SiSFarm.\n\n"
        f"Código de autorização: {clean_code}\n"
        f"Válido por {expires_minutes} minutos e exclusivamente para esta versão do PDF.\n\n"
        f"Documento: {filename}\n"
        f"Titular do certificado: {holder}\n"
        f"SHA-256: {sha256}\n\n"
        "Insira o código somente na tela do SiSFarm em que você iniciou a assinatura.\n\n"
        f"@{env['public_host']} #{clean_code}\n"
    )
    html_body = _render_transactional_html(
        subject=subject,
        preheader=f"Autorize a assinatura digital com certificado ICP-Brasil usando o código {clean_code}.",
        badge_label="ICP-Brasil",
        badge_tone="brand",
        eyebrow="Assinatura com certificado digital",
        title="Confirme a assinatura do PDF",
        intro_paragraphs=[
            "Informe o código de verificação abaixo para autorizar o uso do seu certificado digital nesta versão do documento."
        ],
        otp={
            "code": clean_code,
            "label": "Código de autorização (8 dígitos)",
            "expiry": f"Válido por {expires_minutes} minutos · Uso único para esta versão",
        },
        details=[
            {"label": "Arquivo PDF", "value": filename, "bold": True},
            {"label": "Titular do certificado", "value": holder},
            {"label": "SHA-256 do PDF", "value": sha256, "mono": True},
        ],
        footnotes=[
            "Insira o código somente na tela do SiSFarm em que você iniciou a assinatura. Nunca repasse este código a terceiros."
        ],
        show_support=False,
        base_url=base_url,
    )
    return subject, text_body, html_body


# =============================================================================
# 3. ASSINATURA, TESTE GRÁTIS E COBRANÇA
# =============================================================================


def build_trial_notice_email(
    kind: str,
    *,
    organization_name: str,
    ends_on_str: str,
    link: str,
    base_url: str | None = None,
) -> tuple[str, str, str]:
    ended = kind != "teste_3_dias"
    if not ended:
        subject = f"Seu teste grátis do SiSFarm termina em {ends_on_str}"
        text_body = (
            "Olá!\n\n"
            f"O teste grátis da {organization_name} no SiSFarm termina em {ends_on_str}.\n"
            f"Para continuar usando sem interrupção, contrate seu plano:\n{link}\n\n"
            "Seus cadastros continuam guardados. Qualquer dúvida, é só responder este e-mail.\n\n"
            "SiSFarm · Gestão agrícola inteligente\n"
        )
        html_body = _render_transactional_html(
            subject=subject,
            preheader=f"Contrate seu plano antes de {ends_on_str} para continuar usando o SiSFarm sem interrupção.",
            badge_label="Teste grátis",
            badge_tone="brand",
            eyebrow="Período de avaliação",
            title="Faltam poucos dias para o fim do seu teste grátis",
            intro_paragraphs=[
                f"O período de teste gratuito de {organization_name} no SiSFarm termina em {ends_on_str}."
            ],
            details=[
                {"label": "Organização", "value": organization_name, "bold": True},
                {"label": "Fim do teste grátis", "value": ends_on_str, "color": "#7A4F00", "bold": True},
                {"label": "Seus cadastros", "value": "Mantidos integralmente ao contratar"},
            ],
            details_tone="warning",
            cta={"label": "Escolher meu plano →", "url": link},
            cta_tone="brand",
            footnotes=[
                "Se tiver qualquer dúvida sobre qual plano se encaixa melhor no tamanho da sua fazenda, responda este e-mail ou fale com nossa equipe."
            ],
            context_line=f"Aviso enviado aos administradores de {organization_name} no SiSFarm.",
            show_support=True,
            base_url=base_url,
        )
        return subject, text_body, html_body

    subject = "Seu teste grátis do SiSFarm terminou"
    text_body = (
        "Olá!\n\n"
        f"O teste grátis da {organization_name} no SiSFarm terminou em {ends_on_str}.\n"
        "O sistema segue disponível para consulta e nenhum dado foi apagado.\n"
        f"Para voltar a cadastrar e editar, contrate seu plano:\n{link}\n\n"
        "SiSFarm · Gestão agrícola inteligente\n"
    )
    html_body = _render_transactional_html(
        subject=subject,
        preheader="Seus dados continuam salvos para consulta. Escolha um plano para voltar a cadastrar e editar.",
        badge_label="Teste encerrado",
        badge_tone="warning",
        eyebrow="Período de avaliação",
        title="Seu teste grátis chegou ao fim",
        intro_paragraphs=[f"O período gratuito de {organization_name} no SiSFarm terminou em {ends_on_str}."],
        callout={
            "title": "Seus dados estão seguros:",
            "text": "Nada foi apagado e você continua podendo consultar tudo normalmente. Para voltar a cadastrar e editar informações, basta escolher seu plano.",
        },
        callout_tone="brand",
        cta={"label": "Escolher meu plano →", "url": link},
        cta_tone="brand",
        footnotes=[
            "Se tiver qualquer dúvida sobre qual plano se encaixa melhor no tamanho da sua fazenda, responda este e-mail ou fale com nossa equipe."
        ],
        context_line=f"Aviso enviado aos administradores de {organization_name} no SiSFarm.",
        show_support=True,
        base_url=base_url,
    )
    return subject, text_body, html_body


def build_plan_limits_notice_email(
    *,
    organization_name: str,
    plan_name: str,
    transition_text: str,
    link: str,
    base_url: str | None = None,
) -> tuple[str, str, str]:
    subject = f"Confira os limites do plano contratado pela {organization_name}"
    text_body = (
        f"Olá!\n\n{transition_text} Há cadastros acima dos limites do plano {plan_name}. "
        "Consulte os excedentes em Minha assinatura para ajustar seus cadastros ou escolher outro plano.\n\n"
        f"{link}\n\n"
        "SiSFarm · Gestão agrícola inteligente\n"
    )
    html_body = _render_transactional_html(
        subject=subject,
        preheader=f"Há cadastros acima dos limites do plano {plan_name}. Veja como regularizar em Minha assinatura.",
        badge_label="Atenção ao plano",
        badge_tone="warning",
        eyebrow="Ajuste de assinatura",
        title="Confira os limites do seu plano",
        intro_paragraphs=[
            f"{transition_text} Identificamos cadastros de {organization_name} acima dos limites do plano {plan_name}."
        ],
        details=[
            {"label": "Organização", "value": organization_name, "bold": True},
            {"label": "Plano contratado", "value": plan_name},
            {"label": "Ação recomendada", "value": "Ajustar cadastros ou escolher um plano maior", "color": "#7A4F00"},
        ],
        details_tone="warning",
        cta={"label": "Ver excedentes em Minha assinatura →", "url": link},
        cta_tone="brand",
        footnotes=[
            "Nenhum dado será apagado. Em Minha assinatura você vê exatamente o que está acima do limite e pode regularizar em poucos cliques."
        ],
        context_line=f"Aviso enviado aos administradores de {organization_name} no SiSFarm.",
        show_support=True,
        base_url=base_url,
    )
    return subject, text_body, html_body


def build_pix_renewal_reminder_email(
    kind: str,
    *,
    organization_name: str,
    plan_name: str,
    due_str: str,
    value_str: str,
    link: str,
    base_url: str | None = None,
) -> tuple[str, str, str]:
    if kind == "pix_renovacao_hoje":
        subject = f"SiSFarm: sua assinatura Pix vence hoje ({due_str})"
        lead = f"O ciclo do plano {plan_name} da {organization_name} vence hoje ({due_str})."
        badge_label, badge_tone, details_tone = "Vence hoje", "danger", "danger"
        title = "Sua assinatura Pix vence hoje"
        due_color = "#B42318"
    elif kind == "pix_renovacao_3d":
        subject = f"SiSFarm: sua assinatura Pix vence em breve ({due_str})"
        lead = f"Faltam poucos dias para o vencimento do plano {plan_name} da {organization_name}, em {due_str}."
        badge_label, badge_tone, details_tone = "Vence em breve", "warning", "warning"
        title = "Faltam poucos dias para renovar no Pix"
        due_color = "#7A4F00"
    else:
        subject = f"SiSFarm: lembrete de renovação do plano ({due_str})"
        lead = f"O período atual do plano {plan_name} da {organization_name} termina em {due_str}."
        badge_label, badge_tone, details_tone = "Renovação Pix", "brand", "soft"
        title = "Sua renovação Pix se aproxima"
        due_color = "#16201A"

    text_body = (
        f"Olá,\n\n{lead}\n"
        f"Como o pagamento por Pix não é cobrado automaticamente, gere o novo QR Code Pix ({value_str}) quando preferir "
        "para continuar usando o SiSFarm sem interrupções:\n"
        f"{link}\n\n"
        "Se você já renovou, desconsidere esta mensagem.\n\n"
        "SiSFarm · Gestão agrícola inteligente\n"
    )
    html_body = _render_transactional_html(
        subject=subject,
        preheader=f"Gere seu QR Code Pix de {value_str} para manter o acesso contínuo de {organization_name}.",
        badge_label=badge_label,
        badge_tone=badge_tone,
        eyebrow="Assinatura via Pix",
        title=title,
        intro_paragraphs=[
            f"{lead} Como o pagamento por Pix não é cobrado automaticamente, você pode gerar o novo QR Code quando preferir."
        ],
        details=[
            {"label": "Organização", "value": organization_name, "bold": True},
            {"label": "Plano", "value": plan_name},
            {"label": "Vencimento", "value": due_str, "bold": True, "color": due_color},
            {"label": "Valor da renovação", "value": value_str, "bold": True},
        ],
        details_tone=details_tone,
        cta={"label": "Gerar QR Code Pix →", "url": link},
        cta_tone="brand",
        footnotes=[
            "Se você já realizou o pagamento, desconsidere esta mensagem — a confirmação do Pix é automática em poucos segundos."
        ],
        context_line=f"Aviso financeiro enviado ao responsável pela assinatura de {organization_name}.",
        show_support=True,
        base_url=base_url,
    )
    return subject, text_body, html_body


def build_billing_notice_email(
    kind: str,
    *,
    organization_name: str,
    deadline_str: str,
    link: str,
    renewal_pending: bool = False,
    read_only: bool = False,
    value_str: str | None = None,
    due_str: str | None = None,
    pay_url: str | None = None,
    base_url: str | None = None,
) -> tuple[str, str, str]:
    base_kind = kind.removeprefix("renovacao_")
    if renewal_pending:
        if base_kind == "pre_restricao":
            subject = f"SiSFarm: regularize até {deadline_str} para manter o acesso completo"
            text_body = (
                f"Olá,\n\nAinda não recebemos a confirmação da renovação da assinatura SiSFarm de {organization_name}.\n"
                f"Se o pagamento não for identificado até {deadline_str}, o sistema passa para o modo somente leitura: "
                "os dados continuam disponíveis para consulta, mas não será possível cadastrar ou editar.\n\n"
                f"Confira sua assinatura: {link}\n\n"
                "Se você já regularizou o pagamento, desconsidere esta mensagem; a confirmação é automática.\n\n"
                "SiSFarm · Gestão agrícola inteligente"
            )
            html_body = _render_transactional_html(
                subject=subject,
                preheader=f"Regularize até {deadline_str} para evitar o modo somente leitura.",
                badge_label="Prazo final",
                badge_tone="danger",
                eyebrow="Aviso importante de assinatura",
                title=f"Regularize até {deadline_str} para evitar restrição",
                intro_paragraphs=[
                    f"Ainda não recebemos a confirmação da renovação da assinatura SiSFarm de {organization_name}."
                ],
                details=[
                    {"label": "Organização", "value": organization_name, "bold": True},
                    {"label": "Situação", "value": "Renovação aguardando confirmação", "color": "#B42318"},
                    {"label": "Data limite", "value": deadline_str, "bold": True, "color": "#B42318"},
                ],
                details_tone="danger",
                callout={
                    "title": f"O que acontece após {deadline_str}?",
                    "text": "O sistema passa para o modo somente leitura: todos os seus dados continuam disponíveis para consulta, mas não será possível cadastrar ou editar até a confirmação do pagamento.",
                },
                callout_tone="warning",
                cta={"label": "Conferir minha assinatura →", "url": link},
                cta_tone="danger",
                footnotes=[
                    "Se você já regularizou o pagamento, desconsidere esta mensagem — a confirmação é automática."
                ],
                context_line=f"Aviso financeiro enviado ao responsável pela assinatura de {organization_name}.",
                show_support=True,
                base_url=base_url,
            )
            return subject, text_body, html_body

        subject = "SiSFarm: renovação pendente" + (" (modo somente leitura ativo)" if read_only else "")
        text_body = (
            f"Olá,\n\nAinda não identificamos a confirmação da renovação da assinatura SiSFarm de {organization_name}.\n"
            + (
                "O sistema está em modo somente leitura até a regularização.\n"
                if read_only
                else f"Para evitar o modo somente leitura, confira o meio de pagamento até {deadline_str}.\n"
            )
            + f"\nSua assinatura: {link}\n\n"
            "Se você já regularizou o pagamento, desconsidere esta mensagem; a confirmação é automática.\n\n"
            "SiSFarm · Gestão agrícola inteligente"
        )
        html_body = _render_transactional_html(
            subject=subject,
            preheader=(
                "O sistema está em modo somente leitura até a regularização da renovação."
                if read_only
                else f"Confira o meio de pagamento até {deadline_str} para manter o acesso completo."
            ),
            badge_label="Somente leitura" if read_only else "Cobrança pendente",
            badge_tone="danger" if read_only else "warning",
            eyebrow="Financeiro da assinatura",
            title="Sistema em modo somente leitura" if read_only else "Confirmação de renovação pendente",
            intro_paragraphs=[
                f"Ainda não identificamos a confirmação da renovação da assinatura SiSFarm de {organization_name}. "
                + (
                    "Por isso, o sistema está temporariamente em modo somente leitura até a regularização."
                    if read_only
                    else f"Confira o meio de pagamento até {deadline_str} para evitar interrupções."
                )
            ],
            details=[
                {"label": "Organização", "value": organization_name, "bold": True},
                {
                    "label": "Situação",
                    "value": "Somente leitura (dados preservados)" if read_only else "Renovação aguardando confirmação",
                    "color": "#B42318" if read_only else "#7A4F00",
                    "bold": read_only,
                },
                {"label": "Prazo de tolerância", "value": f"Até {deadline_str}", "bold": True},
            ],
            details_tone="danger" if read_only else "warning",
            callout=(
                {
                    "title": "Seus dados estão intactos:",
                    "text": "Você continua podendo consultar tudo no SiSFarm. Assim que o pagamento for confirmado, a edição volta automaticamente.",
                }
                if read_only
                else None
            ),
            callout_tone="info",
            cta={"label": "Conferir minha assinatura →", "url": link},
            cta_tone="danger" if read_only else "brand",
            footnotes=[
                "Se você já regularizou o pagamento, desconsidere esta mensagem — a confirmação é automática."
            ],
            context_line=f"Aviso financeiro enviado ao responsável pela assinatura de {organization_name}.",
            show_support=True,
            base_url=base_url,
        )
        return subject, text_body, html_body

    resolved_value = value_str or "-"
    resolved_due = due_str or "-"
    resolved_pay = pay_url or link

    if base_kind == "pre_restricao":
        subject = f"SiSFarm: regularize até {deadline_str} para manter o acesso completo"
        text_body = (
            f"Olá,\n\nA assinatura SiSFarm de {organization_name} tem {resolved_value} em aberto desde {resolved_due}.\n"
            f"Se o pagamento não for identificado até {deadline_str}, o sistema passa para o modo somente leitura: "
            "os dados continuam disponíveis para consulta, mas não será possível cadastrar ou editar.\n\n"
            f"Pagar agora: {resolved_pay}\nSua assinatura: {link}\n\n"
            "Se você já pagou, desconsidere esta mensagem; a confirmação é automática.\n\n"
            "SiSFarm · Gestão agrícola inteligente"
        )
        html_body = _render_transactional_html(
            subject=subject,
            preheader=f"Último aviso antes do modo somente leitura. Valor em aberto: {resolved_value}.",
            badge_label="Prazo final",
            badge_tone="danger",
            eyebrow="Aviso importante de assinatura",
            title=f"Regularize até {deadline_str} para evitar restrição",
            intro_paragraphs=[
                f"A assinatura SiSFarm de {organization_name} possui {resolved_value} em aberto desde {resolved_due}."
            ],
            details=[
                {"label": "Organização", "value": organization_name, "bold": True},
                {"label": "Valor em aberto", "value": resolved_value, "bold": True, "color": "#B42318"},
                {"label": "Em aberto desde", "value": resolved_due},
                {"label": "Data limite", "value": deadline_str, "bold": True, "color": "#B42318"},
            ],
            details_tone="danger",
            callout={
                "title": f"O que acontece após {deadline_str}?",
                "text": "O sistema passa para o modo somente leitura: todos os seus dados continuam disponíveis para consulta, mas não será possível cadastrar ou editar registros até a confirmação do pagamento.",
            },
            callout_tone="warning",
            cta={"label": "Pagar agora →", "url": resolved_pay},
            cta_tone="danger",
            secondary_cta={"label": "Ver minha assinatura", "url": link} if resolved_pay != link else None,
            footnotes=["Se você já regularizou o pagamento, desconsidere esta mensagem — a baixa é automática."],
            context_line=f"Aviso financeiro enviado ao responsável pela assinatura de {organization_name}.",
            show_support=True,
            base_url=base_url,
        )
        return subject, text_body, html_body

    subject = "SiSFarm: fatura em atraso" + (" (modo somente leitura ativo)" if read_only else "")
    text_body = (
        f"Olá,\n\nNão identificamos o pagamento da fatura SiSFarm de {organization_name}, "
        f"com vencimento em {resolved_due} ({resolved_value} em aberto).\n"
        + (
            "O sistema está em modo somente leitura até a regularização.\n"
            if read_only
            else f"Para evitar o modo somente leitura, regularize até {deadline_str}.\n"
        )
        + f"\nPagar agora: {resolved_pay}\nSua assinatura: {link}\n\n"
        "Se você já pagou, desconsidere esta mensagem; a confirmação é automática.\n\n"
        "SiSFarm · Gestão agrícola inteligente"
    )
    html_body = _render_transactional_html(
        subject=subject,
        preheader=(
            "O sistema está em modo somente leitura até a regularização da fatura."
            if read_only
            else f"Identificamos uma pendência na assinatura de {organization_name}."
        ),
        badge_label="Somente leitura" if read_only else "Cobrança pendente",
        badge_tone="danger" if read_only else "warning",
        eyebrow="Financeiro da assinatura",
        title="Sistema em modo somente leitura" if read_only else "Fatura da assinatura em aberto",
        intro_paragraphs=[
            (
                f"Não identificamos o pagamento da fatura de {organization_name}. Por isso, o sistema está temporariamente em modo somente leitura até a regularização."
                if read_only
                else f"Não identificamos o pagamento da fatura de {organization_name}. Para manter o acesso completo sem interrupções, regularize até {deadline_str}."
            )
        ],
        details=[
            {"label": "Organização", "value": organization_name, "bold": True},
            {
                "label": "Valor em aberto",
                "value": resolved_value,
                "bold": True,
                "color": "#B42318" if read_only else "#16201A",
            },
            {"label": "Vencimento", "value": resolved_due},
            {
                "label": "Status atual" if read_only else "Regularize até",
                "value": "Somente leitura (dados preservados)" if read_only else deadline_str,
                "color": "#B42318" if read_only else "#7A4F00",
                "bold": True,
            },
        ],
        details_tone="danger" if read_only else "warning",
        callout=(
            {
                "title": "Seus dados estão intactos:",
                "text": "Você continua podendo consultar tudo no SiSFarm. Assim que o pagamento for confirmado, a edição volta automaticamente.",
            }
            if read_only
            else None
        ),
        callout_tone="info",
        cta={"label": "Pagar fatura agora →", "url": resolved_pay},
        cta_tone="danger" if read_only else "brand",
        secondary_cta={"label": "Minha assinatura", "url": link} if resolved_pay != link else None,
        footnotes=["Se você já efetuou o pagamento, desconsidere esta mensagem — a confirmação é automática."],
        context_line=f"Aviso financeiro enviado ao responsável pela assinatura de {organization_name}.",
        show_support=True,
        base_url=base_url,
    )
    return subject, text_body, html_body


# =============================================================================
# 4. EQUIPE INTERNA SISFARM (NOTIFICAÇÕES OPERACIONAIS / COMERCIAIS)
# =============================================================================


def build_backup_failure_email(
    *,
    run_id: int,
    trigger_source: str,
    status: str,
    finished_at: datetime,
    affected_components: str,
) -> tuple[str, str, str]:
    env = _resolve_email_env()
    link = f"{env['base_url']}/backups"
    source = {
        "automatic": "Automático",
        "web_manual": "Manual pelo painel",
        "manual": "Manual",
        "cli": "Linha de comando",
    }.get(trigger_source, "Desconhecida")
    partial = status == "partial"
    title = "Backup concluído parcialmente" if partial else "Backup não concluído"
    description = (
        "A execução do backup salvou apenas parte dos dados previstos."
        if partial else "A execução do backup terminou com falha."
    )
    subject = f"{title} no SiSFarm (#{run_id})"
    rows = [
        ("Execução", f"#{run_id}"),
        ("Origem", source),
        ("Encerrado em", format_app_datetime(finished_at, "%d/%m/%Y às %H:%M (%Z)")),
        ("Falha registrada", affected_components),
    ]
    action = "Consulte os detalhes da falha no histórico de backups."
    plain_details = "\n".join(f"{label}: {value}" for label, value in rows)
    text_body = f"{description}\n\n{plain_details}\n\n{action}\n{link}"
    tone = "warning" if partial else "danger"
    html_body = _render_transactional_html(
        subject=subject,
        preheader=description,
        badge_label="Backup parcial" if partial else "Falha no backup",
        badge_tone=tone,
        eyebrow="Operações · Backups",
        title=title,
        intro_paragraphs=[description, action],
        details=[{"label": label, "value": value} for label, value in rows],
        details_tone=tone,
        cta={"label": "Abrir histórico de backups", "url": link},
        fallback_url=link,
        is_internal=True,
    )
    return subject, text_body, html_body


def send_backup_failure_email(
    recipient_email: str,
    *,
    run_id: int,
    trigger_source: str,
    status: str,
    finished_at: datetime,
    affected_components: str,
) -> None:
    subject, body, html = build_backup_failure_email(
        run_id=run_id,
        trigger_source=trigger_source,
        status=status,
        finished_at=finished_at,
        affected_components=affected_components,
    )
    _send_email(recipient_email, subject, body, "backup_failure", html_body=html)


def build_new_signup_team_email(
    *,
    organization_name: str,
    user_name: str,
    user_email: str,
    user_phone: str | None,
    trial_days: int = 14,
    base_url: str | None = None,
) -> tuple[str, str, str]:
    env = _resolve_email_env(base_url=base_url)
    subject = f"Novo cadastro no SiSFarm: {organization_name}"
    phone_display = (user_phone or "").strip() or "Não informado"
    text_body = (
        f"Nova conta criada pelo site (teste de {trial_days} dias).\n\n"
        f"Organização: {organization_name}\n"
        f"Nome: {user_name}\n"
        f"E-mail: {user_email}\n"
        f"WhatsApp: {phone_display}\n\n"
        f"Painel: {env['base_url']}/organizacoes\n"
    )
    html_body = _render_transactional_html(
        subject=subject,
        preheader=f"{user_name} criou uma nova conta de teste ({trial_days} dias) para {organization_name}.",
        badge_label="Equipe interna",
        badge_tone="neutral",
        eyebrow="Notificação comercial",
        title="Novo cadastro realizado no site",
        intro_paragraphs=[f"Uma nova organização iniciou o período de teste gratuito de {trial_days} dias pelo site."],
        details=[
            {"label": "Organização", "value": organization_name, "bold": True},
            {"label": "Responsável", "value": user_name},
            {"label": "E-mail", "value": user_email},
            {"label": "WhatsApp", "value": phone_display},
            {"label": "Origem", "value": f"Autocadastro · Teste {trial_days} dias"},
        ],
        cta={"label": "Abrir organizações no painel →", "url": f"{env['base_url']}/organizacoes"},
        cta_tone="dark",
        context_line="Notificação interna automática enviada para a equipe SiSFarm.",
        show_support=False,
        is_internal=True,
        base_url=base_url,
    )
    return subject, text_body, html_body


def build_plan_upgrade_request_email(
    *,
    organization_name: str,
    plan_name: str,
    requester_email: str,
    module_label: str | None = None,
    base_url: str | None = None,
) -> tuple[str, str, str]:
    env = _resolve_email_env(base_url=base_url)
    subject = f"Pedido de mudança de plano: {plan_name}"
    text_body = (
        f"A organização {organization_name} pediu para mudar para o plano {plan_name}.\n"
        f"Solicitante: {requester_email}\n"
        + (f"Módulo de interesse: {module_label}\n" if module_label else "")
        + f"Ajuste a assinatura em Configuração > Assinaturas: {env['base_url']}/assinaturas\n"
    )
    rows: list[dict[str, Any]] = [
        {"label": "Organização", "value": organization_name, "bold": True},
        {"label": "Plano solicitado", "value": plan_name, "bold": True, "color": "#446A36"},
        {"label": "Solicitante", "value": requester_email},
    ]
    if module_label:
        rows.append({"label": "Módulo de interesse", "value": module_label})
    html_body = _render_transactional_html(
        subject=subject,
        preheader=f"{organization_name} solicitou migração para o plano {plan_name}.",
        badge_label="Equipe interna",
        badge_tone="neutral",
        eyebrow="Solicitação comercial",
        title="Pedido de mudança de plano",
        intro_paragraphs=[f"A organização {organization_name} solicitou mudança de plano pelo painel."],
        details=rows,
        cta={"label": "Gerenciar assinaturas →", "url": f"{env['base_url']}/assinaturas"},
        cta_tone="dark",
        footnotes=[
            "Ajuste a assinatura em Configuração > Assinaturas e entre em contato com o cliente para confirmar a ativação."
        ],
        context_line="Notificação interna automática enviada para a equipe SiSFarm.",
        show_support=False,
        is_internal=True,
        base_url=base_url,
    )
    return subject, text_body, html_body


# =============================================================================
# 5. ENVIO GENÉRICO E BOAS-VINDAS
# =============================================================================


def send_plain_email(
    recipient_email: str,
    subject: str,
    body: str,
    error_context: str,
    *,
    html_body: str | None = None,
    reply_to: str | None = None,
) -> None:
    """Envia e-mail multipart (texto puro + HTML no padrão SiSFarm)."""
    resolved_html = html_body
    if resolved_html is None:
        paragraphs = [p.strip() for p in re.split(r"\n\s*\n", (body or "").strip()) if p.strip()]
        resolved_html = _render_transactional_html(
            subject=subject,
            title=subject,
            intro_paragraphs=paragraphs or [body or ""],
            show_support=False,
        )
    _send_email(
        recipient_email,
        subject,
        body,
        error_context,
        html_body=resolved_html,
        reply_to=reply_to,
    )


_ORIGINAL_SEND_PLAIN_EMAIL = send_plain_email


def dispatch_email(
    send_fn: Callable[..., Any],
    recipient_email: str,
    subject: str,
    text_body: str,
    error_context: str,
    *,
    html_body: str | None = None,
    reply_to: str | None = None,
) -> None:
    """Envia com HTML quando usando send_plain_email original; mantém assinatura de 4 args para doubles de teste."""
    if send_fn is _ORIGINAL_SEND_PLAIN_EMAIL:
        _ORIGINAL_SEND_PLAIN_EMAIL(
            recipient_email,
            subject,
            text_body,
            error_context,
            html_body=html_body,
            reply_to=reply_to,
        )
        return
    send_fn(recipient_email, subject, text_body, error_context)


def _welcome_first_name(display_name: str | None, full_name: str | None) -> str:
    raw = (display_name or "").strip() or (full_name or "").strip()
    if not raw:
        return "Produtor"
    return raw.split()[0]


def build_welcome_email_content(
    *,
    display_name: str | None,
    full_name: str | None,
    organization_name: str | None,
    trial_days: int = 14,
    base_url: str | None = None,
    support_email: str | None = None,
    whatsapp_display: str | None = None,
) -> tuple[str, str, str]:
    """Monta assunto, versão texto puro e HTML do e-mail de boas-vindas."""
    env = _resolve_email_env(
        base_url=base_url,
        support_email=support_email,
        whatsapp_display=whatsapp_display,
    )
    first_name = _welcome_first_name(display_name, full_name)
    org_name = (organization_name or "").strip() or "sua fazenda"
    public_url = env["base_url"]
    resolved_support = env["support_email"]
    resolved_whatsapp = env["whatsapp_display"]

    subject = f"Bem-vindo ao SiSFarm, {first_name}! Sua conta está pronta"
    text_body = (
        f"Olá, {first_name}! Que alegria ter você por aqui.\n\n"
        f"A conta de {org_name} já está cadastrada no SiSFarm e você já pode começar "
        "a organizar a sua safra, controlar os insumos, acompanhar a produção e o financeiro "
        "da fazenda, tudo em um só lugar.\n\n"
        f"Seu teste gratuito de {trial_days} dias já está ativo com todos os recursos liberados.\n"
        f"Acesse seu SiSFarm: {public_url}/primeiros-passos\n\n"
        "Principais recursos que você já pode usar:\n"
        f"- Fazenda e Talhões: {public_url}/fazendas\n"
        f"- Produção e Vendas: {public_url}/producao\n"
        f"- Insumos: {public_url}/insumos/estoque\n"
        f"- Financeiro: {public_url}/gestao-financeira\n\n"
        "Conheça o Pezinho, seu assistente inteligente!\n"
        "Converse por texto ou áudio no sistema ou no WhatsApp, consulte informações da fazenda "
        f"e registre atividades com facilidade: {public_url}/mobile/assistente\n\n"
        "Precisa de ajuda? Nossa equipe está pronta para te apoiar."
        + (f"\nE-mail: {resolved_support}" if resolved_support else "")
        + (f"\nWhatsApp: {resolved_whatsapp}" if resolved_whatsapp else "")
        + "\n\nBoas safras!\nSiSFarm · Gestão agrícola inteligente\n"
        f"{public_url}\n"
    )
    html_body = _email_jinja_env.get_template("welcome.html").render(
        subject=subject,
        preheader=f"A conta de {org_name} já está pronta no SiSFarm! Comece agora seus {trial_days} dias grátis e conheça o Pezinho.",
        badge_label=f"{trial_days} dias grátis",
        badge=_BADGE_TONES["success"],
        first_name=first_name,
        organization_name=org_name,
        trial_days=trial_days,
        context_line=f"Você recebeu este e-mail porque criou a conta de {org_name} no SiSFarm.",
        show_support=True,
        is_internal=False,
        **env,
    )
    return subject, text_body, html_body


def send_welcome_email(
    *,
    recipient_email: str,
    display_name: str | None,
    full_name: str | None,
    organization_name: str | None,
    trial_days: int = 14,
) -> None:
    """Envia o e-mail HTML de boas-vindas ao novo usuário."""
    settings = get_settings()
    support_email = (getattr(settings, "support_email", None) or "").strip() or None
    subject, text_body, html_body = build_welcome_email_content(
        display_name=display_name,
        full_name=full_name,
        organization_name=organization_name,
        trial_days=trial_days,
    )
    _send_email(
        recipient_email=recipient_email,
        subject=subject,
        body=text_body,
        html_body=html_body,
        reply_to=support_email,
        error_context="welcome",
    )
