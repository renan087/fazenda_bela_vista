import logging
import smtplib
from email.message import EmailMessage
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_EMAIL_TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates" / "emails"
_email_jinja_env = Environment(
    loader=FileSystemLoader(str(_EMAIL_TEMPLATES_DIR)),
    autoescape=select_autoescape(["html", "xml"]),
)


def _generic_error_message(error_context: str) -> str:
    if error_context == "password_reset":
        return "Nao foi possivel enviar o email de redefinicao."
    if error_context == "welcome":
        return "Nao foi possivel enviar o email de boas-vindas."
    return "Nao foi possivel enviar o email de verificacao."


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


def send_access_code_email(recipient_email: str, code: str) -> None:
    _send_email(
        recipient_email=recipient_email,
        subject="Codigo de acesso",
        body=f"Seu codigo de acesso e: {code}",
        error_context="two_factor",
    )


def send_password_reset_email(recipient_email: str, reset_link: str, expires_in_minutes: int) -> None:
    _send_email(
        recipient_email=recipient_email,
        subject="Redefinicao de senha",
        body=(
            "Recebemos um pedido para redefinir sua senha no SiSFarm.\n\n"
            f"Acesse o link abaixo em ate {expires_in_minutes} minutos:\n"
            f"{reset_link}\n\n"
            "Se voce nao solicitou esta redefinicao, ignore esta mensagem."
        ),
        error_context="password_reset",
    )


def send_password_change_code_email(recipient_email: str, code: str, expires_in_minutes: int) -> None:
    _send_email(
        recipient_email=recipient_email,
        subject="Confirmacao de alteracao de senha",
        body=(
            "Recebemos uma solicitacao para alterar a senha da sua conta no SiSFarm.\n\n"
            f"Seu codigo de confirmacao e: {code}\n"
            f"Este codigo expira em {expires_in_minutes} minutos.\n\n"
            "Se voce nao iniciou essa alteracao, ignore esta mensagem."
        ),
        error_context="two_factor",
    )


def send_bootstrap_admin_password_email(recipient_email: str, password: str) -> None:
    _send_email(
        recipient_email=recipient_email,
        subject="Acesso administrativo de contingencia",
        body=(
            "Uma conta administrativa de contingencia foi criada no SiSFarm.\n\n"
            f"Login: {recipient_email}\n"
            f"Senha temporaria: {password}\n\n"
            "Ao acessar, mantenha o 2FA ativo e altere a senha assim que possivel."
        ),
        error_context="two_factor",
    )


def send_plain_email(recipient_email: str, subject: str, body: str, error_context: str) -> None:
    """Envio de e-mail de texto simples (avisos de cobrança e pedidos comerciais)."""
    _send_email(recipient_email, subject, body, error_context)


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
    settings = get_settings()
    first_name = _welcome_first_name(display_name, full_name)
    org_name = (organization_name or "").strip() or "sua fazenda"
    public_url = (base_url or settings.app_public_url or "https://app.sisfarm.com.br").rstrip("/")
    resolved_support = (support_email if support_email is not None else (settings.support_email or "")).strip()
    resolved_whatsapp = (
        whatsapp_display if whatsapp_display is not None else (settings.whatsapp_display_number or "")
    ).strip()

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
        + "\n\nBoas safras!\nEquipe SiSFarm\n"
        f"{public_url}\n"
    )
    html_body = _email_jinja_env.get_template("welcome.html").render(
        first_name=first_name,
        organization_name=org_name,
        trial_days=trial_days,
        base_url=public_url,
        support_email=resolved_support,
        whatsapp_display=resolved_whatsapp,
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
    support_email = (settings.support_email or "").strip() or None
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
