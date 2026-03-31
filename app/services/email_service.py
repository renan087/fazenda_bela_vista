import logging
import smtplib
from email.message import EmailMessage

from app.core.config import get_settings

logger = logging.getLogger(__name__)


def send_access_code_email(recipient_email: str, code: str) -> None:
    settings = get_settings()
    if not settings.smtp_host or not settings.smtp_from_email:
        raise RuntimeError("Servico de email nao configurado.")

    message = EmailMessage()
    message["Subject"] = "Codigo de acesso"
    message["From"] = f"{settings.smtp_from_name} <{settings.smtp_from_email}>"
    message["To"] = recipient_email
    message.set_content(f"Seu codigo de acesso e: {code}")

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
            "Falha de autenticacao SMTP ao enviar codigo 2FA",
            extra={
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
            "Destinatario recusado pelo SMTP ao enviar codigo 2FA",
            extra={
                "smtp_host": settings.smtp_host,
                "smtp_port": settings.smtp_port,
                "smtp_from_email": settings.smtp_from_email,
                "recipient_email": recipient_email,
            },
        )
        raise RuntimeError("Nao foi possivel entregar o email de verificacao.") from exc
    except smtplib.SMTPSenderRefused as exc:
        logger.exception(
            "Remetente recusado pelo SMTP ao enviar codigo 2FA",
            extra={
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
            "Brevo recusou o conteudo ou o remetente ao enviar codigo 2FA",
            extra={
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
            "Falha de conexao SMTP ao enviar codigo 2FA",
            extra={
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
            "Falha SMTP ao enviar codigo 2FA",
            extra={
                "smtp_host": settings.smtp_host,
                "smtp_port": settings.smtp_port,
                "smtp_username": settings.smtp_username,
                "smtp_from_email": settings.smtp_from_email,
                "recipient_email": recipient_email,
                "smtp_use_tls": settings.smtp_use_tls,
            },
        )
        raise RuntimeError("Nao foi possivel enviar o email de verificacao.") from exc
