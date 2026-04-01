import logging
import smtplib
from email.message import EmailMessage

from app.core.config import get_settings

logger = logging.getLogger(__name__)


def _generic_error_message(error_context: str) -> str:
    if error_context == "password_reset":
        return "Nao foi possivel enviar o email de redefinicao."
    return "Nao foi possivel enviar o email de verificacao."


def _send_email(recipient_email: str, subject: str, body: str, error_context: str) -> None:
    settings = get_settings()
    if not settings.smtp_host or not settings.smtp_from_email:
        raise RuntimeError("Servico de email nao configurado.")

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = f"{settings.smtp_from_name} <{settings.smtp_from_email}>"
    message["To"] = recipient_email
    message.set_content(body)

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
