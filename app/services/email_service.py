import smtplib
from email.message import EmailMessage

from app.core.config import get_settings


def send_access_code_email(recipient_email: str, code: str) -> None:
    settings = get_settings()
    if not settings.smtp_host or not settings.smtp_from_email:
        raise RuntimeError("Servico de email nao configurado.")

    message = EmailMessage()
    message["Subject"] = "Codigo de acesso"
    message["From"] = f"{settings.smtp_from_name} <{settings.smtp_from_email}>"
    message["To"] = recipient_email
    message.set_content(f"Seu codigo de acesso e: {code}")

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=20) as server:
        if settings.smtp_use_tls:
            server.starttls()
        if settings.smtp_username and settings.smtp_password:
            server.login(settings.smtp_username, settings.smtp_password)
        server.send_message(message)
