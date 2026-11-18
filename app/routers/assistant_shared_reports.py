"""Acesso temporário e somente leitura a relatórios produzidos pelo servidor."""
import json
import math
from datetime import date, timezone
import secrets

from fastapi import APIRouter, Depends, Form, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.core.timezone import get_app_timezone
from app.routers.whatsapp_record_access import HEADERS
from app.services.shared_report import REPORTS, authorize_shared_report, find_shared_report
from app.services.whatsapp_record_access import attempts_blocked, register_failed_attempt

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _format_value(value, kind):
    if value is None or value == "":
        return "—"
    if kind in {"number", "currency"}:
        try:
            number = float(value)
            if not math.isfinite(number):
                return "—"
            formatted = f"{number:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
            return ("R$ " if kind == "currency" else "") + formatted
        except (TypeError, ValueError, OverflowError):
            return "—"
    if isinstance(value, str) and len(value) == 10:
        try:
            return date.fromisoformat(value).strftime("%d/%m/%Y")
        except ValueError:
            pass
    return str(value)[:400]


def _render(request, state, status_code=200, **kwargs):
    return templates.TemplateResponse("assistant_shared_report.html", {"request": request, "state": state,
                                      "format_value": _format_value, **kwargs}, status_code=status_code, headers=HEADERS)


@router.get("/acesso-compartilhado/relatorio")
def confirmation(request: Request):
    csrf = secrets.token_urlsafe(32)
    request.session["shared_report_csrf"] = csrf
    return _render(request, "confirm", csrf_token=csrf)


@router.post("/acesso-compartilhado/relatorio")
def show_report(request: Request, token: str = Form("", max_length=64), csrf_token: str = Form("", max_length=64),
                db: Session = Depends(get_db)):
    csrf = request.session.pop("shared_report_csrf", "")
    if not csrf or not csrf_token.isascii() or not secrets.compare_digest(csrf, csrf_token):
        return _render(request, "unavailable", status_code=403)
    ip = request.client.host if request.client else "unknown"
    if attempts_blocked(ip):
        return _render(request, "unavailable", status_code=429)
    report = find_shared_report(db, token, lock=True)
    if not report or not authorize_shared_report(db, report):
        db.rollback()
        register_failed_attempt(ip)
        return _render(request, "unavailable", status_code=410)
    try:
        payload = json.loads(report.data_json)
        if not isinstance(payload, dict) or payload.get("version") != 1 or payload.get("tool") != report.tool_name \
                or report.tool_name not in {*REPORTS, "consultar_financeiro"}:
            raise ValueError("Relatório inválido")
    except (TypeError, ValueError):
        db.rollback()
        return _render(request, "unavailable", status_code=410)
    def local(value):
        return (value if value.tzinfo else value.replace(tzinfo=timezone.utc)).astimezone(get_app_timezone())
    response = _render(request, "report", report=payload, generated_at=local(report.created_at), expires_at=local(report.expires_at))
    db.commit()
    return response
