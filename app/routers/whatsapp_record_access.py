"""Visualizacao restrita de um registro por link de uso unico do WhatsApp."""
import secrets

from fastapi import APIRouter, Depends, Form, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services.whatsapp_record_access import (attempts_blocked, authorized_record, consume_token, find_token,
                                                  register_failed_attempt)

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
HEADERS = {
    "Cache-Control": "no-store", "Referrer-Policy": "no-referrer", "X-Robots-Tag": "noindex, nofollow",
    "X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY",
    "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
}


def _response(request, *, state, status_code=200, **context):
    return templates.TemplateResponse("whatsapp_record_access.html", {"request": request, "state": state, **context},
                                      status_code=status_code, headers=HEADERS)


@router.get("/acesso-whatsapp/registro")
def record_confirmation(request: Request):
    csrf = secrets.token_urlsafe(32)
    request.session["wa_record_csrf"] = csrf
    return _response(request, state="confirm", csrf_token=csrf)


def _details(kind, record):
    if kind == "compra":
        item = record.input_catalog.name if record.input_catalog else record.name
        return "Compra", [("Produto", item), ("Data", record.purchase_date),
                          ("Quantidade", f"{record.total_quantity} {record.package_unit or ''}"),
                          ("Fornecedor", record.supplier_name or "Não informado")]
    if kind == "consumo":
        item = record.input_catalog.name if record.input_catalog else "Não informado"
        return "Consumo", [("Produto", item), ("Data", record.movement_date),
                           ("Quantidade", f"{record.quantity} {record.unit or ''}"),
                           ("Setor", record.plot.name if record.plot else "Geral")]
    if kind == "aplicacao":
        return "Aplicação", [("Setor", record.plot.name), ("Data", record.application_date),
                             ("Produto", record.product), ("Dose", record.dose)]
    return "Colheita", [("Setor", record.plot.name), ("Data", record.harvest_date),
                        ("Produção (sacas)", record.sacks_produced), ("Tipo", record.harvest_type or "Não informado")]


@router.post("/acesso-whatsapp/registro")
def open_record(request: Request, token: str = Form("", max_length=64), csrf_token: str = Form("", max_length=64), db: Session = Depends(get_db)):
    csrf = request.session.pop("wa_record_csrf", "")
    if not csrf or not csrf_token.isascii() or not secrets.compare_digest(csrf, csrf_token):
        return _response(request, state="unavailable", status_code=403)
    ip = request.client.host if request.client else "unknown"
    if attempts_blocked(ip):
        return _response(request, state="unavailable", status_code=429)
    access = find_token(db, token)
    if not access:
        register_failed_attempt(ip)
        return _response(request, state="unavailable", status_code=410)
    record = authorized_record(db, access)
    if not record or not consume_token(db, token):
        db.rollback()
        register_failed_attempt(ip)
        return _response(request, state="unavailable", status_code=410)
    title, fields = _details(access.kind, record)
    db.commit()
    return _response(request, state="record", title=title, fields=fields)
