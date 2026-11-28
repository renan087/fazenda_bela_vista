"""Monta a aba "Assinaturas e histórico" do documento: quem assina e linha do tempo."""
from __future__ import annotations

from datetime import datetime, timezone

SEAL_MODALITIES = {"institutional_seal", "sisfarm_manifesto_seal"}


def _initials(name: str | None) -> str:
    parts = [p for p in (name or "").replace(".", " ").split() if p]
    if not parts:
        return "?"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[-1][0]).upper()


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _modality_label(modality: str | None) -> str:
    return "Certificado ICP-Brasil" if modality == "qualified_icp_brasil" else "Assinatura eletrônica"


def _short_modality(modality: str | None) -> str:
    return "ICP-Brasil" if modality == "qualified_icp_brasil" else "eletrônica"


def _evidence_view(ev) -> dict:
    is_icp = ev.modality == "qualified_icp_brasil"
    channel = "WhatsApp" if "whatsapp" in (ev.auth_channel or "") else "E-mail"
    fields = [("CPF", ev.cpf_masked or "Conforme certificado")]
    if is_icp:
        fields.append(("Certificado", ev.cert_issuer or "ICP-Brasil A1"))
    else:
        dest = f" {ev.auth_destination_masked}" if ev.auth_destination_masked else ""
        fields.append(("Confirmou pelo", f"{channel}{dest}"))
        if ev.device_summary:
            fields.append(("Aparelho", ev.device_summary))
        if ev.ip_masked:
            fields.append(("Endereço de internet (IP)", ev.ip_masked))
    fields.append(("Gerou a versão", f"Versão {ev.revision_number}"))
    return {"fields": fields, "code": ev.evidence_code}


def _external_tracking(ext) -> str:
    if ext.whatsapp_status == "failed":
        return "Não foi possível entregar no WhatsApp. Troque para e-mail ou copie o link."
    if ext.first_accessed_at:
        return "Abriu o convite"
    if ext.channel == "whatsapp":
        if ext.whatsapp_status == "read":
            return "Viu o convite no WhatsApp"
        if ext.whatsapp_status == "delivered":
            return "Convite entregue no WhatsApp · ainda não abriu"
        return "Convite enviado por WhatsApp · ainda não abriu"
    return "Convite enviado por e-mail · ainda não abriu"


def _external_tracking_at(ext) -> datetime | None:
    if ext.whatsapp_status == "failed":
        return None
    return ext.first_accessed_at or ext.whatsapp_read_at or ext.whatsapp_delivered_at or ext.whatsapp_sent_at or ext.created_at


def build_signers(*, doc, owner, current_user, participants, external_signers, evidences, revisions) -> list[dict]:
    """Lista única de quem assina: dono, equipe e convidados de fora."""
    is_owner_viewer = current_user.id == doc.owner_id
    ev_by_user: dict[int, object] = {}
    ev_by_ext: dict[str, object] = {}
    for ev in evidences:
        if ev.external_signer_id:
            ev_by_ext.setdefault(ev.external_signer_id, ev)
        elif ev.user_id is not None:
            ev_by_user.setdefault(ev.user_id, ev)
    icp_rev_by_user: dict[int, object] = {}
    for rev in revisions:
        sig = rev.signature or {}
        if rev.user_id is not None and sig and sig.get("modality") not in SEAL_MODALITIES and sig.get("modality") != "electronic_sisfarm":
            icp_rev_by_user.setdefault(rev.user_id, rev)

    def team_row(user, *, role: str, kind: str, must_sign: bool) -> dict:
        ev = ev_by_user.get(user.id)
        rev = icp_rev_by_user.get(user.id)
        name = user.display_name or user.name
        row = {
            "kind": kind, "id": user.id, "name": name, "initials": _initials(name), "role": role,
            "is_me": user.id == current_user.id, "contact": user.email if is_owner_viewer else "",
            "status": "pending", "signed_at": None, "modality": "", "evidence": None,
            "tracking": "", "tracking_at": None, "channel": "", "can_manage": is_owner_viewer and kind == "team",
        }
        if ev is not None:
            row.update(status="signed", signed_at=ev.signed_at, modality=_modality_label(ev.modality), evidence=_evidence_view(ev))
        elif rev is not None:
            row.update(status="signed", signed_at=rev.created_at, modality=_modality_label("qualified_icp_brasil"))
        elif not must_sign:
            row["status"] = "viewer"
        elif kind == "owner":
            row["tracking"] = "Falta você assinar" if row["is_me"] else "Ainda não assinou"
        else:
            row["tracking"] = "Ainda não assinou · recebeu o aviso pelo SiSFarm"
        return row

    rows: list[dict] = []
    sisfarm_fps = {(rev.signature or {}).get("fingerprint") for rev in revisions if rev.signature}
    for sig in ((doc.validation or {}).get("signatures") or []):
        holder = (sig.get("holder") or "").strip()
        if sig.get("fingerprint") in sisfarm_fps or "sisfarm" in holder.lower():
            continue
        name = holder or "Assinatura digital"
        rows.append({
            "kind": "pdf", "id": sig.get("field") or name, "name": name, "initials": _initials(name),
            "role": "Já estava no PDF", "is_me": False, "contact": "",
            "status": "signed", "signed_at": sig.get("signed_at"),
            "modality": "Certificado ICP-Brasil" if sig.get("state") == "valid" else "Certificado digital",
            "evidence": None, "tracking": "", "tracking_at": None, "channel": "", "can_manage": False,
        })
    if owner is not None and getattr(doc, "owner_signs", True):
        rows.append(team_row(owner, role="Responsável", kind="owner", must_sign=True))
    for person in participants:
        rows.append(team_row(person, role="Equipe", kind="team", must_sign=True))
    for ext in external_signers:
        if ext.status == "revoked":
            continue
        ev = ev_by_ext.get(ext.id)
        row = {
            "kind": "external", "id": ext.id, "name": ext.name, "initials": _initials(ext.name),
            "role": f"{ext.role_label or 'Signatário'} · de fora", "is_me": False,
            "contact": (ext.phone or ext.email or "") if is_owner_viewer else "",
            "status": "signed" if ext.status == "signed" else "pending",
            "signed_at": (ev.signed_at if ev is not None else ext.signed_at),
            "modality": _modality_label(ev.modality if ev is not None else "electronic_sisfarm") if ext.status == "signed" else "",
            "evidence": _evidence_view(ev) if ev is not None else None,
            "tracking": "" if ext.status == "signed" else _external_tracking(ext),
            "tracking_at": None if ext.status == "signed" else _external_tracking_at(ext),
            "tracking_failed": ext.whatsapp_status == "failed",
            "channel": ext.channel, "email": ext.email or "", "phone": ext.phone or "",
            "can_manage": is_owner_viewer and ext.status != "signed",
        }
        rows.append(row)
    return rows


def build_history(*, doc, owner, participants, external_signers, evidences, revisions, granted=None) -> list[dict]:
    """Linha do tempo: versões do PDF e acontecimentos, do mais novo para o mais antigo."""
    ev_by_rev = {ev.revision_number: ev for ev in evidences}
    owner_name = (owner.display_name or owner.name) if owner is not None else "Alguém da equipe"
    events: list[dict] = []
    for rev in revisions:
        sig = rev.signature or {}
        version = {"number": rev.number, "current": rev.number == doc.current_revision,
                   "label": "Original" if rev.number == 1 else f"Versão {rev.number}"}
        if rev.number == 1:
            events.append({"at": rev.created_at, "tone": "muted", "icon": "upload",
                           "who": owner_name, "text": "enviou o documento", "version": version})
        elif sig.get("modality") in SEAL_MODALITIES:
            events.append({"at": rev.created_at, "tone": "info", "icon": "shield", "who": "",
                           "text": "Selo de integridade SiSFarm aplicado", "version": version})
        else:
            ev = ev_by_rev.get(rev.number)
            modality = ev.modality if ev is not None else (sig.get("modality") or "qualified_icp_brasil")
            who = (ev.signer_name if ev is not None else sig.get("holder") or sig.get("operator")) or "Signatário"
            events.append({"at": rev.created_at, "tone": "ok", "icon": "check", "who": who,
                           "text": f"assinou ({_short_modality(modality)})", "version": version})
    for ext in external_signers:
        via = "WhatsApp" if ext.channel == "whatsapp" else "e-mail"
        events.append({"at": ext.created_at, "tone": "muted", "icon": "send", "who": "",
                       "text": f"Convite enviado para {ext.name} por {via}", "version": None})
        if ext.first_accessed_at:
            events.append({"at": ext.first_accessed_at, "tone": "info", "icon": "eye", "who": ext.name,
                           "text": "abriu o convite", "version": None})
        if ext.status == "revoked":
            events.append({"at": None, "tone": "muted", "icon": "close", "who": "",
                           "text": f"Convite de {ext.name} cancelado", "version": None})
    for person in participants:
        when = (granted or {}).get(person.id)
        if when:
            events.append({"at": when, "tone": "muted", "icon": "users", "who": person.display_name or person.name,
                           "text": "recebeu acesso para assinar", "version": None})
    floor = datetime.min.replace(tzinfo=timezone.utc)
    ordered = [(i, e) for i, e in enumerate(events) if e["at"] is not None]
    ordered.sort(key=lambda pair: (_utc(pair[1]["at"]) or floor, pair[0]), reverse=True)
    events = [e for _, e in ordered]
    return events


def integrity_summary(validation: dict | None, sig_count: int) -> dict:
    state = (validation or {}).get("state") or "unsigned"
    checked = (validation or {}).get("checked_at")
    if state == "invalid":
        return {"tone": "error", "title": "Atenção: o PDF não confere",
                "text": "O arquivo foi alterado ou alguma assinatura não é válida. Veja os detalhes técnicos.", "checked_at": checked}
    if state == "indeterminate":
        return {"tone": "warn", "title": "Não foi possível confirmar tudo",
                "text": "As assinaturas estão no PDF, mas a origem de algum certificado não foi confirmada.", "checked_at": checked}
    if state == "valid" or sig_count:
        return {"tone": "ok", "title": "Documento íntegro",
                "text": "Nada foi alterado depois das assinaturas.", "checked_at": checked}
    return {"tone": "muted", "title": "Ainda sem assinaturas",
            "text": "Assim que alguém assinar, a integridade do PDF passa a ser conferida.", "checked_at": checked}
