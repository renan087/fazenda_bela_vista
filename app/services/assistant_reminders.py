"""Lembretes pessoais preparados pelo assistente e confirmados pelo usuário."""
from datetime import datetime, timezone
from uuid import uuid4

from app.core.timezone import get_app_timezone
from app.models.assistant_reminder import AssistantReminder
from app.services.assistant_queries import _tool


class ReminderTools:
    def __init__(self, db, user, farm, season):
        self.db, self.user, self.farm, self.season = db, user, farm, season
        self.proposal = None

    def rows(self):
        return self.db.query(AssistantReminder).filter_by(
            user_id=self.user.id, organization_id=self.user.organization_id,
            farm_id=self.farm.id, season_id=self.season.id)

    def list_reminders(self):
        rows = self.rows().filter(AssistantReminder.status.in_(["active", "paused"])).order_by(AssistantReminder.due_at).limit(51).all()
        return {"lembretes": [{"id": row.id, "descricao": row.description,
                               "data_hora": self.local_time(row.due_at).strftime("%d/%m/%Y %H:%M"),
                               "situacao": "ativo" if row.status == "active" else "pausado"} for row in rows[:50]],
                "mais_resultados": len(rows) > 50}

    @staticmethod
    def local_time(value):
        return (value if value.tzinfo else value.replace(tzinfo=timezone.utc)).astimezone(get_app_timezone())

    def propose_create(self, descricao, data_hora):
        if not isinstance(descricao, str) or not descricao.strip() or len(descricao.strip()) > 240:
            raise ValueError("Informe o lembrete com até 240 caracteres.")
        try:
            local = datetime.strptime(data_hora, "%Y-%m-%dT%H:%M").replace(tzinfo=get_app_timezone())
        except (ValueError, TypeError):
            raise ValueError("Pergunte a data e o horário do lembrete.") from None
        if local <= datetime.now(timezone.utc):
            raise ValueError("O lembrete precisa ter data e horário futuros.")
        self.proposal = {"action": "create", "proposal_id": uuid4().hex, "description": descricao.strip(), "due_at": local.astimezone(timezone.utc).isoformat()}
        return {"aguardando_confirmacao": True, "descricao": descricao.strip(), "data_hora": local.strftime("%d/%m/%Y %H:%M")}

    def propose_change(self, lembrete_id, acao):
        if isinstance(lembrete_id, bool) or not isinstance(lembrete_id, int) or acao not in {"pausar", "retomar", "excluir"}:
            raise ValueError("Escolha um lembrete e uma ação válida.")
        row = self.rows().filter_by(id=lembrete_id).first()
        if not row or row.status not in {"active", "paused"}:
            raise ValueError("Lembrete não encontrado neste contexto.")
        if acao == "retomar" and self.local_time(row.due_at) <= datetime.now(timezone.utc):
            raise ValueError("O horário já passou; crie outro lembrete para uma data futura.")
        self.proposal = {"action": acao, "id": row.id}
        return {"aguardando_confirmacao": True, "acao": acao, "descricao": row.description,
                "data_hora": self.local_time(row.due_at).strftime("%d/%m/%Y %H:%M")}

    def propose_edit(self, lembrete_id, descricao, data_hora):
        if isinstance(lembrete_id, bool) or not isinstance(lembrete_id, int):
            raise ValueError("Escolha o lembrete que deseja alterar.")
        row = self.rows().filter_by(id=lembrete_id).first()
        if not row or row.status not in {"active", "paused"}:
            raise ValueError("Lembrete não encontrado neste contexto.")
        if descricao is None and data_hora is None:
            raise ValueError("Pergunte o que o usuário deseja alterar.")
        if descricao is not None and (not isinstance(descricao, str) or not 0 < len(descricao.strip()) <= 240):
            raise ValueError("Informe a descrição com até 240 caracteres.")
        due = self.local_time(row.due_at)
        if data_hora is not None:
            try:
                due = datetime.strptime(data_hora, "%Y-%m-%dT%H:%M").replace(tzinfo=get_app_timezone())
            except (TypeError, ValueError):
                raise ValueError("Informe uma data e um horário válidos.") from None
            if due <= datetime.now(timezone.utc):
                raise ValueError("O novo horário precisa estar no futuro.")
        self.proposal = {"action": "editar", "id": row.id, "description": descricao.strip() if descricao is not None else row.description,
                         "due_at": due.astimezone(timezone.utc).isoformat(), "changes_due": data_hora is not None,
                         "expected_description": row.description,
                         "expected_due": self.local_time(row.due_at).astimezone(timezone.utc).isoformat()}
        return {"aguardando_confirmacao": True, "descricao": self.proposal["description"], "data_hora": due.strftime("%d/%m/%Y %H:%M")}

    def tools(self):
        return [
            (_tool("listar_lembretes", "Lista os lembretes pessoais desta fazenda e safra."), self.list_reminders),
            (_tool("preparar_lembrete", "Prepara lembrete pontual; pergunte se faltar data ou horário. Não confirme sozinho.", {
                "descricao": {"type": "string"}, "data_hora": {"type": "string", "description": "Data e hora locais, AAAA-MM-DDTHH:MM."}}), self.propose_create),
            (_tool("preparar_alteracao_lembrete", "Prepara pausa, retomada ou exclusão de lembrete escolhido pelo usuário.", {
                "lembrete_id": {"type": "integer"}, "acao": {"type": "string", "enum": ["pausar", "retomar", "excluir"]}}), self.propose_change),
            (_tool("preparar_edicao_lembrete", "Prepara alteração de descrição ou data e hora do lembrete escolhido; null mantém o valor atual.", {
                "lembrete_id": {"type": "integer"}, "descricao": {"type": ["string", "null"]},
                "data_hora": {"type": ["string", "null"], "description": "Nova data e hora locais em AAAA-MM-DDTHH:MM, ou null."}}), self.propose_edit),
        ]

    def confirm(self, proposal):
        action = proposal.get("action")
        if action == "create":
            try:
                due = datetime.fromisoformat(proposal["due_at"])
                description = proposal["description"]
                proposal_id = proposal["proposal_id"]
            except (KeyError, ValueError, TypeError):
                raise ValueError("Não foi possível confirmar o lembrete.") from None
            if due.tzinfo is None or due <= datetime.now(timezone.utc) or not isinstance(description, str) or not 0 < len(description) <= 240 or not isinstance(proposal_id, str) or len(proposal_id) != 32:
                raise ValueError("O lembrete não é mais válido. Solicite novamente.")
            if self.rows().filter_by(proposal_id=proposal_id).first():
                return
            self.db.add(AssistantReminder(proposal_id=proposal_id, user_id=self.user.id, organization_id=self.user.organization_id, farm_id=self.farm.id,
                                          season_id=self.season.id, description=description, due_at=due,
                                          status="active", created_at=datetime.now(timezone.utc)))
        elif action in {"pausar", "retomar", "excluir", "editar"}:
            row = self.rows().filter_by(id=proposal.get("id")).first()
            if not row or row.status not in {"active", "paused"}:
                raise ValueError("O lembrete não está mais disponível.")
            if action == "editar":
                if (row.description != proposal.get("expected_description") or
                        self.local_time(row.due_at).astimezone(timezone.utc).isoformat() != proposal.get("expected_due")):
                    raise ValueError("O lembrete foi alterado. Consulte os dados e solicite a edição novamente.")
                try:
                    due = datetime.fromisoformat(proposal["due_at"])
                    description = proposal["description"]
                except (KeyError, TypeError, ValueError):
                    raise ValueError("Edição inválida. Solicite novamente.") from None
                if due.tzinfo is None or not isinstance(description, str) or not 0 < len(description.strip()) <= 240:
                    raise ValueError("Edição inválida. Solicite novamente.")
                if proposal.get("changes_due") and due <= datetime.now(timezone.utc):
                    raise ValueError("O novo horário já passou; solicite outra data.")
                row.description, row.due_at = description.strip(), due
                return
            if action == "retomar" and self.local_time(row.due_at) <= datetime.now(timezone.utc):
                raise ValueError("O horário já passou; crie outro lembrete.")
            row.status = {"pausar": "paused", "retomar": "active", "excluir": "deleted"}[action]
        else:
            raise ValueError("Ação inválida.")
