"""Aplica o limite operacional à fazenda do registro, inclusive em alterações por ID."""

from functools import lru_cache

from fastapi import HTTPException
from sqlalchemy import event, inspect, select
from sqlalchemy.orm import Session

from app.core.admin_access import is_super_admin_email
from app.core.timezone import today_in_app_timezone
from app.db.base import Base
from app.models import Farm
from app.services.entitlements import compute_entitlements
from app.services.plan_usage import plan_usage, user_exceeds_plan


class FarmPlanRestriction(HTTPException):
    def __init__(self, message=None):
        super().__init__(status_code=409, detail=message or (
            "Esta fazenda está disponível para consulta. Ajuste os cadastros em Minha assinatura "
            "para voltar a registrar operações."
        ))


# Consultas, exportações e conversas podem salvar metadados sem lançar operações agrícolas.
_NON_OPERATIONAL = {"users", "stock_assistant_drafts"}


@lru_cache(maxsize=1)
def _farm_links():
    """Localiza os relacionamentos de propriedade, incluindo itens e anexos."""
    owned = {Farm.__table__}
    links = {}
    while True:
        previous = len(owned)
        for table in Base.metadata.tables.values():
            if table in owned or table.name in _NON_OPERATIONAL or table.name.startswith(("assistant_", "whatsapp_")):
                continue
            if any(ref.column.table in owned for ref in table.foreign_keys):
                owned.add(table)
        if len(owned) == previous:
            break
    for table in owned - {Farm.__table__}:
        links[table] = tuple(ref for ref in table.foreign_keys if ref.column.table in owned)
    return links


def bind_farm_plan_actor(db: Session, user) -> None:
    if user.organization_id and not is_super_admin_email(user.email):
        db.info["farm_plan_actor"] = (user.organization_id, user.id)
    else:
        db.info.pop("farm_plan_actor", None)


def _validate_owned_changes(db, changes, blocked_farms):
    links = _farm_links()
    rows = {}
    checked = set()

    def check(table, values):
        if table is Farm.__table__:
            if values.get("id") in blocked_farms:
                raise FarmPlanRestriction()
            return
        for ref in links.get(table, ()):
            target = ref.column.table
            identifier = values.get(ref.parent.key)
            key = (target, ref.column, identifier)
            if identifier is None or key in checked:
                continue
            checked.add(key)
            if target is Farm.__table__:
                check(target, {"id": identifier})
                continue
            if key not in rows:
                ownership_columns = {parent_ref.parent for parent_ref in links[target]}
                rows[key] = db.execute(select(*ownership_columns).where(ref.column == identifier)).mappings().first()
            if rows[key]:
                check(target, rows[key])

    for record in changes:
        state = inspect(record)
        table = state.mapper.local_table
        if table not in links:
            continue
        current = {}
        has_changed_ownership = False
        for ref in links[table]:
            column = ref.parent.key
            attribute = state.attrs[column]
            current[column] = attribute.value
            has_changed_ownership = has_changed_ownership or attribute.history.has_changes()
        check(table, current)
        if state.identity and has_changed_ownership:
            identity_filter = [column == value for column, value in zip(state.mapper.primary_key, state.identity)]
            previous = db.execute(select(*(ref.parent for ref in links[table])).where(*identity_filter)).mappings().first()
            if previous:
                check(table, previous)
        # O ORM pode preencher a chave estrangeira da relação só durante o flush.
        linked_columns = {ref.parent for ref in links[table]}
        for relationship in state.mapper.relationships:
            if relationship.uselist or not linked_columns.intersection(relationship.local_columns):
                continue
            for parent in state.attrs[relationship.key].history.added:
                if parent is not None:
                    parent_state = inspect(parent)
                    parent_table = parent_state.mapper.local_table
                    if parent_table is Farm.__table__:
                        check(parent_table, {"id": parent.id})
                    else:
                        check(parent_table, {ref.parent.key: parent_state.attrs[ref.parent.key].value
                                             for ref in links.get(parent_table, ())})


@event.listens_for(Session, "before_flush")
def _enforce_farm_plan(db, flush_context, instances):
    actor = db.info.get("farm_plan_actor")
    if not actor:
        return
    owned = _farm_links()
    changes = [record for record in db.new | db.dirty | db.deleted if inspect(record).mapper.local_table in owned]
    if not changes:
        return
    organization_id, user_id = actor
    entitlements = compute_entitlements(db, organization_id, today_in_app_timezone())
    if user_exceeds_plan(db, entitlements, user_id):
        raise FarmPlanRestriction("Seu usuário está em modo consulta por exceder o limite do plano.")

    paused = {row.id for row in db.query(Farm.id).filter_by(organization_id=organization_id, operations_paused=True)}
    if entitlements.max_farms is not None or entitlements.max_hectares is not None:
        paused.update(plan_usage(db, organization_id, entitlements).excess_farms)
    if paused:
        _validate_owned_changes(db, changes, paused)
