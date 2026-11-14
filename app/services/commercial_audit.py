"""Eventos comerciais gravados na mesma transação da operação local."""

import json

from app.models.audit_log import AuditLog
from app.services.asaas_environment import asaas_env_name


def record_commercial_event(db, *, user, organization_id, resource, resource_id, action, outcome, details=None):
    metadata = {'ambiente': asaas_env_name(), 'recurso': resource, 'id_local': resource_id, 'acao': action}
    if details:
        metadata.update(details)
    db.add(AuditLog(
        actor_user_id=getattr(user, 'id', None),
        actor_email=getattr(user, 'email', None),
        organization_id=organization_id,
        event_type=f'comercial.{resource}.{action}'[:80],
        outcome=outcome,
        metadata_json=json.dumps(metadata, ensure_ascii=False, default=str),
    ))
