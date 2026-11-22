from types import SimpleNamespace
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db.base import Base
from app.models.nfe_draft import NfeDraftEvent
from app.services.nfe_cycle_simulator import run_offline_cycle_simulation
from app.services.nfe_document_store import verify_stored_document
from app.services.nfe_permissions import (
    FISCAL_PERMISSIONS,
    has_fiscal_permission,
    require_fiscal_permission,
)
from tests.test_nfe_xml_builder import _sample_inputs


def _new_db():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    return Session(engine)


def test_fiscal_permissions_separate_roles_and_certificate_management():
    owner = SimpleNamespace(role='owner')
    operator = SimpleNamespace(role='operator')
    manager = SimpleNamespace(role='manager')
    viewer = SimpleNamespace(role='viewer')

    for perm in FISCAL_PERMISSIONS:
        assert has_fiscal_permission(owner, perm) is True
        assert has_fiscal_permission(viewer, perm) is False

    assert has_fiscal_permission(operator, 'nfe.draft.prepare') is True
    assert has_fiscal_permission(operator, 'nfe.certificate.manage') is False
    assert has_fiscal_permission(operator, 'nfe.transmit') is False
    assert has_fiscal_permission(manager, 'nfe.transmit') is True
    assert has_fiscal_permission(manager, 'nfe.certificate.manage') is False

    with pytest.raises(PermissionError):
        require_fiscal_permission(operator, 'nfe.certificate.manage')
    with pytest.raises(ValueError):
        has_fiscal_permission(owner, 'nfe.invalida')


def test_offline_cycle_simulator_runs_end_to_end_and_handles_rejection():
    db = _new_db()
    snapshot, issuer, details, technical, identification, extra_data = _sample_inputs(cst='40')

    # Cenário 1: autorização simulada (cStat 100) -> salva tentativa + nfeProc travado + evento.
    res_ok = run_offline_cycle_simulation(
        db, draft_id='d' * 32, organization_id=1, farm_id=10, user_id=5,
        snapshot=snapshot, issuer=issuer, details=details, technical=technical,
        identification=identification, issuer_crt='3', extra_data=extra_data,
        simulated_c_stat='100',
    )
    assert res_ok['ok'] is True
    assert res_ok['attempt'].status_category == 'autorizada'
    assert res_ok['stored_document'] is not None
    assert res_ok['stored_document'].is_locked is True
    assert verify_stored_document(res_ok['stored_document']) is True

    # Cenário 2: rejeição simulada (cStat 225) -> registra tentativa, mas não grava nfeProc.
    res_rej = run_offline_cycle_simulation(
        db, draft_id='d' * 32, organization_id=1, farm_id=10, user_id=5,
        snapshot=snapshot, issuer=issuer, details=details, technical=technical,
        identification=identification, issuer_crt='3', extra_data=extra_data,
        simulated_c_stat='225', simulated_x_motivo='Rejeicao: Falha no Schema XML (SIMULACAO)',
    )
    assert res_rej['ok'] is False
    assert res_rej['attempt'].status_category == 'rejeitada'
    assert res_rej['stored_document'] is None

    events = db.query(NfeDraftEvent).filter_by(draft_id='d' * 32).all()
    assert len(events) == 2
    db.close()
