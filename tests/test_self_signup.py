from datetime import date, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request

import app.models  # noqa: F401
from app.db.base import Base
from app.models import Organization, OrganizationAccessGrant, User
from app.routers import auth
from app.services import email_service
from app.services import self_signup as signup
from app.services import subscription_plans as plans

TODAY = date(2026, 11, 14)


def _db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    return Session(engine)


def _plan(db, name, modules, price, featured=False):
    plan = plans.create_plan(db, plans.parse_plan_form(name=name, description=None, price_monthly=price, price_quarterly='', price_semiannual='', price_yearly='',
                                                       max_farms='', max_users='', max_hectares='', modules=modules, trial_days='', display_order='',
                                                       is_featured='1' if featured else None))
    plans.activate_plan(db, plan)
    db.commit()
    return plan


def _data(org='Fazenda Boa Vista'):
    return signup.validate_signup('Ana', 'Souza', '35 99999-8888', org, '1')


def test_validation_rules():
    assert signup.format_phone('+55 (35) 99999-8888') == '(35) 99999-8888'
    assert signup.normalize_email('  Ana@Fazenda.COM.br ') == 'ana@fazenda.com.br'
    for call in (lambda: signup.format_phone('9999'), lambda: signup.normalize_email('ana@'), lambda: signup.validate_password('curta'),
                 lambda: signup.validate_signup('Ana', 'Souza', '35999998888', 'Fazenda', None)):
        with pytest.raises(signup.SignupError):
            call()


def test_creates_organization_admin_and_14_day_trial_on_most_complete_plan():
    db = _db()
    all_modules = [code for code, _ in plans.PLAN_MODULES]
    _plan(db, 'Essencial', all_modules[:3], '99,00', featured=True)
    full = _plan(db, 'Gestão Completa', all_modules, '299,00')
    user = signup.create_self_signup(db, email='ana@fazenda.com.br', data=_data(), password_hash='hash', plan_id=None, today=TODAY)

    assert user.is_admin and user.organization.name == 'Fazenda Boa Vista' and user.phone == '(35) 99999-8888'
    grant = db.query(OrganizationAccessGrant).one()
    assert grant.reason == 'teste_gratis' and grant.plan_id == full.id and grant.ends_on == TODAY + timedelta(days=14)
    with pytest.raises(signup.SignupError):
        signup.create_self_signup(db, email='ana@fazenda.com.br', data=_data(), password_hash='hash', plan_id=None, today=TODAY)


def test_chosen_plan_and_duplicate_organization_name():
    db = _db()
    essential = _plan(db, 'Essencial', [plans.PLAN_MODULES[0][0]], '99,00')
    signup.create_self_signup(db, email='a@x.com', data=_data(), password_hash='h', plan_id=essential.id, today=TODAY)
    second = signup.create_self_signup(db, email='b@x.com', data=_data(), password_hash='h', plan_id=essential.id, today=TODAY)
    assert second.organization.name == 'Fazenda Boa Vista 2' and second.organization.slug == 'fazenda-boa-vista-2'
    assert {g.plan_id for g in db.query(OrganizationAccessGrant)} == {essential.id}


def test_code_digest_is_not_the_code():
    digest = signup.code_digest('a@x.com', '123456')
    assert '123456' not in digest and signup.code_matches('a@x.com', ' 123456 ', digest) and not signup.code_matches('a@x.com', '654321', digest)


def _request(session):
    return Request({'type': 'http', 'method': 'POST', 'path': '/cadastro', 'headers': [], 'query_string': b'', 'session': session, 'client': ('1.2.3.4', 1)})


@pytest.fixture
def flow(monkeypatch):
    sent, rendered = [], []
    monkeypatch.setattr(auth, 'validate_csrf', lambda *a: None)
    monkeypatch.setattr(auth, 'append_audit_event', lambda **k: None)
    monkeypatch.setattr(auth, '_google_login_enabled', lambda: False)
    monkeypatch.setattr(auth.templates, 'TemplateResponse', lambda name, ctx, status_code=200: rendered.append(ctx) or SimpleNamespace(status_code=status_code, ctx=ctx))
    monkeypatch.setattr(email_service, 'send_plain_email', lambda to, subject, body, ctx: sent.append((to, subject, body)))
    monkeypatch.setattr(signup, '_rate_hits', {})
    return sent, rendered


def test_email_signup_flow_creates_account_after_code(flow):
    sent, rendered = flow
    db = _db()
    session = {}
    request = _request(session)
    assert auth.signup_email_action(request, email='Ana@Fazenda.com.br', website='', csrf_token='x', db=db).headers['location'] == '/cadastro/dados'
    response = auth.signup_details_action(request, first_name='Ana', last_name='Souza', phone='35999998888', organization_name='Fazenda Sol',
                                          password='senha-forte', accepted_terms='1', csrf_token='x', db=db)
    assert response.headers['location'] == '/cadastro/confirmar' and sent[0][0] == 'ana@fazenda.com.br'
    code = sent[0][1].split()[0]
    assert code not in str(session)
    wrong = auth.signup_confirm_action(request, code='000000' if code != '000000' else '111111', csrf_token='x', db=db)
    assert wrong.status_code == 400 and not db.query(User).count()
    done = auth.signup_confirm_action(request, code=code, csrf_token='x', db=db)
    assert done.headers['location'] == '/primeiros-passos' and session['user_email'] == 'ana@fazenda.com.br'
    user = db.query(User).one()
    assert auth.verify_password('senha-forte', user.hashed_password) and db.query(Organization).one().name == 'Fazenda Sol'


def test_existing_email_and_honeypot(flow):
    _, rendered = flow
    db = _db()
    db.add(User(name='X', email='ja@x.com', hashed_password='h'))
    db.commit()
    response = auth.signup_email_action(_request({}), email='ja@x.com', website='', csrf_token='x', db=db)
    assert response.status_code == 400 and rendered[-1]['existing_account']
    session = {}
    auth.signup_email_action(_request(session), email='robo@x.com', website='spam', csrf_token='x', db=db)
    assert 'signup' not in session


def test_google_signup_skips_password_and_code(flow):
    sent, _ = flow
    db = _db()
    session = {'signup': {'email': 'g@x.com', 'google': True, 'first_name': 'Gil', 'last_name': 'Lima'}}
    response = auth.signup_details_action(_request(session), first_name='Gil', last_name='Lima', phone='3533334444', organization_name='Sítio G',
                                          password='', accepted_terms='1', csrf_token='x', db=db)
    assert response.headers['location'] == '/primeiros-passos' and db.query(User).one().email == 'g@x.com'
    assert not [s for s in sent if s[0] == 'g@x.com']


def test_farm_names_are_unique_per_organization_only():
    from app.models import Farm
    from app.web import routes
    db = _db()
    a = signup.create_self_signup(db, email='a@x.com', data=_data('Org A'), password_hash='h', plan_id=None, today=TODAY)
    b = signup.create_self_signup(db, email='b@x.com', data=_data('Org B'), password_hash='h', plan_id=None, today=TODAY)
    db.add_all([Farm(name='Fazenda Santa Maria', location='MG', total_area=10, organization_id=a.organization_id),
                Farm(name='Fazenda Santa Maria', location='SP', total_area=20, organization_id=b.organization_id)])
    db.commit()
    assert routes._farm_name_taken(db, a.organization_id, ' fazenda santa maria ')
    farm_id = db.query(Farm.id).filter_by(organization_id=a.organization_id).scalar()
    assert not routes._farm_name_taken(db, a.organization_id, 'Fazenda Santa Maria', exclude_id=farm_id)
    assert not routes._farm_name_taken(db, a.organization_id, 'Outra')
