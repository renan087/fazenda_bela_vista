from datetime import date
from types import SimpleNamespace

from app.services import dashboard


def test_finance_flow_keeps_all_items_for_attention_modal():
    today = date(2026, 11, 15)
    installments = [SimpleNamespace(id=i, due_date=today, installment_number=i, amount=10, status='pendente', paid_at=None) for i in range(1, 9)]
    transaction = SimpleNamespace(operation_type='despesa', amount=80, finance_account_id=None, source='contas', payment_condition='a_prazo', credit_card_id=None, installments=installments, installment_count=8, product_service='Serviço')
    repository = SimpleNamespace(list_finance_accounts=lambda **kwargs: [], list_finance_transactions=lambda **kwargs: [transaction])
    result = dashboard._build_dashboard_finance_flow(repository, 1, today)
    assert len(result['action_items']) == 6
    assert len(result['all_action_items']) == 8


def test_attention_list_preserves_all_eligible_items(monkeypatch):
    monkeypatch.setattr(dashboard, '_dashboard_rain', lambda *args, **kwargs: {'latest': None, 'latest_days': None})
    monkeypatch.setattr(dashboard, '_dashboard_cash_weeks', lambda *args, **kwargs: [])
    monkeypatch.setattr(dashboard, '_dashboard_calendar', lambda *args, **kwargs: [])
    monkeypatch.setattr(dashboard, '_dashboard_agenda', lambda *args, **kwargs: [])
    finance = [{'status': 'overdue', 'type': 'pagar', 'title': f'Conta {i}', 'amount': 10, 'installment': '1/1', 'source': 'Manual', 'link': '/contas'} for i in range(8)]
    stocks = [{'stock_level': 'critical', 'stock_level_label': 'Crítico', 'name': f'Insumo {i}', 'available_quantity': 1, 'unit': 'kg', 'low_stock_threshold': 2} for i in range(3)]
    schedules = [{'status': 'late', 'plot': f'Setor {i}', 'application_method_label': 'Adubação', 'date': '2026-11-14', 'link': f'/agenda/{i}'} for i in range(2)]
    pests = [SimpleNamespace(name=f'Praga {i}', plot=None, plot_id=1, occurrence_date=date(2026, 11, 14), severity=3, category='Praga', id=i) for i in range(2)]
    result = dashboard.build_dashboard_overview(SimpleNamespace(), farm_id=1, season=None, today=date(2026, 11, 15), plots=[], total_area=0, estimated_production=0, operational_cost_season=0, coffee_market={}, finance_flow={'action_items': finance[:6], 'all_action_items': finance}, low_stock_items=stocks, schedule_alerts=schedules, incidents=pests, forecast_plots=[], cost_by_plot={}, sector_cost_per_ha={}, activity_timeline=[])
    assert len(result['inbox']) == 6
    assert len(result['inbox_all']) == 15
    assert [item['permission'] for item in result['inbox_all']].count('page.finance') == 8
    assert [item['permission'] for item in result['inbox_all']].count('page.inputs') == 3
    assert [item['permission'] for item in result['inbox_all']].count('page.agenda') == 2
    assert [item['permission'] for item in result['inbox_all']].count('page.pests') == 2


def test_attention_permissions_apply_before_six_item_limit():
    overview = {'inbox_all': [{'permission': 'page.finance', 'title': f'Conta {i}'} for i in range(8)] + [{'permission': 'page.inputs', 'title': f'Insumo {i}'} for i in range(7)]}
    dashboard.filter_dashboard_attention(overview, {'page.inputs'})
    assert len(overview['inbox']) == 6 and len(overview['inbox_all']) == 7
    assert all(item['permission'] == 'page.inputs' for item in overview['inbox_all'])


def test_attention_link_only_when_more_than_six():
    from pathlib import Path
    from jinja2 import Environment
    source = (Path(__file__).resolve().parents[1] / 'app/templates/dashboard.html').read_text()
    card = source.split('<section class="db-card" aria-label="Precisa de você hoje">', 1)[1].split('</section>', 1)[0]
    template = Environment(autoescape=True).from_string(card)
    items = [{'title': f'Pendência {i}', 'text': '', 'tone': 'warning', 'link': '/agenda', 'action': 'Ver'} for i in range(7)]
    six = template.render(ov={'inbox': items[:6], 'inbox_all': items[:6]})
    seven = template.render(ov={'inbox': items[:6], 'inbox_all': items})
    assert 'data-db-attention-open' not in six
    assert 'Ver todas as 7 pendências' in seven
    assert 'Pendência 6' not in seven
