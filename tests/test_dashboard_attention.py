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


def test_attention_groups_installments_and_splits_sections():
    today = date(2026, 11, 21)
    bill = lambda name, n, status, day, amount: {'permission': 'page.finance', 'kind': 'bill', 'name': name, 'type': 'pagar', 'status': status, 'date': day, 'amount': amount, 'installment': n, 'source': 'Patrimônio', 'link': '/contas', 'title': name, 'text': ''}
    inbox = [bill('Bomba', '8/18', 'overdue', date(2026, 9, 25), 116.11), bill('Bomba', '9/18', 'overdue', date(2026, 10, 25), 116.11),
             bill('Coelba', '1/1', 'today', today, 575.97),
             {'permission': 'page.agenda', 'title': 'Aplicação atrasada — Setor 1', 'text': 'Foliar', 'action': 'Ver', 'link': '/agenda', 'tone': 'danger'}]
    upcoming = [{'status': 'upcoming', 'type': 'pagar', 'title': 'Diesel', 'date': date(2026, 11, 24), 'amount': 120.0, 'installment': '1/1', 'source': 'Suprimentos', 'link': '/contas'}]
    result = dashboard.build_attention(inbox, upcoming, today)
    late, today_sec, nxt = result['sections']
    assert [r['title'] for r in late['rows']] == ['Bomba', 'Aplicação atrasada — Setor 1']
    assert late['rows'][0]['amount'] == 232.22 and len(late['rows'][0]['kids']) == 2
    assert late['rows'][0]['meta'].startswith('2 parcelas vencidas')
    assert today_sec['rows'][0]['meta'].startswith('Vence hoje')
    assert nxt['rows'][0]['title'] == 'Diesel' and result['next_total'] == 120.0
    assert result['overdue_total'] == 232.22 and result['count'] == 4
    assert result['bill_count'] == 3 and result['field_count'] == 1
    assert result['card_rows'][0]['title'] == 'Coelba'
    assert [d['value'] for d in result['days']][7] == 575.97 and [d['value'] for d in result['days']][10] == 120.0


def test_attention_filter_hides_finance_sections_without_permission():
    overview = {'today': date(2026, 11, 21), 'inbox_all': [{'permission': 'page.agenda', 'title': 'A', 'text': '', 'action': 'Ver', 'link': '/a'}],
                'finance_upcoming': [{'status': 'upcoming', 'type': 'pagar', 'title': 'Diesel', 'date': date(2026, 11, 24), 'amount': 1.0, 'installment': '1/1', 'source': 'S', 'link': '/c'}]}
    dashboard.filter_dashboard_attention(overview, {'page.agenda'})
    assert overview['attention']['sections'][2]['rows'] == [] and overview['attention']['field_count'] == 1


def test_spark_path_needs_two_points():
    assert dashboard._spark_path([1500]) is None
    spark = dashboard._spark_path([1500, 1650])
    assert spark['line'].startswith('M4.0') and spark['change'] == 10.0
