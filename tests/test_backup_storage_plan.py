from app.services.backup_service import BYTES_PER_GIB, SUPABASE_STORAGE_QUOTA_BY_PLAN_GIB


def test_storage_quota_for_free_plan():
    assert SUPABASE_STORAGE_QUOTA_BY_PLAN_GIB["free"] * BYTES_PER_GIB == BYTES_PER_GIB


def test_storage_quota_for_paid_plans():
    assert SUPABASE_STORAGE_QUOTA_BY_PLAN_GIB["pro"] == 100
    assert SUPABASE_STORAGE_QUOTA_BY_PLAN_GIB["team"] == 100
