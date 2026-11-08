from app.services.backup_service import BYTES_PER_GIB, _find_storage_quota_bytes


def test_extracts_storage_quota_from_organization_entitlement():
    payload = {
        "entitlements": [
            {
                "feature": {"key": "usage.storage_size", "type": "number"},
                "config": {"quota_gb": 100},
            }
        ]
    }

    assert _find_storage_quota_bytes(payload) == 100 * BYTES_PER_GIB


def test_ignores_unrelated_quota_values():
    payload = {"entitlements": [{"feature": {"key": "usage.egress"}, "config": {"quota_gb": 250}}]}

    assert _find_storage_quota_bytes(payload) is None
