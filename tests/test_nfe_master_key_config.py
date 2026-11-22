from app.services import nfe_certificate_vault as vault


class _S:
    def __init__(self, key):
        self.nfe_cert_master_key = key


def test_master_key_from_environment(monkeypatch):
    monkeypatch.setenv('NFE_CERT_MASTER_KEY', 'a' * 32)
    assert vault.is_master_key_configured()


def test_master_key_falls_back_to_settings(monkeypatch):
    monkeypatch.delenv('NFE_CERT_MASTER_KEY', raising=False)
    monkeypatch.setattr('app.core.config.get_settings', lambda: _S('b' * 40))
    assert vault.is_master_key_configured()
    assert vault._get_master_key() == b'b' * 40


def test_master_key_too_short(monkeypatch):
    monkeypatch.delenv('NFE_CERT_MASTER_KEY', raising=False)
    monkeypatch.setattr('app.core.config.get_settings', lambda: _S('curta'))
    assert not vault.is_master_key_configured()
