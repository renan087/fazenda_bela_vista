from app.core.config import Settings


def test_default_supabase_organization_slug_is_sisfarm():
    assert Settings().supabase_organization_slug == "sisfarm"
