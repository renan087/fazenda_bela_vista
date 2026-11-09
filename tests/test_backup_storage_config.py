from app.core.config import Settings


def test_supabase_organization_slug_is_optional():
    assert Settings().supabase_organization_slug is None
