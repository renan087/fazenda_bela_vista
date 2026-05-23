"""Segurança — senha e tokens."""

from datetime import timedelta

import pytest

from app.core.security import (
    authenticate_user,
    create_access_token,
    decode_token,
    get_password_hash,
    verify_password,
)
from app.models import User

pytestmark = pytest.mark.suite_security


def test_password_hash_roundtrip() -> None:
    hashed = get_password_hash("senha-forte-123")
    assert hashed != "senha-forte-123"
    assert verify_password("senha-forte-123", hashed)
    assert not verify_password("outra-senha", hashed)


def test_authenticate_user_respects_active_flag() -> None:
    user = User(
        name="Teste",
        email="teste@example.com",
        hashed_password=get_password_hash("abc"),
        is_active=True,
    )
    assert authenticate_user(user, "abc")
    user.is_active = False
    assert not authenticate_user(user, "abc")


def test_jwt_create_and_decode() -> None:
    token = create_access_token("user@example.com", expires_delta=timedelta(minutes=5))
    payload = decode_token(token)
    assert payload is not None
    assert payload.get("sub") == "user@example.com"
