from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import secrets

from jose import JWTError, jwt
from passlib.context import CryptContext

from app.core.config import get_settings
from app.models.user import User

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
ALGORITHM = "HS256"


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def authenticate_user(user: User | None, password: str) -> bool:
    if not user or not user.is_active:
        return False
    return verify_password(password, user.hashed_password)


def create_access_token(subject: str, expires_delta: timedelta | None = None) -> str:
    settings = get_settings()
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=settings.access_token_expire_minutes)
    )
    payload = {"sub": subject, "exp": expire}
    return jwt.encode(payload, settings.secret_key, algorithm=ALGORITHM)


def decode_token(token: str) -> dict | None:
    settings = get_settings()
    try:
        return jwt.decode(token, settings.secret_key, algorithms=[ALGORITHM])
    except JWTError:
        return None


def generate_numeric_code(length: int = 6) -> str:
    upper = 10**length
    return f"{secrets.randbelow(upper):0{length}d}"


def hash_verification_code(code: str) -> str:
    settings = get_settings()
    secret = settings.secret_key.encode("utf-8")
    digest = hashlib.sha256(secret + code.encode("utf-8")).hexdigest()
    return digest


def verify_verification_code(code: str, code_hash: str) -> bool:
    calculated = hash_verification_code(code)
    return hmac.compare_digest(calculated, code_hash)


def generate_persistent_token() -> str:
    return secrets.token_urlsafe(32)


def _hash_with_secret(value: str) -> str:
    settings = get_settings()
    secret = settings.secret_key.encode("utf-8")
    digest = hashlib.sha256(secret + value.encode("utf-8")).hexdigest()
    return digest


def hash_persistent_token(token: str) -> str:
    return _hash_with_secret(token)


def hash_browser_fingerprint(user_agent: str) -> str:
    return _hash_with_secret(user_agent or "")
