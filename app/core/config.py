from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Fazenda Bela Vista"
    secret_key: str = "troque-esta-chave-em-producao"
    access_token_expire_minutes: int = 60
    session_idle_timeout_hours: int = 4
    environment: str = "development"
    port: int = 8000
    app_timezone: str = "America/Sao_Paulo"
    session_cookie_name: str = "fazenda_session"
    database_url_override: str | None = None
    postgres_server: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "fazenda_cafe"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    admin_email: str = "admin@fazenda.local"
    admin_password: str = "admin123"
    openai_api_key: str | None = None
    openai_recommendation_model: str = "gpt-5-mini"
    openai_timeout_seconds: float = 25.0
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_username: str | None = None
    smtp_password: str | None = None
    smtp_from_email: str | None = None
    smtp_from_name: str = "Fazenda Bela Vista"
    smtp_use_tls: bool = True
    two_factor_code_minutes: int = 10
    two_factor_max_attempts: int = 5
    password_reset_token_minutes: int = 60
    trusted_browser_days: int = 5
    trusted_browser_cookie_name: str = "fazenda_trusted_browser"

    @property
    def is_production(self) -> bool:
        return self.environment.lower() == "production"

    @property
    def session_idle_timeout_seconds(self) -> int:
        return self.session_idle_timeout_hours * 60 * 60

    @property
    def trusted_browser_seconds(self) -> int:
        return self.trusted_browser_days * 24 * 60 * 60

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def database_url(self) -> str:
        if self.database_url_override:
            if self.database_url_override.startswith("postgresql://"):
                return self.database_url_override.replace("postgresql://", "postgresql+psycopg://", 1)
            if self.database_url_override.startswith("postgres://"):
                return self.database_url_override.replace("postgres://", "postgresql+psycopg://", 1)
            return self.database_url_override
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_server}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
