from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Fazenda Bela Vista"
    secret_key: str = "troque-esta-chave-em-producao"
    access_token_expire_minutes: int = 60
    environment: str = "development"
    port: int = 8000
    database_url_override: str | None = None
    postgres_server: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "fazenda_cafe"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    admin_email: str = "admin@fazenda.local"
    admin_password: str = "admin123"

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def database_url(self) -> str:
        if self.database_url_override:
            return self.database_url_override.replace("postgres://", "postgresql+psycopg://", 1)
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_server}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
