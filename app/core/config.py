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
    admin_name: str = "Renan Terra Castro Correia"
    admin_email: str = "renan087@gmail.com"
    super_admin_email: str | None = None
    openai_api_key: str | None = None
    openai_recommendation_model: str = "gpt-5-mini"
    openai_assistant_model: str = "gpt-4o-mini"
    openai_timeout_seconds: float = 25.0
    whatsapp_verify_token: str | None = None
    whatsapp_access_token: str | None = None
    whatsapp_phone_number_id: str | None = None
    whatsapp_app_secret: str | None = None
    whatsapp_graph_version: str = "v23.0"
    whatsapp_display_number: str | None = None
    whatsapp_signing_invite_template: str | None = "sisfarm_convite_assinatura"
    whatsapp_signing_otp_template: str | None = None
    whatsapp_template_language: str = "pt_BR"
    whatsapp_idle_minutes: int = 30
    whatsapp_closing_minutes: int = 10
    whatsapp_survey_minutes: int = 30
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_username: str | None = None
    smtp_password: str | None = None
    smtp_from_email: str | None = None
    smtp_from_name: str = "SiSFarm"
    smtp_use_tls: bool = True
    two_factor_code_minutes: int = 10
    two_factor_max_attempts: int = 5
    password_reset_token_minutes: int = 60
    trusted_browser_days: int = 5
    trusted_browser_cookie_name: str = "fazenda_trusted_browser"
    google_client_id: str | None = None
    google_client_secret: str | None = None
    google_redirect_uri: str | None = None
    # Maps Static API (satélite + path) para prévia de fazenda; opcional — sem chave usa fallback Esri
    google_maps_api_key: str | None = None
    supabase_url: str | None = None
    supabase_service_role_key: str | None = None
    # Token da Management API. Usado exclusivamente no backend ao executar backups.
    supabase_access_token: str | None = None
    supabase_organization_slug: str | None = None
    supabase_bucket_db: str = "backups-db"
    supabase_bucket_files: str = "anexos"
    # Verificacao diaria do repositorio de backups no PostgreSQL do Supabase.
    supabase_backup_monitor_enabled: bool = True
    backup_local_dirs: str | None = None
    backup_failure_alert_email: str | None = None
    # Asaas (assinaturas / clientes). Opcional: sem ASAAS_API_KEY o formulário só valida e exibe o JSON.
    asaas_api_key: str | None = None
    asaas_api_base_url: str = "https://api-sandbox.asaas.com"
    # Ambiente das assinaturas: "sandbox" (testes) ou "production". Producao exige tambem
    # ASAAS_API_BASE_URL=https://api.asaas.com, para nao ativar cobrancas reais por engano.
    asaas_environment: str = "sandbox"
    asaas_live_enabled: bool = False
    asaas_pilot_organization_ids: str = ""
    # Endereco publico do SiSFarm, usado no retorno do checkout do Asaas.
    app_public_url: str = "https://app.sisfarm.com.br"
    # Contato exibido ao cliente na pagina "Minha assinatura" (opcional).
    support_email: str | None = None
    # Tag do Google (Google Ads/GTM). Vazio desliga a tag.
    google_tag_id: str | None = "GTM-P9XTDXZW"
    maintenance_mode: str | None = None  # plano B: "leitura" ou "fora_do_ar" pelo painel do Render
    # Dias de atraso tolerados antes do modo somente leitura.
    subscription_grace_days: int = 10
    # Token do webhook (campo "Token de autenticação" no Asaas); enviado no header asaas-access-token.
    asaas_webhook_token: str | None = None
    mercadopago_public_key: str | None = None
    mercadopago_access_token: str | None = None
    mercadopago_webhook_secret: str | None = None
    mercadopago_sandbox: bool = False
    mercadopago_live_enabled: bool = False
    mercadopago_live_checkout_enabled: bool = False
    mercadopago_live_seller_id: int | None = None
    mercadopago_live_access_token: str | None = None
    mercadopago_live_webhook_secret: str | None = None
    # Mercado Pago Pix (aplicação independente via Checkout Transparente / API de Orders).
    mercadopago_pix_enabled: bool = False
    mercadopago_pix_application_id: str = "7441327382083069"
    mercadopago_pix_seller_id: int = 82986976
    mercadopago_pix_access_token: str | None = None
    mercadopago_pix_public_key: str | None = None
    mercadopago_pix_webhook_secret: str | None = None
    mercadopago_pix_test_access_token: str | None = None
    mercadopago_pix_test_public_key: str | None = None
    mercadopago_pix_test_webhook_secret: str | None = None
    # Chave mestra do cofre de certificados A1 (AES-256-GCM). Mínimo 32 caracteres; nunca vai ao banco.
    # Não troque depois de enviar certificados: os já guardados deixam de abrir.
    nfe_cert_master_key: str | None = None
    digital_certificate_key: str | None = None
    digital_document_key: str | None = None
    digital_signing_iti_receipts: str | None = None
    digital_signing_pilot_farm_ids: str = ""
    digital_validation_hosts: str = ""
    memory_monitor_enabled: bool = True
    memory_monitor_interval_seconds: int = 60
    coffee_quote_sync_enabled: bool = True
    coffee_quote_sync_retry_interval_seconds: int = 120
    coffee_quote_sync_max_attempts_per_burst: int = 15
    coffee_quote_sync_max_burst_seconds: int = 1800
    coffee_quote_sync_idle_interval_seconds: int = 3600
    automated_tests_state_file: str = ".cache/automated_tests_state.json"
    coffee_quote_market_poll_start_hour: int = 18
    coffee_quote_market_poll_start_minute: int = 30
    coffee_quote_market_poll_interval_seconds: int = 1800

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

    @property
    def database_backup_url(self) -> str:
        if self.database_url_override:
            if self.database_url_override.startswith("postgresql+psycopg://"):
                return self.database_url_override.replace("postgresql+psycopg://", "postgresql://", 1)
            if self.database_url_override.startswith("postgres+psycopg://"):
                return self.database_url_override.replace("postgres+psycopg://", "postgres://", 1)
            return self.database_url_override
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_server}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
