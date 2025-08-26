from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Core API Settings
    api_secret_key: str | None = Field(None, alias="API_SECRET_KEY")
    api_host: str = Field("0.0.0.0", alias="API_HOST")
    api_port: int = Field(8000, alias="API_PORT")
    environment: str = Field("development", alias="ENVIRONMENT")
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    
    # Removed database_url - now exclusively using Supabase
    # Supabase (primary) - required for all database operations
    supabase_project_ref: str = Field(..., alias="SUPABASE_PROJECT_REF", description="Supabase project reference ID (required)")
    supabase_db_password: str = Field(..., alias="SUPABASE_DB_PASSWORD", description="Supabase database password (required)")
    supabase_db_user: str = Field("postgres", alias="SUPABASE_DB_USER")
    supabase_db_name: str = Field("postgres", alias="SUPABASE_DB_NAME")
    
    # Analytics Platform Integrations
    posthog_api_key: str | None = Field(None, alias="POSTHOG_API_KEY")
    posthog_host: str | None = Field(None, alias="POSTHOG_HOST")
    mixpanel_token: str | None = Field(None, alias="MIXPANEL_TOKEN")
    mixpanel_api_secret: str | None = Field(None, alias="MIXPANEL_API_SECRET")
    amplitude_api_key: str | None = Field(None, alias="AMPLITUDE_API_KEY")
    amplitude_secret_key: str | None = Field(None, alias="AMPLITUDE_SECRET_KEY")
    redis_url: str = Field("redis://localhost:6379/0", alias="REDIS_URL")
    slack_bot_token: str | None = Field(None, alias="SLACK_BOT_TOKEN")
    slack_report_channel: str | None = Field(None, alias="SLACK_REPORT_CHANNEL")
    slack_signing_secret: str | None = Field(None, alias="SLACK_SIGNING_SECRET")
    notion_api_key: str | None = Field(None, alias="NOTION_API_KEY")
    notion_database_id: str | None = Field(None, alias="NOTION_DATABASE_ID")
    # Email digest settings
    smtp_host: str | None = Field(None, alias="SMTP_HOST")
    smtp_port: int = Field(587, alias="SMTP_PORT")
    smtp_user: str | None = Field(None, alias="SMTP_USER")
    smtp_password: str | None = Field(None, alias="SMTP_PASSWORD")
    email_from: str | None = Field(None, alias="EMAIL_FROM")
    email_recipients: str | None = Field(None, alias="EMAIL_RECIPIENTS")  # comma list
    cluster_model_refresh_days: int = Field(7, alias="CLUSTER_MODEL_REFRESH_DAYS")
    app_env: str = Field("dev", alias="APP_ENV")
    ingest_secret: str | None = Field(None, alias="INGEST_SECRET")
    retention_days: int = Field(365, alias="RETENTION_DAYS")
    rate_limit_per_minute: int = Field(600, alias="RATE_LIMIT_PER_MINUTE")
    funnel_cache_ttl_seconds: int = Field(30, alias="FUNNEL_CACHE_TTL_SECONDS")
    migrate_on_start: bool = Field(False, alias="MIGRATE_ON_START")
    api_keys: str | None = Field(None, alias="API_KEYS")  # comma-separated list of allowed API keys
    api_key_rate_limit_per_minute: int = Field(600, alias="API_KEY_RATE_LIMIT_PER_MINUTE")
    allowed_events: str | None = Field(None, alias="ALLOWED_EVENTS")  # comma separated list of allowed event names (optional)
    pii_fields: str | None = Field(None, alias="PII_FIELDS")  # comma separated prop field names to mask
    api_key_project_map: str | None = Field(None, alias="API_KEY_PROJECT_MAP")  # format key:project;key2:project2
    api_key_scopes: str | None = Field(None, alias="API_KEY_SCOPES")  # format key:scope1|scope2;key2:scope
    enable_rls: bool = Field(False, alias="ENABLE_RLS")
    rls_tables: str | None = Field("raw_events,funnel_metrics,session_summary,insights", alias="RLS_TABLES")  # comma list
    rls_tenant_column: str = Field("project", alias="RLS_TENANT_COLUMN")
    partitioning_enabled: bool = Field(True, alias="PARTITIONING_ENABLED")
    partition_months_ahead: int = Field(1, alias="PARTITION_MONTHS_AHEAD")
    insight_auto_ship_days: int = Field(14, alias="INSIGHT_AUTO_SHIP_DAYS")
    insight_accept_sla_days: int = Field(7, alias="INSIGHT_ACCEPT_SLA_DAYS")
    # Archive compaction settings
    archive_compact_after_days: int = Field(60, alias="ARCHIVE_COMPACT_AFTER_DAYS")
    archive_compaction_batch_days: int = Field(5, alias="ARCHIVE_COMPACTION_BATCH_DAYS")
    # Ingestion & DLQ enhancements
    late_arrival_threshold_hours: int = Field(24, alias="LATE_ARRIVAL_THRESHOLD_HOURS")
    dlq_quarantine_attempts: int = Field(5, alias="DLQ_QUARANTINE_ATTEMPTS")
    ingest_batch_upsert_size: int = Field(500, alias="INGEST_BATCH_UPSERT_SIZE")
    session_timeout_minutes: int = Field(30, alias="SESSION_TIMEOUT_MINUTES")
    segment_props: str | None = Field(None, alias="SEGMENT_PROPS")  # comma list of property keys for segmentation
    event_name_aliases: str | None = Field(None, alias="EVENT_NAME_ALIASES")  # JSON mapping variant->canonical
    prop_rename_map: str | None = Field(None, alias="PROP_RENAME_MAP")  # JSON mapping old->new

    identity_email_enabled: bool = Field(True, alias="IDENTITY_EMAIL_ENABLED")
    # Streaming / Kafka (optional)
    kafka_bootstrap_servers: str | None = Field(None, alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_events_topic: str | None = Field(None, alias="KAFKA_EVENTS_TOPIC")
    kafka_consumer_group: str | None = Field("onboarding-analyzer", alias="KAFKA_CONSUMER_GROUP")
    kafka_max_batch: int = Field(500, alias="KAFKA_MAX_BATCH")
    kafka_poll_timeout_ms: int = Field(1000, alias="KAFKA_POLL_TIMEOUT_MS")
    # Query DSL limits
    query_max_rows: int = Field(10000, alias="QUERY_MAX_ROWS")
    query_max_interval_days: int = Field(90, alias="QUERY_MAX_INTERVAL_DAYS")
    # Auto index & performance tuning
    auto_index_enabled: bool = Field(True, alias="AUTO_INDEX_ENABLED")
    # Drift thresholds
    feature_psi_drift_threshold: float = Field(0.2, alias="FEATURE_PSI_DRIFT_THRESHOLD")
    feature_ks_drift_threshold: float = Field(0.15, alias="FEATURE_KS_DRIFT_THRESHOLD")
    stream_publish_enabled: bool = Field(True, alias="STREAM_PUBLISH_ENABLED")
    churn_inactivity_days: int = Field(14, alias="CHURN_INACTIVITY_DAYS")
    # Experiment promotion automation tunables
    experiment_promotion_interval_minutes: int = Field(30, alias="EXPERIMENT_PROMOTION_INTERVAL_MINUTES")
    experiment_promotion_min_prob: float = Field(0.8, alias="EXPERIMENT_PROMOTION_MIN_PROB")
    experiment_promotion_min_lift: float = Field(0.0, alias="EXPERIMENT_PROMOTION_MIN_LIFT")
    # Secret management
    secret_provider: str = Field("env", alias="SECRET_PROVIDER")  # env|aws
    secret_rotation_days: int = Field(90, alias="SECRET_ROTATION_DAYS")
    aws_secret_arn: str | None = Field(None, alias="AWS_SECRET_ARN")

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "allow"  # Allow extra environment variables


@lru_cache
def get_settings() -> Settings:
    return Settings()


def reset_settings():
    """Clear cached settings (useful in tests when env vars change)."""
    try:
        get_settings.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass

def parse_api_keys(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [k.strip() for k in raw.split(",") if k.strip()]

def parse_allowed_events(raw: str | None) -> set[str]:
    return {e.strip() for e in raw.split(",") if e.strip()} if raw else set()

def parse_api_key_project_map(raw: str | None) -> dict[str,str]:
    mapping: dict[str,str] = {}
    if not raw:
        return mapping
    parts = [p for p in raw.split(";") if p.strip()]
    for p in parts:
        if ":" in p:
            k,v = p.split(":",1)
            mapping[k.strip()] = v.strip()
    return mapping

def parse_pii_fields(raw: str | None) -> set[str]:
    return {f.strip() for f in raw.split(",") if f.strip()} if raw else set()

def parse_api_key_scopes(raw: str | None) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = {}
    if not raw:
        return mapping
    entries = [e for e in raw.split(";") if e.strip()]
    for e in entries:
        if ":" in e:
            k, scopes = e.split(":", 1)
            scope_set = {s.strip() for s in scopes.split("|") if s.strip()}
            mapping[k.strip()] = scope_set or {"read"}
    return mapping

def parse_segment_props(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [p.strip() for p in raw.split(',') if p.strip()]

