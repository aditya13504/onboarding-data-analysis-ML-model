from __future__ import annotations
from datetime import datetime
from sqlalchemy import String, Integer, DateTime, JSON, ForeignKey, Float, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from onboarding_analyzer.infrastructure.db import Base


class RawEvent(Base):
    __tablename__ = "raw_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    session_id: Mapped[str] = mapped_column(String(64), index=True)
    event_name: Mapped[str] = mapped_column(String(128), index=True)
    ts: Mapped[datetime] = mapped_column(DateTime, index=True)
    props: Mapped[dict] = mapped_column(JSON)
    schema_version: Mapped[str | None] = mapped_column(String(16), index=True, default=None)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    session_event_index: Mapped[int | None] = mapped_column(Integer, default=None, index=True)
    step_index: Mapped[int | None] = mapped_column(Integer, default=None, index=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), index=True, default=None)

    __table_args__ = (
        Index("ix_event_user_session", "user_id", "session_id", "ts"),
        # Additional composite indexes for common query patterns (analytics windows)
        Index("ix_event_name_ts", "event_name", "ts"),
        Index("ix_user_ts", "user_id", "ts"),
        Index("ix_project_event_ts", "project", "event_name", "ts"),
    Index("ix_event_idempotency", "idempotency_key"),
    )


class FunnelMetric(Base):
    __tablename__ = "funnel_metrics"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    step_name: Mapped[str] = mapped_column(String(128), index=True)
    step_order: Mapped[int] = mapped_column(Integer, index=True)
    users_entered: Mapped[int] = mapped_column(Integer)
    users_converted: Mapped[int] = mapped_column(Integer)
    drop_off: Mapped[int] = mapped_column(Integer)
    conversion_rate: Mapped[float] = mapped_column(Float)
    calc_ts: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    # No additional table args


class UserIdentityMap(Base):
    __tablename__ = "user_identity_map"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    primary_user_id: Mapped[str] = mapped_column(String(64), index=True)
    alias_user_id: Mapped[str] = mapped_column(String(64), index=True)
    source: Mapped[str | None] = mapped_column(String(32), index=True, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_user_identity_unique", "primary_user_id", "alias_user_id", unique=True),
    )


class EventNameMap(Base):
    __tablename__ = "event_name_map"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    canonical_name: Mapped[str] = mapped_column(String(128), index=True)
    variant_name: Mapped[str] = mapped_column(String(128), index=True)
    source: Mapped[str | None] = mapped_column(String(32), index=True, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_event_name_variant_unique", "canonical_name", "variant_name", unique=True),
    )


class NormalizationRule(Base):
    """Versioned normalization rules: property renames, value mappings, event aliases.

    rule_type: event_alias|prop_rename|value_map
    payload: JSON structure with keys depending on rule_type.
    """
    __tablename__ = "normalization_rules"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    rule_type: Mapped[str] = mapped_column(String(32), index=True)
    payload: Mapped[dict] = mapped_column(JSON)
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    version: Mapped[int] = mapped_column(Integer, default=1, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    author: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    __table_args__ = (
        Index("ix_norm_rule_type_active", "rule_type", "active"),
    )


class FrictionCluster(Base):
    __tablename__ = "friction_clusters"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    label: Mapped[str] = mapped_column(String(128))
    size: Mapped[int] = mapped_column(Integer)
    drop_off_users: Mapped[int] = mapped_column(Integer)
    impact_score: Mapped[float] = mapped_column(Float)
    features_summary: Mapped[dict] = mapped_column(JSON)
    model_version: Mapped[str] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    # No additional table args


class ClusterLabel(Base):
    """Human-in-the-loop labeling for friction clusters."""
    __tablename__ = "cluster_labels"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cluster_id: Mapped[int] = mapped_column(Integer, ForeignKey("friction_clusters.id"), index=True)
    label: Mapped[str] = mapped_column(String(256))
    rationale: Mapped[str | None] = mapped_column(String(1024), default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    author: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    __table_args__ = (
        Index("ux_cluster_label_unique", "cluster_id", "label", unique=True),
    )


class Insight(Base):
    __tablename__ = "insights"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cluster_id: Mapped[int] = mapped_column(Integer, ForeignKey("friction_clusters.id"))
    title: Mapped[str] = mapped_column(String(256))
    recommendation: Mapped[str] = mapped_column(String(1024))
    priority: Mapped[int] = mapped_column(Integer, index=True)
    impact_score: Mapped[float] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    cluster: Mapped[FrictionCluster] = relationship("FrictionCluster")
    status: Mapped[str] = mapped_column(String(16), default="new", index=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    shipped_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    category: Mapped[str | None] = mapped_column(String(64), default="general", index=True)
    confidence: Mapped[float | None] = mapped_column(Float, default=0.5)
    rationale: Mapped[str | None] = mapped_column(String(1024), default=None)
    score: Mapped[float | None] = mapped_column(Float, default=None, index=True)
    # No additional table args


class SuggestionPattern(Base):
    """Catalog mapping friction pattern signatures to recommended interventions."""
    __tablename__ = "suggestion_patterns"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    description: Mapped[str] = mapped_column(String(512))
    recommendation: Mapped[str] = mapped_column(String(1024))
    category: Mapped[str] = mapped_column(String(64), index=True)
    weight: Mapped[float] = mapped_column(Float, default=1.0)
    base_weight: Mapped[float | None] = mapped_column(Float, default=None)  # original static weight before learning adjustments
    dynamic_adjust: Mapped[int] = mapped_column(Integer, default=1, index=True)  # allow auto-learning to tune weight
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    # No additional table args


class InsightSuppression(Base):
    """Manual or automatic suppression of insight generation (fatigue prevention)."""
    __tablename__ = "insight_suppressions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cluster_id: Mapped[int | None] = mapped_column(Integer, index=True, default=None)
    pattern_key: Mapped[str | None] = mapped_column(String(128), index=True, default=None)
    reason: Mapped[str | None] = mapped_column(String(256), default=None)
    author: Mapped[str | None] = mapped_column(String(64), default=None)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    __table_args__ = (
        Index("ix_suppression_cluster_pattern", "cluster_id", "pattern_key"),
    )


class InsightFeedback(Base):
    """Captures user feedback (thumbs up/down) to adjust confidence and ranking."""
    __tablename__ = "insight_feedback"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    insight_id: Mapped[int] = mapped_column(Integer, ForeignKey("insights.id"), index=True)
    feedback: Mapped[str] = mapped_column(String(8), index=True)  # up|down
    author: Mapped[str | None] = mapped_column(String(64), default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    weight: Mapped[float] = mapped_column(Float, default=1.0)
    # No additional table args


class ModelVersion(Base):
    __tablename__ = "model_versions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_name: Mapped[str] = mapped_column(String(64), index=True)
    version: Mapped[str] = mapped_column(String(32), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    notes: Mapped[str | None] = mapped_column(String(512), default=None)
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    promoted: Mapped[int] = mapped_column(Integer, default=0, index=True)  # flagged as production promoted
    # No additional table args


class IngestionDeadLetter(Base):
    __tablename__ = "ingestion_dead_letter"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    payload: Mapped[dict] = mapped_column(JSON)
    error: Mapped[str] = mapped_column(String(512))
    attempts: Mapped[int] = mapped_column(Integer, default=0, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    # No additional table args


class ApiKey(Base):
    __tablename__ = "api_keys"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    hashed_key: Mapped[str | None] = mapped_column(String(128), index=True, default=None)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    scopes: Mapped[str] = mapped_column(String(256), default="read|ingest")
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    daily_quota: Mapped[int | None] = mapped_column(Integer, default=None, index=True)
    monthly_quota: Mapped[int | None] = mapped_column(Integer, default=None, index=True)
    # No additional table args


class Role(Base):
    """RBAC role aggregating scopes for easier management.

    scopes: pipe-delimited string of scope tokens (e.g., read|ingest|admin|feature_admin)
    """
    __tablename__ = "roles"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    scopes: Mapped[str] = mapped_column(String(512), default="read")
    description: Mapped[str | None] = mapped_column(String(256), default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    __table_args__ = (
        Index("ix_role_active", "active"),
    )


class RoleAssignment(Base):
    """Assignment of a Role to an ApiKey (many-to-one role -> many keys).

    Effective scopes for an ApiKey = api_keys.scopes union role.scopes if assignment active.
    """
    __tablename__ = "role_assignments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    api_key_id: Mapped[int] = mapped_column(Integer, ForeignKey("api_keys.id"), index=True)
    role_id: Mapped[int] = mapped_column(Integer, ForeignKey("roles.id"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    __table_args__ = (
        Index("ix_role_assignment_key_role", "api_key_id", "role_id", unique=True),
    )

class PersonalizationRule(Base):
        """Configurable personalization rule.

        condition_expr: Python-like boolean expression evaluated in limited sandbox.
            Helpers available:
                count('event_name') -> recent event count (lookback_days window defined in task)
                feature('feature_key') -> latest feature view numeric value or 0
            Example:
                count('signup_started') > 0 and count('signup_completed') == 0
        cooldown_hours: minimum hours before creating another identical recommendation for same user.
        """
        __tablename__ = "personalization_rules"
        id: Mapped[int] = mapped_column(Integer, primary_key=True)
        key: Mapped[str] = mapped_column(String(128), index=True)
        project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
        condition_expr: Mapped[str] = mapped_column(String(512))
        recommendation: Mapped[str] = mapped_column(String(64), index=True)
        rationale_template: Mapped[str | None] = mapped_column(String(256), default=None)
        cooldown_hours: Mapped[int] = mapped_column(Integer, default=72)
        active: Mapped[int] = mapped_column(Integer, default=1, index=True)
        created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
        last_triggered_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
        __table_args__ = (
                Index("ux_rule_key_project", "key", "project", unique=True),
                Index("ix_rule_active", "active"),
                Index("ix_rule_recommendation", "recommendation"),
        )


class UserRecommendation(Base):
    """Personalized next-best-action recommendation for a user.

    status: new|accepted|dismissed|converted
    action_event_name: event that constitutes conversion if observed after creation.
    """
    __tablename__ = "user_recommendations"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    recommendation: Mapped[str] = mapped_column(String(64), index=True)
    rationale: Mapped[str | None] = mapped_column(String(256), default=None)
    status: Mapped[str] = mapped_column(String(16), index=True, default="new")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    acted_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    action_event_name: Mapped[str | None] = mapped_column(String(128), default=None, index=True)
    impressions: Mapped[int] = mapped_column(Integer, default=0)
    clicks: Mapped[int] = mapped_column(Integer, default=0)
    __table_args__ = (
        Index("ux_user_rec_unique", "user_id", "recommendation", unique=True),
        Index("ix_rec_status", "status"),
    )


class DataAsset(Base):
    """Logical data asset (raw table, derived dataset, feature view) for lineage graph."""
    __tablename__ = "data_assets"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    asset_key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    asset_type: Mapped[str] = mapped_column(String(32), index=True)  # raw|derived|feature|model_artifact
    description: Mapped[str | None] = mapped_column(String(256), default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    __table_args__ = (
        Index("ix_asset_type", "asset_type"),
    )


class DataLineageEdge(Base):
    """Directed lineage edge from upstream -> downstream asset."""
    __tablename__ = "data_lineage_edges"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    upstream_key: Mapped[str] = mapped_column(String(128), index=True)
    downstream_key: Mapped[str] = mapped_column(String(128), index=True)
    transform: Mapped[str | None] = mapped_column(String(256), default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    __table_args__ = (
        Index("ux_lineage_pair", "upstream_key", "downstream_key", unique=True),
    )


class ConnectorState(Base):
    __tablename__ = "connector_state"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_name: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    last_since_ts: Mapped[datetime | None] = mapped_column(DateTime, default=None)
    last_until_ts: Mapped[datetime | None] = mapped_column(DateTime, default=None)
    cursor: Mapped[str | None] = mapped_column(String(256), default=None)
    failure_count: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(String(256), default=None)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    # No additional table args

class ConnectorCredential(Base):
    """Encrypted credential storage for connectors (rotatable, no plaintext persistence).

    secret_key: logical name (e.g., POSTHOG_PROJECT_API_KEY). encrypted_value: Fernet encrypted bytes (base64 in JSON).
    version: increment on rotation so connectors can detect and refresh in-memory tokens.
    """
    __tablename__ = "connector_credentials"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_name: Mapped[str] = mapped_column(String(64), index=True)
    secret_key: Mapped[str] = mapped_column(String(128), index=True)
    encrypted_value: Mapped[str] = mapped_column(String(1024))
    version: Mapped[int] = mapped_column(Integer, default=1, index=True)
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    rotated_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    __table_args__ = (
        Index("ux_connector_secret_unique", "connector_name", "secret_key", unique=True),
    )


class BackfillJob(Base):
    """Tracks progress of long-running historical backfills for connectors.

    Enables resumable, windowed ingestion across large historical ranges without reprocessing
    already completed windows. Jobs advance current_ts forward in fixed windows until end_ts.
    """
    __tablename__ = "backfill_jobs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_name: Mapped[str] = mapped_column(String(64), index=True)
    start_ts: Mapped[datetime] = mapped_column(DateTime, index=True)
    end_ts: Mapped[datetime] = mapped_column(DateTime, index=True)
    current_ts: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    status: Mapped[str] = mapped_column(String(16), default="pending", index=True)  # pending|running|completed|failed
    total_events: Mapped[int] = mapped_column(Integer, default=0)
    windows_completed: Mapped[int] = mapped_column(Integer, default=0)
    window_minutes: Mapped[int] = mapped_column(Integer, default=60)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_backfill_connector_status", "connector_name", "status"),
    )


class AuditLog(Base):
    __tablename__ = "audit_log"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    endpoint: Mapped[str] = mapped_column(String(256), index=True)
    method: Mapped[str] = mapped_column(String(16))
    status: Mapped[int] = mapped_column(Integer, index=True)
    api_key: Mapped[str | None] = mapped_column(String(128), index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True)
    duration_ms: Mapped[int] = mapped_column(Integer)
    category: Mapped[str | None] = mapped_column(String(32), index=True, default=None)
    # No additional table args


class ModelArtifact(Base):
    __tablename__ = "model_artifacts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_name: Mapped[str] = mapped_column(String(64), index=True)
    version: Mapped[str] = mapped_column(String(32), index=True)
    artifact: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    artifact_hash: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    # No additional table args


class SessionSummary(Base):
    __tablename__ = "session_summary"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    session_id: Mapped[str] = mapped_column(String(64), index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True)
    start_ts: Mapped[datetime] = mapped_column(DateTime, index=True)
    end_ts: Mapped[datetime] = mapped_column(DateTime, index=True)
    duration_sec: Mapped[float] = mapped_column(Float)
    steps_count: Mapped[int] = mapped_column(Integer)
    completion_ratio: Mapped[float] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_session_summary_user_session", "user_id", "session_id"),
    )


class RawEventArchive(Base):
    __tablename__ = "raw_events_archive"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[str] = mapped_column(String(128), index=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    session_id: Mapped[str] = mapped_column(String(64), index=True)
    event_name: Mapped[str] = mapped_column(String(128), index=True)
    ts: Mapped[datetime] = mapped_column(DateTime, index=True)
    props: Mapped[dict] = mapped_column(JSON)
    schema_version: Mapped[str | None] = mapped_column(String(16), index=True, default=None)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    archived_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    # No additional table args


class ArchiveCompactedEvent(Base):
    """Time-bucketed aggregated archive for cold storage querying and size reduction.

    Aggregates archived events into daily buckets per (project, event_name, date) with counts and
    optional sample properties for later analytical hints. This enables deletion of raw rows older
    than configured threshold once compacted.
    """
    __tablename__ = "archive_compacted_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True)
    event_name: Mapped[str] = mapped_column(String(128), index=True)
    event_date: Mapped[datetime] = mapped_column(DateTime, index=True)
    events_count: Mapped[int] = mapped_column(Integer)
    users_count: Mapped[int] = mapped_column(Integer)
    sessions_count: Mapped[int] = mapped_column(Integer)
    sample_props: Mapped[dict | None] = mapped_column(JSON, default=None)
    first_ts: Mapped[datetime] = mapped_column(DateTime)
    last_ts: Mapped[datetime] = mapped_column(DateTime)
    compacted_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ux_archive_compact_key", "project", "event_name", "event_date", unique=True),
    )


class EventSchema(Base):
    """Configurable event schema registry enabling dynamic required property validation per event name.

    Allows declaring required property keys and minimum schema version; ingestion can optionally enforce.
    """
    __tablename__ = "event_schemas"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_name: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    required_props: Mapped[dict] = mapped_column(JSON, default=dict)  # map of prop_name->type (string label)
    min_version: Mapped[str] = mapped_column(String(16), default="v1")
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    status: Mapped[str] = mapped_column(String(16), default="approved", index=True)  # draft|approved
    proposed_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    # No additional table args


class DataQualitySnapshot(Base):
    __tablename__ = "data_quality_snapshots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    captured_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    event_name: Mapped[str | None] = mapped_column(String(128), index=True, default=None)
    metric_type: Mapped[str] = mapped_column(String(64), index=True)
    metric_value: Mapped[float] = mapped_column(Float)
    details: Mapped[dict | None] = mapped_column(JSON, default=None)
    __table_args__ = (
        Index("ix_dq_unique", "captured_at", "project", "event_name", "metric_type"),
    )


class UserFeature(Base):
    __tablename__ = "user_features"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    features: Mapped[dict] = mapped_column(JSON)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ux_user_feature_user_project", "user_id", "project", unique=True),
    )


class FeatureDefinition(Base):
    """Declarative definition of a feature with transformation metadata and versioning.

    feature_key: unique logical name (e.g., user.total_events_30d)
    entity: primary entity (user|session|project)
    expr: serialized transformation spec (SQL fragment or JSON pipeline)
    """
    __tablename__ = "feature_definitions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    feature_key: Mapped[str] = mapped_column(String(256), unique=True, index=True)
    entity: Mapped[str] = mapped_column(String(32), index=True)
    version: Mapped[str] = mapped_column(String(32), index=True, default="v1")
    expr: Mapped[str] = mapped_column(String(2048))
    description: Mapped[str | None] = mapped_column(String(512), default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    status: Mapped[str] = mapped_column(String(16), index=True, default="active")  # active|deprecated
    # No additional table args


class FeatureView(Base):
    """Materialized snapshot of feature values for an entity & version (lightweight store)."""
    __tablename__ = "feature_views"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    feature_key: Mapped[str] = mapped_column(String(256), index=True)
    entity_id: Mapped[str] = mapped_column(String(128), index=True)
    value: Mapped[dict] = mapped_column(JSON)  # could hold typed value + metadata
    feature_version: Mapped[str] = mapped_column(String(32), index=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ux_feature_view_key_entity_version", "feature_key", "entity_id", "feature_version", unique=True),
    )


class UserChurnRisk(Base):
    __tablename__ = "user_churn_risk"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    risk_score: Mapped[float] = mapped_column(Float)
    model_version: Mapped[str] = mapped_column(String(32), index=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_churn_user_project", "user_id", "project"),
    )


class CohortRetention(Base):
    __tablename__ = "cohort_retention"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cohort_date: Mapped[datetime] = mapped_column(DateTime, index=True)
    day_number: Mapped[int] = mapped_column(Integer, index=True)
    retained_users: Mapped[int] = mapped_column(Integer)
    total_users: Mapped[int] = mapped_column(Integer)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_retention_cohort_day_project", "cohort_date", "day_number", "project", unique=True),
    )


class ABTestMetric(Base):
    __tablename__ = "ab_test_metrics"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    experiment_name: Mapped[str] = mapped_column(String(128), index=True)
    variant: Mapped[str] = mapped_column(String(64), index=True)
    metric_name: Mapped[str] = mapped_column(String(64), index=True)
    metric_value: Mapped[float] = mapped_column(Float)
    users: Mapped[int] = mapped_column(Integer)
    p_value: Mapped[float | None] = mapped_column(Float, default=None)
    computed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    lift: Mapped[float | None] = mapped_column(Float, default=None)
    prob_beats_baseline: Mapped[float | None] = mapped_column(Float, default=None)
    hpdi_low: Mapped[float | None] = mapped_column(Float, default=None)
    hpdi_high: Mapped[float | None] = mapped_column(Float, default=None)
    __table_args__ = (
        Index("ix_abtest_exp_variant_metric", "experiment_name", "variant", "metric_name", "project", unique=True),
    )


class ReportLog(Base):
    __tablename__ = "report_logs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    report_type: Mapped[str] = mapped_column(String(64), index=True)
    generated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    content: Mapped[str] = mapped_column(String(8192))  # store text version
    insights_count: Mapped[int] = mapped_column(Integer, default=0)
    funnel_steps: Mapped[int] = mapped_column(Integer, default=0)
    published_slack: Mapped[int] = mapped_column(Integer, default=0, index=True)
    published_notion: Mapped[int] = mapped_column(Integer, default=0, index=True)
    new_insights: Mapped[int] = mapped_column(Integer, default=0)
    progressed_insights: Mapped[int] = mapped_column(Integer, default=0)
    shipped_insights: Mapped[int] = mapped_column(Integer, default=0)
    reopened_insights: Mapped[int] = mapped_column(Integer, default=0)
    # No additional table args


class DatasetSnapshot(Base):
    """Captured evaluation/training dataset snapshot (fingerprint + counts).

    Note: field renamed from 'metadata' to 'meta' to avoid SQLAlchemy reserved attribute conflict.
    """
    __tablename__ = "dataset_snapshots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    event_count: Mapped[int] = mapped_column(Integer)
    user_count: Mapped[int] = mapped_column(Integer)
    fingerprint: Mapped[str] = mapped_column(String(64), index=True)
    meta: Mapped[dict | None] = mapped_column(JSON, default=None)
    # No additional table args


class ProjectQuota(Base):
    __tablename__ = "project_quotas"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    daily_event_limit: Mapped[int | None] = mapped_column(Integer, default=None)
    monthly_event_limit: Mapped[int | None] = mapped_column(Integer, default=None)
    enforced: Mapped[int] = mapped_column(Integer, default=1, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_quota_project_enforced", "project", "enforced"),
    )


class ModelEvaluation(Base):
    __tablename__ = "model_evaluations"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_name: Mapped[str] = mapped_column(String(64), index=True)
    candidate_version: Mapped[str] = mapped_column(String(32), index=True)
    baseline_version: Mapped[str | None] = mapped_column(String(32), index=True, default=None)
    metric_name: Mapped[str] = mapped_column(String(64), index=True)
    candidate_score: Mapped[float] = mapped_column(Float)
    baseline_score: Mapped[float | None] = mapped_column(Float, default=None)
    delta: Mapped[float | None] = mapped_column(Float, default=None, index=True)
    threshold: Mapped[float | None] = mapped_column(Float, default=None)
    decision: Mapped[str | None] = mapped_column(String(16), default=None, index=True)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    details: Mapped[dict | None] = mapped_column(JSON, default=None)
    __table_args__ = (
        Index("ix_model_eval_key", "model_name", "candidate_version", "metric_name", unique=True),
    )


class ModelPromotionAudit(Base):
    """Audit trail of manual model promotions & rollbacks."""
    __tablename__ = "model_promotion_audit"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_name: Mapped[str] = mapped_column(String(64), index=True)
    from_version: Mapped[str | None] = mapped_column(String(32), index=True)
    to_version: Mapped[str | None] = mapped_column(String(32), index=True)
    action: Mapped[str] = mapped_column(String(16), index=True)  # promote|rollback
    reason: Mapped[str | None] = mapped_column(String(512), default=None)
    author: Mapped[str | None] = mapped_column(String(64), default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    # No additional table args


class DataQualityThreshold(Base):
    """Configured thresholds for data quality metrics to drive alerting."""
    __tablename__ = "data_quality_thresholds"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    metric_type: Mapped[str] = mapped_column(String(64), index=True)
    event_name: Mapped[str | None] = mapped_column(String(128), index=True, default=None)
    lower_bound: Mapped[float | None] = mapped_column(Float, default=None)
    upper_bound: Mapped[float | None] = mapped_column(Float, default=None)
    direction: Mapped[str | None] = mapped_column(String(16), default=None)  # increase|decrease|both
    severity: Mapped[str] = mapped_column(String(16), default="warn")  # warn|critical
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    __table_args__ = (
        Index("ix_dq_threshold_key", "metric_type", "event_name", "project", unique=True),
    )


class DataQualityAlert(Base):
    """Alert instances produced when thresholds are violated."""
    __tablename__ = "data_quality_alerts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    metric_type: Mapped[str] = mapped_column(String(64), index=True)
    event_name: Mapped[str | None] = mapped_column(String(128), index=True, default=None)
    metric_value: Mapped[float] = mapped_column(Float)
    severity: Mapped[str] = mapped_column(String(16), index=True)
    status: Mapped[str] = mapped_column(String(16), default="open", index=True)  # open|ack|closed
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    details: Mapped[dict | None] = mapped_column(JSON, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    __table_args__ = (
        Index("ix_dq_alert_key", "metric_type", "event_name", "project", "status"),
    )


class EventSchemaVersion(Base):
    """Versioned history of event schemas for diff & evolution policy."""
    __tablename__ = "event_schema_versions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_name: Mapped[str] = mapped_column(String(128), index=True)
    version: Mapped[int] = mapped_column(Integer, index=True)
    spec: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(16), default="draft", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    author: Mapped[str | None] = mapped_column(String(64), default=None)
    __table_args__ = (
        Index("ix_schema_version_key", "event_name", "version", unique=True),
    )


class ObservedProperty(Base):
    """Observed property types with confidence & conflicts to assist governance."""
    __tablename__ = "observed_properties"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_name: Mapped[str] = mapped_column(String(128), index=True)
    prop_name: Mapped[str] = mapped_column(String(128), index=True)
    inferred_type: Mapped[str] = mapped_column(String(32), index=True)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    samples: Mapped[int] = mapped_column(Integer, default=0)
    conflicts: Mapped[int] = mapped_column(Integer, default=0)
    last_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    __table_args__ = (
        Index("ix_observed_prop_key", "event_name", "prop_name", "project", unique=True),
    )


class ObservedPIIProperty(Base):
    """Dynamically detected potential PII properties.

    Stores only hashed sample value to avoid retaining raw sensitive data.
    """
    __tablename__ = "observed_pii_properties"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_name: Mapped[str | None] = mapped_column(String(128), index=True, default=None)
    prop_name: Mapped[str] = mapped_column(String(128), index=True)
    detection_type: Mapped[str] = mapped_column(String(32), index=True)
    sample_hash: Mapped[str | None] = mapped_column(String(64), default=None)
    first_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    last_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    occurrences: Mapped[int] = mapped_column(Integer, default=1)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    __table_args__ = (
        Index("ux_pii_prop_key", "prop_name", "project", unique=True),
    )


class RetentionPolicy(Base):
    """Data retention & PII masking policy rules."""
    __tablename__ = "retention_policies"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    table_name: Mapped[str] = mapped_column(String(128), index=True)
    max_age_days: Mapped[int] = mapped_column(Integer, default=365)
    pii_fields: Mapped[str | None] = mapped_column(String(256), default=None)  # comma list
    hashing_salt: Mapped[str | None] = mapped_column(String(64), default=None)
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_retention_table_active", "table_name", "active"),
    )


class OpsMetric(Base):
    __tablename__ = "ops_metrics"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    metric_name: Mapped[str] = mapped_column(String(128), index=True)
    metric_value: Mapped[float] = mapped_column(Float)
    captured_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    details: Mapped[dict | None] = mapped_column(JSON, default=None)
    __table_args__ = (
        Index("ix_ops_metric_name_time", "metric_name", "captured_at"),
    )


class ModelTrainingRun(Base):
    """Record of an individual model training/evaluation run for reproducibility & lineage."""
    __tablename__ = "model_training_runs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_name: Mapped[str] = mapped_column(String(64), index=True)
    version: Mapped[str] = mapped_column(String(32), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    status: Mapped[str] = mapped_column(String(16), default="completed", index=True)  # completed|failed|running
    params: Mapped[dict | None] = mapped_column(JSON, default=None)
    metrics: Mapped[dict | None] = mapped_column(JSON, default=None)
    data_fingerprint: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    training_rows: Mapped[int | None] = mapped_column(Integer, default=None)
    notes: Mapped[str | None] = mapped_column(String(512), default=None)
    __table_args__ = (
        Index("ix_training_model_version", "model_name", "version"),
    )


class ModelDriftMetric(Base):
    __tablename__ = "model_drift_metrics"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_name: Mapped[str] = mapped_column(String(64), index=True)
    metric_name: Mapped[str] = mapped_column(String(64), index=True)  # e.g., psi
    metric_value: Mapped[float] = mapped_column(Float)
    captured_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    window: Mapped[str | None] = mapped_column(String(32), default=None)
    details: Mapped[dict | None] = mapped_column(JSON, default=None)
    __table_args__ = (
        Index("ix_drift_model_metric_time", "model_name", "metric_name", "captured_at"),
    )

class ModelDriftThreshold(Base):
    """Configured drift thresholds to trigger automated retraining or alerts.

    comparison: gt|lt|ge|le (applied to metric_value vs boundary)
    action: retrain|alert
    cooldown_hours: minimum hours between repeated actions for same (model,metric).
    """
    __tablename__ = "model_drift_thresholds"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_name: Mapped[str] = mapped_column(String(64), index=True)
    metric_name: Mapped[str] = mapped_column(String(64), index=True)
    comparison: Mapped[str] = mapped_column(String(4), default="gt", index=True)
    boundary: Mapped[float] = mapped_column(Float)
    action: Mapped[str] = mapped_column(String(16), default="retrain", index=True)
    cooldown_hours: Mapped[int] = mapped_column(Integer, default=24)
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    last_triggered_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    __table_args__ = (
        Index("ux_drift_threshold_key", "model_name", "metric_name", "comparison", "boundary", unique=True),
    )

class DriftRetrainAudit(Base):
    """Audit log of actions taken due to drift thresholds (retrain/alert)."""
    __tablename__ = "drift_retrain_audit"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_name: Mapped[str] = mapped_column(String(64), index=True)
    metric_name: Mapped[str] = mapped_column(String(64), index=True)
    metric_value: Mapped[float] = mapped_column(Float)
    action: Mapped[str] = mapped_column(String(16), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    notes: Mapped[str | None] = mapped_column(String(256), default=None)
    # No additional table args


class SecretAccessAudit(Base):
    """Audit of secret (credential) decryption events.

    Records each successful decrypt to provide traceability without storing plaintext.
    """
    __tablename__ = "secret_access_audit"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_name: Mapped[str] = mapped_column(String(64), index=True)
    secret_key: Mapped[str] = mapped_column(String(128), index=True)
    accessed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_secret_access_key", "connector_name", "secret_key", "accessed_at"),
    )


class MaintenanceJob(Base):
    """Generic tracking for operational maintenance tasks (backup, restore, compaction).

    Enables idempotent, resumable operations and external visibility.
    """
    __tablename__ = "maintenance_jobs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_type: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(16), index=True, default="running")  # running|completed|failed
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    params: Mapped[dict | None] = mapped_column(JSON, default=None)
    result: Mapped[dict | None] = mapped_column(JSON, default=None)
    error: Mapped[str | None] = mapped_column(String(512), default=None)
    __table_args__ = (
        Index("ix_maintenance_type_status", "job_type", "status"),
    )

class SecretRotationAudit(Base):
    """Audit of automatic secret rotation operations for connectors."""
    __tablename__ = "secret_rotation_audit"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connector_name: Mapped[str] = mapped_column(String(64), index=True)
    secret_key: Mapped[str] = mapped_column(String(128), index=True)
    old_version: Mapped[int | None] = mapped_column(Integer, default=None)
    new_version: Mapped[int | None] = mapped_column(Integer, default=None)
    rotated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    status: Mapped[str] = mapped_column(String(16), default="success", index=True)  # success|failed|skipped
    reason: Mapped[str | None] = mapped_column(String(256), default=None)
    __table_args__ = (
        Index("ix_secret_rotation_key", "connector_name", "secret_key", "rotated_at"),
    )


class PrivacyDeletionAudit(Base):
    """Audit log of user data deletion requests for compliance."""
    __tablename__ = "privacy_deletion_audit"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    deleted_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    raw_events: Mapped[int] = mapped_column(Integer, default=0)
    session_summaries: Mapped[int] = mapped_column(Integer, default=0)
    features: Mapped[int] = mapped_column(Integer, default=0)
    recommendations: Mapped[int] = mapped_column(Integer, default=0)
    insights: Mapped[int] = mapped_column(Integer, default=0)
    notes: Mapped[str | None] = mapped_column(String(256), default=None)
    __table_args__ = (
        Index("ix_privacy_deletion_user_project", "user_id", "project"),
    )

class ExperimentDefinition(Base):
    """Defines an experiment with deterministic hashing assignment.

    assignment_prop: property name written into event props (e.g., experiment_homepage_variant)
    variants: JSON list of variant keys
    traffic_allocation: JSON mapping variant->percentage (sum ~100) or None for equal.
    hash_salt: salt for stable hashing.
    """
    __tablename__ = "experiment_definitions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    assignment_prop: Mapped[str] = mapped_column(String(128), index=True)
    variants: Mapped[dict] = mapped_column(JSON)  # store list under {'variants': [...]} for extensibility
    traffic_allocation: Mapped[dict | None] = mapped_column(JSON, default=None)
    hash_salt: Mapped[str] = mapped_column(String(32), default="exp")
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    adaptive: Mapped[int] = mapped_column(Integer, default=0, index=True)  # 1 enables adaptive (bandit) allocation
    conversion_event: Mapped[str | None] = mapped_column(String(128), default=None, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_experiment_active", "active"),
    )

class ExperimentAssignment(Base):
    """Persistent record of user -> experiment variant assignment for consistency."""
    __tablename__ = "experiment_assignments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    experiment_key: Mapped[str] = mapped_column(String(128), index=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    variant: Mapped[str] = mapped_column(String(64), index=True)
    assigned_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    __table_args__ = (
        Index("ux_experiment_user", "experiment_key", "user_id", unique=True),
    )

class ExperimentDecisionSnapshot(Base):
    """Time-series snapshot of sequential experiment decision statistics.

    One row per (experiment_prop, variant, computed_at). The overall decision row is stored with
    variant set to '__summary__'. This enables reconstructing trend lines for probabilities and
    decisions without recalculating from historical raw events.
    """
    __tablename__ = "experiment_decision_snapshots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    experiment_prop: Mapped[str] = mapped_column(String(128), index=True)
    experiment_key: Mapped[str | None] = mapped_column(String(128), index=True, default=None)
    conversion_event: Mapped[str] = mapped_column(String(128), index=True)
    decision: Mapped[str] = mapped_column(String(32), index=True)
    baseline: Mapped[str] = mapped_column(String(64))
    winner: Mapped[str | None] = mapped_column(String(64), default=None)
    variant: Mapped[str] = mapped_column(String(64), index=True)
    prob_beats_baseline: Mapped[float | None] = mapped_column(Float, default=None)
    prob_practical_lift: Mapped[float | None] = mapped_column(Float, default=None)
    mean_rate: Mapped[float | None] = mapped_column(Float, default=None)
    users: Mapped[int] = mapped_column(Integer, default=0)
    conversions: Mapped[int] = mapped_column(Integer, default=0)
    computed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_exp_decision_prop_time", "experiment_prop", "computed_at"),
        Index("ix_exp_decision_variant", "experiment_prop", "variant", "computed_at"),
    )


class ObservedEventProperty(Base):
    """Tracks observed properties per event for schema diff detection."""
    __tablename__ = "observed_event_properties"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_name: Mapped[str] = mapped_column(String(128), index=True)
    prop_name: Mapped[str] = mapped_column(String(128), index=True)
    first_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    last_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    count: Mapped[int] = mapped_column(Integer, default=0)
    __table_args__ = (
        Index("ux_observed_event_prop", "event_name", "prop_name", unique=True),
    )


class SchemaDriftAlert(Base):
    """Represents a detected schema drift event (new property observed or required property missing)."""
    __tablename__ = "schema_drift_alerts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_name: Mapped[str] = mapped_column(String(128), index=True)
    prop_name: Mapped[str | None] = mapped_column(String(128), index=True, default=None)
    change_type: Mapped[str] = mapped_column(String(32), index=True)  # new_prop|missing_required
    occurrences: Mapped[int] = mapped_column(Integer, default=1)
    first_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    last_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    status: Mapped[str] = mapped_column(String(16), default="open", index=True)  # open|ignored|resolved
    details: Mapped[dict | None] = mapped_column(JSON, default=None)
    __table_args__ = (
        Index("ix_schema_drift_key", "event_name", "prop_name", "change_type", unique=True),
    )


class AlertRule(Base):
    """Configurable alert rules for experiment decisions, drift, data quality.

    rule_type: experiment_decision|drift|data_quality
    condition: JSON payload whose schema depends on rule_type.
      experiment_decision: {"experiment_prop": str, "decision_in": ["stop_winner", ...]}
    cooldown_minutes: minimum minutes before firing same rule again.
    channels: JSON like {"slack": true, "email": true}
    """
    __tablename__ = "alert_rules"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    rule_type: Mapped[str] = mapped_column(String(32), index=True)
    condition: Mapped[dict] = mapped_column(JSON)
    channels: Mapped[dict] = mapped_column(JSON, default=dict)
    cooldown_minutes: Mapped[int] = mapped_column(Integer, default=60)
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    last_fired_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_alert_rule_type_active", "rule_type", "active"),
    )


class AlertLog(Base):
    """History of fired alerts (audit + duplicate suppression)."""
    __tablename__ = "alert_logs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    rule_id: Mapped[int] = mapped_column(Integer, index=True)
    rule_type: Mapped[str] = mapped_column(String(32), index=True)
    message: Mapped[str] = mapped_column(String(512))
    context: Mapped[dict | None] = mapped_column(JSON, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_alert_log_rule", "rule_id", "created_at"),
    )

class NotificationTemplate(Base):
    """Reusable alert delivery template with simple {placeholders}."""
    __tablename__ = "notification_templates"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    channel: Mapped[str] = mapped_column(String(16), index=True)  # slack|email
    subject: Mapped[str | None] = mapped_column(String(128), default=None)
    body: Mapped[str] = mapped_column(String(1024))
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_template_channel_active", "channel", "active"),
    )

class ExperimentCompositeConfig(Base):
    """Defines weighted composite scoring & guardrails for an experiment property.

    weights: {metric_name: weight}
    guardrails: list of {"metric": str, "min_lift": float | None, "max_negative_lift": float | None}
    """
    __tablename__ = "experiment_composite_configs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    experiment_prop: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    weights: Mapped[dict] = mapped_column(JSON)
    guardrails: Mapped[dict | None] = mapped_column(JSON, default=None)
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

class ExperimentCompositeScore(Base):
    """Persisted composite score per variant per compute run."""
    __tablename__ = "experiment_composite_scores"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    experiment_prop: Mapped[str] = mapped_column(String(128), index=True)
    variant: Mapped[str] = mapped_column(String(64), index=True)
    score: Mapped[float | None] = mapped_column(Float, default=None)
    disqualified: Mapped[int] = mapped_column(Integer, default=0, index=True)
    details: Mapped[dict] = mapped_column(JSON)
    computed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_exp_comp_score_variant", "experiment_prop", "variant", "computed_at"),
    )

class ExperimentPromotionAudit(Base):
    """Audit log of automated or manual experiment variant promotions.

    Records decision snapshot at time of promotion and rationale.
    """
    __tablename__ = "experiment_promotion_audit"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    experiment_prop: Mapped[str] = mapped_column(String(128), index=True)
    experiment_key: Mapped[str | None] = mapped_column(String(128), index=True, default=None)
    winner_variant: Mapped[str] = mapped_column(String(64), index=True)
    decision: Mapped[str] = mapped_column(String(32), index=True)
    rationale: Mapped[str | None] = mapped_column(String(256), default=None)
    details: Mapped[dict | None] = mapped_column(JSON, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_exp_promo_prop_time", "experiment_prop", "created_at"),
    )

class RecommendationImpact(Base):
    """Aggregated impact metrics for a recommendation over a time window.

    Provides uplift measurement by comparing conversion rate for exposed users (who received
    the recommendation and accepted/converted) vs baseline users who performed the target action
    without receiving the recommendation in the same window.
    """
    __tablename__ = "recommendation_impact"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recommendation: Mapped[str] = mapped_column(String(64), index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    window_start: Mapped[datetime] = mapped_column(DateTime, index=True)
    window_end: Mapped[datetime] = mapped_column(DateTime, index=True)
    users_exposed: Mapped[int] = mapped_column(Integer)
    users_converted: Mapped[int] = mapped_column(Integer)
    baseline_users: Mapped[int] = mapped_column(Integer)
    baseline_converted: Mapped[int] = mapped_column(Integer)
    conversion_rate: Mapped[float] = mapped_column(Float)
    baseline_rate: Mapped[float] = mapped_column(Float)
    lift: Mapped[float] = mapped_column(Float)
    computed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_rec_impact_window", "recommendation", "window_start", "window_end", unique=True),
    )

class RecommendationLearningMetric(Base):
    """Aggregated feedback + outcome metrics per recommendation window to drive adaptive weighting."""
    __tablename__ = "recommendation_learning_metrics"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recommendation: Mapped[str] = mapped_column(String(64), index=True)
    window_start: Mapped[datetime] = mapped_column(DateTime, index=True)
    window_end: Mapped[datetime] = mapped_column(DateTime, index=True)
    impressions: Mapped[int] = mapped_column(Integer, default=0)
    accepts: Mapped[int] = mapped_column(Integer, default=0)
    dismisses: Mapped[int] = mapped_column(Integer, default=0)
    converts: Mapped[int] = mapped_column(Integer, default=0)
    ctr: Mapped[float | None] = mapped_column(Float, default=None)
    conversion_rate: Mapped[float | None] = mapped_column(Float, default=None)
    weight_adjustment: Mapped[float | None] = mapped_column(Float, default=None)
    computed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ux_rec_learning_window", "recommendation", "window_start", "window_end", unique=True),
    )


class SLODefinition(Base):
    """Service Level Objective definitions for core reliability metrics."""
    __tablename__ = "slo_definitions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    metric: Mapped[str] = mapped_column(String(64), index=True)  # ingest_max_lag|ingest_success_rate|dq_alert_rate
    target: Mapped[float] = mapped_column(Float)  # e.g., 99.0 for 99%
    window_hours: Mapped[int] = mapped_column(Integer, default=24)
    threshold: Mapped[float | None] = mapped_column(Float, default=None)  # metric-specific threshold
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    description: Mapped[str | None] = mapped_column(String(256), default=None)
    __table_args__ = (
        Index("ix_slo_metric_active", "metric", "active"),
    )

class KafkaConsumerLag(Base):
    """Snapshot of Kafka consumer group lag per topic/partition for auditing and alerting."""
    __tablename__ = "kafka_consumer_lag"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    group_id: Mapped[str] = mapped_column(String(128), index=True)
    topic: Mapped[str] = mapped_column(String(256), index=True)
    partition: Mapped[int] = mapped_column(Integer, index=True)
    end_offset: Mapped[int] = mapped_column(Integer)
    committed_offset: Mapped[int | None] = mapped_column(Integer, default=None)
    lag: Mapped[int | None] = mapped_column(Integer, default=None)
    captured_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    details: Mapped[dict | None] = mapped_column(JSON, default=None)
    __table_args__ = (
        Index("ix_kafka_lag_group_topic_part_time", "group_id", "topic", "partition", "captured_at"),
    )


class IdempotencyRecord(Base):
    """Stores idempotency keys and results for operations beyond raw events."""
    __tablename__ = "idempotency_records"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    operation_type: Mapped[str] = mapped_column(String(64), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), index=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    result_data: Mapped[str | None] = mapped_column(String(8192), default=None)
    status: Mapped[str] = mapped_column(String(16), default='completed', index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime, index=True)
    __table_args__ = (
        Index("ux_idempotency_key", "operation_type", "idempotency_key", unique=True),
    )


class ConnectorState(Base):
    """Tracks connector ingestion state with cursor positions and failure auditing."""
    __tablename__ = "connector_state"
    connector_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    cursor: Mapped[str | None] = mapped_column(Text, default=None)  # Last processed cursor/token
    last_since_ts: Mapped[datetime | None] = mapped_column(DateTime, default=None)
    failure_count: Mapped[int] = mapped_column(Integer, default=0, index=True)
    last_error: Mapped[str | None] = mapped_column(String(255), default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_connector_state_failure_count", "failure_count"),
        Index("ix_connector_state_updated_at", "updated_at"),
    )


class SLOEvaluation(Base):
    """Evaluated SLO attainment records per window."""
    __tablename__ = "slo_evaluations"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slo_id: Mapped[int] = mapped_column(Integer, index=True)
    window_start: Mapped[datetime] = mapped_column(DateTime, index=True)
    window_end: Mapped[datetime] = mapped_column(DateTime, index=True)
    attained: Mapped[float] = mapped_column(Float)  # percentage achieved
    target: Mapped[float] = mapped_column(Float)
    breach: Mapped[int] = mapped_column(Integer, default=0, index=True)
    details: Mapped[dict | None] = mapped_column(JSON, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_slo_eval_slo_window", "slo_id", "window_start", "window_end", unique=True),
    )

# --- Enterprise Expansion Models (anomaly persistence, consent, cost attribution, rec feedback) ---

class AnomalyEvent(Base):
    """Persisted anomaly detections for audit, triage, and alert routing.

    type: volume|latency|funnel_dropoff|custom
    status: open|ack|closed
    """
    __tablename__ = "anomaly_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    anomaly_type: Mapped[str] = mapped_column(String(32), index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    fingerprint: Mapped[str | None] = mapped_column(String(128), index=True, default=None)
    severity: Mapped[float | None] = mapped_column(Float, default=None, index=True)
    delta_pct: Mapped[float | None] = mapped_column(Float, default=None)
    details: Mapped[dict | None] = mapped_column(JSON, default=None)
    status: Mapped[str] = mapped_column(String(16), default="open", index=True)
    detected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime, default=None, index=True)
    __table_args__ = (
        Index("ix_anomaly_type_project", "anomaly_type", "project", "status"),
        Index("ux_anomaly_fingerprint", "fingerprint", unique=False),
    )

class ConsentRecord(Base):
    """User consent state for a given policy version (opt-in/out tracking)."""
    __tablename__ = "consent_records"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    policy_version: Mapped[str] = mapped_column(String(32), index=True)
    granted: Mapped[int] = mapped_column(Integer, default=1, index=True)  # 1 granted, 0 revoked
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    source: Mapped[str | None] = mapped_column(String(64), default=None)
    __table_args__ = (
        Index("ux_consent_user_policy", "user_id", "policy_version", "project", unique=True),
    )

class CostUsage(Base):
    """Periodically aggregated cost & usage metrics for chargeback/attribution."""
    __tablename__ = "cost_usage"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    window_start: Mapped[datetime] = mapped_column(DateTime, index=True)
    window_end: Mapped[datetime] = mapped_column(DateTime, index=True)
    event_count: Mapped[int] = mapped_column(Integer, default=0)
    storage_bytes: Mapped[int] = mapped_column(Integer, default=0)
    compute_seconds: Mapped[float] = mapped_column(Float, default=0.0)
    cost_estimate: Mapped[float | None] = mapped_column(Float, default=None)
    breakdown: Mapped[dict | None] = mapped_column(JSON, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_cost_usage_window", "project", "window_start", "window_end", unique=True),
    )

class RecommendationFeedback(Base):
    """Explicit user feedback on delivered recommendations (accept / dismiss / convert)."""
    __tablename__ = "recommendation_feedback"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recommendation: Mapped[str] = mapped_column(String(64), index=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    feedback: Mapped[str] = mapped_column(String(16), index=True)  # accept|dismiss|convert
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    metadata: Mapped[dict | None] = mapped_column(JSON, default=None)
    __table_args__ = (
        Index("ix_rec_feedback_user", "user_id", "recommendation", "created_at"),
    )

class SegmentDefinition(Base):
    """User segment definition via simple property-based predicate expression.

    expression: JSON describing AND/OR tree of property comparisons, e.g.
      {"and": [ {"prop": "country", "op": "eq", "value": "US"}, {"prop": "plan", "op": "in", "value": ["pro","enterprise"]} ]}
    """
    __tablename__ = "segment_definitions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    expression: Mapped[dict] = mapped_column(JSON)
    description: Mapped[str | None] = mapped_column(String(256), default=None)
    active: Mapped[int] = mapped_column(Integer, default=1, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_segment_active", "active"),
    )

class UserSegmentMembership(Base):
    """Materialized segment membership for faster querying."""
    __tablename__ = "user_segment_memberships"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    segment_key: Mapped[str] = mapped_column(String(128), index=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    project: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    computed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ux_segment_user", "segment_key", "user_id", unique=True),
    )


class BatchProcessingConfig(Base):
    """Configuration for high-volume batch processing pipelines."""
    __tablename__ = "batch_processing_config"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    pipeline_name: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    max_batch_size: Mapped[int] = mapped_column(Integer, default=1000)
    max_wait_seconds: Mapped[int] = mapped_column(Integer, default=5)
    parallel_workers: Mapped[int] = mapped_column(Integer, default=4)
    compression_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    adaptive_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, index=True)


class BatchProcessingMetrics(Base):
    """Historical metrics for batch processing performance."""
    __tablename__ = "batch_processing_metrics"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    pipeline_name: Mapped[str] = mapped_column(String(100), index=True)
    batch_size: Mapped[int] = mapped_column(Integer)
    processing_time_ms: Mapped[int] = mapped_column(Integer)
    throughput_eps: Mapped[float] = mapped_column(Float)
    compression_ratio: Mapped[float | None] = mapped_column(Float, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    __table_args__ = (
        Index("ix_batch_metrics_pipeline_time", "pipeline_name", "created_at"),
    )


class DLQErrorPattern(Base):
    """DLQ error pattern tracking for observability and recovery."""
    __tablename__ = "dlq_error_patterns"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    error_signature: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    error_category: Mapped[str] = mapped_column(String(50), index=True)
    event_pattern: Mapped[str | None] = mapped_column(String(255), default=None)
    occurrence_count: Mapped[int] = mapped_column(Integer, default=1)
    recovery_strategy: Mapped[str] = mapped_column(String(50))
    auto_recoverable: Mapped[bool] = mapped_column(Boolean, default=False)
    first_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    last_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class DLQRecoveryAttempt(Base):
    """DLQ recovery attempt tracking."""
    __tablename__ = "dlq_recovery_attempts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dlq_event_id: Mapped[int] = mapped_column(Integer, ForeignKey("dead_letter_queue.id"), index=True)
    recovery_strategy: Mapped[str] = mapped_column(String(50))
    attempt_number: Mapped[int] = mapped_column(Integer)
    success: Mapped[bool] = mapped_column(Boolean)
    error_message: Mapped[str | None] = mapped_column(String(1000), default=None)
    attempted_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
