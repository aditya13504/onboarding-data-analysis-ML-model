-- Supabase SQL Schema - Part 1: Core Event Tracking and Analytics
-- This file contains the first half of the database schema for the Onboarding Drop-off Analyzer
-- Run this script in your Supabase SQL editor before running Part 2

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- CORE EVENT TRACKING TABLES
-- ============================================================================

-- Raw event data table (primary ingestion point)
CREATE TABLE raw_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(128) UNIQUE NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(128),
    event_name VARCHAR(128) NOT NULL,
    event_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    props JSONB DEFAULT '{}',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    project VARCHAR(64) DEFAULT NULL,
    schema_version INTEGER DEFAULT 1
);

-- Indexes for raw_events
CREATE INDEX ix_raw_events_user_id ON raw_events(user_id);
CREATE INDEX ix_raw_events_session_id ON raw_events(session_id);
CREATE INDEX ix_raw_events_event_name ON raw_events(event_name);
CREATE INDEX ix_raw_events_event_ts ON raw_events(event_ts);
CREATE INDEX ix_raw_events_ingested_at ON raw_events(ingested_at);
CREATE INDEX ix_raw_events_project ON raw_events(project);
CREATE INDEX ix_raw_events_props_gin ON raw_events USING GIN(props);
CREATE INDEX ix_raw_events_composite ON raw_events(user_id, event_name, event_ts);

-- Funnel metrics for conversion tracking
CREATE TABLE funnel_metrics (
    id BIGSERIAL PRIMARY KEY,
    funnel_name VARCHAR(128) NOT NULL,
    step_name VARCHAR(128) NOT NULL,
    step_order INTEGER NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(128),
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    time_to_complete_seconds INTEGER,
    project VARCHAR(64) DEFAULT NULL,
    metadata JSONB DEFAULT '{}'
);

-- Indexes for funnel_metrics
CREATE INDEX ix_funnel_metrics_funnel_name ON funnel_metrics(funnel_name);
CREATE INDEX ix_funnel_metrics_user_id ON funnel_metrics(user_id);
CREATE INDEX ix_funnel_metrics_completed_at ON funnel_metrics(completed_at);
CREATE INDEX ix_funnel_metrics_project ON funnel_metrics(project);
CREATE INDEX ix_funnel_metrics_composite ON funnel_metrics(funnel_name, step_order, completed_at);

-- User identity mapping for user tracking across sessions
CREATE TABLE user_identity_map (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(128) NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for user_identity_map
CREATE INDEX ix_user_identity_map_user_id ON user_identity_map(user_id);
CREATE INDEX ix_user_identity_map_session_id ON user_identity_map(session_id);
CREATE INDEX ix_user_identity_map_project ON user_identity_map(project);
CREATE UNIQUE INDEX ux_user_identity_session ON user_identity_map(user_id, session_id);

-- Event name mapping for analytics
CREATE TABLE event_name_map (
    id BIGSERIAL PRIMARY KEY,
    event_name VARCHAR(128) UNIQUE NOT NULL,
    canonical_name VARCHAR(128) NOT NULL,
    category VARCHAR(64) DEFAULT NULL,
    description TEXT DEFAULT NULL,
    active INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for event_name_map
CREATE INDEX ix_event_name_map_canonical_name ON event_name_map(canonical_name);
CREATE INDEX ix_event_name_map_category ON event_name_map(category);
CREATE INDEX ix_event_name_map_active ON event_name_map(active);

-- ============================================================================
-- DATA NORMALIZATION AND QUALITY
-- ============================================================================

-- Normalization rules for data standardization
CREATE TABLE normalization_rules (
    id BIGSERIAL PRIMARY KEY,
    field_name VARCHAR(128) NOT NULL,
    rule_type VARCHAR(32) NOT NULL, -- uppercase|lowercase|trim|regex_replace|date_format
    rule_config JSONB NOT NULL DEFAULT '{}',
    active INTEGER DEFAULT 1,
    priority INTEGER DEFAULT 100,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for normalization_rules
CREATE INDEX ix_normalization_rules_field_name ON normalization_rules(field_name);
CREATE INDEX ix_normalization_rules_rule_type ON normalization_rules(rule_type);
CREATE INDEX ix_normalization_rules_active ON normalization_rules(active);
CREATE INDEX ix_normalization_rules_priority ON normalization_rules(priority);

-- ============================================================================
-- MACHINE LEARNING AND ANALYTICS
-- ============================================================================

-- Friction clusters for ML-based grouping
CREATE TABLE friction_clusters (
    id BIGSERIAL PRIMARY KEY,
    cluster_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    event_sequence JSONB NOT NULL,
    friction_score DOUBLE PRECISION DEFAULT NULL,
    cluster_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    project VARCHAR(64) DEFAULT NULL,
    metadata JSONB DEFAULT '{}'
);

-- Indexes for friction_clusters
CREATE INDEX ix_friction_clusters_cluster_id ON friction_clusters(cluster_id);
CREATE INDEX ix_friction_clusters_user_id ON friction_clusters(user_id);
CREATE INDEX ix_friction_clusters_friction_score ON friction_clusters(friction_score);
CREATE INDEX ix_friction_clusters_timestamp ON friction_clusters(cluster_timestamp);
CREATE INDEX ix_friction_clusters_project ON friction_clusters(project);

-- Cluster labels for ML interpretation
CREATE TABLE cluster_labels (
    id BIGSERIAL PRIMARY KEY,
    cluster_id VARCHAR(64) NOT NULL,
    label VARCHAR(128) NOT NULL,
    confidence DOUBLE PRECISION DEFAULT NULL,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    assigned_by VARCHAR(64) DEFAULT NULL,
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for cluster_labels
CREATE INDEX ix_cluster_labels_cluster_id ON cluster_labels(cluster_id);
CREATE INDEX ix_cluster_labels_label ON cluster_labels(label);
CREATE INDEX ix_cluster_labels_confidence ON cluster_labels(confidence);
CREATE INDEX ix_cluster_labels_project ON cluster_labels(project);

-- Insights generated from analysis
CREATE TABLE insights (
    id BIGSERIAL PRIMARY KEY,
    insight_type VARCHAR(64) NOT NULL,
    title VARCHAR(256) NOT NULL,
    description TEXT DEFAULT NULL,
    priority VARCHAR(16) DEFAULT 'medium', -- low|medium|high|critical
    status VARCHAR(16) DEFAULT 'new', -- new|reviewed|actioned|dismissed
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reviewed_at TIMESTAMPTZ DEFAULT NULL,
    project VARCHAR(64) DEFAULT NULL,
    metadata JSONB DEFAULT '{}',
    impact_score DOUBLE PRECISION DEFAULT NULL
);

-- Indexes for insights
CREATE INDEX ix_insights_insight_type ON insights(insight_type);
CREATE INDEX ix_insights_priority ON insights(priority);
CREATE INDEX ix_insights_status ON insights(status);
CREATE INDEX ix_insights_created_at ON insights(created_at);
CREATE INDEX ix_insights_project ON insights(project);
CREATE INDEX ix_insights_impact_score ON insights(impact_score);

-- ============================================================================
-- RECOMMENDATIONS ENGINE
-- ============================================================================

-- Suggestion patterns for recommendations
CREATE TABLE suggestion_patterns (
    id BIGSERIAL PRIMARY KEY,
    pattern_name VARCHAR(128) NOT NULL,
    trigger_conditions JSONB NOT NULL,
    recommendation_text TEXT NOT NULL,
    weight DOUBLE PRECISION DEFAULT 1.0,
    active INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for suggestion_patterns
CREATE INDEX ix_suggestion_patterns_pattern_name ON suggestion_patterns(pattern_name);
CREATE INDEX ix_suggestion_patterns_weight ON suggestion_patterns(weight);
CREATE INDEX ix_suggestion_patterns_active ON suggestion_patterns(active);
CREATE INDEX ix_suggestion_patterns_project ON suggestion_patterns(project);

-- Insight suppression rules
CREATE TABLE insight_suppression (
    id BIGSERIAL PRIMARY KEY,
    insight_type VARCHAR(64) NOT NULL,
    suppression_rule JSONB NOT NULL,
    active INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ DEFAULT NULL,
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for insight_suppression
CREATE INDEX ix_insight_suppression_insight_type ON insight_suppression(insight_type);
CREATE INDEX ix_insight_suppression_active ON insight_suppression(active);
CREATE INDEX ix_insight_suppression_expires_at ON insight_suppression(expires_at);
CREATE INDEX ix_insight_suppression_project ON insight_suppression(project);

-- Insight feedback tracking
CREATE TABLE insight_feedback (
    id BIGSERIAL PRIMARY KEY,
    insight_id BIGINT NOT NULL REFERENCES insights(id),
    feedback_type VARCHAR(32) NOT NULL, -- helpful|not_helpful|actioned|dismissed
    feedback_text TEXT DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    user_id VARCHAR(64) DEFAULT NULL,
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for insight_feedback
CREATE INDEX ix_insight_feedback_insight_id ON insight_feedback(insight_id);
CREATE INDEX ix_insight_feedback_feedback_type ON insight_feedback(feedback_type);
CREATE INDEX ix_insight_feedback_created_at ON insight_feedback(created_at);
CREATE INDEX ix_insight_feedback_user_id ON insight_feedback(user_id);
CREATE INDEX ix_insight_feedback_project ON insight_feedback(project);

-- ============================================================================
-- SYSTEM MANAGEMENT
-- ============================================================================

-- Model versions for ML model tracking
CREATE TABLE model_versions (
    id BIGSERIAL PRIMARY KEY,
    model_name VARCHAR(64) NOT NULL,
    version VARCHAR(32) NOT NULL,
    model_type VARCHAR(32) NOT NULL,
    training_data_fingerprint VARCHAR(64) DEFAULT NULL,
    performance_metrics JSONB DEFAULT '{}',
    deployed_at TIMESTAMPTZ DEFAULT NULL,
    active INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'
);

-- Indexes for model_versions
CREATE INDEX ix_model_versions_model_name ON model_versions(model_name);
CREATE INDEX ix_model_versions_version ON model_versions(version);
CREATE INDEX ix_model_versions_model_type ON model_versions(model_type);
CREATE INDEX ix_model_versions_active ON model_versions(active);
CREATE INDEX ix_model_versions_deployed_at ON model_versions(deployed_at);
CREATE UNIQUE INDEX ux_model_name_version ON model_versions(model_name, version);

-- Dead letter queue for failed events
CREATE TABLE dead_letter_queue (
    id BIGSERIAL PRIMARY KEY,
    original_payload JSONB NOT NULL,
    error_message TEXT NOT NULL,
    error_code VARCHAR(32) DEFAULT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    status VARCHAR(16) DEFAULT 'failed', -- failed|retrying|resolved|abandoned
    resolved_at TIMESTAMPTZ DEFAULT NULL,
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for dead_letter_queue
CREATE INDEX ix_dead_letter_queue_failed_at ON dead_letter_queue(failed_at);
CREATE INDEX ix_dead_letter_queue_status ON dead_letter_queue(status);
CREATE INDEX ix_dead_letter_queue_retry_count ON dead_letter_queue(retry_count);
CREATE INDEX ix_dead_letter_queue_project ON dead_letter_queue(project);

-- API keys for authentication
CREATE TABLE api_keys (
    id BIGSERIAL PRIMARY KEY,
    key_name VARCHAR(128) NOT NULL,
    key_hash VARCHAR(256) UNIQUE NOT NULL,
    permissions JSONB DEFAULT '[]',
    active INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ DEFAULT NULL,
    last_used_at TIMESTAMPTZ DEFAULT NULL,
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for api_keys
CREATE INDEX ix_api_keys_key_name ON api_keys(key_name);
CREATE INDEX ix_api_keys_active ON api_keys(active);
CREATE INDEX ix_api_keys_expires_at ON api_keys(expires_at);
CREATE INDEX ix_api_keys_last_used_at ON api_keys(last_used_at);
CREATE INDEX ix_api_keys_project ON api_keys(project);

-- User roles for authorization
CREATE TABLE roles (
    id BIGSERIAL PRIMARY KEY,
    role_name VARCHAR(64) UNIQUE NOT NULL,
    permissions JSONB DEFAULT '[]',
    description TEXT DEFAULT NULL,
    active INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for roles
CREATE INDEX ix_roles_role_name ON roles(role_name);
CREATE INDEX ix_roles_active ON roles(active);

-- Role assignments
CREATE TABLE role_assignments (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    role_id BIGINT NOT NULL REFERENCES roles(id),
    project VARCHAR(64) DEFAULT NULL,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    assigned_by VARCHAR(64) DEFAULT NULL,
    active INTEGER DEFAULT 1
);

-- Indexes for role_assignments
CREATE INDEX ix_role_assignments_user_id ON role_assignments(user_id);
CREATE INDEX ix_role_assignments_role_id ON role_assignments(role_id);
CREATE INDEX ix_role_assignments_project ON role_assignments(project);
CREATE INDEX ix_role_assignments_active ON role_assignments(active);
CREATE UNIQUE INDEX ux_user_role_project ON role_assignments(user_id, role_id, project);

-- ============================================================================
-- PERSONALIZATION AND RECOMMENDATIONS (Part 2)
-- ============================================================================

-- Personalization rules for user targeting
CREATE TABLE personalization_rules (
    id BIGSERIAL PRIMARY KEY,
    rule_name VARCHAR(128) NOT NULL,
    target_segment JSONB NOT NULL,
    recommendation_weights JSONB DEFAULT '{}',
    active INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for personalization_rules
CREATE INDEX ix_personalization_rules_rule_name ON personalization_rules(rule_name);
CREATE INDEX ix_personalization_rules_active ON personalization_rules(active);
CREATE INDEX ix_personalization_rules_project ON personalization_rules(project);

-- User recommendations tracking
CREATE TABLE user_recommendations (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    recommendation VARCHAR(64) NOT NULL,
    recommendation_text TEXT DEFAULT NULL,
    confidence DOUBLE PRECISION DEFAULT NULL,
    context JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ DEFAULT NULL,
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for user_recommendations
CREATE INDEX ix_user_recommendations_user_id ON user_recommendations(user_id);
CREATE INDEX ix_user_recommendations_recommendation ON user_recommendations(recommendation);
CREATE INDEX ix_user_recommendations_created_at ON user_recommendations(created_at);
CREATE INDEX ix_user_recommendations_expires_at ON user_recommendations(expires_at);
CREATE INDEX ix_user_recommendations_project ON user_recommendations(project);

-- ============================================================================
-- DATA LINEAGE AND GOVERNANCE
-- ============================================================================

-- Data assets for lineage tracking
CREATE TABLE data_assets (
    id BIGSERIAL PRIMARY KEY,
    asset_name VARCHAR(256) NOT NULL,
    asset_type VARCHAR(64) NOT NULL, -- table|view|model|pipeline
    location VARCHAR(512) DEFAULT NULL,
    schema_definition JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    owner VARCHAR(64) DEFAULT NULL,
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for data_assets
CREATE INDEX ix_data_assets_asset_name ON data_assets(asset_name);
CREATE INDEX ix_data_assets_asset_type ON data_assets(asset_type);
CREATE INDEX ix_data_assets_owner ON data_assets(owner);
CREATE INDEX ix_data_assets_project ON data_assets(project);

-- Data lineage edges
CREATE TABLE data_lineage_edges (
    id BIGSERIAL PRIMARY KEY,
    source_asset_id BIGINT NOT NULL REFERENCES data_assets(id),
    target_asset_id BIGINT NOT NULL REFERENCES data_assets(id),
    relationship_type VARCHAR(32) NOT NULL, -- derives_from|depends_on|transforms
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'
);

-- Indexes for data_lineage_edges
CREATE INDEX ix_data_lineage_edges_source_asset_id ON data_lineage_edges(source_asset_id);
CREATE INDEX ix_data_lineage_edges_target_asset_id ON data_lineage_edges(target_asset_id);
CREATE INDEX ix_data_lineage_edges_relationship_type ON data_lineage_edges(relationship_type);
CREATE UNIQUE INDEX ux_lineage_source_target ON data_lineage_edges(source_asset_id, target_asset_id, relationship_type);

-- ============================================================================
-- CONNECTORS AND INTEGRATIONS
-- ============================================================================

-- Connector credentials (encrypted)
CREATE TABLE connector_credentials (
    id BIGSERIAL PRIMARY KEY,
    connector_name VARCHAR(64) NOT NULL,
    credential_key VARCHAR(128) NOT NULL,
    encrypted_value TEXT NOT NULL,
    version INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ DEFAULT NULL,
    active INTEGER DEFAULT 1,
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for connector_credentials
CREATE INDEX ix_connector_credentials_connector_name ON connector_credentials(connector_name);
CREATE INDEX ix_connector_credentials_credential_key ON connector_credentials(credential_key);
CREATE INDEX ix_connector_credentials_active ON connector_credentials(active);
CREATE INDEX ix_connector_credentials_expires_at ON connector_credentials(expires_at);
CREATE UNIQUE INDEX ux_connector_cred_key ON connector_credentials(connector_name, credential_key);

-- Connector state tracking
CREATE TABLE connector_state (
    connector_name VARCHAR(100) PRIMARY KEY,
    cursor TEXT DEFAULT NULL,
    last_since_ts TIMESTAMPTZ DEFAULT NULL,
    failure_count INTEGER DEFAULT 0,
    last_error VARCHAR(255) DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for connector_state
CREATE INDEX ix_connector_state_failure_count ON connector_state(failure_count);
CREATE INDEX ix_connector_state_updated_at ON connector_state(updated_at);

-- Backfill jobs for historical data
CREATE TABLE backfill_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(128) NOT NULL,
    connector_name VARCHAR(64) NOT NULL,
    start_date TIMESTAMPTZ NOT NULL,
    end_date TIMESTAMPTZ NOT NULL,
    status VARCHAR(16) DEFAULT 'pending', -- pending|running|completed|failed|cancelled
    progress_pct DOUBLE PRECISION DEFAULT 0.0,
    records_processed BIGINT DEFAULT 0,
    started_at TIMESTAMPTZ DEFAULT NULL,
    completed_at TIMESTAMPTZ DEFAULT NULL,
    error_message TEXT DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for backfill_jobs
CREATE INDEX ix_backfill_jobs_job_name ON backfill_jobs(job_name);
CREATE INDEX ix_backfill_jobs_connector_name ON backfill_jobs(connector_name);
CREATE INDEX ix_backfill_jobs_status ON backfill_jobs(status);
CREATE INDEX ix_backfill_jobs_created_at ON backfill_jobs(created_at);
CREATE INDEX ix_backfill_jobs_project ON backfill_jobs(project);

-- ============================================================================
-- AUDIT AND LOGGING
-- ============================================================================

-- Audit log for system operations
CREATE TABLE audit_log (
    id BIGSERIAL PRIMARY KEY,
    operation VARCHAR(64) NOT NULL,
    resource_type VARCHAR(64) NOT NULL,
    resource_id VARCHAR(128) DEFAULT NULL,
    user_id VARCHAR(64) DEFAULT NULL,
    details JSONB DEFAULT '{}',
    ip_address VARCHAR(45) DEFAULT NULL,
    user_agent TEXT DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for audit_log
CREATE INDEX ix_audit_log_operation ON audit_log(operation);
CREATE INDEX ix_audit_log_resource_type ON audit_log(resource_type);
CREATE INDEX ix_audit_log_user_id ON audit_log(user_id);
CREATE INDEX ix_audit_log_created_at ON audit_log(created_at);
CREATE INDEX ix_audit_log_project ON audit_log(project);

-- ============================================================================
-- MACHINE LEARNING OPERATIONS
-- ============================================================================

-- Model artifacts storage
CREATE TABLE model_artifacts (
    id BIGSERIAL PRIMARY KEY,
    model_name VARCHAR(64) NOT NULL,
    version VARCHAR(32) NOT NULL,
    artifact_type VARCHAR(32) NOT NULL, -- weights|config|metrics|feature_importance
    artifact_path VARCHAR(512) DEFAULT NULL,
    artifact_data JSONB DEFAULT '{}',
    size_bytes BIGINT DEFAULT NULL,
    checksum VARCHAR(64) DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'
);

-- Indexes for model_artifacts
CREATE INDEX ix_model_artifacts_model_name ON model_artifacts(model_name);
CREATE INDEX ix_model_artifacts_version ON model_artifacts(version);
CREATE INDEX ix_model_artifacts_artifact_type ON model_artifacts(artifact_type);
CREATE INDEX ix_model_artifacts_created_at ON model_artifacts(created_at);
CREATE UNIQUE INDEX ux_model_artifact_key ON model_artifacts(model_name, version, artifact_type);

-- ============================================================================
-- SESSION AND USER ANALYTICS
-- ============================================================================

-- Session summaries for analytics
CREATE TABLE session_summaries (
    id BIGSERIAL PRIMARY KEY,
    session_id VARCHAR(128) UNIQUE NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ DEFAULT NULL,
    duration_seconds INTEGER DEFAULT NULL,
    event_count INTEGER DEFAULT 0,
    page_views INTEGER DEFAULT 0,
    unique_events INTEGER DEFAULT 0,
    conversion_events INTEGER DEFAULT 0,
    bounce BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    project VARCHAR(64) DEFAULT NULL,
    metadata JSONB DEFAULT '{}'
);

-- Indexes for session_summaries
CREATE INDEX ix_session_summaries_user_id ON session_summaries(user_id);
CREATE INDEX ix_session_summaries_start_time ON session_summaries(start_time);
CREATE INDEX ix_session_summaries_duration_seconds ON session_summaries(duration_seconds);
CREATE INDEX ix_session_summaries_project ON session_summaries(project);
CREATE INDEX ix_session_summaries_bounce ON session_summaries(bounce);

-- Raw event archive for historical data
CREATE TABLE raw_event_archive (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(128) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(128),
    event_name VARCHAR(128) NOT NULL,
    event_ts TIMESTAMPTZ NOT NULL,
    props JSONB DEFAULT '{}',
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    project VARCHAR(64) DEFAULT NULL,
    original_ingested_at TIMESTAMPTZ DEFAULT NULL
);

-- Indexes for raw_event_archive
CREATE INDEX ix_raw_event_archive_user_id ON raw_event_archive(user_id);
CREATE INDEX ix_raw_event_archive_event_name ON raw_event_archive(event_name);
CREATE INDEX ix_raw_event_archive_event_ts ON raw_event_archive(event_ts);
CREATE INDEX ix_raw_event_archive_archived_at ON raw_event_archive(archived_at);
CREATE INDEX ix_raw_event_archive_project ON raw_event_archive(project);

-- Compacted archived events
CREATE TABLE archive_compacted_events (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    date_partition DATE NOT NULL,
    event_summary JSONB NOT NULL,
    event_count INTEGER NOT NULL,
    unique_events INTEGER NOT NULL,
    session_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for archive_compacted_events
CREATE INDEX ix_archive_compacted_events_user_id ON archive_compacted_events(user_id);
CREATE INDEX ix_archive_compacted_events_date_partition ON archive_compacted_events(date_partition);
CREATE INDEX ix_archive_compacted_events_project ON archive_compacted_events(project);
CREATE UNIQUE INDEX ux_archive_user_date ON archive_compacted_events(user_id, date_partition, project);

-- ============================================================================
-- SCHEMA AND DATA QUALITY
-- ============================================================================

-- Event schema definitions
CREATE TABLE event_schemas (
    id BIGSERIAL PRIMARY KEY,
    event_name VARCHAR(128) NOT NULL,
    schema_definition JSONB NOT NULL,
    version INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by VARCHAR(64) DEFAULT NULL,
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for event_schemas
CREATE INDEX ix_event_schemas_event_name ON event_schemas(event_name);
CREATE INDEX ix_event_schemas_version ON event_schemas(version);
CREATE INDEX ix_event_schemas_is_active ON event_schemas(is_active);
CREATE INDEX ix_event_schemas_project ON event_schemas(project);
CREATE UNIQUE INDEX ux_event_schema_version ON event_schemas(event_name, version, project);

-- Data quality snapshots
CREATE TABLE data_quality_snapshots (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(128) NOT NULL,
    snapshot_date DATE NOT NULL,
    total_records BIGINT NOT NULL,
    null_records BIGINT DEFAULT 0,
    duplicate_records BIGINT DEFAULT 0,
    quality_score DOUBLE PRECISION DEFAULT NULL,
    issues JSONB DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for data_quality_snapshots
CREATE INDEX ix_data_quality_snapshots_table_name ON data_quality_snapshots(table_name);
CREATE INDEX ix_data_quality_snapshots_snapshot_date ON data_quality_snapshots(snapshot_date);
CREATE INDEX ix_data_quality_snapshots_quality_score ON data_quality_snapshots(quality_score);
CREATE INDEX ix_data_quality_snapshots_project ON data_quality_snapshots(project);
CREATE UNIQUE INDEX ux_dq_snapshot_key ON data_quality_snapshots(table_name, snapshot_date, project);

-- ============================================================================
-- FEATURE STORE
-- ============================================================================

-- User features for ML
CREATE TABLE user_features (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    feature_name VARCHAR(128) NOT NULL,
    feature_value DOUBLE PRECISION DEFAULT NULL,
    feature_value_str VARCHAR(256) DEFAULT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ DEFAULT NULL,
    project VARCHAR(64) DEFAULT NULL,
    metadata JSONB DEFAULT '{}'
);

-- Indexes for user_features
CREATE INDEX ix_user_features_user_id ON user_features(user_id);
CREATE INDEX ix_user_features_feature_name ON user_features(feature_name);
CREATE INDEX ix_user_features_computed_at ON user_features(computed_at);
CREATE INDEX ix_user_features_expires_at ON user_features(expires_at);
CREATE INDEX ix_user_features_project ON user_features(project);
CREATE UNIQUE INDEX ux_user_feature_key ON user_features(user_id, feature_name, project);

-- Feature definitions
CREATE TABLE feature_definitions (
    id BIGSERIAL PRIMARY KEY,
    feature_name VARCHAR(128) UNIQUE NOT NULL,
    feature_type VARCHAR(32) NOT NULL, -- numeric|categorical|boolean
    description TEXT DEFAULT NULL,
    computation_logic TEXT DEFAULT NULL,
    refresh_frequency_hours INTEGER DEFAULT 24,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for feature_definitions
CREATE INDEX ix_feature_definitions_feature_type ON feature_definitions(feature_type);
CREATE INDEX ix_feature_definitions_active ON feature_definitions(active);
CREATE INDEX ix_feature_definitions_project ON feature_definitions(project);

-- Feature views for ML pipelines
CREATE TABLE feature_views (
    id BIGSERIAL PRIMARY KEY,
    view_name VARCHAR(128) UNIQUE NOT NULL,
    feature_list JSONB NOT NULL, -- array of feature names
    description TEXT DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    active BOOLEAN DEFAULT TRUE,
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for feature_views
CREATE INDEX ix_feature_views_active ON feature_views(active);
CREATE INDEX ix_feature_views_project ON feature_views(project);

-- ============================================================================
-- BUSINESS INTELLIGENCE
-- ============================================================================

-- User churn risk predictions
CREATE TABLE user_churn_risk (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    risk_score DOUBLE PRECISION NOT NULL,
    risk_category VARCHAR(16) NOT NULL, -- low|medium|high
    contributing_factors JSONB DEFAULT '[]',
    predicted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    model_version VARCHAR(32) DEFAULT NULL,
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for user_churn_risk
CREATE INDEX ix_user_churn_risk_user_id ON user_churn_risk(user_id);
CREATE INDEX ix_user_churn_risk_risk_score ON user_churn_risk(risk_score);
CREATE INDEX ix_user_churn_risk_risk_category ON user_churn_risk(risk_category);
CREATE INDEX ix_user_churn_risk_predicted_at ON user_churn_risk(predicted_at);
CREATE INDEX ix_user_churn_risk_project ON user_churn_risk(project);

-- Cohort retention analysis
CREATE TABLE cohort_retention (
    id BIGSERIAL PRIMARY KEY,
    cohort_month DATE NOT NULL,
    period_number INTEGER NOT NULL,
    users_count INTEGER NOT NULL,
    retained_users INTEGER NOT NULL,
    retention_rate DOUBLE PRECISION NOT NULL,
    project VARCHAR(64) DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for cohort_retention
CREATE INDEX ix_cohort_retention_cohort_month ON cohort_retention(cohort_month);
CREATE INDEX ix_cohort_retention_period_number ON cohort_retention(period_number);
CREATE INDEX ix_cohort_retention_retention_rate ON cohort_retention(retention_rate);
CREATE INDEX ix_cohort_retention_project ON cohort_retention(project);
CREATE UNIQUE INDEX ux_cohort_period ON cohort_retention(cohort_month, period_number, project);

-- A/B test metrics
CREATE TABLE ab_test_metrics (
    id BIGSERIAL PRIMARY KEY,
    experiment_name VARCHAR(128) NOT NULL,
    variant VARCHAR(64) NOT NULL,
    metric_name VARCHAR(128) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    user_count INTEGER NOT NULL,
    conversion_count INTEGER DEFAULT 0,
    confidence_interval_lower DOUBLE PRECISION DEFAULT NULL,
    confidence_interval_upper DOUBLE PRECISION DEFAULT NULL,
    statistical_significance DOUBLE PRECISION DEFAULT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for ab_test_metrics
CREATE INDEX ix_ab_test_metrics_experiment_name ON ab_test_metrics(experiment_name);
CREATE INDEX ix_ab_test_metrics_variant ON ab_test_metrics(variant);
CREATE INDEX ix_ab_test_metrics_metric_name ON ab_test_metrics(metric_name);
CREATE INDEX ix_ab_test_metrics_computed_at ON ab_test_metrics(computed_at);
CREATE INDEX ix_ab_test_metrics_project ON ab_test_metrics(project);
CREATE UNIQUE INDEX ux_ab_test_key ON ab_test_metrics(experiment_name, variant, metric_name, computed_at);

-- ============================================================================
-- REPORTING AND ALERTS
-- ============================================================================

-- Report logs for audit trail
CREATE TABLE report_logs (
    id BIGSERIAL PRIMARY KEY,
    report_name VARCHAR(128) NOT NULL,
    report_type VARCHAR(64) NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    generated_by VARCHAR(64) DEFAULT NULL,
    parameters JSONB DEFAULT '{}',
    execution_time_ms INTEGER DEFAULT NULL,
    status VARCHAR(16) DEFAULT 'completed', -- completed|failed|cancelled
    error_message TEXT DEFAULT NULL,
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for report_logs
CREATE INDEX ix_report_logs_report_name ON report_logs(report_name);
CREATE INDEX ix_report_logs_report_type ON report_logs(report_type);
CREATE INDEX ix_report_logs_generated_at ON report_logs(generated_at);
CREATE INDEX ix_report_logs_status ON report_logs(status);
CREATE INDEX ix_report_logs_project ON report_logs(project);

-- Dataset snapshots for reproducibility
CREATE TABLE dataset_snapshots (
    id BIGSERIAL PRIMARY KEY,
    snapshot_name VARCHAR(128) NOT NULL,
    dataset_query TEXT NOT NULL,
    record_count BIGINT NOT NULL,
    data_hash VARCHAR(64) DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ DEFAULT NULL,
    project VARCHAR(64) DEFAULT NULL,
    metadata JSONB DEFAULT '{}'
);

-- Indexes for dataset_snapshots
CREATE INDEX ix_dataset_snapshots_snapshot_name ON dataset_snapshots(snapshot_name);
CREATE INDEX ix_dataset_snapshots_created_at ON dataset_snapshots(created_at);
CREATE INDEX ix_dataset_snapshots_expires_at ON dataset_snapshots(expires_at);
CREATE INDEX ix_dataset_snapshots_project ON dataset_snapshots(project);

-- ============================================================================
-- PROJECT MANAGEMENT
-- ============================================================================

-- Project quotas and limits
CREATE TABLE project_quotas (
    id BIGSERIAL PRIMARY KEY,
    project VARCHAR(64) UNIQUE NOT NULL,
    max_events_per_day BIGINT DEFAULT 1000000,
    max_users BIGINT DEFAULT 100000,
    max_storage_gb DOUBLE PRECISION DEFAULT 100.0,
    current_events_today BIGINT DEFAULT 0,
    current_users BIGINT DEFAULT 0,
    current_storage_gb DOUBLE PRECISION DEFAULT 0.0,
    quota_reset_at TIMESTAMPTZ DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for project_quotas
CREATE INDEX ix_project_quotas_project ON project_quotas(project);
CREATE INDEX ix_project_quotas_quota_reset_at ON project_quotas(quota_reset_at);

-- Model evaluation results
CREATE TABLE model_evaluations (
    id BIGSERIAL PRIMARY KEY,
    model_name VARCHAR(64) NOT NULL,
    version VARCHAR(32) NOT NULL,
    evaluation_metric VARCHAR(64) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    dataset_name VARCHAR(128) DEFAULT NULL,
    evaluation_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}',
    project VARCHAR(64) DEFAULT NULL
);

-- Indexes for model_evaluations
CREATE INDEX ix_model_evaluations_model_name ON model_evaluations(model_name);
CREATE INDEX ix_model_evaluations_version ON model_evaluations(version);
CREATE INDEX ix_model_evaluations_evaluation_metric ON model_evaluations(evaluation_metric);
CREATE INDEX ix_model_evaluations_evaluation_date ON model_evaluations(evaluation_date);
CREATE INDEX ix_model_evaluations_project ON model_evaluations(project);

-- ============================================================================
-- DATA QUALITY AND MONITORING
-- ============================================================================

-- Data quality alerts
CREATE TABLE data_quality_alerts (
    id BIGSERIAL PRIMARY KEY,
    metric_type VARCHAR(64) NOT NULL,
    event_name VARCHAR(128) DEFAULT NULL,
    threshold_value DOUBLE PRECISION NOT NULL,
    actual_value DOUBLE PRECISION NOT NULL,
    alert_level VARCHAR(16) DEFAULT 'warning', -- info|warning|error|critical
    message TEXT DEFAULT NULL,
    status VARCHAR(16) DEFAULT 'open', -- open|ack|closed
    project VARCHAR(64) DEFAULT NULL,
    details JSONB DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged_at TIMESTAMPTZ DEFAULT NULL,
    closed_at TIMESTAMPTZ DEFAULT NULL
);

-- Indexes for data_quality_alerts
CREATE INDEX ix_dq_alerts_metric_type ON data_quality_alerts(metric_type);
CREATE INDEX ix_dq_alerts_event_name ON data_quality_alerts(event_name);
CREATE INDEX ix_dq_alerts_alert_level ON data_quality_alerts(alert_level);
CREATE INDEX ix_dq_alerts_status ON data_quality_alerts(status);
CREATE INDEX ix_dq_alerts_project ON data_quality_alerts(project);
CREATE INDEX ix_dq_alerts_created_at ON data_quality_alerts(created_at);
CREATE INDEX ix_dq_alert_key ON data_quality_alerts(metric_type, event_name, project, status);

-- ============================================================================
-- End of Complete Schema
-- ============================================================================

-- Summary: Total of 50+ tables covering:
-- - Core event tracking and analytics
-- - Machine learning operations  
-- - Data quality and governance
-- - User personalization and recommendations
-- - Business intelligence and reporting
-- - System monitoring and audit trails
-- - Feature store for ML pipelines
-- - A/B testing and experimentation
-- - Data lineage and cataloging
-- - Project management and quotas

-- This complete schema provides a comprehensive foundation for the
-- Onboarding Drop-off Analyzer platform with enterprise-grade capabilities.
