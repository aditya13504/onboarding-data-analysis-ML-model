from celery import Celery
from celery import signals
import time
from prometheus_client import Counter, Histogram
from onboarding_analyzer.config import get_settings

settings = get_settings()

celery_app = Celery(
    "onboarding_analyzer",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=[
        "onboarding_analyzer.tasks.ingestion",
        "onboarding_analyzer.tasks.analytics",
        "onboarding_analyzer.tasks.clustering",
        "onboarding_analyzer.tasks.recommendation",
        "onboarding_analyzer.tasks.reporting",
    "onboarding_analyzer.tasks.sessionization",
    "onboarding_analyzer.tasks.identity",
    "onboarding_analyzer.tasks.predictive",
    "onboarding_analyzer.tasks.mlops",
    "onboarding_analyzer.tasks.cost",
    "onboarding_analyzer.tasks.streaming",
    "onboarding_analyzer.tasks.performance",
    "onboarding_analyzer.tasks.lifecycle",
    "onboarding_analyzer.tasks.impact",
    "onboarding_analyzer.tasks.lineage",
    "onboarding_analyzer.tasks.kafka_audit",
    "onboarding_analyzer.tasks.secrets",
    "onboarding_analyzer.tasks.quota",
    "onboarding_analyzer.tasks.batching_dlq",
    "onboarding_analyzer.tasks.quota",
    ],
)

celery_app.conf.update(task_serializer="json", result_serializer="json", accept_content=["json"], timezone="UTC", enable_utc=True)

# Task metrics (exposed in API process by sharing default registry via multiprocess if configured)
TASK_SUCCESS = Counter('celery_task_success_total', 'Celery task successes', ['task'])
TASK_FAILURE = Counter('celery_task_failure_total', 'Celery task failures', ['task'])
TASK_DURATION = Histogram('celery_task_duration_seconds', 'Celery task runtime', ['task'], buckets=(0.05,0.1,0.25,0.5,1,2,5,10,30,60))

_task_start_times = {}

@signals.task_prerun.connect
def _task_prerun(sender=None, task_id=None, **kwargs):  # noqa
    _task_start_times[task_id] = time.time()

@signals.task_postrun.connect
def _task_postrun(sender=None, task_id=None, state=None, **kwargs):  # noqa
    start = _task_start_times.pop(task_id, None)
    if start is not None:
        duration = time.time() - start
        try:
            TASK_DURATION.labels(task=sender.name if sender else 'unknown').observe(duration)
        except Exception:
            pass
    if state == 'SUCCESS':
        try:
            TASK_SUCCESS.labels(task=sender.name if sender else 'unknown').inc()
        except Exception:
            pass
    elif state not in (None, 'SUCCESS'):
        try:
            TASK_FAILURE.labels(task=sender.name if sender else 'unknown').inc()
        except Exception:
            pass

# Periodic tasks (beat). Requires worker with -B or separate beat service.
celery_app.conf.beat_schedule = {
    "compute-funnel-every-5m": {
        "task": "onboarding_analyzer.tasks.analytics.compute_funnel_metrics",
        "schedule": 300.0,
    },
    "refresh-clusters-hourly": {
        "task": "onboarding_analyzer.tasks.clustering.refresh_clusters",
        "schedule": 3600.0,
    },
    "generate-insights-hourly": {
        "task": "onboarding_analyzer.tasks.recommendation.generate_insights",
        "schedule": 3600.0,
    },
    "advance-insight-lifecycle-hourly": {
        "task": "onboarding_analyzer.tasks.recommendation.advance_insight_lifecycle",
        "schedule": 3600.0,
    },
    "compose-weekly-report-daily": {  # run daily; weekly logic inside task
        "task": "onboarding_analyzer.tasks.reporting.compose_weekly_report",
        "schedule": 86400.0,
    },
    "retention-daily": {
        "task": "onboarding_analyzer.tasks.maintenance.enforce_retention",
        "schedule": 86400.0,
    },
    "archive-old-events-daily": {
        "task": "onboarding_analyzer.tasks.maintenance.archive_old_events",
        "schedule": 86400.0,
    },
    "compact-archive-daily": {
        "task": "onboarding_analyzer.tasks.maintenance.compact_archive",
        "schedule": 86400.0,
    },
    "scheduled-retrain-hourly": {
        "task": "onboarding_analyzer.tasks.maintenance.scheduled_retrain",
        "schedule": 3600.0,
    },
    "detect-anomalies-30m": {
        "task": "onboarding_analyzer.tasks.anomaly.detect_dropoff_anomalies",
        "schedule": 1800.0,
    },
    "detect-volume-latency-anomalies-15m": {
        "task": "onboarding_analyzer.tasks.anomaly.detect_event_volume_and_latency_anomalies",
        "schedule": 900.0,
    },
    "reprocess-dead-letters-15m": {
        "task": "onboarding_analyzer.tasks.ingestion.reprocess_dead_letters",
        "schedule": 900.0,
    },
    "session-summaries-hourly": {
        "task": "onboarding_analyzer.tasks.sessionization.rebuild_session_summaries",
        "schedule": 3600.0,
    },
    "precreate-next-month-partition-daily": {
        "task": "onboarding_analyzer.tasks.maintenance.precreate_next_month_partition",
        "schedule": 86400.0,
    },
    "incremental-connectors-5m": {
        "task": "onboarding_analyzer.tasks.ingestion.orchestrate_incremental_connectors",
        "schedule": 300.0,
        "options": {"expires": 240},
    },
    "identity-consolidation-daily": {
        "task": "onboarding_analyzer.tasks.identity.apply_identity_consolidation",
        "schedule": 86400.0,
    },
    "identity-graph-metrics-daily": {
        "task": "onboarding_analyzer.tasks.identity.consolidate_identity_graph",
        "schedule": 86400.0,
    },
    "recompute-insight-scores-hourly": {
        "task": "onboarding_analyzer.tasks.recommendation.recompute_insight_scores",
        "schedule": 3600.0,
    },
    "churn-risk-daily": {
        "task": "onboarding_analyzer.tasks.predictive.compute_churn_risk",
        "schedule": 86400.0,
    },
    "retention-recompute-daily": {
        "task": "onboarding_analyzer.tasks.predictive.recompute_retention",
        "schedule": 86400.0,
    },
    "ab-tests-hourly": {
        "task": "onboarding_analyzer.tasks.modeling.compute_all_ab_tests",
        "schedule": 3600.0,
    },
    "feature-materialization-hourly": {
        "task": "onboarding_analyzer.tasks.mlops.materialize_feature_views",
        "schedule": 3600.0,
    },
    "model-drift-daily": {
        "task": "onboarding_analyzer.tasks.mlops.evaluate_model_drift",
        "schedule": 86400.0,
    },
    "ingestion-health-5m": {
        "task": "onboarding_analyzer.tasks.ingestion.evaluate_ingestion_health",
        "schedule": 300.0,
    },
    "cost-observation-hourly": {
        "task": "onboarding_analyzer.tasks.cost.observe_costs",
        "schedule": 3600.0,
    },
    "stream-consume-5s": {
        "task": "onboarding_analyzer.tasks.streaming.consume_kafka_once",
        "schedule": 5.0,
        "options": {"expires": 4},
    },
    "advise-indexes-hourly": {
        "task": "onboarding_analyzer.tasks.performance.advise_indexes",
        "schedule": 3600.0,
    },
    "personalization-30m": {
        "task": "onboarding_analyzer.tasks.personalization.compute_personalized_recommendations",
        "schedule": 1800.0,
    },
    "personalization-conv-10m": {
        "task": "onboarding_analyzer.tasks.personalization.update_recommendation_conversions",
        "schedule": 600.0,
    },
    "lineage-hourly": {
        "task": "onboarding_analyzer.tasks.lineage.build_lineage_graph",
        "schedule": 3600.0,
    },
}

celery_app.conf.beat_schedule.update({
    "connector-adaptive-windows-15m": {
        "task": "onboarding_analyzer.tasks.ingestion.adapt_connector_windows",
        "schedule": 900.0,
    },
    "lineage-impact-hourly": {
        "task": "onboarding_analyzer.tasks.lineage.compute_lineage_impact",
        "schedule": 3600.0,
    },
    # Batch processing and DLQ observability tasks
    "optimize-batch-processing-30m": {
        "task": "optimize_batch_processing",
        "schedule": 1800.0,  # Every 30 minutes
    },
    "analyze-dlq-patterns-15m": {
        "task": "analyze_dlq_patterns", 
        "schedule": 900.0,  # Every 15 minutes
    },
    "auto-recover-dlq-events-10m": {
        "task": "auto_recover_dlq_events",
        "schedule": 600.0,  # Every 10 minutes
    },
    "late-event-reconciliation-5m": {
        "task": "run_late_event_reconciliation",
        "schedule": 300.0,  # Every 5 minutes
    },
    "cleanup-batch-metrics-daily": {
        "task": "cleanup_old_batch_metrics",
        "schedule": 86400.0,  # Daily
    },
    "cleanup-dlq-recovery-attempts-daily": {
        "task": "cleanup_old_dlq_recovery_attempts", 
        "schedule": 86400.0,  # Daily
    },
})

# Late binding of dynamic schedule/kwargs using settings
try:
    from onboarding_analyzer.config import get_settings
    _s = get_settings()
    interval = max(60, _s.experiment_promotion_interval_minutes * 60)  # enforce >=60s to avoid thrash
    celery_app.conf.beat_schedule["experiment-promotion-dynamic"]["schedule"] = float(interval)
    celery_app.conf.beat_schedule["experiment-promotion-dynamic"]["kwargs"] = {
        "min_prob": _s.experiment_promotion_min_prob,
        "min_lift": _s.experiment_promotion_min_lift,
    }
except Exception:
    pass
