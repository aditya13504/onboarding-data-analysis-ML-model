from __future__ import annotations
from celery import shared_task
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent, RawEventArchive, OpsMetric
from prometheus_client import Gauge, Counter
import json

try:
    from onboarding_analyzer.api.main import registry as _api_registry  # type: ignore
except Exception:  # pragma: no cover
    _api_registry = None

EVENT_STORAGE_GB = Gauge('storage_raw_events_gb', 'Approx raw_events logical storage (GB)', registry=_api_registry)
ARCHIVE_STORAGE_GB = Gauge('storage_archive_events_gb', 'Approx archived events logical storage (GB)', registry=_api_registry)
EVENT_INGEST_RATE = Gauge('ingest_events_per_minute_recent', 'Recent ingestion rate (events/min over lookback window)', registry=_api_registry)
STORAGE_COST_ESTIMATE = Gauge('storage_monthly_cost_estimate_usd', 'Estimated monthly storage spend (USD)', registry=_api_registry)
COST_TASK_RUNS = Counter('cost_observation_runs_total', 'Cost observation task executions', registry=_api_registry)

RAW_EVENT_COST_PER_GB = 0.25  # configurable later (USD/GB-month)
ARCHIVE_EVENT_COST_PER_GB = 0.05  # cheaper tier


def _approx_table_size_gb(session: Session, model, sample_rows: int = 2000) -> float:
    """Approximate logical size by sampling row payload JSON length.

    We only serialize props column plus fixed overhead estimation.
    """
    total_rows = session.query(func.count(model.id)).scalar() or 0
    if total_rows == 0:
        return 0.0
    sample = session.query(model).order_by(model.id.desc()).limit(sample_rows).all()
    if not sample:
        return 0.0
    import math
    lengths = []
    for r in sample:
        try:
            if hasattr(r, 'props'):
                lengths.append(len(json.dumps(r.props, separators=(',',':'))))
            else:
                lengths.append(64)
        except Exception:
            lengths.append(64)
    avg_len = sum(lengths)/len(lengths)
    # crude fixed overhead per row (ids, indexes) ~ 150 bytes
    avg_row_bytes = avg_len + 150
    total_bytes = avg_row_bytes * total_rows
    return float(total_bytes / (1024**3))


@shared_task
def observe_costs(ingest_rate_lookback_minutes: int = 15):
    session: Session = SessionLocal()
    try:
        # Storage approximations
        raw_gb = _approx_table_size_gb(session, RawEvent)
        arch_gb = _approx_table_size_gb(session, RawEventArchive)
        # Ingestion rate (events in lookback / minutes)
        since = datetime.utcnow() - timedelta(minutes=ingest_rate_lookback_minutes)
        recent = session.query(func.count(RawEvent.id)).filter(RawEvent.ts >= since).scalar() or 0
        rate = recent / max(ingest_rate_lookback_minutes, 1)
        # Monthly cost estimate (simple linear extrapolation using current GB)
        est_cost = raw_gb * RAW_EVENT_COST_PER_GB + arch_gb * ARCHIVE_EVENT_COST_PER_GB
        # Update Prom metrics
        try:
            EVENT_STORAGE_GB.set(raw_gb)
            ARCHIVE_STORAGE_GB.set(arch_gb)
            EVENT_INGEST_RATE.set(rate)
            STORAGE_COST_ESTIMATE.set(est_cost)
            COST_TASK_RUNS.inc()
        except Exception:
            pass
        # Persist OpsMetric snapshots
        session.add(OpsMetric(metric_name='storage_raw_events_gb', metric_value=raw_gb, details=None))
        session.add(OpsMetric(metric_name='storage_archive_events_gb', metric_value=arch_gb, details=None))
        session.add(OpsMetric(metric_name='ingest_events_per_minute_recent', metric_value=rate, details={"lookback_min": ingest_rate_lookback_minutes}))
        session.add(OpsMetric(metric_name='storage_monthly_cost_estimate_usd', metric_value=est_cost, details={"raw_gb": raw_gb, "archive_gb": arch_gb}))
        session.commit()
        return {"status": "ok", "raw_gb": raw_gb, "archive_gb": arch_gb, "ingest_rate_m": rate, "monthly_cost_usd": est_cost}
    finally:
        session.close()
