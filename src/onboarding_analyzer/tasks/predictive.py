from __future__ import annotations
from celery import shared_task
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent, UserChurnRisk, CohortRetention, ModelVersion, ModelArtifact
from prometheus_client import Counter, Histogram
try:
    from onboarding_analyzer.api.main import registry as _api_registry  # type: ignore
except Exception:  # pragma: no cover
    _api_registry = None

CHURN_RUNS = Counter('churn_model_runs_total', 'Churn risk model executions', registry=_api_registry)
CHURN_USERS_SCORED = Counter('churn_users_scored_total', 'User churn risk rows written', registry=_api_registry)
RETENTION_RUNS = Counter('retention_recompute_runs_total', 'Retention recompute executions', registry=_api_registry)
RETENTION_ROWS = Counter('retention_rows_written_total', 'Cohort retention rows inserted', registry=_api_registry)
CHURN_LATENCY = Histogram('churn_model_latency_seconds', 'Latency of churn scoring', buckets=(0.1,0.5,1,2,5,10), registry=_api_registry)
RETENTION_LATENCY = Histogram('retention_latency_seconds', 'Latency of retention computation', buckets=(0.1,0.5,1,2,5,10), registry=_api_registry)


@shared_task
def compute_churn_risk(window_days: int = 30, inactivity_days_high: int = 14):
    """Score users for churn risk using simple heuristic on recency & activity.

    risk_score in [0,1]: higher means more likely to churn.
    Formula components:
      - recency_factor: days since last event / inactivity_days_high (capped at 1)
      - activity_factor: 1 - (events_last_window / max_window_events) where max_window_events=95th percentile approx via top count.
      - completion_factor: 1 - completion_ratio (if available via session_event_index distribution, fallback 0.5)
    risk = 0.5*recency_factor + 0.3*activity_factor + 0.2*completion_factor
    """
    import time as _t
    t0 = _t.time()
    session: Session = SessionLocal()
    try:
        now = datetime.utcnow()
        window_start = now - timedelta(days=window_days)
        # Gather basic stats: last event per user, count of events in window
        q = (
            select(RawEvent.user_id, func.max(RawEvent.ts), func.count(RawEvent.id))
            .where(RawEvent.ts >= window_start)
            .group_by(RawEvent.user_id)
        )
        rows = session.execute(q).all()
        if not rows:
            return {"status": "no_data"}
        counts = [r[2] for r in rows]
        max_window_events = sorted(counts)[int(0.95 * (len(counts)-1))] if counts else 1
        if max_window_events < 1:
            max_window_events = 1
        scored = 0
        for user_id, last_ts, cnt in rows:
            days_since = (now - last_ts).total_seconds()/86400.0
            recency_factor = min(1.0, days_since / max(1, inactivity_days_high))
            activity_factor = 1 - min(1.0, cnt / max_window_events)
            # Approx completion: ratio of distinct steps vs events (rough proxy). Pull subset quickly.
            distinct_steps = session.query(func.count(func.distinct(RawEvent.step_index))).filter(RawEvent.user_id==user_id, RawEvent.ts>=window_start).scalar() or 0
            total_events = cnt
            completion_ratio = (distinct_steps / total_events) if total_events else 0.0
            if completion_ratio <= 0 or completion_ratio > 1:
                completion_ratio = 0.5
            completion_factor = 1 - completion_ratio
            risk = 0.5*recency_factor + 0.3*activity_factor + 0.2*completion_factor
            existing = session.query(UserChurnRisk).filter_by(user_id=user_id).first()
            version = f"v1-{window_days}d"
            if existing:
                existing.risk_score = risk
                existing.model_version = version
                existing.computed_at = now
            else:
                session.add(UserChurnRisk(user_id=user_id, project=None, risk_score=risk, model_version=version, computed_at=now))
            scored += 1
        session.commit()
        # ModelVersion + artifact (store simple distribution stats)
        from onboarding_analyzer.models.tables import ModelVersion, ModelArtifact
        version = f"churn-{now.strftime('%Y%m%d%H%M%S')}"
        mv = ModelVersion(model_name="churn", version=version)
        session.add(mv)
        dist = session.query(func.avg(UserChurnRisk.risk_score), func.max(UserChurnRisk.risk_score), func.min(UserChurnRisk.risk_score)).all()[0]
        artifact = {
            "avg": float(dist[0] or 0.0),
            "max": float(dist[1] or 0.0),
            "min": float(dist[2] or 0.0),
            "window_days": window_days,
            "inactivity_days_high": inactivity_days_high,
            "users": scored,
        }
        session.add(ModelArtifact(model_name="churn", version=version, artifact=artifact))
        # Promotion logic: always promote first; else if avg risk increases by >0.05 (indicating deterioration needing attention) OR decreases by >0.05 (improvement) promote.
        prev = session.query(ModelVersion).filter(ModelVersion.model_name=="churn", ModelVersion.promoted==1).order_by(ModelVersion.created_at.desc()).first()
        promote = False
        if not prev:
            promote = True
        else:
            prev_art = session.query(ModelArtifact).filter(ModelArtifact.model_name=="churn", ModelArtifact.version==prev.version).first()
            prev_avg = (prev_art.artifact or {}).get("avg") if prev_art else None
            if prev_avg is None or abs(artifact["avg"] - prev_avg) > 0.05:
                promote = True
        if promote:
            session.query(ModelVersion).filter(ModelVersion.model_name=="churn", ModelVersion.promoted==1).update({ModelVersion.promoted: 0})
            mv.promoted = 1
        session.commit()
        try:
            CHURN_RUNS.inc()
            CHURN_USERS_SCORED.inc(scored)
            CHURN_LATENCY.observe(_t.time() - t0)
        except Exception:
            pass
        return {"status": "ok", "scored": scored}
    finally:
        session.close()


@shared_task
def recompute_retention(day_horizon: int = 14, cohort_days: int = 14):
    """Compute retention cohorts using first event date as cohort date.

    For each of the last `cohort_days` cohorts (one per day), compute retention for days 0..day_horizon.
    Retention: users active on that day / cohort size.
    """
    import time as _t
    t0 = _t.time()
    session: Session = SessionLocal()
    try:
        now = datetime.utcnow()
        start_cohort = now - timedelta(days=cohort_days)
        # Determine first event per user (approx signup)
        sub = (
            select(RawEvent.user_id, func.min(RawEvent.ts).label('first_ts'))
            .group_by(RawEvent.user_id)
            .subquery()
        )
        cohorts = session.execute(select(sub.c.user_id, sub.c.first_ts).where(sub.c.first_ts >= start_cohort)).all()
        if not cohorts:
            return {"status": "no_data"}
        # Build mapping: cohort_date -> set(user)
        cohort_map = {}
        for uid, first_ts in cohorts:
            cdate = datetime(first_ts.year, first_ts.month, first_ts.day)
            s = cohort_map.setdefault(cdate, set())
            s.add(uid)
        inserted = 0
        for cdate, users in cohort_map.items():
            cohort_size = len(users)
            if cohort_size == 0:
                continue
            for day in range(day_horizon+1):
                day_start = cdate + timedelta(days=day)
                day_end = day_start + timedelta(days=1)
                active_users = session.execute(
                    select(func.count(func.distinct(RawEvent.user_id)))
                    .where(RawEvent.user_id.in_(users), RawEvent.ts >= day_start, RawEvent.ts < day_end)
                ).scalar() or 0
                existing = session.query(CohortRetention).filter_by(cohort_date=cdate, day_number=day).first()
                if existing:
                    existing.retained_users = active_users
                    existing.total_users = cohort_size
                else:
                    session.add(CohortRetention(cohort_date=cdate, day_number=day, retained_users=active_users, total_users=cohort_size))
                inserted += 1
        session.commit()
        try:
            RETENTION_RUNS.inc()
            RETENTION_ROWS.inc(inserted)
            RETENTION_LATENCY.observe(_t.time() - t0)
        except Exception:
            pass
        return {"status": "ok", "rows": inserted}
    finally:
        session.close()
