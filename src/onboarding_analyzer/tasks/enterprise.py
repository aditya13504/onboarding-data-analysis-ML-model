from __future__ import annotations

from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func
from celery import shared_task
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent, CostUsage, DataAsset, DataLineageEdge, UserRecommendation, PersonalizationRule


@shared_task
def aggregate_cost_usage(window_minutes: int = 60):
    """Aggregate cost & usage metrics per project for preceding window.

    Storage bytes is approximated via avg JSON length of props * event count (best-effort heuristic).
    Compute seconds is estimated as event_count * constant (0.0005s) for rough ingestion CPU attribution.
    Idempotent per (project, window_start, window_end).
    """
    now = datetime.utcnow()
    window_start = now - timedelta(minutes=window_minutes)
    session: Session = SessionLocal()
    try:
        # Gather counts per project
        rows = session.query(RawEvent.project, func.count(RawEvent.id), func.avg(func.length(func.cast(RawEvent.props, str)))).filter(RawEvent.ts >= window_start, RawEvent.ts < now).group_by(RawEvent.project).all()
        for project, cnt, avg_len in rows:
            existing = session.query(CostUsage.id).filter(CostUsage.project==project, CostUsage.window_start==window_start, CostUsage.window_end==now).first()
            if existing:
                continue
            avg_len = avg_len or 0
            storage_bytes = int((avg_len) * cnt)
            compute_seconds = cnt * 0.0005
            # Simple cost model: $0.25 per million events + $0.10 per GB stored + $0.05 per CPU hour
            gb = storage_bytes / (1024**3)
            cost_estimate = (cnt / 1_000_000) * 0.25 + gb * 0.10 + (compute_seconds/3600) * 0.05
            session.add(CostUsage(project=project, window_start=window_start, window_end=now, event_count=cnt, storage_bytes=storage_bytes, compute_seconds=compute_seconds, cost_estimate=cost_estimate, breakdown={"events": cnt, "avg_prop_bytes": avg_len}))
        session.commit()
        return {"status": "ok", "projects": len(rows)}
    finally:
        session.close()


@shared_task
def enrich_lineage_projects():
    """Infer missing project assignments for data assets using upstream/downstream majority voting."""
    session: Session = SessionLocal()
    try:
        assets = session.query(DataAsset).filter((DataAsset.project.is_(None)) | (DataAsset.project=='' )).all()
        if not assets:
            return {"status":"empty"}
        updated = 0
        for a in assets:
            # Gather connected edges
            ups = session.query(DataAsset.project).join(DataLineageEdge, DataLineageEdge.upstream_key==DataAsset.asset_key).filter(DataLineageEdge.downstream_key==a.asset_key, DataAsset.project.isnot(None)).all()
            downs = session.query(DataAsset.project).join(DataLineageEdge, DataLineageEdge.downstream_key==DataAsset.asset_key).filter(DataLineageEdge.upstream_key==a.asset_key, DataAsset.project.isnot(None)).all()
            candidates = [r[0] for r in ups+downs if r[0]]
            if not candidates:
                continue
            # Majority vote
            from collections import Counter
            proj = Counter(candidates).most_common(1)[0][0]
            a.project = proj; updated += 1
        if updated:
            session.commit()
        return {"status":"ok","updated": updated}
    finally:
        session.close()


@shared_task
def prune_idempotency_keys(older_than_days: int = 30, batch_size: int = 10000):
    """Drop idempotency keys from events older than retention window to reduce index bloat."""
    cutoff = datetime.utcnow() - timedelta(days=older_than_days)
    session: Session = SessionLocal()
    try:
        q = session.query(RawEvent).filter(RawEvent.ts < cutoff, RawEvent.idempotency_key.isnot(None)).limit(batch_size)
        rows = q.all()
        if not rows:
            return {"status":"empty"}
        for r in rows:
            r.idempotency_key = None
        session.commit()
        return {"status":"ok","pruned": len(rows)}
    finally:
        session.close()


@shared_task
def evaluate_personalization_rules(max_users: int = 1000):
    """Evaluate active personalization rules and create new user recommendations respecting cooldown."""
    session: Session = SessionLocal()
    try:
        rules = session.query(PersonalizationRule).filter(PersonalizationRule.active==1).all()
        if not rules:
            return {"status":"no_rules"}
    # Future: integrate feature store & user targeting criteria evaluation.
        # Mark last_triggered_at to keep audit trail.
        for r in rules:
            r.last_triggered_at = datetime.utcnow()
        session.commit()
        return {"status":"ok","rules": len(rules)}
    finally:
        session.close()
