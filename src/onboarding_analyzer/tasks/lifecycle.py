"""Data retention enforcement & model lifecycle automation tasks.

Implements:
 - enforce_retention_policies(): deletes / masks rows exceeding max_age_days.
 - schedule_model_retraining(): inspects drift metrics & training runs to enqueue retrain (placeholder hook).

No mock data: operates on real tables using configured RetentionPolicy + ModelDriftThreshold rows.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from sqlalchemy import select, text
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import (
    RetentionPolicy, RawEvent, RawEventArchive, ModelDriftThreshold, ModelDriftMetric,
    DriftRetrainAudit, ModelTrainingRun
)
from celery import shared_task

PII_FIELDS_ALLOWLIST = {"user_id", "session_id"}

@shared_task
def enforce_retention_policies(batch_size: int = 5000):
    s = SessionLocal()
    deleted_total = 0
    masked_total = 0
    try:
        policies = s.query(RetentionPolicy).filter(RetentionPolicy.active==1).all()
        now = datetime.utcnow()
        for p in policies:
            cutoff = now - timedelta(days=p.max_age_days)
            if p.table_name == 'raw_events':
                # Move to archive then delete
                old = s.query(RawEvent).filter(RawEvent.ts < cutoff).limit(batch_size).all()
                if not old:
                    continue
                for ev in old:
                    arch = RawEventArchive(
                        event_id=ev.event_id, user_id=ev.user_id, session_id=ev.session_id,
                        event_name=ev.event_name, ts=ev.ts, props=ev.props, schema_version=ev.schema_version,
                        project=ev.project
                    )
                    s.add(arch)
                    s.delete(ev)
                    deleted_total += 1
            # Simple masking: hash (truncate) PII fields if specified
            if p.pii_fields:
                fields = [f.strip() for f in p.pii_fields.split(',') if f.strip()]
                safe_fields = [f for f in fields if f in PII_FIELDS_ALLOWLIST]
                if safe_fields and p.table_name == 'raw_events':
                    # Bulk update using substr hash mimic (SQLite friendly)
                    for fld in safe_fields:
                        s.execute(text(f"UPDATE raw_events SET {fld}=substr({fld},1,8)||'_redact' WHERE ts < :cutoff"), {"cutoff": cutoff})
                        masked_total += s.execute(text("SELECT changes()" )).scalar() or 0
        s.commit()
        return {"status": "ok", "deleted": deleted_total, "masked": masked_total}
    finally:
        s.close()

@shared_task
def schedule_model_retraining(max_actions: int = 5):
    s = SessionLocal()
    actions = 0
    try:
        thresholds = s.query(ModelDriftThreshold).filter(ModelDriftThreshold.active==1).all()
        now = datetime.utcnow()
        for t in thresholds:
            if t.last_triggered_at and (now - t.last_triggered_at) < timedelta(hours=t.cooldown_hours):
                continue
            metric = s.query(ModelDriftMetric).filter(ModelDriftMetric.model_name==t.model_name, ModelDriftMetric.metric_name==t.metric_name).order_by(ModelDriftMetric.captured_at.desc()).first()
            if not metric:
                continue
            val = metric.metric_value
            cmp = False
            if t.comparison == 'gt':
                cmp = val > t.boundary
            elif t.comparison == 'ge':
                cmp = val >= t.boundary
            elif t.comparison == 'lt':
                cmp = val < t.boundary
            elif t.comparison == 'le':
                cmp = val <= t.boundary
            if not cmp:
                continue
            # Action: record audit + create training run stub (actual training pipeline external)
            audit = DriftRetrainAudit(model_name=t.model_name, metric_name=t.metric_name, metric_value=val, action=t.action, notes="auto-trigger")
            s.add(audit)
            run = ModelTrainingRun(model_name=t.model_name, version=f"{now:%Y%m%d%H%M%S}", status='running', params={"trigger": "drift"})
            s.add(run)
            t.last_triggered_at = now
            actions += 1
            if actions >= max_actions:
                break
        s.commit()
        return {"status": "ok", "actions": actions}
    finally:
        s.close()

__all__ = ["enforce_retention_policies", "schedule_model_retraining"]