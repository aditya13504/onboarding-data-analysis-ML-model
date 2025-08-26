from __future__ import annotations
"""Proactive quota monitoring (daily & monthly) emitting alerts when usage nears limits."""
from datetime import datetime, timedelta
from celery import shared_task
from sqlalchemy.orm import Session
from sqlalchemy import func
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import ProjectQuota, RawEvent, AlertLog
from onboarding_analyzer.config import get_settings

THRESHOLDS = (0.8, 0.9, 1.0)  # 80%, 90%, 100%

@shared_task
def evaluate_project_quotas():
    s = get_settings()
    session: Session = SessionLocal()
    try:
        now = datetime.utcnow()
        day_start = datetime(now.year, now.month, now.day)
        month_start = datetime(now.year, now.month, 1)
        quotas = session.query(ProjectQuota).filter(ProjectQuota.enforced==1).all()
        if not quotas:
            return {"status": "no_quotas"}
        # Precompute usage per project
        daily_rows = session.query(RawEvent.project, func.count(RawEvent.id)).filter(RawEvent.ts >= day_start).group_by(RawEvent.project).all()
        monthly_rows = session.query(RawEvent.project, func.count(RawEvent.id)).filter(RawEvent.ts >= month_start).group_by(RawEvent.project).all()
        daily_map = {p or None: int(c) for p,c in daily_rows}
        monthly_map = {p or None: int(c) for p,c in monthly_rows}
        fired = 0
        for q in quotas:
            proj = q.project
            d_used = daily_map.get(proj, 0)
            m_used = monthly_map.get(proj, 0)
            # Evaluate thresholds
            def _maybe_fire(kind: str, used: int, limit: int | None):
                nonlocal fired
                if not limit:
                    return
                ratio = used / limit if limit else 0
                for thr in THRESHOLDS:
                    if ratio >= thr and ratio < (thr + 0.05):  # fire once at crossing window
                        # Did we already log an alert for this threshold today?
                        recent = session.query(AlertLog).filter(
                            AlertLog.rule_type==f"quota_{kind}",
                            AlertLog.created_at >= day_start,
                            AlertLog.message.like(f"%project={proj}%thr={int(thr*100)}%")
                        ).first()
                        if recent:
                            continue
                        msg = f"Quota {kind} warn project={proj} used={used} limit={limit} thr={int(thr*100)}%"
                        session.add(AlertLog(rule_id=0, rule_type=f"quota_{kind}", message=msg, context={
                            "project": proj, "used": used, "limit": limit, "threshold": thr, "period": kind
                        }))
                        fired += 1
                        break
            _maybe_fire('daily', d_used, q.daily_event_limit)
            _maybe_fire('monthly', m_used, q.monthly_event_limit)
        session.commit()
        return {"status": "ok", "alerts": fired}
    finally:
        session.close()