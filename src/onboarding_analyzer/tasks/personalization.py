from __future__ import annotations
"""Personalization phase: contextual, real-time action recommendations.

Implements rule-driven next-best-action engine using real event data (no mocks):
 - Derives user recent activity vector (counts per event in last N days).
 - Applies deterministic rules (e.g., if user viewed step but not completed funnel step -> recommend).
 - Persists recommendations with TTL-like refresh semantics.

Future extension: integrate model-based ranking (bandits / collaborative filtering).
"""
from celery import shared_task
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent, FeatureView, UserRecommendation, PersonalizationRule
from onboarding_analyzer.expression import evaluate_condition_expr
from prometheus_client import Counter, Histogram

try:
    from onboarding_analyzer.api.main import registry as _api_registry  # type: ignore
except Exception:  # pragma: no cover
    _api_registry = None

PERSONALIZATION_RUNS = Counter('personalization_runs_total', 'Personalization engine executions', registry=_api_registry)
PERSONALIZED_RECS = Counter('personalization_recommendations_total', 'Recommendation rows produced', registry=_api_registry)
PERSONALIZATION_LAT = Histogram('personalization_latency_seconds', 'Personalization run latency', buckets=(0.05,0.1,0.25,0.5,1,2,5), registry=_api_registry)

# Lightweight table creation (only if not already defined) - using direct SQL to avoid new migration for demo
_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS user_recommendations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT NOT NULL,
  project TEXT,
  recommendation TEXT NOT NULL,
  rationale TEXT,
  created_at DATETIME NOT NULL,
  expires_at DATETIME,
  UNIQUE(user_id, recommendation)
);
"""


def _ensure_table(session: Session):
    try:
        session.execute(_CREATE_SQL)
        session.commit()
    except Exception:
        session.rollback()


@shared_task
def compute_personalized_recommendations(lookback_days: int = 7, max_users: int = 500):
    """Evaluate active PersonalizationRule rows and create recommendations.

    No mock data: derives real user event counts & feature values.
    """
    import time as _t
    start = _t.time()
    session: Session = SessionLocal()
    try:
        _ensure_table(session)  # legacy safety (will be present via migration)
        rules = session.query(PersonalizationRule).filter(PersonalizationRule.active==1).all()
        if not rules:
            return {"status": "no_rules"}
        now = datetime.utcnow()
        since = now - timedelta(days=lookback_days)
        users = session.execute(
            select(RawEvent.user_id, func.count(RawEvent.id))
            .where(RawEvent.ts >= since)
            .group_by(RawEvent.user_id)
            .order_by(func.count(RawEvent.id).desc())
            .limit(max_users)
        ).all()
        if not users:
            return {"status": "no_data"}
        produced = 0
        # Preload per-user event counts map to speed rule eval
        # Build per-user event histogram
        ev_counts: dict[str, dict[str,int]] = {}
        for uid,_ in users:
            rows = session.execute(
                select(RawEvent.event_name, func.count(RawEvent.id))
                .where(RawEvent.user_id==uid, RawEvent.ts >= since)
                .group_by(RawEvent.event_name)
            ).all()
            ev_counts[uid] = {n:int(c) for n,c in rows}
        # Helper caches
        feature_cache: dict[tuple[str,str], float] = {}
        def feature_value(uid: str, fkey: str) -> float:
            key = (uid,fkey)
            if key in feature_cache:
                return feature_cache[key]
            fv = session.query(FeatureView).filter_by(feature_key=fkey, entity_id=uid).order_by(FeatureView.computed_at.desc()).first()
            val = 0.0
            if fv and isinstance(fv.value, dict):
                v = fv.value.get('value')
                if isinstance(v,(int,float)):
                    val = float(v)
            feature_cache[key] = val
            return val
    # Use safe expression evaluator from expression module
        for rule in rules:
            # Evaluate over each user (could be optimized with segment pre-filtering later)
            for uid,_cnt in users:
                # Cooldown enforcement
                if rule.last_triggered_at and (now - rule.last_triggered_at) < timedelta(hours=rule.cooldown_hours):
                    continue
                counts = ev_counts.get(uid, {})
                def _count(ev_name: str) -> int:
                    return counts.get(ev_name, 0)
                def _feature(fkey: str) -> float:
                    return feature_value(uid, fkey)
                helpers = {"count": _count, "feature": _feature}
                if evaluate_condition_expr(rule.condition_expr, helpers):
                    rationale = rule.rationale_template or f"Triggered rule {rule.key}"
                    session.execute(
                        "INSERT OR IGNORE INTO user_recommendations (user_id, project, recommendation, rationale, created_at, expires_at) VALUES (:u, :p, :r, :ra, :c, :e)",
                        {"u": uid, "p": rule.project, "r": rule.recommendation, "ra": rationale, "c": now, "e": now + timedelta(days=3)},
                    )
                    produced += 1
            rule.last_triggered_at = now
        session.commit()
        try:
            PERSONALIZATION_RUNS.inc()
            PERSONALIZED_RECS.inc(produced)
            PERSONALIZATION_LAT.observe(_t.time() - start)
        except Exception:
            pass
        return {"status": "ok", "recommendations": produced, "users": len(users), "rules": len(rules)}
    finally:
        session.close()


@shared_task
def update_recommendation_conversions(window_minutes: int = 60):
    """Mark recommendations as converted if their action_event_name occurred after creation.

    This scans recent events and joins to open/accepted recommendations with an action_event_name.
    """
    session: Session = SessionLocal()
    try:
        since = datetime.utcnow() - timedelta(minutes=window_minutes)
        # fetch candidate recs
        recs = session.query(UserRecommendation).filter(UserRecommendation.status.in_(['new','accepted']), UserRecommendation.action_event_name.isnot(None)).all()
        if not recs:
            return {"status": "none"}
        converted = 0
        for rec in recs:
            ev = session.query(RawEvent.id).filter(RawEvent.user_id==rec.user_id, RawEvent.event_name==rec.action_event_name, RawEvent.ts >= rec.created_at).first()
            if ev:
                rec.status = 'converted'
                rec.acted_at = datetime.utcnow()
                converted += 1
        session.commit()
        return {"status": "ok", "converted": converted}
    finally:
        session.close()
