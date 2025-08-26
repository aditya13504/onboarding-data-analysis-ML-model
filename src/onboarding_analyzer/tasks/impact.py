"""Recommendation impact attribution tasks (real data, no mocks).

Computes per-recommendation conversion lift over rolling windows using current RawEvent and
UserRecommendation tables. Conversion defined by action_event_name on recommendation rows.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from sqlalchemy import select, func
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import UserRecommendation, RawEvent, RecommendationImpact

def compute_recommendation_impact(window_hours: int = 24, lookback_windows: int = 3, project: str | None = None):
    s = SessionLocal()
    created = 0
    try:
        now = datetime.utcnow()
        for w in range(lookback_windows):
            end = now - timedelta(hours=w*window_hours)
            start = end - timedelta(hours=window_hours)
            # Distinct recommendations with action_event_name
            rec_q = s.query(UserRecommendation.recommendation, UserRecommendation.action_event_name, UserRecommendation.project).\
                filter(UserRecommendation.created_at >= start, UserRecommendation.created_at < end, UserRecommendation.action_event_name.isnot(None))
            if project is not None:
                rec_q = rec_q.filter(UserRecommendation.project == project)
            rec_rows = rec_q.distinct().all()
            for rec_key, action_event, rec_project in rec_rows:
                # Skip if impact row exists
                exists = s.query(RecommendationImpact.id).filter(
                    RecommendationImpact.recommendation==rec_key,
                    RecommendationImpact.window_start==start,
                    RecommendationImpact.window_end==end,
                    RecommendationImpact.project==rec_project
                ).first()
                if exists:
                    continue
                # Users exposed (received recommendation)
                recs_q = s.query(UserRecommendation.user_id, UserRecommendation.status, UserRecommendation.created_at).\
                    filter(UserRecommendation.recommendation==rec_key, UserRecommendation.created_at >= start, UserRecommendation.created_at < end)
                if project is not None:
                    recs_q = recs_q.filter(UserRecommendation.project==rec_project)
                recs = recs_q.all()
                exposed_users = {r.user_id for r in recs}
                if not exposed_users:
                    continue
                # For each exposed user check conversion
                conv_users = set()
                for uid in exposed_users:
                    ev_q = s.query(RawEvent.id).filter(RawEvent.user_id==uid, RawEvent.event_name==action_event, RawEvent.ts >= start, RawEvent.ts < end)
                    if project is not None:
                        ev_q = ev_q.filter(RawEvent.project==rec_project)
                    ev = ev_q.first()
                    if ev:
                        conv_users.add(uid)
                # Baseline: users with event but not exposed in window
                baseline_q = s.query(RawEvent.user_id).filter(RawEvent.event_name==action_event, RawEvent.ts >= start, RawEvent.ts < end)
                if project is not None:
                    baseline_q = baseline_q.filter(RawEvent.project==rec_project)
                baseline_rows = baseline_q.distinct().all()
                baseline_candidates = {u for (u,) in baseline_rows if u not in exposed_users}
                baseline_converted = len(baseline_candidates)  # All baseline candidates converted by definition (performed action)
                users_exposed = len(exposed_users)
                users_converted = len(conv_users)
                baseline_users = len(baseline_candidates)
                conversion_rate = users_converted / users_exposed if users_exposed else 0.0
                baseline_rate = baseline_converted / baseline_users if baseline_users else 0.0
                lift = (conversion_rate - baseline_rate)
                impact = RecommendationImpact(
                    recommendation=rec_key,
                    project=rec_project,
                    window_start=start,
                    window_end=end,
                    users_exposed=users_exposed,
                    users_converted=users_converted,
                    baseline_users=baseline_users,
                    baseline_converted=baseline_converted,
                    conversion_rate=conversion_rate,
                    baseline_rate=baseline_rate,
                    lift=lift,
                )
                s.add(impact)
                created += 1
        if created:
            s.commit()
        return {"status": "ok", "created": created}
    finally:
        s.close()

__all__ = ["compute_recommendation_impact"]