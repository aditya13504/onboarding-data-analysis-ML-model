from celery import shared_task
from sqlalchemy.orm import Session
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.infrastructure.idempotency import idempotent
from onboarding_analyzer.models.tables import FrictionCluster, Insight, SuggestionPattern, InsightSuppression
from onboarding_analyzer.config import get_settings
from datetime import datetime, timedelta
from prometheus_client import Counter, Histogram
try:
    from onboarding_analyzer.api.main import registry as _api_registry  # type: ignore
except Exception:  # pragma: no cover
    _api_registry = None

INSIGHT_RECOMPUTE = Counter('insight_recompute_total', 'Recompute insight score runs', registry=_api_registry)
INSIGHT_SCORE_UPDATES = Counter('insight_score_updates_total', 'Insights whose score changed', registry=_api_registry)
INSIGHT_RECOMPUTE_LATENCY = Histogram('insight_recompute_latency_seconds', 'Latency to recompute insight scores', buckets=(0.05,0.1,0.25,0.5,1,2,5), registry=_api_registry)


@shared_task
@idempotent("generate_insights", ttl_hours=4)
def generate_insights(max_items: int = 5, business_value_weight: float = 1.0, recency_half_life_days: int = 14):
    session: Session = SessionLocal()
    try:
        settings = get_settings()
        now = datetime.utcnow()
        # Build suggestion pattern catalog (cache existing or seed defaults if empty)
        patterns = session.query(SuggestionPattern).all()
        if not patterns:
            default_specs = [
                ("low_completion", "Low completion ratio", "Add contextual guidance (tooltip/progress indicator) at early friction steps.", "guidance", 1.2),
                ("slow_duration", "High session duration", "Optimize performance: defer heavy assets, parallelize API calls.", "performance", 1.1),
                ("form_complexity", "Moderate completion with medium duration", "Reduce required fields and simplify forms.", "form_simplification", 1.0),
            ]
            for key, desc, rec, cat, w in default_specs:
                session.add(SuggestionPattern(key=key, description=desc, recommendation=rec, category=cat, weight=w))
            session.commit()
            patterns = session.query(SuggestionPattern).all()
        pattern_map = {p.key: p for p in patterns}
        # Active suppressions map
        suppressions = session.query(InsightSuppression).filter(InsightSuppression.active == 1).all()
        sup_cluster = {(s.cluster_id, s.pattern_key) for s in suppressions}
        # Existing recent insights for dedupe / fatigue (exclude those shipped or rejected) in last 30 days
        recent_cutoff = now - timedelta(days=30)
        recent_insights = session.query(Insight).filter(Insight.created_at >= recent_cutoff, Insight.status.notin_(["rejected"]))
        seen_cluster_category = {(i.cluster_id, i.category) for i in recent_insights}
        clusters = session.query(FrictionCluster).order_by(FrictionCluster.impact_score.desc()).all()
        candidates: list[Insight] = []
        for c in clusters:
            summary = c.features_summary or {}
            pattern_key = None
            if summary.get("avg_completion", 1) < 0.6:
                pattern_key = "low_completion"
            elif summary.get("avg_duration", 0) > 300:
                pattern_key = "slow_duration"
            else:
                pattern_key = "form_complexity"
            pat = pattern_map.get(pattern_key)
            if not pat:
                continue
            # Suppression check (cluster-level or pattern-level)
            if (c.id, pattern_key) in sup_cluster or (c.id, None) in sup_cluster or (None, pattern_key) in sup_cluster:
                continue
            # Fatigue / dedupe: skip if same cluster-category produced recently
            if (c.id, pat.category) in seen_cluster_category:
                continue
            # Business value weighting (impact_score * pattern weight * user-provided weight)
            base_impact = c.impact_score * pat.weight * business_value_weight
            # Recency decay (clusters older than 14d lose weight) using half-life
            age_days = (now - c.created_at).total_seconds() / 86400.0
            decay = 0.5 ** (age_days / recency_half_life_days)
            score = base_impact * decay
            # Confidence heuristic: normalized completion gap / duration anomaly
            completion = summary.get("avg_completion", 1.0)
            duration = summary.get("avg_duration", 0)
            confidence = 0.5
            rationale_parts = []
            if pattern_key == "low_completion":
                gap = max(0.0, 1.0 - completion)
                confidence = min(0.9, 0.6 + gap * 0.4)
                rationale_parts.append(f"Low completion {completion:.2f}")
            elif pattern_key == "slow_duration":
                confidence = min(0.85, 0.55 + min(duration/600, 1)*0.3)
                rationale_parts.append(f"High avg duration {duration:.1f}s")
            else:
                confidence = 0.55
                rationale_parts.append("Moderate friction signature")
            rationale_parts.append(f"Impact score {c.impact_score:.2f}")
            rationale_parts.append(f"Decay factor {decay:.2f}")
            rationale = "; ".join(rationale_parts)
            candidates.append(
                Insight(
                    cluster_id=c.id,
                    title=f"Friction Pattern: {c.label}",
                    recommendation=pat.recommendation,
                    priority=0,  # temporary; set after sorting
                    impact_score=c.impact_score,
                    category=pat.category,
                    confidence=confidence,
                    rationale=rationale,
                    score=score,
                )
            )
        # Sort by score and slice
        candidates.sort(key=lambda x: x.score or 0, reverse=True)
        selected = candidates[:max_items]
        for idx, ins in enumerate(selected, start=1):
            ins.priority = idx
            session.add(ins)
        session.commit()
        return {"status": "ok", "generated": len(selected), "candidates": len(candidates)}
    finally:
        session.close()


@shared_task
@idempotent("advance_insight_lifecycle", ttl_hours=2)
def advance_insight_lifecycle():
    """Automate lifecycle transitions:
    - Auto mark 'new' insights as 'accepted' if within SLA window (older than accept_sla_days) and still relevant.
    - Auto ship 'accepted' insights older than auto_ship_days.
    - Close (mark 'rejected') low-impact 'new' insights beyond 2*SLA with low impact.
    """
    settings = get_settings()
    session: Session = SessionLocal()
    try:
        now = datetime.utcnow()
        accept_cutoff = now - timedelta(days=settings.insight_accept_sla_days)
        ship_cutoff = now - timedelta(days=settings.insight_auto_ship_days)
        # Auto accept criteria: high impact_score (> median impact of current new) or priority <=3
        new_insights = session.query(Insight).filter(Insight.status == 'new').all()
        if new_insights:
            impacts = sorted(i.impact_score for i in new_insights)
            median = impacts[len(impacts)//2]
            for ins in new_insights:
                if ins.created_at < accept_cutoff and (ins.impact_score >= median or ins.priority <= 3):
                    ins.status = 'accepted'
                    if not ins.accepted_at:
                        ins.accepted_at = now
                elif ins.created_at < accept_cutoff - timedelta(days=settings.insight_accept_sla_days) and ins.impact_score < median*0.5:
                    ins.status = 'rejected'
        accepted = session.query(Insight).filter(Insight.status == 'accepted').all()
        for ins in accepted:
            if ins.accepted_at and ins.accepted_at < ship_cutoff:
                ins.status = 'shipped'
                if not ins.shipped_at:
                    ins.shipped_at = now
        session.commit()
        return {"status": "ok"}
    finally:
        session.close()


@shared_task
def recompute_insight_scores(freshness_half_life_days: int = 21):
    """Periodic score recalculation incorporating feedback and freshness.

    New score formula:
        score = impact_score * confidence * freshness_decay * feedback_factor

    feedback_factor = 1 + (up - down) / max(5, total_feedback) bounded to [0.5, 1.5].
    freshness_decay uses half-life on age beyond half_life.
    Updates priority ordering accordingly.
    """
    import time as _t
    start = _t.time()
    session: Session = SessionLocal()
    try:
        now = datetime.utcnow()
        insights = session.query(Insight).all()
        changed = 0
        # Fetch feedback aggregated per insight
        from onboarding_analyzer.models.tables import InsightFeedback
        fb_rows = session.query(InsightFeedback.insight_id, InsightFeedback.feedback).all()
        fb_up = {}
        fb_down = {}
        for iid, fb in fb_rows:
            if fb == 'up':
                fb_up[iid] = fb_up.get(iid, 0) + 1
            elif fb == 'down':
                fb_down[iid] = fb_down.get(iid, 0) + 1
        for ins in insights:
            age_days = (now - ins.created_at).total_seconds()/86400.0
            freshness_decay = 0.5 ** (max(0.0, age_days) / max(1, freshness_half_life_days))
            up = fb_up.get(ins.id, 0)
            down = fb_down.get(ins.id, 0)
            total_fb = up + down
            feedback_factor = 1.0 + (up - down)/max(5, total_fb if total_fb else 5)
            if feedback_factor < 0.5:
                feedback_factor = 0.5
            elif feedback_factor > 1.5:
                feedback_factor = 1.5
            new_score = ins.impact_score * (ins.confidence or 0.5) * freshness_decay * feedback_factor
            if ins.score is None or abs(new_score - ins.score) / max(1e-6, (ins.score or 1)) > 0.02:
                ins.score = new_score
                changed += 1
        # Reorder priorities among non-shipped insights by new score
        ranked = [i for i in insights if i.status in ('new','accepted')]
        ranked.sort(key=lambda x: x.score or 0, reverse=True)
        for idx, ins in enumerate(ranked, start=1):
            ins.priority = idx
        session.commit()
        try:
            INSIGHT_RECOMPUTE.inc()
            INSIGHT_SCORE_UPDATES.inc(changed)
            INSIGHT_RECOMPUTE_LATENCY.observe(_t.time() - start)
        except Exception:
            pass
        return {"status": "ok", "changed": changed, "insights": len(insights)}
    finally:
        session.close()
