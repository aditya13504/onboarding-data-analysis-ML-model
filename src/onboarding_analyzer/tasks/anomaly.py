from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

import redis
from celery import shared_task
from slack_sdk import WebClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from onboarding_analyzer.config import get_settings
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import FunnelMetric, RawEvent
from onboarding_analyzer.models.tables import FunnelMetric, RawEvent, AnomalyEvent
from prometheus_client import Counter, Gauge

try:
    from onboarding_analyzer.api.main import registry as _api_registry  # type: ignore
except Exception:  # pragma: no cover
    _api_registry = None

ANOMALY_VOLUME_DETECTIONS = Counter('anomaly_event_volume_detections_total', 'Detected event volume anomalies', ['project'], registry=_api_registry)
ANOMALY_LATENCY_DETECTIONS = Counter('anomaly_latency_detections_total', 'Detected ingestion latency anomalies', ['project'], registry=_api_registry)
ANOMALY_LAST_VOLUME_DELTA = Gauge('anomaly_last_volume_delta_pct', 'Last detected volume anomaly delta percent', ['project'], registry=_api_registry)
ANOMALY_LAST_LATENCY_DELTA = Gauge('anomaly_last_latency_delta_pct', 'Last detected latency anomaly delta percent', ['project'], registry=_api_registry)
from onboarding_analyzer.utils.significance import z_test_proportions


@shared_task
def detect_dropoff_anomalies(
    lookback_days: int = 7,
    z_threshold: float = 2.0,
    min_users: int = 30,
) -> Dict[str, Any]:
    """Detect statistically significant negative conversion drops in funnel steps.

    Approach:
      * For each (step, project) pair compute average conversion and total users in the recent window vs the preceding window.
      * Perform a z-test for proportions. If z <= -z_threshold (meaning a significant drop) record anomaly.
      * Severity is a composite of |z|, absolute proportional change, and relative volume.
      * Anomalies are grouped (project:step) for notification. Only the highest-severity anomaly per group is sent.
      * Adaptive suppression via Redis key with TTL that shortens for high severity and lengthens for low severity to balance noise vs. responsiveness.
    """
    session: Session = SessionLocal()
    try:
        now = datetime.utcnow()
        since = now - timedelta(days=lookback_days)
        results: List[Dict[str, Any]] = []

        # Distinct step/project combinations
        steps = session.query(FunnelMetric.step_name, FunnelMetric.project).distinct().all()
        for step_name, project in steps:
            # Recent window
            q_recent = select(
                func.avg(FunnelMetric.conversion_rate),
                func.sum(FunnelMetric.users_entered),
            ).where(
                FunnelMetric.step_name == step_name,
                FunnelMetric.calc_ts >= since,
                (FunnelMetric.project == project)
                if project is not None
                else FunnelMetric.project.is_(None),
            )
            # Previous window (same length immediately before)
            q_prev = select(
                func.avg(FunnelMetric.conversion_rate),
                func.sum(FunnelMetric.users_entered),
            ).where(
                FunnelMetric.step_name == step_name,
                FunnelMetric.calc_ts < since,
                (FunnelMetric.project == project)
                if project is not None
                else FunnelMetric.project.is_(None),
            )
            p_new, n_new = session.execute(q_recent).one()
            p_old, n_old = session.execute(q_prev).one()

            if not all(v is not None for v in (p_new, n_new, p_old, n_old)):
                continue
            if (n_new or 0) < min_users or (n_old or 0) < min_users:
                continue

            z = z_test_proportions(p_old, p_new, int(n_old), int(n_new))
            if z is None:
                continue

            change = (p_new - p_old) if p_old else 0
            severity = abs(z) * abs(change) * (((n_new or 0) + (n_old or 0)) / max(min_users, 1))

            if z <= -z_threshold and p_new < p_old:
                results.append(
                    {
                        "step": step_name,
                        "project": project,
                        "z": float(z),
                        "change": float(change),
                        "p_old": float(p_old),
                        "p_new": float(p_new),
                        "n_old": int(n_old),
                        "n_new": int(n_new),
                        "severity": float(severity),
                        "window_days": lookback_days,
                        "detected_at": now.isoformat(),
                    }
                )

        notified = False
        if results:
            settings = get_settings()
            if settings.slack_bot_token and settings.slack_report_channel:
                try:
                    r = redis.Redis.from_url(settings.redis_url)
                except Exception:
                    r = None
                try:
                    client = WebClient(token=settings.slack_bot_token)
                    # Group anomalies by project:step
                    grouped: Dict[str, List[Dict[str, Any]]] = {}
                    for row in results:
                        gkey = f"{row['project'] or 'global'}:{row['step']}"
                        grouped.setdefault(gkey, []).append(row)

                    lines: List[str] = []
                    for gkey, anomalies in grouped.items():
                        top = max(anomalies, key=lambda a: a["severity"])  # highest severity wins
                        base_key = f"anomaly:{gkey}"
                        ttl = 3600  # default 1h
                        if top["severity"] > 5:
                            ttl = 1800  # high severity -> check again sooner
                        elif top["severity"] < 1:
                            ttl = 7200  # low severity -> longer suppression

                        suppress = False
                        try:
                            if r and r.get(base_key):
                                suppress = True
                            else:
                                if r:
                                    r.setex(base_key, ttl, b"1")
                        except Exception:
                            # Non-fatal if Redis unavailable
                            pass

                        if not suppress:
                            lines.append(
                                (
                                    f"Anomaly[{gkey}]: z={top['z']:.2f} Δ={(top['change']*100):.2f}pp "
                                    f"severity={top['severity']:.2f} ({top['p_old']:.2%}->{top['p_new']:.2%})"
                                )
                            )

                    if lines:
                        client.chat_postMessage(
                            channel=settings.slack_report_channel, text="\n".join(lines)
                        )
                        notified = True
                except Exception:
                    # Swallow notification issues to keep task green
                    pass

        # Persist anomalies (dedupe by fingerprint within recent hour)
        try:
            for a in results:
                fp = f"funnel:{a['project']}:{a['step']}"
                existing = session.query(AnomalyEvent.id).filter(
                    AnomalyEvent.fingerprint==fp,
                    AnomalyEvent.anomaly_type=='funnel_dropoff',
                    AnomalyEvent.detected_at >= now - timedelta(hours=1)
                ).first()
                if not existing:
                    session.add(AnomalyEvent(
                        anomaly_type='funnel_dropoff',
                        project=a['project'],
                        fingerprint=fp,
                        severity=a['severity'],
                        delta_pct=(a['change']*100) if a.get('change') is not None else None,
                        details=a
                    ))
            session.commit()
        except Exception:
            session.rollback()
        return {"status": "ok", "anomalies": results, "notified": notified}
    finally:
        session.close()


@shared_task
def detect_event_volume_and_latency_anomalies(lookback_hours: int = 24, bucket_minutes: int = 15, z_threshold: float = 3.0, min_events: int = 100):
    """Detect anomalies in overall event volume and median ingestion lag using rolling z-score.

    For each project (including global None), build time buckets of count(events) and median (now - ts).
    Compute mean/std over lookback window; flag most recent bucket if deviation in z-scores exceeds threshold.
    """
    session: Session = SessionLocal()
    try:
        now = datetime.utcnow()
        since = now - timedelta(hours=lookback_hours)
        # Fetch events (project, ts)
        rows = session.query(RawEvent.project, RawEvent.ts).filter(RawEvent.ts >= since).all()
        if not rows:
            return {"status": "empty"}
        from collections import defaultdict
        import statistics
        bucketed_counts: dict[tuple[str|None,int], int] = defaultdict(int)
        bucketed_lags: dict[tuple[str|None,int], list] = defaultdict(list)
        for project, ts in rows:
            # bucket index
            b = int((ts - since).total_seconds() // (bucket_minutes * 60))
            lag = (now - ts).total_seconds()
            bucketed_counts[(project, b)] += 1
            bucketed_lags[(project, b)].append(lag)
        anomalies: list[dict] = []
        # group by project
        projects = {p for p,_ in bucketed_counts.keys()}
        last_bucket = int((now - since).total_seconds() // (bucket_minutes * 60))
        for proj in projects:
            counts_series = []
            latency_series = []
            for b in range(0, last_bucket+1):
                counts_series.append(bucketed_counts.get((proj,b), 0))
                lags = bucketed_lags.get((proj,b), [])
                if lags:
                    latency_series.append(statistics.median(lags))
                else:
                    latency_series.append(0)
            if len(counts_series) < 5:
                continue
            recent_count = counts_series[-1]
            if sum(counts_series) < min_events:
                continue
            mean_c = statistics.mean(counts_series[:-1])
            sd_c = statistics.pstdev(counts_series[:-1]) or 1.0
            zc = (recent_count - mean_c)/sd_c
            recent_latency = latency_series[-1]
            mean_l = statistics.mean(latency_series[:-1])
            sd_l = statistics.pstdev(latency_series[:-1]) or 1.0
            zl = (recent_latency - mean_l)/sd_l
            if abs(zc) >= z_threshold:
                delta_pct = ((recent_count - mean_c)/mean_c*100.0) if mean_c else 0.0
                anomalies.append({"type": "volume", "project": proj, "z": zc, "delta_pct": delta_pct, "recent": recent_count, "mean": mean_c})
                try:
                    ANOMALY_VOLUME_DETECTIONS.labels(project=proj or '_global').inc()
                    ANOMALY_LAST_VOLUME_DELTA.labels(project=proj or '_global').set(delta_pct)
                except Exception:
                    pass
            if abs(zl) >= z_threshold:
                delta_pct_l = ((recent_latency - mean_l)/mean_l*100.0) if mean_l else 0.0
                anomalies.append({"type": "latency", "project": proj, "z": zl, "delta_pct": delta_pct_l, "recent": recent_latency, "mean": mean_l})
                try:
                    ANOMALY_LATENCY_DETECTIONS.labels(project=proj or '_global').inc()
                    ANOMALY_LAST_LATENCY_DELTA.labels(project=proj or '_global').set(delta_pct_l)
                except Exception:
                    pass
        # Persist anomalies (volume/latency) with dedupe window 30m
        try:
            for a in anomalies:
                atype = 'volume' if a['type']=='volume' else 'latency'
                fp = f"{atype}:{a['project']}"
                existing = session.query(AnomalyEvent.id).filter(
                    AnomalyEvent.fingerprint==fp,
                    AnomalyEvent.anomaly_type==atype,
                    AnomalyEvent.detected_at >= now - timedelta(minutes=30)
                ).first()
                if not existing:
                    session.add(AnomalyEvent(
                        anomaly_type=atype,
                        project=a['project'],
                        fingerprint=fp,
                        severity=abs(a.get('z') or 0),
                        delta_pct=a.get('delta_pct'),
                        details=a
                    ))
            session.commit()
        except Exception:
            session.rollback()
        return {"status": "ok", "anomalies": anomalies}
    finally:
        session.close()
