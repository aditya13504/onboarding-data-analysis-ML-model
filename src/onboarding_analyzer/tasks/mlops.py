from __future__ import annotations
from celery import shared_task
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import (
    RawEvent, FeatureDefinition, FeatureView, ModelDriftMetric, ModelDriftThreshold, DriftRetrainAudit, ModelVersion
)
from onboarding_analyzer.config import get_settings
from prometheus_client import Counter, Histogram

try:
    from onboarding_analyzer.api.main import registry as _api_registry  # type: ignore
except Exception:  # pragma: no cover
    _api_registry = None

FEATURE_MAT_RUNS = Counter('feature_materialization_runs_total', 'Feature materialization task runs', registry=_api_registry)
FEATURE_VIEWS_WRITTEN = Counter('feature_views_written_total', 'Feature view rows written', registry=_api_registry)
MODEL_DRIFT_RUNS = Counter('model_drift_runs_total', 'Model drift evaluation runs', registry=_api_registry)
MODEL_DRIFT_ALERTS = Counter('model_drift_alerts_total', 'Model drift alerts raised', registry=_api_registry)
MODEL_DRIFT_LATENCY = Histogram('model_drift_latency_seconds', 'Latency of model drift evaluation', buckets=(0.05,0.1,0.25,0.5,1,2,5), registry=_api_registry)
DRIFT_THRESHOLD_EVALS = Counter('model_drift_threshold_evals_total', 'Drift threshold evaluations', registry=_api_registry)
DRIFT_ACTIONS = Counter('model_drift_actions_total', 'Drift actions taken', ['action'], registry=_api_registry)


def _parse_days(expr: str, prefix: str) -> int | None:
    # pattern prefix_{n}d
    if not expr.startswith(prefix):
        return None
    try:
        part = expr[len(prefix):]
        if part.endswith('d'):
            return int(part[:-1])
    except Exception:
        return None
    return None


@shared_task
def materialize_feature_views(now: str | None = None):
    """Materialize active feature definitions into FeatureView.

    Supported expr patterns (simple deterministic logic):
      - count_events_{N}d : total events per user in last N days
      - unique_events_{N}d : unique event_name count per user in last N days
    """
    session: Session = SessionLocal()
    try:
        active = session.query(FeatureDefinition).filter(FeatureDefinition.status == 'active').all()
        if not active:
            return {"status": "no_defs"}
        now_dt = datetime.utcnow()
        user_feature_cache: dict[tuple[str|None,str], dict] = {}
        written = 0
        for fd in active:
            expr = fd.expr.strip().lower()
            # Determine window size
            days = _parse_days(expr, 'count_events_') or _parse_days(expr, 'unique_events_') or 30
            window_start = now_dt - timedelta(days=days)
            if expr.startswith('count_events_'):
                # total events per entity (user)
                q = (
                    select(RawEvent.user_id, func.count(RawEvent.id))
                    .where(RawEvent.ts >= window_start)
                    .group_by(RawEvent.user_id)
                )
                for user_id, cnt in session.execute(q):
                    fv = session.query(FeatureView).filter_by(feature_key=fd.feature_key, entity_id=user_id, feature_version=fd.version).first()
                    val = {"value": int(cnt), "window_days": days}
                    if fv:
                        fv.value = val
                        fv.computed_at = now_dt
                    else:
                        session.add(FeatureView(feature_key=fd.feature_key, entity_id=user_id, value=val, feature_version=fd.version))
                        written += 1
            elif expr.startswith('unique_events_'):
                q = (
                    select(RawEvent.user_id, func.count(func.distinct(RawEvent.event_name)))
                    .where(RawEvent.ts >= window_start)
                    .group_by(RawEvent.user_id)
                )
                for user_id, uniq in session.execute(q):
                    fv = session.query(FeatureView).filter_by(feature_key=fd.feature_key, entity_id=user_id, feature_version=fd.version).first()
                    val = {"value": int(uniq), "window_days": days}
                    if fv:
                        fv.value = val
                        fv.computed_at = now_dt
                    else:
                        session.add(FeatureView(feature_key=fd.feature_key, entity_id=user_id, value=val, feature_version=fd.version))
                        written += 1
            else:
                # unsupported expression currently
                continue
        session.commit()
        try:
            FEATURE_MAT_RUNS.inc()
            FEATURE_VIEWS_WRITTEN.inc(written)
        except Exception:
            pass
        return {"status": "ok", "feature_defs": len(active), "rows_written": written}
    finally:
        session.close()


def _psi(expected: list[float], actual: list[float], buckets: int = 10) -> float:
    if not expected or not actual:
        return 0.0
    import math
    all_scores = sorted(expected + actual)
    if len(set(all_scores)) == 1:
        return 0.0
    # define bucket breakpoints evenly over sorted combined scores
    bucket_edges = []
    for i in range(1, buckets):
        idx = int(i / buckets * (len(all_scores) - 1))
        bucket_edges.append(all_scores[idx])
    bucket_edges = sorted(set(bucket_edges))
    def _dist(scores):
        counts = []
        prev = None
        total = len(scores)
        remaining = list(bucket_edges)
        bins = [0]*(len(bucket_edges)+1)
        for s in scores:
            placed = False
            for i, edge in enumerate(bucket_edges):
                if s <= edge:
                    bins[i] += 1
                    placed = True
                    break
            if not placed:
                bins[-1] += 1
        return [c/total for c in bins]
    exp_dist = _dist(expected)
    act_dist = _dist(actual)
    psi = 0.0
    for e, a in zip(exp_dist, act_dist):
        if e == 0 or a == 0:
            continue
        psi += (a - e) * (0 if a == 0 or e == 0 else __import__('math').log(a / e))
    return float(abs(psi))


@shared_task
def evaluate_model_drift():
    """Evaluate drift across multiple feature signals.

    Metrics:
      - PSI on inter-arrival time distribution
      - KS statistic on event timestamp hour-of-day distribution
    Additional per-feature PSI (for numeric props referenced in FeatureDefinition expressions as simple props.key).
    """
    settings = get_settings()  # thresholds may be defined per ModelDriftThreshold rows
    session: Session = SessionLocal()
    try:
        q = session.query(RawEvent.ts, RawEvent.props).order_by(RawEvent.ts.desc()).limit(8000).all()
        if len(q) < 100:
            return {"status": "insufficient_data"}
        times = [row[0] for row in q]
        times.sort()
        deltas = [(t2 - t1).total_seconds() for t1, t2 in zip(times[:-1], times[1:])]
        split = len(deltas)//2
        old, new = deltas[:split], deltas[split:]
        psi = _psi(old, new)
        session.add(ModelDriftMetric(model_name="ingest_stream", metric_name="psi_interarrival", metric_value=float(psi)))
        # Hour-of-day KS
        import math
        hours = [t.hour for t in times]
        h_split = len(hours)//2
        old_h, new_h = hours[:h_split], hours[h_split:]
        def cdf(vals):
            from collections import Counter
            c = Counter(vals)
            total = sum(c.values()) or 1
            acc = 0
            out = []
            for h in range(24):
                acc += c.get(h,0)
                out.append(acc/total)
            return out
        c_old, c_new = cdf(old_h), cdf(new_h)
        ks = max(abs(a-b) for a,b in zip(c_old, c_new))
        session.add(ModelDriftMetric(model_name="ingest_stream", metric_name="ks_hour_of_day", metric_value=float(ks)))
        # Per-feature PSI for numeric props appearing in simple feature definitions
        feature_props: set[str] = set()
        fdefs = session.query(FeatureDefinition).filter(FeatureDefinition.status=='active').limit(200).all()
        for fd in fdefs:
            expr = fd.expr.strip()
            # heuristic: expressions of form props['key'] or props.key
            import re
            m = re.findall(r"props\[['\"](\w+)['\"]\]|props\.(\w+)", expr)
            for a,b in m:
                feature_props.add(a or b)
        if feature_props:
            numeric_series: dict[str, list[float]] = {k: [] for k in feature_props}
            for _, props in q:
                if not isinstance(props, dict):
                    continue
                for fp in feature_props:
                    v = props.get(fp)
                    if isinstance(v, (int,float)):
                        numeric_series[fp].append(float(v))
            for fp, series in numeric_series.items():
                if len(series) < 40:
                    continue
                series.sort()
                mid = len(series)//2
                s_old, s_new = series[:mid], series[mid:]
                psi_feat = _psi(s_old, s_new)
                session.add(ModelDriftMetric(model_name="feature", metric_name=f"psi_{fp}", metric_value=float(psi_feat)))
        session.commit()
        return {"status": "ok", "psi": psi, "ks": ks}
    finally:
        session.close()


@shared_task
def evaluate_drift_thresholds():
    """Check latest drift metrics against configured thresholds and trigger actions.

    Actions:
      - retrain: invoke orchestrate_retraining(model_name)
      - alert: create DriftRetrainAudit row (placeholder for external notification)
    """
    session: Session = SessionLocal()
    from datetime import timedelta as _td
    try:
        thresholds = session.query(ModelDriftThreshold).filter(ModelDriftThreshold.active==1).all()
        if not thresholds:
            return {"status": "no_thresholds"}
        triggered = 0
        for th in thresholds:
            # enforce cooldown
            if th.last_triggered_at and (datetime.utcnow() - th.last_triggered_at) < _td(hours=th.cooldown_hours):
                continue
            latest = session.query(ModelDriftMetric).filter(ModelDriftMetric.model_name==th.model_name, ModelDriftMetric.metric_name==th.metric_name).order_by(ModelDriftMetric.captured_at.desc()).first()
            if not latest:
                continue
            mv = latest.metric_value
            comp_ok = (
                (th.comparison == 'gt' and mv > th.boundary) or
                (th.comparison == 'ge' and mv >= th.boundary) or
                (th.comparison == 'lt' and mv < th.boundary) or
                (th.comparison == 'le' and mv <= th.boundary)
            )
            if not comp_ok:
                continue
            # Action
            if th.action == 'retrain':
                from onboarding_analyzer.tasks.mlops import orchestrate_retraining
                orchestrate_retraining(th.model_name)
                session.add(DriftRetrainAudit(model_name=th.model_name, metric_name=th.metric_name, metric_value=mv, action='retrain', notes=f"threshold {th.comparison} {th.boundary}"))
                DRIFT_ACTIONS.labels(action='retrain').inc()
            else:
                session.add(DriftRetrainAudit(model_name=th.model_name, metric_name=th.metric_name, metric_value=mv, action='alert', notes=f"threshold {th.comparison} {th.boundary}"))
                DRIFT_ACTIONS.labels(action='alert').inc()
            th.last_triggered_at = datetime.utcnow()
            triggered += 1
        session.commit()
        DRIFT_THRESHOLD_EVALS.inc()
        return {"status": "ok", "triggered": triggered, "thresholds": len(thresholds)}
    finally:
        session.close()
import hashlib
import json
import math
from sqlalchemy import select
from onboarding_analyzer.models.tables import (
    ModelTrainingRun, ModelArtifact, ModelVersion, ModelDriftMetric, DatasetSnapshot, ModelPromotionAudit, FrictionCluster
)


def _hash_artifact(obj: dict) -> str:
    try:
        payload = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(payload).hexdigest()
    except Exception:
        return "unknown"


@shared_task
def register_training_run(model_name: str, params: dict | None = None, metrics: dict | None = None, artifact: dict | None = None, data_sample_limit: int = 5000):
    """Create a ModelVersion + ModelTrainingRun + optional ModelArtifact.

    Designed for lightweight reproducibility: we store params, metrics, data fingerprint (hash of first N event_ids) and artifact hash.
    """
    session: Session = SessionLocal()
    try:
        version = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        # Data fingerprint: hash of concatenated sorted event_ids (sample)
        ids = [row[0] for row in session.execute(select(RawEvent.event_id).order_by(RawEvent.id).limit(data_sample_limit))]
        fp = hashlib.sha256("|".join(ids).encode()).hexdigest() if ids else None
        session.add(ModelVersion(model_name=model_name, version=version, notes=None, active=1))
        run = ModelTrainingRun(model_name=model_name, version=version, status="completed", params=params, metrics=metrics, data_fingerprint=fp, training_rows=len(ids))
        session.add(run)
        if artifact:
            h = _hash_artifact(artifact)
            session.add(ModelArtifact(model_name=model_name, version=version, artifact=artifact, artifact_hash=h))
        session.commit()
        return {"status": "ok", "version": version}
    finally:
        session.close()


@shared_task
def record_drift_metric(model_name: str, reference_window_hours: int = 72, recent_window_hours: int = 24, buckets: int = 10):
    """Compute simple PSI drift metric on inter-event durations as proxy for model input stability."""
    session: Session = SessionLocal()
    try:
        from datetime import timedelta
        now = datetime.utcnow()
        ref_since = now - timedelta(hours=reference_window_hours + recent_window_hours)
        rows = [row[0] for row in session.execute(select(RawEvent.ts).where(RawEvent.ts >= ref_since).order_by(RawEvent.ts))]
        if len(rows) < 50:
            return {"status": "skipped", "reason": "low_data"}
        # Split reference vs recent
        split_point = -sum(1 for t in rows if (now - t).total_seconds() / 3600.0 <= recent_window_hours)
        if split_point <= 0:
            return {"status": "skipped", "reason": "window_split"}
        ref = rows[:split_point]
        recent = rows[split_point:]
        def inter_arrivals(ts_list):
            return [(t2 - t1).total_seconds() for t1, t2 in zip(ts_list[:-1], ts_list[1:])]
        ref_deltas = inter_arrivals(ref)
        recent_deltas = inter_arrivals(recent)
        if len(ref_deltas) < 10 or len(recent_deltas) < 10:
            return {"status": "skipped", "reason": "insufficient_deltas"}
        mn = min(min(ref_deltas), min(recent_deltas))
        mx = max(max(ref_deltas), max(recent_deltas))
        if mn == mx:
            psi = 0.0
        else:
            width = (mx - mn) / buckets
            def hist(vals):
                h = [0]*buckets
                for v in vals:
                    idx = min(int((v - mn)/width), buckets-1)
                    h[idx]+=1
                total = sum(h) or 1
                return [x/total for x in h]
            pa = hist(ref_deltas)
            pb = hist(recent_deltas)
            psi = 0.0
            for a,b in zip(pa,pb):
                if a>0 and b>0:
                    psi += (a-b)*math.log(a/b)
        session.add(ModelDriftMetric(model_name=model_name, metric_name="psi_interarrival", metric_value=float(psi), window=f"{reference_window_hours}h_ref/{recent_window_hours}h_recent", details=None))
        session.commit()
        return {"status": "ok", "psi": psi}
    finally:
        session.close()


@shared_task
def record_cluster_distribution_drift(model_name: str = "clustering", hours: int = 24):
    """Track basic cluster size distribution shift vs previous snapshot using L1 distance."""
    session: Session = SessionLocal()
    try:
        now = datetime.utcnow()
        since = now.timestamp() - hours*3600
        clusters = session.query(FrictionCluster).all()
        if not clusters:
            return {"status": "skipped"}
        sizes = [c.size for c in clusters]
        total = sum(sizes) or 1
        dist = [s/total for s in sizes]
        # previous metric
        prev = session.query(ModelDriftMetric).filter(ModelDriftMetric.model_name==model_name, ModelDriftMetric.metric_name=='cluster_l1').order_by(ModelDriftMetric.captured_at.desc()).first()
        l1 = None
        if prev and prev.details and 'dist' in prev.details:
            pd = prev.details['dist']
            l1 = sum(abs(a - b) for a,b in zip(dist, pd))
        session.add(ModelDriftMetric(model_name=model_name, metric_name='cluster_l1', metric_value=float(l1 or 0.0), window=f"{hours}h", details={'dist': dist}))
        session.commit()
        return {"status": "ok", "l1": l1}
    finally:
        session.close()


@shared_task
def promote_model(model_name: str, to_version: str, reason: str | None = None, author: str | None = None):
    session: Session = SessionLocal()
    try:
        cur = session.query(ModelVersion).filter(ModelVersion.model_name==model_name, ModelVersion.promoted==1).first()
        tgt = session.query(ModelVersion).filter(ModelVersion.model_name==model_name, ModelVersion.version==to_version).first()
        if not tgt:
            return {"status": "error", "error": "target_not_found"}
        if cur and cur.version == tgt.version:
            return {"status": "noop"}
        if cur:
            cur.promoted = 0
        tgt.promoted = 1
        session.add(ModelPromotionAudit(model_name=model_name, from_version=cur.version if cur else None, to_version=to_version, action='promote', reason=reason, author=author))
        session.commit()
        return {"status": "ok", "promoted": to_version}
    finally:
        session.close()


@shared_task
def rollback_model(model_name: str, reason: str | None = None, author: str | None = None):
    session: Session = SessionLocal()
    try:
        cur = session.query(ModelVersion).filter(ModelVersion.model_name==model_name, ModelVersion.promoted==1).first()
        if not cur:
            return {"status": "error", "error": "no_current"}
        prev = session.query(ModelPromotionAudit).filter(ModelPromotionAudit.model_name==model_name, ModelPromotionAudit.action=='promote').order_by(ModelPromotionAudit.created_at.desc()).offset(1).first()
        if not prev or not prev.to_version:
            return {"status": "error", "error": "no_previous"}
        target = session.query(ModelVersion).filter(ModelVersion.model_name==model_name, ModelVersion.version==prev.to_version).first()
        if not target:
            return {"status": "error", "error": "missing_target_version"}
        cur.promoted = 0
        target.promoted = 1
        session.add(ModelPromotionAudit(model_name=model_name, from_version=cur.version, to_version=target.version, action='rollback', reason=reason, author=author))
        session.commit()
        return {"status": "ok", "rolled_back_to": target.version}
    finally:
        session.close()


@shared_task
def orchestrate_retraining(model_name: str, max_age_days: int = 30, drift_metric: str = 'psi_interarrival', drift_threshold: float = 0.2):
    session: Session = SessionLocal()
    try:
        latest = session.query(ModelVersion).filter(ModelVersion.model_name==model_name).order_by(ModelVersion.created_at.desc()).first()
        age_ok = True
        if latest and (datetime.utcnow() - latest.created_at).days < max_age_days:
            age_ok = False
        # evaluate recent drift
        recent_drift = session.query(ModelDriftMetric).filter(ModelDriftMetric.model_name==model_name, ModelDriftMetric.metric_name==drift_metric).order_by(ModelDriftMetric.captured_at.desc()).limit(5).all()
        drift_exceeds = False
        if recent_drift:
            avg = sum(m.metric_value for m in recent_drift)/len(recent_drift)
            if avg >= drift_threshold:
                drift_exceeds = True
        if not age_ok and not drift_exceeds:
            return {"status": "fresh"}
        params = {"auto": True, "reason": "age" if age_ok else "drift"}
        metrics = {"triggered_by": 'drift' if drift_exceeds else 'age'}
        from onboarding_analyzer.tasks.mlops import register_training_run
        register_training_run(model_name, params=params, metrics=metrics, artifact={"note": "auto retrain", "trigger": metrics['triggered_by']})
        return {"status": "triggered", "trigger": metrics['triggered_by']}
    finally:
        session.close()


@shared_task
def stale_model_alert(model_name: str, stale_days: int = 45):
    session: Session = SessionLocal()
    try:
        latest = session.query(ModelVersion).filter(ModelVersion.model_name==model_name, ModelVersion.promoted==1).order_by(ModelVersion.created_at.desc()).first()
        if not latest:
            return {"status": "skipped"}
        age = (datetime.utcnow() - latest.created_at).days
        if age < stale_days:
            return {"status": "ok", "age": age}
        settings = get_settings()
        if settings.slack_bot_token and settings.slack_report_channel:
            try:
                from slack_sdk import WebClient
                WebClient(token=settings.slack_bot_token).chat_postMessage(channel=settings.slack_report_channel, text=f"Model {model_name} promoted version {latest.version} is stale ({age}d)")
            except Exception:
                pass
        return {"status": "stale", "age": age}
    finally:
        session.close()


@shared_task
def model_lineage_graph(model_name: str):
    session: Session = SessionLocal()
    try:
        versions = session.query(ModelVersion).filter(ModelVersion.model_name==model_name).order_by(ModelVersion.created_at).all()
        runs = session.query(ModelTrainingRun).filter(ModelTrainingRun.model_name==model_name).all()
        artifacts = session.query(ModelArtifact).filter(ModelArtifact.model_name==model_name).all()
        snaps = session.query(DatasetSnapshot).all()
        graph = {
            "nodes": [],
            "edges": []
        }
        for v in versions:
            graph["nodes"].append({"id": f"version:{v.version}", "type": "version", "promoted": bool(v.promoted)})
        for r in runs:
            graph["nodes"].append({"id": f"run:{r.id}", "type": "run", "version": r.version})
            graph["edges"].append({"from": f"run:{r.id}", "to": f"version:{r.version}"})
            if r.data_fingerprint:
                snap = next((s for s in snaps if s.fingerprint == r.data_fingerprint), None)
                if snap:
                    graph["edges"].append({"from": f"snapshot:{snap.snapshot_key}", "to": f"run:{r.id}"})
        for a in artifacts:
            graph["nodes"].append({"id": f"artifact:{a.version}", "type": "artifact"})
            graph["edges"].append({"from": f"version:{a.version}", "to": f"artifact:{a.version}"})
        for s in snaps:
            graph["nodes"].append({"id": f"snapshot:{s.snapshot_key}", "type": "snapshot"})
        return graph
    finally:
        session.close()
