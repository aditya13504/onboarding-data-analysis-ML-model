from __future__ import annotations
from celery import shared_task
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import (
    RawEvent, DatasetSnapshot, ObservedProperty, DataQualitySnapshot, DataQualityThreshold, DataQualityAlert,
    RetentionPolicy, EventSchema, ObservedEventProperty, SchemaDriftAlert, ConnectorState
)
import hashlib
from onboarding_analyzer.config import get_settings
from slack_sdk import WebClient
from prometheus_client import Counter, Gauge, Histogram

# -------------------- Metrics --------------------
DQ_THRESHOLD_BREACHES = Counter(
    "dq_threshold_breaches_total",
    "Total threshold breach evaluations that produced (or updated) an alert",
    ["metric_type", "severity", "project"]
)
DQ_AUTO_REMEDIATIONS = Counter(
    "dq_auto_remediations_total",
    "Total auto-remediation actions executed for data quality alerts",
    ["metric_type", "action", "project"]
)
DQ_ESCALATIONS = Counter(
    "dq_escalations_total",
    "Total alert severity escalations (warn->critical)",
    ["metric_type", "project"]
)
DQ_ACTIVE_ALERTS = Gauge(
    "dq_active_alerts",
    "Current count of open data quality alerts",
    ["project", "severity"]
)
DQ_EVALUATION_DURATION = Histogram(
    "dq_evaluation_duration_seconds",
    "Duration of evaluate_data_quality_thresholds task in seconds"
)

def _fingerprint_events(session: Session, limit: int = 10000) -> tuple[str, int, int]:
    rows = session.execute(select(RawEvent.event_id, RawEvent.user_id).order_by(RawEvent.id).limit(limit))
    ids = []
    users = set()
    for eid, uid in rows:
        ids.append(eid)
        users.add(uid)
    fp = hashlib.sha256("|".join(ids).encode()).hexdigest() if ids else "empty"
    total = session.query(RawEvent.id).count()
    user_total = session.query(func.count(func.distinct(RawEvent.user_id))).scalar() or 0
    return fp, total, user_total

@shared_task
def create_dataset_snapshot(snapshot_key: str, limit: int = 10000):
    # Using explicit try/finally instead of context manager due to earlier indentation issues
    session: Session = SessionLocal()
    try:
        fp, total, user_total = _fingerprint_events(session, limit)
        if session.query(DatasetSnapshot).filter_by(snapshot_key=snapshot_key).first():
            return {"status": "exists"}
        ds = DatasetSnapshot(
            snapshot_key=snapshot_key,
            event_count=total,
            user_count=user_total,
            fingerprint=fp,
            meta={"sample_limit": limit},
        )
        session.add(ds)
        session.commit()
        return {"status": "ok", "fingerprint": fp}
    finally:
        session.close()

TYPE_PRIORITY = ["int","float","bool","string","object","array"]

def _merge_type(state: dict, new_type: str):
    if not state.get("inferred"):
        state["inferred"] = new_type
        state["confidence"] = 1.0
    else:
        if state["inferred"] == new_type:
            state["confidence"] = min(1.0, state["confidence"] + 0.05)
        else:
            # conflict: prefer earlier in priority list
            cur_idx = TYPE_PRIORITY.index(state["inferred"]) if state["inferred"] in TYPE_PRIORITY else 999
            new_idx = TYPE_PRIORITY.index(new_type) if new_type in TYPE_PRIORITY else 999
            if new_idx < cur_idx:
                state["inferred"] = new_type
            state["confidence"] = max(0.1, state["confidence"] - 0.1)
            state["conflicts"] = state.get("conflicts",0) + 1

def _value_type(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int) and not isinstance(v, bool):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, (list, tuple)):
        return "array"
    if isinstance(v, dict):
        return "object"
    return "string"

@shared_task
def infer_observed_properties(limit_events: int = 5000):
    session: Session = SessionLocal()
    try:
        q = session.query(RawEvent).order_by(RawEvent.id.desc()).limit(limit_events)
        state: dict[tuple[str,str,str|None], dict] = {}
        for ev in q:
            props = ev.props or {}
            for k,v in list(props.items())[:50]:
                vt = _value_type(v)
                key = (ev.event_name, k, ev.project)
                st = state.setdefault(key, {"inferred": None, "confidence": 0.0, "samples": 0})
                _merge_type(st, vt)
                st["samples"] += 1
        for (event_name, prop_name, project), st in state.items():
            row = session.query(ObservedProperty).filter_by(event_name=event_name, prop_name=prop_name, project=project).first()
            if not row:
                row = ObservedProperty(event_name=event_name, prop_name=prop_name, inferred_type=st["inferred"], confidence=st["confidence"], samples=st["samples"], conflicts=st.get("conflicts",0))
                session.add(row)
            else:
                row.inferred_type = st["inferred"]
                row.confidence = st["confidence"]
                row.samples = st["samples"]
                row.conflicts = st.get("conflicts",0)
                row.last_seen = datetime.utcnow()
        session.commit()
        return {"status": "ok", "properties": len(state)}
    finally:
        session.close()

def _correlate_connector_errors(session: Session, project: str | None) -> list[dict]:
    """Return recent connector error state summaries for context in alert details."""
    q = session.query(ConnectorState).order_by(ConnectorState.updated_at.desc()).limit(20)
    if project:
        q = q.filter(ConnectorState.project == project)
    out = []
    now = datetime.utcnow()
    for cs in q:
        if cs.last_error and cs.updated_at and (now - cs.updated_at) <= timedelta(hours=6):
            out.append({
                "connector": cs.connector_name if hasattr(cs, "connector_name") else getattr(cs, "name", "unknown"),
                "last_error": cs.last_error,
                "failure_count": cs.failure_count,
                "updated_at": cs.updated_at.isoformat()
            })
    return out


def _auto_remediate(session: Session, metric_type: str, project: str | None, event_name: str | None, severity: str) -> dict | None:
    """Apply limited, safe auto-remediation actions.

    Implemented actions:
      - pii_violation_rate (critical): redact detected PII-like strings in recent events
      - duplicate_rate (critical): remove later duplicates retaining first occurrence
    Returns action summary or None if no action.
    """
    if severity != "critical":
        return None
    since = datetime.utcnow() - timedelta(hours=24)
    base = session.query(RawEvent).filter(RawEvent.ts >= since)
    if project:
        base = base.filter(RawEvent.project == project)
    if event_name:
        base = base.filter(RawEvent.event_name == event_name)
    limit = 5000  # safety cap
    if metric_type == "pii_violation_rate":
        settings = get_settings()
        pii_fields = {f.lower() for f in (settings.pii_fields.split(',') if settings.pii_fields else [])}
        import re
        email_re = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        phone_re = re.compile(r"^[+]?\d{7,15}$")
        redacted = 0
        rows = base.order_by(RawEvent.id.desc()).limit(limit)
        for ev in rows:
            changed = False
            props = ev.props or {}
            for k, v in list(props.items()):
                if k.lower() in pii_fields:
                    continue
                if isinstance(v, str) and (email_re.match(v) or phone_re.match(v)):
                    props[k] = "REDACTED"
                    redacted += 1
                    changed = True
            if changed:
                ev.props = props
        if redacted:
            return {"action": "redact_pii", "items": redacted}
        return None
    if metric_type == "duplicate_rate":
        # collect duplicates by event_id keeping earliest id
        seen = {}
        dup_ids = []
        for ev in base.order_by(RawEvent.id.asc()).limit(limit):
            if ev.event_id in seen:
                dup_ids.append(ev.id)
            else:
                seen[ev.event_id] = ev.id
        if dup_ids:
            # delete duplicates
            session.query(RawEvent).filter(RawEvent.id.in_(dup_ids)).delete(synchronize_session=False)
            return {"action": "dedupe_events", "removed": len(dup_ids)}
        return None
    return None


@shared_task
def evaluate_data_quality_thresholds(hours: int = 24, max_escalation_occurrences: int = 3):
    start_time = datetime.utcnow()
    session: Session = SessionLocal()
    try:
        since = datetime.utcnow() - timedelta(hours=hours)
        thresholds = session.query(DataQualityThreshold).filter(DataQualityThreshold.active == 1).all()
        created = 0
        updated = 0
        escalated = 0
        auto_actions: list[tuple[str, str]] = []  # (metric_type, action)
        for t in thresholds:
            snap = session.query(DataQualitySnapshot).filter(DataQualitySnapshot.metric_type == t.metric_type)
            if t.event_name:
                snap = snap.filter(DataQualitySnapshot.event_name == t.event_name)
            if t.project:
                snap = snap.filter(DataQualitySnapshot.project == t.project)
            snap = snap.filter(DataQualitySnapshot.captured_at >= since).order_by(DataQualitySnapshot.captured_at.desc()).first()
            if not snap:
                continue
            mv = snap.metric_value
            breach = False
            if t.lower_bound is not None and mv < t.lower_bound:
                breach = True
            if t.upper_bound is not None and mv > t.upper_bound:
                breach = True
            if not breach:
                continue
            existing = session.query(DataQualityAlert).filter_by(metric_type=t.metric_type, event_name=t.event_name, project=t.project, status='open').first()
            if existing:
                # update occurrence count in details
                details = existing.details or {}
                occ = int(details.get("occurrences", 1)) + 1
                details["occurrences"] = occ
                details["last_metric_value"] = mv
                details["last_observed_at"] = datetime.utcnow().isoformat()
                # Correlate connector errors once (or refresh every 3 occurrences)
                if occ == 2 or occ % 3 == 0:
                    details["recent_connector_errors"] = _correlate_connector_errors(session, t.project)
                # Potential escalation
                if existing.severity == "warn" and (t.severity == "critical" or occ >= max_escalation_occurrences):
                    existing.severity = "critical"
                    details["escalated_at"] = datetime.utcnow().isoformat()
                    escalated += 1
                    DQ_ESCALATIONS.labels(metric_type=t.metric_type, project=t.project or "_global").inc()
                # Auto-remediation for critical alerts
                remediation = _auto_remediate(session, t.metric_type, t.project, t.event_name, existing.severity)
                if remediation:
                    details.setdefault("auto_remediation", []).append(remediation)
                    auto_actions.append((t.metric_type, remediation.get("action", "unknown")))
                existing.metric_value = mv
                existing.details = details
                updated += 1
                DQ_THRESHOLD_BREACHES.labels(metric_type=t.metric_type, severity=existing.severity, project=t.project or "_global").inc()
            else:
                # new alert
                details = {"threshold_id": t.id, "occurrences": 1, "first_metric_value": mv, "recent_connector_errors": _correlate_connector_errors(session, t.project)}
                alert = DataQualityAlert(metric_type=t.metric_type, event_name=t.event_name, metric_value=mv, severity=t.severity, project=t.project, details=details)
                session.add(alert)
                created += 1
                DQ_THRESHOLD_BREACHES.labels(metric_type=t.metric_type, severity=t.severity, project=t.project or "_global").inc()
                # Auto-remediation if directly critical
                remediation = _auto_remediate(session, t.metric_type, t.project, t.event_name, t.severity)
                if remediation:
                    details.setdefault("auto_remediation", []).append(remediation)
                    auto_actions.append((t.metric_type, remediation.get("action", "unknown")))
        if auto_actions:
            for metric_type, action in auto_actions:
                DQ_AUTO_REMEDIATIONS.labels(metric_type=metric_type, action=action, project=t.project or "_global").inc()
        session.commit()
        # Update active alerts gauge (post-commit)
        open_alerts = session.query(DataQualityAlert).filter(DataQualityAlert.status == 'open').all()
        # Reset gauge by setting for seen combinations (Prom client will keep prior labels but we update current counts)
        from collections import Counter
        c = Counter()
        for a in open_alerts:
            c[(a.project or "_global", a.severity)] += 1
        for (proj, sev), cnt in c.items():
            DQ_ACTIVE_ALERTS.labels(project=proj, severity=sev).set(cnt)
        # Dispatch notifications (Slack only for new alerts or escalations)
        notify_count = created + escalated
        if notify_count:
            settings = get_settings()
            if settings.slack_bot_token and settings.slack_report_channel:
                try:
                    client = WebClient(token=settings.slack_bot_token)
                    client.chat_postMessage(channel=settings.slack_report_channel, text=f"Data Quality Alerts: {created} new, {escalated} escalated")
                except Exception:  # noqa
                    pass
        duration = (datetime.utcnow() - start_time).total_seconds()
        DQ_EVALUATION_DURATION.observe(duration)
        return {"status": "ok", "alerts_created": created, "alerts_updated": updated, "alerts_escalated": escalated, "duration_s": duration}
    finally:
        session.close()

@shared_task
def enforce_retention(batch_size: int = 10000):
    session: Session = SessionLocal()
    try:
        policies = session.query(RetentionPolicy).filter(RetentionPolicy.active==1).all()
        total_deleted = 0
        for p in policies:
            cutoff = datetime.utcnow() - timedelta(days=p.max_age_days)
            if p.table_name == 'raw_events':
                q = session.query(RawEvent).filter(RawEvent.ts < cutoff).limit(batch_size)
                ids = [r.id for r in q]
                if ids:
                    session.query(RawEvent).filter(RawEvent.id.in_(ids)).delete(synchronize_session=False)
                    total_deleted += len(ids)
        if total_deleted:
            session.commit()
        return {"status": "ok", "deleted": total_deleted}
    finally:
        session.close()

# Metric types: completeness_rate, freshness_minutes, duplicate_rate, drift_prop_missing, pii_violation_rate

@shared_task
def compute_data_quality(project: str | None = None, lookback_hours: int = 24):
    """Compute core DQ metrics and store snapshots.

    Completeness: events per user vs median (proxy) per event_name.
    Freshness: minutes since last event per event_name.
    Duplicates: ratio of duplicate event_ids.
    Drift: missing required props occurrences.
    PII violations: unexpected sensitive keys outside configured mask list.
    """
    settings = get_settings()
    session: Session = SessionLocal()
    try:
        since = datetime.utcnow() - timedelta(hours=lookback_hours)
        base = session.query(RawEvent).filter(RawEvent.ts >= since)
        if project:
            base = base.filter(RawEvent.project == project)
        # Gather events
        rows = base.all()
        if not rows:
            return {"status": "empty"}
        # Group by event_name
        from collections import defaultdict, Counter
        events_by_name = defaultdict(list)
        user_counts = defaultdict(set)
        last_ts = {}
        duplicate_counter = Counter()
        seen_ids = set()
        for r in rows:
            events_by_name[r.event_name].append(r)
            user_counts[r.event_name].add(r.user_id)
            last_ts[r.event_name] = max(last_ts.get(r.event_name, r.ts), r.ts)
            if r.event_id in seen_ids:
                duplicate_counter[r.event_name] += 1
            else:
                seen_ids.add(r.event_id)
        # Completeness (proxy): events per user / median across events
        completeness = {}
        per_event_rates = {}
        import statistics
        for name, evts in events_by_name.items():
            users = user_counts[name]
            rate = len(evts) / max(len(users), 1)
            per_event_rates[name] = rate
        median_rate = statistics.median(per_event_rates.values()) if per_event_rates else 0.0
        for name, rate in per_event_rates.items():
            completeness[name] = rate / median_rate if median_rate else 1.0
        # Freshness minutes
        freshness = {name: (datetime.utcnow() - last_ts[name]).total_seconds() / 60.0 for name in last_ts}
        # Duplicate rate per event: duplicates / total
        duplicate_rate = {name: duplicate_counter[name] / max(len(events_by_name[name]),1) for name in events_by_name}
        # Drift / missing required props
        schema_rows = {s.event_name: s for s in session.query(EventSchema).filter(EventSchema.active == 1)}
        drift_missing = {}
        for name, evts in events_by_name.items():
            sch = schema_rows.get(name)
            if not sch:
                continue
            req = list((sch.required_props or {}).keys())
            if not req:
                continue
            missing_total = 0
            for e in evts:
                for k in req:
                    if k not in (e.props or {}):
                        missing_total += 1
            drift_missing[name] = missing_total / max(len(evts),1)
        # PII violation rate: props containing keys not on mask list but matching simple patterns (email, phone)
        pii_fields = {f.lower() for f in (settings.pii_fields.split(',') if settings.pii_fields else [])}
        import re
        email_re = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        phone_re = re.compile(r"^[+]?\d{7,15}$")
        pii_violations = {}
        for name, evts in events_by_name.items():
            violations = 0
            for e in evts:
                for k, v in (e.props or {}).items():
                    if k.lower() in pii_fields:
                        continue
                    if isinstance(v, str) and (email_re.match(v) or phone_re.match(v)):
                        violations += 1
            pii_violations[name] = violations / max(len(evts),1)
        # Persist snapshots
        def save(metric_map, metric_type):
            for name, value in metric_map.items():
                session.add(DataQualitySnapshot(project=project, event_name=name, metric_type=metric_type, metric_value=float(value), details=None))
        save(completeness, "completeness_rate")
        save(freshness, "freshness_minutes")
        save(duplicate_rate, "duplicate_rate")
        save(drift_missing, "drift_prop_missing")
        save(pii_violations, "pii_violation_rate")
        session.commit()
        return {"status": "ok", "events": len(events_by_name)}
    finally:
        session.close()

@shared_task
def snapshot_volume_anomalies():
    # Integrate with anomaly_events table if anomaly detection is enabled.
    return {"status": "noop"}


@shared_task
def detect_schema_drift():
    """Compare observed properties vs schema registry to raise drift alerts.

    Rules:
      - New property: observed prop not in required_props map (treated as candidate extension).
      - Missing required: required prop appearing missing in > threshold% of recent events (leveraging existing drift metric is separate; here we open alert if not already).
    """
    session: Session = SessionLocal()
    try:
        schemas = {s.event_name: s for s in session.query(EventSchema).filter(EventSchema.active == 1)}
        observed = session.query(ObservedEventProperty).all()
        from collections import defaultdict
        observed_props = defaultdict(set)
        for o in observed:
            observed_props[o.event_name].add(o.prop_name)
        for ev_name, sch in schemas.items():
            req = set((sch.required_props or {}).keys())
            obs = observed_props.get(ev_name, set())
            # New props (candidate)
            for prop in obs - req:
                _upsert_alert(session, ev_name, prop, 'new_prop')
            # Missing required props: if required exists but not seen at all (coarse first pass)
            for prop in req - obs:
                _upsert_alert(session, ev_name, prop, 'missing_required')
        session.commit()
        return {"status": "ok", "alerts": session.query(SchemaDriftAlert).filter(SchemaDriftAlert.status=="open").count()}
    finally:
        session.close()


def _upsert_alert(session: Session, event_name: str, prop_name: str | None, change_type: str):
    existing = session.query(SchemaDriftAlert).filter(
        SchemaDriftAlert.event_name==event_name,
        SchemaDriftAlert.prop_name==prop_name,
        SchemaDriftAlert.change_type==change_type
    ).first()
    now = datetime.utcnow()
    if not existing:
        session.add(SchemaDriftAlert(event_name=event_name, prop_name=prop_name, change_type=change_type, first_seen=now, last_seen=now, occurrences=1))
    else:
        existing.last_seen = now
        existing.occurrences += 1
