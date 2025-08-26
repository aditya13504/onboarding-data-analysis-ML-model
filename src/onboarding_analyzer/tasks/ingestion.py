from celery import shared_task

from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent, IngestionDeadLetter, ConnectorState, EventSchema, ObservedEventProperty, BackfillJob, UserIdentityMap, EventNameMap, NormalizationRule, FunnelMetric, ExperimentDefinition, ExperimentAssignment
from onboarding_analyzer.validation.events import validate_event
from prometheus_client import Counter, Histogram, Gauge
ADAPTIVE_WINDOW_SECONDS = Gauge('connector_adaptive_window_seconds', 'Current adaptive window size seconds', ['connector'])

try:
    # Reuse API registry so metrics appear under /metrics
    from onboarding_analyzer.api.main import registry as _api_registry  # type: ignore
except Exception:  # fallback to default registry
    _api_registry = None

VALIDATION_FAILURES = Counter('event_validation_failures_total', 'Event validation failures', ['reason'], registry=_api_registry)
# Ingestion pipeline metrics
INGEST_BATCH_LATENCY = Histogram('ingest_batch_latency_seconds', 'Latency to process an ingestion batch', buckets=(0.01,0.05,0.1,0.25,0.5,1,2,5,10), registry=_api_registry)
EVENTS_RECEIVED = Counter('ingest_events_received_total', 'Events received before filtering', registry=_api_registry)
EVENTS_PERSISTED = Counter('ingest_events_persisted_total', 'Events successfully persisted', registry=_api_registry)
EVENTS_FILTERED = Counter('ingest_events_filtered_total', 'Events filtered out', ['reason'], registry=_api_registry)
EVENTS_DLQ = Counter('ingest_events_dead_letter_total', 'Events routed to DLQ', ['reason'], registry=_api_registry)
DLQ_SIZE = Gauge('ingest_dead_letter_queue_size', 'Current size of ingestion dead letter queue', registry=_api_registry)
BACKLOG_GAUGE = Gauge('ingest_backlog_events', 'Approx backlog: events older than lateness threshold still unprocessed', registry=_api_registry)
INGESTION_MAX_LAG = Gauge('ingest_max_processing_lag_seconds', 'Max seconds between event timestamp and processing time in last evaluation window', registry=_api_registry)
INGESTION_SLA_BREACHES = Counter('ingest_sla_breaches_total', 'Ingestion SLA breach occurrences', ['sla','project'], registry=_api_registry)


def _sanitize_payload(obj):
    from datetime import datetime as _dt
    if isinstance(obj, _dt):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _sanitize_payload(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_payload(v) for v in obj]
    return obj
from typing import Iterable, Dict, Any
import uuid
from onboarding_analyzer.connectors.mixpanel import MixpanelConnector
from onboarding_analyzer.connectors.posthog import PostHogConnector
from onboarding_analyzer.connectors.amplitude import AmplitudeConnector
from onboarding_analyzer.connectors.base import CONNECTOR_WINDOW_LAG
from onboarding_analyzer.connectors.base import CONNECTOR_WINDOW_DURATION, CONNECTOR_WINDOWS, CONNECTOR_BYTES
from onboarding_analyzer.config import get_settings, parse_allowed_events, parse_api_key_project_map, parse_pii_fields
from tenacity import retry, stop_after_attempt, wait_exponential

# Canonical onboarding steps should be configured externally; here we derive from data.
ONBOARDING_STEPS: list[str] = []  # populated dynamically by analytics based on observed order


class EventRecord(Dict[str, Any]):
    """Typed alias for raw event mapping."""


def persist_events(events: Iterable[EventRecord]) -> int:
    """Persist events with minimal round-trips.

    Strategy:
      1. Collect payloads; assign missing IDs.
      2. Fetch existing IDs in one query to drop duplicates.
      3. Bulk save via SQLAlchemy bulk_save_objects for throughput.
    """
    ev_list = list(events)
    if not ev_list:
        return 0
    settings = get_settings()
    for e in ev_list:
        if not e.get("event_id"):
            # Stable event_id: hash connector fields if present else UUID fallback
            try:
                import hashlib as _h
                base = f"{e.get('user_id','')}|{e.get('session_id','')}|{e.get('event_name','')}|{e.get('ts','')}|{sorted(list((e.get('props') or {}).keys()))}"
                e["event_id"] = _h.sha1(base.encode()).hexdigest()  # 40 hex chars
            except Exception:
                e["event_id"] = str(uuid.uuid4())
    ids = [e["event_id"] for e in ev_list]
    idempotency_keys = [e.get("idempotency_key") for e in ev_list if e.get("idempotency_key")]
    session: Session = SessionLocal()
    try:
        existing = set(r[0] for r in session.query(RawEvent.event_id).filter(RawEvent.event_id.in_(ids)))
        if idempotency_keys:
            existing_idem = set(r[0] for r in session.query(RawEvent.idempotency_key).filter(RawEvent.idempotency_key.in_(idempotency_keys)))
        else:
            existing_idem = set()
        to_insert = []
        for e in ev_list:
            # Skip if duplicate by event_id or idempotency_key (exactly-once semantics)
            if e["event_id"] in existing or (e.get("idempotency_key") and e.get("idempotency_key") in existing_idem):
                continue
            try:
                to_insert.append(RawEvent(
                    event_id=e["event_id"],
                    user_id=e["user_id"],
                    session_id=e["session_id"],
                    event_name=e["event_name"],
                    ts=e.get("ts", datetime.utcnow()),
                    props=e.get("props", {}),
                    schema_version=e.get("schema_version"),
                    project=e.get("props", {}).get("project") if isinstance(e.get("props"), dict) else None,
                    idempotency_key=e.get("idempotency_key"),
                ))
            except Exception as err:
                session.add(IngestionDeadLetter(payload=_sanitize_payload(e), error=str(err)))
        if not to_insert:
            session.commit()
            return 0
        # Chunk bulk inserts to manage memory
        batch_size = settings.ingest_batch_upsert_size
        inserted = 0
        for i in range(0, len(to_insert), batch_size):
            chunk = to_insert[i:i+batch_size]
            session.bulk_save_objects(chunk)
            session.flush()
            inserted += len(chunk)
        session.commit()
        if inserted:
            try:
                import redis
                r = redis.Redis.from_url(settings.redis_url)
                r.delete("cache:funnel_metrics")
            except Exception:
                pass
        return inserted
    finally:
        session.close()


@shared_task
def evaluate_ingestion_health(lookback_minutes: int = 30, sla_seconds: int = 120):
    """Assess ingestion pipeline freshness & backlog.

    Computes:
      - max lag between event ts and now over recent persisted events
      - backlog count of events older than lateness threshold but not yet processed (approx by DLQ + lateness filtered)
    Triggers SLA breach counter if max lag > sla_seconds.
    """
    session: Session = SessionLocal()
    try:
        now = datetime.utcnow()
        since = now - timedelta(minutes=lookback_minutes)
        recent = session.query(RawEvent.ts).filter(RawEvent.ts >= since).order_by(RawEvent.ts.desc()).limit(10000).all()
        if not recent:
            INGESTION_MAX_LAG.set(0)
            return {"status": "empty"}
        lags = [(now - r[0]).total_seconds() for r in recent if r[0] <= now]
        max_lag = max(lags) if lags else 0.0
        INGESTION_MAX_LAG.set(max_lag)
        # Approx backlog: events in DLQ with late_arrival or missing_required reasons in lookback
        late_cutoff = now - timedelta(hours=get_settings().late_arrival_threshold_hours)
        from onboarding_analyzer.models.tables import IngestionDeadLetter as _DL
        backlog = session.query(_DL.id).filter(_DL.created_at >= since).count()
        BACKLOG_GAUGE.set(backlog)
        if max_lag > sla_seconds:
            try:
                INGESTION_SLA_BREACHES.labels(sla=str(sla_seconds), project='_global').inc()
            except Exception:
                pass
        # Opportunistically store OpsMetric row for external querying
        try:
            from onboarding_analyzer.models.tables import OpsMetric
            session.add(OpsMetric(metric_name='ingest_max_lag_s', metric_value=float(max_lag), details={"lookback_min": lookback_minutes}))
            session.add(OpsMetric(metric_name='ingest_backlog_events', metric_value=float(backlog), details={"lookback_min": lookback_minutes}))
            session.commit()
        except Exception:
            session.rollback()
        return {"status": "ok", "max_lag_s": max_lag, "backlog": backlog}
    finally:
        session.close()


@shared_task
def ingest_events(batch: list[EventRecord]):
    if not batch:
        return {"status": "empty"}
    start_ts = datetime.utcnow()
    try:
        EVENTS_RECEIVED.inc(len(batch))
    except Exception:
        pass
    settings = get_settings()
    allowed = parse_allowed_events(settings.allowed_events)
    pii = parse_pii_fields(settings.pii_fields)
    # Fetch project quotas (cached lightly per task invocation)
    project_quota = None
    if project:
        try:
            session_q = SessionLocal()
            from onboarding_analyzer.models.tables import ProjectQuota
            q = session_q.query(ProjectQuota).filter(ProjectQuota.project==project, ProjectQuota.enforced==1).first()
            if q:
                project_quota = {"daily": q.daily_event_limit, "monthly": q.monthly_event_limit}
            session_q.close()
        except Exception:
            project_quota = None
    # Determine project from API key header passed via context (if any)
    project_map = parse_api_key_project_map(settings.api_key_project_map)
    project = None
    try:
        from celery import current_task
        headers = getattr(current_task.request, 'headers', {}) if current_task else {}
        api_key = headers.get('X-API-Key') if headers else None
        if api_key and api_key in project_map:
            project = project_map[api_key]
    except Exception:
        pass
    filtered: list[EventRecord] = []
    # Preload dynamic event schema constraints
    session_schema = SessionLocal()
    schema_map: dict[str, EventSchema] = {s.event_name: s for s in session_schema.query(EventSchema).filter(EventSchema.active == 1)}
    session_schema.close()
    late_threshold = datetime.utcnow() - timedelta(hours=settings.late_arrival_threshold_hours)
    # Preload identity and event name mappings
    session_norm = SessionLocal()
    id_maps = {m.alias_user_id: m.primary_user_id for m in session_norm.query(UserIdentityMap).all()}
    name_maps = {m.variant_name: m.canonical_name for m in session_norm.query(EventNameMap).all()}
    # Active normalization rules
    rules = session_norm.query(NormalizationRule).filter(NormalizationRule.active==1).all()
    rule_maps = {"event_alias": {}, "prop_rename": {}, "value_map": {}}
    for r in rules:
        try:
            if r.rule_type in rule_maps and isinstance(r.payload, dict):
                # merge shallow
                for k,v in r.payload.items():
                    rule_maps[r.rule_type][k] = v
        except Exception:
            pass
    for variant, canon in rule_maps["event_alias"].items():
        name_maps.setdefault(variant, canon)
    session_norm.close()
    # Config-driven mapping overrides
    import json as _json
    try:
        if settings.event_name_aliases:
            name_maps.update(_json.loads(settings.event_name_aliases))
    except Exception:
        pass
    prop_renames: dict[str,str] = {}
    try:
        if settings.prop_rename_map:
            prop_renames = _json.loads(settings.prop_rename_map)
    except Exception:
        prop_renames = {}
    # Preload funnel step ordering (top N steps by order) for step_index enrichment
    step_order_map: dict[str,int] = {}
    try:
        session_steps = SessionLocal()
        for fm in session_steps.query(FunnelMetric).order_by(FunnelMetric.step_order).limit(50).all():
            step_order_map[fm.step_name] = fm.step_order
        session_steps.close()
    except Exception:
        pass
    # Preload experiment assignments (active experiments only) for stamping variant props
    session_exp = SessionLocal()
    exp_defs = session_exp.query(ExperimentDefinition).filter(ExperimentDefinition.active==1).all()
    assignments_map: dict[tuple[str,str], str] = {}
    if exp_defs:
        # Load assignments for users present in batch only (optimize)
        batch_user_ids = {ev.get('user_id') for ev in batch if ev.get('user_id')}
        if batch_user_ids:
            rows = session_exp.query(ExperimentAssignment).filter(ExperimentAssignment.user_id.in_(list(batch_user_ids))).all()
            for row in rows:
                assignments_map[(row.experiment_key, row.user_id)] = row.variant
    session_exp.close()
    # Map experiment key -> assignment_prop for stamping
    exp_prop_map = {d.key: d.assignment_prop for d in exp_defs}
    for ev in batch:
        # Quota enforcement (simple Redis counters for throughput)
        if project_quota and project:
            try:
                import redis as _redis
                r = _redis.Redis.from_url(settings.redis_url)
                today_key = f"quota:{project}:{datetime.utcnow().date()}"
                month_key = f"quota:{project}:{datetime.utcnow().strftime('%Y-%m')}"
                # increment pessimistically; if over limit mark filtered
                day_val = r.incr(today_key, 1)
                if day_val == 1:
                    r.expire(today_key, 60*60*24*2)
                month_val = r.incr(month_key, 1)
                if month_val == 1:
                    # expire after ~40 days
                    r.expire(month_key, 60*60*24*40)
                over = False
                if project_quota.get('daily') and day_val > project_quota['daily']:
                    over = True
                if project_quota.get('monthly') and month_val > project_quota['monthly']:
                    over = True
                if over:
                    try:
                        EVENTS_FILTERED.labels(reason='quota_exceeded').inc()
                    except Exception:
                        pass
                    continue
            except Exception:
                pass  # fail open
        if allowed and ev.get('event_name') not in allowed:
            try:
                EVENTS_FILTERED.labels(reason='disallowed_event').inc()
            except Exception:
                pass
            continue
        # PII masking
        props = ev.get('props', {}) or {}
        for f in pii:
            if f in props:
                props[f] = "***"  # simple mask
        ev['props'] = props
        if project:
            ev.setdefault('props', {})['project'] = project
        # Late arrival discard (optional) - keep if within threshold
        ts = ev.get('ts')
        if isinstance(ts, datetime) and ts < late_threshold:
            try:
                EVENTS_FILTERED.labels(reason='late_arrival').inc()
            except Exception:
                pass
            continue
        # Dynamic schema registry enforcement: required props + min version
        schema_row = schema_map.get(ev.get('event_name'))
        if schema_row:
            req = schema_row.required_props or {}
            missing = [k for k in req.keys() if k not in ev.get('props', {})]
            if missing:
                reason = f"missing_required:{','.join(missing)}"
                session = SessionLocal()
                try:
                    session.add(IngestionDeadLetter(payload=_sanitize_payload(ev), error=reason))
                    session.commit()
                finally:
                    session.close()
                try:
                    VALIDATION_FAILURES.labels(reason=reason).inc()
                    EVENTS_DLQ.labels(reason=reason).inc()
                except Exception:
                    pass
                continue
            # Version gate
            min_v = schema_row.min_version
            if min_v and (ev.get('schema_version') or 'v0') < min_v:
                # allow but annotate upgrade path
                ev.setdefault('props', {})['_schema_upgrade_needed'] = True
        ok, reason = validate_event(ev)
        if not ok:
            # route to DLQ immediately
            session = SessionLocal()
            try:
                session.add(IngestionDeadLetter(payload=_sanitize_payload(ev), error=reason or 'invalid'))
                session.commit()
            finally:
                session.close()
            try:
                VALIDATION_FAILURES.labels(reason=reason or 'invalid').inc()
                EVENTS_DLQ.labels(reason=reason or 'invalid').inc()
            except Exception:
                pass
            continue
        # Property renames & simple type coercion (int->float, numeric strings)
        ev_props = ev.get('props', {}) or {}
        # Merge in rule based prop renames
        combined_prop_renames = {**prop_renames, **rule_maps.get('prop_rename', {})}
        for old,new in combined_prop_renames.items():
            if old in ev_props and new not in ev_props:
                ev_props[new] = ev_props.pop(old)
        # Coerce numeric strings
        for k,v in list(ev_props.items()):
            if isinstance(v, str) and v.replace('.','',1).isdigit():
                try:
                    if '.' in v:
                        ev_props[k] = float(v)
                    else:
                        ev_props[k] = int(v)
                except Exception:
                    pass
        ev['props'] = ev_props
        # Identity resolution
        uid = ev.get('user_id')
        if uid in id_maps:
            ev['user_id'] = id_maps[uid]
        # Heuristic identity linking by email property
        if settings.identity_email_enabled:
            email = None
            for key in ('email','user_email','userEmail'):
                if isinstance(ev_props, dict) and key in ev_props and isinstance(ev_props[key], str):
                    email = ev_props[key].lower()
                    break
            if email and '@' in email:
                # Derive primary id as email hash for stability
                import hashlib as _hash
                primary = _hash.sha256(email.encode()).hexdigest()[:32]
                if ev['user_id'] != primary:
                    id_maps[ev['user_id']] = primary
                    ev['user_id'] = primary
        # Event name canonicalization
        en = ev.get('event_name')
        if en in name_maps:
            ev['event_name'] = name_maps[en]
        # session_event_index (count per session_id in this batch – approximate; refined later asynchronously)
        # Use simple incremental map
        if not hasattr(ingest_events, '_session_event_counters'):
            ingest_events._session_event_counters = {}
        counters = ingest_events._session_event_counters  # type: ignore
        sid = ev.get('session_id')
        if sid:
            counters[sid] = counters.get(sid, -1) + 1
            ev['session_event_index'] = counters[sid]
        # step_index enrichment if event_name matches known funnel step
        en2 = ev.get('event_name')
        if en2 in step_order_map:
            ev['step_index'] = step_order_map[en2]
        # Experiment stamping: inject experiment assignment props if user assigned
        uid2 = ev.get('user_id')
        if uid2 and exp_defs:
            for exp in exp_defs:
                variant = assignments_map.get((exp.key, uid2))
                if variant:
                    # stamp if not already present
                    prop_name = exp.assignment_prop
                    if prop_name and prop_name not in ev_props:
                        ev_props[prop_name] = variant
        # Value mapping normalization (exact match replacements)
        val_map = rule_maps.get('value_map', {})
        for pk, mapping in val_map.items():
            if isinstance(mapping, dict) and pk in ev_props:
                current_val = ev_props[pk]
                if current_val in mapping:
                    ev_props[pk] = mapping[current_val]
        ev['props'] = ev_props
        filtered.append(ev)
    # Dynamic PII detection (post-filter, pre-persist) using heuristics
    if filtered:
        try:
            session_pii = SessionLocal()
            from onboarding_analyzer.models.tables import ObservedPIIProperty
            import re, hashlib as _h
            email_re = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
            phone_re = re.compile(r"^\+?[0-9\-()\s]{7,}$")
            for ev in filtered:
                props = ev.get('props') or {}
                proj = (props or {}).get('project')
                for k,v in list(props.items()):
                    if not isinstance(v, (str, int, float)):
                        continue
                    sval = str(v)[:256]
                    detection = None
                    if email_re.match(sval):
                        detection = 'email'
                    elif phone_re.match(sval) and sum(c.isdigit() for c in sval) >= 7:
                        detection = 'phone'
                    elif 'ssn' in k.lower() and sval.replace('-','').isdigit() and len(sval.replace('-','')) in (9,):
                        detection = 'ssn'
                    if detection:
                        hval = _h.sha256(sval.encode()).hexdigest()[:64]
                        row = session_pii.query(ObservedPIIProperty).filter_by(prop_name=k, project=proj).first()
                        if row:
                            row.last_seen = datetime.utcnow()
                            row.occurrences += 1
                        else:
                            row = ObservedPIIProperty(event_name=ev.get('event_name'), prop_name=k, detection_type=detection, sample_hash=hval, project=proj)
                            session_pii.add(row)
            session_pii.commit()
            session_pii.close()
        except Exception:
            pass
    inserted = persist_events(filtered)
    # Real-time publish (best-effort) into in-process ring buffer for /ws/events clients
    try:
        from onboarding_analyzer.api import main as api_main  # type: ignore
        from onboarding_analyzer.config import get_settings as _gs
        if _gs().stream_publish_enabled and filtered:
            buf = getattr(api_main, 'RECENT_EVENTS_BUFFER', None)
            if isinstance(buf, list):
                for ev in filtered[-50:]:  # limit per batch
                    buf.append({
                        'event_name': ev.get('event_name'),
                        'user_id': ev.get('user_id'),
                        'session_id': ev.get('session_id'),
                        'ts': (ev.get('ts') or datetime.utcnow()).isoformat() if hasattr(ev.get('ts'), 'isoformat') else ev.get('ts'),
                        'project': (ev.get('props') or {}).get('project'),
                    })
                # trim
                max_len = getattr(api_main, 'RECENT_EVENTS_MAX', 500)
                if len(buf) > max_len:
                    del buf[:-max_len]
            # Broadcast async (fire-and-forget)
            subs = getattr(api_main, '_ws_subscribers', None)
            lock = getattr(api_main, '_ws_lock', None)
            if subs and lock:
                import asyncio
                async def _broadcast(payloads):
                    async with lock:  # type: ignore
                        dead = []
                        for ws in list(subs):  # type: ignore
                            try:
                                for p in payloads:
                                    await ws.send_json(p)
                            except Exception:
                                dead.append(ws)
                        for d in dead:
                            subs.discard(d)  # type: ignore
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        loop.create_task(_broadcast(buf[-10:]))
                except Exception:
                    pass
    except Exception:
        pass
    try:
        if inserted:
            EVENTS_PERSISTED.inc(inserted)
    except Exception:
        pass
    # Update DLQ size & backlog gauges
    try:
        session = SessionLocal()
        dlq_count = session.query(IngestionDeadLetter).count()
        DLQ_SIZE.set(dlq_count)
        # backlog: events older than lateness threshold that are still unprocessed (approx = those with ts < threshold in raw table in last 24h)
        backlog = session.query(RawEvent).filter(RawEvent.ts < late_threshold, RawEvent.ts >= datetime.utcnow()-timedelta(days=1)).count()
        BACKLOG_GAUGE.set(backlog)
        session.close()
    except Exception:
        pass
    try:
        INGEST_BATCH_LATENCY.observe((datetime.utcnow()-start_ts).total_seconds())
    except Exception:
        pass
    # Record observed properties for schema diff detection (best-effort)
    if inserted and filtered:
        try:
            session_obs = SessionLocal()
            now = datetime.utcnow()
            # Aggregate unique event->props
            aggregate: dict[tuple[str,str], int] = {}
            for ev in filtered:
                ev_name = ev.get('event_name')
                for p in (ev.get('props') or {}).keys():
                    k = (ev_name, p)
                    aggregate[k] = aggregate.get(k, 0) + 1
            for (ev_name, prop), cnt in aggregate.items():
                row = session_obs.query(ObservedEventProperty).filter(
                    ObservedEventProperty.event_name == ev_name,
                    ObservedEventProperty.prop_name == prop
                ).first()
                if not row:
                    session_obs.add(ObservedEventProperty(event_name=ev_name, prop_name=prop, first_seen=now, last_seen=now, count=cnt))
                else:
                    row.last_seen = now
                    row.count += cnt
            session_obs.commit()
            session_obs.close()
        except Exception:
            pass
    return {"status": "ok", "inserted": inserted}


@shared_task
def ingest_from_connectors():
    settings = get_settings()
    # Expect env variables for each connector; only instantiate if creds present.
    connectors = []
    if all(k in settings.__dict__ for k in ("mixpanel_project_id",)):
        pass  # reserved for dynamic config store
    # Build from explicit env naming convention (document later)
    mp_pid = getattr(settings, "mixpanel_project_id", None)
    mp_user = getattr(settings, "mixpanel_username", None)
    mp_secret = getattr(settings, "mixpanel_secret", None)
    if mp_pid and mp_user and mp_secret:
        connectors.append(MixpanelConnector(mp_pid, mp_user, mp_secret))
    ph_key = getattr(settings, "posthog_api_key", None)
    ph_host = getattr(settings, "posthog_host", None)
    if ph_key:
        connectors.append(PostHogConnector(ph_key, ph_host or "https://app.posthog.com"))
    amp_key = getattr(settings, "amplitude_api_key", None)
    amp_secret = getattr(settings, "amplitude_secret_key", None)
    if amp_key and amp_secret:
        connectors.append(AmplitudeConnector(amp_key, amp_secret))
    if not connectors:
        return {"status": "no_connectors"}
    until = datetime.utcnow()
    # Load per-connector state for incremental fetch windows
    session_state = SessionLocal()
    state_map: dict[str, ConnectorState] = {s.connector_name: s for s in session_state.query(ConnectorState).all()}
    total = 0
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=30))
    def _fetch(c, s, u, cur=None):
        return list(c.fetch_events(s, u, cursor=cur))
    for c in connectors:
        try:
            st = state_map.get(c.name)
            since = (st.last_until_ts or (until - timedelta(hours=1))) if st else (until - timedelta(hours=1))
            cursor = st.cursor if st else None
            events = list(c.instrumented_fetch(since, until, cursor))
            last_cursor = None
            for evt in events:
                last_cursor = evt.pop('_cursor', None) or last_cursor
                total += persist_events([evt])
            # update state
            if not st:
                st = ConnectorState(connector_name=c.name)
                session_state.add(st)
            st.last_since_ts = since
            st.last_until_ts = until
            if last_cursor:
                st.cursor = last_cursor
            st.updated_at = datetime.utcnow()
            # window lag metric
            try:
                CONNECTOR_WINDOW_LAG.labels(c.name).set((datetime.utcnow() - st.last_until_ts).total_seconds())
            except Exception:
                pass
            session_state.commit()
        except Exception:  # noqa
            continue
    session_state.close()
    return {"status": "ok", "inserted": total, "connectors": [c.name for c in connectors]}


@shared_task
def run_incremental_connector(connector_name: str, window_minutes: int = 60, max_windows: int = 5, allow_catchup: bool = True):
    """Incrementally advance a single connector in fixed windows.

    Behavior:
      - Reads or creates `ConnectorState`.
      - Determines next window start = last_until_ts or (now - window_minutes) if none.
      - Iterates up to max_windows contiguous windows until it reaches 'now' (or stops earlier).
      - Persists events for each window; updates state after each successful window to avoid replay.
      - Captures cursor if events include `_cursor` field.
      - Updates lag gauge after final window.
      - Categorizes errors (rate_limit, timeout, http_4xx, http_5xx, parse, unknown) best-effort.

    Parameters:
      connector_name: canonical name (mixpanel|amplitude|posthog)
      window_minutes: size of each incremental window
      max_windows: safety cap per invocation
      allow_catchup: if False, processes at most one window even if behind
    """
    settings = get_settings()
    # Build connector instance
    name_map = {}
    mp_pid = getattr(settings, "mixpanel_project_id", None)
    mp_user = getattr(settings, "mixpanel_username", None)
    mp_secret = getattr(settings, "mixpanel_secret", None)
    if mp_pid and mp_user and mp_secret:
        name_map["mixpanel"] = MixpanelConnector(mp_pid, mp_user, mp_secret)
    ph_key = getattr(settings, "posthog_api_key", None)
    ph_host = getattr(settings, "posthog_host", None)
    if ph_key:
        name_map["posthog"] = PostHogConnector(ph_key, ph_host or "https://app.posthog.com")
    amp_key = getattr(settings, "amplitude_api_key", None)
    amp_secret = getattr(settings, "amplitude_secret_key", None)
    if amp_key and amp_secret:
        name_map["amplitude"] = AmplitudeConnector(amp_key, amp_secret)
    connector = name_map.get(connector_name)
    if not connector:
        return {"status": "error", "error": "connector_not_configured"}
    session: Session = SessionLocal()
    try:
        state = session.query(ConnectorState).filter_by(connector_name=connector_name).first()
        now = datetime.utcnow()
        if not state:
            state = ConnectorState(connector_name=connector_name, last_until_ts=None, last_since_ts=None)
            session.add(state)
            session.commit()
        # Compute starting point
        window_delta = timedelta(minutes=window_minutes)
        start = state.last_until_ts or (now - window_delta)
        processed_windows = 0
        inserted_total = 0
        cursor = state.cursor
        # Safety: don't process future
        while processed_windows < max_windows:
            until = start + window_delta
            if until > now:
                until = now
            if until <= start:
                break
            try:
                import time as _t
                wstart = _t.time()
                events = list(connector.instrumented_fetch(start, until, cursor=cursor))
                last_cursor = cursor
                if events:
                    # capture last event cursor if present
                    for ev in events:
                        if '_cursor' in ev and ev['_cursor']:
                            last_cursor = ev['_cursor']
                    # Approx bytes (serialize props only best effort)
                    try:
                        import json as _json
                        b = sum(len(_json.dumps(ev.get('props', {}))) for ev in events)
                        CONNECTOR_BYTES.labels(connector_name).inc(b)
                    except Exception:
                        pass
                    inserted_total += persist_events(events)
                # Update state after each window regardless of event count (to move watermark)
                state.last_since_ts = start
                state.last_until_ts = until
                if last_cursor != cursor and last_cursor:
                    state.cursor = last_cursor
                state.updated_at = datetime.utcnow()
                state.failure_count = 0
                state.last_error = None
                session.commit()
                # Metrics per window
                try:
                    CONNECTOR_WINDOW_DURATION.labels(connector_name).observe(_t.time() - wstart)
                    CONNECTOR_WINDOWS.labels(connector_name).inc()
                except Exception:
                    pass
            except Exception as e:  # categorize
                # Basic categorization heuristics
                cat = 'unknown'
                es = str(e).lower()
                if '429' in es or 'rate limit' in es:
                    cat = 'rate_limit'
                elif 'timeout' in es or 'timed out' in es:
                    cat = 'timeout'
                elif '5' in es and 'http' in es:
                    cat = 'http_5xx'
                elif '4' in es and 'http' in es:
                    cat = 'http_4xx'
                elif 'json' in es or 'parse' in es:
                    cat = 'parse'
                try:
                    from onboarding_analyzer.connectors.base import CONNECTOR_ERROR_CATEGORY
                    CONNECTOR_ERROR_CATEGORY.labels(connector_name, cat).inc()
                except Exception:
                    pass
                # Update failure counters
                try:
                    state.failure_count += 1
                    state.last_error = es[:250]
                    state.updated_at = datetime.utcnow()
                    session.commit()
                except Exception:
                    pass
                # On error break to avoid tight failure loops; next invocation resumes
                break
            processed_windows += 1
            start = until
            if not allow_catchup:
                break
            now = datetime.utcnow()
            # If nearly up-to-date (< half window behind) stop early
            if now - start < window_delta / 2:
                break
            # Light adaptive pacing: if far behind (> 24h), accelerate (no sleep); otherwise brief jitter to avoid thundering herd
            try:
                import time as _t, random as _r
                if (now - start) < timedelta(hours=24):
                    _t.sleep(_r.uniform(0, 0.5))
            except Exception:
                pass
        # Update lag metric
        try:
            if state.last_until_ts:
                CONNECTOR_WINDOW_LAG.labels(connector_name).set((datetime.utcnow() - state.last_until_ts).total_seconds())
        except Exception:
            pass
        return {"status": "ok", "windows": processed_windows, "inserted": inserted_total, "last_until": state.last_until_ts.isoformat() if state.last_until_ts else None}
    finally:
        session.close()


@shared_task
def orchestrate_incremental_connectors(window_minutes: int = 60, max_windows: int = 3):
    """Periodic real incremental advancement for all configured connectors.

    This task performs no mocking: it inspects environment-backed settings and dispatches
    a `run_incremental_connector` sub-task per connector that has valid credentials.
    It intentionally limits max_windows per cycle to avoid long single-task monopolization.
    """
    settings = get_settings()
    dispatched: list[str] = []
    if getattr(settings, "mixpanel_project_id", None) and getattr(settings, "mixpanel_username", None) and getattr(settings, "mixpanel_secret", None):
        run_incremental_connector.delay("mixpanel", window_minutes=window_minutes, max_windows=max_windows)
        dispatched.append("mixpanel")
    if getattr(settings, "posthog_api_key", None):
        run_incremental_connector.delay("posthog", window_minutes=window_minutes, max_windows=max_windows)
        dispatched.append("posthog")
    if getattr(settings, "amplitude_api_key", None) and getattr(settings, "amplitude_secret_key", None):
        run_incremental_connector.delay("amplitude", window_minutes=window_minutes, max_windows=max_windows)
        dispatched.append("amplitude")
    if not dispatched:
        return {"status": "no_configured_connectors"}
    return {"status": "dispatched", "connectors": dispatched, "window_minutes": window_minutes, "max_windows": max_windows}


@shared_task
def reprocess_dead_letters(limit: int = 100):
    session: Session = SessionLocal()
    processed = 0
    try:
        settings = get_settings()
        rows = (
            session.query(IngestionDeadLetter)
            .filter(IngestionDeadLetter.attempts < settings.dlq_quarantine_attempts)
            .order_by(IngestionDeadLetter.created_at)
            .limit(limit)
            .all()
        )
        for row in rows:
            try:
                persist_events([row.payload])
                session.delete(row)
                processed += 1
            except Exception:
                # increment attempts; if exceeds, leave for manual review
                row.attempts += 1
        session.commit()
        return {"status": "ok", "processed": processed}
    finally:
        session.close()


@shared_task
def scheduled_reprocess_and_alert(limit: int = 200, alert_threshold: int = 500):
    """Periodic orchestration: reprocess a chunk and emit alert metric if DLQ too large."""
    res = reprocess_dead_letters(limit=limit)
    try:
        session = SessionLocal()
        dlq_count = session.query(IngestionDeadLetter).count()
        DLQ_SIZE.set(dlq_count)
        if dlq_count > alert_threshold:
            # Emit an over-threshold metric via counter labeled
            EVENTS_DLQ.labels(reason='threshold_exceeded').inc()
        session.close()
    except Exception:
        pass
    return res | {"dlq_size": DLQ_SIZE._value.get() if hasattr(DLQ_SIZE, '_value') else None}


@shared_task
def backfill_connector_history(connector_name: str, hours: int = 24, window_minutes: int = 60):
    """Perform historical backfill for a given connector.

    Strategy:
      - Walk backwards from current state (or now) to the target lower bound (now - hours)
        in fixed windows (window_minutes).
      - For each window, call the connector's fetch_events(since, until) and persist results.
      - Update ConnectorState window markers.
    Notes:
      - Idempotency preserved by event_id uniqueness (if connectors provide stable IDs).
      - Runs synchronously; for very large windows invoke multiple tasks chunked by hours.
    """
    settings = get_settings()
    # Build connector instance matching name
    name_map = {}
    mp_pid = getattr(settings, "mixpanel_project_id", None)
    mp_user = getattr(settings, "mixpanel_username", None)
    mp_secret = getattr(settings, "mixpanel_secret", None)
    if mp_pid and mp_user and mp_secret:
        name_map["mixpanel"] = MixpanelConnector(mp_pid, mp_user, mp_secret)
    ph_key = getattr(settings, "posthog_api_key", None)
    ph_host = getattr(settings, "posthog_host", None)
    if ph_key:
        name_map["posthog"] = PostHogConnector(ph_key, ph_host or "https://app.posthog.com")
    amp_key = getattr(settings, "amplitude_api_key", None)
    amp_secret = getattr(settings, "amplitude_secret_key", None)
    if amp_key and amp_secret:
        name_map["amplitude"] = AmplitudeConnector(amp_key, amp_secret)
    if connector_name not in name_map:
        return {"status": "error", "error": "unknown_or_unconfigured_connector"}
    connector = name_map[connector_name]
    now = datetime.utcnow()
    lower_bound = now - timedelta(hours=hours)
    session: Session = SessionLocal()
    inserted = 0
    windows = 0
    try:
        state = session.query(ConnectorState).filter_by(connector_name=connector_name).first()
        if not state:
            state = ConnectorState(connector_name=connector_name)
            session.add(state)
            session.commit()
        # Start from last_until_ts going backward if already have history, else from now
        pos = state.last_since_ts or now
        if pos < lower_bound:
            pos = lower_bound
        while pos > lower_bound:
            since = max(lower_bound, pos - timedelta(minutes=window_minutes))
            until_window = pos
            try:
                events = list(connector.instrumented_fetch(since, until_window))
                if events:
                    inserted += persist_events(events)
            except Exception:
                # continue despite window failure
                pass
            # Update state to reflect earliest covered window
            state.last_since_ts = since
            state.last_until_ts = until_window
            state.updated_at = datetime.utcnow()
            session.commit()
            pos = since
            windows += 1
        return {"status": "ok", "windows": windows, "inserted": inserted, "oldest": state.last_since_ts}
    finally:
        session.close()


@shared_task
def start_backfill_job(connector_name: str, start_iso: str, end_iso: str, window_minutes: int = 60):
    """Create a BackfillJob row (idempotent if overlapping active job) and return job id."""
    session: Session = SessionLocal()
    from datetime import datetime as _dt
    try:
        start_ts = _dt.fromisoformat(start_iso)
        end_ts = _dt.fromisoformat(end_iso)
        existing = (
            session.query(BackfillJob)
            .filter(BackfillJob.connector_name == connector_name, BackfillJob.status.in_(["pending","running"]))
            .first()
        )
        if existing:
            return {"status": "exists", "job_id": existing.id}
        job = BackfillJob(connector_name=connector_name, start_ts=start_ts, end_ts=end_ts, current_ts=start_ts, window_minutes=window_minutes, status="pending")
        session.add(job)
        session.commit()
        return {"status": "created", "job_id": job.id}
    finally:
        session.close()


@shared_task
def run_backfill_window(job_id: int, max_windows: int = 50):
    """Advance an existing BackfillJob by up to max_windows windows; requeue until complete."""
    session: Session = SessionLocal()
    try:
        job = session.query(BackfillJob).filter_by(id=job_id).first()
        if not job:
            return {"status": "error", "error": "job_not_found"}
        if job.status == "completed":
            return {"status": "completed", "job_id": job.id, "total_events": job.total_events}
        # Build connector
        settings = get_settings()
        mp_pid = getattr(settings, "mixpanel_project_id", None)
        mp_user = getattr(settings, "mixpanel_username", None)
        mp_secret = getattr(settings, "mixpanel_secret", None)
        ph_key = getattr(settings, "posthog_api_key", None)
        ph_host = getattr(settings, "posthog_host", None)
        amp_key = getattr(settings, "amplitude_api_key", None)
        amp_secret = getattr(settings, "amplitude_secret_key", None)
        connector = None
        if job.connector_name == "mixpanel" and mp_pid and mp_user and mp_secret:
            connector = MixpanelConnector(mp_pid, mp_user, mp_secret)
        elif job.connector_name == "posthog" and ph_key:
            connector = PostHogConnector(ph_key, ph_host or "https://app.posthog.com")
        elif job.connector_name == "amplitude" and amp_key and amp_secret:
            connector = AmplitudeConnector(amp_key, amp_secret)
        if not connector:
            job.status = "failed"
            session.commit()
            return {"status": "error", "error": "connector_not_configured"}
        job.status = "running"
        windows_done = 0
        from datetime import timedelta as _td
        while windows_done < max_windows and job.current_ts < job.end_ts:
            since = job.current_ts
            until = min(job.current_ts + _td(minutes=job.window_minutes), job.end_ts)
            try:
                events = list(connector.instrumented_fetch(since, until))
                if events:
                    inserted = persist_events(events)
                    job.total_events += inserted
            except Exception:
                # swallow errors to allow progress; could add failure counter
                pass
            job.current_ts = until
            job.windows_completed += 1
            windows_done += 1
            job.updated_at = datetime.utcnow()
            session.commit()
            if job.current_ts >= job.end_ts:
                job.status = "completed"
                session.commit()
                break
        return {"status": job.status, "job_id": job.id, "current": job.current_ts.isoformat() if job.current_ts else None, "total_events": job.total_events, "windows_completed": job.windows_completed}
    finally:
        session.close()


@shared_task
def adapt_connector_windows(min_window_minutes: int = 5, max_window_minutes: int = 180, target_catchup_hours: int = 6):
    """Adaptive window sizing per connector.

    Strategy:
      - If connector lag (now - last_until_ts) > target_catchup_hours, expand window up to max.
      - If recent failure_count > 0 or last_error within 1h, shrink window (halve) down to min.
      - Persist chosen window in ConnectorState.cursor as special token window:{minutes} (non-destructive reuse of cursor field appended after |).
    """
    session: Session = SessionLocal()
    try:
        now = datetime.utcnow()
        updated = {}
        states = session.query(ConnectorState).all()
        for st in states:
            # parse existing window from cursor metadata
            base_window = 30  # default minutes
            cur_window = base_window
            if st.cursor and 'window:' in st.cursor:
                try:
                    import re
                    m = re.search(r'window:(\d+)', st.cursor)
                    if m:
                        cur_window = int(m.group(1))
                except Exception:
                    pass
            lag_hours = None
            if st.last_until_ts:
                lag_hours = (now - st.last_until_ts).total_seconds()/3600.0
            desired = cur_window
            if lag_hours and lag_hours > target_catchup_hours:
                # scale up proportionally but bounded
                scale = min(3, max(1.2, lag_hours/target_catchup_hours))
                desired = int(min(max_window_minutes, cur_window * scale))
            # shrink on failures
            if st.failure_count and st.failure_count > 0 and st.updated_at and (now - st.updated_at).total_seconds() < 3600:
                desired = int(max(min_window_minutes, cur_window/2))
            desired = max(min_window_minutes, min(max_window_minutes, desired))
            if desired != cur_window or (st.cursor and 'window:' not in st.cursor) or not st.cursor:
                # rewrite cursor metadata (preserving existing base cursor if any before first |)
                base_cursor = st.cursor.split('|',1)[0] if st.cursor else ''
                st.cursor = (base_cursor + '|' if base_cursor else '') + f'window:{desired}'
                updated[st.connector_name] = desired
            try:
                ADAPTIVE_WINDOW_SECONDS.labels(st.connector_name).set(desired*60)
            except Exception:
                pass
        session.commit()
        return {"status": "ok", "windows": updated}
    finally:
        session.close()
