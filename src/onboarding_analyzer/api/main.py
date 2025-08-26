from fastapi import FastAPI, Depends, Response, Request, HTTPException, Query, Header
from fastapi import WebSocket, WebSocketDisconnect
from sqlalchemy.orm import Session
from onboarding_analyzer.infrastructure.db import SessionLocal, healthcheck
from onboarding_analyzer.models.tables import FunnelMetric, Insight, RawEvent, AuditLog, ModelArtifact, ArchiveCompactedEvent, EventSchema, DataQualitySnapshot, ReportLog, RawEventArchive, ApiKey, OpsMetric, ModelTrainingRun, ModelDriftMetric, SchemaDriftAlert, ClusterLabel, InsightFeedback, DataQualityThreshold, DataQualityAlert, ObservedProperty, RetentionPolicy, UserIdentityMap, EventNameMap, NormalizationRule, FeatureDefinition, FeatureView, Role, RoleAssignment, UserRecommendation, DataAsset, DataLineageEdge, PersonalizationRule, ModelDriftThreshold, DriftRetrainAudit, ExperimentDefinition, ExperimentAssignment, FrictionCluster, SuggestionPattern, InsightSuppression, ConnectorState, ModelVersion, MaintenanceJob, ProjectQuota, ModelEvaluation, EventSchemaVersion, AnomalyEvent, ConsentRecord, CostUsage, RecommendationFeedback, SegmentDefinition, UserSegmentMembership
from onboarding_analyzer.models.tables import SLODefinition, SLOEvaluation
from onboarding_analyzer.expression import validate_condition_expr
from onboarding_analyzer.feature_store import register_feature, materialize_feature, get_feature_values
from sqlalchemy import inspect, text
from onboarding_analyzer.api.events import router as events_router
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from prometheus_client import CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client import Counter, Histogram, Gauge
import time
import threading
import redis
import secrets
from onboarding_analyzer.config import get_settings, parse_api_keys, parse_api_key_project_map, parse_api_key_scopes
from connectors import run_connector_incremental, run_connector_backfill
from clustering import run_friction_clustering
from recommender import generate_insights, generate_user_recommendations
from onboarding_analyzer.tasks.lifecycle import enforce_retention_policies, schedule_model_retraining
from onboarding_analyzer.tasks.impact import compute_recommendation_impact
from onboarding_analyzer.tasks.lineage import build_lineage_graph, compute_lineage_impact
from onboarding_analyzer.ingest_queue import drain_queue, queue_length
from onboarding_analyzer.security.crypto import encrypt_secret, decrypt_secret, EncryptionError
from onboarding_analyzer.infrastructure.flow_control import flow_controlled, FlowControlThrottledError
from report import compose_weekly_report
from onboarding_analyzer.security.rls import apply_rls_policies
from sqlalchemy.engine import reflection
import logging
import json
import uuid
import hashlib
import hmac
try:
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    _ot_enabled = True
except Exception:
    _ot_enabled = False
if _ot_enabled:
    try:
        trace.set_tracer_provider(TracerProvider(resource=Resource.create({"service.name": "onboarding-analyzer"})))
        provider = trace.get_tracer_provider()
        exporter = OTLPSpanExporter()
        provider.add_span_processor(BatchSpanProcessor(exporter))
        _tracer = trace.get_tracer(__name__)
    except Exception:
        _ot_enabled = False

registry = CollectorRegistry()
REQUESTS = Counter('api_requests_total', 'API Requests', ['endpoint'], registry=registry)
LATENCY = Histogram('api_request_latency_seconds', 'API request latency', ['endpoint'], registry=registry, buckets=(0.01,0.05,0.1,0.25,0.5,1,2,5))
RATE_LIMIT_HITS = Counter('api_rate_limit_hits_total', 'Rate limit rejections', ['identifier'], registry=registry)
RATE_LIMIT_USAGE = Gauge('api_rate_limit_usage', 'Requests this minute', ['identifier'], registry=registry)
ARCHIVE_COMPACT_EVENTS = Counter('archive_compacted_events_total', 'Events compacted into archive buckets', registry=registry)
ARCHIVE_COMPACT_DAYS = Counter('archive_compacted_days_total', 'Days compacted', registry=registry)
from typing import List, Any, Dict
from sqlalchemy import func

app = FastAPI(title="Onboarding Drop-off Analyzer API", version="0.1.0")
app.include_router(events_router)

# Backwards compatible simple functions expected by legacy tests
def run_archive(days: int = 30):  # pragma: no cover - thin wrapper
    from onboarding_analyzer.tasks.maintenance import run_archive as _run
    return _run(days=days)

def archive_stats():  # pragma: no cover - thin wrapper
    with SessionLocal() as s:
        raw_events = s.query(func.count(RawEvent.id)).scalar() or 0
        archived = s.query(func.count(RawEventArchive.id)).scalar() or 0
        compacted = s.query(func.count(ArchiveCompactedEvent.id)).scalar() or 0
    return {"raw_events": raw_events, "archived_events": archived, "compacted_rows": compacted}

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    cid = getattr(request.state, "correlation_id", "n/a")
    logging.getLogger("app").error(json.dumps({
        "event": "error",
        "path": request.url.path,
        "detail": str(exc),
        "correlation_id": cid,
        "type": exc.__class__.__name__,
    }))
    return Response(content=json.dumps({"error": "internal_error", "correlation_id": cid}), media_type="application/json", status_code=500)
@app.middleware("http")
async def metrics_and_rate_limit(request: Request, call_next):
    settings = get_settings()
    provided_key = request.headers.get("X-API-Key")
    # Dynamic API key cache (DB backed); fallback to env list if table empty
    now_ts = time.time()
    cache_ttl = 30
    if not hasattr(metrics_and_rate_limit, "_api_key_cache"):
        metrics_and_rate_limit._api_key_cache = {"keys": {}, "loaded_at": 0.0}  # type: ignore
    cache = metrics_and_rate_limit._api_key_cache  # type: ignore
    if (now_ts - cache["loaded_at"]) > cache_ttl:
        try:
            db = SessionLocal()
            rows = db.query(ApiKey).filter(ApiKey.active == 1).all()
            updated = False
            key_map: dict[str, dict] = {}
            for r in rows:
                # Backfill hashed_key if missing
                if not r.hashed_key and r.key:
                    try:
                        r.hashed_key = hashlib.sha256(r.key.encode()).hexdigest()
                        updated = True
                    except Exception:
                        pass
                display_key = r.key  # legacy direct lookup
                if display_key:
                    key_map[display_key] = {"project": r.project, "scopes": set(r.scopes.split("|")), "_hashed": r.hashed_key}
                if r.hashed_key:
                    key_map[r.hashed_key] = {"project": r.project, "scopes": set(r.scopes.split("|")), "_hash_only": True}
            if updated:
                try:
                    db.commit()
                except Exception:
                    db.rollback()
            cache["keys"] = key_map
            cache["loaded_at"] = now_ts
            db.close()
        except Exception:
            pass
    db_keys: dict = cache.get("keys", {})
    if not db_keys:  # fallback
        env_keys = parse_api_keys(settings.api_keys)
        db_keys = {k: {"project": None, "scopes": parse_api_key_scopes(settings.api_key_scopes).get(k, {"read","ingest"})} for k in env_keys}
    # For legacy mapping config add project if missing
    project_map = parse_api_key_project_map(settings.api_key_project_map)
    for k,v in project_map.items():
        if k in db_keys and not db_keys[k]["project"]:
            db_keys[k]["project"] = v
    # Simple API key auth (skip for health & metrics endpoints)
    if request.url.path not in ("/health", "/ready", "/metrics") and db_keys and get_settings().app_env != "test":
        effective_key = None
        if provided_key:
            # Prefer hashed lookup
            try:
                h = hashlib.sha256(provided_key.encode()).hexdigest()
                if h in db_keys:
                    effective_key = h
                elif provided_key in db_keys:
                    effective_key = provided_key
            except Exception:
                if provided_key in db_keys:
                    effective_key = provided_key
        if not effective_key:
            raise HTTPException(status_code=401, detail="invalid API key")
        provided_key = effective_key  # use canonical lookup key
        # Scope enforcement (simple mapping of path to required scope)
        required_scope = None
        path = request.url.path
        if path.startswith('/events'):
            required_scope = 'ingest'
        elif path.startswith(('/funnel','/insights','/clusters')):
            required_scope = 'read'
        elif path.startswith('/connectors'):
            required_scope = 'connectors'
        elif path.startswith('/clustering'):
            required_scope = 'clustering'
        elif path.startswith('/recommendations/impact'):
            required_scope = 'impact'
        elif path.startswith('/recommendations'):
            required_scope = 'recommend'
        elif path.startswith('/patterns'):
            required_scope = 'patterns'
        elif path.startswith('/report/weekly'):
            required_scope = 'report'
        elif path.startswith('/lineage'):
            required_scope = 'lineage'
        elif path.startswith('/ingest/queue'):
            required_scope = 'queue'
        elif path.startswith('/lifecycle'):
            required_scope = 'lifecycle'
        elif path.startswith('/ops'):
            required_scope = 'lifecycle'
        elif path.startswith('/datasets'):
            required_scope = 'read'
        elif path.startswith('/secrets/audit'):
            required_scope = 'connectors'
        elif path.startswith('/governance'):
            required_scope = 'analytics'
        if required_scope:
            key_scopes = set(db_keys.get(provided_key, {}).get("scopes", {"read","ingest"}))
            # Merge role-based scopes if any
            try:
                db = SessionLocal()
                from onboarding_analyzer.models.tables import ApiKey as _ApiKey, RoleAssignment as _RA, Role as _Role
                # provided_key may be hashed; match on either key or hashed_key
                krow = db.query(_ApiKey).filter((_ApiKey.key == provided_key) | (_ApiKey.hashed_key == provided_key)).first()
                if krow:
                    ras = db.query(_RA, _Role).join(_Role, _RA.role_id==_Role.id).filter(_RA.api_key_id==krow.id, _RA.active==1, _Role.active==1).all()
                    for ra, role in ras:
                        for s in role.scopes.split('|'):
                            if s:
                                key_scopes.add(s)
                db.close()
            except Exception:
                pass
            if required_scope not in key_scopes:
                raise HTTPException(status_code=403, detail="forbidden:missing_scope")

    # Rate limiting: prefer per API key else global ingest secret
    # Per-project rate limiting if project mapping exists
    project = db_keys.get(provided_key, {}).get("project") if provided_key else project_map.get(provided_key) if provided_key else None
    # Expose project for downstream handlers
    request.state.project = project
    rl_identifier = project or (provided_key if provided_key else getattr(settings, "ingest_secret", None))
    per_minute = settings.api_key_rate_limit_per_minute if provided_key else settings.rate_limit_per_minute
    if rl_identifier and per_minute > 0:
        try:
            r = redis.Redis.from_url(settings.redis_url)
            bucket = f"ratelimit:{rl_identifier}:{int(time.time()//60)}"
            current = r.incr(bucket, 1)
            if current == 1:
                r.expire(bucket, 60)
            try:
                RATE_LIMIT_USAGE.labels(rl_identifier).set(current)
            except Exception:
                pass
            if current > per_minute:
                try:
                    RATE_LIMIT_HITS.labels(rl_identifier).inc()
                except Exception:
                    pass
                raise HTTPException(status_code=429, detail="rate limit exceeded")
        except Exception:
            pass  # fail open
    start = time.time()
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id
    if _ot_enabled:
        with _tracer.start_as_current_span(f"http {request.method} {request.url.path}") as span:  # type: ignore
            span.set_attribute("http.method", request.method)
            span.set_attribute("http.target", request.url.path)
            response = await call_next(request)
            span.set_attribute("http.status_code", response.status_code)
    else:
        response = await call_next(request)
    duration = time.time() - start
    ep = request.url.path
    try:
        REQUESTS.labels(endpoint=ep).inc()
        LATENCY.labels(endpoint=ep).observe(duration)
    except Exception:
        pass
    if 'X-Process-Time' not in response.headers:
        response.headers['X-Process-Time'] = f"{duration:.4f}"
    response.headers['X-Correlation-ID'] = correlation_id
    try:
        logging.getLogger("app").info(json.dumps({
            "event": "request",
            "path": request.url.path,
            "method": request.method,
            "status": response.status_code,
            "duration_ms": int(duration*1000),
            "correlation_id": correlation_id
        }))
    except Exception:
        pass
    # Persist minimal audit record (best-effort, ignore errors)
    try:
        db = SessionLocal()
        api_key = provided_key if 'provided_key' in locals() else None
        proj_val = db_keys.get(api_key, {}).get("project") if api_key else None
        ep_path = request.url.path
        # Categorize endpoint
        if ep_path.startswith('/events'):
            cat = 'ingest'
        elif ep_path.startswith('/connectors'):
            cat = 'connector'
        elif ep_path.startswith('/clustering') or ep_path.startswith('/clusters'):
            cat = 'clustering'
        elif ep_path.startswith('/recommendations'):
            cat = 'recommend'
        elif ep_path.startswith('/lineage'):
            cat = 'lineage'
        elif ep_path.startswith('/ingest/queue'):
            cat = 'queue'
        elif ep_path.startswith('/lifecycle'):
            cat = 'lifecycle'
        elif ep_path.startswith('/funnel'):
            cat = 'analytics'
        elif ep_path.startswith('/insights'):
            cat = 'insight'
        elif ep_path.startswith('/report'):
            cat = 'report'
        else:
            cat = 'other'
        db.add(AuditLog(endpoint=ep_path, method=request.method, status=response.status_code, api_key=api_key, project=proj_val, duration_ms=int(duration*1000), category=cat))
        db.commit()
        db.close()
    except Exception:
        pass
    # Security headers (baseline; CSP left permissive for API JSON)
    response.headers.setdefault('X-Content-Type-Options', 'nosniff')
    response.headers.setdefault('X-Frame-Options', 'DENY')
    response.headers.setdefault('X-XSS-Protection', '1; mode=block')
    response.headers.setdefault('Referrer-Policy', 'no-referrer')
    response.headers.setdefault('Cache-Control', 'no-store')
    response.headers.setdefault('Strict-Transport-Security', 'max-age=63072000; includeSubDomains; preload')
    return response


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Redis-backed funnel cache; fall back to in-process if Redis unavailable
_funnel_cache = {"data": None, "ts": 0.0}
_funnel_lock = threading.Lock()


@app.on_event("startup")
def startup():
    settings = get_settings()
    # Configure structured logger once
    logger = logging.getLogger("app")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(message)s')  # already JSON
        handler.setFormatter(formatter)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
    # If configured, attempt Alembic upgrade (idempotent) for convenience in local dev
    if getattr(settings, 'migrate_on_start', False):
        try:
            import subprocess
            subprocess.run(["alembic", "upgrade", "head"], check=True)
        except Exception:
            logging.getLogger("app").warning(json.dumps({"event":"migration_failed","detail":"startup alembic upgrade failed"}))
    # Apply RLS policies (no-op if disabled / not postgres)
    try:
        apply_rls_policies()
    except Exception:
        logging.getLogger("app").warning(json.dumps({"event":"rls_apply_failed"}))
    # Basic OpenTelemetry tracing if exporter endpoint is set
    otlp_endpoint = getattr(settings, 'otlp_endpoint', None)
    if _ot_enabled and otlp_endpoint:
        try:
            provider = TracerProvider(resource=Resource.create({"service.name": "onboarding-analyzer"}))
            processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint))
            provider.add_span_processor(processor)
            trace.set_tracer_provider(provider)
        except Exception:
            pass


@app.get("/health")
def health():
    return {"db": healthcheck(), "status": "ok"}


@app.get("/ready")
def readiness():
    """Readiness probe that ensures DB and Redis are reachable."""
    settings = get_settings()
    db_ok = healthcheck()
    redis_ok = True
    try:
        r = redis.Redis.from_url(settings.redis_url)
        r.ping()
    except Exception:
        redis_ok = False
    status = db_ok and redis_ok
    return {"status": "ok" if status else "degraded", "db": db_ok, "redis": redis_ok}


@app.get("/metrics")
def metrics():
    data = generate_latest(registry)
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


class FunnelMetricOut(BaseModel):
    step_name: str
    step_order: int
    users_entered: int
    users_converted: int
    drop_off: int
    conversion_rate: float

    class Config:
        from_attributes = True


class DataQualityOut(BaseModel):
    metric_type: str
    event_name: str | None
    project: str | None
    metric_value: float

class QuotaIn(BaseModel):
    project: str
    daily_event_limit: int | None = None
    monthly_event_limit: int | None = None
    enforced: int = 1

class ModelEvalIn(BaseModel):
    model_name: str
    candidate_version: str
    baseline_version: str | None = None
    metric_name: str
    candidate_score: float
    baseline_score: float | None = None
    threshold: float | None = None
    auto_decide: bool = True
    promote_if: str | None = None  # gt|lt
    details: dict | None = None

class SecretRotateIn(BaseModel):
    connector_name: str
    secret_key: str
    plaintext: str

class DSARExportOut(BaseModel):
    user_id: str
    project: str | None
    raw_events: int
    recommendations: int
    identities: int
    features: int | None
    payload: dict

class SLODefIn(BaseModel):
    name: str
    metric: str
    target: float
    window_hours: int = 24
    threshold: float | None = None
    active: int = 1
    description: str | None = None

class SLOEvalOut(BaseModel):
    slo_id: int
    attained: float
    target: float
    breach: bool
    window_start: datetime
    window_end: datetime
    captured_at: datetime
    class Config:
        from_attributes = True

class SchemaVersionIn(BaseModel):
    event_name: str
    spec: dict
    author: str | None = None

class SchemaApproveIn(BaseModel):
    event_name: str
    version: int

class RecommendationFeedbackIn(BaseModel):
    recommendation: str
    user_id: str
    project: str | None = None
    feedback: str  # accept|dismiss|convert
    metadata: dict | None = None

class ConsentUpsertIn(BaseModel):
    user_id: str
    project: str | None = None
    policy_version: str
    granted: int = 1
    source: str | None = None

class RawExportQuery(BaseModel):
    since: datetime
    until: datetime
    project: str | None = None
    limit: int = 10000
    cursor: str | None = None  # pagination cursor (ts|id)

class RawImportIn(BaseModel):
    events: list[dict]

class CostUsageOut(BaseModel):
    project: str | None
    window_start: datetime
    window_end: datetime
    event_count: int
    storage_bytes: int
    compute_seconds: float
    cost_estimate: float | None
class SegmentDefIn(BaseModel):
    key: str
    expression: dict
    description: str | None = None
    active: int = 1

class SegmentDefOut(BaseModel):
    key: str
    expression: dict
    description: str | None
    active: int
    created_at: datetime
    updated_at: datetime
    class Config:
        from_attributes = True

class SegmentMembershipOut(BaseModel):
    segment_key: str
    user_id: str
    project: str | None
    computed_at: datetime
    class Config:
        from_attributes = True


class EventSchemaIn(BaseModel):
    required_props: dict = {}
    min_version: str = "v1"
    status: str = "draft"
    event_name: str | None = None  # optional for activation endpoint


class FeatureDefinitionIn(BaseModel):
    feature_key: str
    entity: str = "user"
    expr: str
    version: str = "v1"
    description: str | None = None


class FeatureDefinitionOut(BaseModel):
    feature_key: str
    entity: str
    expr: str
    version: str
    status: str
    description: str | None
    created_at: datetime
    updated_at: datetime
    class Config:
        from_attributes = True


class FeatureViewOut(BaseModel):
    feature_key: str
    entity_id: str
    feature_version: str
    value: dict
    computed_at: datetime
    class Config:
        from_attributes = True


class ApiKeyOut(BaseModel):
    key: str
    project: str | None
    scopes: list[str]

class ApiKeyRotateIn(BaseModel):
    scopes: list[str] | None = None
    project: str | None = None

class DriftThresholdIn(BaseModel):
    model_name: str
    metric_name: str
    comparison: str = Field(pattern="^(gt|ge|lt|le)$")
    boundary: float
    action: str = Field(default="retrain")  # retrain|alert
    cooldown_hours: int = 24
    active: int = 1

class DriftThresholdOut(BaseModel):
    id: int
    model_name: str
    metric_name: str
    comparison: str
    boundary: float
    action: str
    cooldown_hours: int
    active: int
    last_triggered_at: datetime | None
    created_at: datetime
    class Config:
        from_attributes = True

@app.post("/api-keys/{key}/rotate", response_model=ApiKeyOut)
def rotate_api_key(key: str, body: ApiKeyRotateIn, db: Session = Depends(get_db)):
    import secrets, hashlib
    row = db.query(ApiKey).filter((ApiKey.key==key) | (ApiKey.hashed_key==key)).first()
    if not row:
        raise HTTPException(status_code=404, detail="not_found")
    new_key = secrets.token_urlsafe(32)
    row.key = new_key
    row.hashed_key = hashlib.sha256(new_key.encode()).hexdigest()
    if body.scopes is not None:
        row.scopes = "|".join(body.scopes)
    if body.project is not None:
        row.project = body.project
    db.commit(); db.refresh(row)
    return ApiKeyOut(key=new_key, project=row.project, scopes=row.scopes.split("|"))

class ConnectorCredentialIn(BaseModel):
    secret_key: str
    secret_value: str
    active: int = 1

@app.post("/connectors/{name}/credentials")
def upsert_connector_credential(name: str, body: ConnectorCredentialIn, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import ConnectorCredential
    try:
        enc = encrypt_secret(body.secret_value)
    except EncryptionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    row = db.query(ConnectorCredential).filter(ConnectorCredential.connector_name==name, ConnectorCredential.secret_key==body.secret_key).first()
    if not row:
        row = ConnectorCredential(connector_name=name, secret_key=body.secret_key, encrypted_value=enc, active=body.active)
        db.add(row)
    else:
        row.encrypted_value = enc
        row.version += 1
        row.rotated_at = datetime.utcnow()
        if body.active is not None:
            row.active = body.active
    db.commit(); db.refresh(row)
    return {"connector": name, "secret_key": row.secret_key, "version": row.version, "active": row.active}

@app.post("/connectors/{name}/credentials/rotate")
def rotate_connector_credential(name: str, secret_key: str, new_value: str, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import ConnectorCredential
    row = db.query(ConnectorCredential).filter(ConnectorCredential.connector_name==name, ConnectorCredential.secret_key==secret_key).first()
    if not row:
        raise HTTPException(status_code=404, detail="not_found")
    try:
        enc = encrypt_secret(new_value)
    except EncryptionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    row.encrypted_value = enc
    row.version += 1
    row.rotated_at = datetime.utcnow()
    db.commit(); db.refresh(row)
    return {"connector": name, "secret_key": row.secret_key, "version": row.version}
    created_at: datetime
    class Config:
        from_attributes = True

class RoleIn(BaseModel):
    name: str
    scopes: list[str]
    description: str | None = None

class RoleOut(BaseModel):
    name: str
    scopes: list[str]
    description: str | None
    active: int
    created_at: datetime
    class Config:
        from_attributes = True

class RoleAssignIn(BaseModel):
    api_key: str
    role: str

class ConnectorRunOut(BaseModel):
    fetched: int | None = None
    since: str | None = None
    until: str | None = None
    job_id: int | None = None
    status: str | None = None
    total_events: int | None = None

class PatternIn(BaseModel):
    key: str
    description: str
    recommendation: str
    category: str = 'general'
    weight: float = 1.0

class PatternOut(BaseModel):
    key: str
    description: str
    recommendation: str
    category: str
    weight: float
    created_at: datetime
    class Config:
        from_attributes = True

class WeeklyReportOut(BaseModel):
    clusters: list[dict]
    insights: list[dict]
    recommendation_metrics: dict
    feedback: dict
    text: str

class FeedbackIn(BaseModel):
    feedback: str  # up|down

class SuppressIn(BaseModel):
    reason: str | None = None
    pattern_key: str | None = None
    expires_hours: int | None =  None

class RecActionIn(BaseModel):
    status: str  # accepted|dismissed|converted

@app.post("/security/roles", response_model=RoleOut)
def create_role(body: RoleIn, db: Session = Depends(get_db)):
    existing = db.query(Role).filter(Role.name == body.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="exists")
    r = Role(name=body.name, scopes="|".join(sorted(set(body.scopes))), description=body.description)
    db.add(r)
    db.commit()
    db.refresh(r)
    return RoleOut(name=r.name, scopes=r.scopes.split("|"), description=r.description, active=r.active, created_at=r.created_at)

@app.post("/connectors/{name}/run", response_model=ConnectorRunOut)
def run_connector(name: str):
    return run_connector_incremental(name)

@app.post("/connectors/{name}/backfill", response_model=ConnectorRunOut)
def run_backfill(name: str, start: datetime, end: datetime):
    return run_connector_backfill(name, start, end)

@app.post("/clustering/run")
def run_clustering(request: Request):
    project = getattr(request.state, 'project', None)
    return run_friction_clustering(project=project)

@app.post("/recommendations/insights/generate")
def gen_insights(request: Request):
    project = getattr(request.state, 'project', None)
    return generate_insights(project=project)

@app.post("/recommendations/users/generate")
def gen_user_recs(request: Request):
    project = getattr(request.state, 'project', None)
    return generate_user_recommendations(project=project)

@app.post("/lifecycle/retention/enforce")
def run_retention_enforcement():
    return enforce_retention_policies()

@app.post("/lifecycle/models/drift/schedule")
def run_drift_schedule():
    return schedule_model_retraining()

@app.post("/recommendations/impact/compute")
def compute_impact(request: Request):
    project = getattr(request.state, 'project', None)
    return compute_recommendation_impact(project=project)

@app.get("/recommendations/impact", response_model=list[dict])
def list_impact(request: Request, recommendation: str | None = None, db: Session = Depends(get_db), limit: int = 100):
    from onboarding_analyzer.models.tables import RecommendationImpact
    q = db.query(RecommendationImpact)
    project = getattr(request.state, 'project', None)
    if project:
        q = q.filter(RecommendationImpact.project==project)
    if recommendation:
        q = q.filter(RecommendationImpact.recommendation==recommendation)
    rows = q.order_by(RecommendationImpact.window_start.desc()).limit(limit).all()
    return [
        {
            "recommendation": r.recommendation,
            "window_start": r.window_start,
            "window_end": r.window_end,
            "users_exposed": r.users_exposed,
            "users_converted": r.users_converted,
            "baseline_users": r.baseline_users,
            "baseline_converted": r.baseline_converted,
            "conversion_rate": r.conversion_rate,
            "baseline_rate": r.baseline_rate,
            "lift": r.lift,
            "computed_at": r.computed_at,
        } for r in rows
    ]

@app.post("/lineage/build")
def lineage_build():
    return build_lineage_graph()

@app.post("/lineage/impact")
def lineage_impact():
    return compute_lineage_impact()

@app.get("/lineage/assets", response_model=list[dict])
def lineage_assets(request: Request, db: Session = Depends(get_db), limit: int = 500):
    from onboarding_analyzer.models.tables import DataAsset
    q = db.query(DataAsset)
    project = getattr(request.state, 'project', None)
    if project:
        q = q.filter(DataAsset.project==project)
    rows = q.order_by(DataAsset.created_at.desc()).limit(limit).all()
    return [{"asset_key": r.asset_key, "type": r.asset_type, "created_at": r.created_at, "project": r.project} for r in rows]

@app.get("/lineage/edges", response_model=list[dict])
def lineage_edges(request: Request, db: Session = Depends(get_db), limit: int = 1000):
    from onboarding_analyzer.models.tables import DataLineageEdge
    q = db.query(DataLineageEdge)
    project = getattr(request.state, 'project', None)
    if project:
        q = q.filter(DataLineageEdge.project==project)
    rows = q.order_by(DataLineageEdge.created_at.desc()).limit(limit).all()
    return [{"upstream": r.upstream_key, "downstream": r.downstream_key, "transform": r.transform, "created_at": r.created_at, "project": r.project} for r in rows]

@app.post("/ingest/queue/drain")
@flow_controlled("db_operations", timeout=5.0)
def ingest_queue_drain(batch_size: int = 500):
    return drain_queue(batch_size=batch_size)

@app.get("/ingest/queue/length")
def ingest_queue_length():
    return queue_length()

@app.post("/ingest/dlq/reprocess")
@flow_controlled("db_operations", timeout=5.0)
def ingest_dlq_reprocess(limit: int = 100):
    from onboarding_analyzer.tasks.ingestion import reprocess_dead_letters
    return reprocess_dead_letters(limit=limit)

@app.get("/ratelimit/usage", response_model=dict)
def ratelimit_usage(identifier: str):
    """Return approximate current minute usage counter for an identifier (project or key hash)."""
    settings = get_settings()
    try:
        r = redis.Redis.from_url(settings.redis_url)
        bucket = f"ratelimit:{identifier}:{int(time.time()//60)}"
        val = r.get(bucket)
        return {"identifier": identifier, "current_minute": int(val) if val else 0}
    except Exception:
        raise HTTPException(status_code=400, detail="unavailable")

@app.post("/patterns", response_model=PatternOut)
def create_pattern(body: PatternIn, db: Session = Depends(get_db)):
    existing = db.query(SuggestionPattern).filter(SuggestionPattern.key==body.key).first()
    if existing:
        raise HTTPException(status_code=409, detail="exists")
    p = SuggestionPattern(key=body.key, description=body.description, recommendation=body.recommendation, category=body.category, weight=body.weight)
    db.add(p); db.commit(); db.refresh(p)
    return p

@app.get("/patterns", response_model=list[PatternOut])
def list_patterns(db: Session = Depends(get_db)):
    return db.query(SuggestionPattern).all()

@app.get("/report/weekly", response_model=WeeklyReportOut)
def weekly_report():
    r = compose_weekly_report()
    return WeeklyReportOut(**{k:v for k,v in r.items() if k in {'clusters','insights','recommendation_metrics','feedback','text'}})

@app.get("/clusters", response_model=list[dict])
def list_clusters(request: Request, db: Session = Depends(get_db), limit: int = Query(50, le=500)):
    project = getattr(request.state, 'project', None)
    q = db.query(FrictionCluster)
    if project:
        q = q.filter(FrictionCluster.project==project)
    rows = q.order_by(FrictionCluster.impact_score.desc()).limit(limit).all()
    return [{"label": r.label, "impact": r.impact_score, "size": r.size, "drop_off_users": r.drop_off_users, "features": r.features_summary} for r in rows]

@app.post("/insights/{insight_id}/feedback")
def insight_feedback(insight_id: int, body: FeedbackIn, db: Session = Depends(get_db)):
    ins = db.query(Insight).filter(Insight.id==insight_id).first()
    if not ins:
        raise HTTPException(status_code=404, detail="not_found")
    fb = InsightFeedback(insight_id=insight_id, feedback=body.feedback)
    db.add(fb); db.commit()
    return {"status":"ok"}

@app.post("/insights/{insight_id}/suppress")
def suppress_insight(insight_id: int, body: SuppressIn, db: Session = Depends(get_db)):
    ins = db.query(Insight).filter(Insight.id==insight_id).first()
    if not ins:
        raise HTTPException(status_code=404, detail="not_found")
    expires_at = None
    if body.expires_hours:
        expires_at = datetime.utcnow() + timedelta(hours=body.expires_hours)
    sup = InsightSuppression(cluster_id=ins.cluster_id, pattern_key=body.pattern_key, reason=body.reason, expires_at=expires_at)
    db.add(sup); db.commit(); db.refresh(sup)
    return {"status":"ok","suppression_id": sup.id}

@app.post("/recommendations/{rec_id}/action")
def recommendation_action(rec_id: int, body: RecActionIn, db: Session = Depends(get_db)):
    rec = db.query(UserRecommendation).filter(UserRecommendation.id==rec_id).first()
    if not rec:
        raise HTTPException(status_code=404, detail="not_found")
    if body.status not in ("accepted","dismissed","converted"):
        raise HTTPException(status_code=400, detail="invalid_status")
    rec.status = body.status
    if body.status in ("accepted","converted"):
        rec.acted_at = datetime.utcnow()
    db.commit()
    return {"status":"ok"}

@app.get("/recommendations/users", response_model=list[dict])
def list_user_recommendations(user_id: str, db: Session = Depends(get_db)):
    rows = db.query(UserRecommendation).filter(UserRecommendation.user_id==user_id).all()
    return [{"id": r.id, "recommendation": r.recommendation, "status": r.status, "created_at": r.created_at, "rationale": r.rationale} for r in rows]

@app.get("/connectors/state", response_model=list[dict])
def connector_state(db: Session = Depends(get_db)):
    rows = db.query(ConnectorState).all()
    return [{"name": r.connector_name, "last_since": r.last_since_ts, "last_until": r.last_until_ts, "cursor": r.cursor, "failures": r.failure_count, "last_error": r.last_error} for r in rows]

@app.get("/security/roles", response_model=list[RoleOut])
def list_roles(db: Session = Depends(get_db)):
    roles = db.query(Role).filter(Role.active == 1).all()
    return [RoleOut(name=r.name, scopes=r.scopes.split("|"), description=r.description, active=r.active, created_at=r.created_at) for r in roles]

@app.post("/security/roles/assign")
def assign_role(body: RoleAssignIn, db: Session = Depends(get_db)):
    key = db.query(ApiKey).filter(ApiKey.key == body.api_key).first()
    if not key:
        raise HTTPException(status_code=404, detail="api_key_not_found")
    role = db.query(Role).filter(Role.name == body.role, Role.active == 1).first()
    if not role:
        raise HTTPException(status_code=404, detail="role_not_found")
    existing = db.query(RoleAssignment).filter(RoleAssignment.api_key_id==key.id, RoleAssignment.role_id==role.id).first()
    if existing:
        return {"status": "exists"}
    ra = RoleAssignment(api_key_id=key.id, role_id=role.id)
    db.add(ra)
    db.commit()
    return {"status": "assigned"}

@app.get("/security/roles/effective_scopes")
def effective_scopes(api_key: str, db: Session = Depends(get_db)):
    key = db.query(ApiKey).filter(ApiKey.key == api_key).first()
    if not key:
        raise HTTPException(status_code=404, detail="api_key_not_found")
    scope_set = set(key.scopes.split("|"))
    ras = db.query(RoleAssignment, Role).join(Role, RoleAssignment.role_id==Role.id).filter(RoleAssignment.api_key_id==key.id, RoleAssignment.active==1, Role.active==1).all()
    for ra, role in ras:
        for s in role.scopes.split("|"):
            if s:
                scope_set.add(s)
    return {"api_key": api_key, "scopes": sorted(scope_set)}


class QueryRequest(BaseModel):
    select: list[str]
    where: str | None = None
    since: datetime | None = None
    until: datetime | None = None
    limit: int | None = None
    order_by: str | None = None

class QueryResult(BaseModel):
    columns: list[str]
    rows: list[list]
    row_count: int

RECENT_EVENTS_BUFFER: list[dict] = []  # ring buffer for websocket broadcast
RECENT_EVENTS_MAX = 500
import asyncio
_ws_subscribers: set[WebSocket] = set()
_ws_lock = asyncio.Lock()

ALLOWED_QUERY_COLUMNS = {"event_name","user_id","session_id","project","ts"}

def _parse_where(expr: str):  # tiny expression parser (column op value AND ...)
    # Support patterns: col = 'x', col != 'y', col like 'foo%', col in ('a','b') joined by AND
    import re
    tokens = [t.strip() for t in re.split(r"AND", expr, flags=re.IGNORECASE) if t.strip()]
    clauses = []
    params: dict[str, object] = {}
    for i,tok in enumerate(tokens):
        # in
        m = re.match(r"(\w+)\s+in\s*\(([^)]+)\)", tok, flags=re.IGNORECASE)
        if m:
            col, inner = m.groups()
            if col not in ALLOWED_QUERY_COLUMNS:
                raise HTTPException(status_code=400, detail=f"disallowed column {col}")
            vals = [v.strip().strip("'\"") for v in inner.split(',')]
            key = f"p{i}"
            clauses.append((col, 'in', vals, key))
            continue
        m = re.match(r"(\w+)\s+(=|!=|like)\s+(.+)", tok, flags=re.IGNORECASE)
        if m:
            col, op, val = m.groups()
            if col not in ALLOWED_QUERY_COLUMNS:
                raise HTTPException(status_code=400, detail=f"disallowed column {col}")
            val = val.strip().strip("'\"")
            key = f"p{i}"
            clauses.append((col, op.lower(), val, key))
            continue
        raise HTTPException(status_code=400, detail=f"cannot parse token: {tok}")
    return clauses

@app.post("/query", response_model=QueryResult)
def run_query(q: QueryRequest, db: Session = Depends(get_db), x_api_key: str | None = Header(None)):
    settings = get_settings()
    # Validate columns
    if not q.select:
        raise HTTPException(status_code=400, detail="select required")
    for col in q.select:
        if col not in ALLOWED_QUERY_COLUMNS and not col.startswith('props.'):
            raise HTTPException(status_code=400, detail=f"disallowed select column {col}")
    # Time bounds
    since = q.since or (datetime.utcnow() - timedelta(days=1))
    until = q.until or datetime.utcnow()
    if (until - since).days > settings.query_max_interval_days:
        raise HTTPException(status_code=400, detail="interval too large")
    limit = min(q.limit or 500, settings.query_max_rows)
    # Base query
    base = db.query(RawEvent)
    base = base.filter(RawEvent.ts >= since, RawEvent.ts <= until)
    # Project scoping (middleware sets request.state.project but here we re-derive from key when direct task call)
    # Light re-check: if x_api_key header provided and project mapping exists in env.
    project_map = parse_api_key_project_map(settings.api_key_project_map)
    if x_api_key and x_api_key in project_map:
        base = base.filter(RawEvent.project == project_map[x_api_key])
    # Where parsing
    if q.where:
        clauses = _parse_where(q.where)
        from sqlalchemy import or_, and_
        conds = []
        for col, op, val, key in clauses:
            col_attr = getattr(RawEvent, col)
            if op == 'in':
                conds.append(col_attr.in_(val))
            elif op == '=':
                conds.append(col_attr == val)
            elif op == '!=':
                conds.append(col_attr != val)
            elif op == 'like':
                conds.append(col_attr.like(val))
        if conds:
            base = base.filter(and_(*conds))
    # Order by
    if q.order_by:
        direction = 'asc'
        col = q.order_by
        if col.lower().endswith(' desc'):
            direction = 'desc'
            col = col[:-5].strip()
        if col not in ALLOWED_QUERY_COLUMNS:
            raise HTTPException(status_code=400, detail=f"order_by disallowed column {col}")
        attr = getattr(RawEvent, col)
        base = base.order_by(attr.desc() if direction=='desc' else attr.asc())
    else:
        base = base.order_by(RawEvent.ts.desc())
    rows = base.limit(limit).all()
    out_rows: list[list] = []
    for r in rows:
        row = []
        for col in q.select:
            if col.startswith('props.'):
                key = col.split('.',1)[1]
                row.append((r.props or {}).get(key))
            else:
                row.append(getattr(r, col))
        out_rows.append(row)
    return QueryResult(columns=q.select, rows=out_rows, row_count=len(out_rows))


class RecommendationOut(BaseModel):
    id: int
    recommendation: str
    rationale: str | None
    status: str
    created_at: datetime
    expires_at: datetime | None
    impressions: int
    clicks: int
    class Config:
        from_attributes = True

class PersonalizationRuleIn(BaseModel):
    key: str
    condition_expr: str
    recommendation: str
    rationale_template: str | None = None
    cooldown_hours: int = 72
    project: str | None = None
    active: int = 1

    def validate_condition(self):
        ok, reason = validate_condition_expr(self.condition_expr)
        if not ok:
            raise HTTPException(status_code=400, detail=f"invalid_condition:{reason}")
        return self

class PersonalizationRuleOut(PersonalizationRuleIn):
    id: int
    created_at: datetime
    last_triggered_at: datetime | None
    class Config:
        from_attributes = True

@app.post("/personalization/rules", response_model=PersonalizationRuleOut)
def create_personalization_rule(body: PersonalizationRuleIn, db: Session = Depends(get_db)):
    body.validate_condition()
    existing = db.query(PersonalizationRule).filter(PersonalizationRule.key==body.key, PersonalizationRule.project==body.project).first()
    if existing:
        raise HTTPException(status_code=409, detail="rule_exists")
    r = PersonalizationRule(key=body.key, project=body.project, condition_expr=body.condition_expr, recommendation=body.recommendation, rationale_template=body.rationale_template, cooldown_hours=body.cooldown_hours, active=body.active)
    db.add(r); db.commit(); db.refresh(r)
    return r

class DriftThresholdIn(BaseModel):
    model_name: str
    metric_name: str
    comparison: str = 'gt'
    boundary: float
    action: str = 'retrain'
    cooldown_hours: int = 24
    active: int = 1

class DriftThresholdOut(DriftThresholdIn):
    id: int
    created_at: datetime
    last_triggered_at: datetime | None
    class Config:
        from_attributes = True

class DriftActionOut(BaseModel):
    id: int
    model_name: str
    metric_name: str
    metric_value: float
    action: str
    created_at: datetime
    notes: str | None
    class Config:
        from_attributes = True

class ExperimentDefIn(BaseModel):
    key: str
    assignment_prop: str
    variants: list[str]
    traffic_allocation: dict[str,float] | None = None
    hash_salt: str = "exp"
    active: int = 1

class ExperimentDefOut(ExperimentDefIn):
    id: int
    created_at: datetime
    class Config:
        from_attributes = True

class ExperimentAssignmentOut(BaseModel):
    experiment_key: str
    user_id: str
    variant: str
    assigned_at: datetime
    project: str | None
    class Config:
        from_attributes = True

class ExperimentCompositeConfigIn(BaseModel):
    experiment_prop: str
    weights: Dict[str, float]
    guardrails: list[dict] | None = None
    active: int = 1

class ExperimentCompositeConfigOut(ExperimentCompositeConfigIn):
    id: int
    created_at: datetime
    updated_at: datetime
    class Config:
        from_attributes = True

class ExperimentCompositeScoreOut(BaseModel):
    experiment_prop: str
    variant: str
    score: float | None
    disqualified: int
    details: dict
    computed_at: datetime
    class Config:
        from_attributes = True

class ExperimentPromotionAuditOut(BaseModel):
    id: int
    experiment_prop: str
    experiment_key: str | None
    winner_variant: str
    decision: str
    rationale: str | None
    details: dict | None
    created_at: datetime
    class Config:
        from_attributes = True

# ---- Alert rule & log schemas ----
class AlertRuleIn(BaseModel):
    rule_type: str  # experiment_decision|drift|data_quality|experiment_promotion
    condition: dict
    channels: dict | None = None
    cooldown_minutes: int = 60
    active: int = 1

class AlertRuleOut(AlertRuleIn):
    id: int
    last_fired_at: datetime | None
    created_at: datetime
    class Config:
        from_attributes = True

class AlertLogOut(BaseModel):
    id: int
    rule_id: int
    rule_type: str
    message: str
    context: dict | None
    created_at: datetime
    class Config:
        from_attributes = True

class NotificationTemplateIn(BaseModel):
    name: str
    channel: str  # slack|email
    subject: str | None = None
    body: str
    active: int = 1

class NotificationTemplateOut(NotificationTemplateIn):
    id: int
    created_at: datetime
    updated_at: datetime
    class Config:
        from_attributes = True

# ---- Restored legacy endpoints required by tests (insights, funnel, normalization, schema, models) ----

@app.get("/funnel")
def funnel_metrics(limit: int = 50, db: Session = Depends(get_db), request: Request = None):
    q = db.query(FunnelMetric)
    project = getattr(request.state, 'project', None) if request else None
    if project:
        q = q.filter(FunnelMetric.project==project)
    rows = q.order_by(FunnelMetric.step_order.asc()).limit(limit).all()
    return [
        {
            "step_name": r.step_name,
            "step_order": r.step_order,
            "users_entered": r.users_entered,
            "users_converted": r.users_converted,
            "drop_off": r.drop_off,
            "conversion_rate": r.conversion_rate,
        } for r in rows
    ]

@app.get("/insights")
def list_insights(limit: int = 50, offset: int = 0, status: str | None = None, db: Session = Depends(get_db), request: Request = None):
    q = db.query(Insight)
    project = getattr(request.state, 'project', None) if request else None
    if project:
        # Join cluster to enforce project; insight itself not directly project-scoped
        from onboarding_analyzer.models.tables import FrictionCluster as _FC
        q = q.join(_FC, Insight.cluster_id==_FC.id).filter(_FC.project==project)
    if status:
        q = q.filter(Insight.status == status)
    rows = q.order_by(Insight.created_at.desc()).offset(offset).limit(limit).all()
    return [
        {
            "id": r.id,
            "title": r.title,
            "recommendation": r.recommendation,
            "priority": r.priority,
            "impact_score": r.impact_score,
            "status": r.status,
        } for r in rows
    ]

@app.patch("/insights/{insight_id}")
def update_insight(insight_id: int, body: dict, db: Session = Depends(get_db)):
    ins = db.query(Insight).filter(Insight.id==insight_id).first()
    if not ins:
        raise HTTPException(status_code=404, detail="not_found")
    if 'status' in body:
        ins.status = body['status']
        if body['status'] == 'accepted' and not ins.accepted_at:
            ins.accepted_at = datetime.utcnow()
    db.commit(); db.refresh(ins)
    return {"id": ins.id, "status": ins.status}

@app.post("/schemas/activate")
def activate_schema(body: EventSchemaIn, db: Session = Depends(get_db)):
    event_name = body.event_name or 'generic'
    sch = db.query(EventSchema).filter_by(event_name=event_name).first()
    if sch:
        # Update existing schema definition
        sch.required_props = body.required_props or {}
        sch.min_version = body.min_version
        sch.status = body.status
        if body.status == 'approved' and not sch.approved_at:
            sch.approved_at = datetime.utcnow()
    else:
        sch = EventSchema(event_name=event_name, required_props=body.required_props or {}, min_version=body.min_version, status=body.status)
        if body.status == 'approved':
            sch.approved_at = datetime.utcnow()
        db.add(sch)
    db.commit(); db.refresh(sch)
    return {"id": sch.id, "event_name": sch.event_name, "status": sch.status}

@app.post("/schemas/activate/approve")
def approve_latest_schema(event_name: str | None = None, db: Session = Depends(get_db)):
    q = db.query(EventSchema)
    if event_name:
        q = q.filter(EventSchema.event_name==event_name)
    sch = q.order_by(EventSchema.created_at.desc()).first()
    if not sch:
        raise HTTPException(status_code=404, detail="no_schema")
    sch.status = 'approved'
    if not sch.approved_at:
        sch.approved_at = datetime.utcnow()
    db.commit(); db.refresh(sch)
    return {"id": sch.id, "event_name": sch.event_name, "status": sch.status}

@app.get("/schemas")
def list_schemas(limit: int = 200, db: Session = Depends(get_db)):
    rows = db.query(EventSchema).order_by(EventSchema.created_at.desc()).limit(limit).all()
    return [
        {
            "id": r.id,
            "event_name": r.event_name,
            "required_props": r.required_props,
            "min_version": r.min_version,
            "status": r.status,
            "approved_at": r.approved_at,
        }
        for r in rows
    ]

@app.get("/governance/quality", response_model=list[DataQualityOut])
def list_data_quality(hours: int = 24, db: Session = Depends(get_db)):
    since = datetime.utcnow() - timedelta(hours=hours)
    rows = db.query(DataQualitySnapshot).filter(DataQualitySnapshot.captured_at >= since).order_by(DataQualitySnapshot.captured_at.desc()).limit(200).all()
    return rows

@app.post("/normalization/rules")
def create_normalization_rule(body: dict, db: Session = Depends(get_db)):
    rule_type = body.get('rule_type'); payload = body.get('payload'); active = body.get('active',1); author = body.get('author')
    if not rule_type or not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid")
    r = NormalizationRule(rule_type=rule_type, payload=payload, active=active, author=author)
    db.add(r); db.commit(); db.refresh(r)
    return {"id": r.id, "rule_type": r.rule_type}

@app.get("/normalization/rules")
def list_normalization_rules(active: int | None = None, db: Session = Depends(get_db)):
    q = db.query(NormalizationRule)
    if active is not None:
        q = q.filter(NormalizationRule.active==active)
    rows = q.order_by(NormalizationRule.id.desc()).limit(200).all()
    return [
        {"id": r.id, "rule_type": r.rule_type, "active": r.active, "author": r.author, "payload": r.payload, "created_at": r.created_at}
        for r in rows
    ]

@app.post("/normalization/preview")
def preview_normalization(body: dict, db: Session = Depends(get_db)):
    event_name = body.get('event_name')
    props = body.get('props') or {}
    include_rule = body.get('include_rule')  # optional dry-run rule
    # Apply active event_alias rules
    alias_rules = db.query(NormalizationRule).filter(NormalizationRule.active==1, NormalizationRule.rule_type=='event_alias').all()
    normalized_name = event_name
    for r in alias_rules:
        mapping = r.payload or {}
        if event_name in mapping:
            normalized_name = mapping[event_name]
            break
    # Apply include_rule after stored rules for dry-run preview
    if include_rule and include_rule.get('rule_type') == 'event_alias':
        payload = include_rule.get('payload') or {}
        if event_name in payload:
            normalized_name = payload[event_name]
    return {"event_name": event_name, "normalized_event_name": normalized_name, "props": props}

@app.post("/normalization/rules/{rule_id}/deactivate")
def deactivate_normalization_rule(rule_id: int, db: Session = Depends(get_db)):
    r = db.query(NormalizationRule).filter(NormalizationRule.id==rule_id).first()
    if not r:
        raise HTTPException(status_code=404, detail="not_found")
    r.active = 0
    # If event_alias, also deactivate any other alias rules that map same source event names
    if r.rule_type == 'event_alias' and isinstance(r.payload, dict):
        source_events = set(r.payload.keys())
        if source_events:
            others = db.query(NormalizationRule).filter(NormalizationRule.id!=r.id, NormalizationRule.rule_type=='event_alias', NormalizationRule.active==1).all()
            for o in others:
                if isinstance(o.payload, dict) and source_events.intersection(o.payload.keys()):
                    o.active = 0
    db.commit(); db.refresh(r)
    return {"id": r.id, "active": r.active}

@app.get("/models/cluster/latest")
def latest_cluster_model(db: Session = Depends(get_db)):
    art = db.query(ModelArtifact).filter(ModelArtifact.model_name=='cluster').order_by(ModelArtifact.id.desc()).first()
    if not art:
        return None
    return {"id": art.id, "model_name": art.model_name, "artifact": art.artifact}

@app.post("/ml/drift/thresholds", response_model=DriftThresholdOut)
def create_drift_threshold(body: DriftThresholdIn, db: Session = Depends(get_db)):
    th = ModelDriftThreshold(
        model_name=body.model_name,
        metric_name=body.metric_name,
        comparison=body.comparison,
        boundary=body.boundary,
        action=body.action,
        cooldown_hours=body.cooldown_hours,
        active=body.active,
    )
    db.add(th); db.commit(); db.refresh(th)
    return th

@app.get("/ml/drift/thresholds", response_model=list[DriftThresholdOut])
def list_drift_thresholds(active: int | None = None, db: Session = Depends(get_db)):
    q = db.query(ModelDriftThreshold)
    if active is not None:
        q = q.filter(ModelDriftThreshold.active==active)
    return q.order_by(ModelDriftThreshold.created_at.desc()).limit(200).all()

@app.post("/ml/drift/thresholds/{th_id}/deactivate", response_model=DriftThresholdOut)
def deactivate_drift_threshold(th_id: int, db: Session = Depends(get_db)):
    th = db.query(ModelDriftThreshold).filter(ModelDriftThreshold.id==th_id).first()
    if not th:
        raise HTTPException(status_code=404, detail="not_found")
    th.active = 0
    db.commit(); db.refresh(th)
    return th

@app.post("/ml/drift/evaluate")
def manual_drift_evaluate():
    from onboarding_analyzer.tasks.mlops import evaluate_drift_thresholds
    return evaluate_drift_thresholds()

class MaintenanceJobOut(BaseModel):
    id: int
    job_type: str
    status: str
    started_at: datetime
    completed_at: datetime | None
    params: dict | None
    result: dict | None
    error: str | None
    class Config:
        from_attributes = True

@app.get("/ops/maintenance/jobs", response_model=list[MaintenanceJobOut])
def list_maintenance_jobs(limit: int = 100, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import MaintenanceJob
    rows = db.query(MaintenanceJob).order_by(MaintenanceJob.started_at.desc()).limit(limit).all()
    return rows

@app.post("/ops/backup", response_model=MaintenanceJobOut)
def trigger_backup(db: Session = Depends(get_db)):
    """Create a logical snapshot of table row counts & schema fingerprints (not full dump)."""
    from onboarding_analyzer.models.tables import MaintenanceJob
    insp = reflection.Inspector.from_engine(db.bind)  # type: ignore
    tables = insp.get_table_names()
    summary: dict[str, dict] = {}
    for t in tables:
        try:
            cnt = db.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()  # no user input
            cols = [c['name'] for c in insp.get_columns(t)]
            summary[t] = {"count": int(cnt or 0), "columns_hash": hashlib.sha256("|".join(cols).encode()).hexdigest()}
        except Exception as e:
            summary[t] = {"error": str(e)}
    job = MaintenanceJob(job_type='backup', status='completed', params=None, result={"tables": summary})
    db.add(job); db.commit(); db.refresh(job)
    return job

@app.post("/ops/restore/{job_id}", response_model=MaintenanceJobOut)
def trigger_restore(job_id: int, db: Session = Depends(get_db)):
    """Validate current schema & row count drift against a prior backup job.

    This does not repopulate data (no raw dump retained) but produces a drift report and status.
    """
    from onboarding_analyzer.models.tables import MaintenanceJob
    backup = db.query(MaintenanceJob).filter(MaintenanceJob.id==job_id, MaintenanceJob.job_type=='backup').first()
    if not backup:
        raise HTTPException(status_code=404, detail='backup_not_found')
    insp = reflection.Inspector.from_engine(db.bind)  # type: ignore
    tables = insp.get_table_names()
    drift: dict[str, dict] = {}
    src_tables = (backup.result or {}).get('tables', {}) if backup.result else {}
    for t in tables:
        try:
            cnt = db.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
            cols = [c['name'] for c in insp.get_columns(t)]
            cols_hash = hashlib.sha256("|".join(cols).encode()).hexdigest()
            orig = src_tables.get(t)
            if orig:
                drift[t] = {
                    'count_delta': int(cnt or 0) - int(orig.get('count',0)),
                    'schema_changed': cols_hash != orig.get('columns_hash'),
                }
            else:
                drift[t] = {'new_table': True, 'count': int(cnt or 0)}
        except Exception as e:
            drift[t] = {'error': str(e)}
    job = MaintenanceJob(job_type='restore', status='completed', params={'from_backup_id': job_id}, result={'drift': drift})
    db.add(job); db.commit(); db.refresh(job)
    return job

@app.post("/models/{model_name}/versions/{version}/promote")
def promote_model_version(model_name: str, version: str, db: Session = Depends(get_db)):
    mv = db.query(ModelVersion).filter(ModelVersion.model_name==model_name, ModelVersion.version==version).first()
    if not mv:
        raise HTTPException(status_code=404, detail="not_found")
    # demote others
    others = db.query(ModelVersion).filter(ModelVersion.model_name==model_name, ModelVersion.promoted==1).all()
    for o in others:
        o.promoted = 0
    mv.promoted = 1
    db.commit()
    return {"status":"ok","model":model_name,"version":version}

@app.post("/models/{model_name}/versions/{version}/rollback")
def rollback_model_version(model_name: str, version: str, db: Session = Depends(get_db)):
    # promote the specified version; demote current
    return promote_model_version(model_name, version, db)

class DatasetSnapshotOut(BaseModel):
    id: int
    snapshot_key: str
    created_at: datetime
    event_count: int
    user_count: int
    fingerprint: str
    meta: dict | None
    class Config:
        from_attributes = True

@app.post("/datasets/snapshot", response_model=DatasetSnapshotOut)
def create_dataset_snapshot(key: str, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import DatasetSnapshot, RawEvent
    total_events = db.query(func.count(RawEvent.id)).scalar() or 0
    users = db.query(RawEvent.user_id).distinct().count()
    fingerprint = hashlib.sha256(f"{total_events}:{users}".encode()).hexdigest()
    snap = DatasetSnapshot(snapshot_key=key, event_count=total_events, user_count=users, fingerprint=fingerprint)
    db.add(snap); db.commit(); db.refresh(snap)
    return snap

@app.get("/datasets/snapshots", response_model=list[DatasetSnapshotOut])
def list_dataset_snapshots(limit: int = 50, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import DatasetSnapshot
    return db.query(DatasetSnapshot).order_by(DatasetSnapshot.created_at.desc()).limit(limit).all()

@app.get("/datasets/snapshots/diff")
def diff_dataset_snapshots(a: str, b: str, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import DatasetSnapshot
    sa = db.query(DatasetSnapshot).filter(DatasetSnapshot.snapshot_key==a).first()
    sb = db.query(DatasetSnapshot).filter(DatasetSnapshot.snapshot_key==b).first()
    if not sa or not sb:
        raise HTTPException(status_code=404, detail="snapshot_not_found")
    return {
        "a": a,
        "b": b,
        "event_count_delta": sb.event_count - sa.event_count,
        "user_count_delta": sb.user_count - sa.user_count,
        "same_fingerprint": sa.fingerprint == sb.fingerprint,
    }

class DQThresholdIn(BaseModel):
    metric_type: str
    event_name: str | None = None
    lower_bound: float | None = None
    upper_bound: float | None = None
    direction: str | None = None
    severity: str = 'warn'
    project: str | None = None
    active: int = 1

@app.post("/governance/dq/thresholds")
def create_dq_threshold(body: DQThresholdIn, db: Session = Depends(get_db)):
    th = DataQualityThreshold(metric_type=body.metric_type, event_name=body.event_name, lower_bound=body.lower_bound, upper_bound=body.upper_bound, direction=body.direction, severity=body.severity, project=body.project, active=body.active)
    db.add(th); db.commit(); db.refresh(th)
    return {"id": th.id}

@app.get("/governance/dq/thresholds")
def list_dq_thresholds(metric_type: str | None = None, db: Session = Depends(get_db)):
    q = db.query(DataQualityThreshold)
    if metric_type:
        q = q.filter(DataQualityThreshold.metric_type==metric_type)
    return [
        {"id": r.id, "metric_type": r.metric_type, "event_name": r.event_name, "lower_bound": r.lower_bound, "upper_bound": r.upper_bound, "direction": r.direction, "severity": r.severity, "project": r.project, "active": r.active}
        for r in q.order_by(DataQualityThreshold.id.desc()).limit(200).all()
    ]

@app.post("/governance/retention")
def create_retention_policy(body: dict, db: Session = Depends(get_db)):
    table_name = body.get('table_name'); max_age_days = body.get('max_age_days'); pii_fields = body.get('pii_fields'); hashing_salt = body.get('hashing_salt')
    if not table_name or not isinstance(max_age_days, int):
        raise HTTPException(status_code=400, detail='invalid')
    rp = RetentionPolicy(table_name=table_name, max_age_days=max_age_days, pii_fields=pii_fields, hashing_salt=hashing_salt)
    db.add(rp); db.commit(); db.refresh(rp)
    return {"id": rp.id}

@app.get("/governance/retention", response_model=list[dict])
def list_retention_policies(db: Session = Depends(get_db)):
    rows = db.query(RetentionPolicy).order_by(RetentionPolicy.created_at.desc()).limit(200).all()
    return [{"id": r.id, "table_name": r.table_name, "max_age_days": r.max_age_days, "pii_fields": r.pii_fields, "hashing_salt": r.hashing_salt, "active": r.active} for r in rows]

@app.post("/privacy/delete")
def privacy_delete(user_id: str, db: Session = Depends(get_db), project: str | None = None):
    from onboarding_analyzer.models.tables import PrivacyDeletionAudit, SessionSummary, UserFeature, UserRecommendation, Insight
    if not user_id:
        raise HTTPException(status_code=400, detail='missing_user_id')
    counts = {"raw_events":0,"session_summaries":0,"features":0,"recommendations":0,"insights":0}
    # Raw events
    q = db.query(RawEvent).filter(RawEvent.user_id==user_id)
    if project:
        q = q.filter(RawEvent.project==project)
    counts['raw_events'] = q.count()
    q.delete(synchronize_session=False)
    # Session summaries
    qs = db.query(SessionSummary).filter(SessionSummary.user_id==user_id)
    if project:
        qs = qs.filter(SessionSummary.project==project)
    counts['session_summaries'] = qs.count(); qs.delete(synchronize_session=False)
    # Features
    qf = db.query(UserFeature).filter(UserFeature.user_id==user_id)
    if project:
        qf = qf.filter(UserFeature.project==project)
    counts['features'] = qf.count(); qf.delete(synchronize_session=False)
    # Recommendations
    qr = db.query(UserRecommendation).filter(UserRecommendation.user_id==user_id)
    if project:
        qr = qr.filter(UserRecommendation.project==project)
    counts['recommendations'] = qr.count(); qr.delete(synchronize_session=False)
    # Insights (rarely user-specific but delete if user id embedded in title)
    qi = db.query(Insight).filter(Insight.title.ilike(f"%{user_id}%"))
    counts['insights'] = qi.count(); qi.delete(synchronize_session=False)
    audit = PrivacyDeletionAudit(user_id=user_id, project=project, raw_events=counts['raw_events'], session_summaries=counts['session_summaries'], features=counts['features'], recommendations=counts['recommendations'], insights=counts['insights'])
    db.add(audit)
    db.commit(); db.refresh(audit)
    return {"status":"ok","audit_id": audit.id, **counts}

@app.get("/models/{model_name}/versions", response_model=list[dict])
def list_model_versions(model_name: str, db: Session = Depends(get_db)):
    rows = db.query(ModelVersion).filter(ModelVersion.model_name==model_name).order_by(ModelVersion.created_at.desc()).limit(200).all()
    return [{"version": r.version, "promoted": bool(r.promoted), "created_at": r.created_at, "notes": r.notes} for r in rows]

@app.get("/usage/summary")
def usage_summary(hours: int = 24, db: Session = Depends(get_db)):
    since = datetime.utcnow() - timedelta(hours=hours)
    events = db.query(func.count(RawEvent.id)).filter(RawEvent.ts>=since).scalar() or 0
    insights_new = db.query(func.count(Insight.id)).filter(Insight.created_at>=since).scalar() or 0
    recs = db.query(func.count(UserRecommendation.id)).filter(UserRecommendation.created_at>=since).scalar() or 0
    return {"window_hours": hours, "events": events, "insights": insights_new, "user_recommendations": recs}

@app.get("/usage/projects", response_model=list[dict])
def usage_by_project(hours: int = 24, db: Session = Depends(get_db), limit: int = 100):
    since = datetime.utcnow() - timedelta(hours=hours)
    # Aggregate counts by project (null project grouped under 'global')
    rows = (
        db.query(RawEvent.project, func.count(RawEvent.id))
        .filter(RawEvent.ts>=since)
        .group_by(RawEvent.project)
        .order_by(func.count(RawEvent.id).desc())
        .limit(limit)
        .all()
    )
    out = []
    for project, cnt in rows:
        out.append({"project": project or "global", "events": int(cnt)})
    return out

# Quota management endpoints
@app.post('/governance/quotas')
def upsert_quota(body: QuotaIn, db: Session = Depends(get_db)):
    row = db.query(ProjectQuota).filter(ProjectQuota.project==body.project).first()
    if not row:
        row = ProjectQuota(project=body.project)
        db.add(row)
    row.daily_event_limit = body.daily_event_limit
    row.monthly_event_limit = body.monthly_event_limit
    row.enforced = body.enforced
    row.updated_at = datetime.utcnow()
    db.commit()
    return {"status":"ok","project": body.project}

@app.get('/governance/quotas')
def list_quotas(db: Session = Depends(get_db)):
    rows = db.query(ProjectQuota).all()
    return {"quotas": [ {"project": r.project, "daily": r.daily_event_limit, "monthly": r.monthly_event_limit, "enforced": r.enforced} for r in rows ]}

# Model evaluation endpoints
@app.post('/models/evaluate')
def submit_model_eval(body: ModelEvalIn, db: Session = Depends(get_db)):
    existing = db.query(ModelEvaluation).filter_by(model_name=body.model_name, candidate_version=body.candidate_version, metric_name=body.metric_name).first()
    delta = None
    if body.baseline_score is not None:
        try:
            delta = body.candidate_score - body.baseline_score
        except Exception:
            delta = None
    if not existing:
        existing = ModelEvaluation(model_name=body.model_name, candidate_version=body.candidate_version, baseline_version=body.baseline_version, metric_name=body.metric_name, candidate_score=body.candidate_score, baseline_score=body.baseline_score, delta=delta, threshold=body.threshold, details=body.details)
        db.add(existing)
    else:
        existing.candidate_score = body.candidate_score
        existing.baseline_score = body.baseline_score
        existing.delta = delta
        existing.threshold = body.threshold
        existing.details = body.details
    if body.auto_decide and body.threshold is not None and delta is not None:
        direction = body.promote_if or 'gt'
        promote = False
        if direction == 'gt' and delta >= body.threshold:
            promote = True
        if direction == 'lt' and delta <= body.threshold:
            promote = True
        if promote:
            existing.decision = 'promote'
            existing.decided_at = datetime.utcnow()
            try:
                db.query(ModelVersion).filter(ModelVersion.model_name==body.model_name, ModelVersion.promoted==1).update({ModelVersion.promoted:0})
                mv = db.query(ModelVersion).filter(ModelVersion.model_name==body.model_name, ModelVersion.version==body.candidate_version).first()
                if mv:
                    mv.promoted = 1
                else:
                    mv = ModelVersion(model_name=body.model_name, version=body.candidate_version, promoted=1)
                    db.add(mv)
            except Exception:
                pass
        else:
            existing.decision = existing.decision or 'hold'
    db.commit()
    return {"status":"ok","decision": existing.decision, "delta": existing.delta}

@app.get('/models/evaluate')
def list_model_evals(model: str | None = None, db: Session = Depends(get_db)):
    q = db.query(ModelEvaluation)
    if model:
        q = q.filter(ModelEvaluation.model_name==model)
    rows = q.order_by(ModelEvaluation.created_at.desc()).limit(200).all()
    return {"evaluations": [ {"model": r.model_name, "candidate": r.candidate_version, "baseline": r.baseline_version, "metric": r.metric_name, "candidate_score": r.candidate_score, "baseline_score": r.baseline_score, "delta": r.delta, "decision": r.decision} for r in rows ]}

# Secret rotation endpoint
@app.post('/secrets/rotate')
def rotate_secret(body: SecretRotateIn, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import ConnectorCredential
    row = db.query(ConnectorCredential).filter(ConnectorCredential.connector_name==body.connector_name, ConnectorCredential.secret_key==body.secret_key, ConnectorCredential.active==1).first()
    if not row:
        raise HTTPException(status_code=404, detail='secret_not_found')
    try:
        enc = encrypt_secret(body.plaintext)
        row.encrypted_value = enc
        row.version += 1
        row.rotated_at = datetime.utcnow()
        db.commit()
    except EncryptionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status":"ok","version": row.version}

# DSAR export endpoint
@app.get('/privacy/export', response_model=DSARExportOut)
def privacy_export(user_id: str, project: str | None = Query(default=None), db: Session = Depends(get_db)):
    ev_q = db.query(RawEvent).filter(RawEvent.user_id==user_id)
    if project:
        ev_q = ev_q.filter(RawEvent.project==project)
    events = ev_q.order_by(RawEvent.ts.desc()).limit(5000).all()
    recs = db.query(UserRecommendation).filter(UserRecommendation.user_id==user_id)
    if project:
        recs = recs.filter(UserRecommendation.project==project)
    recs = recs.order_by(UserRecommendation.created_at.desc()).all()
    ids = db.query(UserIdentityMap).filter((UserIdentityMap.primary_user_id==user_id) | (UserIdentityMap.alias_user_id==user_id)).all()
    from onboarding_analyzer.models.tables import UserFeature
    feats = db.query(UserFeature).filter(UserFeature.user_id==user_id)
    if project:
        feats = feats.filter(UserFeature.project==project)
    feats = feats.all()
    cons = db.query(ConsentRecord).filter(ConsentRecord.user_id==user_id)
    if project: cons = cons.filter(ConsentRecord.project==project)
    cons = cons.all()
    fb = db.query(RecommendationFeedback).filter(RecommendationFeedback.user_id==user_id)
    if project: fb = fb.filter(RecommendationFeedback.project==project)
    fb = fb.all()
    payload = {
        'events': [ {'event_name': e.event_name, 'ts': e.ts.isoformat(), 'props': e.props} for e in events ],
        'recommendations': [ {'recommendation': r.recommendation, 'status': r.status, 'created_at': r.created_at.isoformat()} for r in recs ],
        'identities': [ {'primary': i.primary_user_id, 'alias': i.alias_user_id, 'source': i.source} for i in ids ],
        'features': [ {'project': f.project, 'features': f.features, 'updated_at': f.updated_at.isoformat()} for f in feats ],
        'consent': [ {'policy_version': c.policy_version, 'granted': c.granted, 'updated_at': c.updated_at.isoformat()} for c in cons ],
        'recommendation_feedback': [ {'recommendation': r.recommendation, 'feedback': r.feedback, 'created_at': r.created_at.isoformat()} for r in fb ],
    }
    return DSARExportOut(user_id=user_id, project=project, raw_events=len(events), recommendations=len(recs), identities=len(ids), features=len(feats), payload=payload)

# SLO management
@app.post('/ops/slo')
def upsert_slo(body: SLODefIn, db: Session = Depends(get_db)):
    row = db.query(SLODefinition).filter(SLODefinition.name==body.name).first()
    if not row:
        row = SLODefinition(name=body.name, metric=body.metric, target=body.target, window_hours=body.window_hours, threshold=body.threshold, active=body.active, description=body.description)
        db.add(row)
    else:
        row.metric = body.metric
        row.target = body.target
        row.window_hours = body.window_hours
        row.threshold = body.threshold
        row.active = body.active
        row.description = body.description
    db.commit(); db.refresh(row)
    return {"status":"ok","id": row.id}

@app.get('/ops/slo')
def list_slos(db: Session = Depends(get_db)):
    rows = db.query(SLODefinition).filter(SLODefinition.active==1).all()
    return {"slos": [ {"id": r.id, "name": r.name, "metric": r.metric, "target": r.target, "window_hours": r.window_hours, "threshold": r.threshold} for r in rows ]}

@app.post('/ops/slo/evaluate')
def evaluate_slos(db: Session = Depends(get_db)):
    now = datetime.utcnow()
    out = []
    slos = db.query(SLODefinition).filter(SLODefinition.active==1).all()
    for slo in slos:
        start = now - timedelta(hours=slo.window_hours)
        attained = 0.0
        details: dict[str, Any] = {}
        if slo.metric == 'ingest_max_lag':
            # Compute percentile inverse attainment: target means max lag below threshold (threshold field)
            thr = slo.threshold or 120.0
            # Use OpsMetric if present, else direct query
            lag_rows = db.query(OpsMetric.metric_value).filter(OpsMetric.metric_name=='ingest_max_lag_s', OpsMetric.captured_at>=start).all()
            if lag_rows:
                max_lag = max(r[0] for r in lag_rows)
            else:
                max_lag = db.query(func.max(RawEvent.ts)).filter(RawEvent.ts>=start).scalar() or 0.0
            attained = 100.0 if max_lag <= thr else max(0.0, 100.0 - ((max_lag-thr)/thr)*100.0)
            details['max_lag'] = max_lag
            details['threshold'] = thr
        elif slo.metric == 'ingest_success_rate':
            # Success rate = persisted / received (approx using counters stored in OpsMetric if available)
            # Fallback: approximate persisted count / total events in window vs previous window size
            total = db.query(func.count(RawEvent.id)).filter(RawEvent.ts>=start).scalar() or 0
            # We don't store received separately; assume near 100% if no DLQ events
            dlq = db.query(func.count(AuditLog.id)).filter(AuditLog.endpoint.like('%/events%'), AuditLog.status!=200, AuditLog.ts>=start).scalar() or 0
            denom = total + dlq
            attained = 100.0 if denom == 0 else (total/denom)*100.0
            details['total'] = total
            details['dlq_like'] = dlq
        elif slo.metric == 'dq_alert_rate':
            alerts = db.query(func.count(DataQualityAlert.id)).filter(DataQualityAlert.created_at>=start, DataQualityAlert.status=='open').scalar() or 0
            # Lower is better: attainment = (1 - min(1, alerts/threshold)) *100
            thr = slo.threshold or 5.0
            ratio = alerts / thr if thr > 0 else 1.0
            attained = max(0.0, 100.0 - min(1.0, ratio)*100.0)
            details['open_alerts'] = alerts
            details['threshold'] = thr
        else:
            attained = 0.0
        breach = 1 if attained < slo.target else 0
        # Upsert evaluation row
        existing = db.query(SLOEvaluation).filter(SLOEvaluation.slo_id==slo.id, SLOEvaluation.window_start==start, SLOEvaluation.window_end==now).first()
        if not existing:
            existing = SLOEvaluation(slo_id=slo.id, window_start=start, window_end=now, attained=attained, target=slo.target, breach=breach, details=details)
            db.add(existing)
        else:
            existing.attained = attained
            existing.breach = breach
            existing.details = details
        out.append({"slo_id": slo.id, "attained": attained, "target": slo.target, "breach": bool(breach)})
    db.commit()
    return {"evaluations": out}

@app.get("/secrets/audit", response_model=list[dict])
def list_secret_access(limit: int = 200, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import SecretAccessAudit
    rows = db.query(SecretAccessAudit).order_by(SecretAccessAudit.accessed_at.desc()).limit(limit).all()
    return [{"connector": r.connector_name, "secret_key": r.secret_key, "accessed_at": r.accessed_at} for r in rows]

@app.post("/experiments/composite/config", response_model=ExperimentCompositeConfigOut)
def create_composite_config(body: ExperimentCompositeConfigIn, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import ExperimentCompositeConfig
    existing = db.query(ExperimentCompositeConfig).filter_by(experiment_prop=body.experiment_prop).first()
    if existing:
        # Update in-place
        existing.weights = body.weights
        existing.guardrails = body.guardrails
        existing.active = body.active
        existing.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(existing)
        return existing
    cfg = ExperimentCompositeConfig(
        experiment_prop=body.experiment_prop,
        weights=body.weights,
        guardrails=body.guardrails,
        active=body.active,
    )
    db.add(cfg)
    db.commit()
    db.refresh(cfg)
    return cfg

@app.get("/experiments/composite/config", response_model=list[ExperimentCompositeConfigOut])
def list_composite_configs(active: int | None = None, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import ExperimentCompositeConfig
    q = db.query(ExperimentCompositeConfig)
    if active is not None:
        q = q.filter(ExperimentCompositeConfig.active==active)
    return q.order_by(ExperimentCompositeConfig.created_at.desc()).limit(200).all()

@app.post("/experiments/composite/score")
def compute_composite_score(experiment_prop: str):
    from onboarding_analyzer.tasks.modeling import compute_experiment_composite_score
    res = compute_experiment_composite_score.run(experiment_prop=experiment_prop)
    if res.get('status') not in ("ok","no_config","no_weights","no_metrics","no_variants"):
        raise HTTPException(status_code=400, detail=res)
    return res

@app.get("/experiments/composite/scores", response_model=list[ExperimentCompositeScoreOut])
def list_composite_scores(experiment_prop: str, limit: int = 100, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import ExperimentCompositeScore
    rows = db.query(ExperimentCompositeScore).filter(ExperimentCompositeScore.experiment_prop==experiment_prop).order_by(ExperimentCompositeScore.computed_at.desc()).limit(limit).all()
    return rows

@app.post("/experiments/promote")
def promote_experiments(min_prob: float = 0.8, min_lift: float = 0.0):
    from onboarding_analyzer.tasks.modeling import evaluate_and_promote_experiments
    res = evaluate_and_promote_experiments.run(min_prob=min_prob, min_lift=min_lift)
    return res

@app.get("/experiments/promotions", response_model=list[ExperimentPromotionAuditOut])
def list_experiment_promotions(limit: int = 100, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import ExperimentPromotionAudit
    rows = db.query(ExperimentPromotionAudit).order_by(ExperimentPromotionAudit.created_at.desc()).limit(limit).all()
    return rows

# ---- Alert Rules CRUD & Logs ----
@app.post("/alerts/rules", response_model=AlertRuleOut)
def create_alert_rule(body: AlertRuleIn, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import AlertRule
    r = AlertRule(
        rule_type=body.rule_type,
        condition=body.condition,
        channels=body.channels or {},
        cooldown_minutes=body.cooldown_minutes,
        active=body.active,
    )
    db.add(r); db.commit(); db.refresh(r)
    return r

@app.get("/alerts/rules", response_model=list[AlertRuleOut])
def list_alert_rules(rule_type: str | None = None, active: int | None = None, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import AlertRule
    q = db.query(AlertRule)
    if rule_type:
        q = q.filter(AlertRule.rule_type==rule_type)
    if active is not None:
        q = q.filter(AlertRule.active==active)
    return q.order_by(AlertRule.created_at.desc()).limit(200).all()

@app.post("/alerts/rules/{rule_id}/deactivate", response_model=AlertRuleOut)
def deactivate_alert_rule(rule_id: int, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import AlertRule
    r = db.query(AlertRule).filter(AlertRule.id==rule_id).first()
    if not r:
        raise HTTPException(status_code=404, detail="not_found")
    r.active = 0
    db.commit(); db.refresh(r)
    return r

@app.get("/alerts/logs", response_model=list[AlertLogOut])
def list_alert_logs(rule_type: str | None = None, limit: int = 200, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import AlertLog
    q = db.query(AlertLog)
    if rule_type:
        q = q.filter(AlertLog.rule_type==rule_type)
    rows = q.order_by(AlertLog.created_at.desc()).limit(limit).all()
    return rows

@app.post("/alerts/templates", response_model=NotificationTemplateOut)
def create_template(body: NotificationTemplateIn, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import NotificationTemplate
    existing = db.query(NotificationTemplate).filter(NotificationTemplate.name==body.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="name_exists")
    t = NotificationTemplate(name=body.name, channel=body.channel, subject=body.subject, body=body.body, active=body.active)
    db.add(t); db.commit(); db.refresh(t)
    return t

@app.get("/alerts/templates", response_model=list[NotificationTemplateOut])
def list_templates(channel: str | None = None, active: int | None = None, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import NotificationTemplate
    q = db.query(NotificationTemplate)
    if channel:
        q = q.filter(NotificationTemplate.channel==channel)
    if active is not None:
        q = q.filter(NotificationTemplate.active==active)
    return q.order_by(NotificationTemplate.created_at.desc()).limit(200).all()

@app.patch("/alerts/templates/{template_id}", response_model=NotificationTemplateOut)
def update_template(template_id: int, body: dict, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import NotificationTemplate
    t = db.query(NotificationTemplate).filter(NotificationTemplate.id==template_id).first()
    if not t:
        raise HTTPException(status_code=404, detail="not_found")
    for field in ['name','channel','subject','body','active']:
        if field in body:
            setattr(t, field, body[field])
    t.updated_at = datetime.utcnow()
    db.commit(); db.refresh(t)
    return t

@app.post("/alerts/templates/{template_id}/deactivate", response_model=NotificationTemplateOut)
def deactivate_template(template_id: int, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import NotificationTemplate
    t = db.query(NotificationTemplate).filter(NotificationTemplate.id==template_id).first()
    if not t:
        raise HTTPException(status_code=404, detail="not_found")
    t.active = 0; t.updated_at = datetime.utcnow(); db.commit(); db.refresh(t)
    return t

# ---- Observability Enhancements (next phase) ----
@app.get("/alerts/metrics")
def alert_metrics(hours: int = 24, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import AlertRule, AlertLog
    since = datetime.now() - timedelta(hours=hours)
    logs = db.query(AlertLog).filter(AlertLog.created_at >= since).all()
    counts: dict[str,int] = {}
    last_fired: dict[str, datetime] = {}
    for l in logs:
        counts[l.rule_type] = counts.get(l.rule_type,0)+1
        if l.rule_type not in last_fired or l.created_at > last_fired[l.rule_type]:
            last_fired[l.rule_type] = l.created_at
    active_rules = db.query(AlertRule).filter(AlertRule.active==1).count()
    return {
        "window_hours": hours,
        "counts": counts,
        "last_fired": {k: v.isoformat() for k,v in last_fired.items()},
        "active_rules": active_rules,
    }

@app.get("/experiments/summary")
def experiment_summary(experiment_prop: str, db: Session = Depends(get_db)):
    from onboarding_analyzer.models.tables import ExperimentDefinition, ExperimentDecisionSnapshot, ExperimentCompositeScore, ExperimentPromotionAudit
    exp = db.query(ExperimentDefinition).filter(ExperimentDefinition.assignment_prop==experiment_prop).first()
    if not exp:
        raise HTTPException(status_code=404, detail="not_found")
    # Latest decision summary snapshot
    snap = db.query(ExperimentDecisionSnapshot).filter(ExperimentDecisionSnapshot.experiment_prop==experiment_prop, ExperimentDecisionSnapshot.variant=='__summary__').order_by(ExperimentDecisionSnapshot.computed_at.desc()).first()
    # Composite scores (latest per variant)
    scores = db.query(ExperimentCompositeScore).filter(ExperimentCompositeScore.experiment_prop==experiment_prop).order_by(ExperimentCompositeScore.computed_at.desc()).all()
    latest_score_per_variant: dict[str, dict] = {}
    for s in scores:
        if s.variant not in latest_score_per_variant:
            latest_score_per_variant[s.variant] = {
                "variant": s.variant,
                "score": s.score,
                "disqualified": s.disqualified,
                "computed_at": s.computed_at,
            }
    promo = db.query(ExperimentPromotionAudit).filter(ExperimentPromotionAudit.experiment_prop==experiment_prop).order_by(ExperimentPromotionAudit.created_at.desc()).first()
    return {
        "experiment": {
            "key": exp.key,
            "assignment_prop": exp.assignment_prop,
            "variants": exp.variants,
            "active": exp.active,
            "traffic_allocation": exp.traffic_allocation,
            "conversion_event": exp.conversion_event,
            "created_at": exp.created_at,
        },
        "latest_decision": None if not snap else {
            "decision": snap.decision,
            "winner": snap.winner,
            "baseline": snap.baseline,
            "users": snap.users,
            "conversions": snap.conversions,
            "computed_at": snap.computed_at,
        },
        "composite_scores": list(latest_score_per_variant.values()),
        "last_promotion": None if not promo else {
            "winner_variant": promo.winner_variant,
            "decision": promo.decision,
            "created_at": promo.created_at,
            "rationale": promo.rationale,
        },
    }

# ---- Schema Evolution Workflow ----
@app.post('/schema/version/propose')
def propose_schema_version(body: SchemaVersionIn, db: Session = Depends(get_db)):
    latest = db.query(EventSchemaVersion).filter(EventSchemaVersion.event_name==body.event_name).order_by(EventSchemaVersion.version.desc()).first()
    next_version = 1 if not latest else latest.version + 1
    row = EventSchemaVersion(event_name=body.event_name, version=next_version, spec=body.spec, status='draft', author=body.author)
    db.add(row); db.commit(); db.refresh(row)
    return {"status":"ok","version": row.version}

@app.post('/schema/version/approve')
def approve_schema_version(body: SchemaApproveIn, db: Session = Depends(get_db)):
    row = db.query(EventSchemaVersion).filter(EventSchemaVersion.event_name==body.event_name, EventSchemaVersion.version==body.version).first()
    if not row:
        raise HTTPException(status_code=404, detail='not_found')
    row.status = 'approved'; db.commit(); return {"status":"ok"}

@app.get('/schema/version/list')
def list_schema_versions(event_name: str, db: Session = Depends(get_db)):
    rows = db.query(EventSchemaVersion).filter(EventSchemaVersion.event_name==event_name).order_by(EventSchemaVersion.version).all()
    return {"versions": [ {"version": r.version, "status": r.status, "created_at": r.created_at, "author": r.author} for r in rows ]}

@app.get('/schema/version/diff')
def diff_schema_versions(event_name: str, v1: int, v2: int, db: Session = Depends(get_db)):
    r1 = db.query(EventSchemaVersion).filter(EventSchemaVersion.event_name==event_name, EventSchemaVersion.version==v1).first()
    r2 = db.query(EventSchemaVersion).filter(EventSchemaVersion.event_name==event_name, EventSchemaVersion.version==v2).first()
    if not r1 or not r2:
        raise HTTPException(status_code=404, detail='not_found')
    import json as _json, difflib
    s1 = _json.dumps(r1.spec, sort_keys=True, indent=2).splitlines()
    s2 = _json.dumps(r2.spec, sort_keys=True, indent=2).splitlines()
    diff = list(difflib.unified_diff(s1, s2, fromfile=f"v{v1}", tofile=f"v{v2}", lineterm=""))
    return {"from": v1, "to": v2, "diff": diff}

# ---- Anomaly Events ----
@app.get('/anomalies')
def list_anomalies(anomaly_type: str | None = None, status: str | None = None, project: str | None = None, limit: int = 200, db: Session = Depends(get_db)):
    q = db.query(AnomalyEvent)
    if anomaly_type: q = q.filter(AnomalyEvent.anomaly_type==anomaly_type)
    if status: q = q.filter(AnomalyEvent.status==status)
    if project: q = q.filter(AnomalyEvent.project==project)
    rows = q.order_by(AnomalyEvent.detected_at.desc()).limit(limit).all()
    return [ {"id": r.id, "type": r.anomaly_type, "project": r.project, "severity": r.severity, "delta_pct": r.delta_pct, "status": r.status, "detected_at": r.detected_at, "fingerprint": r.fingerprint} for r in rows ]

@app.post('/anomalies/{anomaly_id}/ack')
def ack_anomaly(anomaly_id: int, db: Session = Depends(get_db)):
    r = db.query(AnomalyEvent).filter(AnomalyEvent.id==anomaly_id).first()
    if not r: raise HTTPException(status_code=404, detail='not_found')
    r.status = 'ack'; r.acknowledged_at = datetime.utcnow(); db.commit(); return {"status":"ok"}

@app.post('/anomalies/{anomaly_id}/close')
def close_anomaly(anomaly_id: int, db: Session = Depends(get_db)):
    r = db.query(AnomalyEvent).filter(AnomalyEvent.id==anomaly_id).first()
    if not r: raise HTTPException(status_code=404, detail='not_found')
    r.status = 'closed'; r.closed_at = datetime.utcnow(); db.commit(); return {"status":"ok"}

# ---- Consent Records ----
@app.post('/privacy/consent')
def upsert_consent(body: ConsentUpsertIn, db: Session = Depends(get_db)):
    row = db.query(ConsentRecord).filter(ConsentRecord.user_id==body.user_id, ConsentRecord.policy_version==body.policy_version, ConsentRecord.project==body.project).first()
    if not row:
        row = ConsentRecord(user_id=body.user_id, project=body.project, policy_version=body.policy_version, granted=body.granted, source=body.source)
        db.add(row)
    else:
        row.granted = body.granted; row.source = body.source; row.updated_at = datetime.utcnow()
    db.commit(); return {"status":"ok"}

@app.get('/privacy/consent')
def list_consent(user_id: str | None = None, project: str | None = None, policy_version: str | None = None, db: Session = Depends(get_db)):
    q = db.query(ConsentRecord)
    if user_id: q = q.filter(ConsentRecord.user_id==user_id)
    if project: q = q.filter(ConsentRecord.project==project)
    if policy_version: q = q.filter(ConsentRecord.policy_version==policy_version)
    rows = q.order_by(ConsentRecord.updated_at.desc()).limit(500).all()
    return {"records": [ {"user_id": r.user_id, "project": r.project, "policy_version": r.policy_version, "granted": r.granted, "updated_at": r.updated_at} for r in rows ]}

# ---- Recommendation Feedback ----
@app.post('/recommendations/feedback')
def record_recommendation_feedback(body: RecommendationFeedbackIn, db: Session = Depends(get_db)):
    db.add(RecommendationFeedback(recommendation=body.recommendation, user_id=body.user_id, project=body.project, feedback=body.feedback, metadata=body.metadata))
    rec = db.query(UserRecommendation).filter(UserRecommendation.user_id==body.user_id, UserRecommendation.recommendation==body.recommendation, UserRecommendation.project==body.project).first()
    if rec:
        if body.feedback == 'accept': rec.status = 'accepted'; rec.acted_at = datetime.utcnow()
        elif body.feedback == 'dismiss': rec.status = 'dismissed'; rec.acted_at = datetime.utcnow()
        elif body.feedback == 'convert': rec.status = 'converted'; rec.acted_at = datetime.utcnow()
    db.commit(); return {"status":"ok"}

@app.get('/recommendations/feedback')
def list_recommendation_feedback(recommendation: str | None = None, project: str | None = None, limit: int = 200, db: Session = Depends(get_db)):
    q = db.query(RecommendationFeedback)
    if recommendation: q = q.filter(RecommendationFeedback.recommendation==recommendation)
    if project: q = q.filter(RecommendationFeedback.project==project)
    rows = q.order_by(RecommendationFeedback.created_at.desc()).limit(limit).all()
    return {"feedback": [ {"recommendation": r.recommendation, "user_id": r.user_id, "feedback": r.feedback, "project": r.project, "created_at": r.created_at} for r in rows ]}

# ---- Raw Event Export / Import ----
@app.post('/ops/export/raw')
def export_raw(body: RawExportQuery, db: Session = Depends(get_db)):
    """Export raw events in the time window with forward pagination.

    Pagination strategy:
      - Client supplies time window plus optional cursor (last exported ts + id).
      - We fetch limit events strictly greater than cursor (ts,id) ordering to ensure no duplicates.
      - Response returns next_cursor if more rows remain.
    Cursor format: "<iso-ts>|<id>".
    """
    cursor_ts = None; cursor_id = None
    if getattr(body, 'cursor', None):
        try:
            parts = body.cursor.split('|'); cursor_ts = datetime.fromisoformat(parts[0]); cursor_id = int(parts[1])
        except Exception:
            raise HTTPException(status_code=400, detail="invalid cursor format")
    q = db.query(RawEvent).filter(RawEvent.ts>=body.since, RawEvent.ts < body.until)
    if body.project: q = q.filter(RawEvent.project==body.project)
    if cursor_ts is not None:
        # (ts > cursor_ts) OR (ts == cursor_ts AND id > cursor_id)
        from sqlalchemy import or_, and_
        q = q.filter(or_(RawEvent.ts>cursor_ts, and_(RawEvent.ts==cursor_ts, RawEvent.id>cursor_id)))
    q = q.order_by(RawEvent.ts.asc(), RawEvent.id.asc())
    rows = q.limit(body.limit).all()
    next_cursor = None
    if rows:
        last = rows[-1]
        # Determine if there are more rows after last fetched
        more_q = db.query(RawEvent.id).filter(RawEvent.ts>=body.since, RawEvent.ts < body.until)
        if body.project: more_q = more_q.filter(RawEvent.project==body.project)
        from sqlalchemy import or_, and_
        more_q = more_q.filter(or_(RawEvent.ts>last.ts, and_(RawEvent.ts==last.ts, RawEvent.id>last.id))).order_by(RawEvent.ts.asc()).limit(1)
        if more_q.first():
            next_cursor = f"{last.ts.isoformat()}|{last.id}"
    return {"count": len(rows), "events": [ {"event_id": r.event_id, "user_id": r.user_id, "session_id": r.session_id, "event_name": r.event_name, "ts": r.ts.isoformat(), "props": r.props, "project": r.project, "schema_version": r.schema_version, "idempotency_key": r.idempotency_key} for r in rows ], "next_cursor": next_cursor}

@app.post('/ops/import/raw')
def import_raw(body: RawImportIn):
    from onboarding_analyzer.tasks.ingestion import persist_events
    normalized = []
    for ev in body.events:
        try:
            if isinstance(ev.get('ts'), str): ev['ts'] = datetime.fromisoformat(ev['ts'])
        except Exception:
            ev['ts'] = datetime.utcnow()
        normalized.append(ev)
    inserted = persist_events(normalized)
    return {"status":"ok","inserted": inserted}

# ---- Cost Usage Listing ----
@app.get('/usage/cost')
def list_cost_usage(project: str | None = None, hours: int = 24, db: Session = Depends(get_db)):
    since = datetime.utcnow() - timedelta(hours=hours)
    q = db.query(CostUsage).filter(CostUsage.window_start>=since)
    if project: q = q.filter(CostUsage.project==project)
    rows = q.order_by(CostUsage.window_start.desc()).limit(500).all()
    return {"windows": [ {"project": r.project, "window_start": r.window_start, "window_end": r.window_end, "event_count": r.event_count, "storage_bytes": r.storage_bytes, "compute_seconds": r.compute_seconds, "cost_estimate": r.cost_estimate} for r in rows ]}

# ---- Task Triggers (manual ops) ----
@app.post('/ops/tasks/cost-usage')
def trigger_cost_usage(window_minutes: int = 60):
    from onboarding_analyzer.tasks.enterprise import aggregate_cost_usage
    res = aggregate_cost_usage.run(window_minutes=window_minutes)
    return res

@app.post('/ops/tasks/lineage-enrich')
def trigger_lineage_enrich():
    from onboarding_analyzer.tasks.enterprise import enrich_lineage_projects
    return enrich_lineage_projects.run()

@app.post('/ops/tasks/prune-idempotency')
def trigger_prune_idempotency(days: int = 30, batch_size: int = 10000):
    from onboarding_analyzer.tasks.enterprise import prune_idempotency_keys
    return prune_idempotency_keys.run(older_than_days=days, batch_size=batch_size)

@app.post('/ops/tasks/personalization-eval')
def trigger_personalization_eval():
    from onboarding_analyzer.tasks.enterprise import evaluate_personalization_rules
    return evaluate_personalization_rules.run()

# ---- Slack Interactive Command Endpoint ----
@app.post('/integrations/slack/command')
async def slack_command(request: Request):
    settings = get_settings()
    form = await request.form()
    token = form.get('token')
    if settings.slack_verification_token and token != settings.slack_verification_token:
        raise HTTPException(status_code=401, detail='invalid_token')
    text = (form.get('text') or '').strip()
    # Commands:
    # anomaly ack <id>
    # rec feedback <user_id> <recommendation> <accept|dismiss|convert>
    db = SessionLocal()
    try:
        parts = text.split()
        if len(parts) >= 2 and parts[0] == 'anomaly' and parts[1] == 'ack' and len(parts) >= 3:
            try:
                aid = int(parts[2])
            except ValueError:
                return {"response_type": "ephemeral", "text": "Invalid anomaly id"}
            r = db.query(AnomalyEvent).filter(AnomalyEvent.id==aid).first()
            if not r:
                return {"response_type": "ephemeral", "text": "Not found"}
            r.status = 'ack'; r.acknowledged_at = datetime.utcnow(); db.commit()
            return {"response_type": "in_channel", "text": f"Anomaly {aid} acknowledged"}
        if len(parts) >= 5 and parts[0]=='rec' and parts[1]=='feedback':
            user_id = parts[2]; rec_key = parts[3]; fb = parts[4]
            if fb not in ('accept','dismiss','convert'):
                return {"response_type":"ephemeral","text":"Feedback must be accept|dismiss|convert"}
            db.add(RecommendationFeedback(recommendation=rec_key, user_id=user_id, project=None, feedback=fb))
            rec = db.query(UserRecommendation).filter(UserRecommendation.user_id==user_id, UserRecommendation.recommendation==rec_key).first()
            if rec:
                rec.status = 'accepted' if fb=='accept' else ('dismissed' if fb=='dismiss' else 'converted'); rec.acted_at = datetime.utcnow()
            db.commit()
            return {"response_type":"in_channel","text": f"Feedback recorded for {rec_key} ({fb})"}
        return {"response_type":"ephemeral","text":"Unknown command"}
    finally:
        db.close()

# ---- Segmentation CRUD ----
@app.post('/segments', response_model=SegmentDefOut)
def create_segment(body: SegmentDefIn, db: Session = Depends(get_db)):
    existing = db.query(SegmentDefinition).filter(SegmentDefinition.key==body.key).first()
    if existing:
        raise HTTPException(status_code=409, detail='exists')
    row = SegmentDefinition(key=body.key, expression=body.expression, description=body.description, active=body.active)
    db.add(row); db.commit(); db.refresh(row)
    return row

@app.get('/segments', response_model=list[SegmentDefOut])
def list_segments(active: int | None = None, db: Session = Depends(get_db)):
    q = db.query(SegmentDefinition)
    if active is not None:
        q = q.filter(SegmentDefinition.active==active)
    return q.order_by(SegmentDefinition.created_at.desc()).limit(200).all()

@app.patch('/segments/{key}', response_model=SegmentDefOut)
def update_segment(key: str, body: dict, db: Session = Depends(get_db)):
    row = db.query(SegmentDefinition).filter(SegmentDefinition.key==key).first()
    if not row: raise HTTPException(status_code=404, detail='not_found')
    for f in ['expression','description','active']:
        if f in body: setattr(row, f, body[f])
    row.updated_at = datetime.utcnow(); db.commit(); db.refresh(row); return row

@app.delete('/segments/{key}')
def delete_segment(key: str, db: Session = Depends(get_db)):
    row = db.query(SegmentDefinition).filter(SegmentDefinition.key==key).first()
    if not row: raise HTTPException(status_code=404, detail='not_found')
    db.delete(row); db.commit(); return {"status":"ok"}

@app.get('/segments/{key}/members', response_model=list[SegmentMembershipOut])
def list_segment_members(key: str, limit: int = 500, db: Session = Depends(get_db)):
    rows = db.query(UserSegmentMembership).filter(UserSegmentMembership.segment_key==key).order_by(UserSegmentMembership.computed_at.desc()).limit(limit).all()
    return rows

@app.post('/segments/{key}/compute')
def compute_segment_membership(key: str, project: str | None = None, limit_users: int = 10000, db: Session = Depends(get_db)):
    seg = db.query(SegmentDefinition).filter(SegmentDefinition.key==key, SegmentDefinition.active==1).first()
    if not seg: raise HTTPException(status_code=404, detail='not_found')
    # Simple evaluator: fetch distinct users and apply expression against latest event props sample
    expr = seg.expression or {}
    rows = db.query(RawEvent.user_id, RawEvent.props, RawEvent.project).order_by(RawEvent.ts.desc()).limit(limit_users).all()
    import json as _json
    def eval_clause(clause: dict, props: dict) -> bool:
        if 'and' in clause: return all(eval_clause(c, props) for c in clause['and'])
        if 'or' in clause: return any(eval_clause(c, props) for c in clause['or'])
        prop = clause.get('prop'); op = clause.get('op'); val = clause.get('value')
        if prop is None or op is None: return False
        cur = props.get(prop)
        if op == 'eq': return cur == val
        if op == 'neq': return cur != val
        if op == 'in': return isinstance(val, list) and cur in val
        if op == 'nin': return isinstance(val, list) and cur not in val
        if op == 'exists': return (cur is not None) == bool(val)
        if op == 'gt':
            try: return float(cur) > float(val)
            except Exception: return False
        if op == 'gte':
            try: return float(cur) >= float(val)
            except Exception: return False
        if op == 'lt':
            try: return float(cur) < float(val)
            except Exception: return False
        if op == 'lte':
            try: return float(cur) <= float(val)
            except Exception: return False
        return False
    inserted = 0
    for uid, props, rproj in rows:
        if project and rproj != project: continue
        p = props if isinstance(props, dict) else {}
        try:
            if eval_clause(expr, p):
                exists = db.query(UserSegmentMembership.id).filter(UserSegmentMembership.segment_key==seg.key, UserSegmentMembership.user_id==uid).first()
                if not exists:
                    db.add(UserSegmentMembership(segment_key=seg.key, user_id=uid, project=rproj))
                    inserted += 1
        except Exception:
            continue
    db.commit(); return {"status":"ok","inserted": inserted}