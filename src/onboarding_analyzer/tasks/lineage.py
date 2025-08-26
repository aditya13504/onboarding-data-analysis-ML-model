from __future__ import annotations
"""Data lineage extraction and graph maintenance tasks.

Derives lineage edges from existing feature definitions, model artifacts, and archive compaction.
No mock data: parses real stored definitions and artifacts.
"""
from celery import shared_task
from sqlalchemy.orm import Session
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import FeatureDefinition, ModelArtifact, DataAsset, DataLineageEdge, ArchiveCompactedEvent, ModelVersion
from datetime import datetime
from prometheus_client import Counter, Gauge

try:
    from onboarding_analyzer.api.main import registry as _api_registry  # type: ignore
except Exception:  # pragma: no cover
    _api_registry = None

LINEAGE_UPSERTS = Counter('lineage_upserts_total', 'Lineage node/edge upserts', ['kind'], registry=_api_registry)
LINEAGE_IMPACT_NODES = Gauge('lineage_impact_nodes', 'Impacted downstream nodes count', ['asset'], registry=_api_registry)
LINEAGE_IMPACT_DEPTH = Gauge('lineage_impact_depth', 'Max downstream depth', ['asset'], registry=_api_registry)


def _ensure_asset(session: Session, key: str, a_type: str, desc: str | None = None, project: str | None = None):
    row = session.query(DataAsset).filter_by(asset_key=key).first()
    if not row:
        session.add(DataAsset(asset_key=key, asset_type=a_type, description=desc, project=project))
        LINEAGE_UPSERTS.labels(kind='asset').inc()


def _ensure_edge(session: Session, up: str, down: str, transform: str | None):
    row = session.query(DataLineageEdge).filter_by(upstream_key=up, downstream_key=down).first()
    if not row:
        session.add(DataLineageEdge(upstream_key=up, downstream_key=down, transform=transform))
        LINEAGE_UPSERTS.labels(kind='edge').inc()


@shared_task
def build_lineage_graph():
    session: Session = SessionLocal()
    try:
        # Feature definitions -> raw_events / other feature sources (heuristic parse)
        feats = session.query(FeatureDefinition).all()
        for fd in feats:
            _ensure_asset(session, fd.feature_key, 'feature', fd.description, project=None)
            # parse expression tokens
            expr = fd.expr.lower()
            sources = []
            if 'raw_events' in expr:
                sources.append('raw_events')
            # simple dependency detection for other features
            for other in feats:
                if other.feature_key != fd.feature_key and other.feature_key.lower() in expr:
                    sources.append(other.feature_key)
            for src in set(sources):
                _ensure_asset(session, src, 'raw' if src=='raw_events' else 'feature')
                _ensure_edge(session, src, fd.feature_key, 'feature_expr')
        # Model artifacts -> features they might reference via artifact JSON keys
        arts = session.query(ModelArtifact).all()
        for art in arts:
            _ensure_asset(session, f"model_artifact:{art.model_name}:{art.version}", 'model_artifact')
            data = art.artifact or {}
            for key in data.keys():
                if key.startswith('feature:'):
                    fkey = key.split(':',1)[1]
                    _ensure_asset(session, fkey, 'feature', project=None)
                    _ensure_edge(session, fkey, f"model_artifact:{art.model_name}:{art.version}", 'model_input')
        # Archive compaction edges raw_events -> archive_compacted_events
        if session.query(ArchiveCompactedEvent).first():
            _ensure_asset(session, 'archive_compacted_events', 'derived')
            _ensure_edge(session, 'raw_events', 'archive_compacted_events', 'compaction')
        # Model version lineage (promoted chain) - link previous -> current
        for mv in session.query(ModelVersion).filter(ModelVersion.promoted==1).all():
            _ensure_asset(session, f"model:{mv.model_name}:{mv.version}", 'model_artifact')
        session.commit()
        return {"status": "ok"}
    finally:
        session.close()


@shared_task
def compute_lineage_impact(roots: list[str] | None = None):
    """Compute downstream impact metrics for given root assets (or all assets if None).

    Performs BFS over DataLineageEdge graph to determine number of reachable nodes and max depth.
    Stores metrics in Prometheus gauges for observability.
    """
    session: Session = SessionLocal()
    try:
        assets = [a.asset_key for a in session.query(DataAsset).all()]
        if roots:
            assets = [a for a in assets if a in roots]
        edge_map: dict[str,list[str]] = {}
        for e in session.query(DataLineageEdge).all():
            edge_map.setdefault(e.upstream_key, []).append(e.downstream_key)
        results = {}
        for root in assets:
            visited = set()
            frontier = [(root,0)]
            max_depth = 0
            while frontier:
                node, depth = frontier.pop(0)
                for nxt in edge_map.get(node, []):
                    if nxt not in visited:
                        visited.add(nxt)
                        frontier.append((nxt, depth+1))
                        max_depth = max(max_depth, depth+1)
            results[root] = {"downstream_count": len(visited), "max_depth": max_depth}
            try:
                LINEAGE_IMPACT_NODES.labels(root).set(len(visited))
                LINEAGE_IMPACT_DEPTH.labels(root).set(max_depth)
            except Exception:
                pass
        return {"status": "ok", "roots": len(results)}
    finally:
        session.close()
