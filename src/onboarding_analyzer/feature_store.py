from __future__ import annotations
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import FeatureDefinition, FeatureView

"""Lightweight feature store abstraction.

Provides:
 - Declarative registration of feature definitions (SQL/expr string) with versioning.
 - Materialization task stores results in FeatureView (entity_id scoped).
 - Retrieval helper to fetch latest values for a set of features & entities.

NOTE: Expressions are stored as plain strings; for safety only allow whitelisted patterns.
"""

SAFE_PREFIXES = ("SELECT", "select")

def register_feature(feature_key: str, entity: str, expr: str, version: str = "v1", description: str | None = None) -> dict:
    if not expr.strip().startswith(SAFE_PREFIXES):
        raise ValueError("expr must start with SELECT")
    session: Session = SessionLocal()
    try:
        row = session.query(FeatureDefinition).filter_by(feature_key=feature_key).first()
        if row:
            row.expr = expr
            row.version = version
            row.description = description
            row.updated_at = datetime.utcnow()
        else:
            row = FeatureDefinition(feature_key=feature_key, entity=entity, expr=expr, version=version, description=description)
            session.add(row)
        session.commit()
        return {"status": "ok", "feature_key": feature_key, "version": version}
    finally:
        session.close()


def get_feature_values(feature_keys: list[str], entity_ids: list[str]) -> dict[str, dict[str, object]]:
    """Return mapping feature_key -> {entity_id: value} (latest version)."""
    session: Session = SessionLocal()
    try:
        out: dict[str, dict[str, object]] = {k: {} for k in feature_keys}
        rows = session.query(FeatureView).filter(FeatureView.feature_key.in_(feature_keys), FeatureView.entity_id.in_(entity_ids)).all()
        for r in rows:
            out[r.feature_key][r.entity_id] = r.value.get("value") if isinstance(r.value, dict) else r.value
        return out
    finally:
        session.close()


def materialize_feature(feature_key: str, limit_entities: int | None = None) -> dict:
    session: Session = SessionLocal()
    try:
        fd = session.query(FeatureDefinition).filter_by(feature_key=feature_key, status="active").first()
        if not fd:
            return {"status": "error", "error": "definition_not_found"}
        sql = text(fd.expr)
        result = session.execute(sql)
        count = 0
        for row in result:
            # Expect first column entity_id, second value (JSON serializable)
            entity_id = str(row[0])
            value = row[1]
            existing = session.query(FeatureView).filter_by(feature_key=feature_key, entity_id=entity_id, feature_version=fd.version).first()
            if existing:
                existing.value = {"value": value}
                existing.computed_at = datetime.utcnow()
            else:
                session.add(FeatureView(feature_key=feature_key, entity_id=entity_id, value={"value": value}, feature_version=fd.version))
            count += 1
            if limit_entities and count >= limit_entities:
                break
        session.commit()
        return {"status": "ok", "materialized": count}
    finally:
        session.close()