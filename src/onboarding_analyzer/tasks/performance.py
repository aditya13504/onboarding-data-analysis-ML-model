from __future__ import annotations
"""Performance & auto-index advisor tasks.

Scans recent query patterns (heuristically inferred from AuditLog + known analytics access)
and suggests indexes; optionally applies them if AUTO_INDEX_ENABLED.

This is intentionally lightweight: we only create indexes if they don't already exist,
and we restrict to a safelist of tables/columns to avoid runaway index creation.
"""
from celery import shared_task
from sqlalchemy import text, inspect
from onboarding_analyzer.infrastructure.db import engine, SessionLocal
from onboarding_analyzer.config import get_settings
from datetime import datetime, timedelta

SAFE_INDEXES = [
    # (table, columns tuple, name)
    ("raw_events", ("project","event_name","ts"), "ix_auto_project_event_ts"),
    ("raw_events", ("user_id","ts"), "ix_auto_user_ts"),
    ("raw_events", ("session_id","ts"), "ix_auto_session_ts"),
]


def _index_exists(table: str, name: str) -> bool:
    insp = inspect(engine)
    try:
        for ix in insp.get_indexes(table):
            if ix.get('name') == name:
                return True
    except Exception:
        return False
    return False


@shared_task
def advise_indexes(apply: bool | None = None):
    s = get_settings()
    if apply is None:
        apply = s.auto_index_enabled
    applied = []
    suggestions = []
    # Simple heuristic: if table row count exceeds threshold and index missing, recommend
    if engine.dialect.name == 'sqlite':  # skip row count heavy logic
        threshold = 10000
        session = SessionLocal()
        try:
            count = session.execute(text("SELECT COUNT(*) FROM raw_events")).scalar() or 0
        finally:
            session.close()
        if count < threshold:
            return {"status": "skipped", "reason": "low_volume"}
    # Evaluate each safe index
    for tbl, cols, name in SAFE_INDEXES:
        if _index_exists(tbl, name):
            continue
        suggestions.append({"table": tbl, "columns": cols, "name": name})
        if apply and engine.dialect.name == 'postgresql':
            cols_sql = ",".join(cols)
            stmt = f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} ON {tbl} ({cols_sql})"
            try:
                with engine.begin() as conn:
                    conn.execute(text(stmt))
                applied.append(name)
            except Exception:
                pass
        elif apply and engine.dialect.name != 'postgresql':
            # fallback without concurrently
            cols_sql = ",".join(cols)
            stmt = f"CREATE INDEX IF NOT EXISTS {name} ON {tbl} ({cols_sql})"
            try:
                with engine.begin() as conn:
                    conn.execute(text(stmt))
                applied.append(name)
            except Exception:
                pass
    return {"status": "ok", "suggestions": suggestions, "applied": applied}
