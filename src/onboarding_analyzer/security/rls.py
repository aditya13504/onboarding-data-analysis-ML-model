from __future__ import annotations
from sqlalchemy import text
from onboarding_analyzer.infrastructure.db import engine
from onboarding_analyzer.config import get_settings
import logging

def apply_rls_policies():
    """Enable RLS and create basic tenant isolation policies on configured tables.

    Expects a session variable app.current_project to be set for each request's transaction.
    If using Supabase, you can set this via a PostgreSQL function or by executing
      SET app.current_project = '<project>'
    at session start.
    """
    settings = get_settings()
    if not settings.enable_rls:
        return {"status": "skipped", "reason": "disabled"}
    if engine.dialect.name != 'postgresql':
        return {"status": "skipped", "reason": "not_postgres"}
    tables = [t.strip() for t in (settings.rls_tables or "").split(",") if t.strip()]
    tenant_col = settings.rls_tenant_column
    applied: list[str] = []
    with engine.begin() as conn:
        for tbl in tables:
            try:
                conn.execute(text(f"ALTER TABLE {tbl} ENABLE ROW LEVEL SECURITY"))
            except Exception:
                # ignore if already enabled or table missing
                pass
            # policy name deterministic
            policy_name = f"rls_{tbl}_{tenant_col}"
            # Drop existing to allow idempotent updates
            try:
                conn.execute(text(f"DROP POLICY IF EXISTS {policy_name} ON {tbl}"))
            except Exception:
                pass
            policy_sql = (
                f"CREATE POLICY {policy_name} ON {tbl} FOR SELECT USING ("
                f"{tenant_col} IS NULL OR {tenant_col} = current_setting('app.current_project', true))"
            )
            try:
                conn.execute(text(policy_sql))
                applied.append(tbl)
            except Exception as e:
                logging.getLogger("app").warning(f"RLS policy create failed for {tbl}: {e}")
    return {"status": "ok", "applied": applied}
