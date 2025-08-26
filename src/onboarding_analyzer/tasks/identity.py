from __future__ import annotations
from celery import shared_task
from sqlalchemy.orm import Session
from sqlalchemy import select
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import UserIdentityMap, RawEvent
from datetime import datetime
from prometheus_client import Counter, Gauge
try:
    from onboarding_analyzer.api.main import registry as _api_registry  # type: ignore
except Exception:  # pragma: no cover
    _api_registry = None

IDENTITY_SETS = Gauge('identity_sets_total', 'Total consolidated identity groups', registry=_api_registry)
IDENTITY_MERGES = Counter('identity_merges_total', 'User identity merges applied', registry=_api_registry)
IDENTITY_EVENTS_REWRITTEN = Counter('identity_events_rewritten_total', 'RawEvent rows updated to canonical user_id', registry=_api_registry)


def _build_canonical_map(session: Session) -> dict[str, str]:
    """Compute canonical user_id for every alias using union-find like grouping.

    Canonical choice: lexicographically smallest primary/alias encountered to ensure stability.
    """
    rows = session.execute(select(UserIdentityMap.primary_user_id, UserIdentityMap.alias_user_id)).all()
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(a: str, b: str):
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        # choose lexicographically smaller root as canonical
        if ra < rb:
            parent[rb] = ra
        else:
            parent[ra] = rb

    for p, a in rows:
        if p and a:
            union(p, a)
    # Path compress all
    for k in list(parent.keys()):
        find(k)
    return parent


@shared_task
def consolidate_identity_graph():
    """Analyze identity map and report total groups (no data mutation)."""
    session: Session = SessionLocal()
    try:
        parent = _build_canonical_map(session)
        groups = {v for v in parent.values()}
        try:
            IDENTITY_SETS.set(len(groups))
        except Exception:
            pass
        return {"status": "ok", "groups": len(groups), "aliases": len(parent)}
    finally:
        session.close()


@shared_task
def apply_identity_consolidation(batch_size: int = 500):
    """Rewrite RawEvent.user_id to canonical IDs in batches (real data, no mocks).

    Safe to run repeatedly; idempotent once all rows use canonical. Processes at most batch_size
    distinct non-canonical user_ids per invocation to limit lock scope.
    """
    session: Session = SessionLocal()
    try:
        mapping = _build_canonical_map(session)
        if not mapping:
            return {"status": "noop"}
        noncanonical = {u for u, root in mapping.items() if u != root}
        if not noncanonical:
            return {"status": "converged"}
        target_users = list(noncanonical)[:batch_size]
        total_events = 0
        for uid in target_users:
            canonical = mapping.get(uid, uid)
            if canonical == uid:
                continue
            # Update events for this uid
            updated = session.query(RawEvent).filter(RawEvent.user_id == uid).update({RawEvent.user_id: canonical})
            if updated:
                total_events += updated
                IDENTITY_EVENTS_REWRITTEN.inc(updated)
                IDENTITY_MERGES.inc()
        session.commit()
        return {"status": "ok", "updated_users": len(target_users), "events_rewritten": total_events}
    finally:
        session.close()
