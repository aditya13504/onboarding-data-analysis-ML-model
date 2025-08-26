from __future__ import annotations
from celery import shared_task
from sqlalchemy.orm import Session
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent, FunnelMetric, NormalizationRule

BATCH_SIZE = 500


def _load_rule_maps(session: Session):
    maps = {"event_alias": {}, "prop_rename": {}, "value_map": {}}
    for r in session.query(NormalizationRule).filter(NormalizationRule.active == 1).all():
        if r.rule_type in maps and isinstance(r.payload, dict):
            for k, v in r.payload.items():
                maps[r.rule_type][k] = v
    return maps


def _load_step_order(session: Session):
    out = {}
    for fm in session.query(FunnelMetric).order_by(FunnelMetric.step_order).limit(200).all():
        out[fm.step_name] = fm.step_order
    return out


@shared_task
def backfill_event_enrichment(limit_sessions: int | None = None):
    """Assign deterministic session_event_index and step_index for events lacking them.

    Processes events ordered by (user_id, session_id, ts, id). Optionally limit number of sessions.
    """
    session: Session = SessionLocal()
    try:
        step_order = _load_step_order(session)
        # Fetch ordered events where any enrichment is missing
        q = (
            session.query(RawEvent.id, RawEvent.user_id, RawEvent.session_id, RawEvent.event_name, RawEvent.ts)
            .filter((RawEvent.session_event_index.is_(None)) | (RawEvent.step_index.is_(None)))
            .order_by(RawEvent.user_id, RawEvent.session_id, RawEvent.ts, RawEvent.id)
        )
        current_key = None
        counter = 0
        updates = []
        sessions_seen = 0
        for row in q.yield_per(BATCH_SIZE):
            key = (row.user_id, row.session_id)
            if key != current_key:
                current_key = key
                counter = 0
                sessions_seen += 1
                if limit_sessions and sessions_seen > limit_sessions:
                    break
            counter += 1
            step_idx = step_order.get(row.event_name)
            updates.append((row.id, counter, step_idx))
            if len(updates) >= BATCH_SIZE:
                _apply_enrichment_updates(session, updates)
                updates = []
        if updates:
            _apply_enrichment_updates(session, updates)
        session.commit()
        return {"status": "ok", "sessions_processed": sessions_seen}
    finally:
        session.close()


def _apply_enrichment_updates(session: Session, updates: list[tuple[int,int,int|None]]):
    for rid, sidx, step_idx in updates:
        session.query(RawEvent).filter(RawEvent.id == rid).update({
            RawEvent.session_event_index: sidx,
            RawEvent.step_index: step_idx,
        })
    session.flush()


@shared_task
def retroactively_normalize_events(batch_limit: int = 2000):
    """Apply current active normalization rules to historical events.

    For simplicity we apply alias changes and property renames/value maps to the first `batch_limit`
    events that may still need updates (heuristic).
    """
    session: Session = SessionLocal()
    try:
        maps = _load_rule_maps(session)
        alias_map = maps["event_alias"]
        prop_renames = maps["prop_rename"]
        value_maps = maps["value_map"]
        processed = 0
        q = session.query(RawEvent).order_by(RawEvent.id).limit(batch_limit)
        for ev in q.all():
            changed = False
            new_name = alias_map.get(ev.event_name)
            if new_name and new_name != ev.event_name:
                ev.event_name = new_name
                changed = True
            props_orig = ev.props or {}
            props = dict(props_orig)
            # property renames
            for src, dest in prop_renames.items():
                if src in props and dest not in props:
                    val = props.pop(src)
                    props[dest] = val
                    changed = True
            # value maps
            for key, mapping in value_maps.items():
                if key in props and isinstance(mapping, dict):
                    before = props[key]
                    after = mapping.get(str(before), before)
                    if after != before:
                        props[key] = after
                        changed = True
            if changed:
                ev.props = props
                session.flush()
            processed += 1
        session.commit()
        return {"status": "ok", "events_processed": processed}
    finally:
        session.close()
