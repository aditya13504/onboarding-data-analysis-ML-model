"""Asynchronous ingestion pipeline using Redis as a lightweight queue.

No mocks: pushes real event JSON payloads onto a Redis list; worker task pops and persists.
Designed to decouple external connector fetch latency from DB writes and enable backpressure.
"""
from __future__ import annotations
import json, os, time
from typing import Iterable
import redis
from onboarding_analyzer.config import get_settings
from prometheus_client import Gauge, Counter
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent
from datetime import datetime, timezone

QUEUE_KEY = "ingest:q:raw"

# Metrics (module-level singletons; registry defaults to global, scraped via /metrics in api)
INGEST_QUEUE_LENGTH = Gauge('ingest_queue_length', 'Current length of raw ingestion queue')
INGEST_ENQUEUED_TOTAL = Counter('ingest_events_enqueued_total', 'Total events enqueued for ingestion')
INGEST_PERSISTED_TOTAL = Counter('ingest_events_persisted_total', 'Total events persisted from queue')
INGEST_DRAIN_BATCH_SIZE = Gauge('ingest_last_drain_batch_size', 'Size of last drain batch processed')

def enqueue_events(events: Iterable[dict]):
    settings = get_settings()
    r = redis.Redis.from_url(settings.redis_url)
    pipe = r.pipeline()
    count = 0
    for ev in events:
        pipe.rpush(QUEUE_KEY, json.dumps(ev))
        count += 1
    if count:
        pipe.execute()
    try:
        if count:
            INGEST_ENQUEUED_TOTAL.inc(count)
        # Update queue length gauge
        qlen = r.llen(QUEUE_KEY)
        INGEST_QUEUE_LENGTH.set(qlen)
    except Exception:
        pass
    return {"enqueued": count, "queue_length": int(qlen) if 'qlen' in locals() else None}

def drain_queue(batch_size: int = 500):
    settings = get_settings()
    r = redis.Redis.from_url(settings.redis_url)
    batch: list[dict] = []
    for _ in range(batch_size):
        raw = r.lpop(QUEUE_KEY)
        if not raw:
            break
        try:
            batch.append(json.loads(raw))
        except Exception:
            continue
    if not batch:
        # Update gauge even if empty
        try:
            INGEST_QUEUE_LENGTH.set(r.llen(QUEUE_KEY))
            INGEST_DRAIN_BATCH_SIZE.set(0)
        except Exception:
            pass
        return {"persisted": 0, "queue_length": int(r.llen(QUEUE_KEY)) if r else None}
    with SessionLocal() as s:
        rows = []
        for e in batch:
            try:
                ts_raw = e.get('timestamp')
                ts = datetime.fromisoformat(ts_raw.replace('Z','+00:00')) if isinstance(ts_raw,str) else datetime.utcnow()
                ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
                props = e.get('properties') or {}
                rows.append(dict(event_id=e['id'], user_id=e.get('user_id') or 'anon', session_id=props.get('session_id') or e['id'], event_name=e['event'], ts=ts, props=props, schema_version='v1'))
            except Exception:
                continue
        if rows:
            try:
                s.bulk_insert_mappings(RawEvent, rows)
                s.commit()
            except Exception:
                s.rollback()
        persisted = len(rows)
        try:
            if persisted:
                INGEST_PERSISTED_TOTAL.inc(persisted)
            INGEST_DRAIN_BATCH_SIZE.set(persisted)
            INGEST_QUEUE_LENGTH.set(r.llen(QUEUE_KEY))
        except Exception:
            pass
        return {"persisted": persisted, "queue_length": int(r.llen(QUEUE_KEY)) if r else None}

def queue_length():
    """Return current queue length (for admin/monitoring)."""
    settings = get_settings()
    try:
        r = redis.Redis.from_url(settings.redis_url)
        qlen = r.llen(QUEUE_KEY)
        try:
            INGEST_QUEUE_LENGTH.set(qlen)
        except Exception:
            pass
        return {"queue_length": int(qlen)}
    except Exception:
        return {"queue_length": None, "error": "unavailable"}

__all__ = ["enqueue_events", "drain_queue", "queue_length"]