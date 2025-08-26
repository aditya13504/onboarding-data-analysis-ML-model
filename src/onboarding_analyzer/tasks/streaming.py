from __future__ import annotations
"""Real-time streaming ingestion tasks.

Implements (optional) Kafka consumer loop that batches events and feeds existing ingest pipeline.
If Kafka settings are absent, tasks are no-ops to avoid failures. Designed for deployment
as a dedicated Celery worker queue (e.g., -Q streaming) or triggered ad-hoc.
"""
from celery import shared_task
from datetime import datetime
from onboarding_analyzer.config import get_settings
from onboarding_analyzer.tasks.ingestion import ingest_events
from prometheus_client import Counter, Histogram, Gauge

try:
    from kafka import KafkaConsumer  # type: ignore
except Exception:  # pragma: no cover
    KafkaConsumer = None  # type: ignore

STREAM_EVENTS_CONSUMED = Counter('stream_events_consumed_total', 'Events consumed from streaming bus')
STREAM_BATCH_LATENCY = Histogram('stream_batch_latency_seconds', 'Kafka poll to enqueue latency', buckets=(0.01,0.05,0.1,0.25,0.5,1,2,5))
STREAM_LOOP_ERRORS = Counter('stream_loop_errors_total', 'Errors in streaming consumer loop', ['stage'])
STREAM_CONSUMER_LAG = Gauge('stream_consumer_lag_seconds', 'Consumer-record timestamp lag (approx)')
STREAM_INVALID_EVENTS = Counter('stream_invalid_events_total', 'Events dropped due to validation failures', ['reason'])


def _build_consumer():
    s = get_settings()
    if not (s.kafka_bootstrap_servers and s.kafka_events_topic):
        return None
    if KafkaConsumer is None:
        return None
    consumer = KafkaConsumer(
        s.kafka_events_topic,
        bootstrap_servers=[h.strip() for h in s.kafka_bootstrap_servers.split(',') if h.strip()],
        group_id=s.kafka_consumer_group,
        enable_auto_commit=True,
        auto_offset_reset='latest',
        value_deserializer=lambda v: _safe_json(v),
        consumer_timeout_ms=s.kafka_poll_timeout_ms,
        max_poll_records=s.kafka_max_batch,
    )
    return consumer


def _safe_json(raw: bytes):  # lenient parsing
    import json
    try:
        return json.loads(raw.decode('utf-8'))
    except Exception:
        return None


@shared_task
def consume_kafka_once(max_records: int | None = None):
    """Consume up to max_records (or configured batch) from Kafka and enqueue ingestion.

    Intended to be scheduled frequently (e.g., every few seconds) or run in a loop
    by a long-running process supervisor. Keeps logic idempotent and lightweight.
    """
    s = get_settings()
    consumer = _build_consumer()
    if not consumer:
        return {"status": "skipped", "reason": "kafka_not_configured"}
    batch = []
    invalid_counts: dict[str,int] = {}
    import time
    start = time.time()
    limit = max_records or s.kafka_max_batch
    try:
        for msg in consumer:  # iter stops on consumer_timeout_ms
            val = msg.value
            if not isinstance(val, dict):
                invalid_counts['non_dict'] = invalid_counts.get('non_dict',0)+1
                continue
            # Map incoming generic envelope to internal event schema
            ev = {
                'user_id': val.get('user_id') or val.get('uid') or 'anonymous',
                'session_id': val.get('session_id') or val.get('sid') or val.get('user_id') or 'sess',
                'event_name': val.get('event_name') or val.get('name') or 'unknown_event',
                'ts': _parse_ts(val.get('ts')),
                'props': val.get('props') or {k: v for k, v in val.items() if k not in {'user_id','uid','session_id','sid','event_name','name','ts'}},
                'schema_version': val.get('schema_version'),
            }
            # Basic validation: event_name not empty, ts not too far in future, props size reasonable
            if not ev['event_name']:
                invalid_counts['empty_name'] = invalid_counts.get('empty_name',0)+1
                continue
            if isinstance(ev['ts'], datetime) and (ev['ts'] - datetime.utcnow()).total_seconds() > 3600:
                invalid_counts['future_ts'] = invalid_counts.get('future_ts',0)+1
                continue
            if len(ev['props']) > 200:  # arbitrary safety cap
                invalid_counts['props_too_large'] = invalid_counts.get('props_too_large',0)+1
                continue
            batch.append(ev)
            if len(batch) >= limit:
                break
        if batch:
            STREAM_EVENTS_CONSUMED.inc(len(batch))
            ingest_events.delay(batch)  # reuse existing task path (async)
            try:
                newest = max(e['ts'] for e in batch if isinstance(e.get('ts'), datetime))
                if newest:
                    STREAM_CONSUMER_LAG.set((datetime.utcnow() - newest).total_seconds())
            except Exception:
                pass
    except Exception:
        STREAM_LOOP_ERRORS.labels(stage='consume').inc()
    finally:
        try:
            consumer.close()
        except Exception:
            pass
    import time as _t
    STREAM_BATCH_LATENCY.observe(time.time() - start)
    # Record invalid metrics
    for reason, cnt in invalid_counts.items():
        try:
            STREAM_INVALID_EVENTS.labels(reason=reason).inc(cnt)
        except Exception:
            pass
    return {"status": "ok", "consumed": len(batch), "invalid": invalid_counts}


def _parse_ts(val):
    if not val:
        return datetime.utcnow()
    if isinstance(val, datetime):
        return val
    import datetime as _dt
    for fmt in ('%Y-%m-%dT%H:%M:%S.%fZ','%Y-%m-%dT%H:%M:%S.%f','%Y-%m-%dT%H:%M:%SZ','%Y-%m-%dT%H:%M:%S'):
        try:
            return _dt.datetime.strptime(str(val), fmt)
        except Exception:
            continue
    try:
        # epoch seconds
        return datetime.utcfromtimestamp(float(val))
    except Exception:
        return datetime.utcnow()


@shared_task
def stream_health():
    s = get_settings()
    configured = bool(s.kafka_bootstrap_servers and s.kafka_events_topic)
    return {"configured": configured}
