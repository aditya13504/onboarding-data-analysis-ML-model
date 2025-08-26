from __future__ import annotations

"""Kafka offset auditing tasks.

Collects end (high watermark) offsets and committed consumer group offsets for
configured topics to derive lag, persisting results for observability and alerting.

If kafka-python is not installed or settings not provided, tasks return skipped.
"""
from datetime import datetime
from celery import shared_task
from prometheus_client import Gauge
from onboarding_analyzer.config import get_settings
from onboarding_analyzer.infrastructure.db import SessionLocal
from sqlalchemy.orm import Session
from sqlalchemy import Index

try:
    from kafka import KafkaAdminClient, KafkaConsumer  # type: ignore
except Exception:  # pragma: no cover
    KafkaAdminClient = None  # type: ignore
    KafkaConsumer = None  # type: ignore

from sqlalchemy import Column, Integer, String, DateTime, JSON, Float
from onboarding_analyzer.models.tables import KafkaConsumerLag


KAFKA_PARTITION_LAG = Gauge(
    'kafka_partition_lag', 'Kafka partition lag (end_offset - committed)', ['group','topic','partition']
)
KAFKA_TOPIC_LAG = Gauge(
    'kafka_topic_total_lag', 'Kafka aggregated topic lag for group', ['group','topic']
)


def _ensure_table(session: Session):
    # Table managed by Alembic migration; fallback creation if missing (idempotent)
    try:
        engine = session.get_bind()
        if not engine.dialect.has_table(engine.connect(), KafkaConsumerLag.__tablename__):  # type: ignore
            KafkaConsumerLag.__table__.create(bind=engine)
    except Exception:
        pass


@shared_task
def audit_kafka_consumer_offsets():
    s = get_settings()
    if not (s.kafka_bootstrap_servers and s.kafka_events_topic and s.kafka_consumer_group):
        return {"status": "skipped", "reason": "kafka_not_configured"}
    if KafkaConsumer is None:
        return {"status": "skipped", "reason": "client_not_installed"}
    topics = [t.strip() for t in s.kafka_events_topic.split(',') if t.strip()]
    group = s.kafka_consumer_group
    # Use a consumer to fetch end offsets and committed offsets.
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=[h.strip() for h in s.kafka_bootstrap_servers.split(',') if h.strip()],
        group_id=group,
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
    )
    session: Session = SessionLocal()
    try:
        _ensure_table(session)
        assignment = consumer.assignment()
        if not assignment:
            # poll once to get assignment
            consumer.poll(timeout_ms=100)
            assignment = consumer.assignment()
        end_offsets = consumer.end_offsets(list(assignment)) if assignment else {}
        results = []
        topic_lag_acc: dict[tuple[str,str], int] = {}
        for tp, end in end_offsets.items():
            committed = consumer.committed(tp)
            lag = None
            if committed is not None and end is not None:
                lag = max(end - committed, 0)
            rec = KafkaConsumerLag(
                group_id=group,
                topic=tp.topic,
                partition=tp.partition,
                end_offset=end or 0,
                committed_offset=committed,
                lag=lag,
                details=None,
            )
            session.add(rec)
            results.append({
                "topic": tp.topic,
                "partition": tp.partition,
                "end_offset": end,
                "committed_offset": committed,
                "lag": lag,
            })
            if lag is not None:
                topic_lag_acc[(group, tp.topic)] = topic_lag_acc.get((group, tp.topic), 0) + lag
                try:
                    KAFKA_PARTITION_LAG.labels(group=group, topic=tp.topic, partition=str(tp.partition)).set(lag)
                except Exception:
                    pass
        session.commit()
        # Aggregate gauges
        for (g, topic), lag_sum in topic_lag_acc.items():
            try:
                KAFKA_TOPIC_LAG.labels(group=g, topic=topic).set(lag_sum)
            except Exception:
                pass
        return {"status": "ok", "partitions": len(results), "results": results}
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        session.close()