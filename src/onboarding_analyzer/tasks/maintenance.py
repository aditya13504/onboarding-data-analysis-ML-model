from __future__ import annotations
from celery import shared_task
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import delete
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent, RawEventArchive, ArchiveCompactedEvent, OpsMetric, IngestionDeadLetter
from onboarding_analyzer.config import get_settings
from .clustering import refresh_clusters
from sqlalchemy import select, func, text
import math


@shared_task
def enforce_retention():
    settings = get_settings()
    session: Session = SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(days=settings.retention_days)
        stmt = delete(RawEvent).where(RawEvent.ts < cutoff)
        res = session.execute(stmt)
        session.commit()
        return {"status": "ok", "deleted": res.rowcount or 0}
    finally:
        session.close()


@shared_task
def archive_old_events(days: int = 30, batch: int = 1000):
    """Move events older than N days into archive table (soft partitioning)."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    session: Session = SessionLocal()
    moved = 0
    try:
        while True:
            rows = session.query(RawEvent).filter(RawEvent.ts < cutoff).order_by(RawEvent.id).limit(batch).all()
            if not rows:
                break
            for r in rows:
                session.add(RawEventArchive(
                    event_id=r.event_id,
                    user_id=r.user_id,
                    session_id=r.session_id,
                    event_name=r.event_name,
                    ts=r.ts,
                    props=r.props,
                    schema_version=r.schema_version,
                    project=r.project,
                ))
                session.delete(r)
                moved += 1
            session.commit()
        return {"status": "ok", "archived": moved}
    finally:
        session.close()

def run_archive(days: int = 30):  # convenience callable expected by tests (non-task wrapper)
    """Run archival flow (move old events into archive table) synchronously.

    Returns same structure as archive_old_events task body.
    """
    return archive_old_events.run(days=days)  # type: ignore[attr-defined]


@shared_task
def compact_archive():
    """Aggregate old archived events into compacted daily buckets and delete raw archive rows.

    Strategy:
      * Identify archive rows older than settings.archive_compact_after_days not yet compacted.
      * Group by (project, event_name, date) and compute counts (#events, distinct users, distinct sessions), sample props.
      * Insert/update into archive_compacted_events (idempotent via unique key).
      * Delete the raw archive rows processed (in batches) to reclaim space.
    """
    settings = get_settings()
    session: Session = SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(days=settings.archive_compact_after_days)
        # Process in date batches (oldest first)
        # Find distinct dates needing compaction
        dates = (
            session.query(func.date(RawEventArchive.ts))
            .filter(RawEventArchive.ts < cutoff)
            .distinct()
            .order_by(func.date(RawEventArchive.ts))
            .limit(settings.archive_compaction_batch_days)
            .all()
        )
        if not dates:
            return {"status": "skipped", "reason": "nothing_to_compact"}
        processed_days = 0
        total_events = 0
        for (d,) in dates:
            # SQLite returns string for func.date, Postgres returns date object
            if isinstance(d, str):
                year, month, day = map(int, d.split('-'))
                day_start = datetime(year, month, day)
            else:  # date/datetime
                day_start = datetime(d.year, d.month, d.day)
            day_end = day_start + timedelta(days=1)
            # Aggregate per (project,event_name)
            rows = (
                session.query(
                    RawEventArchive.project,
                    RawEventArchive.event_name,
                    func.count(RawEventArchive.id),
                    func.count(func.distinct(RawEventArchive.user_id)),
                    func.count(func.distinct(RawEventArchive.session_id)),
                    func.min(RawEventArchive.ts),
                    func.max(RawEventArchive.ts),
                )
                .filter(RawEventArchive.ts >= day_start, RawEventArchive.ts < day_end)
                .group_by(RawEventArchive.project, RawEventArchive.event_name)
                .all()
            )
            for project, event_name, ev_count, user_count, sess_count, first_ts, last_ts in rows:
                sample = (
                    session.query(RawEventArchive.props)
                    .filter(
                        RawEventArchive.project == project,
                        RawEventArchive.event_name == event_name,
                        RawEventArchive.ts >= day_start,
                        RawEventArchive.ts < day_end,
                    )
                    .first()
                )
                # Upsert (manual) - try fetch existing
                existing = (
                    session.query(ArchiveCompactedEvent)
                    .filter(
                        ArchiveCompactedEvent.project == project,
                        ArchiveCompactedEvent.event_name == event_name,
                        ArchiveCompactedEvent.event_date == day_start,
                    )
                    .first()
                )
                if existing:
                    # Update counts if changed (idempotent safe)
                    existing.events_count = int(ev_count)
                    existing.users_count = int(user_count)
                    existing.sessions_count = int(sess_count)
                    existing.first_ts = first_ts
                    existing.last_ts = last_ts
                    if sample and not existing.sample_props:
                        existing.sample_props = sample[0]
                else:
                    session.add(
                        ArchiveCompactedEvent(
                            project=project,
                            event_name=event_name,
                            event_date=day_start,
                            events_count=int(ev_count),
                            users_count=int(user_count),
                            sessions_count=int(sess_count),
                            sample_props=sample[0] if sample else None,
                            first_ts=first_ts,
                            last_ts=last_ts,
                        )
                    )
                total_events += int(ev_count)
            # Delete raw archive rows for this day
            session.query(RawEventArchive).filter(
                RawEventArchive.ts >= day_start, RawEventArchive.ts < day_end
            ).delete(synchronize_session=False)
            session.commit()
            processed_days += 1
        return {
            "status": "ok",
            "days": processed_days,
            "events_compacted": total_events,
        }
    finally:
        session.close()


def _psi(dist_a: list[float], dist_b: list[float], buckets: int = 10) -> float:
    if not dist_a or not dist_b:
        return 0.0
    mn = min(min(dist_a), min(dist_b))
    mx = max(max(dist_a), max(dist_b))
    if mn == mx:
        return 0.0
    width = (mx - mn) / buckets
    def hist(data):
        h = [0]*buckets
        for v in data:
            idx = min(int((v - mn)/width), buckets-1)
            h[idx]+=1
        total = sum(h) or 1
        return [x/total for x in h]
    pa = hist(dist_a)
    pb = hist(dist_b)
    psi = 0.0
    for a,b in zip(pa,pb):
        if a>0 and b>0:
            psi += (a-b)*math.log(a/b)
    return psi


@shared_task
def scheduled_retrain(psi_threshold: float = 0.2):
    session: Session = SessionLocal()
    try:
        # Use event inter-arrival durations as drift proxy
        q = select(RawEvent.ts).order_by(RawEvent.ts.desc()).limit(5000)
        times = [row[0] for row in session.execute(q)]
        times.sort()
        if len(times) < 50:
            refresh_clusters.delay()
            return {"status": "queued", "reason": "low_data"}
        deltas = [(t2 - t1).total_seconds() for t1, t2 in zip(times[:-1], times[1:])]
        split = len(deltas)//2
        old, new = deltas[:split], deltas[split:]
        psi = _psi(old, new)
        if psi >= psi_threshold:
            refresh_clusters.delay()
            return {"status": "queued", "psi": psi}
        return {"status": "skipped", "psi": psi}
    finally:
        session.close()


@shared_task
def precreate_next_month_partition():
    """Proactively create the next month's partition (Postgres only)."""
    settings = get_settings()
    if not settings.partitioning_enabled:
        return {"status": "skipped", "reason": "disabled"}
    # Only meaningful for postgres
    from onboarding_analyzer.infrastructure.db import engine
    if engine.dialect.name != 'postgresql':
        return {"status": "skipped", "reason": "not_postgres"}
    from datetime import datetime
    # Compute first day of next month (UTC) without external deps
    now = datetime.utcnow()
    year = now.year + (1 if now.month == 12 else 0)
    month = 1 if now.month == 12 else now.month + 1
    target = datetime(year, month, 1)
    suffix = target.strftime('%Y%m')
    part_name = f'raw_events_{suffix}'
    sql = f"SELECT raw_events_ensure_partition('{target.isoformat()}')"
    with engine.begin() as conn:
        try:
            conn.execute(text(sql))
        except Exception as e:
            return {"status": "error", "error": str(e)}
    return {"status": "ok", "partition": part_name}


@shared_task
def capture_ops_metrics():
    """Capture lightweight operational KPIs (ingest backlog, DLQ size, event growth)."""
    session: Session = SessionLocal()
    try:
        now = datetime.utcnow()
        # Total events last 24h
        last_day = now - timedelta(hours=24)
        ev_24h = session.query(func.count(RawEvent.id)).filter(RawEvent.ts >= last_day).scalar() or 0
        # Dead letter size
        dlq = session.query(func.count(IngestionDeadLetter.id)).scalar() or 0
        # Event growth rate (compare last 24h vs previous 24h)
        prev_start = last_day - timedelta(hours=24)
        prev_count = session.query(func.count(RawEvent.id)).filter(RawEvent.ts >= prev_start, RawEvent.ts < last_day).scalar() or 0
        growth = ((ev_24h - prev_count) / prev_count) if prev_count else None
        metrics = [
            ("events_24h", ev_24h, {"window_hours":24}),
            ("dlq_count", dlq, None),
        ]
        if growth is not None:
            metrics.append(("event_growth_ratio", float(growth), {"prev_window_hours":24}))
        for name, value, details in metrics:
            session.add(OpsMetric(metric_name=name, metric_value=float(value), details=details))
        session.commit()
        return {"status":"ok","metrics_captured":len(metrics)}
    finally:
        session.close()
