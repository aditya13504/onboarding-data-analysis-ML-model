from __future__ import annotations
from celery import shared_task
from sqlalchemy.orm import Session
from sqlalchemy import select
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent, SessionSummary
from onboarding_analyzer.tasks.analytics import _derive_ordered_steps
from onboarding_analyzer.config import get_settings

# Session timeout is now configurable via settings.session_timeout_minutes
# Backward compatibility constant expected by tests
SESSION_TIMEOUT_MIN = get_settings().session_timeout_minutes


@shared_task
def rebuild_session_summaries(limit_users: int | None = None):
    session: Session = SessionLocal()
    try:
        # naive full rebuild (optimize later with incremental logic)
        session.query(SessionSummary).delete()
        q = select(RawEvent.user_id, RawEvent.session_id, RawEvent.project, RawEvent.event_name, RawEvent.ts).order_by(RawEvent.user_id, RawEvent.session_id, RawEvent.ts)
        ordered_steps = _derive_ordered_steps(session)
        total_possible = max(len(ordered_steps), 1)
        current_key = None
        events_buffer = []
        first_ts = None
        last_ts = None
        project = None
        def flush():
            nonlocal events_buffer, first_ts, last_ts, project
            if not events_buffer:
                return
            steps = [e[0] for e in events_buffer]
            unique_steps = set(steps)
            completion_ratio = len(unique_steps)/total_possible
            ss = SessionSummary(
                user_id=events_buffer[0][1],
                session_id=events_buffer[0][2],
                project=project,
                start_ts=first_ts,
                end_ts=last_ts,
                duration_sec=(last_ts-first_ts).total_seconds() if last_ts and first_ts else 0.0,
                steps_count=len(unique_steps),
                completion_ratio=completion_ratio,
            )
            session.add(ss)
            events_buffer = []
        last_event_ts = None
        part_counter = 1
        settings = get_settings()
        timeout_min = settings.session_timeout_minutes
        synthetic_counter = 0
        for user_id, session_id, proj, event_name, ts in session.execute(q):
            key = (user_id, session_id)
            if current_key != key:
                flush()
                current_key = key
                first_ts = ts
                last_event_ts = ts
                project = proj
                part_counter = 1
            else:
                if last_event_ts and (ts - last_event_ts).total_seconds() > timeout_min * 60:
                    flush()
                    part_counter += 1
                    current_key = key
                    first_ts = ts
                last_event_ts = ts
            last_ts = ts
            effective_session_id = session_id or "synthetic"
            if not session_id:
                synthetic_counter += 1
                effective_session_id = f"syn_{user_id}_{synthetic_counter}"
            virtual_session_id = f"{effective_session_id}_p{part_counter}" if part_counter > 1 else effective_session_id
            events_buffer.append((event_name, user_id, virtual_session_id, ts))
        flush()
        session.commit()
        return {"status": "ok"}
    finally:
        session.close()
