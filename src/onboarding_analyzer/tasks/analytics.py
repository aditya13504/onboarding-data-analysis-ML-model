from celery import shared_task
from sqlalchemy.orm import Session
from sqlalchemy import select
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent, FunnelMetric
from sqlalchemy import func


def _derive_ordered_steps(session: Session) -> list[str]:
    # Derive steps by average timestamp order across sessions/users
    q = select(RawEvent.event_name, func.avg(func.extract("epoch", RawEvent.ts))).group_by(RawEvent.event_name).order_by(func.avg(func.extract("epoch", RawEvent.ts)))
    steps = [row[0] for row in session.execute(q)]
    return steps


@shared_task
def compute_funnel_metrics(project: str | None = None):
    session: Session = SessionLocal()
    try:
        # Determine which project scopes to compute
        if project:
            project_scopes = [project]
        else:
            # include None (global aggregate) plus distinct project values
            proj_rows = session.execute(select(RawEvent.project).distinct())
            distinct_projects = [row[0] for row in proj_rows if row[0] is not None]
            project_scopes = [None] + distinct_projects
        total_steps = 0
        for proj in project_scopes:
            # Clear existing metrics for this scope
            if proj is None:
                session.query(FunnelMetric).filter(FunnelMetric.project.is_(None)).delete()
            else:
                session.query(FunnelMetric).filter(FunnelMetric.project == proj).delete()
            users_per_step: list[set[str]] = []
            if proj is None:
                steps = _derive_ordered_steps(session)
            else:
                q_steps = select(RawEvent.event_name, func.avg(func.extract("epoch", RawEvent.ts))).where(RawEvent.project == proj).group_by(RawEvent.event_name).order_by(func.avg(func.extract("epoch", RawEvent.ts)))
                steps = [row[0] for row in session.execute(q_steps)]
            for step in steps:
                if proj is None:
                    q = select(RawEvent.user_id).where(RawEvent.event_name == step)
                else:
                    q = select(RawEvent.user_id).where(RawEvent.event_name == step, RawEvent.project == proj)
                users = {row[0] for row in session.execute(q)}
                users_per_step.append(users)
            for i, step in enumerate(steps):
                entered = users_per_step[i - 1] if i > 0 else users_per_step[0] if users_per_step else set()
                converted = users_per_step[i] if users_per_step else set()
                drop_off_users = entered - converted if i > 0 else set()
                metric = FunnelMetric(
                    step_name=step,
                    step_order=i,
                    users_entered=len(entered),
                    users_converted=len(converted),
                    drop_off=len(drop_off_users),
                    conversion_rate=(len(converted) / len(entered) if entered else 0.0),
                    project=proj,
                )
                session.add(metric)
            total_steps += len(steps)
        session.commit()
        return {"status": "ok", "scopes": len(project_scopes), "total_steps": total_steps}
    finally:
        session.close()
