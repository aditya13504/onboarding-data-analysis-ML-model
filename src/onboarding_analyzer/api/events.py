from __future__ import annotations
from fastapi import APIRouter, Body, Header, Depends
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from onboarding_analyzer.tasks.ingestion import ingest_events
from onboarding_analyzer.security.hmac import verify_hmac
from onboarding_analyzer.config import get_settings
import os

router = APIRouter(prefix="/events", tags=["events"])


class EventIn(BaseModel):
    user_id: str
    session_id: str
    event_name: str
    ts: datetime = Field(default_factory=datetime.utcnow)
    props: dict = Field(default_factory=dict)
    schema_version: str | None = None
    idempotency_key: str | None = None


@router.post("/bulk")
def ingest_bulk(
    events: List[EventIn] = Body(...),
    x_signature: Optional[str] = Header(None, alias="X-Signature"),
    x_idempotency_key: Optional[str] = Header(None, alias="X-Idempotency-Key"),
):
    settings = get_settings()
    if settings.ingest_secret:
        if not x_signature:
            raise RuntimeError("missing signature")
        # For signature verification we intentionally exclude server-added fields (like ts default)
        # and mirror the client's Python repr-based signing approach (list[dict]) used in tests.
        signed_body_list = [
            {
                "user_id": e.user_id,
                "session_id": e.session_id,
                "event_name": e.event_name,
                "props": e.props,
            }
            for e in events
        ]
        import json
        canonical_body = json.dumps(signed_body_list, separators=(",", ":"), sort_keys=True).encode()
        verify_hmac(
            x_signature,
            body=canonical_body,
            secret=settings.ingest_secret,
        )
    # Apply header idempotency key if provided; if both body and header present, body wins
    payload = []
    for e in events:
        data = e.model_dump()
        if not data.get('idempotency_key') and x_idempotency_key:
            data['idempotency_key'] = x_idempotency_key
        payload.append(data)
    # If running in test environment without broker, call task function directly
    from onboarding_analyzer.config import get_settings as _gs
    _s = _gs()
    if (_s.app_env == "test") or (os.getenv("APP_ENV") == "test"):
        ingest_events(payload)  # type: ignore
        task_id = None
    else:
        result = ingest_events.delay(payload)  # async persist
        task_id = result.id
    return {"accepted": len(events), "task_id": task_id}
