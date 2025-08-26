from __future__ import annotations
from pydantic import BaseModel, Field, ValidationError
from datetime import datetime
from typing import Any, Dict


class EventSchemaV1(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)
    session_id: str = Field(min_length=1, max_length=128)
    event_name: str = Field(min_length=1, max_length=256)
    ts: datetime
    props: Dict[str, Any] = Field(default_factory=dict)

class EventSchemaV2(EventSchemaV1):  # example future version extension
    # could add fields or constraints
    pass

SCHEMA_REGISTRY = {
    "v1": EventSchemaV1,
    "v2": EventSchemaV2,
}

DEFAULT_SCHEMA_VERSION = "v1"


def validate_event(evt: dict) -> tuple[bool, str | None]:
    version = evt.get("schema_version") or DEFAULT_SCHEMA_VERSION
    model = SCHEMA_REGISTRY.get(version)
    if not model:
        return False, f"unknown_schema_version:{version}"
    try:
        model(**{**evt, "schema_version": version})
        # Additional lightweight limits: props size
        props = evt.get("props", {}) or {}
        if len(props) > 100:
            return False, "too_many_props"
        return True, None
    except ValidationError as ve:
        return False, f"validation_error:{ve.errors()[0].get('msg','invalid')}"