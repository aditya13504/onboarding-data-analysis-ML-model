from __future__ import annotations
from .base import AnalyticsConnector, Event, logger
from datetime import datetime
from typing import Iterable
import requests
import io
import zipfile
import json


class AmplitudeConnector(AnalyticsConnector):
    name = "amplitude"

    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    def fetch_events(self, since: datetime, until: datetime, cursor: str | None = None) -> Iterable[Event]:
        url = "https://amplitude.com/api/2/export"
        params = {"start": since.strftime("%Y%m%dT%H"), "end": until.strftime("%Y%m%dT%H")}
        try:
            resp = requests.get(url, params=params, auth=(self.api_key, self.secret_key), timeout=300)
            if resp.status_code == 429:
                self.backoff(int(resp.headers.get("Retry-After", "5") or 5))
                return []
            resp.raise_for_status()
            zf = zipfile.ZipFile(io.BytesIO(resp.content))
            for name in zf.namelist():
                with zf.open(name) as f:
                    for line in f:
                        if not line.strip():
                            continue
                        try:
                            raw = json.loads(line)
                        except Exception:
                            continue
                        props = raw.get("event_properties", {}) | raw.get("user_properties", {})
                        ts = raw.get("client_event_time") or raw.get("event_time")
                        try:
                            ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) if ts else datetime.utcnow()
                        except Exception:
                            ts_dt = datetime.utcnow()
                        yield {
                            "user_id": raw.get("user_id") or raw.get("amplitude_id"),
                            "session_id": str(raw.get("session_id")),
                            "event_name": raw.get("event_type"),
                            "ts": ts_dt,
                            "props": props,
                            "_cursor": params["end"],
                        }
        except Exception as e:  # noqa
            logger.warning("amplitude fetch failed: %s", e)
            return []
