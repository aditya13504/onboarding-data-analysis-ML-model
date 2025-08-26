from __future__ import annotations
from .base import AnalyticsConnector, Event, logger
from datetime import datetime
from typing import Iterable
import requests
import base64


class MixpanelConnector(AnalyticsConnector):
    name = "mixpanel"

    def __init__(self, project_id: str, username: str, secret: str):
        self.project_id = project_id
        self.username = username
        self.secret = secret

    def _auth_header(self) -> dict:
        token = base64.b64encode(f"{self.username}:{self.secret}".encode()).decode()
        return {"Authorization": f"Basic {token}"}

    def fetch_events(self, since: datetime, until: datetime, cursor: str | None = None) -> Iterable[Event]:
        # Mixpanel export API only supports day range + optional hourly slices via 'from_date'/'to_date'.
        # We'll iterate day by day to avoid huge payloads; no native cursor, so we synthesize.
        from datetime import timedelta
        day_start = datetime(since.year, since.month, since.day)
        cur = day_start
        while cur <= until:
            day_end = cur + timedelta(days=1)
            params = {
                "from_date": cur.strftime("%Y-%m-%d"),
                "to_date": cur.strftime("%Y-%m-%d"),
                "project_id": self.project_id,
            }
            url = "https://data.mixpanel.com/api/2.0/export/"
            try:
                resp = requests.get(url, params=params, headers=self._auth_header(), timeout=120)
                if resp.status_code == 429:
                    self.backoff(int(resp.headers.get("Retry-After", "5") or 5))
                    continue
                resp.raise_for_status()
                for line in resp.text.splitlines():
                    if not line.strip():
                        continue
                    try:
                        evt = requests.utils.json.loads(line)
                    except Exception:  # noqa
                        continue
                    ts_epoch = evt.get("properties", {}).get("time", 0)
                    ts_dt = datetime.utcfromtimestamp(ts_epoch/1000) if ts_epoch else datetime.utcnow()
                    if ts_dt < since or ts_dt > until:
                        continue
                    yield {
                        "user_id": evt.get("distinct_id"),
                        "session_id": evt.get("properties", {}).get("$mp_session_id") or evt.get("properties", {}).get("distinct_id"),
                        "event_name": evt.get("event"),
                        "ts": ts_dt,
                        "props": evt.get("properties", {}),
                        "_cursor": cur.strftime("%Y-%m-%d"),
                    }
            except Exception as e:
                logger.warning("mixpanel fetch failed: %s", e)
            cur = day_end
