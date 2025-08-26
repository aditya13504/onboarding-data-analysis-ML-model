from __future__ import annotations
from .base import AnalyticsConnector, Event, logger
from datetime import datetime
from typing import Iterable
import requests


class PostHogConnector(AnalyticsConnector):
    name = "posthog"

    def __init__(self, api_key: str, project_api_host: str = "https://app.posthog.com"):
        self.api_key = api_key
        self.host = project_api_host.rstrip("/")

    def fetch_events(self, since: datetime, until: datetime, cursor: str | None = None) -> Iterable[Event]:
        url = cursor or f"{self.host}/api/events/"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        params = None if cursor else {"after": since.isoformat() + "Z", "before": until.isoformat() + "Z", "limit": 500}
        while url:
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=30)
                retry_after = int(resp.headers.get("Retry-After", "0") or 0)
                if resp.status_code == 429:
                    self.backoff(retry_after or 5)
                    continue
                resp.raise_for_status()
                data = resp.json()
                next_url = data.get("next")
                for evt in data.get("results", []):
                    yield {
                        "user_id": evt.get("distinct_id"),
                        "session_id": evt.get("properties", {}).get("$session_id") or evt.get("distinct_id"),
                        "event_name": evt.get("event"),
                        "ts": datetime.fromisoformat(evt.get("timestamp").replace("Z", "+00:00")),
                        "props": evt.get("properties", {}),
                        "_cursor": next_url,
                    }
                if not next_url:
                    break
                url, params = next_url, None
            except Exception as e:  # noqa
                logger.warning("posthog fetch failed: %s", e)
                break
