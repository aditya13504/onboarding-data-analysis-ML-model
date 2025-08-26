from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Iterable, Dict, Any, Generator, Optional
import logging
import time
from prometheus_client import Counter, Histogram, Gauge
try:
    from onboarding_analyzer.api.main import registry as _api_registry  # type: ignore
except Exception:  # pragma: no cover - fallback
    _api_registry = None

CONNECTOR_CALLS = Counter('connector_calls_total', 'Connector fetch invocations', ['connector'], registry=_api_registry)
CONNECTOR_EVENTS = Counter('connector_events_total', 'Events pulled per connector', ['connector'], registry=_api_registry)
CONNECTOR_RETRIES = Counter('connector_retries_total', 'Retry attempts per connector', ['connector'], registry=_api_registry)
CONNECTOR_RATE_LIMITS = Counter('connector_rate_limits_total', 'Rate limit hits per connector', ['connector'], registry=_api_registry)
CONNECTOR_ERRORS = Counter('connector_errors_total', 'Errors during connector fetch', ['connector'], registry=_api_registry)
CONNECTOR_ERROR_CATEGORY = Counter('connector_error_category_total', 'Connector errors by category', ['connector','category'], registry=_api_registry)
CONNECTOR_LATENCY = Histogram('connector_fetch_latency_seconds', 'Latency of connector fetch calls', ['connector'], buckets=(0.1,0.5,1,2,5,10,30,60), registry=_api_registry)
CONNECTOR_BYTES = Counter('connector_bytes_total', 'Approx bytes received (if measurable)', ['connector'], registry=_api_registry)
CONNECTOR_WINDOW_LAG = Gauge('connector_window_lag_seconds', 'Lag between now and last_until_ts per connector', ['connector'], registry=_api_registry)
CONNECTOR_WINDOW_DURATION = Histogram('connector_window_duration_seconds', 'Duration seconds of processed connector windows', ['connector'], buckets=(30,60,120,300,600,1800), registry=_api_registry)
CONNECTOR_WINDOWS = Counter('connector_windows_total', 'Count of processed connector windows', ['connector'], registry=_api_registry)

logger = logging.getLogger(__name__)

Event = Dict[str, Any]


class AnalyticsConnector(ABC):
    name: str

    @abstractmethod
    def fetch_events(self, since: datetime, until: datetime, cursor: str | None = None) -> Iterable[Event]:
        """Yield events between windows.

        Implementations may use cursor for pagination / incremental checkpoints. Returned events
        may include a special field _cursor to persist for next call.
        """
        ...

    def backoff(self, retry_after: int | None):  # optional rate-limit handler
        if retry_after:
            CONNECTOR_RATE_LIMITS.labels(self.name).inc()
            time.sleep(min(retry_after, 60))

    def normalize(self, raw: Event) -> Event:
        # Basic passthrough; connector-specific overrides may map field names
        return raw

    def instrumented_fetch(self, since: datetime, until: datetime, cursor: Optional[str] = None) -> Generator[Event, None, None]:
        """Wrap fetch_events with metrics, retries, and normalization.

        Emits metrics for call count, events, errors, latency, and bytes (best-effort)."""
        CONNECTOR_CALLS.labels(self.name).inc()
        start = time.time()
        events_yielded = 0
        try:
            for ev in self.fetch_events(since, until, cursor=cursor):
                events_yielded += 1
                CONNECTOR_EVENTS.labels(self.name).inc()
                yield self.normalize(ev)
        except Exception:
            CONNECTOR_ERRORS.labels(self.name).inc()
            raise
        finally:
            CONNECTOR_LATENCY.labels(self.name).observe(time.time() - start)
        # lag gauge updated by orchestration when state persisted
        # (bytes metric left for connectors that can estimate payload sizes)


def window_chunks(start: datetime, end: datetime, delta: timedelta):
    cur = start
    while cur < end:
        nxt = min(cur + delta, end)
        yield cur, nxt
        cur = nxt
