"""Late and out-of-order event handling with watermarking and reconciliation.

Provides production-grade handling of events that arrive late or out of sequence.
"""
from __future__ import annotations
import logging
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from prometheus_client import Counter, Histogram, Gauge
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent, SessionSummary, ProjectFunnel

logger = logging.getLogger(__name__)

class EventOrderingStrategy(Enum):
    STRICT = "strict"           # Reject out-of-order events
    BUFFER = "buffer"           # Buffer events and reorder
    RECONCILE = "reconcile"     # Accept late events and reconcile
    APPEND_ONLY = "append_only" # Accept all events, handle in processing

@dataclass
class WatermarkConfig:
    max_lateness: timedelta     # Maximum allowed lateness
    buffer_window: timedelta    # How long to buffer events
    reconcile_window: timedelta # How far back to reconcile
    trigger_interval: timedelta # How often to trigger processing

@dataclass
class LateEvent:
    event_id: str
    user_id: str
    event_name: str
    timestamp: datetime
    received_at: datetime
    lateness: timedelta
    properties: Dict

# Metrics
LATE_EVENTS_TOTAL = Counter('late_events_total', 'Total late events received', ['event_name', 'lateness_bucket'])
OUT_OF_ORDER_EVENTS = Counter('out_of_order_events_total', 'Out of order events', ['event_name'])
RECONCILIATION_RUNS = Counter('reconciliation_runs_total', 'Reconciliation job runs', ['status'])
WATERMARK_LAG = Gauge('watermark_lag_seconds', 'Watermark lag behind wall clock time', ['pipeline'])
BUFFERED_EVENTS = Gauge('buffered_events_count', 'Events currently buffered', ['event_name'])

class EventWatermark:
    """Manages event processing watermarks for handling late arrivals."""
    
    def __init__(self, config: WatermarkConfig):
        self.config = config
        self._watermarks: Dict[str, datetime] = {}
        self._event_buffers: Dict[str, List[RawEvent]] = {}
        self._last_trigger: Dict[str, datetime] = {}
        
    def update_watermark(self, pipeline: str, event_time: datetime) -> bool:
        """Update watermark for a pipeline and return if processing should be triggered."""
        current_watermark = self._watermarks.get(pipeline, datetime.min)
        
        # Calculate new watermark (event time - max lateness)
        new_watermark = event_time - self.config.max_lateness
        
        if new_watermark > current_watermark:
            self._watermarks[pipeline] = new_watermark
            
            # Update metrics
            now = datetime.utcnow()
            lag = (now - new_watermark).total_seconds()
            WATERMARK_LAG.labels(pipeline=pipeline).set(lag)
            
            # Check if we should trigger processing
            last_trigger = self._last_trigger.get(pipeline, datetime.min)
            if now - last_trigger >= self.config.trigger_interval:
                self._last_trigger[pipeline] = now
                return True
        
        return False
    
    def get_watermark(self, pipeline: str) -> datetime:
        """Get current watermark for a pipeline."""
        return self._watermarks.get(pipeline, datetime.min)
    
    def is_late(self, pipeline: str, event_time: datetime) -> bool:
        """Check if an event is late based on current watermark."""
        watermark = self.get_watermark(pipeline)
        return event_time < watermark
    
    def buffer_event(self, pipeline: str, event: RawEvent) -> bool:
        """Buffer an event for reordering. Returns True if buffered, False if too late."""
        if pipeline not in self._event_buffers:
            self._event_buffers[pipeline] = []
        
        # Check if event is within buffer window
        watermark = self.get_watermark(pipeline)
        if event.timestamp < watermark - self.config.buffer_window:
            return False  # Too late to buffer
        
        self._event_buffers[pipeline].append(event)
        BUFFERED_EVENTS.labels(event_name=event.event_name).inc()
        
        return True
    
    def drain_buffer(self, pipeline: str) -> List[RawEvent]:
        """Drain buffered events that are ready for processing."""
        if pipeline not in self._event_buffers:
            return []
        
        watermark = self.get_watermark(pipeline)
        ready_events = []
        remaining_events = []
        
        for event in self._event_buffers[pipeline]:
            if event.timestamp <= watermark:
                ready_events.append(event)
                BUFFERED_EVENTS.labels(event_name=event.event_name).dec()
            else:
                remaining_events.append(event)
        
        self._event_buffers[pipeline] = remaining_events
        
        # Sort events by timestamp
        ready_events.sort(key=lambda e: e.timestamp)
        
        return ready_events

class LateEventHandler:
    """Handles late and out-of-order events with configurable strategies."""
    
    def __init__(self, strategy: EventOrderingStrategy = EventOrderingStrategy.RECONCILE):
        self.strategy = strategy
        self.watermark = EventWatermark(WatermarkConfig(
            max_lateness=timedelta(hours=1),
            buffer_window=timedelta(minutes=5),
            reconcile_window=timedelta(hours=24),
            trigger_interval=timedelta(minutes=1)
        ))
        
    def handle_event(self, event: RawEvent, pipeline: str = "default") -> Tuple[bool, Optional[str]]:
        """Handle an incoming event based on the configured strategy."""
        now = datetime.utcnow()
        lateness = now - event.timestamp
        
        # Update watermark
        should_trigger = self.watermark.update_watermark(pipeline, event.timestamp)
        
        # Record late event metrics
        if lateness > self.watermark.config.max_lateness:
            lateness_bucket = self._get_lateness_bucket(lateness)
            LATE_EVENTS_TOTAL.labels(
                event_name=event.event_name,
                lateness_bucket=lateness_bucket
            ).inc()
            
            late_event = LateEvent(
                event_id=event.event_id,
                user_id=event.user_id,
                event_name=event.event_name,
                timestamp=event.timestamp,
                received_at=now,
                lateness=lateness,
                properties=event.properties
            )
            
            logger.warning(f"Late event {event.event_id}: {lateness.total_seconds():.1f}s late")
        
        # Handle based on strategy
        if self.strategy == EventOrderingStrategy.STRICT:
            return self._handle_strict(event, pipeline)
        elif self.strategy == EventOrderingStrategy.BUFFER:
            return self._handle_buffer(event, pipeline)
        elif self.strategy == EventOrderingStrategy.RECONCILE:
            return self._handle_reconcile(event, pipeline)
        else:  # APPEND_ONLY
            return self._handle_append_only(event, pipeline)
    
    def _handle_strict(self, event: RawEvent, pipeline: str) -> Tuple[bool, Optional[str]]:
        """Strict ordering - reject events that are too late."""
        if self.watermark.is_late(pipeline, event.timestamp):
            return False, f"Event rejected: too late for strict ordering"
        return True, None
    
    def _handle_buffer(self, event: RawEvent, pipeline: str) -> Tuple[bool, Optional[str]]:
        """Buffer events for reordering."""
        if self.watermark.is_late(pipeline, event.timestamp):
            if self.watermark.buffer_event(pipeline, event):
                return False, f"Event buffered for reordering"
            else:
                return False, f"Event too late for buffering"
        return True, None
    
    def _handle_reconcile(self, event: RawEvent, pipeline: str) -> Tuple[bool, Optional[str]]:
        """Accept late events and mark for reconciliation."""
        if self.watermark.is_late(pipeline, event.timestamp):
            # Mark event for reconciliation
            event.properties = event.properties or {}
            event.properties['_requires_reconciliation'] = True
            event.properties['_received_late'] = True
            return True, f"Event accepted for reconciliation"
        return True, None
    
    def _handle_append_only(self, event: RawEvent, pipeline: str) -> Tuple[bool, Optional[str]]:
        """Accept all events - handle ordering in downstream processing."""
        if self.watermark.is_late(pipeline, event.timestamp):
            OUT_OF_ORDER_EVENTS.labels(event_name=event.event_name).inc()
            event.properties = event.properties or {}
            event.properties['_out_of_order'] = True
        return True, None
    
    def _get_lateness_bucket(self, lateness: timedelta) -> str:
        """Categorize lateness for metrics."""
        seconds = lateness.total_seconds()
        if seconds < 60:
            return "under_1m"
        elif seconds < 300:
            return "1m_to_5m"
        elif seconds < 3600:
            return "5m_to_1h"
        elif seconds < 86400:
            return "1h_to_1d"
        else:
            return "over_1d"
    
    def process_buffered_events(self, pipeline: str = "default") -> List[RawEvent]:
        """Process events that are ready from the buffer."""
        return self.watermark.drain_buffer(pipeline)

class ReconciliationEngine:
    """Reconciles late events with existing aggregations and summaries."""
    
    def __init__(self):
        self._reconciliation_queue: Set[Tuple[str, datetime]] = set()
        
    def mark_for_reconciliation(self, user_id: str, session_date: datetime):
        """Mark a user session for reconciliation."""
        self._reconciliation_queue.add((user_id, session_date.date()))
    
    def run_reconciliation(self) -> Dict[str, int]:
        """Run reconciliation for marked sessions."""
        session = SessionLocal()
        results = {
            'sessions_reconciled': 0,
            'funnels_updated': 0,
            'errors': 0
        }
        
        try:
            for user_id, session_date in list(self._reconciliation_queue):
                try:
                    # Reconcile session summary
                    if self._reconcile_session_summary(session, user_id, session_date):
                        results['sessions_reconciled'] += 1
                    
                    # Reconcile project funnels affected by this user
                    funnel_count = self._reconcile_project_funnels(session, user_id, session_date)
                    results['funnels_updated'] += funnel_count
                    
                    # Remove from queue
                    self._reconciliation_queue.discard((user_id, session_date))
                    
                except Exception as e:
                    logger.error(f"Reconciliation failed for {user_id} on {session_date}: {e}")
                    results['errors'] += 1
            
            session.commit()
            RECONCILIATION_RUNS.labels(status='success').inc()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Reconciliation batch failed: {e}")
            RECONCILIATION_RUNS.labels(status='error').inc()
            results['errors'] += len(self._reconciliation_queue)
        finally:
            session.close()
        
        return results
    
    def _reconcile_session_summary(self, session, user_id: str, session_date: datetime) -> bool:
        """Reconcile session summary for a user on a specific date."""
        from sqlalchemy import func, and_
        
        # Find existing session summary
        existing_summary = session.query(SessionSummary).filter(
            and_(
                SessionSummary.user_id == user_id,
                func.date(SessionSummary.session_date) == session_date
            )
        ).first()
        
        if not existing_summary:
            return False
        
        # Recalculate metrics from raw events
        events = session.query(RawEvent).filter(
            and_(
                RawEvent.user_id == user_id,
                func.date(RawEvent.timestamp) == session_date
            )
        ).order_by(RawEvent.timestamp).all()
        
        if not events:
            return False
        
        # Recalculate session metrics
        start_time = min(e.timestamp for e in events)
        end_time = max(e.timestamp for e in events)
        duration = (end_time - start_time).total_seconds()
        
        # Update existing summary
        existing_summary.start_time = start_time
        existing_summary.end_time = end_time
        existing_summary.duration_seconds = duration
        existing_summary.event_count = len(events)
        existing_summary.updated_at = datetime.utcnow()
        
        logger.info(f"Reconciled session summary for {user_id} on {session_date}")
        return True
    
    def _reconcile_project_funnels(self, session, user_id: str, session_date: datetime) -> int:
        """Reconcile project funnels affected by user events."""
        # Get projects affected by this user
        from sqlalchemy import func, and_, distinct
        
        projects = session.query(distinct(RawEvent.project)).filter(
            and_(
                RawEvent.user_id == user_id,
                func.date(RawEvent.timestamp) == session_date
            )
        ).all()
        
        reconciled_count = 0
        for (project,) in projects:
            if project:
                # Find funnel entries for this project and user
                funnel_entries = session.query(ProjectFunnel).filter(
                    and_(
                        ProjectFunnel.project == project,
                        ProjectFunnel.user_id == user_id,
                        func.date(ProjectFunnel.event_timestamp) == session_date
                    )
                ).all()
                
                # For each funnel entry, recalculate if it should exist
                # This is a simplified reconciliation - in practice you'd have more complex logic
                for entry in funnel_entries:
                    entry.updated_at = datetime.utcnow()
                    reconciled_count += 1
        
        return reconciled_count

# Global instances
late_event_handler = LateEventHandler()
reconciliation_engine = ReconciliationEngine()

def handle_late_event(event: RawEvent, pipeline: str = "default") -> bool:
    """Convenience function to handle late events."""
    accepted, reason = late_event_handler.handle_event(event, pipeline)
    
    if not accepted:
        logger.info(f"Event {event.event_id} handling: {reason}")
    elif reason:
        # Event accepted but requires special handling
        logger.debug(f"Event {event.event_id} handling: {reason}")
        
        # If event requires reconciliation, mark it
        if 'reconciliation' in reason.lower():
            reconciliation_engine.mark_for_reconciliation(event.user_id, event.timestamp)
    
    return accepted

def process_buffered_events(pipeline: str = "default") -> List[RawEvent]:
    """Process events ready from buffer."""
    return late_event_handler.process_buffered_events(pipeline)

def run_reconciliation() -> Dict[str, int]:
    """Run reconciliation for marked sessions."""
    return reconciliation_engine.run_reconciliation()
