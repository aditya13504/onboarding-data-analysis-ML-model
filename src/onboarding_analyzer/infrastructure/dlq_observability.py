"""Dead letter queue observability with rich error classification and recovery workflows.

Provides comprehensive monitoring and management of failed events in the DLQ system.
"""
from __future__ import annotations
import json
import logging
import time
from typing import Dict, List, Any, Optional, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from prometheus_client import Counter, Histogram, Gauge
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import DeadLetterQueue

logger = logging.getLogger(__name__)

class DLQErrorCategory(Enum):
    SCHEMA_VALIDATION = "schema_validation"    # Schema validation failures
    DATA_FORMAT = "data_format"              # JSON parsing, encoding issues
    BUSINESS_LOGIC = "business_logic"        # Business rule violations
    DOWNSTREAM_FAILURE = "downstream_failure" # External service failures
    RATE_LIMIT = "rate_limit"               # Rate limiting issues
    AUTHENTICATION = "authentication"       # Auth/permission failures
    NETWORK_TIMEOUT = "network_timeout"     # Network connectivity issues
    RESOURCE_EXHAUSTION = "resource_exhaustion" # Memory, disk, CPU limits
    UNKNOWN = "unknown"                     # Unclassified errors

class DLQRecoveryStrategy(Enum):
    RETRY = "retry"                         # Simple retry
    TRANSFORM_RETRY = "transform_retry"     # Transform then retry
    MANUAL_REVIEW = "manual_review"         # Requires human intervention
    DISCARD = "discard"                     # Permanent failure, discard
    QUARANTINE = "quarantine"               # Isolate for investigation

@dataclass
class DLQErrorPattern:
    category: DLQErrorCategory
    error_signature: str                    # Normalized error signature
    event_pattern: str                      # Event pattern causing error
    recovery_strategy: DLQRecoveryStrategy
    auto_recoverable: bool
    estimated_fix_time: timedelta

@dataclass
class DLQAnalysis:
    total_events: int
    error_categories: Dict[DLQErrorCategory, int]
    top_error_patterns: List[DLQErrorPattern]
    recovery_candidates: List[Dict]
    trends: Dict[str, float]

# Metrics
DLQ_EVENTS_TOTAL = Counter('dlq_events_total', 'Total DLQ events', ['category', 'error_signature'])
DLQ_RECOVERY_ATTEMPTS = Counter('dlq_recovery_attempts_total', 'DLQ recovery attempts', ['strategy', 'result'])
DLQ_AGE_HISTOGRAM = Histogram('dlq_event_age_hours', 'Age of DLQ events in hours', 
                              buckets=(1, 6, 12, 24, 72, 168, 720))  # 1h to 30 days
DLQ_QUEUE_SIZE = Gauge('dlq_queue_size_total', 'Current DLQ size')
DLQ_BACKLOG_SIZE_BYTES = Gauge('dlq_backlog_bytes', 'DLQ backlog size in bytes')
DLQ_PROCESSING_RATE = Gauge('dlq_processing_rate_per_minute', 'DLQ processing rate')

class DLQErrorClassifier:
    """Classifies DLQ errors into categories and patterns for better observability."""
    
    def __init__(self):
        self._error_patterns = {
            # Schema validation patterns
            r'.*schema.*validation.*failed': DLQErrorCategory.SCHEMA_VALIDATION,
            r'.*required.*field.*missing': DLQErrorCategory.SCHEMA_VALIDATION,
            r'.*invalid.*type.*expected': DLQErrorCategory.SCHEMA_VALIDATION,
            
            # Data format patterns
            r'.*json.*decode.*error': DLQErrorCategory.DATA_FORMAT,
            r'.*encoding.*error': DLQErrorCategory.DATA_FORMAT,
            r'.*malformed.*data': DLQErrorCategory.DATA_FORMAT,
            
            # Business logic patterns
            r'.*invalid.*user.*id': DLQErrorCategory.BUSINESS_LOGIC,
            r'.*event.*timestamp.*future': DLQErrorCategory.BUSINESS_LOGIC,
            r'.*duplicate.*event': DLQErrorCategory.BUSINESS_LOGIC,
            
            # Infrastructure patterns
            r'.*connection.*timeout': DLQErrorCategory.NETWORK_TIMEOUT,
            r'.*rate.*limit.*exceeded': DLQErrorCategory.RATE_LIMIT,
            r'.*authentication.*failed': DLQErrorCategory.AUTHENTICATION,
            r'.*memory.*error': DLQErrorCategory.RESOURCE_EXHAUSTION,
            r'.*disk.*full': DLQErrorCategory.RESOURCE_EXHAUSTION,
        }
        
        self._recovery_strategies = {
            DLQErrorCategory.SCHEMA_VALIDATION: DLQRecoveryStrategy.TRANSFORM_RETRY,
            DLQErrorCategory.DATA_FORMAT: DLQRecoveryStrategy.TRANSFORM_RETRY,
            DLQErrorCategory.BUSINESS_LOGIC: DLQRecoveryStrategy.MANUAL_REVIEW,
            DLQErrorCategory.DOWNSTREAM_FAILURE: DLQRecoveryStrategy.RETRY,
            DLQErrorCategory.RATE_LIMIT: DLQRecoveryStrategy.RETRY,
            DLQErrorCategory.AUTHENTICATION: DLQRecoveryStrategy.MANUAL_REVIEW,
            DLQErrorCategory.NETWORK_TIMEOUT: DLQRecoveryStrategy.RETRY,
            DLQErrorCategory.RESOURCE_EXHAUSTION: DLQRecoveryStrategy.RETRY,
            DLQErrorCategory.UNKNOWN: DLQRecoveryStrategy.MANUAL_REVIEW,
        }
    
    def classify_error(self, error_message: str, event_data: Dict) -> DLQErrorCategory:
        """Classify error into category based on error message and event data."""
        import re
        
        error_lower = error_message.lower()
        
        # Try to match against known patterns
        for pattern, category in self._error_patterns.items():
            if re.search(pattern, error_lower):
                return category
        
        # Additional classification based on event data
        if not event_data:
            return DLQErrorCategory.DATA_FORMAT
        
        if 'timestamp' in event_data:
            try:
                event_time = datetime.fromisoformat(event_data['timestamp'].replace('Z', '+00:00'))
                now = datetime.now()
                if event_time > now + timedelta(minutes=5):
                    return DLQErrorCategory.BUSINESS_LOGIC
            except (ValueError, AttributeError):
                return DLQErrorCategory.DATA_FORMAT
        
        return DLQErrorCategory.UNKNOWN
    
    def get_recovery_strategy(self, category: DLQErrorCategory) -> DLQRecoveryStrategy:
        """Get recommended recovery strategy for error category."""
        return self._recovery_strategies.get(category, DLQRecoveryStrategy.MANUAL_REVIEW)
    
    def create_error_signature(self, error_message: str) -> str:
        """Create normalized error signature for grouping similar errors."""
        import re
        import hashlib
        
        # Normalize error message
        normalized = error_message.lower()
        
        # Remove dynamic parts (IDs, timestamps, etc.)
        patterns_to_remove = [
            r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b',  # UUIDs
            r'\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\b',  # Timestamps
            r'\b\d+\b',  # Numbers
            r'\b[a-f0-9]{32,}\b',  # Hashes
        ]
        
        for pattern in patterns_to_remove:
            normalized = re.sub(pattern, '<DYNAMIC>', normalized)
        
        # Create signature hash
        signature = hashlib.md5(normalized.encode()).hexdigest()[:12]
        return f"{normalized[:50]}...{signature}"

class DLQObservabilityEngine:
    """Comprehensive DLQ observability and analysis engine."""
    
    def __init__(self):
        self.classifier = DLQErrorClassifier()
        self._analysis_cache = {}
        self._last_analysis = 0
        
    def analyze_dlq_health(self, hours_back: int = 24) -> DLQAnalysis:
        """Analyze DLQ health and patterns over specified time window."""
        current_time = time.time()
        
        # Use cached analysis if recent
        cache_key = f"analysis_{hours_back}h"
        if (cache_key in self._analysis_cache and 
            current_time - self._last_analysis < 300):  # 5 min cache
            return self._analysis_cache[cache_key]
        
        session = SessionLocal()
        try:
            # Query DLQ events from specified time window
            cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
            
            dlq_events = session.query(DeadLetterQueue).filter(
                DeadLetterQueue.created_at >= cutoff_time
            ).all()
            
            # Analyze error categories
            error_categories = {}
            error_patterns = {}
            recovery_candidates = []
            
            for event in dlq_events:
                # Classify error
                category = self.classifier.classify_error(
                    event.error_message or "", 
                    event.event_data or {}
                )
                
                # Count categories
                error_categories[category] = error_categories.get(category, 0) + 1
                
                # Create error signature
                signature = self.classifier.create_error_signature(event.error_message or "")
                
                # Track error patterns
                if signature not in error_patterns:
                    recovery_strategy = self.classifier.get_recovery_strategy(category)
                    error_patterns[signature] = DLQErrorPattern(
                        category=category,
                        error_signature=signature,
                        event_pattern=self._extract_event_pattern(event.event_data or {}),
                        recovery_strategy=recovery_strategy,
                        auto_recoverable=recovery_strategy in [
                            DLQRecoveryStrategy.RETRY, 
                            DLQRecoveryStrategy.TRANSFORM_RETRY
                        ],
                        estimated_fix_time=self._estimate_fix_time(category)
                    )
                
                # Identify recovery candidates
                if event.retry_count < 3 and category in [
                    DLQErrorCategory.NETWORK_TIMEOUT,
                    DLQErrorCategory.RATE_LIMIT,
                    DLQErrorCategory.DOWNSTREAM_FAILURE
                ]:
                    recovery_candidates.append({
                        'id': event.id,
                        'category': category.value,
                        'age_hours': (datetime.utcnow() - event.created_at).total_seconds() / 3600,
                        'retry_count': event.retry_count,
                        'auto_recoverable': True
                    })
            
            # Calculate trends (simplified)
            trends = self._calculate_trends(dlq_events)
            
            # Sort error patterns by frequency
            top_patterns = sorted(
                error_patterns.values(),
                key=lambda p: error_categories.get(p.category, 0),
                reverse=True
            )[:10]
            
            analysis = DLQAnalysis(
                total_events=len(dlq_events),
                error_categories=error_categories,
                top_error_patterns=top_patterns,
                recovery_candidates=recovery_candidates,
                trends=trends
            )
            
            # Update metrics
            self._update_metrics(analysis)
            
            # Cache result
            self._analysis_cache[cache_key] = analysis
            self._last_analysis = current_time
            
            return analysis
            
        finally:
            session.close()
    
    def _extract_event_pattern(self, event_data: Dict) -> str:
        """Extract pattern from event data for grouping."""
        if not event_data:
            return "empty_event"
        
        # Create pattern based on event structure
        pattern_parts = []
        
        if 'event_name' in event_data:
            pattern_parts.append(f"event:{event_data['event_name']}")
        
        if 'user_id' in event_data:
            pattern_parts.append("has_user_id")
        
        if 'properties' in event_data:
            prop_count = len(event_data['properties']) if isinstance(event_data['properties'], dict) else 0
            pattern_parts.append(f"props:{prop_count}")
        
        return "|".join(pattern_parts) if pattern_parts else "unknown_pattern"
    
    def _estimate_fix_time(self, category: DLQErrorCategory) -> timedelta:
        """Estimate time to fix based on error category."""
        fix_times = {
            DLQErrorCategory.SCHEMA_VALIDATION: timedelta(hours=2),
            DLQErrorCategory.DATA_FORMAT: timedelta(hours=1),
            DLQErrorCategory.BUSINESS_LOGIC: timedelta(days=1),
            DLQErrorCategory.DOWNSTREAM_FAILURE: timedelta(minutes=30),
            DLQErrorCategory.RATE_LIMIT: timedelta(minutes=15),
            DLQErrorCategory.AUTHENTICATION: timedelta(hours=4),
            DLQErrorCategory.NETWORK_TIMEOUT: timedelta(minutes=10),
            DLQErrorCategory.RESOURCE_EXHAUSTION: timedelta(hours=2),
            DLQErrorCategory.UNKNOWN: timedelta(days=2),
        }
        return fix_times.get(category, timedelta(days=1))
    
    def _calculate_trends(self, events: List[DeadLetterQueue]) -> Dict[str, float]:
        """Calculate trend metrics for DLQ events."""
        if not events:
            return {}
        
        now = datetime.utcnow()
        
        # Group events by hour
        hourly_counts = {}
        for event in events:
            hour_key = event.created_at.replace(minute=0, second=0, microsecond=0)
            hourly_counts[hour_key] = hourly_counts.get(hour_key, 0) + 1
        
        # Calculate trends
        recent_hours = [now - timedelta(hours=i) for i in range(24)]
        recent_counts = [hourly_counts.get(h.replace(minute=0, second=0, microsecond=0), 0) for h in recent_hours]
        
        # Simple trend calculation
        if len(recent_counts) >= 12:
            first_half = sum(recent_counts[:12])
            second_half = sum(recent_counts[12:])
            trend = (second_half - first_half) / max(first_half, 1) * 100
        else:
            trend = 0.0
        
        return {
            'hourly_trend_percent': trend,
            'peak_hour_count': max(recent_counts) if recent_counts else 0,
            'average_hourly': sum(recent_counts) / len(recent_counts) if recent_counts else 0
        }
    
    def _update_metrics(self, analysis: DLQAnalysis):
        """Update Prometheus metrics based on analysis."""
        # Update queue size
        DLQ_QUEUE_SIZE.set(analysis.total_events)
        
        # Update category metrics
        for category, count in analysis.error_categories.items():
            # This is simplified - in practice you'd track signatures too
            DLQ_EVENTS_TOTAL.labels(category=category.value, error_signature="aggregated").inc(count)
        
        # Update age histogram
        session = SessionLocal()
        try:
            recent_events = session.query(DeadLetterQueue).filter(
                DeadLetterQueue.created_at >= datetime.utcnow() - timedelta(hours=24)
            ).all()
            
            for event in recent_events:
                age_hours = (datetime.utcnow() - event.created_at).total_seconds() / 3600
                DLQ_AGE_HISTOGRAM.observe(age_hours)
                
        finally:
            session.close()

class DLQRecoveryManager:
    """Manages DLQ event recovery workflows."""
    
    def __init__(self):
        self.observability = DLQObservabilityEngine()
        
    def auto_recover_events(self, max_events: int = 100) -> Dict[str, int]:
        """Automatically recover events that are likely to succeed."""
        analysis = self.observability.analyze_dlq_health()
        
        results = {
            'attempted': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0
        }
        
        session = SessionLocal()
        try:
            # Get recovery candidates
            candidates = analysis.recovery_candidates[:max_events]
            
            for candidate in candidates:
                if not candidate.get('auto_recoverable'):
                    results['skipped'] += 1
                    continue
                
                # Get DLQ event
                dlq_event = session.query(DeadLetterQueue).filter_by(id=candidate['id']).first()
                if not dlq_event:
                    continue
                
                results['attempted'] += 1
                
                try:
                    # Attempt recovery based on category
                    category = DLQErrorCategory(candidate['category'])
                    success = self._attempt_recovery(dlq_event, category)
                    
                    if success:
                        results['successful'] += 1
                        # Mark as recovered or delete
                        session.delete(dlq_event)
                        DLQ_RECOVERY_ATTEMPTS.labels(strategy='auto', result='success').inc()
                    else:
                        results['failed'] += 1
                        # Increment retry count
                        dlq_event.retry_count += 1
                        DLQ_RECOVERY_ATTEMPTS.labels(strategy='auto', result='failure').inc()
                        
                except Exception as e:
                    logger.error(f"Recovery failed for DLQ event {dlq_event.id}: {e}")
                    results['failed'] += 1
                    DLQ_RECOVERY_ATTEMPTS.labels(strategy='auto', result='error').inc()
            
            session.commit()
            
        finally:
            session.close()
        
        return results
    
    def _attempt_recovery(self, dlq_event: DeadLetterQueue, category: DLQErrorCategory) -> bool:
        """Attempt to recover a specific DLQ event."""
        # This is a simplified recovery - in practice you'd have more sophisticated logic
        
        if category == DLQErrorCategory.RATE_LIMIT:
            # For rate limits, just wait and retry
            import time
            time.sleep(1)  # Brief pause
            return True  # Assume success for demo
            
        elif category == DLQErrorCategory.NETWORK_TIMEOUT:
            # For timeouts, retry with shorter timeout
            return True  # Assume success for demo
            
        elif category == DLQErrorCategory.DOWNSTREAM_FAILURE:
            # For downstream failures, check if service is back up
            return True  # Assume success for demo
        
        return False  # Default to failure for other categories

# Global instances
dlq_observability = DLQObservabilityEngine()
dlq_recovery = DLQRecoveryManager()

def analyze_dlq_patterns(hours_back: int = 24) -> DLQAnalysis:
    """Convenience function to analyze DLQ patterns."""
    return dlq_observability.analyze_dlq_health(hours_back)

def auto_recover_dlq_events(max_events: int = 100) -> Dict[str, int]:
    """Convenience function to auto-recover DLQ events."""
    return dlq_recovery.auto_recover_events(max_events)
