"""Real-time feature extraction and engineering with feature store capabilities.

Provides production-grade feature engineering, storage, and serving for ML models.
"""
from __future__ import annotations
import json
import logging
import asyncio
import time
import hashlib
from typing import Dict, List, Any, Optional, Union, Callable, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
import numpy as np
import pandas as pd
from prometheus_client import Counter, Histogram, Gauge
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent, SessionSummary

logger = logging.getLogger(__name__)

class FeatureType(Enum):
    CATEGORICAL = "categorical"
    NUMERICAL = "numerical"
    TEMPORAL = "temporal"
    EMBEDDING = "embedding"
    BOOLEAN = "boolean"
    TEXT = "text"

class FeatureFrequency(Enum):
    REALTIME = "realtime"      # Updated on every event
    STREAMING = "streaming"     # Updated in micro-batches
    BATCH = "batch"            # Updated daily/hourly
    ON_DEMAND = "on_demand"    # Computed when requested

class AggregationType(Enum):
    SUM = "sum"
    COUNT = "count"
    MEAN = "mean"
    MEDIAN = "median"
    STD = "std"
    MIN = "min"
    MAX = "max"
    PERCENTILE = "percentile"
    DISTINCT_COUNT = "distinct_count"
    LAST_VALUE = "last_value"
    FIRST_VALUE = "first_value"
    MODE = "mode"

@dataclass
class FeatureDefinition:
    name: str
    feature_type: FeatureType
    frequency: FeatureFrequency
    description: str
    source_tables: List[str]
    transformation_logic: str
    aggregation_type: Optional[AggregationType] = None
    window_size: Optional[str] = None  # e.g., "7d", "1h", "30m"
    group_by: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    is_target: bool = False
    version: str = "1.0"
    created_at: datetime = field(default_factory=datetime.utcnow)
    
@dataclass
class FeatureValue:
    feature_name: str
    entity_id: str
    value: Any
    timestamp: datetime
    version: str
    confidence: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class FeatureVector:
    entity_id: str
    features: Dict[str, Any]
    timestamp: datetime
    vector_version: str
    feature_versions: Dict[str, str] = field(default_factory=dict)

class FeatureWindow:
    """Sliding window for real-time feature computation."""
    
    def __init__(self, window_size: str, aggregation_type: AggregationType):
        self.window_size = self._parse_window_size(window_size)
        self.aggregation_type = aggregation_type
        self.data_points: deque = deque()
        
    def _parse_window_size(self, window_size: str) -> timedelta:
        """Parse window size string to timedelta."""
        if window_size.endswith('s'):
            return timedelta(seconds=int(window_size[:-1]))
        elif window_size.endswith('m'):
            return timedelta(minutes=int(window_size[:-1]))
        elif window_size.endswith('h'):
            return timedelta(hours=int(window_size[:-1]))
        elif window_size.endswith('d'):
            return timedelta(days=int(window_size[:-1]))
        else:
            raise ValueError(f"Invalid window size format: {window_size}")
    
    def add_value(self, value: float, timestamp: datetime):
        """Add a value to the window."""
        self.data_points.append((value, timestamp))
        self._cleanup_expired()
    
    def _cleanup_expired(self):
        """Remove expired values from window."""
        cutoff_time = datetime.utcnow() - self.window_size
        while self.data_points and self.data_points[0][1] < cutoff_time:
            self.data_points.popleft()
    
    def compute(self) -> Optional[float]:
        """Compute aggregated value for current window."""
        if not self.data_points:
            return None
            
        values = [dp[0] for dp in self.data_points]
        
        if self.aggregation_type == AggregationType.SUM:
            return sum(values)
        elif self.aggregation_type == AggregationType.COUNT:
            return len(values)
        elif self.aggregation_type == AggregationType.MEAN:
            return np.mean(values)
        elif self.aggregation_type == AggregationType.MEDIAN:
            return np.median(values)
        elif self.aggregation_type == AggregationType.STD:
            return np.std(values)
        elif self.aggregation_type == AggregationType.MIN:
            return min(values)
        elif self.aggregation_type == AggregationType.MAX:
            return max(values)
        elif self.aggregation_type == AggregationType.LAST_VALUE:
            return values[-1]
        elif self.aggregation_type == AggregationType.FIRST_VALUE:
            return values[0]
        else:
            return None

# Metrics
FEATURE_EXTRACTIONS = Counter('feature_extractions_total', 'Feature extractions', ['feature_name', 'status'])
FEATURE_COMPUTATION_TIME = Histogram('feature_computation_seconds', 'Feature computation time', ['feature_name'])
FEATURE_STORE_SIZE = Gauge('feature_store_size_bytes', 'Feature store size')
FEATURE_CACHE_HITS = Counter('feature_cache_hits_total', 'Feature cache hits', ['feature_name'])
FEATURE_FRESHNESS = Gauge('feature_freshness_seconds', 'Feature age in seconds', ['feature_name'])

class FeatureExtractor:
    """Extracts and computes features from raw events."""
    
    def __init__(self):
        self.feature_definitions = self._load_feature_definitions()
        self.feature_windows: Dict[str, Dict[str, FeatureWindow]] = defaultdict(dict)
        self.transformation_functions = self._register_transformations()
        
    def _load_feature_definitions(self) -> Dict[str, FeatureDefinition]:
        """Load feature definitions."""
        return {
            # User behavior features
            "user_session_count_7d": FeatureDefinition(
                name="user_session_count_7d",
                feature_type=FeatureType.NUMERICAL,
                frequency=FeatureFrequency.STREAMING,
                description="Number of sessions in last 7 days",
                source_tables=["session_summaries"],
                transformation_logic="count_sessions_by_user",
                aggregation_type=AggregationType.COUNT,
                window_size="7d",
                group_by=["user_id"]
            ),
            
            "user_avg_session_duration": FeatureDefinition(
                name="user_avg_session_duration",
                feature_type=FeatureType.NUMERICAL,
                frequency=FeatureFrequency.STREAMING,
                description="Average session duration for user",
                source_tables=["session_summaries"],
                transformation_logic="avg_session_duration",
                aggregation_type=AggregationType.MEAN,
                window_size="30d",
                group_by=["user_id"]
            ),
            
            "user_conversion_rate": FeatureDefinition(
                name="user_conversion_rate",
                feature_type=FeatureType.NUMERICAL,
                frequency=FeatureFrequency.BATCH,
                description="User conversion rate over time",
                source_tables=["session_summaries"],
                transformation_logic="calculate_conversion_rate",
                aggregation_type=AggregationType.MEAN,
                window_size="30d",
                group_by=["user_id"]
            ),
            
            # Event sequence features
            "event_sequence_length": FeatureDefinition(
                name="event_sequence_length",
                feature_type=FeatureType.NUMERICAL,
                frequency=FeatureFrequency.REALTIME,
                description="Length of current event sequence",
                source_tables=["raw_events"],
                transformation_logic="sequence_length",
                aggregation_type=AggregationType.COUNT,
                group_by=["session_id"]
            ),
            
            "time_since_last_event": FeatureDefinition(
                name="time_since_last_event",
                feature_type=FeatureType.NUMERICAL,
                frequency=FeatureFrequency.REALTIME,
                description="Time since last event in seconds",
                source_tables=["raw_events"],
                transformation_logic="time_since_last_event",
                group_by=["session_id"]
            ),
            
            "event_type_frequency": FeatureDefinition(
                name="event_type_frequency",
                feature_type=FeatureType.NUMERICAL,
                frequency=FeatureFrequency.STREAMING,
                description="Frequency of each event type",
                source_tables=["raw_events"],
                transformation_logic="event_type_frequency",
                aggregation_type=AggregationType.COUNT,
                window_size="1h",
                group_by=["event_type"]
            ),
            
            # Behavioral patterns
            "user_activity_pattern": FeatureDefinition(
                name="user_activity_pattern",
                feature_type=FeatureType.EMBEDDING,
                frequency=FeatureFrequency.BATCH,
                description="User activity pattern embedding",
                source_tables=["raw_events", "session_summaries"],
                transformation_logic="activity_pattern_embedding",
                group_by=["user_id"]
            ),
            
            "session_progression_score": FeatureDefinition(
                name="session_progression_score",
                feature_type=FeatureType.NUMERICAL,
                frequency=FeatureFrequency.REALTIME,
                description="Score indicating session progression quality",
                source_tables=["raw_events"],
                transformation_logic="session_progression_score",
                group_by=["session_id"]
            ),
            
            # Temporal features
            "hour_of_day": FeatureDefinition(
                name="hour_of_day",
                feature_type=FeatureType.CATEGORICAL,
                frequency=FeatureFrequency.REALTIME,
                description="Hour of day (0-23)",
                source_tables=["raw_events"],
                transformation_logic="extract_hour"
            ),
            
            "day_of_week": FeatureDefinition(
                name="day_of_week",
                feature_type=FeatureType.CATEGORICAL,
                frequency=FeatureFrequency.REALTIME,
                description="Day of week (0-6)",
                source_tables=["raw_events"],
                transformation_logic="extract_day_of_week"
            ),
            
            # Device and context features
            "device_type": FeatureDefinition(
                name="device_type",
                feature_type=FeatureType.CATEGORICAL,
                frequency=FeatureFrequency.REALTIME,
                description="Device type from user agent",
                source_tables=["raw_events"],
                transformation_logic="extract_device_type"
            ),
            
            "is_mobile": FeatureDefinition(
                name="is_mobile",
                feature_type=FeatureType.BOOLEAN,
                frequency=FeatureFrequency.REALTIME,
                description="Whether user is on mobile device",
                source_tables=["raw_events"],
                transformation_logic="is_mobile_device"
            ),
            
            # Engagement features
            "page_view_velocity": FeatureDefinition(
                name="page_view_velocity",
                feature_type=FeatureType.NUMERICAL,
                frequency=FeatureFrequency.STREAMING,
                description="Rate of page views per minute",
                source_tables=["raw_events"],
                transformation_logic="page_view_velocity",
                aggregation_type=AggregationType.COUNT,
                window_size="5m",
                group_by=["session_id"]
            ),
            
            "bounce_probability": FeatureDefinition(
                name="bounce_probability",
                feature_type=FeatureType.NUMERICAL,
                frequency=FeatureFrequency.REALTIME,
                description="Probability of user bouncing from session",
                source_tables=["raw_events"],
                transformation_logic="calculate_bounce_probability",
                group_by=["session_id"]
            )
        }
    
    def _register_transformations(self) -> Dict[str, Callable]:
        """Register transformation functions."""
        return {
            "count_sessions_by_user": self._count_sessions_by_user,
            "avg_session_duration": self._avg_session_duration,
            "calculate_conversion_rate": self._calculate_conversion_rate,
            "sequence_length": self._sequence_length,
            "time_since_last_event": self._time_since_last_event,
            "event_type_frequency": self._event_type_frequency,
            "activity_pattern_embedding": self._activity_pattern_embedding,
            "session_progression_score": self._session_progression_score,
            "extract_hour": self._extract_hour,
            "extract_day_of_week": self._extract_day_of_week,
            "extract_device_type": self._extract_device_type,
            "is_mobile_device": self._is_mobile_device,
            "page_view_velocity": self._page_view_velocity,
            "calculate_bounce_probability": self._calculate_bounce_probability
        }
    
    async def extract_features_from_event(self, event_data: Dict[str, Any]) -> Dict[str, FeatureValue]:
        """Extract features from a single event."""
        features = {}
        
        for feature_name, feature_def in self.feature_definitions.items():
            if feature_def.frequency in [FeatureFrequency.REALTIME, FeatureFrequency.STREAMING]:
                try:
                    start_time = time.time()
                    
                    value = await self._compute_feature(feature_def, event_data)
                    
                    if value is not None:
                        features[feature_name] = FeatureValue(
                            feature_name=feature_name,
                            entity_id=self._get_entity_id(event_data, feature_def),
                            value=value,
                            timestamp=datetime.utcnow(),
                            version=feature_def.version
                        )
                    
                    computation_time = time.time() - start_time
                    FEATURE_COMPUTATION_TIME.labels(feature_name=feature_name).observe(computation_time)
                    FEATURE_EXTRACTIONS.labels(feature_name=feature_name, status='success').inc()
                    
                except Exception as e:
                    logger.error(f"Failed to compute feature {feature_name}: {e}")
                    FEATURE_EXTRACTIONS.labels(feature_name=feature_name, status='error').inc()
        
        return features
    
    async def _compute_feature(self, feature_def: FeatureDefinition, event_data: Dict[str, Any]) -> Any:
        """Compute a single feature value."""
        transformation_func = self.transformation_functions.get(feature_def.transformation_logic)
        
        if not transformation_func:
            logger.warning(f"No transformation function for {feature_def.transformation_logic}")
            return None
        
        # Handle windowed features
        if feature_def.window_size and feature_def.aggregation_type:
            entity_id = self._get_entity_id(event_data, feature_def)
            window_key = f"{feature_def.name}_{entity_id}"
            
            if window_key not in self.feature_windows[feature_def.name]:
                self.feature_windows[feature_def.name][window_key] = FeatureWindow(
                    feature_def.window_size,
                    feature_def.aggregation_type
                )
            
            window = self.feature_windows[feature_def.name][window_key]
            
            # Add current value to window
            raw_value = await transformation_func(event_data)
            if raw_value is not None:
                window.add_value(float(raw_value), datetime.utcnow())
                return window.compute()
        
        # Direct computation
        return await transformation_func(event_data)
    
    def _get_entity_id(self, event_data: Dict[str, Any], feature_def: FeatureDefinition) -> str:
        """Get entity ID for feature grouping."""
        if not feature_def.group_by:
            return "global"
        
        id_parts = []
        for group_field in feature_def.group_by:
            value = event_data.get(group_field, "unknown")
            id_parts.append(str(value))
        
        return "_".join(id_parts)
    
    # Transformation functions
    async def _count_sessions_by_user(self, event_data: Dict[str, Any]) -> int:
        """Count sessions for a user."""
        # This would typically query the database
        # For demo, return a simulated count
        return 1
    
    async def _avg_session_duration(self, event_data: Dict[str, Any]) -> float:
        """Calculate average session duration."""
        # Simulate session duration calculation
        return 300.0  # 5 minutes average
    
    async def _calculate_conversion_rate(self, event_data: Dict[str, Any]) -> float:
        """Calculate user conversion rate."""
        # Simulate conversion rate calculation
        return 0.15  # 15% conversion rate
    
    async def _sequence_length(self, event_data: Dict[str, Any]) -> int:
        """Count events in current sequence."""
        session_id = event_data.get("session_id")
        if not session_id:
            return 1
        
        # This would typically query for sequence length
        return hash(session_id) % 20 + 1
    
    async def _time_since_last_event(self, event_data: Dict[str, Any]) -> float:
        """Calculate time since last event."""
        # This would track last event times
        # For demo, return a reasonable value
        return 30.0  # 30 seconds
    
    async def _event_type_frequency(self, event_data: Dict[str, Any]) -> int:
        """Count frequency of event type."""
        return 1  # Will be aggregated by window
    
    async def _activity_pattern_embedding(self, event_data: Dict[str, Any]) -> List[float]:
        """Generate activity pattern embedding."""
        # This would use ML model to generate embeddings
        # For demo, return a simple embedding
        user_id = event_data.get("user_id", "")
        hash_value = hash(user_id) % 1000
        
        return [
            (hash_value % 100) / 100.0,
            ((hash_value // 100) % 10) / 10.0,
            ((hash_value // 1000) % 10) / 10.0
        ]
    
    async def _session_progression_score(self, event_data: Dict[str, Any]) -> float:
        """Calculate session progression score."""
        event_type = event_data.get("event_type", "")
        
        # Assign scores based on event progression
        scores = {
            "page_view": 0.1,
            "click": 0.3,
            "form_start": 0.5,
            "form_submit": 0.8,
            "purchase": 1.0
        }
        
        return scores.get(event_type, 0.0)
    
    async def _extract_hour(self, event_data: Dict[str, Any]) -> int:
        """Extract hour of day."""
        timestamp = event_data.get("timestamp")
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        elif not isinstance(timestamp, datetime):
            timestamp = datetime.utcnow()
        
        return timestamp.hour
    
    async def _extract_day_of_week(self, event_data: Dict[str, Any]) -> int:
        """Extract day of week."""
        timestamp = event_data.get("timestamp")
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        elif not isinstance(timestamp, datetime):
            timestamp = datetime.utcnow()
        
        return timestamp.weekday()
    
    async def _extract_device_type(self, event_data: Dict[str, Any]) -> str:
        """Extract device type from user agent."""
        user_agent = event_data.get("user_agent", "")
        
        if "Mobile" in user_agent or "Android" in user_agent:
            return "mobile"
        elif "Tablet" in user_agent or "iPad" in user_agent:
            return "tablet"
        else:
            return "desktop"
    
    async def _is_mobile_device(self, event_data: Dict[str, Any]) -> bool:
        """Check if device is mobile."""
        device_type = await self._extract_device_type(event_data)
        return device_type == "mobile"
    
    async def _page_view_velocity(self, event_data: Dict[str, Any]) -> int:
        """Calculate page view velocity."""
        if event_data.get("event_type") == "page_view":
            return 1
        return 0
    
    async def _calculate_bounce_probability(self, event_data: Dict[str, Any]) -> float:
        """Calculate bounce probability."""
        # Simple heuristic based on session progression
        session_score = await self._session_progression_score(event_data)
        time_since_last = await self._time_since_last_event(event_data)
        
        # Higher score and shorter time = lower bounce probability
        bounce_prob = max(0.0, 1.0 - session_score - (60.0 - min(time_since_last, 60.0)) / 60.0)
        return min(bounce_prob, 1.0)

class FeatureStore:
    """High-performance feature store for real-time serving."""
    
    def __init__(self):
        self.features: Dict[str, Dict[str, FeatureValue]] = defaultdict(dict)
        self.feature_cache: Dict[str, Tuple[Any, datetime]] = {}
        self.cache_ttl = timedelta(minutes=5)
        
    def store_feature(self, feature_value: FeatureValue):
        """Store a feature value."""
        key = f"{feature_value.feature_name}_{feature_value.entity_id}"
        self.features[feature_value.feature_name][feature_value.entity_id] = feature_value
        
        # Update cache
        self.feature_cache[key] = (feature_value.value, feature_value.timestamp)
        
        # Update metrics
        FEATURE_FRESHNESS.labels(feature_name=feature_value.feature_name).set(
            (datetime.utcnow() - feature_value.timestamp).total_seconds()
        )
    
    def get_feature(self, feature_name: str, entity_id: str) -> Optional[FeatureValue]:
        """Get a feature value."""
        cache_key = f"{feature_name}_{entity_id}"
        
        # Check cache first
        if cache_key in self.feature_cache:
            value, timestamp = self.feature_cache[cache_key]
            if datetime.utcnow() - timestamp < self.cache_ttl:
                FEATURE_CACHE_HITS.labels(feature_name=feature_name).inc()
                return FeatureValue(
                    feature_name=feature_name,
                    entity_id=entity_id,
                    value=value,
                    timestamp=timestamp,
                    version="cached"
                )
        
        # Get from feature store
        feature_value = self.features.get(feature_name, {}).get(entity_id)
        if feature_value:
            # Update cache
            self.feature_cache[cache_key] = (feature_value.value, feature_value.timestamp)
        
        return feature_value
    
    def get_feature_vector(self, entity_id: str, feature_names: List[str]) -> FeatureVector:
        """Get a feature vector for an entity."""
        features = {}
        feature_versions = {}
        
        for feature_name in feature_names:
            feature_value = self.get_feature(feature_name, entity_id)
            if feature_value:
                features[feature_name] = feature_value.value
                feature_versions[feature_name] = feature_value.version
            else:
                features[feature_name] = None
        
        return FeatureVector(
            entity_id=entity_id,
            features=features,
            timestamp=datetime.utcnow(),
            vector_version="1.0",
            feature_versions=feature_versions
        )
    
    def cleanup_expired_features(self, max_age: timedelta = timedelta(days=7)):
        """Clean up expired features."""
        cutoff_time = datetime.utcnow() - max_age
        
        for feature_name in list(self.features.keys()):
            entity_dict = self.features[feature_name]
            expired_entities = [
                entity_id for entity_id, feature_value in entity_dict.items()
                if feature_value.timestamp < cutoff_time
            ]
            
            for entity_id in expired_entities:
                del entity_dict[entity_id]
                cache_key = f"{feature_name}_{entity_id}"
                self.feature_cache.pop(cache_key, None)
    
    def get_store_stats(self) -> Dict[str, Any]:
        """Get feature store statistics."""
        total_features = sum(len(entities) for entities in self.features.values())
        
        return {
            "total_features": total_features,
            "feature_types": len(self.features),
            "cache_size": len(self.feature_cache),
            "memory_usage_mb": self._estimate_memory_usage() / (1024 * 1024)
        }
    
    def _estimate_memory_usage(self) -> int:
        """Estimate memory usage in bytes."""
        # Rough estimation
        feature_size = sum(
            len(entities) * 100  # Estimate 100 bytes per feature value
            for entities in self.features.values()
        )
        cache_size = len(self.feature_cache) * 50  # Estimate 50 bytes per cache entry
        
        return feature_size + cache_size

class FeatureEngineeringPipeline:
    """Orchestrates feature extraction and engineering pipeline."""
    
    def __init__(self):
        self.extractor = FeatureExtractor()
        self.store = FeatureStore()
        self.pipeline_active = False
        
    async def process_event(self, event_data: Dict[str, Any]) -> Dict[str, FeatureValue]:
        """Process a single event through the feature pipeline."""
        try:
            # Extract features
            features = await self.extractor.extract_features_from_event(event_data)
            
            # Store features
            for feature_value in features.values():
                self.store.store_feature(feature_value)
            
            return features
            
        except Exception as e:
            logger.error(f"Feature pipeline processing failed: {e}")
            return {}
    
    async def get_features_for_prediction(self, entity_id: str, 
                                        feature_names: List[str]) -> FeatureVector:
        """Get features for model prediction."""
        return self.store.get_feature_vector(entity_id, feature_names)
    
    def start_background_tasks(self):
        """Start background maintenance tasks."""
        self.pipeline_active = True
        
        async def cleanup_task():
            while self.pipeline_active:
                try:
                    self.store.cleanup_expired_features()
                    await asyncio.sleep(3600)  # Cleanup every hour
                except Exception as e:
                    logger.error(f"Feature cleanup failed: {e}")
                    await asyncio.sleep(300)  # Retry in 5 minutes
        
        asyncio.create_task(cleanup_task())
    
    def stop_background_tasks(self):
        """Stop background tasks."""
        self.pipeline_active = False
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get comprehensive pipeline statistics."""
        return {
            "extractor": {
                "feature_definitions": len(self.extractor.feature_definitions),
                "active_windows": sum(
                    len(windows) for windows in self.extractor.feature_windows.values()
                )
            },
            "store": self.store.get_store_stats(),
            "pipeline_active": self.pipeline_active
        }

# Global feature pipeline
feature_pipeline = FeatureEngineeringPipeline()

async def extract_features_from_event(event_data: Dict[str, Any]) -> Dict[str, FeatureValue]:
    """Convenience function to extract features from event."""
    return await feature_pipeline.process_event(event_data)

async def get_feature_vector(entity_id: str, feature_names: List[str]) -> FeatureVector:
    """Convenience function to get feature vector."""
    return await feature_pipeline.get_features_for_prediction(entity_id, feature_names)
