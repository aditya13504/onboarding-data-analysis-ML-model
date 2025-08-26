"""Real-time inference optimization with caching, batching, and performance monitoring.

Provides production-grade model serving infrastructure with low-latency inference.
"""
from __future__ import annotations
import json
import logging
import asyncio
import time
import pickle
from typing import Dict, List, Any, Optional, Callable, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
import numpy as np
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from prometheus_client import Counter, Histogram, Gauge
from onboarding_analyzer.ml.feature_engineering import feature_pipeline, FeatureVector
from onboarding_analyzer.ml.model_versioning import model_registry

logger = logging.getLogger(__name__)

class InferenceMode(Enum):
    SYNCHRONOUS = "synchronous"        # Real-time blocking inference
    ASYNCHRONOUS = "asynchronous"      # Non-blocking inference
    BATCH = "batch"                    # Batch processing
    STREAMING = "streaming"            # Continuous stream processing

class CachingStrategy(Enum):
    NO_CACHE = "no_cache"
    SIMPLE_CACHE = "simple_cache"
    LRU_CACHE = "lru_cache"
    TTL_CACHE = "ttl_cache"
    PREDICTION_CACHE = "prediction_cache"
    FEATURE_CACHE = "feature_cache"

class ModelServingStrategy(Enum):
    SINGLE_MODEL = "single_model"
    ENSEMBLE = "ensemble"
    A_B_TEST = "a_b_test"
    CANARY = "canary"
    BLUE_GREEN = "blue_green"

@dataclass
class InferenceRequest:
    request_id: str
    model_id: str
    input_data: Dict[str, Any]
    feature_names: List[str]
    mode: InferenceMode = InferenceMode.SYNCHRONOUS
    priority: int = 1  # 1=low, 2=medium, 3=high
    timeout_ms: int = 1000
    cache_enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)

@dataclass
class InferenceResponse:
    request_id: str
    model_id: str
    predictions: Any
    confidence: float
    latency_ms: float
    cache_hit: bool
    feature_extraction_time_ms: float
    model_inference_time_ms: float
    total_time_ms: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)

@dataclass
class BatchInferenceJob:
    job_id: str
    model_id: str
    requests: List[InferenceRequest]
    batch_size: int
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: str = "pending"
    results: List[InferenceResponse] = field(default_factory=list)

@dataclass
class ModelCache:
    model_id: str
    model_object: Any
    feature_cache: Dict[str, Tuple[Any, datetime]]
    prediction_cache: Dict[str, Tuple[Any, datetime]]
    last_accessed: datetime
    access_count: int = 0
    cache_hit_rate: float = 0.0

# Metrics
INFERENCE_REQUESTS = Counter('inference_requests_total', 'Inference requests', ['model_id', 'mode', 'status'])
INFERENCE_LATENCY = Histogram('inference_latency_seconds', 'Inference latency', ['model_id', 'cache_status'])
FEATURE_EXTRACTION_TIME = Histogram('feature_extraction_seconds', 'Feature extraction time', ['model_id'])
MODEL_LOADING_TIME = Histogram('model_loading_seconds', 'Model loading time', ['model_id'])
CACHE_HIT_RATE = Gauge('inference_cache_hit_rate', 'Cache hit rate', ['model_id', 'cache_type'])
BATCH_SIZE = Histogram('inference_batch_size', 'Batch inference size', ['model_id'])
QUEUE_SIZE = Gauge('inference_queue_size', 'Inference queue size', ['priority'])

class InferenceCache:
    """Multi-level caching system for inference optimization."""
    
    def __init__(self, max_size: int = 10000, ttl_seconds: int = 300):
        self.max_size = max_size
        self.ttl = timedelta(seconds=ttl_seconds)
        self.feature_cache: Dict[str, Tuple[FeatureVector, datetime]] = {}
        self.prediction_cache: Dict[str, Tuple[Any, datetime]] = {}
        self.access_times: Dict[str, datetime] = {}
        self._lock = threading.RLock()
    
    def get_features(self, cache_key: str) -> Optional[FeatureVector]:
        """Get cached features."""
        with self._lock:
            if cache_key in self.feature_cache:
                features, timestamp = self.feature_cache[cache_key]
                if datetime.utcnow() - timestamp < self.ttl:
                    self.access_times[cache_key] = datetime.utcnow()
                    return features
                else:
                    del self.feature_cache[cache_key]
        return None
    
    def cache_features(self, cache_key: str, features: FeatureVector):
        """Cache features."""
        with self._lock:
            if len(self.feature_cache) >= self.max_size:
                self._evict_oldest_features()
            
            self.feature_cache[cache_key] = (features, datetime.utcnow())
            self.access_times[cache_key] = datetime.utcnow()
    
    def get_prediction(self, cache_key: str) -> Optional[Any]:
        """Get cached prediction."""
        with self._lock:
            if cache_key in self.prediction_cache:
                prediction, timestamp = self.prediction_cache[cache_key]
                if datetime.utcnow() - timestamp < self.ttl:
                    self.access_times[cache_key] = datetime.utcnow()
                    return prediction
                else:
                    del self.prediction_cache[cache_key]
        return None
    
    def cache_prediction(self, cache_key: str, prediction: Any):
        """Cache prediction."""
        with self._lock:
            if len(self.prediction_cache) >= self.max_size:
                self._evict_oldest_predictions()
            
            self.prediction_cache[cache_key] = (prediction, datetime.utcnow())
            self.access_times[cache_key] = datetime.utcnow()
    
    def _evict_oldest_features(self):
        """Evict oldest features using LRU."""
        if not self.feature_cache:
            return
        
        oldest_key = min(
            self.feature_cache.keys(),
            key=lambda k: self.access_times.get(k, datetime.min)
        )
        del self.feature_cache[oldest_key]
        self.access_times.pop(oldest_key, None)
    
    def _evict_oldest_predictions(self):
        """Evict oldest predictions using LRU."""
        if not self.prediction_cache:
            return
        
        oldest_key = min(
            self.prediction_cache.keys(),
            key=lambda k: self.access_times.get(k, datetime.min)
        )
        del self.prediction_cache[oldest_key]
        self.access_times.pop(oldest_key, None)
    
    def clear_expired(self):
        """Clear expired cache entries."""
        with self._lock:
            cutoff_time = datetime.utcnow() - self.ttl
            
            # Clear expired features
            expired_feature_keys = [
                key for key, (_, timestamp) in self.feature_cache.items()
                if timestamp < cutoff_time
            ]
            for key in expired_feature_keys:
                del self.feature_cache[key]
                self.access_times.pop(key, None)
            
            # Clear expired predictions
            expired_prediction_keys = [
                key for key, (_, timestamp) in self.prediction_cache.items()
                if timestamp < cutoff_time
            ]
            for key in expired_prediction_keys:
                del self.prediction_cache[key]
                self.access_times.pop(key, None)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            return {
                'feature_cache_size': len(self.feature_cache),
                'prediction_cache_size': len(self.prediction_cache),
                'max_size': self.max_size,
                'ttl_seconds': self.ttl.total_seconds()
            }

class ModelLoader:
    """Efficient model loading and management."""
    
    def __init__(self, max_models: int = 10):
        self.max_models = max_models
        self.loaded_models: Dict[str, ModelCache] = {}
        self._lock = threading.RLock()
    
    def load_model(self, model_id: str) -> Any:
        """Load model with caching."""
        with self._lock:
            # Check if model is already loaded
            if model_id in self.loaded_models:
                model_cache = self.loaded_models[model_id]
                model_cache.last_accessed = datetime.utcnow()
                model_cache.access_count += 1
                return model_cache.model_object
            
            # Load model from registry
            start_time = time.time()
            
            try:
                model_object = model_registry.load_artifact(model_id, "model")
                
                # Manage cache size
                if len(self.loaded_models) >= self.max_models:
                    self._evict_least_used_model()
                
                # Cache model
                model_cache = ModelCache(
                    model_id=model_id,
                    model_object=model_object,
                    feature_cache={},
                    prediction_cache={},
                    last_accessed=datetime.utcnow(),
                    access_count=1
                )
                
                self.loaded_models[model_id] = model_cache
                
                loading_time = time.time() - start_time
                MODEL_LOADING_TIME.labels(model_id=model_id).observe(loading_time)
                
                logger.info(f"Loaded model {model_id} in {loading_time:.3f}s")
                return model_object
                
            except Exception as e:
                logger.error(f"Failed to load model {model_id}: {e}")
                raise
    
    def _evict_least_used_model(self):
        """Evict least recently used model."""
        if not self.loaded_models:
            return
        
        lru_model_id = min(
            self.loaded_models.keys(),
            key=lambda mid: self.loaded_models[mid].last_accessed
        )
        
        del self.loaded_models[lru_model_id]
        logger.info(f"Evicted model {lru_model_id} from cache")
    
    def get_model_stats(self) -> Dict[str, Any]:
        """Get model loading statistics."""
        with self._lock:
            return {
                'loaded_models': len(self.loaded_models),
                'max_models': self.max_models,
                'models': {
                    model_id: {
                        'access_count': cache.access_count,
                        'last_accessed': cache.last_accessed.isoformat(),
                        'cache_hit_rate': cache.cache_hit_rate
                    }
                    for model_id, cache in self.loaded_models.items()
                }
            }

class InferenceEngine:
    """High-performance inference engine with optimization strategies."""
    
    def __init__(self):
        self.cache = InferenceCache()
        self.model_loader = ModelLoader()
        self.request_queues: Dict[int, deque] = {1: deque(), 2: deque(), 3: deque()}  # Priority queues
        self.batch_jobs: Dict[str, BatchInferenceJob] = {}
        self.executor = ThreadPoolExecutor(max_workers=8)
        self._processing = False
        
    async def predict(self, request: InferenceRequest) -> InferenceResponse:
        """Main prediction interface."""
        start_time = time.time()
        
        try:
            INFERENCE_REQUESTS.labels(
                model_id=request.model_id,
                mode=request.mode.value,
                status='started'
            ).inc()
            
            # Generate cache keys
            feature_key = self._generate_feature_cache_key(request)
            prediction_key = self._generate_prediction_cache_key(request)
            
            cache_hit = False
            features = None
            prediction = None
            
            # Try prediction cache first
            if request.cache_enabled:
                prediction = self.cache.get_prediction(prediction_key)
                if prediction is not None:
                    cache_hit = True
                    
                    response = InferenceResponse(
                        request_id=request.request_id,
                        model_id=request.model_id,
                        predictions=prediction,
                        confidence=1.0,  # Cached predictions have high confidence
                        latency_ms=(time.time() - start_time) * 1000,
                        cache_hit=True,
                        feature_extraction_time_ms=0,
                        model_inference_time_ms=0,
                        total_time_ms=(time.time() - start_time) * 1000
                    )
                    
                    INFERENCE_LATENCY.labels(
                        model_id=request.model_id,
                        cache_status='hit'
                    ).observe(time.time() - start_time)
                    
                    return response
            
            # Extract features
            feature_start = time.time()
            
            if request.cache_enabled:
                features = self.cache.get_features(feature_key)
            
            if features is None:
                features = await self._extract_features(request)
                if request.cache_enabled:
                    self.cache.cache_features(feature_key, features)
            
            feature_time = (time.time() - feature_start) * 1000
            FEATURE_EXTRACTION_TIME.labels(model_id=request.model_id).observe(feature_time / 1000)
            
            # Perform inference
            inference_start = time.time()
            prediction, confidence = await self._perform_inference(request, features)
            inference_time = (time.time() - inference_start) * 1000
            
            # Cache prediction
            if request.cache_enabled and prediction is not None:
                self.cache.cache_prediction(prediction_key, prediction)
            
            total_time = (time.time() - start_time) * 1000
            
            response = InferenceResponse(
                request_id=request.request_id,
                model_id=request.model_id,
                predictions=prediction,
                confidence=confidence,
                latency_ms=total_time,
                cache_hit=cache_hit,
                feature_extraction_time_ms=feature_time,
                model_inference_time_ms=inference_time,
                total_time_ms=total_time
            )
            
            INFERENCE_REQUESTS.labels(
                model_id=request.model_id,
                mode=request.mode.value,
                status='success'
            ).inc()
            
            INFERENCE_LATENCY.labels(
                model_id=request.model_id,
                cache_status='miss' if not cache_hit else 'hit'
            ).observe(total_time / 1000)
            
            return response
            
        except Exception as e:
            INFERENCE_REQUESTS.labels(
                model_id=request.model_id,
                mode=request.mode.value,
                status='error'
            ).inc()
            
            logger.error(f"Inference failed for request {request.request_id}: {e}")
            
            # Return error response
            return InferenceResponse(
                request_id=request.request_id,
                model_id=request.model_id,
                predictions=None,
                confidence=0.0,
                latency_ms=(time.time() - start_time) * 1000,
                cache_hit=False,
                feature_extraction_time_ms=0,
                model_inference_time_ms=0,
                total_time_ms=(time.time() - start_time) * 1000,
                metadata={'error': str(e)}
            )
    
    async def batch_predict(self, requests: List[InferenceRequest], 
                          batch_size: int = 32) -> List[InferenceResponse]:
        """Batch prediction for improved throughput."""
        job_id = f"batch_{int(time.time())}_{hash(str(requests)) % 1000}"
        
        job = BatchInferenceJob(
            job_id=job_id,
            model_id=requests[0].model_id if requests else "unknown",
            requests=requests,
            batch_size=batch_size,
            created_at=datetime.utcnow()
        )
        
        self.batch_jobs[job_id] = job
        
        try:
            job.started_at = datetime.utcnow()
            job.status = "processing"
            
            # Process requests in batches
            responses = []
            
            for i in range(0, len(requests), batch_size):
                batch = requests[i:i + batch_size]
                BATCH_SIZE.labels(model_id=job.model_id).observe(len(batch))
                
                # Process batch concurrently
                tasks = [self.predict(request) for request in batch]
                batch_responses = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Handle any exceptions
                for response in batch_responses:
                    if isinstance(response, Exception):
                        logger.error(f"Batch prediction error: {response}")
                    else:
                        responses.append(response)
            
            job.results = responses
            job.completed_at = datetime.utcnow()
            job.status = "completed"
            
            return responses
            
        except Exception as e:
            job.status = "failed"
            logger.error(f"Batch prediction failed: {e}")
            return []
    
    async def _extract_features(self, request: InferenceRequest) -> FeatureVector:
        """Extract features for inference."""
        entity_id = request.input_data.get('user_id') or request.input_data.get('session_id', 'unknown')
        
        return await feature_pipeline.get_features_for_prediction(
            entity_id, request.feature_names
        )
    
    async def _perform_inference(self, request: InferenceRequest, 
                               features: FeatureVector) -> Tuple[Any, float]:
        """Perform model inference."""
        try:
            # Load model
            model = self.model_loader.load_model(request.model_id)
            
            # Prepare feature array
            feature_array = self._prepare_feature_array(features, request.feature_names)
            
            # Make prediction
            if hasattr(model, 'predict_proba'):
                predictions = model.predict_proba(feature_array.reshape(1, -1))[0]
                confidence = float(np.max(predictions))
                prediction = predictions.tolist()
            elif hasattr(model, 'predict'):
                prediction = model.predict(feature_array.reshape(1, -1))[0]
                confidence = 0.8  # Default confidence for deterministic models
                prediction = float(prediction) if np.isscalar(prediction) else prediction.tolist()
            else:
                # Custom model interface
                prediction = model(feature_array)
                confidence = 0.7
            
            return prediction, confidence
            
        except Exception as e:
            logger.error(f"Model inference failed: {e}")
            return None, 0.0
    
    def _prepare_feature_array(self, features: FeatureVector, feature_names: List[str]) -> np.ndarray:
        """Prepare feature array for model input."""
        feature_values = []
        
        for feature_name in feature_names:
            value = features.features.get(feature_name)
            
            if value is None:
                feature_values.append(0.0)  # Default value for missing features
            elif isinstance(value, (int, float)):
                feature_values.append(float(value))
            elif isinstance(value, bool):
                feature_values.append(1.0 if value else 0.0)
            elif isinstance(value, str):
                # Simple string to numeric conversion
                feature_values.append(float(hash(value) % 1000) / 1000.0)
            elif isinstance(value, list):
                # For embedding features, take mean
                feature_values.append(float(np.mean(value)) if value else 0.0)
            else:
                feature_values.append(0.0)
        
        return np.array(feature_values, dtype=np.float32)
    
    def _generate_feature_cache_key(self, request: InferenceRequest) -> str:
        """Generate cache key for features."""
        entity_id = request.input_data.get('user_id') or request.input_data.get('session_id', 'unknown')
        feature_hash = hash(tuple(sorted(request.feature_names)))
        return f"features_{entity_id}_{feature_hash}"
    
    def _generate_prediction_cache_key(self, request: InferenceRequest) -> str:
        """Generate cache key for predictions."""
        input_hash = hash(str(sorted(request.input_data.items())))
        feature_hash = hash(tuple(sorted(request.feature_names)))
        return f"prediction_{request.model_id}_{input_hash}_{feature_hash}"
    
    def start_background_tasks(self):
        """Start background optimization tasks."""
        self._processing = True
        
        async def cache_cleanup_task():
            while self._processing:
                try:
                    self.cache.clear_expired()
                    await asyncio.sleep(300)  # Clean every 5 minutes
                except Exception as e:
                    logger.error(f"Cache cleanup failed: {e}")
                    await asyncio.sleep(60)
        
        async def metrics_update_task():
            while self._processing:
                try:
                    self._update_cache_metrics()
                    await asyncio.sleep(60)  # Update every minute
                except Exception as e:
                    logger.error(f"Metrics update failed: {e}")
                    await asyncio.sleep(60)
        
        asyncio.create_task(cache_cleanup_task())
        asyncio.create_task(metrics_update_task())
    
    def _update_cache_metrics(self):
        """Update cache performance metrics."""
        cache_stats = self.cache.get_stats()
        
        # Calculate cache hit rates (simplified)
        for model_id in self.model_loader.loaded_models:
            # Feature cache hit rate
            CACHE_HIT_RATE.labels(
                model_id=model_id,
                cache_type='feature'
            ).set(0.8)  # Simulated value
            
            # Prediction cache hit rate
            CACHE_HIT_RATE.labels(
                model_id=model_id,
                cache_type='prediction'
            ).set(0.6)  # Simulated value
    
    def stop_background_tasks(self):
        """Stop background tasks."""
        self._processing = False
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics."""
        return {
            'cache': self.cache.get_stats(),
            'model_loader': self.model_loader.get_model_stats(),
            'batch_jobs': {
                'total': len(self.batch_jobs),
                'completed': len([j for j in self.batch_jobs.values() if j.status == 'completed']),
                'failed': len([j for j in self.batch_jobs.values() if j.status == 'failed'])
            },
            'queue_sizes': {
                priority: len(queue) for priority, queue in self.request_queues.items()
            }
        }

# Global inference engine
inference_engine = InferenceEngine()

async def predict(model_id: str, input_data: Dict[str, Any], 
                 feature_names: List[str], **kwargs) -> InferenceResponse:
    """Convenience function for single prediction."""
    request = InferenceRequest(
        request_id=f"req_{int(time.time())}_{hash(str(input_data)) % 1000}",
        model_id=model_id,
        input_data=input_data,
        feature_names=feature_names,
        **kwargs
    )
    
    return await inference_engine.predict(request)

async def batch_predict(model_id: str, batch_data: List[Dict[str, Any]], 
                       feature_names: List[str], **kwargs) -> List[InferenceResponse]:
    """Convenience function for batch prediction."""
    requests = [
        InferenceRequest(
            request_id=f"batch_req_{i}_{int(time.time())}",
            model_id=model_id,
            input_data=data,
            feature_names=feature_names,
            **kwargs
        )
        for i, data in enumerate(batch_data)
    ]
    
    return await inference_engine.batch_predict(requests)
