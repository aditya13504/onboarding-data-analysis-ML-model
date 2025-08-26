"""High-volume event batching with intelligent queueing and throughput optimization.

Provides production-grade batching for high-throughput event ingestion scenarios.
"""
from __future__ import annotations
import asyncio
import logging
import time
from typing import List, Dict, Any, Optional, Callable, Iterator
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from prometheus_client import Counter, Histogram, Gauge, Summary
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import RawEvent

logger = logging.getLogger(__name__)

class BatchStrategy(Enum):
    SIZE_BASED = "size_based"           # Batch when size threshold reached
    TIME_BASED = "time_based"           # Batch when time window expires
    HYBRID = "hybrid"                   # Batch on size OR time threshold
    ADAPTIVE = "adaptive"               # Dynamically adjust based on load

@dataclass
class BatchConfig:
    max_batch_size: int = 1000          # Maximum events per batch
    max_wait_time: timedelta = field(default_factory=lambda: timedelta(seconds=5))  # Maximum wait time
    min_batch_size: int = 100           # Minimum batch size for efficiency
    adaptive_threshold: float = 0.8     # CPU/memory threshold for adaptive batching
    compression_enabled: bool = True    # Enable batch compression
    parallel_writers: int = 4           # Number of parallel batch writers

@dataclass
class BatchMetrics:
    events_processed: int
    batch_size: int
    processing_time: float
    compression_ratio: float
    throughput_eps: float  # Events per second

# Metrics
BATCH_EVENTS_TOTAL = Counter('batch_events_processed_total', 'Total events processed in batches', ['strategy'])
BATCH_SIZE_HISTOGRAM = Histogram('batch_size_events', 'Batch size distribution', buckets=(10, 50, 100, 500, 1000, 2000, 5000))
BATCH_PROCESSING_TIME = Histogram('batch_processing_seconds', 'Batch processing time', ['stage'])
BATCH_THROUGHPUT = Gauge('batch_throughput_eps', 'Current batch throughput (events/sec)')
BATCH_QUEUE_SIZE = Gauge('batch_queue_size', 'Current batch queue size')
BATCH_COMPRESSION_RATIO = Histogram('batch_compression_ratio', 'Batch compression ratio')

class BatchBuffer:
    """Thread-safe buffer for accumulating events before batching."""
    
    def __init__(self, config: BatchConfig):
        self.config = config
        self._buffer: List[RawEvent] = []
        self._lock = asyncio.Lock()
        self._last_flush = time.time()
        self._total_size_bytes = 0
        
    async def add_event(self, event: RawEvent) -> bool:
        """Add event to buffer. Returns True if batch should be triggered."""
        async with self._lock:
            self._buffer.append(event)
            
            # Estimate event size for memory tracking
            event_size = len(str(event.properties)) + 100  # Rough estimate
            self._total_size_bytes += event_size
            
            BATCH_QUEUE_SIZE.set(len(self._buffer))
            
            # Check if batch should be triggered
            return self._should_trigger_batch()
    
    def _should_trigger_batch(self) -> bool:
        """Determine if batch should be triggered based on strategy."""
        current_time = time.time()
        time_elapsed = current_time - self._last_flush
        
        # Size-based trigger
        if len(self._buffer) >= self.config.max_batch_size:
            return True
        
        # Time-based trigger
        if time_elapsed >= self.config.max_wait_time.total_seconds():
            if len(self._buffer) >= self.config.min_batch_size:
                return True
        
        # Memory pressure trigger (rough heuristic)
        if self._total_size_bytes > 10 * 1024 * 1024:  # 10MB
            return True
        
        return False
    
    async def flush_batch(self) -> List[RawEvent]:
        """Flush current buffer and return events for processing."""
        async with self._lock:
            if not self._buffer:
                return []
            
            batch = self._buffer.copy()
            self._buffer.clear()
            self._total_size_bytes = 0
            self._last_flush = time.time()
            
            BATCH_QUEUE_SIZE.set(0)
            BATCH_SIZE_HISTOGRAM.observe(len(batch))
            
            return batch
    
    async def force_flush(self) -> List[RawEvent]:
        """Force flush regardless of batch size (for shutdown)."""
        return await self.flush_batch()

class BatchProcessor:
    """High-performance batch processor with adaptive strategies."""
    
    def __init__(self, config: BatchConfig):
        self.config = config
        self._buffer = BatchBuffer(config)
        self._processing_stats = []
        self._adaptive_config = config
        self._last_adaptation = time.time()
        
    async def process_event(self, event: RawEvent) -> bool:
        """Process single event through batching system."""
        should_trigger = await self._buffer.add_event(event)
        
        if should_trigger:
            await self._process_batch()
        
        return True
    
    async def _process_batch(self):
        """Process accumulated batch of events."""
        start_time = time.time()
        
        try:
            batch = await self._buffer.flush_batch()
            if not batch:
                return
            
            # Record batch metrics
            BATCH_EVENTS_TOTAL.labels(strategy=self.config.__class__.__name__).inc(len(batch))
            
            # Process batch with compression if enabled
            if self.config.compression_enabled:
                compressed_batch = await self._compress_batch(batch)
                compression_ratio = len(compressed_batch) / len(batch) if batch else 1.0
                BATCH_COMPRESSION_RATIO.observe(compression_ratio)
            else:
                compressed_batch = batch
                compression_ratio = 1.0
            
            # Persist batch to database
            await self._persist_batch(compressed_batch)
            
            # Record processing metrics
            processing_time = time.time() - start_time
            throughput = len(batch) / processing_time if processing_time > 0 else 0
            
            BATCH_PROCESSING_TIME.labels(stage='total').observe(processing_time)
            BATCH_THROUGHPUT.set(throughput)
            
            # Store metrics for adaptive adjustment
            metrics = BatchMetrics(
                events_processed=len(batch),
                batch_size=len(batch),
                processing_time=processing_time,
                compression_ratio=compression_ratio,
                throughput_eps=throughput
            )
            self._processing_stats.append(metrics)
            
            # Trim stats history
            if len(self._processing_stats) > 100:
                self._processing_stats = self._processing_stats[-50:]
            
            # Adaptive tuning
            await self._adapt_batch_config()
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            raise
    
    async def _compress_batch(self, batch: List[RawEvent]) -> List[RawEvent]:
        """Apply compression techniques to batch."""
        # Simplified compression - in practice you'd use proper algorithms
        
        # Deduplicate events by event_id
        seen_ids = set()
        compressed = []
        
        for event in batch:
            if event.event_id not in seen_ids:
                compressed.append(event)
                seen_ids.add(event.event_id)
        
        # Sort by timestamp for better compression in downstream systems
        compressed.sort(key=lambda e: e.timestamp)
        
        return compressed
    
    async def _persist_batch(self, batch: List[RawEvent]):
        """Persist batch to database with optimized bulk insert."""
        if not batch:
            return
        
        session = SessionLocal()
        start_time = time.time()
        
        try:
            # Use bulk insert for better performance
            session.bulk_save_objects(batch)
            session.commit()
            
            persist_time = time.time() - start_time
            BATCH_PROCESSING_TIME.labels(stage='persist').observe(persist_time)
            
            logger.debug(f"Persisted batch of {len(batch)} events in {persist_time:.3f}s")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to persist batch: {e}")
            raise
        finally:
            session.close()
    
    async def _adapt_batch_config(self):
        """Dynamically adapt batch configuration based on performance."""
        current_time = time.time()
        
        # Only adapt every 30 seconds
        if current_time - self._last_adaptation < 30:
            return
        
        if len(self._processing_stats) < 10:
            return  # Need more samples
        
        recent_stats = self._processing_stats[-10:]
        avg_throughput = sum(s.throughput_eps for s in recent_stats) / len(recent_stats)
        avg_processing_time = sum(s.processing_time for s in recent_stats) / len(recent_stats)
        
        # Adapt batch size based on performance
        if avg_processing_time < 0.1 and avg_throughput > 1000:
            # System is handling load well, can increase batch size
            new_size = min(self._adaptive_config.max_batch_size + 200, 5000)
            self._adaptive_config.max_batch_size = new_size
            logger.info(f"Increased batch size to {new_size} (high throughput)")
            
        elif avg_processing_time > 1.0 or avg_throughput < 100:
            # System is struggling, decrease batch size
            new_size = max(self._adaptive_config.max_batch_size - 200, 100)
            self._adaptive_config.max_batch_size = new_size
            logger.info(f"Decreased batch size to {new_size} (performance issues)")
        
        # Adapt wait time based on queue pressure
        current_queue_size = len(self._buffer._buffer)
        if current_queue_size > self._adaptive_config.max_batch_size * 2:
            # High queue pressure, reduce wait time
            new_wait = max(self._adaptive_config.max_wait_time.total_seconds() - 1, 1)
            self._adaptive_config.max_wait_time = timedelta(seconds=new_wait)
        elif current_queue_size < self._adaptive_config.max_batch_size * 0.1:
            # Low queue pressure, can increase wait time for efficiency
            new_wait = min(self._adaptive_config.max_wait_time.total_seconds() + 1, 10)
            self._adaptive_config.max_wait_time = timedelta(seconds=new_wait)
        
        self._last_adaptation = current_time
    
    async def shutdown(self):
        """Graceful shutdown - flush remaining events."""
        logger.info("Shutting down batch processor...")
        
        # Force flush any remaining events
        remaining_batch = await self._buffer.force_flush()
        if remaining_batch:
            await self._persist_batch(remaining_batch)
            logger.info(f"Flushed {len(remaining_batch)} remaining events during shutdown")

class ParallelBatchProcessor:
    """Parallel batch processing with multiple workers."""
    
    def __init__(self, config: BatchConfig):
        self.config = config
        self._processors = [BatchProcessor(config) for _ in range(config.parallel_writers)]
        self._current_processor = 0
        
    async def process_event(self, event: RawEvent) -> bool:
        """Distribute events across parallel processors using round-robin."""
        processor = self._processors[self._current_processor]
        self._current_processor = (self._current_processor + 1) % len(self._processors)
        
        return await processor.process_event(event)
    
    async def shutdown(self):
        """Shutdown all parallel processors."""
        shutdown_tasks = [processor.shutdown() for processor in self._processors]
        await asyncio.gather(*shutdown_tasks)

# Global batch processor
_batch_config = BatchConfig(
    max_batch_size=1000,
    max_wait_time=timedelta(seconds=5),
    min_batch_size=100,
    adaptive_threshold=0.8,
    compression_enabled=True,
    parallel_writers=4
)

batch_processor = ParallelBatchProcessor(_batch_config)

async def process_event_batch(event: RawEvent) -> bool:
    """Convenience function to process events through batching system."""
    return await batch_processor.process_event(event)

async def shutdown_batch_processor():
    """Shutdown batch processor gracefully."""
    await batch_processor.shutdown()
