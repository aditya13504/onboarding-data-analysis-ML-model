"""High-volume batching and DLQ observability tasks.

Provides scheduled tasks for batch processing optimization and DLQ management.
"""
from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timedelta
from onboarding_analyzer.infrastructure.celery_app import app
from onboarding_analyzer.infrastructure.high_volume_batching import batch_processor, shutdown_batch_processor
from onboarding_analyzer.infrastructure.dlq_observability import dlq_observability, dlq_recovery
from onboarding_analyzer.infrastructure.late_event_handling import reconciliation_engine
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import (
    BatchProcessingConfig, 
    BatchProcessingMetrics, 
    DLQErrorPattern,
    DLQRecoveryAttempt
)

logger = logging.getLogger(__name__)

@app.task(name="optimize_batch_processing")
def optimize_batch_processing():
    """Optimize batch processing configuration based on recent performance."""
    session = SessionLocal()
    try:
        # Analyze recent batch performance
        cutoff_time = datetime.utcnow() - timedelta(hours=1)
        
        recent_metrics = session.query(BatchProcessingMetrics).filter(
            BatchProcessingMetrics.created_at >= cutoff_time
        ).all()
        
        if not recent_metrics:
            logger.info("No recent batch metrics found for optimization")
            return {"status": "no_data"}
        
        # Group by pipeline
        pipeline_stats = {}
        for metric in recent_metrics:
            pipeline = metric.pipeline_name
            if pipeline not in pipeline_stats:
                pipeline_stats[pipeline] = {
                    'throughputs': [],
                    'processing_times': [],
                    'batch_sizes': []
                }
            
            pipeline_stats[pipeline]['throughputs'].append(metric.throughput_eps)
            pipeline_stats[pipeline]['processing_times'].append(metric.processing_time_ms)
            pipeline_stats[pipeline]['batch_sizes'].append(metric.batch_size)
        
        optimizations = 0
        
        # Optimize each pipeline
        for pipeline_name, stats in pipeline_stats.items():
            config = session.query(BatchProcessingConfig).filter_by(
                pipeline_name=pipeline_name
            ).first()
            
            if not config:
                continue
            
            avg_throughput = sum(stats['throughputs']) / len(stats['throughputs'])
            avg_processing_time = sum(stats['processing_times']) / len(stats['processing_times'])
            avg_batch_size = sum(stats['batch_sizes']) / len(stats['batch_sizes'])
            
            # Optimization logic
            if avg_processing_time < 100 and avg_throughput > 1000:  # Fast processing
                # Can increase batch size
                new_batch_size = min(config.max_batch_size + 200, 5000)
                if new_batch_size != config.max_batch_size:
                    config.max_batch_size = new_batch_size
                    config.updated_at = datetime.utcnow()
                    optimizations += 1
                    logger.info(f"Increased batch size for {pipeline_name} to {new_batch_size}")
            
            elif avg_processing_time > 1000 or avg_throughput < 100:  # Slow processing
                # Decrease batch size
                new_batch_size = max(config.max_batch_size - 200, 100)
                if new_batch_size != config.max_batch_size:
                    config.max_batch_size = new_batch_size
                    config.updated_at = datetime.utcnow()
                    optimizations += 1
                    logger.info(f"Decreased batch size for {pipeline_name} to {new_batch_size}")
        
        session.commit()
        
        return {
            "status": "completed",
            "pipelines_analyzed": len(pipeline_stats),
            "optimizations_applied": optimizations
        }
        
    except Exception as e:
        session.rollback()
        logger.error(f"Batch optimization failed: {e}")
        return {"status": "error", "error": str(e)}
    finally:
        session.close()

@app.task(name="analyze_dlq_patterns")
def analyze_dlq_patterns():
    """Analyze DLQ error patterns and update pattern tracking."""
    try:
        # Get recent DLQ analysis
        analysis = dlq_observability.analyze_dlq_health(hours_back=24)
        
        session = SessionLocal()
        try:
            # Update error patterns
            patterns_updated = 0
            
            for pattern in analysis.top_error_patterns:
                existing_pattern = session.query(DLQErrorPattern).filter_by(
                    error_signature=pattern.error_signature
                ).first()
                
                if existing_pattern:
                    # Update existing pattern
                    existing_pattern.occurrence_count += 1
                    existing_pattern.last_seen = datetime.utcnow()
                    existing_pattern.recovery_strategy = pattern.recovery_strategy.value
                    existing_pattern.auto_recoverable = pattern.auto_recoverable
                else:
                    # Create new pattern
                    new_pattern = DLQErrorPattern(
                        error_signature=pattern.error_signature,
                        error_category=pattern.category.value,
                        event_pattern=pattern.event_pattern,
                        occurrence_count=1,
                        recovery_strategy=pattern.recovery_strategy.value,
                        auto_recoverable=pattern.auto_recoverable,
                        first_seen=datetime.utcnow(),
                        last_seen=datetime.utcnow()
                    )
                    session.add(new_pattern)
                
                patterns_updated += 1
            
            session.commit()
            
            return {
                "status": "completed",
                "total_events": analysis.total_events,
                "error_categories": {k.value: v for k, v in analysis.error_categories.items()},
                "patterns_updated": patterns_updated,
                "recovery_candidates": len(analysis.recovery_candidates)
            }
            
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"DLQ pattern analysis failed: {e}")
        return {"status": "error", "error": str(e)}

@app.task(name="auto_recover_dlq_events")
def auto_recover_dlq_events():
    """Automatically recover DLQ events that are likely to succeed."""
    try:
        # Attempt auto-recovery
        results = dlq_recovery.auto_recover_events(max_events=100)
        
        # Log recovery attempts
        session = SessionLocal()
        try:
            # This is simplified - in practice you'd log individual attempts
            if results['attempted'] > 0:
                logger.info(f"DLQ auto-recovery: {results['successful']}/{results['attempted']} successful")
            
            session.commit()
            
        finally:
            session.close()
        
        return {
            "status": "completed",
            **results
        }
        
    except Exception as e:
        logger.error(f"DLQ auto-recovery failed: {e}")
        return {"status": "error", "error": str(e)}

@app.task(name="run_late_event_reconciliation")
def run_late_event_reconciliation():
    """Run reconciliation for late/out-of-order events."""
    try:
        results = reconciliation_engine.run_reconciliation()
        
        logger.info(f"Late event reconciliation: {results}")
        
        return {
            "status": "completed",
            **results
        }
        
    except Exception as e:
        logger.error(f"Late event reconciliation failed: {e}")
        return {"status": "error", "error": str(e)}

@app.task(name="cleanup_old_batch_metrics")
def cleanup_old_batch_metrics():
    """Clean up old batch processing metrics to prevent unbounded growth."""
    session = SessionLocal()
    try:
        # Keep only last 7 days of metrics
        cutoff_time = datetime.utcnow() - timedelta(days=7)
        
        deleted_count = session.query(BatchProcessingMetrics).filter(
            BatchProcessingMetrics.created_at < cutoff_time
        ).delete(synchronize_session=False)
        
        session.commit()
        
        logger.info(f"Cleaned up {deleted_count} old batch metrics")
        
        return {
            "status": "completed",
            "deleted_count": deleted_count
        }
        
    except Exception as e:
        session.rollback()
        logger.error(f"Batch metrics cleanup failed: {e}")
        return {"status": "error", "error": str(e)}
    finally:
        session.close()

@app.task(name="cleanup_old_dlq_recovery_attempts")
def cleanup_old_dlq_recovery_attempts():
    """Clean up old DLQ recovery attempts."""
    session = SessionLocal()
    try:
        # Keep only last 30 days of recovery attempts
        cutoff_time = datetime.utcnow() - timedelta(days=30)
        
        deleted_count = session.query(DLQRecoveryAttempt).filter(
            DLQRecoveryAttempt.attempted_at < cutoff_time
        ).delete(synchronize_session=False)
        
        session.commit()
        
        logger.info(f"Cleaned up {deleted_count} old DLQ recovery attempts")
        
        return {
            "status": "completed",
            "deleted_count": deleted_count
        }
        
    except Exception as e:
        session.rollback()
        logger.error(f"DLQ recovery cleanup failed: {e}")
        return {"status": "error", "error": str(e)}
    finally:
        session.close()
