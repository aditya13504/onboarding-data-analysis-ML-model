"""Enhanced idempotency system for all major operations beyond raw events.

Provides idempotency keys for recommendations, anomalies, insights, and other operations
to prevent duplicate processing and ensure exactly-once semantics.
"""
from __future__ import annotations
import hashlib
import json
from datetime import datetime, timedelta
from typing import Any, Optional, TypeVar, Callable
from sqlalchemy.orm import Session
from sqlalchemy import Column, String, DateTime, Text, Integer
from onboarding_analyzer.models.tables import Base
from onboarding_analyzer.infrastructure.db import SessionLocal
from prometheus_client import Counter

T = TypeVar('T')

# Metrics
IDEMPOTENCY_HITS = Counter('idempotency_cache_hits_total', 'Idempotency cache hits', ['operation_type'])
IDEMPOTENCY_MISSES = Counter('idempotency_cache_misses_total', 'Idempotency cache misses', ['operation_type'])
IDEMPOTENCY_CONFLICTS = Counter('idempotency_conflicts_total', 'Idempotency key conflicts', ['operation_type'])

class IdempotencyRecord(Base):
    """Stores idempotency keys and results for operations."""
    __tablename__ = "idempotency_records"
    
    id = Column(Integer, primary_key=True)
    operation_type = Column(String(64), nullable=False, index=True)
    idempotency_key = Column(String(128), nullable=False, index=True)
    request_hash = Column(String(64), nullable=False)  # Hash of request parameters
    result_data = Column(Text, nullable=True)  # JSON serialized result
    status = Column(String(16), nullable=False, default='completed')  # completed|failed|processing
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False, index=True)
    
    __table_args__ = (
        {'mysql_engine': 'InnoDB'},
    )

def generate_idempotency_key(operation_type: str, **params) -> str:
    """Generate idempotency key from operation type and parameters."""
    # Sort params for consistent hashing
    sorted_params = json.dumps(params, sort_keys=True, default=str)
    content = f"{operation_type}:{sorted_params}"
    return hashlib.sha256(content.encode()).hexdigest()[:32]

def compute_request_hash(**params) -> str:
    """Compute hash of request parameters for conflict detection."""
    sorted_params = json.dumps(params, sort_keys=True, default=str)
    return hashlib.sha256(sorted_params.encode()).hexdigest()

class IdempotentOperation:
    def __init__(self, operation_type: str, ttl_hours: int = 24):
        self.operation_type = operation_type
        self.ttl_hours = ttl_hours
    
    def execute(self, 
                idempotency_key: Optional[str],
                operation_func: Callable[..., T],
                **params) -> T:
        """Execute operation with idempotency protection."""
        
        if not idempotency_key:
            # Generate key if not provided
            idempotency_key = generate_idempotency_key(self.operation_type, **params)
        
        request_hash = compute_request_hash(**params)
        session = SessionLocal()
        
        try:
            # Check for existing record
            existing = session.query(IdempotencyRecord).filter_by(
                operation_type=self.operation_type,
                idempotency_key=idempotency_key
            ).first()
            
            if existing:
                if existing.expires_at < datetime.utcnow():
                    # Expired - delete and proceed
                    session.delete(existing)
                    session.commit()
                elif existing.request_hash != request_hash:
                    # Key conflict - different parameters for same key
                    IDEMPOTENCY_CONFLICTS.labels(operation_type=self.operation_type).inc()
                    raise IdempotencyConflictError(
                        f"Idempotency key {idempotency_key} used with different parameters"
                    )
                else:
                    # Valid cached result
                    IDEMPOTENCY_HITS.labels(operation_type=self.operation_type).inc()
                    if existing.status == 'completed' and existing.result_data:
                        return json.loads(existing.result_data)
                    elif existing.status == 'failed':
                        raise IdempotencyError("Previous operation failed")
                    else:  # processing
                        raise IdempotencyError("Operation still in progress")
            
            # No existing record - execute operation
            IDEMPOTENCY_MISSES.labels(operation_type=self.operation_type).inc()
            
            # Create processing record
            record = IdempotencyRecord(
                operation_type=self.operation_type,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                status='processing',
                expires_at=datetime.utcnow() + timedelta(hours=self.ttl_hours)
            )
            session.add(record)
            session.commit()
            
            try:
                # Execute the operation
                result = operation_func(**params)
                
                # Update record with success
                record.status = 'completed'
                record.result_data = json.dumps(result, default=str)
                session.commit()
                
                return result
                
            except Exception as e:
                # Update record with failure
                record.status = 'failed'
                session.commit()
                raise
                
        finally:
            session.close()

def idempotent(operation_type: str, ttl_hours: int = 24, key_func: Optional[Callable] = None):
    """Decorator for idempotent operations."""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        operation = IdempotentOperation(operation_type, ttl_hours)
        
        def wrapper(*args, **kwargs):
            # Extract idempotency key if provided
            idempotency_key = kwargs.pop('idempotency_key', None)
            
            # Use custom key function if provided
            if key_func and not idempotency_key:
                idempotency_key = key_func(*args, **kwargs)
            
            return operation.execute(idempotency_key, func, *args, **kwargs)
        
        return wrapper
    return decorator

class IdempotencyError(Exception):
    pass

class IdempotencyConflictError(IdempotencyError):
    pass

# Cleanup task for expired records
def cleanup_expired_idempotency_records(batch_size: int = 1000):
    """Remove expired idempotency records."""
    session = SessionLocal()
    try:
        cutoff = datetime.utcnow()
        deleted = 0
        
        while True:
            batch = session.query(IdempotencyRecord).filter(
                IdempotencyRecord.expires_at < cutoff
            ).limit(batch_size).all()
            
            if not batch:
                break
                
            for record in batch:
                session.delete(record)
            
            session.commit()
            deleted += len(batch)
            
            if len(batch) < batch_size:
                break
        
        return {"deleted": deleted}
    finally:
        session.close()
