"""Advanced database connection pooling with intelligent routing and failover.

Provides enterprise-grade connection management with health monitoring and optimization.
"""
from __future__ import annotations
import asyncio
import logging
import time
import threading
from typing import Dict, List, Any, Optional, Callable, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from contextlib import contextmanager, asynccontextmanager
from collections import defaultdict, deque
import psycopg2
from psycopg2 import pool
from prometheus_client import Counter, Histogram, Gauge
from onboarding_analyzer.infrastructure.circuit_breaker import CircuitBreaker

logger = logging.getLogger(__name__)

class ConnectionType(Enum):
    READ_WRITE = "read_write"
    READ_ONLY = "read_only"
    ANALYTICS = "analytics"
    BACKUP = "backup"

class PoolStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    MAINTENANCE = "maintenance"

class ConnectionStatus(Enum):
    ACTIVE = "active"
    IDLE = "idle"
    TESTING = "testing"
    FAILED = "failed"
    EXPIRED = "expired"

@dataclass
class ConnectionConfig:
    host: str
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = ""
    connection_type: ConnectionType = ConnectionType.READ_WRITE
    priority: int = 100  # Lower number = higher priority
    max_connections: int = 20
    min_connections: int = 2
    max_idle_time: int = 300  # seconds
    max_lifetime: int = 3600  # seconds
    health_check_interval: int = 30  # seconds
    connect_timeout: int = 10
    read_timeout: int = 30
    sslmode: str = "prefer"

@dataclass
class ConnectionMetrics:
    total_created: int = 0
    total_closed: int = 0
    total_failed: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    wait_queue_size: int = 0
    avg_checkout_time: float = 0.0
    avg_execution_time: float = 0.0
    last_health_check: Optional[datetime] = None
    health_check_failures: int = 0

@dataclass
class PooledConnection:
    connection: Any
    created_at: datetime
    last_used: datetime
    status: ConnectionStatus = ConnectionStatus.IDLE
    checkout_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None

# Metrics
POOL_CONNECTIONS = Gauge('db_pool_connections', 'Database pool connections', ['pool', 'status'])
CONNECTION_CHECKOUTS = Counter('db_connection_checkouts_total', 'Connection checkouts', ['pool', 'type'])
CONNECTION_CHECKOUT_DURATION = Histogram('db_connection_checkout_seconds', 'Connection checkout time', ['pool'])
CONNECTION_EXECUTION_DURATION = Histogram('db_connection_execution_seconds', 'Query execution time', ['pool', 'type'])
CONNECTION_ERRORS = Counter('db_connection_errors_total', 'Connection errors', ['pool', 'error_type'])
POOL_HEALTH = Gauge('db_pool_health_score', 'Pool health score (0-1)', ['pool'])
CONNECTION_LEAKS = Counter('db_connection_leaks_total', 'Connection leaks detected', ['pool'])

class ConnectionPool:
    """Advanced connection pool with health monitoring and failover."""
    
    def __init__(self, pool_name: str, config: ConnectionConfig):
        self.pool_name = pool_name
        self.config = config
        self.connections: Dict[str, PooledConnection] = {}
        self.available_connections: deque = deque()
        self.wait_queue: deque = deque()
        self.metrics = ConnectionMetrics()
        self.status = PoolStatus.HEALTHY
        self.circuit_breaker = CircuitBreaker(f"db_pool_{pool_name}")
        
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._health_check_task = None
        self._leak_detection_task = None
        self._shutdown = False
        
        # Initialize minimum connections
        self._initialize_pool()
        
        # Start background tasks
        self._start_background_tasks()
    
    def _initialize_pool(self):
        """Initialize the pool with minimum connections."""
        with self._lock:
            for _ in range(self.config.min_connections):
                try:
                    conn = self._create_connection()
                    if conn:
                        self.available_connections.append(conn.connection.get_dsn_parameters()['host'])
                except Exception as e:
                    logger.error(f"Failed to initialize connection in pool {self.pool_name}: {e}")
    
    def _create_connection(self) -> Optional[PooledConnection]:
        """Create a new database connection."""
        try:
            start_time = time.time()
            
            conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                connect_timeout=self.config.connect_timeout,
                sslmode=self.config.sslmode
            )
            
            # Set connection properties
            conn.autocommit = False
            if self.config.connection_type == ConnectionType.READ_ONLY:
                with conn.cursor() as cur:
                    cur.execute("SET default_transaction_read_only = on")
            
            connection_id = f"{self.pool_name}_{id(conn)}"
            pooled_conn = PooledConnection(
                connection=conn,
                created_at=datetime.utcnow(),
                last_used=datetime.utcnow(),
                status=ConnectionStatus.IDLE
            )
            
            self.connections[connection_id] = pooled_conn
            self.metrics.total_created += 1
            self.metrics.idle_connections += 1
            
            create_time = time.time() - start_time
            logger.debug(f"Created connection {connection_id} in {create_time:.3f}s")
            
            POOL_CONNECTIONS.labels(pool=self.pool_name, status='idle').inc()
            
            return pooled_conn
            
        except Exception as e:
            self.metrics.total_failed += 1
            CONNECTION_ERRORS.labels(pool=self.pool_name, error_type='create').inc()
            logger.error(f"Failed to create connection for pool {self.pool_name}: {e}")
            return None
    
    def _start_background_tasks(self):
        """Start background monitoring tasks."""
        def health_check_loop():
            while not self._shutdown:
                try:
                    self._perform_health_check()
                    time.sleep(self.config.health_check_interval)
                except Exception as e:
                    logger.error(f"Health check failed for pool {self.pool_name}: {e}")
                    time.sleep(self.config.health_check_interval)
        
        def leak_detection_loop():
            while not self._shutdown:
                try:
                    self._detect_connection_leaks()
                    time.sleep(60)  # Check for leaks every minute
                except Exception as e:
                    logger.error(f"Leak detection failed for pool {self.pool_name}: {e}")
                    time.sleep(60)
        
        self._health_check_task = threading.Thread(target=health_check_loop, daemon=True)
        self._health_check_task.start()
        
        self._leak_detection_task = threading.Thread(target=leak_detection_loop, daemon=True)
        self._leak_detection_task.start()
    
    def _perform_health_check(self):
        """Perform health check on all connections."""
        with self._lock:
            healthy_connections = 0
            total_connections = len(self.connections)
            
            for conn_id, pooled_conn in list(self.connections.items()):
                try:
                    if pooled_conn.status == ConnectionStatus.ACTIVE:
                        continue  # Skip active connections
                    
                    pooled_conn.status = ConnectionStatus.TESTING
                    
                    # Perform simple health check
                    with pooled_conn.connection.cursor() as cur:
                        cur.execute("SELECT 1")
                        cur.fetchone()
                    
                    pooled_conn.status = ConnectionStatus.IDLE
                    healthy_connections += 1
                    
                except Exception as e:
                    logger.warning(f"Health check failed for connection {conn_id}: {e}")
                    pooled_conn.status = ConnectionStatus.FAILED
                    pooled_conn.error_count += 1
                    pooled_conn.last_error = str(e)
                    
                    # Remove failed connection
                    self._remove_connection(conn_id)
            
            # Update health metrics
            self.metrics.last_health_check = datetime.utcnow()
            
            if total_connections > 0:
                health_score = healthy_connections / total_connections
                POOL_HEALTH.labels(pool=self.pool_name).set(health_score)
                
                # Update pool status
                if health_score >= 0.8:
                    self.status = PoolStatus.HEALTHY
                elif health_score >= 0.5:
                    self.status = PoolStatus.DEGRADED
                else:
                    self.status = PoolStatus.UNHEALTHY
                    self.metrics.health_check_failures += 1
    
    def _detect_connection_leaks(self):
        """Detect and handle connection leaks."""
        with self._lock:
            now = datetime.utcnow()
            leaked_connections = []
            
            for conn_id, pooled_conn in self.connections.items():
                # Check for connections that have been active too long
                if (pooled_conn.status == ConnectionStatus.ACTIVE and
                    (now - pooled_conn.last_used).total_seconds() > self.config.max_lifetime):
                    leaked_connections.append(conn_id)
                
                # Check for expired idle connections
                elif (pooled_conn.status == ConnectionStatus.IDLE and
                      (now - pooled_conn.last_used).total_seconds() > self.config.max_idle_time):
                    pooled_conn.status = ConnectionStatus.EXPIRED
                    leaked_connections.append(conn_id)
            
            # Clean up leaked/expired connections
            for conn_id in leaked_connections:
                logger.warning(f"Cleaning up leaked/expired connection {conn_id}")
                CONNECTION_LEAKS.labels(pool=self.pool_name).inc()
                self._remove_connection(conn_id)
    
    def _remove_connection(self, connection_id: str):
        """Remove a connection from the pool."""
        try:
            pooled_conn = self.connections.get(connection_id)
            if pooled_conn:
                try:
                    pooled_conn.connection.close()
                except Exception:
                    pass  # Ignore close errors
                
                del self.connections[connection_id]
                self.metrics.total_closed += 1
                
                if pooled_conn.status == ConnectionStatus.IDLE:
                    self.metrics.idle_connections -= 1
                    POOL_CONNECTIONS.labels(pool=self.pool_name, status='idle').dec()
                elif pooled_conn.status == ConnectionStatus.ACTIVE:
                    self.metrics.active_connections -= 1
                    POOL_CONNECTIONS.labels(pool=self.pool_name, status='active').dec()
                
        except Exception as e:
            logger.error(f"Error removing connection {connection_id}: {e}")
    
    @contextmanager
    def get_connection(self, timeout: float = 30.0):
        """Get a connection from the pool."""
        start_time = time.time()
        connection_id = None
        pooled_conn = None
        
        try:
            # Check circuit breaker
            if not self.circuit_breaker.call_allowed():
                CONNECTION_ERRORS.labels(pool=self.pool_name, error_type='circuit_open').inc()
                raise Exception("Circuit breaker is open")
            
            with self._condition:
                # Wait for available connection
                deadline = time.time() + timeout
                
                while True:
                    # Try to get an available connection
                    available_conn = self._get_available_connection()
                    if available_conn:
                        connection_id, pooled_conn = available_conn
                        break
                    
                    # Check if we can create a new connection
                    if len(self.connections) < self.config.max_connections:
                        new_conn = self._create_connection()
                        if new_conn:
                            connection_id = list(self.connections.keys())[-1]
                            pooled_conn = new_conn
                            break
                    
                    # Wait for a connection to become available
                    remaining_time = deadline - time.time()
                    if remaining_time <= 0:
                        CONNECTION_ERRORS.labels(pool=self.pool_name, error_type='timeout').inc()
                        raise Exception(f"Timeout waiting for connection from pool {self.pool_name}")
                    
                    self._condition.wait(min(remaining_time, 1.0))
                
                # Mark connection as active
                pooled_conn.status = ConnectionStatus.ACTIVE
                pooled_conn.last_used = datetime.utcnow()
                pooled_conn.checkout_count += 1
                
                self.metrics.active_connections += 1
                self.metrics.idle_connections -= 1
                
                POOL_CONNECTIONS.labels(pool=self.pool_name, status='active').inc()
                POOL_CONNECTIONS.labels(pool=self.pool_name, status='idle').dec()
                CONNECTION_CHECKOUTS.labels(pool=self.pool_name, type=self.config.connection_type.value).inc()
            
            checkout_time = time.time() - start_time
            CONNECTION_CHECKOUT_DURATION.labels(pool=self.pool_name).observe(checkout_time)
            
            logger.debug(f"Checked out connection {connection_id} in {checkout_time:.3f}s")
            
            # Record successful operation with circuit breaker
            self.circuit_breaker.record_success()
            
            yield pooled_conn.connection
            
        except Exception as e:
            # Record failure with circuit breaker
            self.circuit_breaker.record_failure()
            CONNECTION_ERRORS.labels(pool=self.pool_name, error_type='checkout').inc()
            logger.error(f"Error getting connection from pool {self.pool_name}: {e}")
            raise
            
        finally:
            # Return connection to pool
            if connection_id and pooled_conn:
                with self._condition:
                    pooled_conn.status = ConnectionStatus.IDLE
                    pooled_conn.last_used = datetime.utcnow()
                    
                    self.metrics.active_connections -= 1
                    self.metrics.idle_connections += 1
                    
                    POOL_CONNECTIONS.labels(pool=self.pool_name, status='active').dec()
                    POOL_CONNECTIONS.labels(pool=self.pool_name, status='idle').inc()
                    
                    self._condition.notify()
                
                logger.debug(f"Returned connection {connection_id} to pool")
    
    def _get_available_connection(self) -> Optional[tuple]:
        """Get an available connection from the pool."""
        for conn_id, pooled_conn in self.connections.items():
            if pooled_conn.status == ConnectionStatus.IDLE:
                return conn_id, pooled_conn
        return None
    
    def execute_query(self, query: str, params: tuple = None, fetch: bool = True) -> Any:
        """Execute a query using a connection from the pool."""
        start_time = time.time()
        
        with self.get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    
                    if fetch:
                        if query.strip().upper().startswith('SELECT'):
                            result = cur.fetchall()
                        else:
                            result = cur.rowcount
                    else:
                        result = None
                    
                    if not conn.autocommit:
                        conn.commit()
                    
                    execution_time = time.time() - start_time
                    CONNECTION_EXECUTION_DURATION.labels(
                        pool=self.pool_name, 
                        type=self.config.connection_type.value
                    ).observe(execution_time)
                    
                    return result
                    
            except Exception as e:
                if not conn.autocommit:
                    conn.rollback()
                CONNECTION_ERRORS.labels(pool=self.pool_name, error_type='execution').inc()
                logger.error(f"Query execution failed in pool {self.pool_name}: {e}")
                raise
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get comprehensive pool statistics."""
        with self._lock:
            return {
                'pool_name': self.pool_name,
                'status': self.status.value,
                'config': {
                    'host': self.config.host,
                    'port': self.config.port,
                    'database': self.config.database,
                    'connection_type': self.config.connection_type.value,
                    'max_connections': self.config.max_connections,
                    'min_connections': self.config.min_connections
                },
                'metrics': {
                    'total_connections': len(self.connections),
                    'active_connections': self.metrics.active_connections,
                    'idle_connections': self.metrics.idle_connections,
                    'total_created': self.metrics.total_created,
                    'total_closed': self.metrics.total_closed,
                    'total_failed': self.metrics.total_failed,
                    'health_check_failures': self.metrics.health_check_failures,
                    'last_health_check': self.metrics.last_health_check.isoformat() if self.metrics.last_health_check else None
                },
                'circuit_breaker': {
                    'state': self.circuit_breaker.state.value,
                    'failure_count': self.circuit_breaker.failure_count,
                    'success_count': self.circuit_breaker.success_count
                }
            }
    
    def shutdown(self):
        """Shutdown the connection pool."""
        self._shutdown = True
        
        with self._lock:
            # Close all connections
            for conn_id in list(self.connections.keys()):
                self._remove_connection(conn_id)
            
            self._condition.notify_all()
        
        logger.info(f"Connection pool {self.pool_name} shutdown complete")

class ConnectionPoolManager:
    """Manages multiple connection pools with intelligent routing."""
    
    def __init__(self):
        self.pools: Dict[str, ConnectionPool] = {}
        self.routing_rules: List[Dict[str, Any]] = []
        self._lock = threading.RLock()
    
    def create_pool(self, pool_name: str, config: ConnectionConfig) -> ConnectionPool:
        """Create a new connection pool."""
        with self._lock:
            if pool_name in self.pools:
                raise ValueError(f"Pool {pool_name} already exists")
            
            pool = ConnectionPool(pool_name, config)
            self.pools[pool_name] = pool
            
            logger.info(f"Created connection pool {pool_name} for {config.host}:{config.port}")
            return pool
    
    def add_routing_rule(self, rule: Dict[str, Any]):
        """Add a routing rule for query distribution."""
        with self._lock:
            self.routing_rules.append(rule)
            # Sort by priority (lower number = higher priority)
            self.routing_rules.sort(key=lambda x: x.get('priority', 100))
    
    def get_pool_for_query(self, query: str, query_type: str = None) -> Optional[ConnectionPool]:
        """Get the best pool for a given query."""
        with self._lock:
            # Apply routing rules
            for rule in self.routing_rules:
                if self._matches_rule(query, query_type, rule):
                    pool_name = rule.get('pool_name')
                    pool = self.pools.get(pool_name)
                    if pool and pool.status != PoolStatus.UNHEALTHY:
                        return pool
            
            # Fallback to default routing
            return self._get_default_pool(query, query_type)
    
    def _matches_rule(self, query: str, query_type: str, rule: Dict[str, Any]) -> bool:
        """Check if a query matches a routing rule."""
        # Check query type
        if rule.get('query_type') and query_type != rule['query_type']:
            return False
        
        # Check query patterns
        patterns = rule.get('patterns', [])
        if patterns:
            query_upper = query.upper().strip()
            for pattern in patterns:
                if pattern.upper() in query_upper:
                    return True
            return False
        
        # Check time-based rules
        time_rules = rule.get('time_rules')
        if time_rules:
            current_hour = datetime.utcnow().hour
            start_hour = time_rules.get('start_hour', 0)
            end_hour = time_rules.get('end_hour', 23)
            
            if not (start_hour <= current_hour <= end_hour):
                return False
        
        return True
    
    def _get_default_pool(self, query: str, query_type: str) -> Optional[ConnectionPool]:
        """Get default pool based on query characteristics."""
        query_upper = query.upper().strip()
        
        # Route read-only queries to read replicas
        if (query_upper.startswith('SELECT') and 
            'FOR UPDATE' not in query_upper and 
            'LOCK' not in query_upper):
            
            read_pools = [
                pool for pool in self.pools.values()
                if (pool.config.connection_type in [ConnectionType.READ_ONLY, ConnectionType.ANALYTICS] and
                    pool.status != PoolStatus.UNHEALTHY)
            ]
            
            if read_pools:
                # Choose pool with lowest load
                return min(read_pools, key=lambda p: p.metrics.active_connections)
        
        # Route write queries to primary
        write_pools = [
            pool for pool in self.pools.values()
            if (pool.config.connection_type == ConnectionType.READ_WRITE and
                pool.status != PoolStatus.UNHEALTHY)
        ]
        
        if write_pools:
            return min(write_pools, key=lambda p: p.metrics.active_connections)
        
        # Fallback to any healthy pool
        healthy_pools = [
            pool for pool in self.pools.values()
            if pool.status != PoolStatus.UNHEALTHY
        ]
        
        if healthy_pools:
            return min(healthy_pools, key=lambda p: p.metrics.active_connections)
        
        return None
    
    @contextmanager
    def get_connection(self, query: str = None, query_type: str = None, pool_name: str = None):
        """Get a connection with intelligent routing."""
        if pool_name:
            pool = self.pools.get(pool_name)
            if not pool:
                raise ValueError(f"Pool {pool_name} not found")
        else:
            pool = self.get_pool_for_query(query or "", query_type)
            if not pool:
                raise Exception("No healthy pools available")
        
        with pool.get_connection() as conn:
            yield conn
    
    def execute_query(self, query: str, params: tuple = None, fetch: bool = True, 
                     query_type: str = None, pool_name: str = None) -> Any:
        """Execute a query with intelligent routing."""
        if pool_name:
            pool = self.pools.get(pool_name)
            if not pool:
                raise ValueError(f"Pool {pool_name} not found")
        else:
            pool = self.get_pool_for_query(query, query_type)
            if not pool:
                raise Exception("No healthy pools available")
        
        return pool.execute_query(query, params, fetch)
    
    def get_all_pool_stats(self) -> Dict[str, Any]:
        """Get statistics for all pools."""
        with self._lock:
            return {
                pool_name: pool.get_pool_stats()
                for pool_name, pool in self.pools.items()
            }
    
    def shutdown_all_pools(self):
        """Shutdown all connection pools."""
        with self._lock:
            for pool in self.pools.values():
                pool.shutdown()
            self.pools.clear()
        
        logger.info("All connection pools shut down")

# Global connection pool manager
pool_manager = ConnectionPoolManager()

def setup_database_pools(configs: Dict[str, ConnectionConfig]):
    """Setup database connection pools from configuration."""
    for pool_name, config in configs.items():
        pool_manager.create_pool(pool_name, config)

def get_connection(query: str = None, query_type: str = None, pool_name: str = None):
    """Convenience function to get a database connection."""
    return pool_manager.get_connection(query, query_type, pool_name)

def execute_query(query: str, params: tuple = None, fetch: bool = True, 
                 query_type: str = None, pool_name: str = None) -> Any:
    """Convenience function to execute a query."""
    return pool_manager.execute_query(query, params, fetch, query_type, pool_name)
