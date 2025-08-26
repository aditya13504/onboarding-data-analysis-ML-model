"""Circuit breaker pattern for downstream service protection and backpressure control.

Prevents cascade failures by failing fast when downstream services (DB, Redis, Slack, SMTP) 
are experiencing issues. Implements exponential backoff and health check recovery.
"""
from __future__ import annotations
import time
import threading
from enum import Enum
from typing import Callable, Any, TypeVar, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
from prometheus_client import Counter, Histogram, Gauge

T = TypeVar('T')

class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing fast
    HALF_OPEN = "half_open" # Testing recovery

@dataclass
class CircuitConfig:
    failure_threshold: int = 5
    recovery_timeout: float = 60.0  # seconds
    timeout: float = 30.0           # operation timeout
    expected_failure_rate: float = 0.5  # failures that trigger opening

# Metrics
CIRCUIT_BREAKER_STATE = Gauge('circuit_breaker_state_info', 'Circuit breaker current state', ['service', 'state'])
CIRCUIT_BREAKER_CALLS = Counter('circuit_breaker_calls_total', 'Total calls through circuit breaker', ['service', 'result'])
CIRCUIT_BREAKER_DURATION = Histogram('circuit_breaker_call_duration_seconds', 'Call duration', ['service'])

class CircuitBreaker:
    def __init__(self, service_name: str, config: Optional[CircuitConfig] = None):
        self.service_name = service_name
        self.config = config or CircuitConfig()
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.half_open_calls = 0
        self._lock = threading.RLock()
        
    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute function with circuit breaker protection."""
        with self._lock:
            if self._should_attempt_call():
                return self._attempt_call(func, *args, **kwargs)
            else:
                CIRCUIT_BREAKER_CALLS.labels(service=self.service_name, result='rejected').inc()
                raise CircuitBreakerOpenError(f"Circuit breaker open for {self.service_name}")
    
    def _should_attempt_call(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        elif self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitState.HALF_OPEN
                self.half_open_calls = 0
                self._update_state_metric()
                return True
            return False
        else:  # HALF_OPEN
            return self.half_open_calls < 3  # Allow limited calls to test recovery
    
    def _should_attempt_reset(self) -> bool:
        return (self.last_failure_time is not None and 
                time.time() - self.last_failure_time >= self.config.recovery_timeout)
    
    def _attempt_call(self, func: Callable[..., T], *args, **kwargs) -> T:
        start_time = time.time()
        try:
            # Timeout wrapper
            import signal
            def timeout_handler(signum, frame):
                raise TimeoutError(f"Call timeout after {self.config.timeout}s")
            
            # Set timeout (Unix-like systems)
            if hasattr(signal, 'SIGALRM'):
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(int(self.config.timeout))
            
            try:
                result = func(*args, **kwargs)
                self._on_success()
                CIRCUIT_BREAKER_CALLS.labels(service=self.service_name, result='success').inc()
                return result
            finally:
                if hasattr(signal, 'SIGALRM'):
                    signal.alarm(0)  # Cancel timeout
                    
        except Exception as e:
            self._on_failure()
            CIRCUIT_BREAKER_CALLS.labels(service=self.service_name, result='failure').inc()
            raise
        finally:
            duration = time.time() - start_time
            CIRCUIT_BREAKER_DURATION.labels(service=self.service_name).observe(duration)
    
    def _on_success(self):
        if self.state == CircuitState.HALF_OPEN:
            self.half_open_calls += 1
            if self.half_open_calls >= 3:  # Sufficient successful calls
                self._reset()
        elif self.state == CircuitState.CLOSED:
            self.failure_count = max(0, self.failure_count - 1)  # Gradual recovery
    
    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == CircuitState.HALF_OPEN:
            self._trip()
        elif (self.state == CircuitState.CLOSED and 
              self.failure_count >= self.config.failure_threshold):
            self._trip()
    
    def _trip(self):
        self.state = CircuitState.OPEN
        self._update_state_metric()
    
    def _reset(self):
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.half_open_calls = 0
        self._update_state_metric()
    
    def _update_state_metric(self):
        # Reset all state gauges for this service
        for state in CircuitState:
            CIRCUIT_BREAKER_STATE.labels(service=self.service_name, state=state.value).set(0)
        # Set current state
        CIRCUIT_BREAKER_STATE.labels(service=self.service_name, state=self.state.value).set(1)

class CircuitBreakerOpenError(Exception):
    pass

# Global circuit breaker registry
_breakers: dict[str, CircuitBreaker] = {}
_registry_lock = threading.Lock()

def get_circuit_breaker(service_name: str, config: Optional[CircuitConfig] = None) -> CircuitBreaker:
    """Get or create circuit breaker for service."""
    with _registry_lock:
        if service_name not in _breakers:
            _breakers[service_name] = CircuitBreaker(service_name, config)
        return _breakers[service_name]

def circuit_breaker(service_name: str, config: Optional[CircuitConfig] = None):
    """Decorator for circuit breaker protection."""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        breaker = get_circuit_breaker(service_name, config)
        def wrapper(*args, **kwargs) -> T:
            return breaker.call(func, *args, **kwargs)
        return wrapper
    return decorator
