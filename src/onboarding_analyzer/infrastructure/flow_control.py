"""Flow control and backpressure management for high-throughput scenarios.

Implements adaptive throttling based on downstream health and queue depth monitoring.
"""
from __future__ import annotations
import time
import threading
from typing import Optional, Callable, Any
from dataclasses import dataclass
from prometheus_client import Gauge, Counter
from onboarding_analyzer.infrastructure.circuit_breaker import get_circuit_breaker, CircuitBreakerOpenError

@dataclass
class FlowControlConfig:
    max_requests_per_second: float = 100.0
    burst_capacity: int = 50
    queue_depth_threshold: int = 1000
    backpressure_factor: float = 0.5  # Reduce rate by this factor under pressure

# Metrics
FLOW_CONTROL_RATE = Gauge('flow_control_current_rate_per_second', 'Current allowed rate', ['service'])
FLOW_CONTROL_QUEUE_DEPTH = Gauge('flow_control_queue_depth', 'Current queue depth', ['service'])
FLOW_CONTROL_THROTTLED = Counter('flow_control_throttled_total', 'Requests throttled', ['service'])

class FlowController:
    def __init__(self, service_name: str, config: Optional[FlowControlConfig] = None):
        self.service_name = service_name
        self.config = config or FlowControlConfig()
        self.current_rate = self.config.max_requests_per_second
        self.token_bucket = self.config.burst_capacity
        self.last_refill = time.time()
        self.queue_depth = 0
        self._lock = threading.RLock()
        
    def acquire(self, timeout: float = 1.0) -> bool:
        """Acquire permission to proceed with operation."""
        with self._lock:
            self._refill_tokens()
            
            # Check queue depth for backpressure
            if self.queue_depth > self.config.queue_depth_threshold:
                self._apply_backpressure()
            
            if self.token_bucket >= 1:
                self.token_bucket -= 1
                self.queue_depth += 1
                FLOW_CONTROL_QUEUE_DEPTH.labels(service=self.service_name).set(self.queue_depth)
                return True
            else:
                FLOW_CONTROL_THROTTLED.labels(service=self.service_name).inc()
                return False
    
    def release(self):
        """Release operation (decrease queue depth)."""
        with self._lock:
            self.queue_depth = max(0, self.queue_depth - 1)
            FLOW_CONTROL_QUEUE_DEPTH.labels(service=self.service_name).set(self.queue_depth)
    
    def _refill_tokens(self):
        now = time.time()
        elapsed = now - self.last_refill
        tokens_to_add = elapsed * self.current_rate
        self.token_bucket = min(self.config.burst_capacity, self.token_bucket + tokens_to_add)
        self.last_refill = now
        FLOW_CONTROL_RATE.labels(service=self.service_name).set(self.current_rate)
    
    def _apply_backpressure(self):
        """Reduce rate when under pressure."""
        target_rate = self.config.max_requests_per_second * self.config.backpressure_factor
        self.current_rate = max(1.0, min(self.current_rate, target_rate))
    
    def reset_rate(self):
        """Reset to normal rate (called on successful operations)."""
        with self._lock:
            self.current_rate = min(
                self.config.max_requests_per_second,
                self.current_rate * 1.1  # Gradual recovery
            )

# Global flow controller registry
_controllers: dict[str, FlowController] = {}
_controller_lock = threading.Lock()

def get_flow_controller(service_name: str, config: Optional[FlowControlConfig] = None) -> FlowController:
    """Get or create flow controller for service."""
    with _controller_lock:
        if service_name not in _controllers:
            _controllers[service_name] = FlowController(service_name, config)
        return _controllers[service_name]

def flow_controlled(service_name: str, config: Optional[FlowControlConfig] = None, timeout: float = 1.0):
    """Decorator for flow control protection."""
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        controller = get_flow_controller(service_name, config)
        breaker = get_circuit_breaker(service_name)
        
        def wrapper(*args, **kwargs):
            # Flow control
            if not controller.acquire(timeout):
                raise FlowControlThrottledError(f"Flow control throttled for {service_name}")
            
            try:
                # Circuit breaker protection
                result = breaker.call(func, *args, **kwargs)
                controller.reset_rate()  # Success - allow rate recovery
                return result
            except CircuitBreakerOpenError:
                raise
            except Exception:
                raise
            finally:
                controller.release()
        
        return wrapper
    return decorator

class FlowControlThrottledError(Exception):
    pass
