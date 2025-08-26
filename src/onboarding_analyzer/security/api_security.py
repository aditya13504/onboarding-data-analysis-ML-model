"""
API Security and Rate Limiting System

Enterprise-grade API security with rate limiting, request validation, DDoS protection,
API key management, and comprehensive security monitoring.
"""

import asyncio
import logging
import time
import hashlib
import secrets
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Set, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import json
import ipaddress
from pathlib import Path

# FastAPI and security
from fastapi import HTTPException, Request, Response, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.base import BaseHTTPMiddleware
from starlette.middleware.base import BaseHTTPMiddleware as StarletteBaseMiddleware
from starlette.requests import Request as StarletteRequest
from starlette.responses import Response as StarletteResponse

# Redis for distributed rate limiting
import redis.asyncio as redis

# Monitoring
from prometheus_client import Counter, Histogram, Gauge

# Import authentication components
from .authentication import SecurityEvent, SecurityEventType, SecurityAuditLogger

# Initialize metrics
api_requests_total = Counter('api_requests_total', 'Total API requests', ['method', 'endpoint', 'status'])
api_rate_limit_exceeded = Counter('api_rate_limit_exceeded_total', 'Rate limit exceeded', ['limit_type', 'identifier'])
api_request_duration = Histogram('api_request_duration_seconds', 'API request duration', ['method', 'endpoint'])
api_active_connections = Gauge('api_active_connections', 'Active API connections')
api_security_events = Counter('api_security_events_total', 'API security events', ['event_type', 'severity'])
api_blocked_requests = Counter('api_blocked_requests_total', 'Blocked API requests', ['reason'])

logger = logging.getLogger(__name__)

class RateLimitType(Enum):
    """Types of rate limiting."""
    PER_USER = "per_user"
    PER_IP = "per_ip"
    PER_API_KEY = "per_api_key"
    GLOBAL = "global"
    PER_ENDPOINT = "per_endpoint"

class SecurityLevel(Enum):
    """Security levels for different endpoints."""
    PUBLIC = "public"
    AUTHENTICATED = "authenticated"
    PRIVILEGED = "privileged"
    RESTRICTED = "restricted"

class ThreatLevel(Enum):
    """Threat level classifications."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class RateLimitRule:
    """Rate limiting rule configuration."""
    rule_id: str
    name: str
    limit_type: RateLimitType
    requests_per_minute: int
    requests_per_hour: int
    requests_per_day: int
    burst_limit: int
    window_size: int = 60  # seconds
    is_enabled: bool = True
    endpoints: List[str] = field(default_factory=list)
    methods: List[str] = field(default_factory=list)
    user_roles: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class APIKey:
    """API key configuration."""
    key_id: str
    key_hash: str
    name: str
    user_id: str
    permissions: List[str]
    rate_limits: Dict[str, int]
    created_at: datetime
    expires_at: Optional[datetime] = None
    last_used: Optional[datetime] = None
    is_active: bool = True
    allowed_ips: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class SecurityRule:
    """Security validation rule."""
    rule_id: str
    name: str
    description: str
    rule_type: str  # validation, blocking, monitoring
    pattern: str
    action: str  # block, warn, log
    severity: ThreatLevel
    is_enabled: bool = True
    endpoints: List[str] = field(default_factory=list)
    conditions: Dict[str, Any] = field(default_factory=dict)

@dataclass
class RateLimitInfo:
    """Rate limit status information."""
    identifier: str
    limit_type: RateLimitType
    current_count: int
    limit: int
    window_start: datetime
    window_end: datetime
    reset_time: datetime
    remaining: int
    is_exceeded: bool

@dataclass
class SecurityThreat:
    """Detected security threat."""
    threat_id: str
    threat_type: str
    severity: ThreatLevel
    source_ip: str
    user_id: Optional[str]
    description: str
    detected_at: datetime
    evidence: Dict[str, Any]
    is_blocked: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

# Default rate limiting rules
DEFAULT_RATE_LIMITS = [
    RateLimitRule(
        rule_id="default_user_limit",
        name="Default User Rate Limit",
        limit_type=RateLimitType.PER_USER,
        requests_per_minute=100,
        requests_per_hour=1000,
        requests_per_day=10000,
        burst_limit=20
    ),
    RateLimitRule(
        rule_id="default_ip_limit",
        name="Default IP Rate Limit",
        limit_type=RateLimitType.PER_IP,
        requests_per_minute=200,
        requests_per_hour=2000,
        requests_per_day=20000,
        burst_limit=50
    ),
    RateLimitRule(
        rule_id="auth_endpoint_limit",
        name="Authentication Endpoint Limit",
        limit_type=RateLimitType.PER_IP,
        requests_per_minute=10,
        requests_per_hour=50,
        requests_per_day=200,
        burst_limit=5,
        endpoints=["/auth/login", "/auth/register", "/auth/reset-password"]
    ),
    RateLimitRule(
        rule_id="api_key_limit",
        name="API Key Rate Limit",
        limit_type=RateLimitType.PER_API_KEY,
        requests_per_minute=500,
        requests_per_hour=5000,
        requests_per_day=50000,
        burst_limit=100
    )
]

# Default security rules
DEFAULT_SECURITY_RULES = [
    SecurityRule(
        rule_id="sql_injection_detection",
        name="SQL Injection Detection",
        description="Detect potential SQL injection attempts",
        rule_type="validation",
        pattern=r"(?i)(union|select|insert|update|delete|drop|exec|script)",
        action="block",
        severity=ThreatLevel.HIGH
    ),
    SecurityRule(
        rule_id="xss_detection",
        name="XSS Detection",
        description="Detect potential XSS attempts",
        rule_type="validation",
        pattern=r"(?i)(<script|javascript:|data:text/html)",
        action="block",
        severity=ThreatLevel.HIGH
    ),
    SecurityRule(
        rule_id="path_traversal_detection",
        name="Path Traversal Detection",
        description="Detect path traversal attempts",
        rule_type="validation",
        pattern=r"(\.\./|\.\.\\|%2e%2e%2f|%2e%2e%5c)",
        action="block",
        severity=ThreatLevel.MEDIUM
    ),
    SecurityRule(
        rule_id="suspicious_user_agent",
        name="Suspicious User Agent Detection",
        description="Detect suspicious user agents",
        rule_type="monitoring",
        pattern=r"(?i)(bot|crawler|scanner|hack|attack|exploit)",
        action="warn",
        severity=ThreatLevel.LOW
    )
]

class RateLimitBackend:
    """Rate limiting backend using Redis."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis_client: Optional[redis.Redis] = None
        self.fallback_storage: Dict[str, Dict[str, Any]] = {}
    
    async def initialize(self):
        """Initialize Redis connection."""
        try:
            self.redis_client = redis.from_url(self.redis_url)
            await self.redis_client.ping()
            logger.info("Redis connection established for rate limiting")
        except Exception as e:
            logger.warning(f"Redis connection failed, using in-memory fallback: {e}")
            self.redis_client = None
    
    async def check_rate_limit(self, key: str, limit: int, window: int) -> Tuple[bool, int, int]:
        """Check rate limit for key."""
        try:
            if self.redis_client:
                return await self._redis_rate_limit(key, limit, window)
            else:
                return await self._memory_rate_limit(key, limit, window)
        except Exception as e:
            logger.error(f"Rate limit check failed: {e}")
            return False, 0, 0  # Allow on error, but log
    
    async def _redis_rate_limit(self, key: str, limit: int, window: int) -> Tuple[bool, int, int]:
        """Redis-based rate limiting using sliding window."""
        current_time = time.time()
        window_start = current_time - window
        
        pipe = self.redis_client.pipeline()
        
        # Remove expired entries
        pipe.zremrangebyscore(key, 0, window_start)
        
        # Count current requests
        pipe.zcard(key)
        
        # Add current request
        pipe.zadd(key, {str(current_time): current_time})
        
        # Set expiration
        pipe.expire(key, window)
        
        results = await pipe.execute()
        current_count = results[1]
        
        if current_count >= limit:
            # Remove the request we just added since we're over limit
            await self.redis_client.zrem(key, str(current_time))
            remaining = 0
            reset_time = window
        else:
            remaining = limit - current_count - 1
            reset_time = window
        
        return current_count < limit, current_count, reset_time
    
    async def _memory_rate_limit(self, key: str, limit: int, window: int) -> Tuple[bool, int, int]:
        """In-memory rate limiting fallback."""
        current_time = time.time()
        
        if key not in self.fallback_storage:
            self.fallback_storage[key] = {"requests": [], "created_at": current_time}
        
        storage = self.fallback_storage[key]
        
        # Remove expired requests
        window_start = current_time - window
        storage["requests"] = [
            req_time for req_time in storage["requests"]
            if req_time > window_start
        ]
        
        current_count = len(storage["requests"])
        
        if current_count >= limit:
            remaining = 0
            reset_time = window
            return False, current_count, reset_time
        else:
            storage["requests"].append(current_time)
            remaining = limit - current_count - 1
            reset_time = window
            return True, current_count + 1, reset_time
    
    async def get_rate_limit_info(self, key: str, limit: int, window: int) -> RateLimitInfo:
        """Get detailed rate limit information."""
        current_time = datetime.now(timezone.utc)
        
        try:
            allowed, current_count, reset_time = await self.check_rate_limit(key, limit, window)
            
            return RateLimitInfo(
                identifier=key,
                limit_type=RateLimitType.PER_USER,  # Default, should be determined by caller
                current_count=current_count,
                limit=limit,
                window_start=current_time - timedelta(seconds=window),
                window_end=current_time,
                reset_time=current_time + timedelta(seconds=reset_time),
                remaining=max(0, limit - current_count),
                is_exceeded=not allowed
            )
        except Exception as e:
            logger.error(f"Failed to get rate limit info: {e}")
            return RateLimitInfo(
                identifier=key,
                limit_type=RateLimitType.PER_USER,
                current_count=0,
                limit=limit,
                window_start=current_time,
                window_end=current_time,
                reset_time=current_time,
                remaining=limit,
                is_exceeded=False
            )

class APIKeyManager:
    """API Key management system."""
    
    def __init__(self):
        self.api_keys: Dict[str, APIKey] = {}
        self.key_hash_map: Dict[str, str] = {}  # hash -> key_id
    
    def generate_api_key(self, user_id: str, name: str, permissions: List[str],
                        rate_limits: Dict[str, int] = None,
                        expires_at: Optional[datetime] = None,
                        allowed_ips: List[str] = None) -> Tuple[str, APIKey]:
        """Generate new API key."""
        # Generate key
        key_value = f"ak_{secrets.token_urlsafe(32)}"
        key_hash = hashlib.sha256(key_value.encode()).hexdigest()
        key_id = secrets.token_urlsafe(16)
        
        # Create API key object
        api_key = APIKey(
            key_id=key_id,
            key_hash=key_hash,
            name=name,
            user_id=user_id,
            permissions=permissions,
            rate_limits=rate_limits or {},
            created_at=datetime.now(timezone.utc),
            expires_at=expires_at,
            allowed_ips=allowed_ips or []
        )
        
        # Store
        self.api_keys[key_id] = api_key
        self.key_hash_map[key_hash] = key_id
        
        logger.info(f"API key generated for user {user_id}: {name}")
        return key_value, api_key
    
    def validate_api_key(self, key_value: str, request_ip: str = None) -> Optional[APIKey]:
        """Validate API key."""
        try:
            key_hash = hashlib.sha256(key_value.encode()).hexdigest()
            
            if key_hash not in self.key_hash_map:
                return None
            
            key_id = self.key_hash_map[key_hash]
            api_key = self.api_keys.get(key_id)
            
            if not api_key or not api_key.is_active:
                return None
            
            # Check expiration
            if api_key.expires_at and datetime.now(timezone.utc) > api_key.expires_at:
                return None
            
            # Check IP restrictions
            if api_key.allowed_ips and request_ip:
                if not self._is_ip_allowed(request_ip, api_key.allowed_ips):
                    return None
            
            # Update last used
            api_key.last_used = datetime.now(timezone.utc)
            
            return api_key
            
        except Exception as e:
            logger.error(f"API key validation failed: {e}")
            return None
    
    def revoke_api_key(self, key_id: str) -> bool:
        """Revoke API key."""
        if key_id in self.api_keys:
            api_key = self.api_keys[key_id]
            api_key.is_active = False
            
            # Remove from hash map
            if api_key.key_hash in self.key_hash_map:
                del self.key_hash_map[api_key.key_hash]
            
            logger.info(f"API key revoked: {key_id}")
            return True
        
        return False
    
    def get_user_api_keys(self, user_id: str) -> List[APIKey]:
        """Get all API keys for user."""
        return [key for key in self.api_keys.values() if key.user_id == user_id]
    
    def _is_ip_allowed(self, request_ip: str, allowed_ips: List[str]) -> bool:
        """Check if IP is in allowed list."""
        try:
            request_addr = ipaddress.ip_address(request_ip)
            
            for allowed_ip in allowed_ips:
                try:
                    if "/" in allowed_ip:
                        # CIDR notation
                        if request_addr in ipaddress.ip_network(allowed_ip, strict=False):
                            return True
                    else:
                        # Exact IP
                        if request_addr == ipaddress.ip_address(allowed_ip):
                            return True
                except Exception:
                    continue
            
            return False
            
        except Exception as e:
            logger.error(f"IP validation failed: {e}")
            return False

class SecurityValidator:
    """Request security validation."""
    
    def __init__(self):
        self.security_rules: Dict[str, SecurityRule] = {}
        self.blocked_ips: Set[str] = set()
        self.suspicious_patterns: Dict[str, int] = {}
        
        # Initialize default rules
        for rule in DEFAULT_SECURITY_RULES:
            self.security_rules[rule.rule_id] = rule
    
    def add_security_rule(self, rule: SecurityRule):
        """Add security rule."""
        self.security_rules[rule.rule_id] = rule
        logger.info(f"Security rule added: {rule.name}")
    
    def validate_request(self, request: Request) -> Tuple[bool, List[SecurityThreat]]:
        """Validate request against security rules."""
        threats = []
        is_safe = True
        
        # Get request data
        url = str(request.url)
        query_params = str(request.query_params)
        headers = dict(request.headers)
        user_agent = headers.get("user-agent", "")
        
        # Check blocked IPs
        client_ip = self._get_client_ip(request)
        if client_ip in self.blocked_ips:
            threat = SecurityThreat(
                threat_id=secrets.token_urlsafe(16),
                threat_type="blocked_ip",
                severity=ThreatLevel.HIGH,
                source_ip=client_ip,
                user_id=None,
                description="Request from blocked IP address",
                detected_at=datetime.now(timezone.utc),
                evidence={"ip": client_ip},
                is_blocked=True
            )
            threats.append(threat)
            is_safe = False
        
        # Apply security rules
        for rule in self.security_rules.values():
            if not rule.is_enabled:
                continue
            
            # Check if rule applies to this endpoint
            if rule.endpoints and not any(endpoint in url for endpoint in rule.endpoints):
                continue
            
            # Validate against rule pattern
            threat = self._check_pattern(rule, url, query_params, headers, client_ip)
            if threat:
                threats.append(threat)
                if rule.action == "block":
                    is_safe = False
        
        return is_safe, threats
    
    def _check_pattern(self, rule: SecurityRule, url: str, query_params: str,
                      headers: Dict[str, str], client_ip: str) -> Optional[SecurityThreat]:
        """Check request against security pattern."""
        import re
        
        try:
            # Combine all text to check
            text_to_check = f"{url} {query_params} {headers.get('user-agent', '')}"
            
            if re.search(rule.pattern, text_to_check):
                return SecurityThreat(
                    threat_id=secrets.token_urlsafe(16),
                    threat_type=rule.rule_type,
                    severity=rule.severity,
                    source_ip=client_ip,
                    user_id=None,
                    description=f"Security rule triggered: {rule.name}",
                    detected_at=datetime.now(timezone.utc),
                    evidence={
                        "rule_id": rule.rule_id,
                        "pattern": rule.pattern,
                        "matched_text": text_to_check[:500]  # Truncate for storage
                    },
                    is_blocked=(rule.action == "block")
                )
        except Exception as e:
            logger.error(f"Pattern check failed for rule {rule.rule_id}: {e}")
        
        return None
    
    def _get_client_ip(self, request: Request) -> str:
        """Get client IP address."""
        # Check X-Forwarded-For header (for load balancers/proxies)
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        
        # Check X-Real-IP header
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip
        
        # Fall back to direct connection IP
        return request.client.host if request.client else "unknown"
    
    def block_ip(self, ip_address: str, reason: str = ""):
        """Block IP address."""
        self.blocked_ips.add(ip_address)
        logger.warning(f"IP blocked: {ip_address} - {reason}")
    
    def unblock_ip(self, ip_address: str):
        """Unblock IP address."""
        self.blocked_ips.discard(ip_address)
        logger.info(f"IP unblocked: {ip_address}")

class APISecurityManager:
    """Main API security manager."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.rate_limit_backend = RateLimitBackend(redis_url)
        self.api_key_manager = APIKeyManager()
        self.security_validator = SecurityValidator()
        self.audit_logger = SecurityAuditLogger()
        
        # Rate limiting rules
        self.rate_limit_rules: Dict[str, RateLimitRule] = {}
        for rule in DEFAULT_RATE_LIMITS:
            self.rate_limit_rules[rule.rule_id] = rule
        
        # Threat detection
        self.active_threats: Dict[str, SecurityThreat] = {}
        self.threat_counts: Dict[str, int] = {}
        
        # Initialize
        asyncio.create_task(self._initialize())
    
    async def _initialize(self):
        """Initialize security manager."""
        await self.rate_limit_backend.initialize()
        logger.info("API Security Manager initialized")
    
    async def check_rate_limits(self, request: Request, user_id: str = None,
                              api_key: APIKey = None) -> Tuple[bool, List[RateLimitInfo]]:
        """Check all applicable rate limits."""
        client_ip = self.security_validator._get_client_ip(request)
        endpoint = request.url.path
        method = request.method
        
        rate_limit_infos = []
        all_passed = True
        
        # Check applicable rules
        for rule in self.rate_limit_rules.values():
            if not rule.is_enabled:
                continue
            
            # Check if rule applies
            if rule.endpoints and endpoint not in rule.endpoints:
                continue
            
            if rule.methods and method not in rule.methods:
                continue
            
            # Determine identifier
            identifier = self._get_rate_limit_identifier(rule, client_ip, user_id, api_key)
            if not identifier:
                continue
            
            # Check rate limit
            limit = rule.requests_per_minute
            window = rule.window_size
            
            allowed, current_count, reset_time = await self.rate_limit_backend.check_rate_limit(
                f"rate_limit:{rule.rule_id}:{identifier}",
                limit,
                window
            )
            
            rate_limit_info = RateLimitInfo(
                identifier=identifier,
                limit_type=rule.limit_type,
                current_count=current_count,
                limit=limit,
                window_start=datetime.now(timezone.utc) - timedelta(seconds=window),
                window_end=datetime.now(timezone.utc),
                reset_time=datetime.now(timezone.utc) + timedelta(seconds=reset_time),
                remaining=max(0, limit - current_count),
                is_exceeded=not allowed
            )
            
            rate_limit_infos.append(rate_limit_info)
            
            if not allowed:
                all_passed = False
                api_rate_limit_exceeded.labels(
                    limit_type=rule.limit_type.value,
                    identifier=identifier
                ).inc()
        
        return all_passed, rate_limit_infos
    
    def _get_rate_limit_identifier(self, rule: RateLimitRule, client_ip: str,
                                  user_id: str = None, api_key: APIKey = None) -> Optional[str]:
        """Get identifier for rate limiting."""
        if rule.limit_type == RateLimitType.PER_IP:
            return client_ip
        elif rule.limit_type == RateLimitType.PER_USER and user_id:
            return user_id
        elif rule.limit_type == RateLimitType.PER_API_KEY and api_key:
            return api_key.key_id
        elif rule.limit_type == RateLimitType.GLOBAL:
            return "global"
        elif rule.limit_type == RateLimitType.PER_ENDPOINT:
            return f"endpoint:{rule.endpoints[0] if rule.endpoints else 'unknown'}"
        
        return None
    
    async def validate_request_security(self, request: Request) -> Tuple[bool, List[SecurityThreat]]:
        """Validate request security."""
        return self.security_validator.validate_request(request)
    
    def add_rate_limit_rule(self, rule: RateLimitRule):
        """Add rate limiting rule."""
        self.rate_limit_rules[rule.rule_id] = rule
        logger.info(f"Rate limit rule added: {rule.name}")
    
    def add_security_rule(self, rule: SecurityRule):
        """Add security rule."""
        self.security_validator.add_security_rule(rule)
    
    async def log_security_event(self, event_type: str, request: Request, 
                                details: Dict[str, Any], severity: str = "info"):
        """Log security event."""
        client_ip = self.security_validator._get_client_ip(request)
        user_agent = request.headers.get("user-agent", "")
        
        event = SecurityEvent(
            event_id=secrets.token_urlsafe(16),
            event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,  # Map as needed
            user_id=details.get("user_id"),
            username=details.get("username"),
            ip_address=client_ip,
            user_agent=user_agent,
            timestamp=datetime.now(timezone.utc),
            details=details,
            severity=severity,
            resource=str(request.url.path)
        )
        
        self.audit_logger.log_security_event(event)
        
        # Update metrics
        api_security_events.labels(
            event_type=event_type,
            severity=severity
        ).inc()

class APISecurityMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware for API security."""
    
    def __init__(self, app, security_manager: APISecurityManager):
        super().__init__(app)
        self.security_manager = security_manager
    
    async def dispatch(self, request: Request, call_next) -> Response:
        """Process request through security checks."""
        start_time = time.time()
        
        # Increment active connections
        api_active_connections.inc()
        
        try:
            # Validate request security
            is_safe, threats = await self.security_manager.validate_request_security(request)
            
            if not is_safe:
                # Block request
                for threat in threats:
                    if threat.is_blocked:
                        await self.security_manager.log_security_event(
                            "request_blocked",
                            request,
                            {
                                "threat_id": threat.threat_id,
                                "threat_type": threat.threat_type,
                                "reason": threat.description
                            },
                            "warning"
                        )
                
                api_blocked_requests.labels(reason="security_threat").inc()
                
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Request blocked by security policy"
                )
            
            # Extract user/API key info (this would be set by auth middleware)
            user_id = getattr(request.state, 'user_id', None)
            api_key = getattr(request.state, 'api_key', None)
            
            # Check rate limits
            rate_limit_passed, rate_limit_infos = await self.security_manager.check_rate_limits(
                request, user_id, api_key
            )
            
            if not rate_limit_passed:
                # Rate limit exceeded
                await self.security_manager.log_security_event(
                    "rate_limit_exceeded",
                    request,
                    {
                        "user_id": user_id,
                        "rate_limits": [info.__dict__ for info in rate_limit_infos if info.is_exceeded]
                    },
                    "warning"
                )
                
                api_blocked_requests.labels(reason="rate_limit").inc()
                
                # Set rate limit headers
                exceeded_info = next((info for info in rate_limit_infos if info.is_exceeded), None)
                headers = {}
                if exceeded_info:
                    headers.update({
                        "X-RateLimit-Limit": str(exceeded_info.limit),
                        "X-RateLimit-Remaining": str(exceeded_info.remaining),
                        "X-RateLimit-Reset": str(int(exceeded_info.reset_time.timestamp()))
                    })
                
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="Rate limit exceeded",
                    headers=headers
                )
            
            # Process request
            response = await call_next(request)
            
            # Add security headers
            self._add_security_headers(response)
            
            # Add rate limit headers
            if rate_limit_infos:
                main_info = rate_limit_infos[0]  # Use first applicable limit
                response.headers["X-RateLimit-Limit"] = str(main_info.limit)
                response.headers["X-RateLimit-Remaining"] = str(main_info.remaining)
                response.headers["X-RateLimit-Reset"] = str(int(main_info.reset_time.timestamp()))
            
            # Record metrics
            duration = time.time() - start_time
            api_request_duration.labels(
                method=request.method,
                endpoint=request.url.path
            ).observe(duration)
            
            api_requests_total.labels(
                method=request.method,
                endpoint=request.url.path,
                status=response.status_code
            ).inc()
            
            return response
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Security middleware error: {e}")
            api_blocked_requests.labels(reason="middleware_error").inc()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Security check failed"
            )
        finally:
            api_active_connections.dec()
    
    def _add_security_headers(self, response: Response):
        """Add security headers to response."""
        security_headers = {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "X-XSS-Protection": "1; mode=block",
            "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
            "Content-Security-Policy": "default-src 'self'",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "Permissions-Policy": "geolocation=(), microphone=(), camera=()"
        }
        
        for header, value in security_headers.items():
            response.headers[header] = value

# Global instance
_api_security_manager: Optional[APISecurityManager] = None

def initialize_api_security(redis_url: str = "redis://localhost:6379") -> APISecurityManager:
    """Initialize API security system."""
    global _api_security_manager
    
    _api_security_manager = APISecurityManager(redis_url)
    
    logger.info("API Security system initialized")
    return _api_security_manager

def get_api_security_manager() -> APISecurityManager:
    """Get API security manager instance."""
    if _api_security_manager is None:
        raise RuntimeError("API Security system not initialized")
    return _api_security_manager
