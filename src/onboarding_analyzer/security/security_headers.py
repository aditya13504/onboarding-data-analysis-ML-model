"""
Security Headers and CORS Configuration System

Enterprise-grade security headers, CORS policy management, content security policy (CSP),
and comprehensive web security configuration for all HTTP responses.
"""

import logging
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Set, Union
from dataclasses import dataclass, field
from enum import Enum

# FastAPI and Starlette
from fastapi import Response, Request
from fastapi.middleware.base import BaseHTTPMiddleware
from starlette.middleware.base import BaseHTTPMiddleware as StarletteBaseMiddleware
from starlette.requests import Request as StarletteRequest
from starlette.responses import Response as StarletteResponse
from starlette.middleware.cors import CORSMiddleware

# Monitoring
from prometheus_client import Counter, Gauge

# Initialize metrics
security_headers_applied = Counter('security_headers_applied_total', 'Security headers applied', ['header_name'])
cors_requests_total = Counter('cors_requests_total', 'CORS requests', ['origin', 'method', 'result'])
csp_violations = Counter('csp_violations_total', 'CSP violations reported', ['directive'])
security_policy_violations = Counter('security_policy_violations_total', 'Security policy violations', ['policy_type'])

logger = logging.getLogger(__name__)

class SecurityLevel(Enum):
    """Security configuration levels."""
    MINIMAL = "minimal"
    STANDARD = "standard"
    STRICT = "strict"
    MAXIMUM = "maximum"

class FrameOptions(Enum):
    """X-Frame-Options values."""
    DENY = "DENY"
    SAMEORIGIN = "SAMEORIGIN"
    ALLOWFROM = "ALLOW-FROM"

class ContentTypeOptions(Enum):
    """X-Content-Type-Options values."""
    NOSNIFF = "nosniff"

class ReferrerPolicy(Enum):
    """Referrer-Policy values."""
    NO_REFERRER = "no-referrer"
    NO_REFERRER_WHEN_DOWNGRADE = "no-referrer-when-downgrade"
    ORIGIN = "origin"
    ORIGIN_WHEN_CROSS_ORIGIN = "origin-when-cross-origin"
    SAME_ORIGIN = "same-origin"
    STRICT_ORIGIN = "strict-origin"
    STRICT_ORIGIN_WHEN_CROSS_ORIGIN = "strict-origin-when-cross-origin"
    UNSAFE_URL = "unsafe-url"

class SameSitePolicy(Enum):
    """SameSite cookie attribute values."""
    STRICT = "Strict"
    LAX = "Lax"
    NONE = "None"

@dataclass
class SecurityHeadersConfig:
    """Security headers configuration."""
    # Content Security Policy
    enable_csp: bool = True
    csp_directives: Dict[str, List[str]] = field(default_factory=dict)
    csp_report_only: bool = False
    csp_report_uri: Optional[str] = None
    
    # X-Frame-Options
    frame_options: FrameOptions = FrameOptions.DENY
    frame_allow_from: Optional[str] = None
    
    # X-Content-Type-Options
    content_type_options: ContentTypeOptions = ContentTypeOptions.NOSNIFF
    
    # X-XSS-Protection
    enable_xss_protection: bool = True
    xss_protection_mode: str = "1; mode=block"
    
    # Strict-Transport-Security (HSTS)
    enable_hsts: bool = True
    hsts_max_age: int = 31536000  # 1 year
    hsts_include_subdomains: bool = True
    hsts_preload: bool = False
    
    # Referrer-Policy
    referrer_policy: ReferrerPolicy = ReferrerPolicy.STRICT_ORIGIN_WHEN_CROSS_ORIGIN
    
    # Permissions Policy (formerly Feature Policy)
    enable_permissions_policy: bool = True
    permissions_policy: Dict[str, str] = field(default_factory=dict)
    
    # Cross-Origin-Embedder-Policy
    enable_coep: bool = False
    coep_value: str = "require-corp"
    
    # Cross-Origin-Opener-Policy
    enable_coop: bool = False
    coop_value: str = "same-origin"
    
    # Cross-Origin-Resource-Policy
    enable_corp: bool = False
    corp_value: str = "same-origin"
    
    # Server header
    server_header: Optional[str] = None
    hide_server_header: bool = True
    
    # Additional custom headers
    custom_headers: Dict[str, str] = field(default_factory=dict)

@dataclass
class CORSConfig:
    """CORS configuration."""
    # Allowed origins
    allow_origins: List[str] = field(default_factory=list)
    allow_origin_regex: Optional[str] = None
    allow_all_origins: bool = False
    
    # Allowed methods
    allow_methods: List[str] = field(default_factory=lambda: ["GET"])
    
    # Allowed headers
    allow_headers: List[str] = field(default_factory=list)
    
    # Exposed headers
    expose_headers: List[str] = field(default_factory=list)
    
    # Credentials
    allow_credentials: bool = False
    
    # Preflight cache
    max_age: int = 600  # 10 minutes
    
    # Additional settings
    vary_header: bool = True

@dataclass
class CookieSecurityConfig:
    """Cookie security configuration."""
    secure: bool = True
    http_only: bool = True
    same_site: SameSitePolicy = SameSitePolicy.STRICT
    max_age: Optional[int] = None
    domain: Optional[str] = None
    path: str = "/"

class CSPBuilder:
    """Content Security Policy builder."""
    
    def __init__(self):
        self.directives: Dict[str, Set[str]] = {}
    
    def add_directive(self, directive: str, sources: Union[str, List[str]]):
        """Add CSP directive."""
        if directive not in self.directives:
            self.directives[directive] = set()
        
        if isinstance(sources, str):
            sources = [sources]
        
        for source in sources:
            self.directives[directive].add(source)
    
    def remove_directive(self, directive: str):
        """Remove CSP directive."""
        if directive in self.directives:
            del self.directives[directive]
    
    def build_policy(self) -> str:
        """Build CSP policy string."""
        policy_parts = []
        
        for directive, sources in self.directives.items():
            if sources:
                sources_str = " ".join(sorted(sources))
                policy_parts.append(f"{directive} {sources_str}")
            else:
                policy_parts.append(directive)
        
        return "; ".join(policy_parts)
    
    @classmethod
    def create_strict_policy(cls) -> 'CSPBuilder':
        """Create strict CSP policy."""
        builder = cls()
        
        # Default strict directives
        builder.add_directive("default-src", "'self'")
        builder.add_directive("script-src", ["'self'", "'unsafe-inline'"])
        builder.add_directive("style-src", ["'self'", "'unsafe-inline'"])
        builder.add_directive("img-src", ["'self'", "data:", "https:"])
        builder.add_directive("font-src", ["'self'", "https:"])
        builder.add_directive("connect-src", "'self'")
        builder.add_directive("frame-src", "'none'")
        builder.add_directive("object-src", "'none'")
        builder.add_directive("base-uri", "'self'")
        builder.add_directive("form-action", "'self'")
        builder.add_directive("frame-ancestors", "'none'")
        
        return builder
    
    @classmethod
    def create_permissive_policy(cls) -> 'CSPBuilder':
        """Create more permissive CSP policy."""
        builder = cls()
        
        # More permissive directives
        builder.add_directive("default-src", "'self'")
        builder.add_directive("script-src", ["'self'", "'unsafe-inline'", "'unsafe-eval'"])
        builder.add_directive("style-src", ["'self'", "'unsafe-inline'"])
        builder.add_directive("img-src", ["'self'", "data:", "https:", "http:"])
        builder.add_directive("font-src", ["'self'", "https:", "data:"])
        builder.add_directive("connect-src", ["'self'", "https:", "wss:"])
        builder.add_directive("frame-src", ["'self'", "https:"])
        builder.add_directive("object-src", "'self'")
        
        return builder

class SecurityHeadersManager:
    """Security headers management."""
    
    def __init__(self, config: SecurityHeadersConfig):
        self.config = config
        self.csp_builder = CSPBuilder()
        
        # Initialize CSP if enabled
        if config.enable_csp:
            self._initialize_csp()
    
    def _initialize_csp(self):
        """Initialize Content Security Policy."""
        if self.config.csp_directives:
            # Use custom directives
            for directive, sources in self.config.csp_directives.items():
                self.csp_builder.add_directive(directive, sources)
        else:
            # Use default strict policy
            self.csp_builder = CSPBuilder.create_strict_policy()
    
    def build_headers(self, request: Request, response: Response) -> Dict[str, str]:
        """Build security headers for response."""
        headers = {}
        
        # Content Security Policy
        if self.config.enable_csp:
            csp_header = "Content-Security-Policy-Report-Only" if self.config.csp_report_only else "Content-Security-Policy"
            csp_value = self.csp_builder.build_policy()
            
            if self.config.csp_report_uri:
                csp_value += f"; report-uri {self.config.csp_report_uri}"
            
            headers[csp_header] = csp_value
            security_headers_applied.labels(header_name=csp_header).inc()
        
        # X-Frame-Options
        if self.config.frame_options == FrameOptions.ALLOWFROM and self.config.frame_allow_from:
            headers["X-Frame-Options"] = f"{self.config.frame_options.value} {self.config.frame_allow_from}"
        else:
            headers["X-Frame-Options"] = self.config.frame_options.value
        security_headers_applied.labels(header_name="X-Frame-Options").inc()
        
        # X-Content-Type-Options
        headers["X-Content-Type-Options"] = self.config.content_type_options.value
        security_headers_applied.labels(header_name="X-Content-Type-Options").inc()
        
        # X-XSS-Protection
        if self.config.enable_xss_protection:
            headers["X-XSS-Protection"] = self.config.xss_protection_mode
            security_headers_applied.labels(header_name="X-XSS-Protection").inc()
        
        # Strict-Transport-Security
        if self.config.enable_hsts and request.url.scheme == "https":
            hsts_value = f"max-age={self.config.hsts_max_age}"
            if self.config.hsts_include_subdomains:
                hsts_value += "; includeSubDomains"
            if self.config.hsts_preload:
                hsts_value += "; preload"
            headers["Strict-Transport-Security"] = hsts_value
            security_headers_applied.labels(header_name="Strict-Transport-Security").inc()
        
        # Referrer-Policy
        headers["Referrer-Policy"] = self.config.referrer_policy.value
        security_headers_applied.labels(header_name="Referrer-Policy").inc()
        
        # Permissions Policy
        if self.config.enable_permissions_policy:
            if self.config.permissions_policy:
                policy_parts = []
                for feature, allowlist in self.config.permissions_policy.items():
                    policy_parts.append(f"{feature}=({allowlist})")
                headers["Permissions-Policy"] = ", ".join(policy_parts)
            else:
                # Default restrictive policy
                default_policy = [
                    "geolocation=()",
                    "microphone=()",
                    "camera=()",
                    "payment=()",
                    "usb=()",
                    "magnetometer=()",
                    "gyroscope=()",
                    "accelerometer=()"
                ]
                headers["Permissions-Policy"] = ", ".join(default_policy)
            security_headers_applied.labels(header_name="Permissions-Policy").inc()
        
        # Cross-Origin-Embedder-Policy
        if self.config.enable_coep:
            headers["Cross-Origin-Embedder-Policy"] = self.config.coep_value
            security_headers_applied.labels(header_name="Cross-Origin-Embedder-Policy").inc()
        
        # Cross-Origin-Opener-Policy
        if self.config.enable_coop:
            headers["Cross-Origin-Opener-Policy"] = self.config.coop_value
            security_headers_applied.labels(header_name="Cross-Origin-Opener-Policy").inc()
        
        # Cross-Origin-Resource-Policy
        if self.config.enable_corp:
            headers["Cross-Origin-Resource-Policy"] = self.config.corp_value
            security_headers_applied.labels(header_name="Cross-Origin-Resource-Policy").inc()
        
        # Server header
        if self.config.hide_server_header:
            headers["Server"] = ""
        elif self.config.server_header:
            headers["Server"] = self.config.server_header
        
        # Custom headers
        headers.update(self.config.custom_headers)
        for header_name in self.config.custom_headers:
            security_headers_applied.labels(header_name=header_name).inc()
        
        return headers
    
    def add_csp_directive(self, directive: str, sources: Union[str, List[str]]):
        """Add CSP directive."""
        self.csp_builder.add_directive(directive, sources)
    
    def remove_csp_directive(self, directive: str):
        """Remove CSP directive."""
        self.csp_builder.remove_directive(directive)

class CORSManager:
    """CORS policy management."""
    
    def __init__(self, config: CORSConfig):
        self.config = config
        self.origin_cache: Dict[str, bool] = {}
        self.cache_ttl = timedelta(hours=1)
        self.last_cache_cleanup = datetime.now(timezone.utc)
    
    def is_origin_allowed(self, origin: str) -> bool:
        """Check if origin is allowed."""
        if self.config.allow_all_origins:
            return True
        
        # Check cache first
        current_time = datetime.now(timezone.utc)
        if origin in self.origin_cache:
            return self.origin_cache[origin]
        
        # Check allowed origins
        if origin in self.config.allow_origins:
            self.origin_cache[origin] = True
            return True
        
        # Check origin regex
        if self.config.allow_origin_regex:
            import re
            try:
                if re.match(self.config.allow_origin_regex, origin):
                    self.origin_cache[origin] = True
                    return True
            except re.error:
                logger.error(f"Invalid origin regex: {self.config.allow_origin_regex}")
        
        self.origin_cache[origin] = False
        
        # Cleanup cache periodically
        if current_time - self.last_cache_cleanup > self.cache_ttl:
            self._cleanup_cache()
            self.last_cache_cleanup = current_time
        
        return False
    
    def _cleanup_cache(self):
        """Clean up origin cache."""
        # Simple cleanup - remove all entries
        # In production, you might want more sophisticated cache management
        self.origin_cache.clear()
    
    def build_cors_headers(self, request: Request, origin: str) -> Dict[str, str]:
        """Build CORS headers."""
        headers = {}
        
        if not self.is_origin_allowed(origin):
            cors_requests_total.labels(origin=origin, method=request.method, result='denied').inc()
            return headers
        
        # Access-Control-Allow-Origin
        if self.config.allow_all_origins:
            headers["Access-Control-Allow-Origin"] = "*"
        else:
            headers["Access-Control-Allow-Origin"] = origin
        
        # Access-Control-Allow-Credentials
        if self.config.allow_credentials:
            headers["Access-Control-Allow-Credentials"] = "true"
        
        # Access-Control-Allow-Methods
        if self.config.allow_methods:
            headers["Access-Control-Allow-Methods"] = ", ".join(self.config.allow_methods)
        
        # Access-Control-Allow-Headers
        if self.config.allow_headers:
            headers["Access-Control-Allow-Headers"] = ", ".join(self.config.allow_headers)
        
        # Access-Control-Expose-Headers
        if self.config.expose_headers:
            headers["Access-Control-Expose-Headers"] = ", ".join(self.config.expose_headers)
        
        # Access-Control-Max-Age (for preflight requests)
        if request.method == "OPTIONS":
            headers["Access-Control-Max-Age"] = str(self.config.max_age)
        
        # Vary header
        if self.config.vary_header and not self.config.allow_all_origins:
            headers["Vary"] = "Origin"
        
        cors_requests_total.labels(origin=origin, method=request.method, result='allowed').inc()
        
        return headers

class SecurityConfigurationManager:
    """Main security configuration manager."""
    
    def __init__(self, security_level: SecurityLevel = SecurityLevel.STANDARD):
        self.security_level = security_level
        self.headers_config = self._create_headers_config()
        self.cors_config = self._create_cors_config()
        self.cookie_config = self._create_cookie_config()
        
        self.headers_manager = SecurityHeadersManager(self.headers_config)
        self.cors_manager = CORSManager(self.cors_config)
    
    def _create_headers_config(self) -> SecurityHeadersConfig:
        """Create security headers configuration based on security level."""
        if self.security_level == SecurityLevel.MINIMAL:
            return SecurityHeadersConfig(
                enable_csp=False,
                enable_hsts=False,
                enable_xss_protection=True,
                enable_permissions_policy=False
            )
        elif self.security_level == SecurityLevel.STANDARD:
            return SecurityHeadersConfig(
                enable_csp=True,
                csp_report_only=False,
                enable_hsts=True,
                hsts_max_age=31536000,
                enable_permissions_policy=True
            )
        elif self.security_level == SecurityLevel.STRICT:
            return SecurityHeadersConfig(
                enable_csp=True,
                csp_report_only=False,
                frame_options=FrameOptions.DENY,
                enable_hsts=True,
                hsts_max_age=63072000,  # 2 years
                hsts_include_subdomains=True,
                hsts_preload=True,
                enable_permissions_policy=True,
                enable_coep=True,
                enable_coop=True,
                enable_corp=True
            )
        else:  # MAXIMUM
            return SecurityHeadersConfig(
                enable_csp=True,
                csp_report_only=False,
                frame_options=FrameOptions.DENY,
                enable_hsts=True,
                hsts_max_age=63072000,
                hsts_include_subdomains=True,
                hsts_preload=True,
                enable_permissions_policy=True,
                enable_coep=True,
                enable_coop=True,
                enable_corp=True,
                hide_server_header=True
            )
    
    def _create_cors_config(self) -> CORSConfig:
        """Create CORS configuration based on security level."""
        if self.security_level in [SecurityLevel.MINIMAL, SecurityLevel.STANDARD]:
            return CORSConfig(
                allow_origins=["https://localhost:3000"],
                allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
                allow_headers=["Content-Type", "Authorization"],
                allow_credentials=True,
                max_age=600
            )
        else:  # STRICT or MAXIMUM
            return CORSConfig(
                allow_origins=["https://yourdomain.com"],
                allow_methods=["GET", "POST"],
                allow_headers=["Content-Type", "Authorization"],
                allow_credentials=False,
                max_age=300
            )
    
    def _create_cookie_config(self) -> CookieSecurityConfig:
        """Create cookie security configuration."""
        if self.security_level == SecurityLevel.MINIMAL:
            return CookieSecurityConfig(
                secure=False,
                http_only=True,
                same_site=SameSitePolicy.LAX
            )
        elif self.security_level == SecurityLevel.STANDARD:
            return CookieSecurityConfig(
                secure=True,
                http_only=True,
                same_site=SameSitePolicy.LAX
            )
        else:  # STRICT or MAXIMUM
            return CookieSecurityConfig(
                secure=True,
                http_only=True,
                same_site=SameSitePolicy.STRICT
            )

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware for security headers."""
    
    def __init__(self, app, config_manager: SecurityConfigurationManager):
        super().__init__(app)
        self.config_manager = config_manager
    
    async def dispatch(self, request: Request, call_next) -> Response:
        """Add security headers to response."""
        response = await call_next(request)
        
        try:
            # Build security headers
            security_headers = self.config_manager.headers_manager.build_headers(request, response)
            
            # Build CORS headers if needed
            origin = request.headers.get("origin")
            if origin:
                cors_headers = self.config_manager.cors_manager.build_cors_headers(request, origin)
                security_headers.update(cors_headers)
            
            # Apply headers to response
            for header_name, header_value in security_headers.items():
                if header_value:  # Only set non-empty headers
                    response.headers[header_name] = header_value
            
            # Handle preflight OPTIONS requests
            if request.method == "OPTIONS" and origin:
                response.status_code = 200
                return response
            
        except Exception as e:
            logger.error(f"Security headers middleware error: {e}")
        
        return response

def create_security_configuration(security_level: SecurityLevel = SecurityLevel.STANDARD,
                                custom_origins: List[str] = None,
                                custom_csp_directives: Dict[str, List[str]] = None) -> SecurityConfigurationManager:
    """Create security configuration with customizations."""
    config_manager = SecurityConfigurationManager(security_level)
    
    # Apply custom origins
    if custom_origins:
        config_manager.cors_config.allow_origins = custom_origins
    
    # Apply custom CSP directives
    if custom_csp_directives:
        for directive, sources in custom_csp_directives.items():
            config_manager.headers_manager.add_csp_directive(directive, sources)
    
    return config_manager

# Preset configurations
def create_api_security_config() -> SecurityConfigurationManager:
    """Create security configuration optimized for API services."""
    config_manager = SecurityConfigurationManager(SecurityLevel.STRICT)
    
    # API-specific CSP
    config_manager.headers_manager.csp_builder = CSPBuilder()
    config_manager.headers_manager.csp_builder.add_directive("default-src", "'none'")
    config_manager.headers_manager.csp_builder.add_directive("frame-ancestors", "'none'")
    
    # API-friendly CORS
    config_manager.cors_config.allow_methods = ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"]
    config_manager.cors_config.allow_headers = [
        "Content-Type", "Authorization", "X-Requested-With", "Accept", "X-API-Key"
    ]
    
    return config_manager

def create_web_app_security_config() -> SecurityConfigurationManager:
    """Create security configuration optimized for web applications."""
    config_manager = SecurityConfigurationManager(SecurityLevel.STANDARD)
    
    # Web app CSP
    config_manager.headers_manager.csp_builder = CSPBuilder.create_permissive_policy()
    
    # Add common web app sources
    config_manager.headers_manager.add_csp_directive("script-src", [
        "'self'", "https://cdn.jsdelivr.net", "https://unpkg.com"
    ])
    config_manager.headers_manager.add_csp_directive("style-src", [
        "'self'", "'unsafe-inline'", "https://fonts.googleapis.com"
    ])
    config_manager.headers_manager.add_csp_directive("font-src", [
        "'self'", "https://fonts.gstatic.com"
    ])
    
    return config_manager

# Global instance
_security_config_manager: Optional[SecurityConfigurationManager] = None

def initialize_security_headers(security_level: SecurityLevel = SecurityLevel.STANDARD,
                               custom_origins: List[str] = None,
                               custom_csp_directives: Dict[str, List[str]] = None) -> SecurityConfigurationManager:
    """Initialize security headers system."""
    global _security_config_manager
    
    _security_config_manager = create_security_configuration(
        security_level, custom_origins, custom_csp_directives
    )
    
    logger.info(f"Security headers system initialized with {security_level.value} level")
    return _security_config_manager

def get_security_config_manager() -> SecurityConfigurationManager:
    """Get security configuration manager instance."""
    if _security_config_manager is None:
        raise RuntimeError("Security headers system not initialized")
    return _security_config_manager
