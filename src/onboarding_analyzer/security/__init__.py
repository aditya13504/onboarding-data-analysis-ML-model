"""
Security Module Initialization

Comprehensive security system initialization and configuration for the onboarding analytics platform.
Provides unified security management with authentication, authorization, encryption, and monitoring.
"""

import logging
from typing import Optional, Dict, List, Any
from datetime import timedelta

# Import all security components
from .authentication import (
    initialize_security as init_auth,
    get_auth_service,
    get_security_deps,
    UserRole,
    Permission
)

from .rbac import (
    initialize_rbac,
    get_rbac_manager,
    ResourceType,
    ActionType,
    PermissionScope
)

from .api_security import (
    initialize_api_security,
    get_api_security_manager,
    APISecurityMiddleware
)

from .encryption import (
    initialize_encryption,
    get_encryption_service,
    EncryptionAlgorithm,
    EncryptionLevel
)

from .audit_logging import (
    initialize_audit_system,
    get_audit_system,
    ComplianceStandard,
    AuditLevel
)

from .input_validation import (
    initialize_input_validation,
    get_input_validator,
    get_request_validator
)

from .security_headers import (
    initialize_security_headers,
    get_security_config_manager,
    SecurityLevel,
    SecurityHeadersMiddleware
)

logger = logging.getLogger(__name__)

class SecurityConfiguration:
    """Central security configuration."""
    
    def __init__(self):
        # Authentication settings
        self.jwt_secret_key: str = "your-secret-key-here"
        self.token_expire_minutes: int = 30
        self.refresh_token_expire_days: int = 7
        
        # Database settings
        self.database_url: str = "sqlite:///./security.db"
        self.redis_url: str = "redis://localhost:6379"
        self.elasticsearch_url: Optional[str] = None
        
        # Security levels
        self.security_level: SecurityLevel = SecurityLevel.STANDARD
        self.encryption_level: EncryptionLevel = EncryptionLevel.STANDARD
        self.audit_level: AuditLevel = AuditLevel.STANDARD
        
        # CORS settings
        self.allowed_origins: List[str] = ["http://localhost:3000", "https://localhost:3000"]
        self.allowed_methods: List[str] = ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
        
        # Compliance settings
        self.compliance_standards: List[ComplianceStandard] = [
            ComplianceStandard.SOC2,
            ComplianceStandard.GDPR
        ]
        
        # Custom CSP directives
        self.custom_csp_directives: Dict[str, List[str]] = {
            "connect-src": ["'self'", "https://api.analytics.com"],
            "img-src": ["'self'", "data:", "https:"],
            "font-src": ["'self'", "https://fonts.gstatic.com"]
        }
        
        # Encryption settings
        self.master_encryption_key: Optional[str] = None
        self.field_encryption_rules: Dict[str, str] = {
            "email": "fernet",
            "phone": "fernet", 
            "personal_data": "aes_256_gcm",
            "sensitive_analytics": "aes_256_gcm"
        }

class SecurityManager:
    """Unified security management."""
    
    def __init__(self, config: SecurityConfiguration):
        self.config = config
        self.is_initialized = False
        
        # Service references
        self.auth_service = None
        self.rbac_manager = None
        self.api_security_manager = None
        self.encryption_service = None
        self.audit_system = None
        self.input_validator = None
        self.security_config_manager = None
    
    async def initialize(self):
        """Initialize all security components."""
        if self.is_initialized:
            logger.warning("Security system already initialized")
            return
        
        try:
            logger.info("Initializing comprehensive security system...")
            
            # 1. Initialize Authentication System
            logger.info("Initializing authentication system...")
            self.auth_service = init_auth(
                secret_key=self.config.jwt_secret_key,
                database_url=self.config.database_url
            )
            
            # 2. Initialize RBAC System
            logger.info("Initializing RBAC system...")
            self.rbac_manager = initialize_rbac()
            
            # Setup default admin user
            await self._setup_default_users()
            
            # 3. Initialize API Security
            logger.info("Initializing API security system...")
            self.api_security_manager = initialize_api_security(
                redis_url=self.config.redis_url
            )
            
            # 4. Initialize Encryption
            logger.info("Initializing encryption system...")
            from .encryption import EncryptionConfig
            encryption_config = EncryptionConfig(
                default_algorithm=EncryptionAlgorithm.FERNET,
                key_rotation_interval=timedelta(days=30),
                encryption_level=self.config.encryption_level,
                field_encryption_rules=self.config.field_encryption_rules
            )
            
            self.encryption_service = initialize_encryption(
                master_key=self.config.master_encryption_key,
                config=encryption_config
            )
            
            # 5. Initialize Audit System
            logger.info("Initializing audit logging system...")
            self.audit_system = initialize_audit_system(
                database_url=self.config.database_url,
                elasticsearch_url=self.config.elasticsearch_url,
                encryption_service=self.encryption_service
            )
            
            # 6. Initialize Input Validation
            logger.info("Initializing input validation system...")
            self.input_validator = initialize_input_validation()
            
            # 7. Initialize Security Headers
            logger.info("Initializing security headers system...")
            self.security_config_manager = initialize_security_headers(
                security_level=self.config.security_level,
                custom_origins=self.config.allowed_origins,
                custom_csp_directives=self.config.custom_csp_directives
            )
            
            self.is_initialized = True
            logger.info("Security system initialization complete")
            
            # Log security initialization audit event
            await self._log_security_initialization()
            
        except Exception as e:
            logger.error(f"Security system initialization failed: {e}")
            raise
    
    async def _setup_default_users(self):
        """Setup default users and roles."""
        try:
            # Assign admin role to default admin user
            await self.rbac_manager.assign_role(
                user_id="admin_001",
                role=UserRole.ADMIN,
                assigned_by="system",
                metadata={"created_during_initialization": True}
            )
            
            # Assign analyst role to default analyst user
            await self.rbac_manager.assign_role(
                user_id="analyst_001", 
                role=UserRole.ANALYST,
                assigned_by="system",
                metadata={"created_during_initialization": True}
            )
            
            logger.info("Default users and roles configured")
            
        except Exception as e:
            logger.error(f"Failed to setup default users: {e}")
    
    async def _log_security_initialization(self):
        """Log security system initialization."""
        try:
            self.audit_system.log_audit_event(
                event_type="security_system_initialization",
                user_id="system",
                username="system",
                session_id=None,
                ip_address="127.0.0.1",
                user_agent="security_manager",
                resource="security_system",
                action="initialize",
                outcome="success",
                details={
                    "security_level": self.config.security_level.value,
                    "encryption_level": self.config.encryption_level.value,
                    "audit_level": self.config.audit_level.value,
                    "components_initialized": [
                        "authentication",
                        "rbac", 
                        "api_security",
                        "encryption",
                        "audit_logging",
                        "input_validation",
                        "security_headers"
                    ]
                },
                severity="info"
            )
            
        except Exception as e:
            logger.error(f"Failed to log security initialization: {e}")
    
    def get_middleware_stack(self):
        """Get FastAPI middleware stack for security."""
        if not self.is_initialized:
            raise RuntimeError("Security system not initialized")
        
        return [
            # Security headers middleware (outermost)
            (SecurityHeadersMiddleware, {"config_manager": self.security_config_manager}),
            # API security middleware  
            (APISecurityMiddleware, {"security_manager": self.api_security_manager})
        ]
    
    def get_auth_dependencies(self):
        """Get authentication dependencies for FastAPI."""
        if not self.is_initialized:
            raise RuntimeError("Security system not initialized")
        
        return get_security_deps()
    
    async def validate_request(self, request, endpoint_rules: Dict[str, str] = None):
        """Validate incoming request."""
        if endpoint_rules:
            request_validator = get_request_validator()
            request_validator.configure_endpoint(request.url.path, endpoint_rules)
            return await request_validator.validate_request(request)
        
        return {"valid": True, "errors": []}
    
    async def check_permission(self, user_id: str, username: str, 
                             resource_type: ResourceType, action: ActionType,
                             resource_id: str = None, 
                             user_attributes: Dict[str, Any] = None):
        """Check user permissions."""
        if not self.is_initialized:
            raise RuntimeError("Security system not initialized")
        
        return await self.rbac_manager.check_permission(
            user_id=user_id,
            username=username,
            resource_type=resource_type,
            resource_id=resource_id,
            action=action,
            user_attributes=user_attributes or {}
        )
    
    def encrypt_sensitive_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Encrypt sensitive data fields."""
        if not self.is_initialized:
            raise RuntimeError("Security system not initialized")
        
        return self.encryption_service.encrypt_sensitive_data(data)
    
    def decrypt_sensitive_data(self, encrypted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Decrypt sensitive data fields."""
        if not self.is_initialized:
            raise RuntimeError("Security system not initialized")
        
        return self.encryption_service.decrypt_sensitive_data(encrypted_data)
    
    async def log_audit_event(self, event_type: str, user_id: str, username: str,
                            resource: str, action: str, outcome: str,
                            details: Dict[str, Any], severity: str = "info"):
        """Log audit event."""
        if not self.is_initialized:
            raise RuntimeError("Security system not initialized")
        
        return self.audit_system.log_audit_event(
            event_type=event_type,
            user_id=user_id,
            username=username,
            session_id=None,
            ip_address="unknown",
            user_agent="unknown",
            resource=resource,
            action=action,
            outcome=outcome,
            details=details,
            severity=severity
        )
    
    def get_security_status(self) -> Dict[str, Any]:
        """Get comprehensive security system status."""
        if not self.is_initialized:
            return {"initialized": False}
        
        try:
            return {
                "initialized": True,
                "security_level": self.config.security_level.value,
                "encryption_level": self.config.encryption_level.value,
                "components": {
                    "authentication": self.auth_service is not None,
                    "rbac": self.rbac_manager is not None,
                    "api_security": self.api_security_manager is not None,
                    "encryption": self.encryption_service is not None,
                    "audit_logging": self.audit_system is not None,
                    "input_validation": self.input_validator is not None,
                    "security_headers": self.security_config_manager is not None
                },
                "encryption_status": self.encryption_service.get_encryption_status() if self.encryption_service else {},
                "compliance_standards": [std.value for std in self.config.compliance_standards]
            }
        except Exception as e:
            logger.error(f"Failed to get security status: {e}")
            return {"initialized": True, "error": str(e)}

# Global security manager instance
_security_manager: Optional[SecurityManager] = None

def initialize_security_system(config: Optional[SecurityConfiguration] = None) -> SecurityManager:
    """Initialize the complete security system."""
    global _security_manager
    
    if config is None:
        config = SecurityConfiguration()
    
    _security_manager = SecurityManager(config)
    
    logger.info("Security system manager created")
    return _security_manager

def get_security_manager() -> SecurityManager:
    """Get the security manager instance."""
    if _security_manager is None:
        raise RuntimeError("Security system not initialized. Call initialize_security_system() first.")
    return _security_manager

# Convenience functions for direct access to security components
def get_current_auth_service():
    """Get current authentication service."""
    return get_auth_service()

def get_current_rbac_manager():
    """Get current RBAC manager.""" 
    return get_rbac_manager()

def get_current_encryption_service():
    """Get current encryption service."""
    return get_encryption_service()

def get_current_audit_system():
    """Get current audit system."""
    return get_audit_system()

# Export all important classes and enums
__all__ = [
    # Main classes
    'SecurityConfiguration',
    'SecurityManager',
    'initialize_security_system',
    'get_security_manager',
    
    # Authentication
    'UserRole',
    'Permission',
    
    # RBAC
    'ResourceType', 
    'ActionType',
    'PermissionScope',
    
    # Encryption
    'EncryptionAlgorithm',
    'EncryptionLevel',
    
    # Audit
    'ComplianceStandard',
    'AuditLevel',
    
    # Security Headers
    'SecurityLevel',
    
    # Convenience functions
    'get_current_auth_service',
    'get_current_rbac_manager', 
    'get_current_encryption_service',
    'get_current_audit_system'
]
