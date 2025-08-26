"""
Advanced Authentication and Authorization System

Enterprise-grade authentication system supporting JWT, OAuth2, and role-based access control.
Provides comprehensive security for the analytics platform with audit logging and session management.
"""

import asyncio
import logging
import hashlib
import secrets
import hmac
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Union, Set
from dataclasses import dataclass, field
from enum import Enum
import json
import base64
from pathlib import Path

# Cryptography and JWT
import jwt
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import bcrypt

# FastAPI security
from fastapi import HTTPException, Depends, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from passlib.context import CryptContext

# Database
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# Monitoring
from prometheus_client import Counter, Histogram, Gauge

# Initialize metrics
auth_attempts_total = Counter('auth_attempts_total', 'Authentication attempts', ['method', 'result'])
auth_failures_total = Counter('auth_failures_total', 'Authentication failures', ['reason'])
active_sessions = Gauge('active_sessions_total', 'Active user sessions')
security_events = Counter('security_events_total', 'Security events', ['event_type', 'severity'])

logger = logging.getLogger(__name__)

# Security configuration
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 7
PASSWORD_MIN_LENGTH = 12
MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_DURATION_MINUTES = 30

class UserRole(Enum):
    """User roles for RBAC."""
    ADMIN = "admin"
    ANALYST = "analyst"
    VIEWER = "viewer"
    MANAGER = "manager"
    AUDITOR = "auditor"

class Permission(Enum):
    """System permissions."""
    READ_ANALYTICS = "read_analytics"
    WRITE_ANALYTICS = "write_analytics"
    READ_REPORTS = "read_reports"
    CREATE_REPORTS = "create_reports"
    MANAGE_USERS = "manage_users"
    MANAGE_SYSTEM = "manage_system"
    VIEW_AUDIT_LOGS = "view_audit_logs"
    EXPORT_DATA = "export_data"
    MANAGE_DASHBOARDS = "manage_dashboards"
    READ_SENSITIVE_DATA = "read_sensitive_data"

class SecurityEventType(Enum):
    """Types of security events."""
    LOGIN_SUCCESS = "login_success"
    LOGIN_FAILURE = "login_failure"
    LOGOUT = "logout"
    PASSWORD_CHANGE = "password_change"
    PERMISSION_DENIED = "permission_denied"
    ACCOUNT_LOCKED = "account_locked"
    ACCOUNT_UNLOCKED = "account_unlocked"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"
    TOKEN_REFRESH = "token_refresh"
    DATA_ACCESS = "data_access"

@dataclass
class TokenPayload:
    """JWT token payload structure."""
    user_id: str
    username: str
    email: str
    roles: List[str]
    permissions: List[str]
    session_id: str
    issued_at: datetime
    expires_at: datetime
    token_type: str = "access"
    additional_claims: Dict[str, Any] = field(default_factory=dict)

@dataclass
class SecurityEvent:
    """Security event for audit logging."""
    event_id: str
    event_type: SecurityEventType
    user_id: Optional[str]
    username: Optional[str]
    ip_address: str
    user_agent: str
    timestamp: datetime
    details: Dict[str, Any]
    severity: str = "info"  # info, warning, critical
    resource: Optional[str] = None

@dataclass
class UserSession:
    """Active user session."""
    session_id: str
    user_id: str
    username: str
    roles: List[str]
    ip_address: str
    user_agent: str
    created_at: datetime
    last_activity: datetime
    expires_at: datetime
    is_active: bool = True
    additional_data: Dict[str, Any] = field(default_factory=dict)

# Role-based permissions mapping
ROLE_PERMISSIONS = {
    UserRole.ADMIN: [
        Permission.READ_ANALYTICS, Permission.WRITE_ANALYTICS,
        Permission.READ_REPORTS, Permission.CREATE_REPORTS,
        Permission.MANAGE_USERS, Permission.MANAGE_SYSTEM,
        Permission.VIEW_AUDIT_LOGS, Permission.EXPORT_DATA,
        Permission.MANAGE_DASHBOARDS, Permission.READ_SENSITIVE_DATA
    ],
    UserRole.ANALYST: [
        Permission.READ_ANALYTICS, Permission.WRITE_ANALYTICS,
        Permission.READ_REPORTS, Permission.CREATE_REPORTS,
        Permission.EXPORT_DATA, Permission.MANAGE_DASHBOARDS
    ],
    UserRole.MANAGER: [
        Permission.READ_ANALYTICS, Permission.READ_REPORTS,
        Permission.CREATE_REPORTS, Permission.EXPORT_DATA,
        Permission.MANAGE_DASHBOARDS, Permission.READ_SENSITIVE_DATA
    ],
    UserRole.VIEWER: [
        Permission.READ_ANALYTICS, Permission.READ_REPORTS
    ],
    UserRole.AUDITOR: [
        Permission.READ_ANALYTICS, Permission.READ_REPORTS,
        Permission.VIEW_AUDIT_LOGS, Permission.EXPORT_DATA
    ]
}

class CryptographyManager:
    """Advanced cryptography manager for data protection."""
    
    def __init__(self, encryption_key: Optional[str] = None):
        self.encryption_key = encryption_key or Fernet.generate_key()
        self.fernet = Fernet(self.encryption_key)
        self.password_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        
        # Generate RSA key pair for advanced encryption
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        self.public_key = self.private_key.public_key()
    
    def encrypt_data(self, data: str) -> str:
        """Encrypt sensitive data."""
        try:
            encrypted_data = self.fernet.encrypt(data.encode())
            return base64.b64encode(encrypted_data).decode()
        except Exception as e:
            logger.error(f"Data encryption failed: {e}")
            raise
    
    def decrypt_data(self, encrypted_data: str) -> str:
        """Decrypt sensitive data."""
        try:
            decoded_data = base64.b64decode(encrypted_data.encode())
            decrypted_data = self.fernet.decrypt(decoded_data)
            return decrypted_data.decode()
        except Exception as e:
            logger.error(f"Data decryption failed: {e}")
            raise
    
    def hash_password(self, password: str) -> str:
        """Hash password using bcrypt."""
        return self.password_context.hash(password)
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify password against hash."""
        return self.password_context.verify(plain_password, hashed_password)
    
    def generate_secure_token(self, length: int = 32) -> str:
        """Generate cryptographically secure random token."""
        return secrets.token_urlsafe(length)
    
    def derive_key(self, password: str, salt: bytes) -> bytes:
        """Derive encryption key from password."""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        return kdf.derive(password.encode())
    
    def rsa_encrypt(self, data: str) -> str:
        """RSA encrypt for key exchange."""
        encrypted = self.public_key.encrypt(
            data.encode(),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return base64.b64encode(encrypted).decode()
    
    def rsa_decrypt(self, encrypted_data: str) -> str:
        """RSA decrypt for key exchange."""
        decoded_data = base64.b64decode(encrypted_data.encode())
        decrypted = self.private_key.decrypt(
            decoded_data,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return decrypted.decode()

class JWTManager:
    """Advanced JWT token management."""
    
    def __init__(self, secret_key: str, algorithm: str = ALGORITHM):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.crypto_manager = CryptographyManager()
    
    def create_access_token(self, payload: TokenPayload) -> str:
        """Create JWT access token."""
        try:
            # Prepare claims
            claims = {
                "sub": payload.user_id,
                "username": payload.username,
                "email": payload.email,
                "roles": payload.roles,
                "permissions": payload.permissions,
                "session_id": payload.session_id,
                "iat": payload.issued_at.timestamp(),
                "exp": payload.expires_at.timestamp(),
                "type": payload.token_type,
                **payload.additional_claims
            }
            
            # Create token
            token = jwt.encode(claims, self.secret_key, algorithm=self.algorithm)
            
            logger.info(f"Access token created for user: {payload.username}")
            return token
            
        except Exception as e:
            logger.error(f"Token creation failed: {e}")
            raise
    
    def create_refresh_token(self, user_id: str, session_id: str) -> str:
        """Create refresh token."""
        try:
            payload = {
                "sub": user_id,
                "session_id": session_id,
                "type": "refresh",
                "iat": datetime.now(timezone.utc).timestamp(),
                "exp": (datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)).timestamp()
            }
            
            token = jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
            
            logger.info(f"Refresh token created for user: {user_id}")
            return token
            
        except Exception as e:
            logger.error(f"Refresh token creation failed: {e}")
            raise
    
    def verify_token(self, token: str) -> Dict[str, Any]:
        """Verify and decode JWT token."""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            # Check expiration
            if datetime.fromtimestamp(payload['exp'], timezone.utc) < datetime.now(timezone.utc):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Token expired"
                )
            
            return payload
            
        except jwt.ExpiredSignatureError:
            logger.warning("Token verification failed: expired")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token expired"
            )
        except jwt.InvalidTokenError as e:
            logger.warning(f"Token verification failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token"
            )
    
    def refresh_access_token(self, refresh_token: str, session_manager: 'SessionManager') -> Dict[str, str]:
        """Refresh access token using refresh token."""
        try:
            # Verify refresh token
            payload = self.verify_token(refresh_token)
            
            if payload.get('type') != 'refresh':
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid token type"
                )
            
            # Get session
            session_id = payload.get('session_id')
            user_session = session_manager.get_session(session_id)
            
            if not user_session or not user_session.is_active:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid session"
                )
            
            # Create new access token
            new_payload = TokenPayload(
                user_id=user_session.user_id,
                username=user_session.username,
                email="",  # Would be loaded from user data
                roles=user_session.roles,
                permissions=self._get_permissions_for_roles(user_session.roles),
                session_id=session_id,
                issued_at=datetime.now(timezone.utc),
                expires_at=datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
            )
            
            access_token = self.create_access_token(new_payload)
            
            # Update session activity
            session_manager.update_session_activity(session_id)
            
            security_events.labels(event_type='token_refresh', severity='info').inc()
            
            return {
                "access_token": access_token,
                "token_type": "bearer",
                "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60
            }
            
        except Exception as e:
            logger.error(f"Token refresh failed: {e}")
            raise
    
    def _get_permissions_for_roles(self, roles: List[str]) -> List[str]:
        """Get permissions for given roles."""
        permissions = set()
        for role_str in roles:
            try:
                role = UserRole(role_str)
                role_perms = ROLE_PERMISSIONS.get(role, [])
                permissions.update([perm.value for perm in role_perms])
            except ValueError:
                logger.warning(f"Unknown role: {role_str}")
        
        return list(permissions)

class SessionManager:
    """Session management for user authentication."""
    
    def __init__(self):
        self.active_sessions: Dict[str, UserSession] = {}
        self.user_sessions: Dict[str, Set[str]] = {}  # user_id -> session_ids
        self.session_lock = asyncio.Lock()
    
    async def create_session(self, user_id: str, username: str, roles: List[str],
                           ip_address: str, user_agent: str) -> UserSession:
        """Create new user session."""
        async with self.session_lock:
            session_id = secrets.token_urlsafe(32)
            
            session = UserSession(
                session_id=session_id,
                user_id=user_id,
                username=username,
                roles=roles,
                ip_address=ip_address,
                user_agent=user_agent,
                created_at=datetime.now(timezone.utc),
                last_activity=datetime.now(timezone.utc),
                expires_at=datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
            )
            
            self.active_sessions[session_id] = session
            
            if user_id not in self.user_sessions:
                self.user_sessions[user_id] = set()
            self.user_sessions[user_id].add(session_id)
            
            active_sessions.inc()
            
            logger.info(f"Session created for user: {username} ({session_id})")
            return session
    
    def get_session(self, session_id: str) -> Optional[UserSession]:
        """Get session by ID."""
        return self.active_sessions.get(session_id)
    
    def update_session_activity(self, session_id: str):
        """Update session last activity."""
        if session_id in self.active_sessions:
            self.active_sessions[session_id].last_activity = datetime.now(timezone.utc)
    
    async def terminate_session(self, session_id: str) -> bool:
        """Terminate specific session."""
        async with self.session_lock:
            if session_id in self.active_sessions:
                session = self.active_sessions[session_id]
                session.is_active = False
                
                # Remove from user sessions
                if session.user_id in self.user_sessions:
                    self.user_sessions[session.user_id].discard(session_id)
                
                del self.active_sessions[session_id]
                active_sessions.dec()
                
                logger.info(f"Session terminated: {session_id}")
                return True
            
            return False
    
    async def terminate_user_sessions(self, user_id: str) -> int:
        """Terminate all sessions for a user."""
        async with self.session_lock:
            if user_id not in self.user_sessions:
                return 0
            
            session_ids = self.user_sessions[user_id].copy()
            terminated_count = 0
            
            for session_id in session_ids:
                if await self.terminate_session(session_id):
                    terminated_count += 1
            
            logger.info(f"Terminated {terminated_count} sessions for user: {user_id}")
            return terminated_count
    
    async def cleanup_expired_sessions(self):
        """Clean up expired sessions."""
        async with self.session_lock:
            current_time = datetime.now(timezone.utc)
            expired_sessions = []
            
            for session_id, session in self.active_sessions.items():
                if current_time > session.expires_at:
                    expired_sessions.append(session_id)
            
            for session_id in expired_sessions:
                await self.terminate_session(session_id)
            
            if expired_sessions:
                logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")
    
    def get_user_sessions(self, user_id: str) -> List[UserSession]:
        """Get all active sessions for a user."""
        if user_id not in self.user_sessions:
            return []
        
        sessions = []
        for session_id in self.user_sessions[user_id]:
            if session_id in self.active_sessions:
                sessions.append(self.active_sessions[session_id])
        
        return sessions

class SecurityAuditLogger:
    """Security event audit logging."""
    
    def __init__(self, log_file_path: str = "security_audit.log"):
        self.log_file_path = Path(log_file_path)
        self.log_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Setup dedicated security logger
        self.security_logger = logging.getLogger("security_audit")
        self.security_logger.setLevel(logging.INFO)
        
        # File handler for security events
        file_handler = logging.FileHandler(self.log_file_path)
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - SECURITY - %(message)s'
        )
        file_handler.setFormatter(formatter)
        self.security_logger.addHandler(file_handler)
    
    def log_security_event(self, event: SecurityEvent):
        """Log security event."""
        try:
            # Update metrics
            security_events.labels(
                event_type=event.event_type.value,
                severity=event.severity
            ).inc()
            
            # Prepare log message
            log_data = {
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "user_id": event.user_id,
                "username": event.username,
                "ip_address": event.ip_address,
                "user_agent": event.user_agent,
                "timestamp": event.timestamp.isoformat(),
                "severity": event.severity,
                "resource": event.resource,
                "details": event.details
            }
            
            log_message = json.dumps(log_data)
            
            # Log based on severity
            if event.severity == "critical":
                self.security_logger.critical(log_message)
            elif event.severity == "warning":
                self.security_logger.warning(log_message)
            else:
                self.security_logger.info(log_message)
            
        except Exception as e:
            logger.error(f"Security event logging failed: {e}")
    
    def log_authentication_attempt(self, username: str, ip_address: str,
                                 user_agent: str, success: bool, reason: str = ""):
        """Log authentication attempt."""
        event = SecurityEvent(
            event_id=secrets.token_urlsafe(16),
            event_type=SecurityEventType.LOGIN_SUCCESS if success else SecurityEventType.LOGIN_FAILURE,
            user_id=None,
            username=username,
            ip_address=ip_address,
            user_agent=user_agent,
            timestamp=datetime.now(timezone.utc),
            details={"reason": reason} if reason else {},
            severity="info" if success else "warning"
        )
        
        self.log_security_event(event)
    
    def log_permission_denied(self, user_id: str, username: str, ip_address: str,
                            resource: str, required_permission: str):
        """Log permission denied event."""
        event = SecurityEvent(
            event_id=secrets.token_urlsafe(16),
            event_type=SecurityEventType.PERMISSION_DENIED,
            user_id=user_id,
            username=username,
            ip_address=ip_address,
            user_agent="",
            timestamp=datetime.now(timezone.utc),
            details={
                "resource": resource,
                "required_permission": required_permission
            },
            severity="warning",
            resource=resource
        )
        
        self.log_security_event(event)
    
    def log_data_access(self, user_id: str, username: str, ip_address: str,
                       resource: str, action: str):
        """Log data access event."""
        event = SecurityEvent(
            event_id=secrets.token_urlsafe(16),
            event_type=SecurityEventType.DATA_ACCESS,
            user_id=user_id,
            username=username,
            ip_address=ip_address,
            user_agent="",
            timestamp=datetime.now(timezone.utc),
            details={
                "resource": resource,
                "action": action
            },
            severity="info",
            resource=resource
        )
        
        self.log_security_event(event)

class AuthenticationService:
    """Main authentication service."""
    
    def __init__(self, secret_key: str, database_url: str = ""):
        self.secret_key = secret_key
        self.jwt_manager = JWTManager(secret_key)
        self.session_manager = SessionManager()
        self.crypto_manager = CryptographyManager()
        self.audit_logger = SecurityAuditLogger()
        
        # User lockout tracking
        self.failed_attempts: Dict[str, List[datetime]] = {}
        self.locked_accounts: Dict[str, datetime] = {}
        
        # Initialize cleanup task
        self._start_cleanup_task()
    
    def _start_cleanup_task(self):
        """Start background cleanup task."""
        async def cleanup_loop():
            while True:
                try:
                    await self.session_manager.cleanup_expired_sessions()
                    await asyncio.sleep(3600)  # Clean up every hour
                except Exception as e:
                    logger.error(f"Cleanup task error: {e}")
                    await asyncio.sleep(300)  # Retry after 5 minutes
        
        asyncio.create_task(cleanup_loop())
    
    async def authenticate_user(self, username: str, password: str, 
                              ip_address: str, user_agent: str) -> Dict[str, Any]:
        """Authenticate user and create session."""
        try:
            # Check if account is locked
            if self._is_account_locked(username):
                self.audit_logger.log_authentication_attempt(
                    username, ip_address, user_agent, False, "Account locked"
                )
                auth_failures_total.labels(reason='account_locked').inc()
                raise HTTPException(
                    status_code=status.HTTP_423_LOCKED,
                    detail="Account is locked due to too many failed attempts"
                )
            
            # This would typically verify against database
            # For now, implementing a simplified user verification
            user_data = await self._verify_user_credentials(username, password)
            
            if not user_data:
                self._record_failed_attempt(username)
                self.audit_logger.log_authentication_attempt(
                    username, ip_address, user_agent, False, "Invalid credentials"
                )
                auth_attempts_total.labels(method='password', result='failure').inc()
                auth_failures_total.labels(reason='invalid_credentials').inc()
                
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid username or password"
                )
            
            # Clear failed attempts on successful login
            self._clear_failed_attempts(username)
            
            # Create session
            session = await self.session_manager.create_session(
                user_id=user_data['user_id'],
                username=username,
                roles=user_data['roles'],
                ip_address=ip_address,
                user_agent=user_agent
            )
            
            # Create tokens
            permissions = self.jwt_manager._get_permissions_for_roles(user_data['roles'])
            
            access_payload = TokenPayload(
                user_id=user_data['user_id'],
                username=username,
                email=user_data.get('email', ''),
                roles=user_data['roles'],
                permissions=permissions,
                session_id=session.session_id,
                issued_at=datetime.now(timezone.utc),
                expires_at=datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
            )
            
            access_token = self.jwt_manager.create_access_token(access_payload)
            refresh_token = self.jwt_manager.create_refresh_token(
                user_data['user_id'], session.session_id
            )
            
            # Log successful authentication
            self.audit_logger.log_authentication_attempt(
                username, ip_address, user_agent, True
            )
            auth_attempts_total.labels(method='password', result='success').inc()
            
            return {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "token_type": "bearer",
                "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
                "user": {
                    "user_id": user_data['user_id'],
                    "username": username,
                    "email": user_data.get('email', ''),
                    "roles": user_data['roles'],
                    "permissions": permissions
                },
                "session_id": session.session_id
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            auth_failures_total.labels(reason='system_error').inc()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Authentication service error"
            )
    
    async def logout_user(self, session_id: str, user_id: str, username: str):
        """Logout user and terminate session."""
        try:
            await self.session_manager.terminate_session(session_id)
            
            # Log logout
            event = SecurityEvent(
                event_id=secrets.token_urlsafe(16),
                event_type=SecurityEventType.LOGOUT,
                user_id=user_id,
                username=username,
                ip_address="",
                user_agent="",
                timestamp=datetime.now(timezone.utc),
                details={"session_id": session_id},
                severity="info"
            )
            
            self.audit_logger.log_security_event(event)
            
        except Exception as e:
            logger.error(f"Logout failed: {e}")
    
    def verify_permission(self, user_permissions: List[str], 
                         required_permission: Permission) -> bool:
        """Verify if user has required permission."""
        return required_permission.value in user_permissions
    
    def _is_account_locked(self, username: str) -> bool:
        """Check if account is locked."""
        if username in self.locked_accounts:
            lock_time = self.locked_accounts[username]
            if datetime.now(timezone.utc) > lock_time + timedelta(minutes=LOCKOUT_DURATION_MINUTES):
                # Unlock account
                del self.locked_accounts[username]
                return False
            return True
        return False
    
    def _record_failed_attempt(self, username: str):
        """Record failed login attempt."""
        current_time = datetime.now(timezone.utc)
        
        if username not in self.failed_attempts:
            self.failed_attempts[username] = []
        
        # Remove attempts older than lockout window
        cutoff_time = current_time - timedelta(minutes=LOCKOUT_DURATION_MINUTES)
        self.failed_attempts[username] = [
            attempt for attempt in self.failed_attempts[username]
            if attempt > cutoff_time
        ]
        
        # Add current attempt
        self.failed_attempts[username].append(current_time)
        
        # Check if account should be locked
        if len(self.failed_attempts[username]) >= MAX_LOGIN_ATTEMPTS:
            self.locked_accounts[username] = current_time
            
            # Log account lock
            event = SecurityEvent(
                event_id=secrets.token_urlsafe(16),
                event_type=SecurityEventType.ACCOUNT_LOCKED,
                user_id=None,
                username=username,
                ip_address="",
                user_agent="",
                timestamp=current_time,
                details={"attempts": len(self.failed_attempts[username])},
                severity="warning"
            )
            
            self.audit_logger.log_security_event(event)
    
    def _clear_failed_attempts(self, username: str):
        """Clear failed attempts for user."""
        if username in self.failed_attempts:
            del self.failed_attempts[username]
        if username in self.locked_accounts:
            del self.locked_accounts[username]
    
    async def _verify_user_credentials(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """Verify user credentials against database."""
        # This is a simplified implementation
        # In production, this would query the actual user database
        
        # Default admin user for system initialization
        if username == "admin" and password == "admin_password_123":
            return {
                "user_id": "admin_001",
                "username": "admin",
                "email": "admin@company.com",
                "roles": [UserRole.ADMIN.value]
            }
        
        # Analyst user
        if username == "analyst" and password == "analyst_password_123":
            return {
                "user_id": "analyst_001", 
                "username": "analyst",
                "email": "analyst@company.com",
                "roles": [UserRole.ANALYST.value]
            }
        
        return None

# FastAPI Security Dependencies
security = HTTPBearer()

class SecurityDependencies:
    """FastAPI security dependencies."""
    
    def __init__(self, auth_service: AuthenticationService):
        self.auth_service = auth_service
    
    async def get_current_user(self, request: Request,
                             credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
        """Get current authenticated user."""
        try:
            # Extract IP and User-Agent
            ip_address = request.client.host if request.client else "unknown"
            user_agent = request.headers.get("user-agent", "unknown")
            
            # Verify token
            payload = self.auth_service.jwt_manager.verify_token(credentials.credentials)
            
            # Verify session
            session_id = payload.get('session_id')
            session = self.auth_service.session_manager.get_session(session_id)
            
            if not session or not session.is_active:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid session"
                )
            
            # Update session activity
            self.auth_service.session_manager.update_session_activity(session_id)
            
            return {
                "user_id": payload['sub'],
                "username": payload['username'],
                "email": payload.get('email', ''),
                "roles": payload['roles'],
                "permissions": payload['permissions'],
                "session_id": session_id,
                "ip_address": ip_address,
                "user_agent": user_agent
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"User authentication failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication"
            )
    
    def require_permission(self, required_permission: Permission):
        """Dependency to require specific permission."""
        async def permission_checker(current_user: Dict[str, Any] = Depends(self.get_current_user)):
            if not self.auth_service.verify_permission(current_user['permissions'], required_permission):
                # Log permission denied
                self.auth_service.audit_logger.log_permission_denied(
                    current_user['user_id'],
                    current_user['username'],
                    current_user['ip_address'],
                    "unknown_resource",
                    required_permission.value
                )
                
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Permission denied: {required_permission.value} required"
                )
            
            return current_user
        
        return permission_checker
    
    def require_role(self, required_role: UserRole):
        """Dependency to require specific role."""
        async def role_checker(current_user: Dict[str, Any] = Depends(self.get_current_user)):
            if required_role.value not in current_user['roles']:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Role denied: {required_role.value} required"
                )
            
            return current_user
        
        return role_checker

# Global instances
_auth_service: Optional[AuthenticationService] = None
_security_deps: Optional[SecurityDependencies] = None

def initialize_security(secret_key: str, database_url: str = "") -> AuthenticationService:
    """Initialize security system."""
    global _auth_service, _security_deps
    
    _auth_service = AuthenticationService(secret_key, database_url)
    _security_deps = SecurityDependencies(_auth_service)
    
    logger.info("Security system initialized")
    return _auth_service

def get_auth_service() -> AuthenticationService:
    """Get authentication service instance."""
    if _auth_service is None:
        raise RuntimeError("Security system not initialized")
    return _auth_service

def get_security_deps() -> SecurityDependencies:
    """Get security dependencies instance."""
    if _security_deps is None:
        raise RuntimeError("Security system not initialized")
    return _security_deps
