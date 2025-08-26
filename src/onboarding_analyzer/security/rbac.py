"""
Role-Based Access Control (RBAC) System

Enterprise-grade RBAC implementation with granular permissions, hierarchical roles,
dynamic permission evaluation, and comprehensive access control management.
"""

import asyncio
import logging
import secrets
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Any, Union, Callable
from dataclasses import dataclass, field
from enum import Enum
import json
from pathlib import Path

# Database
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy import Column, String, Integer, DateTime, Boolean, Text, ForeignKey, Table

# FastAPI
from fastapi import HTTPException, status

# Monitoring
from prometheus_client import Counter, Histogram, Gauge

# Import authentication components
from .authentication import UserRole, Permission, SecurityEventType, SecurityEvent

# Initialize metrics
rbac_checks_total = Counter('rbac_checks_total', 'RBAC permission checks', ['resource', 'permission', 'result'])
rbac_role_assignments = Gauge('rbac_role_assignments_total', 'Total role assignments', ['role'])
rbac_permission_denials = Counter('rbac_permission_denials_total', 'Permission denials', ['resource', 'permission', 'reason'])
rbac_policy_evaluations = Counter('rbac_policy_evaluations_total', 'Policy evaluations', ['policy_type', 'result'])

logger = logging.getLogger(__name__)

# Extended permission system
class ResourceType(Enum):
    """Types of resources in the system."""
    ANALYTICS = "analytics"
    REPORTS = "reports" 
    DASHBOARDS = "dashboards"
    USERS = "users"
    SYSTEM = "system"
    AUDIT_LOGS = "audit_logs"
    DATA_EXPORT = "data_export"
    INSIGHTS = "insights"
    MODELS = "models"
    WORKFLOWS = "workflows"
    CONNECTORS = "connectors"
    NOTIFICATIONS = "notifications"

class ActionType(Enum):
    """Types of actions that can be performed."""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    EXECUTE = "execute"
    APPROVE = "approve"
    EXPORT = "export"
    SHARE = "share"
    MANAGE = "manage"
    CONFIGURE = "configure"

class PermissionScope(Enum):
    """Scope of permissions."""
    GLOBAL = "global"
    ORGANIZATION = "organization"
    PROJECT = "project"
    PERSONAL = "personal"

@dataclass
class ResourcePermission:
    """Detailed resource-specific permission."""
    resource_type: ResourceType
    resource_id: Optional[str]
    action: ActionType
    scope: PermissionScope
    conditions: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class PolicyRule:
    """Access control policy rule."""
    rule_id: str
    name: str
    description: str
    resource_type: ResourceType
    actions: List[ActionType]
    conditions: Dict[str, Any]
    effect: str = "allow"  # allow or deny
    priority: int = 0
    is_active: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

@dataclass
class RoleHierarchy:
    """Role hierarchy definition."""
    parent_role: UserRole
    child_role: UserRole
    inheritance_type: str = "full"  # full, partial, conditional
    conditions: Dict[str, Any] = field(default_factory=dict)

@dataclass
class UserContext:
    """User context for permission evaluation."""
    user_id: str
    username: str
    roles: List[str]
    organization_id: Optional[str] = None
    project_ids: List[str] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)
    session_metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class AccessRequest:
    """Access request for permission evaluation."""
    user_context: UserContext
    resource_type: ResourceType
    resource_id: Optional[str]
    action: ActionType
    scope: PermissionScope = PermissionScope.GLOBAL
    request_metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class AccessDecision:
    """Result of access control evaluation."""
    granted: bool
    reason: str
    applied_policies: List[str]
    conditions: Dict[str, Any] = field(default_factory=dict)
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

# Extended role permissions with more granular control
EXTENDED_ROLE_PERMISSIONS = {
    UserRole.ADMIN: {
        ResourceType.ANALYTICS: [ActionType.CREATE, ActionType.READ, ActionType.UPDATE, ActionType.DELETE, ActionType.MANAGE],
        ResourceType.REPORTS: [ActionType.CREATE, ActionType.READ, ActionType.UPDATE, ActionType.DELETE, ActionType.EXPORT, ActionType.SHARE],
        ResourceType.DASHBOARDS: [ActionType.CREATE, ActionType.READ, ActionType.UPDATE, ActionType.DELETE, ActionType.MANAGE],
        ResourceType.USERS: [ActionType.CREATE, ActionType.READ, ActionType.UPDATE, ActionType.DELETE, ActionType.MANAGE],
        ResourceType.SYSTEM: [ActionType.READ, ActionType.UPDATE, ActionType.CONFIGURE, ActionType.MANAGE],
        ResourceType.AUDIT_LOGS: [ActionType.READ, ActionType.EXPORT],
        ResourceType.DATA_EXPORT: [ActionType.CREATE, ActionType.READ, ActionType.EXECUTE],
        ResourceType.INSIGHTS: [ActionType.CREATE, ActionType.READ, ActionType.UPDATE, ActionType.DELETE],
        ResourceType.MODELS: [ActionType.CREATE, ActionType.READ, ActionType.UPDATE, ActionType.DELETE, ActionType.EXECUTE],
        ResourceType.WORKFLOWS: [ActionType.CREATE, ActionType.READ, ActionType.UPDATE, ActionType.DELETE, ActionType.EXECUTE],
        ResourceType.CONNECTORS: [ActionType.CREATE, ActionType.READ, ActionType.UPDATE, ActionType.DELETE, ActionType.CONFIGURE],
        ResourceType.NOTIFICATIONS: [ActionType.CREATE, ActionType.READ, ActionType.UPDATE, ActionType.DELETE, ActionType.CONFIGURE]
    },
    UserRole.ANALYST: {
        ResourceType.ANALYTICS: [ActionType.CREATE, ActionType.READ, ActionType.UPDATE],
        ResourceType.REPORTS: [ActionType.CREATE, ActionType.READ, ActionType.UPDATE, ActionType.EXPORT],
        ResourceType.DASHBOARDS: [ActionType.CREATE, ActionType.READ, ActionType.UPDATE],
        ResourceType.USERS: [ActionType.READ],
        ResourceType.DATA_EXPORT: [ActionType.CREATE, ActionType.READ, ActionType.EXECUTE],
        ResourceType.INSIGHTS: [ActionType.CREATE, ActionType.READ, ActionType.UPDATE],
        ResourceType.MODELS: [ActionType.READ, ActionType.EXECUTE],
        ResourceType.WORKFLOWS: [ActionType.READ, ActionType.EXECUTE],
        ResourceType.CONNECTORS: [ActionType.READ],
        ResourceType.NOTIFICATIONS: [ActionType.READ]
    },
    UserRole.MANAGER: {
        ResourceType.ANALYTICS: [ActionType.READ],
        ResourceType.REPORTS: [ActionType.CREATE, ActionType.READ, ActionType.EXPORT, ActionType.SHARE],
        ResourceType.DASHBOARDS: [ActionType.READ, ActionType.CREATE],
        ResourceType.USERS: [ActionType.READ],
        ResourceType.DATA_EXPORT: [ActionType.READ, ActionType.EXECUTE],
        ResourceType.INSIGHTS: [ActionType.READ, ActionType.APPROVE],
        ResourceType.MODELS: [ActionType.READ],
        ResourceType.WORKFLOWS: [ActionType.READ],
        ResourceType.NOTIFICATIONS: [ActionType.READ, ActionType.CREATE]
    },
    UserRole.VIEWER: {
        ResourceType.ANALYTICS: [ActionType.READ],
        ResourceType.REPORTS: [ActionType.READ],
        ResourceType.DASHBOARDS: [ActionType.READ],
        ResourceType.INSIGHTS: [ActionType.READ],
        ResourceType.MODELS: [ActionType.READ],
        ResourceType.WORKFLOWS: [ActionType.READ]
    },
    UserRole.AUDITOR: {
        ResourceType.ANALYTICS: [ActionType.READ],
        ResourceType.REPORTS: [ActionType.READ, ActionType.EXPORT],
        ResourceType.DASHBOARDS: [ActionType.READ],
        ResourceType.AUDIT_LOGS: [ActionType.READ, ActionType.EXPORT],
        ResourceType.DATA_EXPORT: [ActionType.READ, ActionType.EXECUTE],
        ResourceType.INSIGHTS: [ActionType.READ],
        ResourceType.USERS: [ActionType.READ]
    }
}

# Role hierarchy definitions
ROLE_HIERARCHIES = [
    RoleHierarchy(UserRole.ADMIN, UserRole.MANAGER),
    RoleHierarchy(UserRole.ADMIN, UserRole.ANALYST),
    RoleHierarchy(UserRole.ADMIN, UserRole.AUDITOR),
    RoleHierarchy(UserRole.MANAGER, UserRole.VIEWER),
    RoleHierarchy(UserRole.ANALYST, UserRole.VIEWER)
]

class PermissionEvaluator:
    """Advanced permission evaluation engine."""
    
    def __init__(self):
        self.policy_rules: Dict[str, PolicyRule] = {}
        self.custom_evaluators: Dict[str, Callable] = {}
        self.role_hierarchies = {
            (h.parent_role, h.child_role): h for h in ROLE_HIERARCHIES
        }
    
    def add_policy_rule(self, rule: PolicyRule):
        """Add a policy rule."""
        self.policy_rules[rule.rule_id] = rule
        logger.info(f"Policy rule added: {rule.name} ({rule.rule_id})")
    
    def remove_policy_rule(self, rule_id: str):
        """Remove a policy rule."""
        if rule_id in self.policy_rules:
            del self.policy_rules[rule_id]
            logger.info(f"Policy rule removed: {rule_id}")
    
    def register_custom_evaluator(self, name: str, evaluator: Callable):
        """Register custom permission evaluator."""
        self.custom_evaluators[name] = evaluator
        logger.info(f"Custom evaluator registered: {name}")
    
    async def evaluate_access(self, request: AccessRequest) -> AccessDecision:
        """Evaluate access request against all policies."""
        try:
            # Start with base role permissions
            base_decision = await self._evaluate_role_permissions(request)
            
            # Apply policy rules
            policy_decision = await self._evaluate_policy_rules(request)
            
            # Apply custom evaluators
            custom_decision = await self._evaluate_custom_rules(request)
            
            # Combine decisions (deny takes precedence)
            final_decision = self._combine_decisions(
                request, [base_decision, policy_decision, custom_decision]
            )
            
            # Log the decision
            rbac_checks_total.labels(
                resource=request.resource_type.value,
                permission=request.action.value,
                result='granted' if final_decision.granted else 'denied'
            ).inc()
            
            if not final_decision.granted:
                rbac_permission_denials.labels(
                    resource=request.resource_type.value,
                    permission=request.action.value,
                    reason=final_decision.reason
                ).inc()
            
            return final_decision
            
        except Exception as e:
            logger.error(f"Access evaluation failed: {e}")
            return AccessDecision(
                granted=False,
                reason=f"Evaluation error: {str(e)}",
                applied_policies=[]
            )
    
    async def _evaluate_role_permissions(self, request: AccessRequest) -> AccessDecision:
        """Evaluate based on role permissions."""
        user_roles = [UserRole(role) for role in request.user_context.roles if role in [r.value for r in UserRole]]
        
        # Check direct role permissions
        for role in user_roles:
            if self._check_role_permission(role, request.resource_type, request.action):
                return AccessDecision(
                    granted=True,
                    reason=f"Granted by role: {role.value}",
                    applied_policies=[f"role_{role.value}"]
                )
        
        # Check inherited permissions through role hierarchy
        for role in user_roles:
            if self._check_inherited_permissions(role, request.resource_type, request.action):
                return AccessDecision(
                    granted=True,
                    reason=f"Granted by inherited role permissions: {role.value}",
                    applied_policies=[f"inherited_role_{role.value}"]
                )
        
        return AccessDecision(
            granted=False,
            reason="No role-based permissions found",
            applied_policies=[]
        )
    
    async def _evaluate_policy_rules(self, request: AccessRequest) -> AccessDecision:
        """Evaluate policy rules."""
        applicable_rules = []
        
        # Find applicable rules
        for rule in self.policy_rules.values():
            if (rule.is_active and 
                rule.resource_type == request.resource_type and
                request.action in rule.actions):
                
                # Check conditions
                if self._evaluate_rule_conditions(rule, request):
                    applicable_rules.append(rule)
        
        # Sort by priority (higher priority first)
        applicable_rules.sort(key=lambda r: r.priority, reverse=True)
        
        # Apply rules in priority order
        for rule in applicable_rules:
            rbac_policy_evaluations.labels(
                policy_type=rule.name,
                result=rule.effect
            ).inc()
            
            if rule.effect == "deny":
                return AccessDecision(
                    granted=False,
                    reason=f"Denied by policy: {rule.name}",
                    applied_policies=[rule.rule_id]
                )
            elif rule.effect == "allow":
                return AccessDecision(
                    granted=True,
                    reason=f"Granted by policy: {rule.name}",
                    applied_policies=[rule.rule_id]
                )
        
        return AccessDecision(
            granted=False,
            reason="No applicable policy rules",
            applied_policies=[]
        )
    
    async def _evaluate_custom_rules(self, request: AccessRequest) -> AccessDecision:
        """Evaluate custom rules."""
        for name, evaluator in self.custom_evaluators.items():
            try:
                result = await evaluator(request)
                if isinstance(result, AccessDecision):
                    if not result.granted:
                        return result
                    # Continue with other evaluators for allow decisions
            except Exception as e:
                logger.error(f"Custom evaluator {name} failed: {e}")
        
        return AccessDecision(
            granted=True,
            reason="No custom rules applied",
            applied_policies=[]
        )
    
    def _check_role_permission(self, role: UserRole, resource_type: ResourceType, 
                              action: ActionType) -> bool:
        """Check if role has permission for resource/action."""
        role_perms = EXTENDED_ROLE_PERMISSIONS.get(role, {})
        resource_actions = role_perms.get(resource_type, [])
        return action in resource_actions
    
    def _check_inherited_permissions(self, role: UserRole, resource_type: ResourceType,
                                   action: ActionType) -> bool:
        """Check inherited permissions through role hierarchy."""
        # Find parent roles
        for (parent_role, child_role), hierarchy in self.role_hierarchies.items():
            if child_role == role:
                if self._check_role_permission(parent_role, resource_type, action):
                    return True
                # Recursively check parent's parents
                if self._check_inherited_permissions(parent_role, resource_type, action):
                    return True
        
        return False
    
    def _evaluate_rule_conditions(self, rule: PolicyRule, request: AccessRequest) -> bool:
        """Evaluate rule conditions."""
        for condition_name, condition_value in rule.conditions.items():
            if condition_name == "user_attributes":
                if not self._check_user_attributes(condition_value, request.user_context.attributes):
                    return False
            elif condition_name == "time_restriction":
                if not self._check_time_restriction(condition_value):
                    return False
            elif condition_name == "resource_id":
                if request.resource_id != condition_value:
                    return False
            elif condition_name == "scope":
                if request.scope.value != condition_value:
                    return False
            # Add more condition types as needed
        
        return True
    
    def _check_user_attributes(self, required_attrs: Dict[str, Any], 
                              user_attrs: Dict[str, Any]) -> bool:
        """Check user attributes against requirements."""
        for attr_name, attr_value in required_attrs.items():
            if attr_name not in user_attrs:
                return False
            if user_attrs[attr_name] != attr_value:
                return False
        return True
    
    def _check_time_restriction(self, time_restriction: Dict[str, Any]) -> bool:
        """Check time-based restrictions."""
        current_time = datetime.now(timezone.utc)
        
        if "start_time" in time_restriction:
            start_time = datetime.fromisoformat(time_restriction["start_time"])
            if current_time < start_time:
                return False
        
        if "end_time" in time_restriction:
            end_time = datetime.fromisoformat(time_restriction["end_time"])
            if current_time > end_time:
                return False
        
        if "days_of_week" in time_restriction:
            allowed_days = time_restriction["days_of_week"]
            if current_time.weekday() not in allowed_days:
                return False
        
        if "hours" in time_restriction:
            allowed_hours = time_restriction["hours"]
            if current_time.hour not in allowed_hours:
                return False
        
        return True
    
    def _combine_decisions(self, request: AccessRequest, 
                          decisions: List[AccessDecision]) -> AccessDecision:
        """Combine multiple access decisions."""
        # Deny takes precedence
        for decision in decisions:
            if not decision.granted and decision.reason != "No applicable policy rules":
                return decision
        
        # Find the first allow decision
        for decision in decisions:
            if decision.granted:
                return decision
        
        # Default deny
        return AccessDecision(
            granted=False,
            reason="Access denied by default policy",
            applied_policies=["default_deny"]
        )

class RBACManager:
    """Role-Based Access Control Manager."""
    
    def __init__(self):
        self.permission_evaluator = PermissionEvaluator()
        self.user_roles: Dict[str, Set[str]] = {}
        self.role_assignments: Dict[str, Dict[str, Any]] = {}
        self.access_cache: Dict[str, AccessDecision] = {}
        self.cache_ttl = timedelta(minutes=5)
        
        # Initialize default policies
        self._initialize_default_policies()
        
        # Start cache cleanup task
        self._start_cache_cleanup()
    
    def _initialize_default_policies(self):
        """Initialize default RBAC policies."""
        # Time-based access restriction
        time_policy = PolicyRule(
            rule_id="time_restriction_policy",
            name="Business Hours Access",
            description="Restrict access to business hours for non-admin users",
            resource_type=ResourceType.SYSTEM,
            actions=[ActionType.CONFIGURE, ActionType.MANAGE],
            conditions={
                "time_restriction": {
                    "hours": list(range(9, 18)),  # 9 AM to 6 PM
                    "days_of_week": list(range(0, 5))  # Monday to Friday
                }
            },
            effect="deny",
            priority=100
        )
        self.permission_evaluator.add_policy_rule(time_policy)
        
        # Sensitive data access policy
        sensitive_data_policy = PolicyRule(
            rule_id="sensitive_data_policy",
            name="Sensitive Data Access Control",
            description="Control access to sensitive analytics data",
            resource_type=ResourceType.ANALYTICS,
            actions=[ActionType.READ, ActionType.EXPORT],
            conditions={
                "user_attributes": {
                    "security_clearance": "high"
                }
            },
            effect="allow",
            priority=90
        )
        self.permission_evaluator.add_policy_rule(sensitive_data_policy)
        
        # Export limitation policy
        export_policy = PolicyRule(
            rule_id="export_limitation_policy",
            name="Data Export Limitations",
            description="Limit data export to authorized users only",
            resource_type=ResourceType.DATA_EXPORT,
            actions=[ActionType.EXECUTE],
            conditions={
                "scope": "global"
            },
            effect="deny",
            priority=80
        )
        self.permission_evaluator.add_policy_rule(export_policy)
        
        logger.info("Default RBAC policies initialized")
    
    def _start_cache_cleanup(self):
        """Start cache cleanup task."""
        async def cleanup_loop():
            while True:
                try:
                    await self._cleanup_cache()
                    await asyncio.sleep(300)  # Clean up every 5 minutes
                except Exception as e:
                    logger.error(f"Cache cleanup error: {e}")
                    await asyncio.sleep(60)
        
        asyncio.create_task(cleanup_loop())
    
    async def assign_role(self, user_id: str, role: UserRole, 
                         assigned_by: str, metadata: Dict[str, Any] = None) -> bool:
        """Assign role to user."""
        try:
            if user_id not in self.user_roles:
                self.user_roles[user_id] = set()
            
            if role.value not in self.user_roles[user_id]:
                self.user_roles[user_id].add(role.value)
                
                # Record assignment metadata
                assignment_key = f"{user_id}:{role.value}"
                self.role_assignments[assignment_key] = {
                    "user_id": user_id,
                    "role": role.value,
                    "assigned_by": assigned_by,
                    "assigned_at": datetime.now(timezone.utc),
                    "metadata": metadata or {}
                }
                
                # Update metrics
                rbac_role_assignments.labels(role=role.value).inc()
                
                # Clear cache for user
                await self._clear_user_cache(user_id)
                
                logger.info(f"Role {role.value} assigned to user {user_id} by {assigned_by}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Role assignment failed: {e}")
            return False
    
    async def revoke_role(self, user_id: str, role: UserRole, 
                         revoked_by: str) -> bool:
        """Revoke role from user."""
        try:
            if user_id in self.user_roles and role.value in self.user_roles[user_id]:
                self.user_roles[user_id].remove(role.value)
                
                # Remove assignment record
                assignment_key = f"{user_id}:{role.value}"
                if assignment_key in self.role_assignments:
                    del self.role_assignments[assignment_key]
                
                # Update metrics
                rbac_role_assignments.labels(role=role.value).dec()
                
                # Clear cache for user
                await self._clear_user_cache(user_id)
                
                logger.info(f"Role {role.value} revoked from user {user_id} by {revoked_by}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Role revocation failed: {e}")
            return False
    
    async def check_permission(self, user_id: str, username: str,
                             resource_type: ResourceType, resource_id: Optional[str],
                             action: ActionType, scope: PermissionScope = PermissionScope.GLOBAL,
                             user_attributes: Dict[str, Any] = None) -> AccessDecision:
        """Check if user has permission for specific action."""
        try:
            # Create user context
            user_context = UserContext(
                user_id=user_id,
                username=username,
                roles=list(self.user_roles.get(user_id, set())),
                attributes=user_attributes or {}
            )
            
            # Create access request
            request = AccessRequest(
                user_context=user_context,
                resource_type=resource_type,
                resource_id=resource_id,
                action=action,
                scope=scope
            )
            
            # Check cache first
            cache_key = self._generate_cache_key(request)
            if cache_key in self.access_cache:
                cached_decision = self.access_cache[cache_key]
                if self._is_cache_valid(cached_decision):
                    return cached_decision
                else:
                    del self.access_cache[cache_key]
            
            # Evaluate access
            decision = await self.permission_evaluator.evaluate_access(request)
            
            # Cache the decision
            decision.metadata["cached_at"] = datetime.now(timezone.utc)
            self.access_cache[cache_key] = decision
            
            return decision
            
        except Exception as e:
            logger.error(f"Permission check failed: {e}")
            return AccessDecision(
                granted=False,
                reason=f"Permission check error: {str(e)}",
                applied_policies=[]
            )
    
    def get_user_roles(self, user_id: str) -> List[str]:
        """Get all roles assigned to user."""
        return list(self.user_roles.get(user_id, set()))
    
    def get_role_assignments(self, user_id: str) -> List[Dict[str, Any]]:
        """Get role assignment details for user."""
        assignments = []
        user_roles = self.user_roles.get(user_id, set())
        
        for role in user_roles:
            assignment_key = f"{user_id}:{role}"
            if assignment_key in self.role_assignments:
                assignments.append(self.role_assignments[assignment_key])
        
        return assignments
    
    def get_users_with_role(self, role: UserRole) -> List[str]:
        """Get all users with specific role."""
        users_with_role = []
        for user_id, roles in self.user_roles.items():
            if role.value in roles:
                users_with_role.append(user_id)
        
        return users_with_role
    
    async def get_user_permissions(self, user_id: str) -> Dict[str, List[str]]:
        """Get all effective permissions for user."""
        user_roles = [UserRole(role) for role in self.user_roles.get(user_id, set()) 
                     if role in [r.value for r in UserRole]]
        
        permissions = {}
        
        # Direct role permissions
        for role in user_roles:
            role_perms = EXTENDED_ROLE_PERMISSIONS.get(role, {})
            for resource_type, actions in role_perms.items():
                resource_key = resource_type.value
                if resource_key not in permissions:
                    permissions[resource_key] = set()
                permissions[resource_key].update([action.value for action in actions])
        
        # Inherited permissions
        for role in user_roles:
            inherited_perms = self._get_inherited_permissions(role)
            for resource_type, actions in inherited_perms.items():
                resource_key = resource_type.value
                if resource_key not in permissions:
                    permissions[resource_key] = set()
                permissions[resource_key].update([action.value for action in actions])
        
        # Convert sets to lists
        return {resource: list(actions) for resource, actions in permissions.items()}
    
    def _get_inherited_permissions(self, role: UserRole) -> Dict[ResourceType, Set[ActionType]]:
        """Get inherited permissions for role."""
        inherited_perms = {}
        
        # Find parent roles
        for (parent_role, child_role), hierarchy in self.permission_evaluator.role_hierarchies.items():
            if child_role == role:
                parent_perms = EXTENDED_ROLE_PERMISSIONS.get(parent_role, {})
                for resource_type, actions in parent_perms.items():
                    if resource_type not in inherited_perms:
                        inherited_perms[resource_type] = set()
                    inherited_perms[resource_type].update(actions)
                
                # Recursively get parent's inherited permissions
                parent_inherited = self._get_inherited_permissions(parent_role)
                for resource_type, actions in parent_inherited.items():
                    if resource_type not in inherited_perms:
                        inherited_perms[resource_type] = set()
                    inherited_perms[resource_type].update(actions)
        
        return inherited_perms
    
    def _generate_cache_key(self, request: AccessRequest) -> str:
        """Generate cache key for access request."""
        key_parts = [
            request.user_context.user_id,
            request.resource_type.value,
            request.resource_id or "none",
            request.action.value,
            request.scope.value,
            str(hash(frozenset(request.user_context.attributes.items())))
        ]
        return ":".join(key_parts)
    
    def _is_cache_valid(self, decision: AccessDecision) -> bool:
        """Check if cached decision is still valid."""
        if "cached_at" not in decision.metadata:
            return False
        
        cached_at = decision.metadata["cached_at"]
        return datetime.now(timezone.utc) - cached_at < self.cache_ttl
    
    async def _clear_user_cache(self, user_id: str):
        """Clear cache entries for specific user."""
        keys_to_remove = []
        for key in self.access_cache.keys():
            if key.startswith(f"{user_id}:"):
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.access_cache[key]
    
    async def _cleanup_cache(self):
        """Clean up expired cache entries."""
        current_time = datetime.now(timezone.utc)
        keys_to_remove = []
        
        for key, decision in self.access_cache.items():
            if not self._is_cache_valid(decision):
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.access_cache[key]
        
        if keys_to_remove:
            logger.debug(f"Cleaned up {len(keys_to_remove)} expired cache entries")

class RBACMiddleware:
    """RBAC middleware for FastAPI integration."""
    
    def __init__(self, rbac_manager: RBACManager):
        self.rbac_manager = rbac_manager
    
    def require_permission(self, resource_type: ResourceType, action: ActionType,
                          scope: PermissionScope = PermissionScope.GLOBAL,
                          resource_id_param: Optional[str] = None):
        """Decorator for requiring specific permissions."""
        def decorator(func):
            async def wrapper(*args, **kwargs):
                # Extract current user from kwargs (injected by auth middleware)
                current_user = kwargs.get('current_user')
                if not current_user:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Authentication required"
                    )
                
                # Extract resource ID if specified
                resource_id = None
                if resource_id_param and resource_id_param in kwargs:
                    resource_id = kwargs[resource_id_param]
                
                # Check permission
                decision = await self.rbac_manager.check_permission(
                    user_id=current_user['user_id'],
                    username=current_user['username'],
                    resource_type=resource_type,
                    resource_id=resource_id,
                    action=action,
                    scope=scope,
                    user_attributes=current_user.get('attributes', {})
                )
                
                if not decision.granted:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"Access denied: {decision.reason}"
                    )
                
                # Add decision to kwargs for potential use in handler
                kwargs['access_decision'] = decision
                
                return await func(*args, **kwargs)
            
            return wrapper
        return decorator

# Global instance
_rbac_manager: Optional[RBACManager] = None

def initialize_rbac() -> RBACManager:
    """Initialize RBAC system."""
    global _rbac_manager
    
    _rbac_manager = RBACManager()
    
    logger.info("RBAC system initialized")
    return _rbac_manager

def get_rbac_manager() -> RBACManager:
    """Get RBAC manager instance."""
    if _rbac_manager is None:
        raise RuntimeError("RBAC system not initialized")
    return _rbac_manager
