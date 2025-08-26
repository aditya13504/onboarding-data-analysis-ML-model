"""
Input Validation and Sanitization System

Enterprise-grade input validation, data sanitization, injection attack prevention,
and comprehensive request validation for all API endpoints and data inputs.
"""

import re
import html
import json
import logging
import secrets
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union, Callable, Type
from dataclasses import dataclass, field
from enum import Enum
import ipaddress
from urllib.parse import urlparse
from pathlib import Path

# Validation libraries
import validators
from pydantic import BaseModel, validator, ValidationError
from marshmallow import Schema, fields, validate, ValidationError as MarshmallowValidationError

# FastAPI
from fastapi import HTTPException, Request, status
from fastapi.params import Query, Path as PathParam, Body

# Monitoring
from prometheus_client import Counter, Histogram

# Initialize metrics
validation_attempts = Counter('validation_attempts_total', 'Validation attempts', ['validator_type', 'result'])
validation_errors = Counter('validation_errors_total', 'Validation errors', ['error_type', 'field'])
sanitization_operations = Counter('sanitization_operations_total', 'Sanitization operations', ['operation_type'])
injection_attempts_blocked = Counter('injection_attempts_blocked_total', 'Blocked injection attempts', ['attack_type'])

logger = logging.getLogger(__name__)

class ValidationType(Enum):
    """Types of validation."""
    EMAIL = "email"
    URL = "url"
    IP_ADDRESS = "ip_address"
    PHONE = "phone"
    CREDIT_CARD = "credit_card"
    SSN = "ssn"
    UUID = "uuid"
    ALPHANUMERIC = "alphanumeric"
    NUMERIC = "numeric"
    TEXT = "text"
    HTML = "html"
    JSON = "json"
    SQL = "sql"
    XPATH = "xpath"
    FILENAME = "filename"
    PATH = "path"

class SanitizationType(Enum):
    """Types of sanitization."""
    HTML_ESCAPE = "html_escape"
    SQL_ESCAPE = "sql_escape"
    JAVASCRIPT_ESCAPE = "javascript_escape"
    URL_ENCODE = "url_encode"
    STRIP_TAGS = "strip_tags"
    NORMALIZE_WHITESPACE = "normalize_whitespace"
    REMOVE_CONTROL_CHARS = "remove_control_chars"
    LOWERCASE = "lowercase"
    UPPERCASE = "uppercase"
    TRIM = "trim"

class InjectionType(Enum):
    """Types of injection attacks."""
    SQL_INJECTION = "sql_injection"
    XSS = "xss"
    COMMAND_INJECTION = "command_injection"
    LDAP_INJECTION = "ldap_injection"
    XPATH_INJECTION = "xpath_injection"
    XML_INJECTION = "xml_injection"
    TEMPLATE_INJECTION = "template_injection"

@dataclass
class ValidationRule:
    """Validation rule configuration."""
    rule_id: str
    name: str
    description: str
    validation_type: ValidationType
    pattern: Optional[str] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    allowed_values: Optional[List[Any]] = None
    custom_validator: Optional[Callable] = None
    is_required: bool = True
    sanitization_types: List[SanitizationType] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ValidationResult:
    """Result of validation operation."""
    is_valid: bool
    sanitized_value: Any
    original_value: Any
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class InjectionPattern:
    """Injection attack pattern."""
    pattern_id: str
    name: str
    injection_type: InjectionType
    pattern: str
    severity: str = "high"
    is_regex: bool = True
    case_sensitive: bool = False

# Common injection attack patterns
INJECTION_PATTERNS = [
    # SQL Injection
    InjectionPattern(
        pattern_id="sql_union",
        name="SQL UNION Attack",
        injection_type=InjectionType.SQL_INJECTION,
        pattern=r"(?i)(\bunion\b.+\bselect\b|\bselect\b.+\bunion\b)",
        case_sensitive=False
    ),
    InjectionPattern(
        pattern_id="sql_blind",
        name="SQL Blind Injection",
        injection_type=InjectionType.SQL_INJECTION,
        pattern=r"(?i)(\bor\b\s+1\s*=\s*1|\band\b\s+1\s*=\s*1|\bor\b\s+true|\band\b\s+false)",
        case_sensitive=False
    ),
    InjectionPattern(
        pattern_id="sql_comment",
        name="SQL Comment Injection",
        injection_type=InjectionType.SQL_INJECTION,
        pattern=r"(--|/\*|\*/|#)",
        case_sensitive=False
    ),
    
    # XSS
    InjectionPattern(
        pattern_id="xss_script",
        name="XSS Script Tag",
        injection_type=InjectionType.XSS,
        pattern=r"(?i)<script[^>]*>.*?</script>",
        case_sensitive=False
    ),
    InjectionPattern(
        pattern_id="xss_javascript",
        name="XSS JavaScript Protocol",
        injection_type=InjectionType.XSS,
        pattern=r"(?i)javascript\s*:",
        case_sensitive=False
    ),
    InjectionPattern(
        pattern_id="xss_onload",
        name="XSS Event Handler",
        injection_type=InjectionType.XSS,
        pattern=r"(?i)on\w+\s*=",
        case_sensitive=False
    ),
    
    # Command Injection
    InjectionPattern(
        pattern_id="cmd_chaining",
        name="Command Chaining",
        injection_type=InjectionType.COMMAND_INJECTION,
        pattern=r"[;&|`$]",
        case_sensitive=False
    ),
    InjectionPattern(
        pattern_id="cmd_substitution",
        name="Command Substitution",
        injection_type=InjectionType.COMMAND_INJECTION,
        pattern=r"\$\(.*?\)|\`.*?\`",
        case_sensitive=False
    ),
    
    # LDAP Injection
    InjectionPattern(
        pattern_id="ldap_operators",
        name="LDAP Operators",
        injection_type=InjectionType.LDAP_INJECTION,
        pattern=r"[*()\\\/\x00]",
        case_sensitive=False
    ),
    
    # XPath Injection
    InjectionPattern(
        pattern_id="xpath_operators",
        name="XPath Operators",
        injection_type=InjectionType.XPATH_INJECTION,
        pattern=r"(?i)(\bor\b|\band\b|\bnot\b).*?[\[\]'\"()]",
        case_sensitive=False
    ),
]

class InjectionDetector:
    """Injection attack detection."""
    
    def __init__(self):
        self.patterns = {pattern.pattern_id: pattern for pattern in INJECTION_PATTERNS}
        self.custom_patterns: Dict[str, InjectionPattern] = {}
    
    def add_custom_pattern(self, pattern: InjectionPattern):
        """Add custom injection pattern."""
        self.custom_patterns[pattern.pattern_id] = pattern
        logger.info(f"Custom injection pattern added: {pattern.name}")
    
    def detect_injection(self, value: str) -> List[InjectionPattern]:
        """Detect injection attempts in value."""
        detected_patterns = []
        
        if not isinstance(value, str):
            return detected_patterns
        
        # Check all patterns
        all_patterns = {**self.patterns, **self.custom_patterns}
        
        for pattern in all_patterns.values():
            try:
                if pattern.is_regex:
                    flags = 0 if pattern.case_sensitive else re.IGNORECASE
                    if re.search(pattern.pattern, value, flags):
                        detected_patterns.append(pattern)
                        injection_attempts_blocked.labels(attack_type=pattern.injection_type.value).inc()
                else:
                    # Simple string matching
                    search_value = value if pattern.case_sensitive else value.lower()
                    search_pattern = pattern.pattern if pattern.case_sensitive else pattern.pattern.lower()
                    
                    if search_pattern in search_value:
                        detected_patterns.append(pattern)
                        injection_attempts_blocked.labels(attack_type=pattern.injection_type.value).inc()
            
            except re.error as e:
                logger.error(f"Invalid regex pattern {pattern.pattern_id}: {e}")
        
        return detected_patterns

class DataSanitizer:
    """Data sanitization utilities."""
    
    @staticmethod
    def html_escape(value: str) -> str:
        """Escape HTML characters."""
        return html.escape(value, quote=True)
    
    @staticmethod
    def sql_escape(value: str) -> str:
        """Escape SQL special characters."""
        return value.replace("'", "''").replace("\\", "\\\\")
    
    @staticmethod
    def javascript_escape(value: str) -> str:
        """Escape JavaScript special characters."""
        escape_dict = {
            '"': '\\"',
            "'": "\\'",
            '\\': '\\\\',
            '\n': '\\n',
            '\r': '\\r',
            '\t': '\\t',
            '\b': '\\b',
            '\f': '\\f'
        }
        
        for char, escaped in escape_dict.items():
            value = value.replace(char, escaped)
        
        return value
    
    @staticmethod
    def strip_tags(value: str) -> str:
        """Strip HTML/XML tags."""
        return re.sub(r'<[^>]*>', '', value)
    
    @staticmethod
    def normalize_whitespace(value: str) -> str:
        """Normalize whitespace characters."""
        return re.sub(r'\s+', ' ', value).strip()
    
    @staticmethod
    def remove_control_chars(value: str) -> str:
        """Remove control characters."""
        return ''.join(char for char in value if ord(char) >= 32 or char in '\t\n\r')
    
    @staticmethod
    def sanitize_filename(value: str) -> str:
        """Sanitize filename."""
        # Remove path separators and dangerous characters
        value = re.sub(r'[<>:"/\\|?*]', '', value)
        # Remove leading/trailing dots and spaces
        value = value.strip('. ')
        # Limit length
        return value[:255] if value else 'unnamed'
    
    @staticmethod
    def sanitize_path(value: str) -> str:
        """Sanitize file path."""
        # Normalize path separators
        value = value.replace('\\', '/')
        # Remove path traversal attempts
        value = re.sub(r'\.\./', '', value)
        value = re.sub(r'/\.\.', '', value)
        # Remove multiple slashes
        value = re.sub(r'/+', '/', value)
        return value
    
    def sanitize_value(self, value: Any, sanitization_types: List[SanitizationType]) -> Any:
        """Apply multiple sanitization operations."""
        if not isinstance(value, str):
            return value
        
        sanitized = value
        
        for sanitization_type in sanitization_types:
            try:
                if sanitization_type == SanitizationType.HTML_ESCAPE:
                    sanitized = self.html_escape(sanitized)
                elif sanitization_type == SanitizationType.SQL_ESCAPE:
                    sanitized = self.sql_escape(sanitized)
                elif sanitization_type == SanitizationType.JAVASCRIPT_ESCAPE:
                    sanitized = self.javascript_escape(sanitized)
                elif sanitization_type == SanitizationType.STRIP_TAGS:
                    sanitized = self.strip_tags(sanitized)
                elif sanitization_type == SanitizationType.NORMALIZE_WHITESPACE:
                    sanitized = self.normalize_whitespace(sanitized)
                elif sanitization_type == SanitizationType.REMOVE_CONTROL_CHARS:
                    sanitized = self.remove_control_chars(sanitized)
                elif sanitization_type == SanitizationType.LOWERCASE:
                    sanitized = sanitized.lower()
                elif sanitization_type == SanitizationType.UPPERCASE:
                    sanitized = sanitized.upper()
                elif sanitization_type == SanitizationType.TRIM:
                    sanitized = sanitized.strip()
                elif sanitization_type == SanitizationType.FILENAME:
                    sanitized = self.sanitize_filename(sanitized)
                elif sanitization_type == SanitizationType.PATH:
                    sanitized = self.sanitize_path(sanitized)
                
                sanitization_operations.labels(operation_type=sanitization_type.value).inc()
                
            except Exception as e:
                logger.error(f"Sanitization failed for {sanitization_type}: {e}")
        
        return sanitized

class InputValidator:
    """Input validation engine."""
    
    def __init__(self):
        self.injection_detector = InjectionDetector()
        self.sanitizer = DataSanitizer()
        self.validation_rules: Dict[str, ValidationRule] = {}
        
        # Initialize default validation rules
        self._initialize_default_rules()
    
    def _initialize_default_rules(self):
        """Initialize default validation rules."""
        # Email validation
        email_rule = ValidationRule(
            rule_id="email",
            name="Email Validation",
            description="Validate email addresses",
            validation_type=ValidationType.EMAIL,
            max_length=254,
            sanitization_types=[SanitizationType.TRIM, SanitizationType.LOWERCASE]
        )
        self.validation_rules["email"] = email_rule
        
        # URL validation
        url_rule = ValidationRule(
            rule_id="url",
            name="URL Validation",
            description="Validate URLs",
            validation_type=ValidationType.URL,
            max_length=2048,
            sanitization_types=[SanitizationType.TRIM]
        )
        self.validation_rules["url"] = url_rule
        
        # Username validation
        username_rule = ValidationRule(
            rule_id="username",
            name="Username Validation",
            description="Validate usernames",
            validation_type=ValidationType.ALPHANUMERIC,
            pattern=r"^[a-zA-Z0-9_-]{3,30}$",
            min_length=3,
            max_length=30,
            sanitization_types=[SanitizationType.TRIM]
        )
        self.validation_rules["username"] = username_rule
        
        # Password validation
        password_rule = ValidationRule(
            rule_id="password",
            name="Password Validation",
            description="Validate passwords",
            validation_type=ValidationType.TEXT,
            min_length=12,
            max_length=128,
            pattern=r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]",
            sanitization_types=[]  # Don't sanitize passwords
        )
        self.validation_rules["password"] = password_rule
        
        # Phone validation
        phone_rule = ValidationRule(
            rule_id="phone",
            name="Phone Validation",
            description="Validate phone numbers",
            validation_type=ValidationType.PHONE,
            pattern=r"^\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$",
            sanitization_types=[SanitizationType.TRIM]
        )
        self.validation_rules["phone"] = phone_rule
    
    def add_validation_rule(self, rule: ValidationRule):
        """Add custom validation rule."""
        self.validation_rules[rule.rule_id] = rule
        logger.info(f"Validation rule added: {rule.name}")
    
    def validate_value(self, value: Any, rule_id: str, field_name: str = "") -> ValidationResult:
        """Validate single value against rule."""
        if rule_id not in self.validation_rules:
            return ValidationResult(
                is_valid=False,
                sanitized_value=value,
                original_value=value,
                errors=[f"Unknown validation rule: {rule_id}"]
            )
        
        rule = self.validation_rules[rule_id]
        errors = []
        warnings = []
        
        try:
            # Check if required
            if rule.is_required and (value is None or value == ""):
                errors.append(f"Field {field_name or 'value'} is required")
                return ValidationResult(
                    is_valid=False,
                    sanitized_value=value,
                    original_value=value,
                    errors=errors
                )
            
            # Skip validation if value is None/empty and not required
            if not rule.is_required and (value is None or value == ""):
                return ValidationResult(
                    is_valid=True,
                    sanitized_value=value,
                    original_value=value
                )
            
            # Convert to string for validation
            str_value = str(value) if value is not None else ""
            
            # Check for injection attacks
            injection_patterns = self.injection_detector.detect_injection(str_value)
            if injection_patterns:
                for pattern in injection_patterns:
                    errors.append(f"Potential {pattern.injection_type.value} attack detected")
            
            # Length validation
            if rule.min_length is not None and len(str_value) < rule.min_length:
                errors.append(f"Value too short (minimum {rule.min_length} characters)")
            
            if rule.max_length is not None and len(str_value) > rule.max_length:
                errors.append(f"Value too long (maximum {rule.max_length} characters)")
            
            # Pattern validation
            if rule.pattern and not re.match(rule.pattern, str_value):
                errors.append(f"Value does not match required pattern")
            
            # Type-specific validation
            validation_errors = self._validate_by_type(str_value, rule.validation_type)
            errors.extend(validation_errors)
            
            # Allowed values validation
            if rule.allowed_values and value not in rule.allowed_values:
                errors.append(f"Value not in allowed list")
            
            # Custom validator
            if rule.custom_validator:
                try:
                    custom_result = rule.custom_validator(value)
                    if isinstance(custom_result, bool):
                        if not custom_result:
                            errors.append("Custom validation failed")
                    elif isinstance(custom_result, str):
                        errors.append(custom_result)
                except Exception as e:
                    errors.append(f"Custom validation error: {str(e)}")
            
            # Sanitize value
            sanitized_value = self.sanitizer.sanitize_value(str_value, rule.sanitization_types)
            
            is_valid = len(errors) == 0
            
            # Update metrics
            validation_attempts.labels(
                validator_type=rule.validation_type.value,
                result='success' if is_valid else 'failure'
            ).inc()
            
            if not is_valid:
                validation_errors.labels(
                    error_type='validation_failed',
                    field=field_name or 'unknown'
                ).inc()
            
            return ValidationResult(
                is_valid=is_valid,
                sanitized_value=sanitized_value,
                original_value=value,
                errors=errors,
                warnings=warnings
            )
            
        except Exception as e:
            logger.error(f"Validation failed for {field_name}: {e}")
            validation_errors.labels(
                error_type='validation_error',
                field=field_name or 'unknown'
            ).inc()
            
            return ValidationResult(
                is_valid=False,
                sanitized_value=value,
                original_value=value,
                errors=[f"Validation error: {str(e)}"]
            )
    
    def _validate_by_type(self, value: str, validation_type: ValidationType) -> List[str]:
        """Validate value by type."""
        errors = []
        
        try:
            if validation_type == ValidationType.EMAIL:
                if not validators.email(value):
                    errors.append("Invalid email format")
            
            elif validation_type == ValidationType.URL:
                if not validators.url(value):
                    errors.append("Invalid URL format")
            
            elif validation_type == ValidationType.IP_ADDRESS:
                try:
                    ipaddress.ip_address(value)
                except ValueError:
                    errors.append("Invalid IP address format")
            
            elif validation_type == ValidationType.UUID:
                import uuid
                try:
                    uuid.UUID(value)
                except ValueError:
                    errors.append("Invalid UUID format")
            
            elif validation_type == ValidationType.NUMERIC:
                try:
                    float(value)
                except ValueError:
                    errors.append("Value must be numeric")
            
            elif validation_type == ValidationType.ALPHANUMERIC:
                if not value.replace('_', '').replace('-', '').isalnum():
                    errors.append("Value must be alphanumeric")
            
            elif validation_type == ValidationType.JSON:
                try:
                    json.loads(value)
                except json.JSONDecodeError:
                    errors.append("Invalid JSON format")
            
            elif validation_type == ValidationType.FILENAME:
                # Check for invalid filename characters
                invalid_chars = r'[<>:"/\\|?*]'
                if re.search(invalid_chars, value):
                    errors.append("Filename contains invalid characters")
                
                # Check for reserved names (Windows)
                reserved_names = ['CON', 'PRN', 'AUX', 'NUL'] + [f'COM{i}' for i in range(1, 10)] + [f'LPT{i}' for i in range(1, 10)]
                if value.upper() in reserved_names:
                    errors.append("Filename uses reserved name")
            
            elif validation_type == ValidationType.PATH:
                # Check for path traversal
                if '..' in value or value.startswith('/'):
                    errors.append("Path contains invalid sequences")
        
        except Exception as e:
            logger.error(f"Type validation error for {validation_type}: {e}")
            errors.append(f"Type validation failed: {str(e)}")
        
        return errors
    
    def validate_dict(self, data: Dict[str, Any], field_rules: Dict[str, str]) -> Dict[str, ValidationResult]:
        """Validate dictionary of values."""
        results = {}
        
        for field_name, rule_id in field_rules.items():
            value = data.get(field_name)
            results[field_name] = self.validate_value(value, rule_id, field_name)
        
        return results
    
    def is_dict_valid(self, data: Dict[str, Any], field_rules: Dict[str, str]) -> bool:
        """Check if all fields in dictionary are valid."""
        results = self.validate_dict(data, field_rules)
        return all(result.is_valid for result in results.values())

class RequestValidator:
    """FastAPI request validation middleware."""
    
    def __init__(self, input_validator: InputValidator):
        self.input_validator = input_validator
        self.endpoint_rules: Dict[str, Dict[str, str]] = {}
    
    def configure_endpoint(self, endpoint: str, field_rules: Dict[str, str]):
        """Configure validation rules for endpoint."""
        self.endpoint_rules[endpoint] = field_rules
        logger.info(f"Validation rules configured for endpoint: {endpoint}")
    
    async def validate_request(self, request: Request) -> Dict[str, Any]:
        """Validate incoming request."""
        endpoint = request.url.path
        
        if endpoint not in self.endpoint_rules:
            return {"valid": True, "errors": []}
        
        field_rules = self.endpoint_rules[endpoint]
        
        try:
            # Get request data
            data = {}
            
            # Query parameters
            for key, value in request.query_params.items():
                data[key] = value
            
            # Form data (if applicable)
            if request.headers.get("content-type", "").startswith("application/x-www-form-urlencoded"):
                form_data = await request.form()
                for key, value in form_data.items():
                    data[key] = value
            
            # JSON data (if applicable)
            elif request.headers.get("content-type", "").startswith("application/json"):
                try:
                    json_data = await request.json()
                    if isinstance(json_data, dict):
                        data.update(json_data)
                except Exception:
                    pass
            
            # Validate data
            validation_results = self.input_validator.validate_dict(data, field_rules)
            
            # Collect errors
            errors = []
            sanitized_data = {}
            
            for field_name, result in validation_results.items():
                if not result.is_valid:
                    errors.extend([f"{field_name}: {error}" for error in result.errors])
                sanitized_data[field_name] = result.sanitized_value
            
            return {
                "valid": len(errors) == 0,
                "errors": errors,
                "sanitized_data": sanitized_data,
                "validation_results": validation_results
            }
            
        except Exception as e:
            logger.error(f"Request validation failed: {e}")
            return {
                "valid": False,
                "errors": [f"Validation error: {str(e)}"],
                "sanitized_data": {},
                "validation_results": {}
            }

# Global instance
_input_validator: Optional[InputValidator] = None
_request_validator: Optional[RequestValidator] = None

def initialize_input_validation() -> InputValidator:
    """Initialize input validation system."""
    global _input_validator, _request_validator
    
    _input_validator = InputValidator()
    _request_validator = RequestValidator(_input_validator)
    
    logger.info("Input validation system initialized")
    return _input_validator

def get_input_validator() -> InputValidator:
    """Get input validator instance."""
    if _input_validator is None:
        raise RuntimeError("Input validation system not initialized")
    return _input_validator

def get_request_validator() -> RequestValidator:
    """Get request validator instance."""
    if _request_validator is None:
        raise RuntimeError("Input validation system not initialized")
    return _request_validator
