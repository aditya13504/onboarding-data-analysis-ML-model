"""
Point 19: Configuration Management (Part 1/2)
Comprehensive configuration management system for onboarding analytics platform.
"""

import os
import json
import yaml
import time
import threading
import asyncio
import re
from typing import Dict, List, Optional, Any, Union, Set, Tuple, NamedTuple, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, Counter
import logging
from abc import ABC, abstractmethod
from pathlib import Path
import hashlib
import uuid
import secrets
from cryptography.fernet import Fernet
import copy
from sqlalchemy import create_engine, text
from onboarding_analyzer.infrastructure.db import engine

class ConfigScope(Enum):
    """Configuration scope levels"""
    GLOBAL = "global"
    ENVIRONMENT = "environment"
    FLOW = "flow"
    USER_SEGMENT = "user_segment"
    FEATURE = "feature"
    INTEGRATION = "integration"
    ANALYTICS = "analytics"

class ConfigType(Enum):
    """Configuration value types"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    LIST = "list"
    DICT = "dict"
    JSON = "json"
    SECRET = "secret"
    ENUM = "enum"

class EnvironmentType(Enum):
    """Environment types"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"

class ConfigValidationRule(Enum):
    """Configuration validation rules"""
    REQUIRED = "required"
    MIN_VALUE = "min_value"
    MAX_VALUE = "max_value"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    REGEX_PATTERN = "regex_pattern"
    ALLOWED_VALUES = "allowed_values"
    URL_FORMAT = "url_format"
    EMAIL_FORMAT = "email_format"
    POSITIVE_NUMBER = "positive_number"
    NON_EMPTY_STRING = "non_empty_string"

@dataclass
class ConfigDefinition:
    """Configuration parameter definition"""
    key: str
    description: str
    config_type: ConfigType
    scope: ConfigScope
    
    # Default and validation
    default_value: Any = None
    validation_rules: Dict[ConfigValidationRule, Any] = field(default_factory=dict)
    allowed_values: Optional[List[Any]] = None
    
    # Metadata
    category: str = "general"
    tags: List[str] = field(default_factory=list)
    documentation_url: Optional[str] = None
    
    # Behavior
    requires_restart: bool = False
    hot_reloadable: bool = True
    environment_specific: bool = False
    sensitive: bool = False
    
    # Dependencies
    depends_on: List[str] = field(default_factory=list)
    conflicts_with: List[str] = field(default_factory=list)
    
    # Versioning
    introduced_in_version: str = "1.0"
    deprecated_in_version: Optional[str] = None
    
    created_at: datetime = field(default_factory=datetime.utcnow)

@dataclass
class ConfigValue:
    """Configuration value instance"""
    key: str
    value: Any
    scope: ConfigScope
    environment: EnvironmentType
    
    # Source tracking
    source: str = "default"  # default, file, database, api, override
    source_file: Optional[str] = None
    set_by: str = "system"
    
    # Validation
    is_valid: bool = True
    validation_errors: List[str] = field(default_factory=list)
    
    # Security
    is_encrypted: bool = False
    encryption_key_id: Optional[str] = None
    
    # Metadata
    last_modified: datetime = field(default_factory=datetime.utcnow)
    version: int = 1
    hash: Optional[str] = None
    
    def __post_init__(self):
        """Calculate hash after initialization"""
        self.hash = self._calculate_hash()
    
    def _calculate_hash(self) -> str:
        """Calculate hash of the configuration value"""
        content = f"{self.key}:{self.value}:{self.scope.value}:{self.environment.value}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

@dataclass
class ConfigurationSet:
    """Complete configuration set for a scope/environment"""
    set_id: str
    name: str
    scope: ConfigScope
    environment: EnvironmentType
    
    # Configuration values
    values: Dict[str, ConfigValue] = field(default_factory=dict)
    
    # Metadata
    description: str = ""
    created_by: str = "system"
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_modified: datetime = field(default_factory=datetime.utcnow)
    version: str = "1.0"
    
    # Status
    is_active: bool = True
    is_locked: bool = False
    
    # Validation
    validation_status: str = "valid"  # valid, invalid, warning
    validation_errors: List[str] = field(default_factory=list)
    validation_warnings: List[str] = field(default_factory=list)
    
    # Dependencies
    parent_sets: List[str] = field(default_factory=list)
    child_sets: List[str] = field(default_factory=list)

class EncryptionManager:
    """Handles encryption/decryption of sensitive configuration values"""
    
    def __init__(self, master_key: Optional[str] = None):
        self.master_key = master_key or self._generate_master_key()
        self.encryption_keys: Dict[str, bytes] = {}
        self.cipher_suite = Fernet(self.master_key)
    
    def _generate_master_key(self) -> bytes:
        """Generate a new master key"""
        return Fernet.generate_key()
    
    def encrypt_value(self, value: str, key_id: Optional[str] = None) -> Tuple[str, str]:
        """Encrypt a configuration value"""
        if not key_id:
            key_id = f"key_{secrets.token_hex(8)}"
        
        encrypted_value = self.cipher_suite.encrypt(value.encode()).decode()
        return encrypted_value, key_id
    
    def decrypt_value(self, encrypted_value: str, key_id: str) -> str:
        """Decrypt a configuration value"""
        try:
            return self.cipher_suite.decrypt(encrypted_value.encode()).decode()
        except Exception as e:
            raise ValueError(f"Failed to decrypt value: {e}")

class ConfigValidator:
    """Validates configuration values against definitions"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def validate_value(self, value: Any, definition: ConfigDefinition) -> Tuple[bool, List[str]]:
        """Validate a configuration value against its definition"""
        errors = []
        
        # Check if required
        if ConfigValidationRule.REQUIRED in definition.validation_rules:
            if value is None or (isinstance(value, str) and not value.strip()):
                errors.append(f"Configuration '{definition.key}' is required")
                return False, errors
        
        # Skip further validation if value is None and not required
        if value is None:
            return True, []
        
        # Type validation
        if not self._validate_type(value, definition.config_type):
            errors.append(f"Configuration '{definition.key}' must be of type {definition.config_type.value}")
        
        # Value-specific validations
        errors.extend(self._validate_constraints(value, definition))
        
        # Allowed values validation
        if definition.allowed_values and value not in definition.allowed_values:
            errors.append(f"Configuration '{definition.key}' must be one of: {definition.allowed_values}")
        
        return len(errors) == 0, errors
    
    def _validate_type(self, value: Any, config_type: ConfigType) -> bool:
        """Validate value type"""
        type_map = {
            ConfigType.STRING: str,
            ConfigType.INTEGER: int,
            ConfigType.FLOAT: (int, float),
            ConfigType.BOOLEAN: bool,
            ConfigType.LIST: list,
            ConfigType.DICT: dict,
            ConfigType.SECRET: str
        }
        
        expected_type = type_map.get(config_type)
        if expected_type:
            return isinstance(value, expected_type)
        
        if config_type == ConfigType.JSON:
            try:
                json.dumps(value)
                return True
            except (TypeError, ValueError):
                return False
        
        return True
    
    def _validate_constraints(self, value: Any, definition: ConfigDefinition) -> List[str]:
        """Validate value constraints"""
        errors = []
        rules = definition.validation_rules
        
        # Numeric constraints
        if ConfigValidationRule.MIN_VALUE in rules:
            min_val = rules[ConfigValidationRule.MIN_VALUE]
            if isinstance(value, (int, float)) and value < min_val:
                errors.append(f"Value must be >= {min_val}")
        
        if ConfigValidationRule.MAX_VALUE in rules:
            max_val = rules[ConfigValidationRule.MAX_VALUE]
            if isinstance(value, (int, float)) and value > max_val:
                errors.append(f"Value must be <= {max_val}")
        
        # String/List length constraints
        if ConfigValidationRule.MIN_LENGTH in rules:
            min_len = rules[ConfigValidationRule.MIN_LENGTH]
            if hasattr(value, '__len__') and len(value) < min_len:
                errors.append(f"Length must be >= {min_len}")
        
        if ConfigValidationRule.MAX_LENGTH in rules:
            max_len = rules[ConfigValidationRule.MAX_LENGTH]
            if hasattr(value, '__len__') and len(value) > max_len:
                errors.append(f"Length must be <= {max_len}")
        
        # Pattern validation
        if ConfigValidationRule.REGEX_PATTERN in rules:
            import re
            pattern = rules[ConfigValidationRule.REGEX_PATTERN]
            if isinstance(value, str) and not re.match(pattern, value):
                errors.append(f"Value must match pattern: {pattern}")
        
        # Format validations
        if ConfigValidationRule.URL_FORMAT in rules and isinstance(value, str):
            if not value.startswith(('http://', 'https://')):
                errors.append("Value must be a valid URL")
        
        if ConfigValidationRule.EMAIL_FORMAT in rules and isinstance(value, str):
            import re
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if not re.match(email_pattern, value):
                errors.append("Value must be a valid email address")
        
        if ConfigValidationRule.POSITIVE_NUMBER in rules:
            if isinstance(value, (int, float)) and value <= 0:
                errors.append("Value must be a positive number")
        
        return errors

class ConfigurationManager:
    """Main configuration management system"""
    
    def __init__(self, config_dir: str = "config", 
                 environment: EnvironmentType = EnvironmentType.DEVELOPMENT):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)
        self.environment = environment
        self.logger = logging.getLogger(__name__)
        
        # Components
        self.encryption_manager = EncryptionManager()
        self.validator = ConfigValidator()
        
        # Storage
        self.definitions: Dict[str, ConfigDefinition] = {}
        self.configuration_sets: Dict[str, ConfigurationSet] = {}
        self.config: Dict[str, Any] = {}  # Initialize the config dictionary
        self.watchers: List[Callable] = []
        
        # Caching
        self._config_cache: Dict[str, Any] = {}
        self._cache_timestamps: Dict[str, datetime] = {}
        self.cache_ttl_seconds = 300  # 5 minutes
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Load built-in definitions
        self._load_builtin_definitions()
        
        # Load configurations
        self._load_configurations()
    
    def _load_builtin_definitions(self):
        """Load built-in configuration definitions"""
        builtin_definitions = [
            # Analytics Configuration
            ConfigDefinition(
                key="analytics.posthog.api_key",
                description="PostHog API key for analytics integration",
                config_type=ConfigType.SECRET,
                scope=ConfigScope.INTEGRATION,
                category="analytics",
                environment_specific=True,
                sensitive=True,
                default_value="demo_posthog_key",  # Default for development
                validation_rules={ConfigValidationRule.NON_EMPTY_STRING: True}
            ),
            ConfigDefinition(
                key="analytics.posthog.host",
                description="PostHog host URL",
                config_type=ConfigType.STRING,
                scope=ConfigScope.INTEGRATION,
                default_value="https://app.posthog.com",
                category="analytics",
                validation_rules={ConfigValidationRule.URL_FORMAT: True}
            ),
            ConfigDefinition(
                key="analytics.batch_size",
                description="Batch size for analytics event processing",
                config_type=ConfigType.INTEGER,
                scope=ConfigScope.ANALYTICS,
                default_value=1000,
                category="analytics",
                validation_rules={
                    ConfigValidationRule.MIN_VALUE: 1,
                    ConfigValidationRule.MAX_VALUE: 10000
                }
            ),
            
            # Friction Detection Configuration
            ConfigDefinition(
                key="friction_detection.abandonment_threshold",
                description="Abandonment rate threshold for friction detection",
                config_type=ConfigType.FLOAT,
                scope=ConfigScope.ANALYTICS,
                default_value=0.3,
                category="friction_detection",
                validation_rules={
                    ConfigValidationRule.MIN_VALUE: 0.0,
                    ConfigValidationRule.MAX_VALUE: 1.0
                }
            ),
            ConfigDefinition(
                key="friction_detection.min_session_count",
                description="Minimum sessions required for friction analysis",
                config_type=ConfigType.INTEGER,
                scope=ConfigScope.ANALYTICS,
                default_value=100,
                category="friction_detection",
                validation_rules={ConfigValidationRule.POSITIVE_NUMBER: True}
            ),
            ConfigDefinition(
                key="friction_detection.confidence_threshold",
                description="Minimum confidence threshold for friction patterns",
                config_type=ConfigType.FLOAT,
                scope=ConfigScope.ANALYTICS,
                default_value=0.7,
                category="friction_detection",
                validation_rules={
                    ConfigValidationRule.MIN_VALUE: 0.0,
                    ConfigValidationRule.MAX_VALUE: 1.0
                }
            ),
            
            # Recommendation Engine Configuration
            ConfigDefinition(
                key="recommendations.max_recommendations_per_flow",
                description="Maximum recommendations to generate per flow",
                config_type=ConfigType.INTEGER,
                scope=ConfigScope.ANALYTICS,
                default_value=10,
                category="recommendations",
                validation_rules={
                    ConfigValidationRule.MIN_VALUE: 1,
                    ConfigValidationRule.MAX_VALUE: 50
                }
            ),
            ConfigDefinition(
                key="recommendations.min_impact_threshold",
                description="Minimum estimated impact for recommendations",
                config_type=ConfigType.FLOAT,
                scope=ConfigScope.ANALYTICS,
                default_value=0.05,
                category="recommendations",
                validation_rules={
                    ConfigValidationRule.MIN_VALUE: 0.0,
                    ConfigValidationRule.MAX_VALUE: 1.0
                }
            ),
            
            # Database Configuration
            ConfigDefinition(
                key="database.connection_string",
                description="Database connection string",
                config_type=ConfigType.SECRET,
                scope=ConfigScope.GLOBAL,
                category="database",
                environment_specific=True,
                sensitive=True,
                default_value="sqlite:///demo.db",  # Default for development
                validation_rules={ConfigValidationRule.NON_EMPTY_STRING: True}
            ),
            ConfigDefinition(
                key="database.pool_size",
                description="Database connection pool size",
                config_type=ConfigType.INTEGER,
                scope=ConfigScope.GLOBAL,
                default_value=10,
                category="database",
                validation_rules={
                    ConfigValidationRule.MIN_VALUE: 1,
                    ConfigValidationRule.MAX_VALUE: 100
                }
            ),
            
            # Email Configuration
            ConfigDefinition(
                key="email.smtp_host",
                description="SMTP server hostname",
                config_type=ConfigType.STRING,
                scope=ConfigScope.INTEGRATION,
                category="email",
                environment_specific=True,
                default_value="localhost",  # Default for development
                validation_rules={ConfigValidationRule.NON_EMPTY_STRING: True}
            ),
            ConfigDefinition(
                key="email.smtp_port",
                description="SMTP server port",
                config_type=ConfigType.INTEGER,
                scope=ConfigScope.INTEGRATION,
                default_value=587,
                category="email",
                validation_rules={
                    ConfigValidationRule.MIN_VALUE: 1,
                    ConfigValidationRule.MAX_VALUE: 65535
                }
            ),
            ConfigDefinition(
                key="email.from_address",
                description="Default from email address",
                config_type=ConfigType.STRING,
                scope=ConfigScope.INTEGRATION,
                category="email",
                validation_rules={ConfigValidationRule.EMAIL_FORMAT: True}
            ),
            
            # Security Configuration
            ConfigDefinition(
                key="security.encryption_key",
                description="Master encryption key",
                config_type=ConfigType.SECRET,
                scope=ConfigScope.GLOBAL,
                category="security",
                sensitive=True,
                requires_restart=True,
                hot_reloadable=False
            ),
            ConfigDefinition(
                key="security.session_timeout_minutes",
                description="Session timeout in minutes",
                config_type=ConfigType.INTEGER,
                scope=ConfigScope.GLOBAL,
                default_value=60,
                category="security",
                validation_rules={
                    ConfigValidationRule.MIN_VALUE: 5,
                    ConfigValidationRule.MAX_VALUE: 1440
                }
            ),
            
            # Performance Configuration
            ConfigDefinition(
                key="performance.max_concurrent_analyses",
                description="Maximum concurrent analysis operations",
                config_type=ConfigType.INTEGER,
                scope=ConfigScope.GLOBAL,
                default_value=4,
                category="performance",
                validation_rules={
                    ConfigValidationRule.MIN_VALUE: 1,
                    ConfigValidationRule.MAX_VALUE: 20
                }
            ),
            ConfigDefinition(
                key="performance.cache_ttl_seconds",
                description="Cache time-to-live in seconds",
                config_type=ConfigType.INTEGER,
                scope=ConfigScope.GLOBAL,
                default_value=300,
                category="performance",
                validation_rules={ConfigValidationRule.POSITIVE_NUMBER: True}
            ),
            
            # Feature Flags
            ConfigDefinition(
                key="features.advanced_clustering_enabled",
                description="Enable advanced friction clustering algorithms",
                config_type=ConfigType.BOOLEAN,
                scope=ConfigScope.FEATURE,
                default_value=True,
                category="features",
                hot_reloadable=True
            ),
            ConfigDefinition(
                key="features.real_time_analysis_enabled",
                description="Enable real-time analysis capabilities",
                config_type=ConfigType.BOOLEAN,
                scope=ConfigScope.FEATURE,
                default_value=False,
                category="features",
                requires_restart=True
            ),
            ConfigDefinition(
                key="features.ai_recommendations_enabled",
                description="Enable AI-powered recommendation generation",
                config_type=ConfigType.BOOLEAN,
                scope=ConfigScope.FEATURE,
                default_value=True,
                category="features"
            )
        ]
        
        for definition in builtin_definitions:
            self.definitions[definition.key] = definition
    
    def _load_configurations(self):
        """Load configurations from files and environment"""
        # Load from configuration files
        self._load_from_files()
        
        # Load from environment variables
        self._load_from_environment()
        
        # Validate all configurations
        self._validate_all_configurations()
    
    def _load_from_files(self):
        """Load configurations from YAML/JSON files"""
        config_files = [
            self.config_dir / "default.yaml",
            self.config_dir / f"{self.environment.value}.yaml",
            self.config_dir / "local.yaml"
        ]
        
        for config_file in config_files:
            if config_file.exists():
                try:
                    with open(config_file, 'r') as f:
                        if config_file.suffix == '.yaml':
                            config_data = yaml.safe_load(f)
                        else:
                            config_data = json.load(f)
                    
                    self._process_config_file(config_data, str(config_file))
                    self.logger.info(f"Loaded configuration from {config_file}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to load configuration from {config_file}: {e}")
    
    def _process_config_file(self, config_data: Dict[str, Any], source_file: str):
        """Process configuration data from a file"""
        def process_nested_config(data: Dict[str, Any], prefix: str = ""):
            for key, value in data.items():
                full_key = f"{prefix}.{key}" if prefix else key
                
                if isinstance(value, dict) and not any(
                    full_key.startswith(scope.value) for scope in ConfigScope
                ):
                    # Recursively process nested dictionaries
                    process_nested_config(value, full_key)
                else:
                    # Create configuration value
                    config_value = ConfigValue(
                        key=full_key,
                        value=value,
                        scope=self._determine_scope(full_key),
                        environment=self.environment,
                        source="file",
                        source_file=source_file
                    )
                    
                    # Validate if definition exists
                    if full_key in self.definitions:
                        is_valid, errors = self.validator.validate_value(value, self.definitions[full_key])
                        config_value.is_valid = is_valid
                        config_value.validation_errors = errors
                    
                    # Add to appropriate configuration set
                    set_id = f"{config_value.scope.value}_{self.environment.value}"
                    if set_id not in self.configuration_sets:
                        self.configuration_sets[set_id] = ConfigurationSet(
                            set_id=set_id,
                            name=f"{config_value.scope.value.title()} - {self.environment.value.title()}",
                            scope=config_value.scope,
                            environment=self.environment
                        )
                    
                    self.configuration_sets[set_id].values[full_key] = config_value
        
        process_nested_config(config_data)
    
    def _determine_scope(self, key: str) -> ConfigScope:
        """Determine configuration scope from key"""
        if key.startswith("global."):
            return ConfigScope.GLOBAL
        elif key.startswith("analytics."):
            return ConfigScope.ANALYTICS
        elif key.startswith("integration.") or any(platform in key for platform in ["posthog", "mixpanel", "amplitude", "email", "slack"]):
            return ConfigScope.INTEGRATION
        elif key.startswith("features."):
            return ConfigScope.FEATURE
        elif key.startswith("flow."):
            return ConfigScope.FLOW
        else:
            return ConfigScope.GLOBAL
    
    def _load_from_environment(self):
        """Load configurations from environment variables"""
        env_loader = EnvironmentConfigLoader()
        env_config = env_loader.load_from_environment()
        
        for key, value in env_config.items():
            if key in self.definitions:
                self.config[key] = value
                self.logger.info(f"Loaded configuration from environment: {key}")
    
    def _validate_all_configurations(self):
        """Validate all loaded configurations"""
        validator = ConfigValidator()
        result = validator.validate_config(self.config, self.definitions)
        
        if not result.is_valid:
            self.logger.error(f"Configuration validation failed: {result.errors}")
            for error in result.errors:
                self.logger.error(f"Validation error: {error}")
        
        if result.warnings:
            for warning in result.warnings:
                self.logger.warning(f"Configuration warning: {warning}")
        
        return result.is_valid
    
    def get(self, key: str, default: Any = None, use_cache: bool = True) -> Any:
        """Get configuration value"""
        if use_cache and key in self.config:
            return self.config[key]
        
        # Check if definition exists
        definition = self.definitions.get(key)
        if not definition:
            self.logger.warning(f"No definition found for configuration key: {key}")
            return default
        
        # Use default value from definition if available
        if key not in self.config and definition.default_value is not None:
            return definition.default_value
        
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any, scope: ConfigScope = ConfigScope.GLOBAL) -> bool:
        """Set configuration value"""
        try:
            # Get definition for validation
            definition = self.definitions.get(key)
            if definition:
                # Validate the value
                validator = ConfigValidator()
                temp_config = {key: value}
                temp_definitions = {key: definition}
                result = validator.validate_config(temp_config, temp_definitions)
                
                if not result.is_valid:
                    self.logger.error(f"Configuration validation failed for {key}: {result.errors}")
                    return False
            
            # Store old value for watchers
            old_value = self.config.get(key)
            
            # Set the value
            self.config[key] = value
            
            # Encrypt if sensitive
            if definition and definition.sensitive:
                encrypted_value = self.encryption_manager.encrypt_value(value)
                self.config[key] = encrypted_value
            
            # Notify watchers
            for watcher in self.watchers:
                try:
                    watcher(key, old_value, value)
                except Exception as e:
                    self.logger.error(f"Configuration watcher error: {e}")
            
            self.logger.info(f"Configuration updated: {key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to set configuration {key}: {e}")
            return False
    
    def register_watcher(self, callback: Callable[[str, Any, Any], None]):
        """Register configuration change watcher"""
        self.watchers.append(callback)
        self.logger.info("Configuration watcher registered")
    
    def get_all_config(self) -> Dict[str, Any]:
        """Get all configuration values"""
        return self.config.copy()
    
    def reload_configuration(self):
        """Reload configuration from all sources"""
        try:
            # Clear existing config
            self.config.clear()
            
            # Load built-in defaults
            for key, definition in self.definitions.items():
                if definition.default_value is not None:
                    self.config[key] = definition.default_value
            
            # Load from environment
            self._load_from_environment()
            
            # Load from files if specified
            if hasattr(self, 'config_files'):
                for config_file in self.config_files:
                    if os.path.exists(config_file):
                        env_loader = EnvironmentConfigLoader()
                        file_config = env_loader.load_from_file(config_file)
                        self.config.update(file_config)
            
            # Validate all configurations
            self._validate_all_configurations()
            
            self.logger.info("Configuration reloaded successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to reload configuration: {e}")
            raise

class EnvironmentConfigLoader:
    """Loads configuration from environment variables and files"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.env_prefix = "ONBOARDING_"
    
    def load_from_environment(self) -> Dict[str, Any]:
        """Load configuration from environment variables"""
        env_config = {}
        
        for key, value in os.environ.items():
            if key.startswith(self.env_prefix):
                config_key = key[len(self.env_prefix):].lower()
                env_config[config_key] = self._parse_env_value(value)
        
        return env_config
    
    def load_from_file(self, file_path: str) -> Dict[str, Any]:
        """Load configuration from JSON/YAML file"""
        try:
            with open(file_path, 'r') as f:
                if file_path.endswith('.json'):
                    return json.load(f)
                elif file_path.endswith(('.yml', '.yaml')):
                    import yaml
                    return yaml.safe_load(f)
                else:
                    raise ValueError(f"Unsupported file format: {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to load config from file {file_path}: {e}")
            return {}
    
    def _parse_env_value(self, value: str) -> Any:
        """Parse environment variable value to appropriate type"""
        # Try to parse as JSON first
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            pass
        
        # Try boolean
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'
        
        # Try integer
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try float
        try:
            return float(value)
        except ValueError:
            pass
        
        # Return as string
        return value

class ConfigValidator:
    """Enhanced configuration validator with comprehensive validation rules"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.validation_rules = {
            ConfigValidationRule.REQUIRED: self._validate_required,
            ConfigValidationRule.MIN_VALUE: self._validate_min_value,
            ConfigValidationRule.MAX_VALUE: self._validate_max_value,
            ConfigValidationRule.MIN_LENGTH: self._validate_min_length,
            ConfigValidationRule.MAX_LENGTH: self._validate_max_length,
            ConfigValidationRule.REGEX_PATTERN: self._validate_regex_pattern,
            ConfigValidationRule.ALLOWED_VALUES: self._validate_allowed_values,
            ConfigValidationRule.URL_FORMAT: self._validate_url_format,
            ConfigValidationRule.EMAIL_FORMAT: self._validate_email_format,
            ConfigValidationRule.POSITIVE_NUMBER: self._validate_positive_number,
            ConfigValidationRule.NON_EMPTY_STRING: self._validate_non_empty_string
        }
    
    def validate_config(self, config: Dict[str, Any], definitions: Dict[str, ConfigDefinition]) -> 'ValidationResult':
        """Validate configuration against definitions"""
        errors = []
        warnings = []
        
        for key, definition in definitions.items():
            value = config.get(key)
            
            # Type validation
            type_error = self._validate_type(key, value, definition)
            if type_error:
                errors.append(type_error)
                continue
            
            # Rule validation
            for rule, rule_value in definition.validation_rules.items():
                rule_error = self._validate_rule(key, value, rule, rule_value)
                if rule_error:
                    if rule == ConfigValidationRule.REQUIRED:
                        errors.append(rule_error)
                    else:
                        warnings.append(rule_error)
        
        # Check for unknown configuration keys
        unknown_keys = set(config.keys()) - set(definitions.keys())
        for unknown_key in unknown_keys:
            warnings.append(f"Unknown configuration key: {unknown_key}")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    def _validate_type(self, key: str, value: Any, definition: ConfigDefinition) -> Optional[str]:
        """Validate value type"""
        if value is None and not definition.validation_rules.get(ConfigValidationRule.REQUIRED, False):
            return None
        
        expected_type = {
            ConfigType.STRING: str,
            ConfigType.INTEGER: int,
            ConfigType.FLOAT: float,
            ConfigType.BOOLEAN: bool,
            ConfigType.LIST: list,
            ConfigType.DICT: dict,
            ConfigType.JSON: (dict, list),
            ConfigType.SECRET: str,
            ConfigType.ENUM: str
        }.get(definition.config_type)
        
        if expected_type and not isinstance(value, expected_type):
            return f"Configuration '{key}' must be of type {expected_type.__name__}, got {type(value).__name__}"
        
        return None
    
    def _validate_rule(self, key: str, value: Any, rule: ConfigValidationRule, rule_value: Any) -> Optional[str]:
        """Validate against specific rule"""
        if value is None:
            return None
        
        validator = self.validation_rules.get(rule)
        if validator:
            return validator(key, value, rule_value)
        
        return None
    
    def _validate_required(self, key: str, value: Any, rule_value: bool) -> Optional[str]:
        """Validate required field"""
        if rule_value and value is None:
            return f"Configuration '{key}' is required"
        return None
    
    def _validate_min_value(self, key: str, value: Any, rule_value: float) -> Optional[str]:
        """Validate minimum value"""
        if isinstance(value, (int, float)) and value < rule_value:
            return f"Configuration '{key}' must be at least {rule_value}, got {value}"
        return None
    
    def _validate_max_value(self, key: str, value: Any, rule_value: float) -> Optional[str]:
        """Validate maximum value"""
        if isinstance(value, (int, float)) and value > rule_value:
            return f"Configuration '{key}' must be at most {rule_value}, got {value}"
        return None
    
    def _validate_min_length(self, key: str, value: Any, rule_value: int) -> Optional[str]:
        """Validate minimum length"""
        if hasattr(value, '__len__') and len(value) < rule_value:
            return f"Configuration '{key}' must have at least {rule_value} characters/items, got {len(value)}"
        return None
    
    def _validate_max_length(self, key: str, value: Any, rule_value: int) -> Optional[str]:
        """Validate maximum length"""
        if hasattr(value, '__len__') and len(value) > rule_value:
            return f"Configuration '{key}' must have at most {rule_value} characters/items, got {len(value)}"
        return None
    
    def _validate_regex_pattern(self, key: str, value: Any, rule_value: str) -> Optional[str]:
        """Validate regex pattern"""
        if isinstance(value, str) and not re.match(rule_value, value):
            return f"Configuration '{key}' does not match required pattern: {rule_value}"
        return None
    
    def _validate_allowed_values(self, key: str, value: Any, rule_value: List[Any]) -> Optional[str]:
        """Validate allowed values"""
        if value not in rule_value:
            return f"Configuration '{key}' must be one of {rule_value}, got {value}"
        return None
    
    def _validate_url_format(self, key: str, value: Any, rule_value: bool) -> Optional[str]:
        """Validate URL format"""
        if rule_value and isinstance(value, str):
            url_pattern = r'^https?://(?:[-\w.])+(?:[:\d]+)?(?:/(?:[\w/_.])*)?(?:\?(?:[\w&=%.])*)?(?:#(?:[\w.])*)?$'
            if not re.match(url_pattern, value):
                return f"Configuration '{key}' must be a valid URL"
        return None
    
    def _validate_email_format(self, key: str, value: Any, rule_value: bool) -> Optional[str]:
        """Validate email format"""
        if rule_value and isinstance(value, str):
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if not re.match(email_pattern, value):
                return f"Configuration '{key}' must be a valid email address"
        return None
    
    def _validate_positive_number(self, key: str, value: Any, rule_value: bool) -> Optional[str]:
        """Validate positive number"""
        if rule_value and isinstance(value, (int, float)) and value <= 0:
            return f"Configuration '{key}' must be a positive number, got {value}"
        return None
    
    def _validate_non_empty_string(self, key: str, value: Any, rule_value: bool) -> Optional[str]:
        """Validate non-empty string"""
        if rule_value and isinstance(value, str) and not value.strip():
            return f"Configuration '{key}' cannot be empty"
        return None

class ConfigChangeWatcher:
    """Watches for configuration changes and triggers reload"""
    
    def __init__(self, config_manager, watch_files: List[str] = None):
        self.config_manager = config_manager
        self.watch_files = watch_files or []
        self.file_timestamps = {}
        self.is_watching = False
        self.watch_thread = None
        self.logger = logging.getLogger(__name__)
    
    def start_watching(self):
        """Start watching for configuration changes"""
        if self.is_watching:
            return
        
        self.is_watching = True
        self.watch_thread = threading.Thread(target=self._watch_loop, daemon=True)
        self.watch_thread.start()
        self.logger.info("Configuration change watcher started")
    
    def stop_watching(self):
        """Stop watching for configuration changes"""
        self.is_watching = False
        if self.watch_thread:
            self.watch_thread.join(timeout=1)
        self.logger.info("Configuration change watcher stopped")
    
    def _watch_loop(self):
        """Main watch loop"""
        # Initialize file timestamps
        for file_path in self.watch_files:
            if os.path.exists(file_path):
                self.file_timestamps[file_path] = os.path.getmtime(file_path)
        
        while self.is_watching:
            try:
                changes_detected = False
                
                for file_path in self.watch_files:
                    if os.path.exists(file_path):
                        current_timestamp = os.path.getmtime(file_path)
                        last_timestamp = self.file_timestamps.get(file_path, 0)
                        
                        if current_timestamp > last_timestamp:
                            self.file_timestamps[file_path] = current_timestamp
                            changes_detected = True
                            self.logger.info(f"Configuration file changed: {file_path}")
                
                if changes_detected:
                    self._trigger_reload()
                
                time.sleep(1)  # Check every second
                
            except Exception as e:
                self.logger.error(f"Error in configuration watcher: {e}")
                time.sleep(5)  # Wait longer on error
    
    def _trigger_reload(self):
        """Trigger configuration reload"""
        try:
            self.config_manager.reload_configuration()
            self.logger.info("Configuration reloaded due to file changes")
        except Exception as e:
            self.logger.error(f"Failed to reload configuration: {e}")

class ConfigExporter:
    """Exports and imports configuration"""
    
    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.logger = logging.getLogger(__name__)
    
    def export_configuration(self, export_path: str, include_secrets: bool = False) -> bool:
        """Export configuration to file"""
        try:
            config_data = {}
            
            for key, value in self.config_manager.get_all_config().items():
                definition = self.config_manager.definitions.get(key)
                
                # Skip secrets unless explicitly included
                if definition and definition.config_type == ConfigType.SECRET and not include_secrets:
                    config_data[key] = "<REDACTED>"
                else:
                    config_data[key] = value
            
            export_data = {
                'exported_at': datetime.utcnow().isoformat(),
                'version': '1.0',
                'configuration': config_data
            }
            
            with open(export_path, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            self.logger.info(f"Configuration exported to: {export_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to export configuration: {e}")
            return False
    
    def import_configuration(self, import_path: str, merge: bool = True) -> bool:
        """Import configuration from file"""
        try:
            with open(import_path, 'r') as f:
                import_data = json.load(f)
            
            config_data = import_data.get('configuration', {})
            
            if merge:
                # Merge with existing configuration
                for key, value in config_data.items():
                    if value != "<REDACTED>":  # Skip redacted secrets
                        self.config_manager.set_config(key, value)
            else:
                # Replace entire configuration
                self.config_manager.config.clear()
                for key, value in config_data.items():
                    if value != "<REDACTED>":
                        self.config_manager.set_config(key, value)
            
            self.logger.info(f"Configuration imported from: {import_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to import configuration: {e}")
            return False

class ConfigBackupManager:
    """Manages configuration backups and restore"""
    
    def __init__(self, config_manager, backup_dir: str = "config_backups"):
        self.config_manager = config_manager
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def create_backup(self, backup_name: str = None) -> str:
        """Create configuration backup"""
        try:
            if not backup_name:
                backup_name = f"config_backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            
            backup_path = self.backup_dir / f"{backup_name}.json"
            
            backup_data = {
                'backup_name': backup_name,
                'created_at': datetime.utcnow().isoformat(),
                'configuration': self.config_manager.get_all_config()
            }
            
            with open(backup_path, 'w') as f:
                json.dump(backup_data, f, indent=2, default=str)
            
            self.logger.info(f"Configuration backup created: {backup_path}")
            return str(backup_path)
            
        except Exception as e:
            self.logger.error(f"Failed to create configuration backup: {e}")
            raise
    
    def restore_backup(self, backup_path: str) -> bool:
        """Restore configuration from backup"""
        try:
            with open(backup_path, 'r') as f:
                backup_data = json.load(f)
            
            config_data = backup_data.get('configuration', {})
            
            # Clear current configuration
            self.config_manager.config.clear()
            
            # Restore from backup
            for key, value in config_data.items():
                self.config_manager.set_config(key, value)
            
            self.logger.info(f"Configuration restored from backup: {backup_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to restore configuration backup: {e}")
            return False
    
    def list_backups(self) -> List[Dict[str, Any]]:
        """List available configuration backups"""
        backups = []
        
        for backup_file in self.backup_dir.glob("*.json"):
            try:
                with open(backup_file, 'r') as f:
                    backup_data = json.load(f)
                
                backups.append({
                    'backup_name': backup_data.get('backup_name'),
                    'created_at': backup_data.get('created_at'),
                    'file_path': str(backup_file),
                    'file_size': backup_file.stat().st_size
                })
                
            except Exception as e:
                self.logger.warning(f"Failed to read backup file {backup_file}: {e}")
        
        return sorted(backups, key=lambda x: x['created_at'], reverse=True)
    
    def cleanup_old_backups(self, max_backups: int = 10):
        """Clean up old backup files"""
        backups = self.list_backups()
        
        if len(backups) > max_backups:
            for backup in backups[max_backups:]:
                try:
                    os.remove(backup['file_path'])
                    self.logger.info(f"Deleted old backup: {backup['backup_name']}")
                except Exception as e:
                    self.logger.warning(f"Failed to delete backup {backup['backup_name']}: {e}")

@dataclass 
class ValidationResult:
    """Validation result containing errors and warnings"""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
