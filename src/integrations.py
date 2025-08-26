"""
Integration Configuration and Adapters
"""

from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class IntegrationType(Enum):
    """Types of integrations"""
    ANALYTICS_PLATFORM = "analytics_platform"
    CRM_SYSTEM = "crm_system"
    EMAIL_SERVICE = "email_service"
    DATABASE = "database"
    API_WEBHOOK = "api_webhook"
    MESSAGE_QUEUE = "message_queue"

class SyncOperation(Enum):
    """Synchronization operation types"""
    PULL = "pull"
    PUSH = "push"
    BIDIRECTIONAL = "bidirectional"
    REAL_TIME = "real_time"
    BATCH = "batch"

@dataclass
class IntegrationConfiguration:
    """Configuration for external integrations"""
    integration_id: str
    integration_type: IntegrationType
    name: str
    description: Optional[str] = None
    
    # Connection settings
    endpoint_url: Optional[str] = None
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    authentication_method: str = "api_key"  # api_key, oauth2, basic_auth
    
    # Sync settings
    sync_operation: SyncOperation = SyncOperation.PULL
    sync_frequency_minutes: int = 60
    batch_size: int = 1000
    timeout_seconds: int = 30
    
    # Data mapping
    field_mappings: Dict[str, str] = field(default_factory=dict)
    transformation_rules: List[Dict[str, Any]] = field(default_factory=list)
    
    # Status
    enabled: bool = True
    last_sync: Optional[datetime] = None
    error_count: int = 0
    last_error: Optional[str] = None
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    tags: List[str] = field(default_factory=list)

class BaseIntegrationAdapter:
    """Base class for integration adapters"""
    
    def __init__(self, config: IntegrationConfiguration):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
    def connect(self) -> bool:
        """Establish connection to external system"""
        try:
            # Base implementation - override in subclasses
            self.logger.info(f"Connecting to {self.config.name}")
            return True
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            return False
            
    def test_connection(self) -> Dict[str, Any]:
        """Test connection and return status"""
        try:
            connected = self.connect()
            return {
                "status": "success" if connected else "failed",
                "integration_id": self.config.integration_id,
                "integration_type": self.config.integration_type.value,
                "tested_at": datetime.utcnow().isoformat(),
                "message": "Connection successful" if connected else "Connection failed"
            }
        except Exception as e:
            return {
                "status": "error",
                "integration_id": self.config.integration_id,
                "integration_type": self.config.integration_type.value,
                "tested_at": datetime.utcnow().isoformat(),
                "error": str(e)
            }
    
    def sync_data(self) -> Dict[str, Any]:
        """Perform data synchronization"""
        try:
            # Base implementation - override in subclasses
            self.logger.info(f"Syncing data for {self.config.name}")
            return {
                "status": "success",
                "records_processed": 0,
                "sync_started": datetime.utcnow().isoformat(),
                "sync_completed": datetime.utcnow().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Sync failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "sync_started": datetime.utcnow().isoformat()
            }

class AnalyticsPlatformAdapter(BaseIntegrationAdapter):
    """Adapter for analytics platforms (PostHog, Mixpanel, Amplitude, etc.)"""
    
    def __init__(self, config: IntegrationConfiguration):
        super().__init__(config)
        self.platform_name = config.name.lower()
        
    def fetch_events(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Fetch events from analytics platform"""
        try:
            self.logger.info(f"Fetching events from {self.platform_name}")
            # Implementation would depend on specific platform
            return []
        except Exception as e:
            self.logger.error(f"Failed to fetch events: {e}")
            return []
    
    def fetch_user_profiles(self, user_ids: List[str]) -> List[Dict[str, Any]]:
        """Fetch user profiles from analytics platform"""
        try:
            self.logger.info(f"Fetching user profiles from {self.platform_name}")
            # Implementation would depend on specific platform
            return []
        except Exception as e:
            self.logger.error(f"Failed to fetch user profiles: {e}")
            return []

class CRMSystemAdapter(BaseIntegrationAdapter):
    """Adapter for CRM systems (Salesforce, HubSpot, etc.)"""
    
    def __init__(self, config: IntegrationConfiguration):
        super().__init__(config)
        self.crm_name = config.name.lower()
        
    def sync_users(self) -> Dict[str, Any]:
        """Sync user data with CRM system"""
        try:
            self.logger.info(f"Syncing users with {self.crm_name}")
            # Implementation would depend on specific CRM
            return {
                "status": "success",
                "users_synced": 0,
                "sync_type": "users"
            }
        except Exception as e:
            self.logger.error(f"Failed to sync users: {e}")
            return {
                "status": "error",
                "error": str(e),
                "sync_type": "users"
            }
    
    def push_conversion_data(self, conversions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Push conversion data to CRM system"""
        try:
            self.logger.info(f"Pushing {len(conversions)} conversions to {self.crm_name}")
            # Implementation would depend on specific CRM
            return {
                "status": "success",
                "conversions_pushed": len(conversions),
                "sync_type": "conversions"
            }
        except Exception as e:
            self.logger.error(f"Failed to push conversions: {e}")
            return {
                "status": "error",
                "error": str(e),
                "sync_type": "conversions"
            }

# Integration factory
def create_integration_adapter(config: IntegrationConfiguration) -> BaseIntegrationAdapter:
    """Factory function to create appropriate integration adapter"""
    if config.integration_type == IntegrationType.ANALYTICS_PLATFORM:
        return AnalyticsPlatformAdapter(config)
    elif config.integration_type == IntegrationType.CRM_SYSTEM:
        return CRMSystemAdapter(config)
    else:
        return BaseIntegrationAdapter(config)

# Mock configurations for common integrations
COMMON_INTEGRATIONS = {
    "posthog": IntegrationConfiguration(
        integration_id="posthog",
        integration_type=IntegrationType.ANALYTICS_PLATFORM,
        name="PostHog",
        description="PostHog analytics platform integration",
        authentication_method="api_key",
        sync_operation=SyncOperation.PULL,
        sync_frequency_minutes=15
    ),
    "mixpanel": IntegrationConfiguration(
        integration_id="mixpanel",
        integration_type=IntegrationType.ANALYTICS_PLATFORM,
        name="Mixpanel",
        description="Mixpanel analytics platform integration",
        authentication_method="basic_auth",
        sync_operation=SyncOperation.PULL,
        sync_frequency_minutes=30
    ),
    "amplitude": IntegrationConfiguration(
        integration_id="amplitude",
        integration_type=IntegrationType.ANALYTICS_PLATFORM,
        name="Amplitude",
        description="Amplitude analytics platform integration",
        authentication_method="api_key",
        sync_operation=SyncOperation.PULL,
        sync_frequency_minutes=20
    ),
    "salesforce": IntegrationConfiguration(
        integration_id="salesforce",
        integration_type=IntegrationType.CRM_SYSTEM,
        name="Salesforce",
        description="Salesforce CRM integration",
        authentication_method="oauth2",
        sync_operation=SyncOperation.BIDIRECTIONAL,
        sync_frequency_minutes=60
    )
}