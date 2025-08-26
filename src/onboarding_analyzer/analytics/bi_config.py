"""
Business Intelligence System Configuration and Initialization

Complete configuration management and system initialization for the BI platform.
Provides easy setup and deployment of the comprehensive BI system.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from pathlib import Path
import json
import os
from dotenv import load_dotenv

# Import BI components
from .bi_integration import (
    ComprehensiveBISystem, BISystemConfig, create_bi_system,
    set_global_bi_system, get_global_bi_system
)
from .business_intelligence import KPIDefinition
from .bi_export_system import EmailConfig

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

@dataclass
class DefaultKPIConfiguration:
    """Default KPI configurations for common business metrics."""
    
    @staticmethod
    def get_default_kpis() -> List[KPIDefinition]:
        """Get list of default KPI definitions."""
        return [
            # User Engagement KPIs
            KPIDefinition(
                kpi_id="daily_active_users",
                kpi_name="Daily Active Users",
                description="Number of unique users active daily",
                category="engagement",
                calculation_method="count",
                aggregation_field="user_id",
                filter_conditions={"event_type": "login"},
                target_value=1000,
                warning_threshold=800,
                critical_threshold=500,
                format_type="count"
            ),
            
            KPIDefinition(
                kpi_id="user_retention_rate",
                kpi_name="User Retention Rate",
                description="Percentage of users returning within 7 days",
                category="engagement",
                calculation_method="percentage",
                aggregation_field="user_id",
                target_value=70.0,
                warning_threshold=60.0,
                critical_threshold=40.0,
                format_type="percentage"
            ),
            
            KPIDefinition(
                kpi_id="session_duration_avg",
                kpi_name="Average Session Duration",
                description="Average time users spend in each session",
                category="engagement",
                calculation_method="average",
                aggregation_field="session_duration",
                target_value=15.0,
                warning_threshold=10.0,
                critical_threshold=5.0,
                format_type="duration"
            ),
            
            # Business Performance KPIs
            KPIDefinition(
                kpi_id="conversion_rate",
                kpi_name="Conversion Rate",
                description="Percentage of users completing desired actions",
                category="business",
                calculation_method="ratio",
                aggregation_field="conversion_events",
                target_value=5.0,
                warning_threshold=3.0,
                critical_threshold=1.0,
                format_type="percentage"
            ),
            
            KPIDefinition(
                kpi_id="revenue_per_user",
                kpi_name="Revenue Per User",
                description="Average revenue generated per user",
                category="financial",
                calculation_method="average",
                aggregation_field="revenue",
                target_value=50.0,
                warning_threshold=30.0,
                critical_threshold=15.0,
                format_type="currency"
            ),
            
            KPIDefinition(
                kpi_id="customer_acquisition_cost",
                kpi_name="Customer Acquisition Cost",
                description="Cost to acquire each new customer",
                category="financial",
                calculation_method="ratio",
                aggregation_field="marketing_spend",
                target_value=25.0,
                warning_threshold=35.0,
                critical_threshold=50.0,
                format_type="currency"
            ),
            
            # Operational KPIs
            KPIDefinition(
                kpi_id="system_uptime",
                kpi_name="System Uptime",
                description="Percentage of time system is operational",
                category="operational",
                calculation_method="percentage",
                aggregation_field="uptime_events",
                target_value=99.9,
                warning_threshold=99.0,
                critical_threshold=95.0,
                format_type="percentage"
            ),
            
            KPIDefinition(
                kpi_id="error_rate",
                kpi_name="Error Rate",
                description="Percentage of requests resulting in errors",
                category="operational",
                calculation_method="percentage",
                aggregation_field="error_events",
                target_value=1.0,
                warning_threshold=3.0,
                critical_threshold=5.0,
                format_type="percentage"
            ),
            
            KPIDefinition(
                kpi_id="response_time_avg",
                kpi_name="Average Response Time",
                description="Average time to respond to requests",
                category="operational",
                calculation_method="average",
                aggregation_field="response_time",
                target_value=200.0,
                warning_threshold=500.0,
                critical_threshold=1000.0,
                format_type="duration"
            ),
            
            # Growth KPIs
            KPIDefinition(
                kpi_id="user_growth_rate",
                kpi_name="User Growth Rate",
                description="Monthly percentage growth in user base",
                category="growth",
                calculation_method="percentage",
                aggregation_field="new_users",
                target_value=10.0,
                warning_threshold=5.0,
                critical_threshold=0.0,
                format_type="percentage"
            )
        ]

class BISystemConfigurator:
    """Configuration manager for BI system setup."""
    
    def __init__(self):
        self.config_cache: Dict[str, Any] = {}
        
    def create_default_config(self, system_name: str = "Default BI System") -> BISystemConfig:
        """Create default BI system configuration."""
        try:
            # Generate system ID
            system_id = f"bi_system_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Email configuration from environment
            email_config = self._create_email_config()
            
            # Default data sources
            data_sources = [
                {
                    'source_id': 'primary_db',
                    'source_type': 'database',
                    'connection_string': os.getenv('DATABASE_URL', 'sqlite:///./bi_data.db'),
                    'description': 'Primary application database'
                },
                {
                    'source_id': 'analytics_events',
                    'source_type': 'event_stream',
                    'connection_string': os.getenv('EVENTS_SOURCE', ''),
                    'description': 'Real-time analytics events'
                }
            ]
            
            # Scheduler configuration
            scheduler_config = {
                'max_workers': int(os.getenv('BI_SCHEDULER_WORKERS', '5')),
                'check_interval': int(os.getenv('BI_SCHEDULER_INTERVAL', '60')),
                'max_execution_history': int(os.getenv('BI_MAX_HISTORY', '1000'))
            }
            
            # Notification configuration
            notification_config = {
                'default_priority': os.getenv('BI_DEFAULT_PRIORITY', 'normal'),
                'retry_attempts': int(os.getenv('BI_NOTIFICATION_RETRIES', '3')),
                'webhook_timeout': int(os.getenv('BI_WEBHOOK_TIMEOUT', '30'))
            }
            
            # Dashboard configuration
            dashboard_config = {
                'auto_refresh': os.getenv('BI_DASHBOARD_AUTO_REFRESH', 'true').lower() == 'true',
                'refresh_interval': int(os.getenv('BI_DASHBOARD_REFRESH_INTERVAL', '300')),
                'max_widgets': int(os.getenv('BI_MAX_DASHBOARD_WIDGETS', '20'))
            }
            
            # Export base path
            export_base_path = os.getenv('BI_EXPORT_PATH', './exports')
            
            # Create configuration
            config = BISystemConfig(
                system_id=system_id,
                system_name=system_name,
                data_sources=data_sources,
                default_kpis=DefaultKPIConfiguration.get_default_kpis(),
                email_config=email_config,
                export_base_path=export_base_path,
                scheduler_config=scheduler_config,
                notification_config=notification_config,
                dashboard_config=dashboard_config
            )
            
            logger.info(f"Created default BI system configuration: {system_id}")
            return config
            
        except Exception as e:
            logger.error(f"Failed to create default config: {e}")
            raise
    
    def load_config_from_file(self, config_path: str) -> BISystemConfig:
        """Load BI system configuration from file."""
        try:
            config_file = Path(config_path)
            if not config_file.exists():
                raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
            with open(config_file, 'r') as f:
                config_data = json.load(f)
            
            # Create email config if present
            email_config = None
            if 'email_config' in config_data:
                email_data = config_data['email_config']
                email_config = EmailConfig(
                    smtp_server=email_data['smtp_server'],
                    smtp_port=email_data['smtp_port'],
                    username=email_data['username'],
                    password=email_data['password'],
                    use_tls=email_data.get('use_tls', True),
                    use_ssl=email_data.get('use_ssl', False),
                    sender_email=email_data.get('sender_email'),
                    sender_name=email_data.get('sender_name', 'BI System')
                )
            
            # Create KPI definitions
            default_kpis = []
            for kpi_data in config_data.get('default_kpis', []):
                kpi = KPIDefinition(
                    kpi_id=kpi_data['kpi_id'],
                    kpi_name=kpi_data['kpi_name'],
                    description=kpi_data['description'],
                    category=kpi_data['category'],
                    calculation_method=kpi_data['calculation_method'],
                    aggregation_field=kpi_data['aggregation_field'],
                    filter_conditions=kpi_data.get('filter_conditions', {}),
                    target_value=kpi_data.get('target_value'),
                    warning_threshold=kpi_data.get('warning_threshold'),
                    critical_threshold=kpi_data.get('critical_threshold'),
                    format_type=kpi_data.get('format_type', 'count')
                )
                default_kpis.append(kpi)
            
            # Create configuration
            config = BISystemConfig(
                system_id=config_data['system_id'],
                system_name=config_data['system_name'],
                data_sources=config_data.get('data_sources', []),
                default_kpis=default_kpis,
                email_config=email_config,
                export_base_path=config_data.get('export_base_path', './exports'),
                scheduler_config=config_data.get('scheduler_config', {}),
                notification_config=config_data.get('notification_config', {}),
                dashboard_config=config_data.get('dashboard_config', {})
            )
            
            logger.info(f"Loaded BI system configuration from: {config_path}")
            return config
            
        except Exception as e:
            logger.error(f"Failed to load config from file: {e}")
            raise
    
    def save_config_to_file(self, config: BISystemConfig, config_path: str):
        """Save BI system configuration to file."""
        try:
            # Convert config to dictionary
            config_data = {
                'system_id': config.system_id,
                'system_name': config.system_name,
                'data_sources': config.data_sources,
                'export_base_path': config.export_base_path,
                'scheduler_config': config.scheduler_config,
                'notification_config': config.notification_config,
                'dashboard_config': config.dashboard_config
            }
            
            # Add email config if present
            if config.email_config:
                config_data['email_config'] = {
                    'smtp_server': config.email_config.smtp_server,
                    'smtp_port': config.email_config.smtp_port,
                    'username': config.email_config.username,
                    'password': config.email_config.password,
                    'use_tls': config.email_config.use_tls,
                    'use_ssl': config.email_config.use_ssl,
                    'sender_email': config.email_config.sender_email,
                    'sender_name': config.email_config.sender_name
                }
            
            # Add KPI definitions
            config_data['default_kpis'] = []
            for kpi in config.default_kpis:
                kpi_data = {
                    'kpi_id': kpi.kpi_id,
                    'kpi_name': kpi.kpi_name,
                    'description': kpi.description,
                    'category': kpi.category,
                    'calculation_method': kpi.calculation_method,
                    'aggregation_field': kpi.aggregation_field,
                    'filter_conditions': kpi.filter_conditions,
                    'target_value': kpi.target_value,
                    'warning_threshold': kpi.warning_threshold,
                    'critical_threshold': kpi.critical_threshold,
                    'format_type': kpi.format_type
                }
                config_data['default_kpis'].append(kpi_data)
            
            # Save to file
            config_file = Path(config_path)
            config_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(config_file, 'w') as f:
                json.dump(config_data, f, indent=2)
            
            logger.info(f"Saved BI system configuration to: {config_path}")
            
        except Exception as e:
            logger.error(f"Failed to save config to file: {e}")
            raise
    
    def _create_email_config(self) -> Optional[EmailConfig]:
        """Create email configuration from environment variables."""
        try:
            smtp_server = os.getenv('BI_SMTP_SERVER')
            if not smtp_server:
                logger.warning("No SMTP server configured")
                return None
            
            return EmailConfig(
                smtp_server=smtp_server,
                smtp_port=int(os.getenv('BI_SMTP_PORT', '587')),
                username=os.getenv('BI_SMTP_USERNAME', ''),
                password=os.getenv('BI_SMTP_PASSWORD', ''),
                use_tls=os.getenv('BI_SMTP_TLS', 'true').lower() == 'true',
                use_ssl=os.getenv('BI_SMTP_SSL', 'false').lower() == 'true',
                sender_email=os.getenv('BI_SENDER_EMAIL'),
                sender_name=os.getenv('BI_SENDER_NAME', 'BI System')
            )
            
        except Exception as e:
            logger.warning(f"Failed to create email config: {e}")
            return None

class BISystemInitializer:
    """Initializer for BI system deployment."""
    
    def __init__(self):
        self.configurator = BISystemConfigurator()
    
    async def initialize_system(self, config: Optional[BISystemConfig] = None,
                              config_file: Optional[str] = None) -> ComprehensiveBISystem:
        """Initialize the complete BI system."""
        try:
            # Determine configuration
            if config_file:
                system_config = self.configurator.load_config_from_file(config_file)
            elif config:
                system_config = config
            else:
                system_config = self.configurator.create_default_config()
            
            # Create BI system
            bi_system = create_bi_system(system_config)
            
            # Initialize system
            await bi_system.initialize_system()
            
            # Set as global instance
            set_global_bi_system(bi_system)
            
            logger.info(f"BI System initialized successfully: {system_config.system_id}")
            return bi_system
            
        except Exception as e:
            logger.error(f"Failed to initialize BI system: {e}")
            raise
    
    async def quick_start(self, system_name: str = "Quick Start BI System") -> ComprehensiveBISystem:
        """Quick start method for immediate BI system deployment."""
        try:
            logger.info("Starting quick BI system deployment...")
            
            # Create default configuration
            config = self.configurator.create_default_config(system_name)
            
            # Initialize system
            bi_system = await self.initialize_system(config)
            
            # Create sample workflows
            await self._create_sample_workflows(bi_system)
            
            logger.info("Quick start BI system deployment completed")
            return bi_system
            
        except Exception as e:
            logger.error(f"Quick start deployment failed: {e}")
            raise
    
    async def _create_sample_workflows(self, bi_system: ComprehensiveBISystem):
        """Create sample workflows for demonstration."""
        try:
            # Daily performance report
            daily_report_config = {
                'name': 'Daily Performance Report',
                'report_config': {
                    'report_name': 'Daily Business Performance',
                    'report_type': 'operational',
                    'kpi_ids': ['daily_active_users', 'conversion_rate', 'system_uptime'],
                    'time_period': 'daily'
                },
                'trigger_config': {
                    'schedule_type': 'daily',
                    'schedule_config': {'hour': 9, 'minute': 0}
                },
                'export_config': {
                    'format': 'excel',
                    'include_visualizations': True
                },
                'notification_config': {
                    'type': 'email',
                    'recipients': ['business@company.com'],
                    'subject': 'Daily Performance Report'
                }
            }
            
            await bi_system.create_scheduled_report_workflow(daily_report_config)
            
            # Weekly executive summary
            weekly_report_config = {
                'name': 'Weekly Executive Summary',
                'report_config': {
                    'report_name': 'Weekly Executive Summary',
                    'report_type': 'executive',
                    'kpi_ids': ['user_growth_rate', 'revenue_per_user', 'customer_acquisition_cost'],
                    'time_period': 'weekly'
                },
                'trigger_config': {
                    'schedule_type': 'weekly',
                    'schedule_config': {'weekday': 0, 'hour': 8, 'minute': 0}  # Monday
                },
                'export_config': {
                    'format': 'pdf',
                    'include_visualizations': True
                },
                'notification_config': {
                    'type': 'email',
                    'recipients': ['executives@company.com'],
                    'subject': 'Weekly Executive Summary'
                }
            }
            
            await bi_system.create_scheduled_report_workflow(weekly_report_config)
            
            logger.info("Sample workflows created")
            
        except Exception as e:
            logger.warning(f"Failed to create sample workflows: {e}")

# Global initializer instance
_initializer = BISystemInitializer()

# Convenience functions
async def quick_start_bi_system(system_name: str = "Default BI System") -> ComprehensiveBISystem:
    """Quick start function for immediate BI system deployment."""
    return await _initializer.quick_start(system_name)

async def initialize_bi_system_from_config(config_file: str) -> ComprehensiveBISystem:
    """Initialize BI system from configuration file."""
    return await _initializer.initialize_system(config_file=config_file)

async def initialize_bi_system_with_config(config: BISystemConfig) -> ComprehensiveBISystem:
    """Initialize BI system with provided configuration."""
    return await _initializer.initialize_system(config=config)

def create_default_bi_config(system_name: str = "Default BI System") -> BISystemConfig:
    """Create default BI system configuration."""
    configurator = BISystemConfigurator()
    return configurator.create_default_config(system_name)

def save_bi_config(config: BISystemConfig, config_path: str):
    """Save BI configuration to file."""
    configurator = BISystemConfigurator()
    configurator.save_config_to_file(config, config_path)

def load_bi_config(config_path: str) -> BISystemConfig:
    """Load BI configuration from file."""
    configurator = BISystemConfigurator()
    return configurator.load_config_from_file(config_path)
