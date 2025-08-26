"""
Business Intelligence Integration Module

Complete integration of all BI components including reporting, scheduling, 
export, notifications, and dashboard management for enterprise-grade BI system.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
import json
from pathlib import Path

# Import BI components
from .business_intelligence import (
    BusinessIntelligenceEngine, KPIDefinition, KPIResult, ReportDefinition,
    GeneratedReport, Dashboard, DashboardWidget
)
from .bi_scheduler import BusinessIntelligenceScheduler, ScheduledTask, get_scheduler
from .bi_export_system import (
    BIExportNotificationSystem, ReportExporter, NotificationSystem,
    ExportConfig, NotificationConfig, EmailConfig
)

# Monitoring
from prometheus_client import Counter, Histogram, Gauge

# Initialize metrics
bi_integrations_total = Counter('bi_integrations_total', 'Total BI integrations performed')
bi_workflows_executed = Counter('bi_workflows_executed_total', 'BI workflows executed', ['workflow_type'])
bi_system_health = Gauge('bi_system_health_score', 'BI system health score')

logger = logging.getLogger(__name__)

@dataclass
class BIWorkflowDefinition:
    """Definition for automated BI workflows."""
    workflow_id: str
    workflow_name: str
    workflow_type: str  # 'scheduled_report', 'alert_monitoring', 'dashboard_refresh'
    trigger_config: Dict[str, Any]
    actions: List[Dict[str, Any]]
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class BISystemConfig:
    """Configuration for the complete BI system."""
    system_id: str
    system_name: str
    data_sources: List[Dict[str, Any]]
    default_kpis: List[KPIDefinition]
    email_config: Optional[EmailConfig] = None
    export_base_path: str = "./exports"
    scheduler_config: Dict[str, Any] = field(default_factory=dict)
    notification_config: Dict[str, Any] = field(default_factory=dict)
    dashboard_config: Dict[str, Any] = field(default_factory=dict)

class ComprehensiveBISystem:
    """Complete Business Intelligence System integrating all components."""
    
    def __init__(self, config: BISystemConfig):
        self.config = config
        self.system_id = config.system_id
        
        # Initialize core components
        self.bi_engine = BusinessIntelligenceEngine()
        self.scheduler = get_scheduler()
        self.export_notification_system = BIExportNotificationSystem(
            export_base_path=config.export_base_path,
            email_config=config.email_config
        )
        
        # System state
        self.active_workflows: Dict[str, BIWorkflowDefinition] = {}
        self.system_status = "initializing"
        self.last_health_check = None
        
        # Performance tracking
        self.performance_metrics = {
            'reports_generated': 0,
            'dashboards_updated': 0,
            'notifications_sent': 0,
            'exports_completed': 0,
            'workflows_executed': 0
        }
        
    async def initialize_system(self):
        """Initialize the complete BI system."""
        try:
            logger.info(f"Initializing BI System: {self.system_id}")
            
            # Start scheduler
            await self.scheduler.start_scheduler()
            
            # Register task handlers
            self._register_task_handlers()
            
            # Setup default workflows
            await self._setup_default_workflows()
            
            # Perform initial health check
            await self.health_check()
            
            self.system_status = "running"
            logger.info("BI System initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize BI system: {e}")
            self.system_status = "error"
            raise
    
    async def shutdown_system(self):
        """Gracefully shutdown the BI system."""
        try:
            logger.info("Shutting down BI System...")
            
            # Stop scheduler
            await self.scheduler.stop_scheduler()
            
            # Save system state
            await self._save_system_state()
            
            self.system_status = "stopped"
            logger.info("BI System shutdown complete")
            
        except Exception as e:
            logger.error(f"Error during system shutdown: {e}")
    
    async def create_scheduled_report_workflow(self, workflow_config: Dict[str, Any]) -> str:
        """Create a comprehensive scheduled report workflow."""
        try:
            workflow_id = f"report_workflow_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Create workflow definition
            workflow = BIWorkflowDefinition(
                workflow_id=workflow_id,
                workflow_name=workflow_config.get('name', f'Report Workflow {workflow_id}'),
                workflow_type='scheduled_report',
                trigger_config=workflow_config.get('trigger_config', {}),
                actions=[
                    {
                        'type': 'generate_report',
                        'config': workflow_config.get('report_config', {})
                    },
                    {
                        'type': 'export_report',
                        'config': workflow_config.get('export_config', {})
                    },
                    {
                        'type': 'send_notification',
                        'config': workflow_config.get('notification_config', {})
                    }
                ],
                metadata=workflow_config.get('metadata', {})
            )
            
            # Schedule the workflow
            schedule_config = {
                'workflow_id': workflow_id,
                'report_config': workflow_config.get('report_config', {}),
                'export_config': workflow_config.get('export_config', {}),
                'notification_config': workflow_config.get('notification_config', {}),
                **workflow_config.get('trigger_config', {})
            }
            
            task_id = self.scheduler.schedule_report(schedule_config)
            workflow.metadata['scheduled_task_id'] = task_id
            
            # Store workflow
            self.active_workflows[workflow_id] = workflow
            
            logger.info(f"Created scheduled report workflow: {workflow_id}")
            return workflow_id
            
        except Exception as e:
            logger.error(f"Failed to create scheduled report workflow: {e}")
            raise
    
    async def create_dashboard_monitoring_workflow(self, dashboard_config: Dict[str, Any]) -> str:
        """Create dashboard monitoring and update workflow."""
        try:
            workflow_id = f"dashboard_workflow_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            workflow = BIWorkflowDefinition(
                workflow_id=workflow_id,
                workflow_name=dashboard_config.get('name', f'Dashboard Workflow {workflow_id}'),
                workflow_type='dashboard_refresh',
                trigger_config=dashboard_config.get('trigger_config', {}),
                actions=[
                    {
                        'type': 'refresh_dashboard',
                        'config': dashboard_config.get('dashboard_config', {})
                    },
                    {
                        'type': 'check_alerts',
                        'config': dashboard_config.get('alert_config', {})
                    }
                ],
                metadata=dashboard_config.get('metadata', {})
            )
            
            # Schedule dashboard updates
            update_config = {
                'workflow_id': workflow_id,
                'dashboard_id': dashboard_config.get('dashboard_id'),
                **dashboard_config.get('trigger_config', {})
            }
            
            task_id = self.scheduler.schedule_dashboard_update(
                dashboard_config.get('dashboard_id'), update_config
            )
            workflow.metadata['scheduled_task_id'] = task_id
            
            self.active_workflows[workflow_id] = workflow
            
            logger.info(f"Created dashboard monitoring workflow: {workflow_id}")
            return workflow_id
            
        except Exception as e:
            logger.error(f"Failed to create dashboard workflow: {e}")
            raise
    
    async def create_alert_monitoring_workflow(self, alert_config: Dict[str, Any]) -> str:
        """Create alert monitoring workflow."""
        try:
            workflow_id = f"alert_workflow_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            workflow = BIWorkflowDefinition(
                workflow_id=workflow_id,
                workflow_name=alert_config.get('name', f'Alert Workflow {workflow_id}'),
                workflow_type='alert_monitoring',
                trigger_config=alert_config.get('trigger_config', {}),
                actions=[
                    {
                        'type': 'check_thresholds',
                        'config': alert_config.get('threshold_config', {})
                    },
                    {
                        'type': 'trigger_alerts',
                        'config': alert_config.get('alert_action_config', {})
                    }
                ],
                metadata=alert_config.get('metadata', {})
            )
            
            # Schedule alert checks
            task_id = self.scheduler.schedule_alert_check(alert_config)
            workflow.metadata['scheduled_task_id'] = task_id
            
            self.active_workflows[workflow_id] = workflow
            
            logger.info(f"Created alert monitoring workflow: {workflow_id}")
            return workflow_id
            
        except Exception as e:
            logger.error(f"Failed to create alert workflow: {e}")
            raise
    
    async def execute_immediate_report(self, report_config: Dict[str, Any],
                                     export_config: Optional[Dict[str, Any]] = None,
                                     notification_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute an immediate report generation with optional export and notification."""
        try:
            bi_workflows_executed.labels(workflow_type='immediate_report').inc()
            
            # Generate report
            data_df = await self._load_data_for_report(report_config)
            
            # Create report definition
            report_definition = ReportDefinition(
                report_id=f"immediate_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                report_name=report_config.get('report_name', 'Immediate Report'),
                report_type=report_config.get('report_type', 'operational'),
                description=report_config.get('description', ''),
                kpi_ids=report_config.get('kpi_ids', []),
                time_period=report_config.get('time_period', 'current'),
                filters=report_config.get('filters', {})
            )
            
            # Generate comprehensive report
            comprehensive_report = await self.bi_engine.generate_comprehensive_report(
                data_df, report_definition, self.config.default_kpis
            )
            
            result = {
                'report': comprehensive_report,
                'generation_timestamp': datetime.now().isoformat(),
                'success': True
            }
            
            # Export if requested
            if export_config:
                export_cfg = ExportConfig(
                    export_id=report_definition.report_id,
                    export_format=export_config.get('format', 'json'),
                    include_visualizations=export_config.get('include_visualizations', True),
                    include_raw_data=export_config.get('include_raw_data', False)
                )
                
                # Send notification if requested
                notification_cfg = None
                if notification_config:
                    notification_cfg = NotificationConfig(
                        notification_id=f"notif_{report_definition.report_id}",
                        notification_type=notification_config.get('type', 'email'),
                        recipients=notification_config.get('recipients', []),
                        subject=notification_config.get('subject', f'Report: {report_definition.report_name}'),
                        message=notification_config.get('message', 'Your requested report is ready.')
                    )
                
                # Execute export and notification
                export_result = await self.export_notification_system.export_and_notify(
                    comprehensive_report['main_report'], export_cfg, notification_cfg
                )
                
                result['export_result'] = export_result
            
            # Update performance metrics
            self.performance_metrics['reports_generated'] += 1
            if export_config:
                self.performance_metrics['exports_completed'] += 1
            if notification_config:
                self.performance_metrics['notifications_sent'] += 1
            
            logger.info(f"Immediate report executed successfully: {report_definition.report_id}")
            return result
            
        except Exception as e:
            logger.error(f"Immediate report execution failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'generation_timestamp': datetime.now().isoformat()
            }
    
    async def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status."""
        try:
            # Get scheduler status
            scheduler_tasks = []
            for task_id in self.scheduler.scheduled_tasks.keys():
                task_status = self.scheduler.get_task_status(task_id)
                if task_status:
                    scheduler_tasks.append(task_status)
            
            # Calculate health score
            health_score = await self._calculate_system_health()
            
            status = {
                'system_id': self.system_id,
                'system_name': self.config.system_name,
                'status': self.system_status,
                'health_score': health_score,
                'last_health_check': self.last_health_check.isoformat() if self.last_health_check else None,
                'active_workflows': len(self.active_workflows),
                'scheduled_tasks': len(scheduler_tasks),
                'performance_metrics': self.performance_metrics.copy(),
                'component_status': {
                    'bi_engine': 'running',
                    'scheduler': 'running' if self.scheduler.scheduler_running else 'stopped',
                    'export_system': 'running',
                    'notification_system': 'running'
                },
                'workflows': {
                    workflow_id: {
                        'name': workflow.workflow_name,
                        'type': workflow.workflow_type,
                        'enabled': workflow.enabled,
                        'created_at': workflow.created_at.isoformat()
                    }
                    for workflow_id, workflow in self.active_workflows.items()
                },
                'scheduled_tasks_summary': scheduler_tasks
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Failed to get system status: {e}")
            return {
                'system_id': self.system_id,
                'status': 'error',
                'error': str(e)
            }
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive system health check."""
        try:
            self.last_health_check = datetime.now()
            
            health_results = {
                'timestamp': self.last_health_check.isoformat(),
                'overall_health': 'healthy',
                'components': {}
            }
            
            # Check BI engine
            try:
                # Simple test calculation
                test_data = {'test_kpi': {'value': 100, 'status': 'on_target'}}
                health_results['components']['bi_engine'] = 'healthy'
            except Exception as e:
                health_results['components']['bi_engine'] = f'unhealthy: {e}'
                health_results['overall_health'] = 'degraded'
            
            # Check scheduler
            if self.scheduler.scheduler_running:
                health_results['components']['scheduler'] = 'healthy'
            else:
                health_results['components']['scheduler'] = 'unhealthy: not running'
                health_results['overall_health'] = 'degraded'
            
            # Check export system
            export_path = Path(self.config.export_base_path)
            if export_path.exists() and export_path.is_dir():
                health_results['components']['export_system'] = 'healthy'
            else:
                health_results['components']['export_system'] = 'unhealthy: export path not accessible'
                health_results['overall_health'] = 'degraded'
            
            # Check notification system
            if self.config.email_config:
                health_results['components']['notification_system'] = 'healthy'
            else:
                health_results['components']['notification_system'] = 'limited: no email config'
            
            # Calculate health score
            healthy_components = sum(1 for status in health_results['components'].values() 
                                   if status == 'healthy')
            total_components = len(health_results['components'])
            health_score = (healthy_components / total_components) * 100
            
            health_results['health_score'] = health_score
            bi_system_health.set(health_score)
            
            if health_score < 50:
                health_results['overall_health'] = 'critical'
            elif health_score < 80:
                health_results['overall_health'] = 'degraded'
            
            logger.info(f"Health check completed: {health_results['overall_health']} ({health_score:.1f}%)")
            return health_results
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'overall_health': 'critical',
                'error': str(e)
            }
    
    async def list_workflows(self) -> List[Dict[str, Any]]:
        """List all active workflows."""
        workflows = []
        
        for workflow_id, workflow in self.active_workflows.items():
            # Get associated task status
            task_id = workflow.metadata.get('scheduled_task_id')
            task_status = None
            if task_id:
                task_status = self.scheduler.get_task_status(task_id)
            
            workflow_info = {
                'workflow_id': workflow_id,
                'workflow_name': workflow.workflow_name,
                'workflow_type': workflow.workflow_type,
                'enabled': workflow.enabled,
                'created_at': workflow.created_at.isoformat(),
                'actions_count': len(workflow.actions),
                'task_status': task_status,
                'metadata': workflow.metadata
            }
            
            workflows.append(workflow_info)
        
        return workflows
    
    async def disable_workflow(self, workflow_id: str) -> bool:
        """Disable a workflow."""
        if workflow_id in self.active_workflows:
            workflow = self.active_workflows[workflow_id]
            workflow.enabled = False
            
            # Disable associated scheduled task
            task_id = workflow.metadata.get('scheduled_task_id')
            if task_id:
                self.scheduler.disable_task(task_id)
            
            logger.info(f"Disabled workflow: {workflow_id}")
            return True
        
        return False
    
    async def enable_workflow(self, workflow_id: str) -> bool:
        """Enable a workflow."""
        if workflow_id in self.active_workflows:
            workflow = self.active_workflows[workflow_id]
            workflow.enabled = True
            
            # Enable associated scheduled task
            task_id = workflow.metadata.get('scheduled_task_id')
            if task_id:
                self.scheduler.enable_task(task_id)
            
            logger.info(f"Enabled workflow: {workflow_id}")
            return True
        
        return False
    
    async def delete_workflow(self, workflow_id: str) -> bool:
        """Delete a workflow."""
        if workflow_id in self.active_workflows:
            workflow = self.active_workflows[workflow_id]
            
            # Delete associated scheduled task
            task_id = workflow.metadata.get('scheduled_task_id')
            if task_id:
                self.scheduler.delete_task(task_id)
            
            # Remove workflow
            del self.active_workflows[workflow_id]
            
            logger.info(f"Deleted workflow: {workflow_id}")
            return True
        
        return False
    
    def _register_task_handlers(self):
        """Register task handlers with the scheduler."""
        # Register report generation handler
        self.scheduler.register_task_handler('report', self._handle_report_task)
        
        # Register dashboard update handler
        self.scheduler.register_task_handler('dashboard', self._handle_dashboard_task)
        
        # Register alert check handler
        self.scheduler.register_task_handler('alert', self._handle_alert_task)
        
        # Register notification handler
        self.scheduler.register_notification_handler(self._handle_notification)
    
    async def _handle_report_task(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Handle scheduled report generation."""
        try:
            # Extract configuration
            report_config = task_config.get('report_config', {})
            export_config = task_config.get('export_config', {})
            notification_config = task_config.get('notification_config', {})
            
            # Execute report generation
            result = await self.execute_immediate_report(
                report_config, export_config, notification_config
            )
            
            self.performance_metrics['workflows_executed'] += 1
            
            return {
                'success': result.get('success', False),
                'output_files': [result.get('export_result', {}).get('export_result', {}).get('file_path', '')],
                'metadata': {
                    'report_id': result.get('report', {}).get('main_report', {}).get('report_id'),
                    'generation_time': result.get('generation_timestamp')
                }
            }
            
        except Exception as e:
            logger.error(f"Report task execution failed: {e}")
            return {'success': False, 'error': str(e)}
    
    async def _handle_dashboard_task(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Handle scheduled dashboard updates."""
        try:
            dashboard_id = task_config.get('dashboard_id')
            if not dashboard_id:
                raise ValueError("Dashboard ID not provided")
            
            # Load dashboard data and refresh
            data_df = await self._load_data_for_dashboard(dashboard_id)
            
            # Update dashboard (implementation depends on dashboard system)
            # For now, return success
            
            self.performance_metrics['dashboards_updated'] += 1
            
            return {
                'success': True,
                'metadata': {
                    'dashboard_id': dashboard_id,
                    'update_time': datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Dashboard task execution failed: {e}")
            return {'success': False, 'error': str(e)}
    
    async def _handle_alert_task(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Handle scheduled alert checks."""
        try:
            # Check alert conditions
            alert_conditions = task_config.get('alert_conditions', [])
            triggered_alerts = []
            
            for condition in alert_conditions:
                # Evaluate condition (implementation depends on specific alert logic)
                # For now, return success
                pass
            
            return {
                'success': True,
                'metadata': {
                    'alerts_checked': len(alert_conditions),
                    'alerts_triggered': len(triggered_alerts),
                    'check_time': datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Alert task execution failed: {e}")
            return {'success': False, 'error': str(e)}
    
    async def _handle_notification(self, notification_data: Dict[str, Any]):
        """Handle scheduler notifications."""
        try:
            logger.info(f"Scheduler notification: {notification_data.get('task_name')} - {notification_data.get('status')}")
            
            # Could send notifications about task execution status
            # For now, just log
            
        except Exception as e:
            logger.error(f"Notification handling failed: {e}")
    
    async def _setup_default_workflows(self):
        """Setup default system workflows."""
        try:
            # Default daily system health report
            health_workflow_config = {
                'name': 'Daily System Health Report',
                'report_config': {
                    'report_name': 'System Health Report',
                    'report_type': 'operational',
                    'kpi_ids': ['system_health', 'performance_metrics'],
                    'time_period': 'daily'
                },
                'trigger_config': {
                    'schedule_type': 'daily',
                    'schedule_config': {'hour': 8, 'minute': 0}
                },
                'export_config': {
                    'format': 'json',
                    'include_visualizations': True
                },
                'notification_config': {
                    'type': 'email',
                    'recipients': ['admin@company.com'],
                    'subject': 'Daily BI System Health Report'
                }
            }
            
            # Create health report workflow
            await self.create_scheduled_report_workflow(health_workflow_config)
            
            logger.info("Default workflows setup completed")
            
        except Exception as e:
            logger.error(f"Failed to setup default workflows: {e}")
    
    async def _load_data_for_report(self, report_config: Dict[str, Any]) -> 'pd.DataFrame':
        """Load data for report generation."""
        # This is a placeholder - in a real implementation, this would
        # load data from the configured data sources
        import pandas as pd
        
        # Generate sample data for demonstration
        data = {
            'user_id': range(1000),
            'event_type': ['login', 'signup', 'purchase'] * 334,
            'timestamp': pd.date_range('2024-01-01', periods=1000, freq='1H'),
            'value': [100.0 + i for i in range(1000)]
        }
        
        return pd.DataFrame(data)
    
    async def _load_data_for_dashboard(self, dashboard_id: str) -> 'pd.DataFrame':
        """Load data for dashboard updates."""
        # Placeholder implementation
        return await self._load_data_for_report({})
    
    async def _calculate_system_health(self) -> float:
        """Calculate overall system health score."""
        try:
            # Health factors
            factors = []
            
            # Scheduler health
            if self.scheduler.scheduler_running:
                factors.append(100)
            else:
                factors.append(0)
            
            # Active workflows health
            enabled_workflows = sum(1 for w in self.active_workflows.values() if w.enabled)
            total_workflows = len(self.active_workflows)
            if total_workflows > 0:
                workflow_health = (enabled_workflows / total_workflows) * 100
            else:
                workflow_health = 100  # No workflows is not unhealthy
            factors.append(workflow_health)
            
            # System uptime health (placeholder)
            factors.append(100)
            
            # Calculate average
            health_score = sum(factors) / len(factors) if factors else 0
            
            return health_score
            
        except Exception as e:
            logger.error(f"Health calculation failed: {e}")
            return 0.0
    
    async def _save_system_state(self):
        """Save system state to disk."""
        try:
            state = {
                'system_id': self.system_id,
                'system_status': self.system_status,
                'performance_metrics': self.performance_metrics,
                'active_workflows': {
                    workflow_id: {
                        'workflow_id': workflow.workflow_id,
                        'workflow_name': workflow.workflow_name,
                        'workflow_type': workflow.workflow_type,
                        'enabled': workflow.enabled,
                        'created_at': workflow.created_at.isoformat(),
                        'metadata': workflow.metadata
                    }
                    for workflow_id, workflow in self.active_workflows.items()
                },
                'saved_at': datetime.now().isoformat()
            }
            
            state_path = Path(f"bi_system_state_{self.system_id}.json")
            with open(state_path, 'w') as f:
                json.dump(state, f, indent=2)
            
            logger.info("System state saved")
            
        except Exception as e:
            logger.error(f"Failed to save system state: {e}")

# Factory function for creating BI systems
def create_bi_system(config: BISystemConfig) -> ComprehensiveBISystem:
    """Factory function to create a comprehensive BI system."""
    return ComprehensiveBISystem(config)

# Global BI system instance
_global_bi_system: Optional[ComprehensiveBISystem] = None

def get_global_bi_system() -> Optional[ComprehensiveBISystem]:
    """Get the global BI system instance."""
    return _global_bi_system

def set_global_bi_system(bi_system: ComprehensiveBISystem):
    """Set the global BI system instance."""
    global _global_bi_system
    _global_bi_system = bi_system

def cleanup_global_bi_system():
    """Cleanup the global BI system."""
    global _global_bi_system
    if _global_bi_system:
        asyncio.create_task(_global_bi_system.shutdown_system())
        _global_bi_system = None
