"""
Business Intelligence Scheduler

Automated scheduling system for BI reports, dashboard updates, and alert notifications.
Provides enterprise-grade scheduling capabilities with reliability and monitoring.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import json
import threading
from concurrent.futures import ThreadPoolExecutor
import time
from pathlib import Path

# Monitoring
from prometheus_client import Counter, Histogram, Gauge

# Initialize metrics
scheduled_reports_total = Counter('bi_scheduled_reports_total', 'Total scheduled reports executed')
scheduled_reports_failed = Counter('bi_scheduled_reports_failed_total', 'Failed scheduled reports')
scheduler_execution_time = Histogram('bi_scheduler_execution_seconds', 'Scheduler execution time')
active_schedules = Gauge('bi_active_schedules', 'Number of active schedules')

logger = logging.getLogger(__name__)

class ScheduleType(Enum):
    """Types of scheduling patterns."""
    ONCE = "once"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    CUSTOM_CRON = "custom_cron"

class ReportStatus(Enum):
    """Report execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class ScheduledTask:
    """Scheduled task configuration."""
    task_id: str
    task_name: str
    task_type: str  # 'report', 'dashboard', 'alert', 'export'
    schedule_type: ScheduleType
    schedule_config: Dict[str, Any]
    task_config: Dict[str, Any]
    enabled: bool = True
    next_execution: Optional[datetime] = None
    last_execution: Optional[datetime] = None
    execution_count: int = 0
    failure_count: int = 0
    max_retries: int = 3
    retry_delay: int = 300  # seconds
    timeout: int = 3600  # seconds
    created_at: datetime = field(default_factory=datetime.now)
    recipients: List[str] = field(default_factory=list)

@dataclass
class ExecutionResult:
    """Result of a scheduled task execution."""
    task_id: str
    execution_id: str
    status: ReportStatus
    start_time: datetime
    end_time: Optional[datetime]
    duration: Optional[float]
    output_files: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

class BusinessIntelligenceScheduler:
    """Advanced scheduler for BI operations with enterprise features."""
    
    def __init__(self, max_workers: int = 10):
        self.scheduled_tasks: Dict[str, ScheduledTask] = {}
        self.execution_history: Dict[str, List[ExecutionResult]] = {}
        self.running_tasks: Dict[str, asyncio.Task] = {}
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.scheduler_running = False
        self.scheduler_thread: Optional[threading.Thread] = None
        
        # Task handlers
        self.task_handlers: Dict[str, Callable] = {}
        self.notification_handlers: List[Callable] = []
        
        # Configuration
        self.check_interval = 60  # seconds
        self.max_execution_history = 1000
        self.persistence_path = Path("bi_scheduler_state.json")
        
    async def start_scheduler(self):
        """Start the scheduler service."""
        try:
            logger.info("Starting BI Scheduler...")
            self.scheduler_running = True
            
            # Load persisted state
            await self._load_scheduler_state()
            
            # Start scheduler loop
            self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self.scheduler_thread.start()
            
            # Update metrics
            active_schedules.set(len([task for task in self.scheduled_tasks.values() if task.enabled]))
            
            logger.info(f"BI Scheduler started with {len(self.scheduled_tasks)} scheduled tasks")
            
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            raise
    
    async def stop_scheduler(self):
        """Stop the scheduler service."""
        try:
            logger.info("Stopping BI Scheduler...")
            self.scheduler_running = False
            
            # Cancel running tasks
            for task_id, task in self.running_tasks.items():
                if not task.done():
                    task.cancel()
                    logger.info(f"Cancelled running task: {task_id}")
            
            # Wait for thread to finish
            if self.scheduler_thread and self.scheduler_thread.is_alive():
                self.scheduler_thread.join(timeout=30)
            
            # Save state
            await self._save_scheduler_state()
            
            # Shutdown executor
            self.executor.shutdown(wait=True)
            
            logger.info("BI Scheduler stopped")
            
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")
    
    def schedule_report(self, task_config: Dict[str, Any]) -> str:
        """Schedule a recurring report."""
        try:
            task_id = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(self.scheduled_tasks)}"
            
            # Create scheduled task
            scheduled_task = ScheduledTask(
                task_id=task_id,
                task_name=task_config.get('report_name', f'Report {task_id}'),
                task_type='report',
                schedule_type=ScheduleType(task_config.get('schedule_type', 'daily')),
                schedule_config=task_config.get('schedule_config', {}),
                task_config=task_config,
                recipients=task_config.get('recipients', []),
                max_retries=task_config.get('max_retries', 3),
                timeout=task_config.get('timeout', 3600)
            )
            
            # Calculate next execution
            scheduled_task.next_execution = self._calculate_next_execution(scheduled_task)
            
            # Store task
            self.scheduled_tasks[task_id] = scheduled_task
            self.execution_history[task_id] = []
            
            # Update metrics
            active_schedules.inc()
            
            logger.info(f"Scheduled report task: {task_id} for {scheduled_task.next_execution}")
            return task_id
            
        except Exception as e:
            logger.error(f"Failed to schedule report: {e}")
            raise
    
    def schedule_dashboard_update(self, dashboard_id: str, update_config: Dict[str, Any]) -> str:
        """Schedule dashboard updates."""
        try:
            task_id = f"dashboard_{dashboard_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            scheduled_task = ScheduledTask(
                task_id=task_id,
                task_name=f"Dashboard Update: {dashboard_id}",
                task_type='dashboard',
                schedule_type=ScheduleType(update_config.get('schedule_type', 'daily')),
                schedule_config=update_config.get('schedule_config', {}),
                task_config={
                    'dashboard_id': dashboard_id,
                    **update_config
                },
                max_retries=update_config.get('max_retries', 2),
                timeout=update_config.get('timeout', 1800)
            )
            
            scheduled_task.next_execution = self._calculate_next_execution(scheduled_task)
            
            self.scheduled_tasks[task_id] = scheduled_task
            self.execution_history[task_id] = []
            
            active_schedules.inc()
            
            logger.info(f"Scheduled dashboard update: {task_id}")
            return task_id
            
        except Exception as e:
            logger.error(f"Failed to schedule dashboard update: {e}")
            raise
    
    def schedule_alert_check(self, alert_config: Dict[str, Any]) -> str:
        """Schedule alert monitoring."""
        try:
            task_id = f"alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(self.scheduled_tasks)}"
            
            scheduled_task = ScheduledTask(
                task_id=task_id,
                task_name=alert_config.get('alert_name', f'Alert {task_id}'),
                task_type='alert',
                schedule_type=ScheduleType(alert_config.get('schedule_type', 'daily')),
                schedule_config=alert_config.get('schedule_config', {}),
                task_config=alert_config,
                recipients=alert_config.get('recipients', []),
                max_retries=alert_config.get('max_retries', 1),
                timeout=alert_config.get('timeout', 900)
            )
            
            scheduled_task.next_execution = self._calculate_next_execution(scheduled_task)
            
            self.scheduled_tasks[task_id] = scheduled_task
            self.execution_history[task_id] = []
            
            active_schedules.inc()
            
            logger.info(f"Scheduled alert check: {task_id}")
            return task_id
            
        except Exception as e:
            logger.error(f"Failed to schedule alert: {e}")
            raise
    
    def register_task_handler(self, task_type: str, handler: Callable):
        """Register a handler for specific task types."""
        self.task_handlers[task_type] = handler
        logger.info(f"Registered handler for task type: {task_type}")
    
    def register_notification_handler(self, handler: Callable):
        """Register a notification handler."""
        self.notification_handlers.append(handler)
        logger.info("Registered notification handler")
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a scheduled task."""
        if task_id not in self.scheduled_tasks:
            return None
        
        task = self.scheduled_tasks[task_id]
        history = self.execution_history.get(task_id, [])
        
        last_execution = history[-1] if history else None
        
        return {
            'task_id': task_id,
            'task_name': task.task_name,
            'task_type': task.task_type,
            'enabled': task.enabled,
            'next_execution': task.next_execution.isoformat() if task.next_execution else None,
            'last_execution': task.last_execution.isoformat() if task.last_execution else None,
            'execution_count': task.execution_count,
            'failure_count': task.failure_count,
            'last_status': last_execution.status.value if last_execution else None,
            'last_duration': last_execution.duration if last_execution else None,
            'is_running': task_id in self.running_tasks
        }
    
    def get_execution_history(self, task_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get execution history for a task."""
        if task_id not in self.execution_history:
            return []
        
        history = self.execution_history[task_id][-limit:]
        
        return [
            {
                'execution_id': result.execution_id,
                'status': result.status.value,
                'start_time': result.start_time.isoformat(),
                'end_time': result.end_time.isoformat() if result.end_time else None,
                'duration': result.duration,
                'error_message': result.error_message,
                'retry_count': result.retry_count,
                'output_files': result.output_files
            }
            for result in history
        ]
    
    def enable_task(self, task_id: str) -> bool:
        """Enable a scheduled task."""
        if task_id in self.scheduled_tasks:
            self.scheduled_tasks[task_id].enabled = True
            active_schedules.inc()
            logger.info(f"Enabled task: {task_id}")
            return True
        return False
    
    def disable_task(self, task_id: str) -> bool:
        """Disable a scheduled task."""
        if task_id in self.scheduled_tasks:
            self.scheduled_tasks[task_id].enabled = False
            active_schedules.dec()
            logger.info(f"Disabled task: {task_id}")
            return True
        return False
    
    def delete_task(self, task_id: str) -> bool:
        """Delete a scheduled task."""
        if task_id in self.scheduled_tasks:
            # Cancel if running
            if task_id in self.running_tasks:
                self.running_tasks[task_id].cancel()
                del self.running_tasks[task_id]
            
            # Remove from schedules
            del self.scheduled_tasks[task_id]
            if task_id in self.execution_history:
                del self.execution_history[task_id]
            
            active_schedules.dec()
            logger.info(f"Deleted task: {task_id}")
            return True
        return False
    
    def _scheduler_loop(self):
        """Main scheduler loop."""
        while self.scheduler_running:
            try:
                with scheduler_execution_time.time():
                    self._check_and_execute_tasks()
                
                time.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")
                time.sleep(self.check_interval)
    
    def _check_and_execute_tasks(self):
        """Check for tasks ready to execute."""
        current_time = datetime.now()
        
        for task_id, task in self.scheduled_tasks.items():
            if not task.enabled:
                continue
            
            if task.next_execution and current_time >= task.next_execution:
                if task_id not in self.running_tasks:
                    # Start task execution
                    execution_task = asyncio.create_task(self._execute_task(task))
                    self.running_tasks[task_id] = execution_task
                    
                    # Schedule next execution
                    task.next_execution = self._calculate_next_execution(task)
                    
                    logger.info(f"Started execution of task: {task_id}")
    
    async def _execute_task(self, task: ScheduledTask):
        """Execute a scheduled task."""
        execution_id = f"{task.task_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        start_time = datetime.now()
        
        result = ExecutionResult(
            task_id=task.task_id,
            execution_id=execution_id,
            status=ReportStatus.RUNNING,
            start_time=start_time
        )
        
        try:
            # Check if handler exists
            if task.task_type not in self.task_handlers:
                raise ValueError(f"No handler registered for task type: {task.task_type}")
            
            handler = self.task_handlers[task.task_type]
            
            # Execute task with timeout
            try:
                output = await asyncio.wait_for(
                    handler(task.task_config),
                    timeout=task.timeout
                )
                
                result.status = ReportStatus.COMPLETED
                result.output_files = output.get('output_files', []) if isinstance(output, dict) else []
                result.metadata = output.get('metadata', {}) if isinstance(output, dict) else {}
                
                # Update task statistics
                task.execution_count += 1
                task.last_execution = start_time
                
                scheduled_reports_total.inc()
                
                logger.info(f"Task {task.task_id} completed successfully")
                
            except asyncio.TimeoutError:
                result.status = ReportStatus.FAILED
                result.error_message = f"Task timed out after {task.timeout} seconds"
                task.failure_count += 1
                scheduled_reports_failed.inc()
                
                logger.error(f"Task {task.task_id} timed out")
                
        except Exception as e:
            result.status = ReportStatus.FAILED
            result.error_message = str(e)
            task.failure_count += 1
            scheduled_reports_failed.inc()
            
            logger.error(f"Task {task.task_id} failed: {e}")
            
        finally:
            # Complete execution result
            result.end_time = datetime.now()
            result.duration = (result.end_time - result.start_time).total_seconds()
            
            # Store execution result
            if task.task_id not in self.execution_history:
                self.execution_history[task.task_id] = []
            
            self.execution_history[task.task_id].append(result)
            
            # Limit history size
            if len(self.execution_history[task.task_id]) > self.max_execution_history:
                self.execution_history[task.task_id] = self.execution_history[task.task_id][-self.max_execution_history:]
            
            # Remove from running tasks
            if task.task_id in self.running_tasks:
                del self.running_tasks[task.task_id]
            
            # Send notifications
            await self._send_notifications(task, result)
    
    def _calculate_next_execution(self, task: ScheduledTask) -> datetime:
        """Calculate next execution time based on schedule."""
        current_time = datetime.now()
        
        if task.schedule_type == ScheduleType.ONCE:
            # One-time execution
            return current_time + timedelta(minutes=1)
        
        elif task.schedule_type == ScheduleType.DAILY:
            # Daily execution
            hour = task.schedule_config.get('hour', 9)
            minute = task.schedule_config.get('minute', 0)
            
            next_exec = current_time.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if next_exec <= current_time:
                next_exec += timedelta(days=1)
            
            return next_exec
        
        elif task.schedule_type == ScheduleType.WEEKLY:
            # Weekly execution
            weekday = task.schedule_config.get('weekday', 0)  # Monday = 0
            hour = task.schedule_config.get('hour', 9)
            minute = task.schedule_config.get('minute', 0)
            
            days_ahead = weekday - current_time.weekday()
            if days_ahead <= 0:  # Target day already happened this week
                days_ahead += 7
            
            next_exec = current_time + timedelta(days=days_ahead)
            next_exec = next_exec.replace(hour=hour, minute=minute, second=0, microsecond=0)
            
            return next_exec
        
        elif task.schedule_type == ScheduleType.MONTHLY:
            # Monthly execution
            day = task.schedule_config.get('day', 1)
            hour = task.schedule_config.get('hour', 9)
            minute = task.schedule_config.get('minute', 0)
            
            # Try this month first
            try:
                next_exec = current_time.replace(day=day, hour=hour, minute=minute, second=0, microsecond=0)
                if next_exec <= current_time:
                    # Next month
                    if current_time.month == 12:
                        next_exec = next_exec.replace(year=current_time.year + 1, month=1)
                    else:
                        next_exec = next_exec.replace(month=current_time.month + 1)
            except ValueError:
                # Day doesn't exist in this month, use last day of month
                if current_time.month == 12:
                    next_month = current_time.replace(year=current_time.year + 1, month=1, day=1)
                else:
                    next_month = current_time.replace(month=current_time.month + 1, day=1)
                
                last_day = (next_month - timedelta(days=1)).day
                next_exec = current_time.replace(day=last_day, hour=hour, minute=minute, second=0, microsecond=0)
            
            return next_exec
        
        elif task.schedule_type == ScheduleType.QUARTERLY:
            # Quarterly execution
            month_offset = task.schedule_config.get('month_offset', 0)  # 0, 1, or 2
            day = task.schedule_config.get('day', 1)
            hour = task.schedule_config.get('hour', 9)
            minute = task.schedule_config.get('minute', 0)
            
            # Calculate next quarter
            quarter_start_months = [1, 4, 7, 10]
            current_quarter = (current_time.month - 1) // 3
            next_quarter_month = quarter_start_months[current_quarter] + month_offset
            
            if next_quarter_month <= current_time.month:
                # Next quarter
                if current_quarter == 3:  # Q4, go to next year Q1
                    next_quarter_month = quarter_start_months[0] + month_offset
                    year = current_time.year + 1
                else:
                    next_quarter_month = quarter_start_months[current_quarter + 1] + month_offset
                    year = current_time.year
            else:
                year = current_time.year
            
            next_exec = datetime(year, next_quarter_month, day, hour, minute)
            
            return next_exec
        
        else:
            # Default to daily
            return current_time + timedelta(days=1)
    
    async def _send_notifications(self, task: ScheduledTask, result: ExecutionResult):
        """Send notifications about task execution."""
        try:
            notification_data = {
                'task_id': task.task_id,
                'task_name': task.task_name,
                'task_type': task.task_type,
                'status': result.status.value,
                'execution_time': result.start_time.isoformat(),
                'duration': result.duration,
                'error_message': result.error_message,
                'recipients': task.recipients
            }
            
            # Send to registered notification handlers
            for handler in self.notification_handlers:
                try:
                    await handler(notification_data)
                except Exception as e:
                    logger.error(f"Notification handler failed: {e}")
            
        except Exception as e:
            logger.error(f"Failed to send notifications: {e}")
    
    async def _save_scheduler_state(self):
        """Save scheduler state to disk."""
        try:
            state = {
                'scheduled_tasks': {
                    task_id: {
                        'task_id': task.task_id,
                        'task_name': task.task_name,
                        'task_type': task.task_type,
                        'schedule_type': task.schedule_type.value,
                        'schedule_config': task.schedule_config,
                        'task_config': task.task_config,
                        'enabled': task.enabled,
                        'next_execution': task.next_execution.isoformat() if task.next_execution else None,
                        'last_execution': task.last_execution.isoformat() if task.last_execution else None,
                        'execution_count': task.execution_count,
                        'failure_count': task.failure_count,
                        'max_retries': task.max_retries,
                        'retry_delay': task.retry_delay,
                        'timeout': task.timeout,
                        'created_at': task.created_at.isoformat(),
                        'recipients': task.recipients
                    }
                    for task_id, task in self.scheduled_tasks.items()
                },
                'saved_at': datetime.now().isoformat()
            }
            
            with open(self.persistence_path, 'w') as f:
                json.dump(state, f, indent=2)
            
            logger.info("Scheduler state saved")
            
        except Exception as e:
            logger.error(f"Failed to save scheduler state: {e}")
    
    async def _load_scheduler_state(self):
        """Load scheduler state from disk."""
        try:
            if not self.persistence_path.exists():
                logger.info("No saved scheduler state found")
                return
            
            with open(self.persistence_path, 'r') as f:
                state = json.load(f)
            
            # Restore scheduled tasks
            for task_id, task_data in state.get('scheduled_tasks', {}).items():
                task = ScheduledTask(
                    task_id=task_data['task_id'],
                    task_name=task_data['task_name'],
                    task_type=task_data['task_type'],
                    schedule_type=ScheduleType(task_data['schedule_type']),
                    schedule_config=task_data['schedule_config'],
                    task_config=task_data['task_config'],
                    enabled=task_data['enabled'],
                    next_execution=datetime.fromisoformat(task_data['next_execution']) if task_data['next_execution'] else None,
                    last_execution=datetime.fromisoformat(task_data['last_execution']) if task_data['last_execution'] else None,
                    execution_count=task_data['execution_count'],
                    failure_count=task_data['failure_count'],
                    max_retries=task_data['max_retries'],
                    retry_delay=task_data['retry_delay'],
                    timeout=task_data['timeout'],
                    created_at=datetime.fromisoformat(task_data['created_at']),
                    recipients=task_data['recipients']
                )
                
                self.scheduled_tasks[task_id] = task
                self.execution_history[task_id] = []
            
            logger.info(f"Loaded {len(self.scheduled_tasks)} scheduled tasks from state")
            
        except Exception as e:
            logger.error(f"Failed to load scheduler state: {e}")

# Global scheduler instance
_scheduler_instance: Optional[BusinessIntelligenceScheduler] = None

def get_scheduler() -> BusinessIntelligenceScheduler:
    """Get the global scheduler instance."""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = BusinessIntelligenceScheduler()
    return _scheduler_instance

def cleanup_scheduler():
    """Cleanup the global scheduler."""
    global _scheduler_instance
    if _scheduler_instance:
        asyncio.create_task(_scheduler_instance.stop_scheduler())
        _scheduler_instance = None
