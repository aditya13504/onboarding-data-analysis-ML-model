"""
Email Scheduler Module for Email Digest Management
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import json
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class EmailScheduleConfig:
    """Configuration for email scheduling"""
    schedule_id: str
    recipient_email: str
    subject_template: str
    frequency: str = "daily"  # daily, weekly, monthly
    enabled: bool = True
    last_sent: Optional[datetime] = None
    next_scheduled: Optional[datetime] = None
    template_data: Dict[str, Any] = field(default_factory=dict)

@dataclass
class EmailDigestData:
    """Data structure for email digest content"""
    digest_id: str
    generated_at: datetime
    period_start: datetime
    period_end: datetime
    
    # Core metrics
    total_sessions: int = 0
    completed_sessions: int = 0
    abandoned_sessions: int = 0
    completion_rate: float = 0.0
    
    # Top insights
    top_friction_points: List[Dict[str, Any]] = field(default_factory=list)
    improvement_recommendations: List[Dict[str, Any]] = field(default_factory=list)
    performance_trends: Dict[str, Any] = field(default_factory=dict)
    
    # Summary statistics
    summary_stats: Dict[str, Any] = field(default_factory=dict)

class EmailDigestScheduler:
    """Scheduler for automated email digest sending"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.EmailDigestScheduler")
        self.active_schedules: Dict[str, EmailScheduleConfig] = {}
        self.last_digest_data: Optional[EmailDigestData] = None
        self.is_running = False
        
    def add_schedule(self, config: EmailScheduleConfig) -> bool:
        """Add a new email schedule"""
        try:
            self.active_schedules[config.schedule_id] = config
            self.logger.info(f"Added email schedule: {config.schedule_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to add schedule: {e}")
            return False
    
    def remove_schedule(self, schedule_id: str) -> bool:
        """Remove an email schedule"""
        try:
            if schedule_id in self.active_schedules:
                del self.active_schedules[schedule_id]
                self.logger.info(f"Removed email schedule: {schedule_id}")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Failed to remove schedule: {e}")
            return False
    
    def update_schedule(self, schedule_id: str, config: EmailScheduleConfig) -> bool:
        """Update an existing email schedule"""
        try:
            if schedule_id in self.active_schedules:
                self.active_schedules[schedule_id] = config
                self.logger.info(f"Updated email schedule: {schedule_id}")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Failed to update schedule: {e}")
            return False
    
    def get_schedule(self, schedule_id: str) -> Optional[EmailScheduleConfig]:
        """Get a specific email schedule"""
        return self.active_schedules.get(schedule_id)
    
    def list_schedules(self) -> List[EmailScheduleConfig]:
        """List all active email schedules"""
        return list(self.active_schedules.values())
    
    async def generate_digest_data(self) -> EmailDigestData:
        """Generate digest data from analysis results"""
        try:
            # Generate digest for the last 24 hours
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=1)
            
            # Mock data - in real implementation, this would query the database
            digest_data = EmailDigestData(
                digest_id=f"digest_{int(end_time.timestamp())}",
                generated_at=end_time,
                period_start=start_time,
                period_end=end_time,
                total_sessions=150,
                completed_sessions=98,
                abandoned_sessions=52,
                completion_rate=0.653,
                top_friction_points=[
                    {
                        "step_id": "email_verification",
                        "abandonment_rate": 0.34,
                        "affected_users": 45,
                        "friction_type": "slow_progression"
                    },
                    {
                        "step_id": "profile_completion",
                        "abandonment_rate": 0.28,
                        "affected_users": 32,
                        "friction_type": "form_errors"
                    }
                ],
                improvement_recommendations=[
                    {
                        "title": "Simplify Email Verification Process",
                        "impact_score": 0.85,
                        "estimated_improvement": "15% reduction in abandonment",
                        "priority": "high"
                    },
                    {
                        "title": "Add Real-time Form Validation",
                        "impact_score": 0.72,
                        "estimated_improvement": "12% reduction in form errors",
                        "priority": "medium"
                    }
                ],
                performance_trends={
                    "completion_rate_change": "+5.2%",
                    "average_time_change": "-23 seconds",
                    "error_rate_change": "-8.1%"
                },
                summary_stats={
                    "total_users_affected": 77,
                    "potential_recovered_users": 23,
                    "estimated_revenue_impact": "$1,150"
                }
            )
            
            self.last_digest_data = digest_data
            return digest_data
            
        except Exception as e:
            self.logger.error(f"Failed to generate digest data: {e}")
            # Return empty digest data on error
            return EmailDigestData(
                digest_id="error_digest",
                generated_at=datetime.utcnow(),
                period_start=datetime.utcnow() - timedelta(days=1),
                period_end=datetime.utcnow()
            )
    
    async def send_digest_email(self, schedule: EmailScheduleConfig, digest_data: EmailDigestData) -> bool:
        """Send digest email (mock implementation)"""
        try:
            self.logger.info(f"Sending digest email to {schedule.recipient_email}")
            
            # Mock email sending - in real implementation, this would use an email service
            email_content = self._format_email_content(schedule, digest_data)
            
            # Update schedule
            schedule.last_sent = datetime.utcnow()
            schedule.next_scheduled = self._calculate_next_schedule(schedule)
            
            self.logger.info(f"Email digest sent successfully to {schedule.recipient_email}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send digest email: {e}")
            return False
    
    def _format_email_content(self, schedule: EmailScheduleConfig, digest_data: EmailDigestData) -> str:
        """Format email content from digest data"""
        try:
            content = f"""
            Subject: {schedule.subject_template}
            
            Onboarding Analytics Digest
            Period: {digest_data.period_start.strftime('%Y-%m-%d')} to {digest_data.period_end.strftime('%Y-%m-%d')}
            
            📊 Key Metrics:
            - Total Sessions: {digest_data.total_sessions}
            - Completion Rate: {digest_data.completion_rate:.1%}
            - Performance Trend: {digest_data.performance_trends.get('completion_rate_change', 'N/A')}
            
            🚨 Top Friction Points:
            """
            
            for i, friction in enumerate(digest_data.top_friction_points[:3], 1):
                content += f"\n{i}. {friction['step_id']} - {friction['abandonment_rate']:.1%} abandonment"
            
            content += "\n\n💡 Recommended Actions:"
            for i, rec in enumerate(digest_data.improvement_recommendations[:3], 1):
                content += f"\n{i}. {rec['title']} (Impact: {rec['impact_score']:.1f})"
            
            content += f"\n\n📈 Potential Impact: {digest_data.summary_stats.get('estimated_revenue_impact', 'N/A')}"
            
            return content
            
        except Exception as e:
            self.logger.error(f"Failed to format email content: {e}")
            return "Error generating email content"
    
    def _calculate_next_schedule(self, schedule: EmailScheduleConfig) -> datetime:
        """Calculate next scheduled time based on frequency"""
        try:
            now = datetime.utcnow()
            
            if schedule.frequency == "daily":
                return now + timedelta(days=1)
            elif schedule.frequency == "weekly":
                return now + timedelta(weeks=1)
            elif schedule.frequency == "monthly":
                return now + timedelta(days=30)
            else:
                return now + timedelta(days=1)  # Default to daily
                
        except Exception as e:
            self.logger.error(f"Failed to calculate next schedule: {e}")
            return datetime.utcnow() + timedelta(days=1)
    
    async def process_scheduled_emails(self) -> Dict[str, Any]:
        """Process all scheduled emails that are due"""
        try:
            now = datetime.utcnow()
            results = {
                "processed": 0,
                "successful": 0,
                "failed": 0,
                "details": []
            }
            
            for schedule_id, schedule in self.active_schedules.items():
                if not schedule.enabled:
                    continue
                    
                # Check if email is due
                is_due = (
                    schedule.next_scheduled is None or 
                    schedule.next_scheduled <= now
                )
                
                if is_due:
                    results["processed"] += 1
                    
                    # Generate fresh digest data
                    digest_data = await self.generate_digest_data()
                    
                    # Send email
                    success = await self.send_digest_email(schedule, digest_data)
                    
                    if success:
                        results["successful"] += 1
                    else:
                        results["failed"] += 1
                    
                    results["details"].append({
                        "schedule_id": schedule_id,
                        "recipient": schedule.recipient_email,
                        "status": "success" if success else "failed",
                        "sent_at": now.isoformat()
                    })
            
            self.logger.info(f"Processed {results['processed']} scheduled emails")
            return results
            
        except Exception as e:
            self.logger.error(f"Failed to process scheduled emails: {e}")
            return {
                "processed": 0,
                "successful": 0,
                "failed": 1,
                "error": str(e)
            }
    
    async def start_scheduler(self) -> None:
        """Start the email scheduler background task"""
        try:
            self.is_running = True
            self.logger.info("Email scheduler started")
            
            while self.is_running:
                await self.process_scheduled_emails()
                # Check every hour
                await asyncio.sleep(3600)
                
        except Exception as e:
            self.logger.error(f"Email scheduler error: {e}")
            self.is_running = False
    
    def stop_scheduler(self) -> None:
        """Stop the email scheduler"""
        self.is_running = False
        self.logger.info("Email scheduler stopped")
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """Get current scheduler status"""
        return {
            "running": self.is_running,
            "active_schedules": len(self.active_schedules),
            "last_digest_generated": self.last_digest_data.generated_at.isoformat() if self.last_digest_data else None,
            "schedules": [
                {
                    "schedule_id": config.schedule_id,
                    "recipient": config.recipient_email,
                    "frequency": config.frequency,
                    "enabled": config.enabled,
                    "last_sent": config.last_sent.isoformat() if config.last_sent else None,
                    "next_scheduled": config.next_scheduled.isoformat() if config.next_scheduled else None
                }
                for config in self.active_schedules.values()
            ]
        }

# Global scheduler instance
_email_scheduler: Optional[EmailDigestScheduler] = None

def get_email_scheduler() -> EmailDigestScheduler:
    """Get or create the global email scheduler instance"""
    global _email_scheduler
    if _email_scheduler is None:
        _email_scheduler = EmailDigestScheduler()
    return _email_scheduler

# Initialize default schedule for testing
def initialize_default_schedules():
    """Initialize default email schedules for testing"""
    scheduler = get_email_scheduler()
    
    # Add a default daily schedule
    default_schedule = EmailScheduleConfig(
        schedule_id="default_daily",
        recipient_email="admin@example.com",
        subject_template="Daily Onboarding Analytics Digest",
        frequency="daily",
        enabled=True,
        next_scheduled=datetime.utcnow() + timedelta(hours=1)  # Next hour for testing
    )
    
    scheduler.add_schedule(default_schedule)
    
    return scheduler

# Background task for email digest scheduling
async def start_email_digest_scheduler():
    """Start the email digest scheduler as a background task"""
    scheduler = get_email_scheduler()
    await scheduler.start_scheduler()

async def stop_email_digest_scheduler():
    """Stop the email digest scheduler"""
    scheduler = get_email_scheduler()
    scheduler.stop_scheduler()

# Auto-initialize on import
try:
    initialize_default_schedules()
except Exception as e:
    logger.warning(f"Failed to initialize default schedules: {e}")
