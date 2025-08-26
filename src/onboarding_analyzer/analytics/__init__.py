"""
Advanced Analytics Package

Comprehensive analytics suite including:
- Real-time analytics engine
- Automated insight generation  
- Predictive analytics and forecasting
- User behavior analysis and segmentation
- Cohort analysis and retention tracking
- Advanced funnel analysis with attribution
- Complete Business Intelligence reporting system
- Automated scheduling and notifications
- Multi-format export capabilities
- Executive dashboards and monitoring

This package provides enterprise-grade analytics capabilities for
comprehensive business intelligence and data-driven decision making.
"""

# Core analytics modules
from .real_time_analytics import RealTimeAnalyticsEngine, StreamProcessor, MetricsCalculator
from .insight_generation import InsightGenerator, StatisticalAnalyzer
from .predictive_analytics import (
    PredictiveAnalyticsManager, ForecastingEngine, BusinessMetricsPredictor
)
from .user_behavior_analysis import (
    UserBehaviorAnalyzer, SessionAnalyzer, UserProfileBuilder,
    BehavioralSegmentation
)
from .cohort_analysis import CohortAnalyzer, CohortBuilder, RetentionCohort
from .funnel_analysis_enhanced import (
    AdvancedFunnelAnalyzer, FunnelProcessor, AttributionAnalysisResult
)

# Business Intelligence system
from .business_intelligence import (
    BusinessIntelligenceEngine, KPIDefinition, KPIResult, KPICalculator,
    VisualizationGenerator, ReportDefinition, GeneratedReport
)
from .bi_scheduler import (
    BusinessIntelligenceScheduler, ScheduledTask, ExecutionResult,
    ScheduleType, ReportStatus, get_scheduler
)
from .bi_export_system import (
    BIExportNotificationSystem, ReportExporter, NotificationSystem,
    ExportConfig, NotificationConfig, EmailConfig
)
from .bi_integration import (
    ComprehensiveBISystem, BIWorkflowDefinition, BISystemConfig,
    create_bi_system, get_global_bi_system, set_global_bi_system
)
from .bi_config import (
    BISystemConfigurator, BISystemInitializer, DefaultKPIConfiguration,
    quick_start_bi_system, initialize_bi_system_from_config,
    create_default_bi_config, save_bi_config, load_bi_config
)

# Version information
__version__ = "1.0.0"
__author__ = "Analytics Team"
__description__ = "Comprehensive Business Intelligence and Analytics Platform"

# Package metadata
__all__ = [
    # Real-time analytics
    "RealTimeAnalyticsEngine",
    "StreamProcessor", 
    "MetricsCalculator",
    
    # Automated insights
    "AutomatedInsightEngine",
    "InsightGenerator",
    "StatisticalAnalyzer",
    
    # Predictive analytics
    "PredictiveAnalyticsEngine",
    "ForecastingEngine",
    "ModelManager",
    "TimeSeriesForecaster",
    "BehaviorPredictor",
    
    # User behavior analysis
    "UserBehaviorAnalyzer",
    "SessionAnalyzer",
    "UserProfileBuilder",
    "BehavioralSegmentation",
    
    # Cohort analysis
    "CohortAnalyzer",
    "CohortBuilder",
    "RetentionCohort",
    
    # Funnel analysis
    "AdvancedFunnelAnalyzer",
    "FunnelProcessor",
    "AttributionAnalysisResult",
    
    # Business Intelligence core
    "BusinessIntelligenceEngine",
    "KPIDefinition",
    "KPIResult", 
    "KPICalculator",
    "VisualizationGenerator",
    "ReportDefinition",
    "GeneratedReport",
    
    # BI Scheduling
    "BusinessIntelligenceScheduler",
    "ScheduledTask",
    "ExecutionResult",
    "ScheduleType",
    "ReportStatus",
    "get_scheduler",
    
    # BI Export and Notifications
    "BIExportNotificationSystem",
    "ReportExporter",
    "NotificationSystem",
    "ExportConfig",
    "NotificationConfig",
    "EmailConfig",
    
    # BI Integration
    "ComprehensiveBISystem",
    "BIWorkflowDefinition",
    "BISystemConfig",
    "create_bi_system",
    "get_global_bi_system",
    "set_global_bi_system",
    
    # BI Configuration
    "BISystemConfigurator",
    "BISystemInitializer", 
    "DefaultKPIConfiguration",
    "quick_start_bi_system",
    "initialize_bi_system_from_config",
    "create_default_bi_config",
    "save_bi_config",
    "load_bi_config"
]

# Module categories for documentation
ANALYTICS_MODULES = {
    "real_time": [
        "RealTimeAnalyticsEngine",
        "StreamProcessor",
        "MetricsCalculator"
    ],
    "insights": [
        "AutomatedInsightEngine",
        "InsightGenerator", 
        "StatisticalAnalyzer"
    ],
    "predictive": [
        "PredictiveAnalyticsEngine",
        "ForecastingEngine",
        "ModelManager",
        "TimeSeriesForecaster",
        "BehaviorPredictor"
    ],
    "behavioral": [
        "UserBehaviorAnalyzer",
        "SessionAnalyzer",
        "UserProfileBuilder",
        "BehavioralSegmentation"
    ],
    "cohort": [
        "CohortAnalyzer",
        "CohortBuilder", 
        "RetentionCohort"
    ],
    "funnel": [
        "AdvancedFunnelAnalyzer",
        "FunnelProcessor",
        "AttributionAnalysisResult"
    ],
    "business_intelligence": [
        "BusinessIntelligenceEngine",
        "KPIDefinition",
        "KPIResult",
        "KPICalculator",
        "VisualizationGenerator",
        "ReportDefinition",
        "GeneratedReport"
    ],
    "scheduling": [
        "BusinessIntelligenceScheduler",
        "ScheduledTask",
        "ExecutionResult",
        "ScheduleType",
        "ReportStatus"
    ],
    "export_notifications": [
        "BIExportNotificationSystem",
        "ReportExporter",
        "NotificationSystem",
        "ExportConfig",
        "NotificationConfig",
        "EmailConfig"
    ],
    "integration": [
        "ComprehensiveBISystem",
        "BIWorkflowDefinition", 
        "BISystemConfig"
    ],
    "configuration": [
        "BISystemConfigurator",
        "BISystemInitializer",
        "DefaultKPIConfiguration"
    ]
}

# Quick access functions
QUICK_START_FUNCTIONS = [
    "quick_start_bi_system",
    "initialize_bi_system_from_config", 
    "create_default_bi_config"
]

def get_package_info() -> dict:
    """Get comprehensive package information."""
    return {
        "name": "onboarding_analyzer.analytics",
        "version": __version__,
        "description": __description__,
        "author": __author__,
        "modules": len(__all__),
        "categories": list(ANALYTICS_MODULES.keys()),
        "quick_start_functions": QUICK_START_FUNCTIONS
    }

def list_analytics_capabilities() -> dict:
    """List all analytics capabilities by category."""
    return ANALYTICS_MODULES.copy()

# Initialize logging for the analytics package
import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

# Package-level configuration
DEFAULT_CONFIG = {
    "analytics_enabled": True,
    "real_time_processing": True,
    "predictive_modeling": True,
    "automated_insights": True,
    "business_intelligence": True,
    "export_capabilities": True,
    "notification_system": True,
    "scheduling_system": True
}
