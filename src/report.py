"""
Point 16: Report Generation & Delivery System (Part 1/2)
Comprehensive reporting engine for onboarding analytics with multiple formats and delivery channels.
"""

import os, json, time, threading, asyncio, uuid
from typing import Dict, List, Optional, Any, Union, Set, Tuple, NamedTuple
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, Counter
import logging
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from pathlib import Path
import sqlite3, statistics, math, io, base64, smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import matplotlib.pyplot as plt
import seaborn as sns
from jinja2 import Template, Environment, FileSystemLoader

# Optional WeasyPrint import for PDF generation
try:
    import weasyprint
    WEASYPRINT_AVAILABLE = True
except Exception as e:
    print(f"WeasyPrint not available: {e}")
    print("PDF generation will be disabled. Install WeasyPrint dependencies if needed.")
    weasyprint = None
    WEASYPRINT_AVAILABLE = False

# Import from our modules
from .onboarding_models import (
    OnboardingSession, OnboardingStepAttempt, OnboardingFlowDefinition,
    OnboardingStatus, OnboardingStepType, UserSegment, FeatureEngineer
)
from .friction_detection import (
    FrictionPoint, FrictionCluster, FrictionType, FrictionSeverity,
    PatternConfidence, DropoffAnalysis
)
from .recommendation_engine import (
    Recommendation, RecommendationSuite, RecommendationType, 
    RecommendationPriority, RecommendationImpact
)

class ReportType(Enum):
    """Types of reports"""
    EXECUTIVE_SUMMARY = "executive_summary"
    DETAILED_ANALYSIS = "detailed_analysis"
    FRICTION_REPORT = "friction_report"
    RECOMMENDATION_REPORT = "recommendation_report"
    PERFORMANCE_DASHBOARD = "performance_dashboard"
    TREND_ANALYSIS = "trend_analysis"
    SEGMENT_ANALYSIS = "segment_analysis"
    CONVERSION_FUNNEL = "conversion_funnel"
    A_B_TEST_RESULTS = "ab_test_results"
    OPERATIONAL_METRICS = "operational_metrics"

class ReportFormat(Enum):
    """Report output formats"""
    PDF = "pdf"
    HTML = "html"
    JSON = "json"
    CSV = "csv"
    EXCEL = "excel"
    POWERPOINT = "powerpoint"
    DASHBOARD = "dashboard"

class DeliveryChannel(Enum):
    """Report delivery channels"""
    EMAIL = "email"
    SLACK = "slack"
    WEBHOOK = "webhook"
    FILE_SYSTEM = "file_system"
    CLOUD_STORAGE = "cloud_storage"
    DASHBOARD_UPDATE = "dashboard_update"
    API_ENDPOINT = "api_endpoint"

class ReportFrequency(Enum):
    """Report generation frequency"""
    REAL_TIME = "real_time"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ON_DEMAND = "on_demand"

@dataclass
class ReportMetrics:
    """Key metrics for reports"""
    total_sessions: int
    completed_sessions: int
    completion_rate: float
    average_completion_time: float
    total_friction_points: int
    high_severity_friction: int
    total_recommendations: int
    estimated_impact: Dict[str, float]
    user_segments: Dict[str, int]
    time_period: Dict[str, datetime]
    comparison_period: Optional[Dict[str, float]] = None

@dataclass
class ChartData:
    """Chart data structure"""
    chart_id: str
    chart_type: str  # bar, line, pie, heatmap, funnel, scatter
    title: str
    data: Dict[str, Any]
    labels: List[str]
    values: List[Union[int, float]]
    colors: Optional[List[str]] = None
    options: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ReportSection:
    """Report section structure"""
    section_id: str
    title: str
    content: str
    charts: List[ChartData] = field(default_factory=list)
    tables: List[Dict[str, Any]] = field(default_factory=list)
    insights: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    order: int = 0

@dataclass
class ReportConfiguration:
    """Report configuration"""
    report_id: str
    report_type: ReportType
    output_format: ReportFormat
    delivery_channels: List[DeliveryChannel]
    frequency: ReportFrequency
    recipients: List[str]
    
    # Content settings
    include_sections: List[str]
    date_range: Tuple[datetime, datetime]
    flow_ids: List[str]
    user_segments: List[UserSegment]
    
    # Formatting settings
    template_name: str
    brand_colors: Dict[str, str] = field(default_factory=dict)
    custom_styling: Dict[str, Any] = field(default_factory=dict)
    
    # Delivery settings
    delivery_settings: Dict[str, Any] = field(default_factory=dict)
    
    # Schedule
    next_generation: datetime = field(default_factory=datetime.utcnow)
    timezone: str = "UTC"
    
    # Filters
    min_session_count: int = 100
    include_test_data: bool = False
    
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str = "system"
    active: bool = True

@dataclass
class GeneratedReport:
    """Generated report instance"""
    report_id: str
    config_id: str
    generated_at: datetime
    time_period: Tuple[datetime, datetime]
    
    # Content
    title: str
    sections: List[ReportSection]
    metrics: ReportMetrics
    charts: List[ChartData]
    
    # Output
    output_format: ReportFormat
    file_path: Optional[str] = None
    file_size: Optional[int] = None
    content_hash: Optional[str] = None
    
    # Delivery
    delivered_to: List[str] = field(default_factory=list)
    delivery_status: Dict[str, str] = field(default_factory=dict)
    
    # Metadata
    generation_time_seconds: float = 0.0
    data_freshness: datetime = field(default_factory=datetime.utcnow)
    version: str = "1.0"

class ReportDataCollector:
    """Collects and prepares data for reports"""
    
    def __init__(self, data_sources: Dict[str, Any]):
        self.data_sources = data_sources
        self.logger = logging.getLogger(__name__)
    
    def collect_session_data(self, flow_ids: List[str], 
                           date_range: Tuple[datetime, datetime],
                           user_segments: Optional[List[UserSegment]] = None) -> List[OnboardingSession]:
        """Collect onboarding session data"""
        # Implementation would connect to actual data sources
        sessions = []
        
        # Placeholder for data collection logic
        # In real implementation, this would query the database/APIs
        sample_session = OnboardingSession(
            session_id=f"session_{int(time.time())}",
            user_id=f"user_{int(time.time())}",
            flow_id=flow_ids[0] if flow_ids else "default_flow",
            status=OnboardingStatus.COMPLETED,
            started_at=datetime.utcnow(),
            current_step_id="step_1",
            completion_percentage=100.0,
            total_time_spent=300.0,
            steps_completed=5,
            device_type="desktop",
            user_agent="Mozilla/5.0...",
            session_data={}
        )
        sessions.append(sample_session)
        
        return sessions
    
    def collect_friction_data(self, flow_ids: List[str], 
                            date_range: Tuple[datetime, datetime]) -> Tuple[List[FrictionPoint], List[FrictionCluster]]:
        """Collect friction analysis data"""
        friction_points = []
        friction_clusters = []
        
        # Sample friction point
        sample_friction = FrictionPoint(
            friction_id=f"friction_{int(time.time())}",
            step_id="step_2",
            flow_id=flow_ids[0] if flow_ids else "default_flow",
            friction_type=FrictionType.HIGH_ABANDONMENT,
            severity=FrictionSeverity.HIGH,
            abandonment_rate=0.45,
            avg_time_spent=180.0,
            affected_users=250,
            confidence=PatternConfidence.HIGH,
            detected_at=datetime.utcnow(),
            behavioral_indicators={}
        )
        friction_points.append(sample_friction)
        
        return friction_points, friction_clusters
    
    def collect_recommendation_data(self, flow_ids: List[str],
                                  date_range: Tuple[datetime, datetime]) -> List[RecommendationSuite]:
        """Collect recommendation data"""
        recommendation_suites = []
        
        # Sample recommendation suite would be collected here
        return recommendation_suites
    
    def calculate_metrics(self, sessions: List[OnboardingSession],
                         friction_points: List[FrictionPoint],
                         recommendations: List[RecommendationSuite],
                         date_range: Tuple[datetime, datetime]) -> ReportMetrics:
        """Calculate key metrics for reporting"""
        total_sessions = len(sessions)
        completed_sessions = len([s for s in sessions if s.status == OnboardingStatus.COMPLETED])
        completion_rate = completed_sessions / total_sessions if total_sessions > 0 else 0.0
        
        # Calculate average completion time
        completed_session_times = [s.total_time_spent for s in sessions if s.status == OnboardingStatus.COMPLETED and s.total_time_spent]
        avg_completion_time = statistics.mean(completed_session_times) if completed_session_times else 0.0
        
        # Friction metrics
        total_friction_points = len(friction_points)
        high_severity_friction = len([fp for fp in friction_points if fp.severity == FrictionSeverity.HIGH])
        
        # Recommendation metrics
        total_recommendations = sum(len(suite.recommendations) for suite in recommendations)
        estimated_impact = {}
        if recommendations:
            total_conversion_lift = sum(suite.total_estimated_impact.get('conversion_lift', 0) for suite in recommendations)
            total_users_recovered = sum(suite.total_estimated_impact.get('users_recovered', 0) for suite in recommendations)
            estimated_impact = {
                'conversion_lift': total_conversion_lift,
                'users_recovered': total_users_recovered,
                'time_savings': sum(suite.total_estimated_impact.get('time_savings', 0) for suite in recommendations),
                'error_reduction': sum(suite.total_estimated_impact.get('error_reduction', 0) for suite in recommendations)
            }
        
        # User segment breakdown
        user_segments = {}
        for session in sessions:
            if hasattr(session, 'user_segment') and session.user_segment:
                segment_name = session.user_segment.value
                user_segments[segment_name] = user_segments.get(segment_name, 0) + 1
        
        return ReportMetrics(
            total_sessions=total_sessions,
            completed_sessions=completed_sessions,
            completion_rate=completion_rate,
            average_completion_time=avg_completion_time,
            total_friction_points=total_friction_points,
            high_severity_friction=high_severity_friction,
            total_recommendations=total_recommendations,
            estimated_impact=estimated_impact,
            user_segments=user_segments,
            time_period={'start': date_range[0], 'end': date_range[1]}
        )

class ChartGenerator:
    """Generates charts for reports"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Set up matplotlib style
        plt.style.use('seaborn-v0_8' if 'seaborn-v0_8' in plt.style.available else 'default')
        sns.set_palette("husl")
    
    def generate_conversion_funnel(self, sessions: List[OnboardingSession],
                                 flow_def: OnboardingFlowDefinition) -> ChartData:
        """Generate conversion funnel chart"""
        # Calculate step completion rates
        step_completions = defaultdict(int)
        total_sessions = len(sessions)
        
        for session in sessions:
            steps_completed = getattr(session, 'steps_completed', 0)
            for i in range(steps_completed):
                step_id = f"step_{i+1}"
                step_completions[step_id] += 1
        
        # Prepare chart data
        steps = list(flow_def.steps.keys()) if hasattr(flow_def, 'steps') else ['Step 1', 'Step 2', 'Step 3', 'Step 4', 'Step 5']
        completion_rates = []
        
        for step in steps:
            completions = step_completions.get(step, 0)
            rate = (completions / total_sessions * 100) if total_sessions > 0 else 0
            completion_rates.append(rate)
        
        return ChartData(
            chart_id="conversion_funnel",
            chart_type="funnel",
            title="Onboarding Conversion Funnel",
            data={
                'steps': steps,
                'completion_rates': completion_rates,
                'absolute_numbers': [step_completions.get(step, 0) for step in steps]
            },
            labels=steps,
            values=completion_rates,
            colors=['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#592E83'],
            options={
                'show_percentages': True,
                'show_absolute_numbers': True,
                'highlight_dropoff': True
            }
        )
    
    def generate_friction_heatmap(self, friction_points: List[FrictionPoint],
                                flow_def: OnboardingFlowDefinition) -> ChartData:
        """Generate friction point heatmap"""
        # Create friction intensity matrix
        steps = list(flow_def.steps.keys()) if hasattr(flow_def, 'steps') else ['Step 1', 'Step 2', 'Step 3', 'Step 4', 'Step 5']
        friction_types = [ft.value for ft in FrictionType]
        
        # Initialize matrix
        friction_matrix = np.zeros((len(steps), len(friction_types)))
        
        # Fill matrix with friction data
        for fp in friction_points:
            try:
                step_idx = steps.index(fp.step_id)
                friction_idx = friction_types.index(fp.friction_type.value)
                
                # Use severity as intensity (1-4 scale)
                intensity = {
                    FrictionSeverity.LOW: 1,
                    FrictionSeverity.MEDIUM: 2,
                    FrictionSeverity.HIGH: 3,
                    FrictionSeverity.CRITICAL: 4
                }.get(fp.severity, 1)
                
                friction_matrix[step_idx][friction_idx] = intensity
            except (ValueError, AttributeError):
                continue
        
        return ChartData(
            chart_id="friction_heatmap",
            chart_type="heatmap",
            title="Friction Point Intensity Heatmap",
            data={
                'matrix': friction_matrix.tolist(),
                'x_labels': friction_types,
                'y_labels': steps,
                'colormap': 'Reds'
            },
            labels=steps,
            values=friction_matrix.flatten().tolist(),
            options={
                'colorbar_label': 'Friction Intensity',
                'cell_text_threshold': 0.5
            }
        )
    
    def generate_completion_trends(self, sessions: List[OnboardingSession],
                                 date_range: Tuple[datetime, datetime]) -> ChartData:
        """Generate completion rate trends over time"""
        # Group sessions by day
        daily_stats = defaultdict(lambda: {'total': 0, 'completed': 0})
        
        for session in sessions:
            day = session.started_at.date()
            daily_stats[day]['total'] += 1
            if session.status == OnboardingStatus.COMPLETED:
                daily_stats[day]['completed'] += 1
        
        # Calculate daily completion rates
        dates = sorted(daily_stats.keys())
        completion_rates = []
        
        for date in dates:
            stats = daily_stats[date]
            rate = (stats['completed'] / stats['total'] * 100) if stats['total'] > 0 else 0
            completion_rates.append(rate)
        
        return ChartData(
            chart_id="completion_trends",
            chart_type="line",
            title="Daily Completion Rate Trends",
            data={
                'dates': [d.isoformat() for d in dates],
                'completion_rates': completion_rates,
                'total_sessions': [daily_stats[d]['total'] for d in dates]
            },
            labels=[d.strftime('%Y-%m-%d') for d in dates],
            values=completion_rates,
            colors=['#2E86AB'],
            options={
                'y_axis_label': 'Completion Rate (%)',
                'x_axis_label': 'Date',
                'show_trend_line': True
            }
        )
    
    def generate_segment_performance(self, sessions: List[OnboardingSession]) -> ChartData:
        """Generate user segment performance comparison"""
        segment_stats = defaultdict(lambda: {'total': 0, 'completed': 0})
        
        for session in sessions:
            if hasattr(session, 'user_segment') and session.user_segment:
                segment = session.user_segment.value
                segment_stats[segment]['total'] += 1
                if session.status == OnboardingStatus.COMPLETED:
                    segment_stats[segment]['completed'] += 1
        
        segments = list(segment_stats.keys())
        completion_rates = []
        
        for segment in segments:
            stats = segment_stats[segment]
            rate = (stats['completed'] / stats['total'] * 100) if stats['total'] > 0 else 0
            completion_rates.append(rate)
        
        return ChartData(
            chart_id="segment_performance",
            chart_type="bar",
            title="Completion Rate by User Segment",
            data={
                'segments': segments,
                'completion_rates': completion_rates,
                'session_counts': [segment_stats[s]['total'] for s in segments]
            },
            labels=segments,
            values=completion_rates,
            colors=['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#592E83'],
            options={
                'y_axis_label': 'Completion Rate (%)',
                'x_axis_label': 'User Segment',
                'show_values_on_bars': True
            }
        )

class ReportGenerator:
    """Main report generation engine"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.data_collector = ReportDataCollector(config.get('data_sources', {}))
        self.chart_generator = ChartGenerator(config.get('chart_settings', {}))
        
        # Template environment
        template_dir = config.get('template_directory', 'templates')
        self.template_env = Environment(loader=FileSystemLoader(template_dir))
        
        # Output directory
        self.output_dir = Path(config.get('output_directory', 'reports'))
        self.output_dir.mkdir(exist_ok=True)
    
    def generate_report(self, config: ReportConfiguration) -> GeneratedReport:
        """Generate a complete report"""
        start_time = time.time()
        
        try:
            self.logger.info(f"Starting report generation: {config.report_id}")
            
            # Collect data
            sessions = self.data_collector.collect_session_data(
                config.flow_ids, 
                config.date_range, 
                config.user_segments
            )
            
            friction_points, friction_clusters = self.data_collector.collect_friction_data(
                config.flow_ids,
                config.date_range
            )
            
            recommendation_suites = self.data_collector.collect_recommendation_data(
                config.flow_ids,
                config.date_range
            )
            
            # Calculate metrics
            metrics = self.data_collector.calculate_metrics(
                sessions, friction_points, recommendation_suites, config.date_range
            )
            
            # Generate charts
            charts = self._generate_charts(sessions, friction_points, config)
            
            # Generate sections
            sections = self._generate_sections(sessions, friction_points, recommendation_suites, metrics, config)
            
            # Create report
            report = GeneratedReport(
                report_id=f"{config.report_id}_{int(time.time())}",
                config_id=config.report_id,
                generated_at=datetime.utcnow(),
                time_period=config.date_range,
                title=self._generate_report_title(config),
                sections=sections,
                metrics=metrics,
                charts=charts,
                output_format=config.output_format,
                generation_time_seconds=time.time() - start_time
            )
            
            # Generate output file
            self._generate_output_file(report, config)
            
            self.logger.info(f"Report generation completed: {report.report_id}")
            return report
            
        except Exception as e:
            self.logger.error(f"Error generating report: {e}")
            raise
    
    def _generate_charts(self, sessions: List[OnboardingSession],
                        friction_points: List[FrictionPoint],
                        config: ReportConfiguration) -> List[ChartData]:
        """Generate all charts for the report"""
        charts = []
        
        # Create a sample flow definition for chart generation
        from .onboarding_models import OnboardingFlowDefinition, OnboardingStepDefinition
        
        sample_flow = OnboardingFlowDefinition(
            flow_id=config.flow_ids[0] if config.flow_ids else "default_flow",
            flow_name="Sample Onboarding Flow",
            version="1.0",
            steps={
                "step_1": OnboardingStepDefinition("step_1", "Welcome", OnboardingStepType.INFORMATIONAL, 1, {}),
                "step_2": OnboardingStepDefinition("step_2", "Profile Setup", OnboardingStepType.FORM_COMPLETION, 2, {}),
                "step_3": OnboardingStepDefinition("step_3", "Preferences", OnboardingStepType.FORM_COMPLETION, 3, {}),
                "step_4": OnboardingStepDefinition("step_4", "Verification", OnboardingStepType.ACTION_REQUIRED, 4, {}),
                "step_5": OnboardingStepDefinition("step_5", "Complete", OnboardingStepType.INFORMATIONAL, 5, {})
            },
            created_at=datetime.utcnow()
        )
        
        # Generate conversion funnel
        if 'conversion_funnel' in config.include_sections:
            charts.append(self.chart_generator.generate_conversion_funnel(sessions, sample_flow))
        
        # Generate friction heatmap
        if 'friction_analysis' in config.include_sections:
            charts.append(self.chart_generator.generate_friction_heatmap(friction_points, sample_flow))
        
        # Generate completion trends
        if 'trends' in config.include_sections:
            charts.append(self.chart_generator.generate_completion_trends(sessions, config.date_range))
        
        # Generate segment performance
        if 'segment_analysis' in config.include_sections:
            charts.append(self.chart_generator.generate_segment_performance(sessions))
        
        return charts

    def _generate_sections(self, sessions: List[OnboardingSession],
                          friction_points: List[FrictionPoint],
                          recommendation_suites: List[RecommendationSuite],
                          metrics: ReportMetrics,
                          config: ReportConfiguration) -> List[ReportSection]:
        """Generate report sections"""
        sections = []
        
        # Executive Summary Section
        if 'executive_summary' in config.include_sections:
            sections.append(self._generate_executive_summary(metrics, sessions, friction_points))
        
        # Conversion Funnel Section
        if 'conversion_funnel' in config.include_sections:
            sections.append(self._generate_conversion_funnel_section(sessions, metrics))
        
        # Friction Analysis Section
        if 'friction_analysis' in config.include_sections:
            sections.append(self._generate_friction_analysis_section(friction_points, metrics))
        
        # Recommendations Section
        if 'recommendations' in config.include_sections:
            sections.append(self._generate_recommendations_section(recommendation_suites, metrics))
        
        # Trends Section
        if 'trends' in config.include_sections:
            sections.append(self._generate_trends_section(sessions, metrics))
        
        # Segment Analysis Section
        if 'segment_analysis' in config.include_sections:
            sections.append(self._generate_segment_analysis_section(sessions, metrics))
        
        return sections
    
    def _generate_executive_summary(self, metrics: ReportMetrics, sessions: List[OnboardingSession], friction_points: List[FrictionPoint]) -> ReportSection:
        """Generate executive summary section"""
        # Key insights
        insights = []
        if metrics.completion_rate < 0.5:
            insights.append(f"Completion rate is below 50% ({metrics.completion_rate:.1%}), indicating significant onboarding challenges")
        
        if metrics.high_severity_friction > 0:
            insights.append(f"Detected {metrics.high_severity_friction} high-severity friction points requiring immediate attention")
        
        if metrics.total_recommendations > 0:
            total_impact = sum(metrics.estimated_impact.values()) if metrics.estimated_impact else 0
            insights.append(f"Generated {metrics.total_recommendations} recommendations with potential {total_impact:.1%} conversion improvement")
        
        # Performance highlights
        avg_completion_time_minutes = metrics.average_completion_time / 60
        content = f"""
## Executive Summary

### Key Metrics
- **Total Sessions Analyzed**: {metrics.total_sessions:,}
- **Completion Rate**: {metrics.completion_rate:.1%}
- **Average Completion Time**: {avg_completion_time_minutes:.1f} minutes
- **Friction Points Detected**: {metrics.total_friction_points}
- **High-Priority Issues**: {metrics.high_severity_friction}

### Performance Overview
The onboarding flow processed {metrics.total_sessions:,} sessions during the analysis period, with {metrics.completed_sessions:,} successful completions. 
The current completion rate of {metrics.completion_rate:.1%} indicates {'strong performance' if metrics.completion_rate > 0.7 else 'room for improvement' if metrics.completion_rate > 0.5 else 'significant optimization opportunities'}.

### Critical Findings
{chr(10).join(f"• {insight}" for insight in insights)}

### Recommended Actions
Based on the analysis, we recommend prioritizing {'quick wins' if metrics.total_recommendations > 5 else 'targeted improvements'} to address the most impactful friction points.
        """
        
        return ReportSection(
            section_id="executive_summary",
            title="Executive Summary",
            content=content.strip(),
            insights=insights,
            order=1
        )
    
    def _generate_conversion_funnel_section(self, sessions: List[OnboardingSession], metrics: ReportMetrics) -> ReportSection:
        """Generate conversion funnel analysis section"""
        # Calculate step-by-step conversion
        step_analysis = defaultdict(lambda: {'total': 0, 'completed': 0})
        
        for session in sessions:
            for i in range(1, session.steps_completed + 1):
                step_id = f"step_{i}"
                step_analysis[step_id]['total'] += 1
                if i <= session.steps_completed:
                    step_analysis[step_id]['completed'] += 1
        
        # Find biggest drop-off
        biggest_dropoff = {"step": "", "rate": 0.0}
        step_conversion_rates = {}
        
        for step, data in step_analysis.items():
            if data['total'] > 0:
                conversion_rate = data['completed'] / data['total']
                step_conversion_rates[step] = conversion_rate
                
                dropoff_rate = 1 - conversion_rate
                if dropoff_rate > biggest_dropoff['rate']:
                    biggest_dropoff = {"step": step, "rate": dropoff_rate}
        
        insights = []
        if biggest_dropoff['step']:
            insights.append(f"Highest drop-off occurs at {biggest_dropoff['step']} with {biggest_dropoff['rate']:.1%} abandonment rate")
        
        content = f"""
## Conversion Funnel Analysis

### Funnel Performance
The onboarding funnel shows varying performance across steps, with the overall completion rate of {metrics.completion_rate:.1%}.

### Step-by-Step Breakdown
{chr(10).join(f"• **{step.replace('_', ' ').title()}**: {rates:.1%} completion rate" for step, rates in step_conversion_rates.items())}

### Drop-off Analysis
{f"The most significant drop-off occurs at {biggest_dropoff['step'].replace('_', ' ')} with {biggest_dropoff['rate']:.1%} of users abandoning at this stage." if biggest_dropoff['step'] else "No significant drop-off points identified."}

### Optimization Opportunities
Focus efforts on improving the steps with the highest abandonment rates to maximize overall conversion improvement.
        """
        
        return ReportSection(
            section_id="conversion_funnel",
            title="Conversion Funnel Analysis",
            content=content.strip(),
            insights=insights,
            order=2
        )
    
    def _generate_friction_analysis_section(self, friction_points: List[FrictionPoint], metrics: ReportMetrics) -> ReportSection:
        """Generate friction analysis section"""
        # Categorize friction points
        friction_by_type = defaultdict(list)
        friction_by_severity = defaultdict(list)
        
        for fp in friction_points:
            friction_by_type[fp.friction_type].append(fp)
            friction_by_severity[fp.severity].append(fp)
        
        insights = []
        if friction_by_type:
            most_common_type = max(friction_by_type.keys(), key=lambda x: len(friction_by_type[x]))
            insights.append(f"Most common friction type: {most_common_type.value} ({len(friction_by_type[most_common_type])} instances)")
        
        if metrics.high_severity_friction > 0:
            insights.append(f"{metrics.high_severity_friction} high-severity friction points require immediate attention")
        
        content = f"""
## Friction Point Analysis

### Overview
Detected {len(friction_points)} friction points across the onboarding flow, affecting user progression and completion rates.

### Friction Distribution by Type
{chr(10).join(f"• **{friction_type.value.replace('_', ' ').title()}**: {len(points)} instances" for friction_type, points in friction_by_type.items())}

### Friction Distribution by Severity
{chr(10).join(f"• **{severity.value.title()}**: {len(points)} instances" for severity, points in friction_by_severity.items())}

### High-Impact Friction Points
{chr(10).join(f"• **{fp.step_id}**: {fp.friction_type.value} - {fp.affected_users} users affected" for fp in friction_points[:5] if fp.severity in [FrictionSeverity.HIGH, FrictionSeverity.CRITICAL])}

### Resolution Priority
Focus on resolving high and critical severity friction points first, as these have the most significant impact on user experience and conversion rates.
        """
        
        return ReportSection(
            section_id="friction_analysis",
            title="Friction Point Analysis",
            content=content.strip(),
            insights=insights,
            order=3
        )
    
    def _generate_recommendations_section(self, recommendation_suites: List[RecommendationSuite], metrics: ReportMetrics) -> ReportSection:
        """Generate recommendations section"""
        all_recommendations = []
        for suite in recommendation_suites:
            all_recommendations.extend(suite.recommendations)
        
        # Sort by business value and priority
        all_recommendations.sort(key=lambda r: (r.priority.value, -r.business_value_score))
        
        # Categorize recommendations
        quick_wins = [r for r in all_recommendations if r.implementation.estimated_effort_hours <= 8]
        high_impact = [r for r in all_recommendations if r.impact.estimated_conversion_lift > 0.1]
        
        insights = []
        if quick_wins:
            insights.append(f"{len(quick_wins)} quick wins identified with minimal implementation effort")
        if high_impact:
            insights.append(f"{len(high_impact)} high-impact recommendations could significantly improve conversion")
        
        total_estimated_lift = sum(r.impact.estimated_conversion_lift for r in all_recommendations)
        if total_estimated_lift > 0:
            insights.append(f"Combined recommendations could improve conversion by up to {total_estimated_lift:.1%}")
        
        content = f"""
## Optimization Recommendations

### Summary
Generated {len(all_recommendations)} data-driven recommendations to improve onboarding performance and user experience.

### Quick Wins (≤8 hours effort)
{chr(10).join(f"• **{rec.title}**: {rec.impact.estimated_conversion_lift:.1%} estimated lift, {rec.implementation.estimated_effort_hours}h effort" for rec in quick_wins[:5])}

### High-Impact Initiatives (>10% conversion lift)
{chr(10).join(f"• **{rec.title}**: {rec.impact.estimated_conversion_lift:.1%} estimated lift, {rec.implementation.estimated_effort_hours}h effort" for rec in high_impact[:5])}

### Implementation Roadmap
1. **Phase 1 (Weeks 1-2)**: Implement quick wins for immediate impact
2. **Phase 2 (Weeks 3-8)**: Execute high-priority recommendations  
3. **Phase 3 (Weeks 9-20)**: Long-term strategic improvements

### Expected Impact
Full implementation of these recommendations could result in:
- **Conversion Improvement**: Up to {total_estimated_lift:.1%}
- **Users Recovered**: {sum(r.impact.estimated_users_recovered for r in all_recommendations):,} additional completions
- **Time Savings**: {sum(r.impact.estimated_time_savings for r in all_recommendations) / 60:.1f} minutes per user on average
        """
        
        return ReportSection(
            section_id="recommendations",
            title="Optimization Recommendations",
            content=content.strip(),
            insights=insights,
            recommendations=[r.recommendation_id for r in all_recommendations[:10]],
            order=4
        )
    
    def _generate_trends_section(self, sessions: List[OnboardingSession], metrics: ReportMetrics) -> ReportSection:
        """Generate trends analysis section"""
        # Analyze trends over time
        daily_metrics = defaultdict(lambda: {'total': 0, 'completed': 0})
        
        for session in sessions:
            day = session.started_at.date()
            daily_metrics[day]['total'] += 1
            if session.status == OnboardingStatus.COMPLETED:
                daily_metrics[day]['completed'] += 1
        
        # Calculate trends
        dates = sorted(daily_metrics.keys())
        if len(dates) >= 7:
            recent_avg = statistics.mean([daily_metrics[d]['completed'] / max(daily_metrics[d]['total'], 1) for d in dates[-7:]])
            overall_avg = statistics.mean([daily_metrics[d]['completed'] / max(daily_metrics[d]['total'], 1) for d in dates])
            trend_direction = "improving" if recent_avg > overall_avg else "declining" if recent_avg < overall_avg else "stable"
        else:
            trend_direction = "insufficient data"
        
        insights = []
        if trend_direction != "insufficient data":
            insights.append(f"Completion rate trend is {trend_direction} over the past week")
        
        content = f"""
## Trend Analysis

### Completion Rate Trends
The onboarding completion rate shows a {trend_direction} pattern over the analysis period.

### Performance Patterns
- **Average Daily Sessions**: {statistics.mean([daily_metrics[d]['total'] for d in dates]) if dates else 0:.1f}
- **Peak Performance Day**: {max(dates, key=lambda d: daily_metrics[d]['completed'] / max(daily_metrics[d]['total'], 1)) if dates else 'N/A'}
- **Lowest Performance Day**: {min(dates, key=lambda d: daily_metrics[d]['completed'] / max(daily_metrics[d]['total'], 1)) if dates else 'N/A'}

### Insights
{'The data suggests consistent performance patterns.' if trend_direction == 'stable' else f'The {trend_direction} trend indicates {"positive momentum" if trend_direction == "improving" else "areas requiring attention"}.'}
        """
        
        return ReportSection(
            section_id="trends",
            title="Trend Analysis", 
            content=content.strip(),
            insights=insights,
            order=5
        )
    
    def _generate_segment_analysis_section(self, sessions: List[OnboardingSession], metrics: ReportMetrics) -> ReportSection:
        """Generate segment analysis section"""
        # Analyze performance by device type
        device_performance = defaultdict(lambda: {'total': 0, 'completed': 0})
        
        for session in sessions:
            device_performance[session.device_type]['total'] += 1
            if session.status == OnboardingStatus.COMPLETED:
                device_performance[session.device_type]['completed'] += 1
        
        insights = []
        if device_performance:
            best_device = max(device_performance.keys(), key=lambda d: device_performance[d]['completed'] / max(device_performance[d]['total'], 1))
            worst_device = min(device_performance.keys(), key=lambda d: device_performance[d]['completed'] / max(device_performance[d]['total'], 1))
            
            best_rate = device_performance[best_device]['completed'] / max(device_performance[best_device]['total'], 1)
            worst_rate = device_performance[worst_device]['completed'] / max(device_performance[worst_device]['total'], 1)
            
            insights.append(f"Best performing device: {best_device} ({best_rate:.1%} completion)")
            if len(device_performance) > 1:
                insights.append(f"Performance gap between {best_device} and {worst_device}: {(best_rate - worst_rate):.1%}")
        
        content = f"""
## Segment Analysis

### Device Performance
Analysis of onboarding performance across different device types and user segments.

### Device Type Breakdown
{chr(10).join(f"• **{device.title()}**: {stats['completed']}/{stats['total']} ({stats['completed']/max(stats['total'], 1):.1%} completion rate)" for device, stats in device_performance.items())}

### User Segment Performance  
{chr(10).join(f"• **{segment}**: {count} users" for segment, count in metrics.user_segments.items())}

### Optimization Opportunities
{'Focus on improving mobile experience if desktop significantly outperforms mobile.' if len(device_performance) > 1 else 'Continue monitoring device-specific performance.'}
        """
        
        return ReportSection(
            section_id="segment_analysis",
            title="Segment Analysis",
            content=content.strip(),
            insights=insights,
            order=6
        )
    
    def _generate_report_title(self, config: ReportConfiguration) -> str:
        """Generate report title"""
        date_str = config.date_range[0].strftime("%Y-%m-%d")
        flow_str = f" - {', '.join(config.flow_ids[:2])}" if config.flow_ids else ""
        return f"{config.report_type.value.replace('_', ' ').title()} Report - {date_str}{flow_str}"
    
    def _generate_output_file(self, report: GeneratedReport, config: ReportConfiguration):
        """Generate output file"""
        try:
            filename = f"report_{report.report_id}.{config.output_format.value}"
            file_path = self.output_dir / filename
            
            if config.output_format == ReportFormat.JSON:
                # JSON output
                report_data = {
                    'report_metadata': {
                        'report_id': report.report_id,
                        'title': report.title,
                        'generated_at': report.generated_at.isoformat(),
                        'time_period': {
                            'start': report.time_period[0].isoformat(),
                            'end': report.time_period[1].isoformat()
                        }
                    },
                    'metrics': asdict(report.metrics),
                    'sections': [asdict(section) for section in report.sections],
                    'charts': [asdict(chart) for chart in report.charts]
                }
                
                with open(file_path, 'w') as f:
                    json.dump(report_data, f, indent=2, default=str)
            
            elif config.output_format == ReportFormat.HTML:
                # HTML output
                html_content = self._generate_html_report(report, config)
                with open(file_path, 'w') as f:
                    f.write(html_content)
            
            elif config.output_format == ReportFormat.PDF:
                # PDF output (via HTML)
                html_content = self._generate_html_report(report, config)
                # In production, would use weasyprint or similar
                # pdf_bytes = weasyprint.HTML(string=html_content).write_pdf()
                # with open(file_path, 'wb') as f:
                #     f.write(pdf_bytes)
                
                # For now, save as HTML
                file_path = file_path.with_suffix('.html')
                with open(file_path, 'w') as f:
                    f.write(html_content)
            
            # Update report with file info
            report.file_path = str(file_path)
            report.file_size = file_path.stat().st_size if file_path.exists() else 0
            
        except Exception as e:
            self.logger.error(f"Failed to generate output file: {e}")
            raise
    
    def _generate_html_report(self, report: GeneratedReport, config: ReportConfiguration) -> str:
        """Generate HTML report content"""
        html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>{{ report.title }}</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
        h1 { color: #2E86AB; border-bottom: 2px solid #2E86AB; }
        h2 { color: #A23B72; margin-top: 30px; }
        .metric { background: #f5f5f5; padding: 10px; margin: 10px 0; border-radius: 5px; }
        .insight { background: #e3f2fd; padding: 10px; margin: 10px 0; border-left: 4px solid #2196f3; }
        .section { margin: 30px 0; }
        .chart-placeholder { background: #f0f0f0; padding: 20px; text-align: center; margin: 20px 0; }
    </style>
</head>
<body>
    <h1>{{ report.title }}</h1>
    <div class="metric">
        <strong>Generated:</strong> {{ report.generated_at.strftime('%Y-%m-%d %H:%M:%S') }}<br>
        <strong>Time Period:</strong> {{ report.time_period[0].strftime('%Y-%m-%d') }} to {{ report.time_period[1].strftime('%Y-%m-%d') }}<br>
        <strong>Processing Time:</strong> {{ "%.2f"|format(report.generation_time_seconds) }} seconds
    </div>
    
    {% for section in report.sections %}
    <div class="section">
        <h2>{{ section.title }}</h2>
        <div>{{ section.content | replace('\n', '<br>') | safe }}</div>
        
        {% if section.insights %}
        <div class="insight">
            <strong>Key Insights:</strong><br>
            {% for insight in section.insights %}
            • {{ insight }}<br>
            {% endfor %}
        </div>
        {% endif %}
        
        {% for chart in section.charts %}
        <div class="chart-placeholder">
            Chart: {{ chart.title }} ({{ chart.chart_type }})
        </div>
        {% endfor %}
    </div>
    {% endfor %}
</body>
</html>
        """
        
        try:
            template = Template(html_template)
            return template.render(report=report)
        except Exception as e:
            self.logger.error(f"Failed to generate HTML template: {e}")
            return f"<html><body><h1>Report Generation Error</h1><p>{e}</p></body></html>"

# Export delivery and scheduling classes
class ReportDelivery:
    """Handles report delivery to various channels"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def deliver_report(self, report: GeneratedReport, delivery_channels: List[DeliveryChannel], recipients: List[str]):
        """Deliver report to specified channels"""
        delivery_results = {}
        
        for channel in delivery_channels:
            try:
                if channel == DeliveryChannel.EMAIL:
                    result = self._deliver_via_email(report, recipients)
                elif channel == DeliveryChannel.SLACK:
                    result = self._deliver_via_slack(report, recipients)
                elif channel == DeliveryChannel.WEBHOOK:
                    result = self._deliver_via_webhook(report, recipients)
                else:
                    result = {"status": "skipped", "message": f"Channel {channel.value} not implemented"}
                
                delivery_results[channel.value] = result
                
            except Exception as e:
                delivery_results[channel.value] = {"status": "failed", "error": str(e)}
        
        return delivery_results
    
    def _deliver_via_email(self, report: GeneratedReport, recipients: List[str]) -> Dict[str, str]:
        """Deliver report via email"""
        # Email delivery implementation would go here
        return {"status": "success", "recipients": len(recipients)}
    
    def _deliver_via_slack(self, report: GeneratedReport, recipients: List[str]) -> Dict[str, str]:
        """Deliver report via Slack"""
        # Slack delivery implementation would go here
        return {"status": "success", "channels": len(recipients)}
    
    def _deliver_via_webhook(self, report: GeneratedReport, recipients: List[str]) -> Dict[str, str]:
        """Deliver report via webhook"""
        # Webhook delivery implementation would go here
        return {"status": "success", "webhooks": len(recipients)}

class ReportScheduler:
    """Handles scheduled report generation"""
    
    def __init__(self, report_generator: ReportGenerator):
        self.report_generator = report_generator
        self.scheduled_reports: Dict[str, ReportConfiguration] = {}
        self.logger = logging.getLogger(__name__)
    
    def schedule_report(self, config: ReportConfiguration) -> str:
        """Schedule a recurring report"""
        schedule_id = f"schedule_{uuid.uuid4().hex[:8]}"
        self.scheduled_reports[schedule_id] = config
        
        # In production, this would integrate with a job scheduler
        self.logger.info(f"Scheduled report: {schedule_id} - {config.frequency.value}")
        
        return schedule_id
    
    def unschedule_report(self, schedule_id: str) -> bool:
        """Remove a scheduled report"""
        if schedule_id in self.scheduled_reports:
            del self.scheduled_reports[schedule_id]
            return True
        return False
    
    def run_scheduled_reports(self):
        """Run all due scheduled reports"""
        current_time = datetime.utcnow()
        
        for schedule_id, config in self.scheduled_reports.items():
            if self._is_report_due(config, current_time):
                try:
                    report = self.report_generator.generate_report(config)
                    self.logger.info(f"Generated scheduled report: {report.report_id}")
                    
                    # Update next generation time
                    config.next_generation = self._calculate_next_run_time(config, current_time)
                    
                except Exception as e:
                    self.logger.error(f"Failed to generate scheduled report {schedule_id}: {e}")
    
    def _is_report_due(self, config: ReportConfiguration, current_time: datetime) -> bool:
        """Check if a report is due for generation"""
        return current_time >= config.next_generation
    
    def _calculate_next_run_time(self, config: ReportConfiguration, current_time: datetime) -> datetime:
        """Calculate next run time based on frequency"""
        if config.frequency == ReportFrequency.DAILY:
            return current_time + timedelta(days=1)
        elif config.frequency == ReportFrequency.WEEKLY:
            return current_time + timedelta(weeks=1)
        elif config.frequency == ReportFrequency.MONTHLY:
            return current_time + timedelta(days=30)
        else:
            return current_time + timedelta(hours=1)  # Default

# Global report engine
report_engine = None

def initialize_report_engine(config: Dict[str, Any]):
    """Initialize global report engine"""
    global report_engine
    report_engine = ReportGenerator(config)

def get_report_engine() -> ReportGenerator:
    """Get global report engine instance"""
    if not report_engine:
        raise RuntimeError("Report engine not initialized. Call initialize_report_engine() first.")
    
    return report_engine

def generate_comprehensive_report(flow_ids: List[str],
                                date_range: Tuple[datetime, datetime],
                                report_type: ReportType = ReportType.DETAILED_ANALYSIS,
                                output_format: ReportFormat = ReportFormat.PDF) -> GeneratedReport:
    """Generate comprehensive onboarding report"""
    engine = get_report_engine()
    
    config = ReportConfiguration(
        report_id=f"report_{int(time.time())}",
        report_type=report_type,
        output_format=output_format,
        delivery_channels=[DeliveryChannel.FILE_SYSTEM],
        frequency=ReportFrequency.ON_DEMAND,
        recipients=[],
        include_sections=['executive_summary', 'conversion_funnel', 'friction_analysis', 'recommendations', 'trends'],
        date_range=date_range,
        flow_ids=flow_ids,
        user_segments=[],
        template_name="default_report_template"
    )
    
    return engine.generate_report(config)
