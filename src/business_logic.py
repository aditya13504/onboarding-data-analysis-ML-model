"""
Point 18: Business Logic Layer (Part 1/2)
Core business logic orchestration for onboarding drop-off analysis and optimization.
"""

import os
import json
import time
import threading
import asyncio
from typing import Dict, List, Optional, Any, Union, Set, Tuple, NamedTuple, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, Counter
import logging
import statistics
import math
from abc import ABC, abstractmethod
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid
from sqlalchemy import create_engine, text
from onboarding_analyzer.infrastructure.db import engine, SessionLocal

# Import from our modules
from .onboarding_models import (
    OnboardingSession, OnboardingStepAttempt, OnboardingFlowDefinition,
    OnboardingStatus, OnboardingStepType, UserSegment, FeatureEngineer,
    SessionReconstructor, UserProfile
)
from .friction_detection import (
    FrictionPoint, FrictionCluster, FrictionType, FrictionSeverity,
    FrictionPatternDetector, FrictionClustering, AdvancedFrictionAnalyzer
)
from .recommendation_engine import (
    Recommendation, RecommendationSuite, RecommendationType,
    RecommendationPriority, RecommendationEngine
)
from .report import (
    GeneratedReport, ReportConfiguration, ReportType,
    ReportGenerator
)
from .integrations import (
    IntegrationConfiguration, IntegrationType, SyncOperation,
    BaseIntegrationAdapter, AnalyticsPlatformAdapter, CRMSystemAdapter
)

class AnalysisScope(Enum):
    """Scope of analysis"""
    SINGLE_FLOW = "single_flow"
    MULTIPLE_FLOWS = "multiple_flows"
    USER_SEGMENT = "user_segment"
    TIME_PERIOD = "time_period"
    GLOBAL = "global"

class OptimizationGoal(Enum):
    """Optimization goals"""
    MAXIMIZE_COMPLETION = "maximize_completion"
    MINIMIZE_TIME_TO_COMPLETE = "minimize_time_to_complete"
    REDUCE_FRICTION_POINTS = "reduce_friction_points"
    IMPROVE_USER_SATISFACTION = "improve_user_satisfaction"
    INCREASE_ENGAGEMENT = "increase_engagement"
    OPTIMIZE_CONVERSION_VALUE = "optimize_conversion_value"

class AnalysisStatus(Enum):
    """Analysis operation status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class RecommendationStatus(Enum):
    """Recommendation implementation status"""
    GENERATED = "generated"
    APPROVED = "approved"
    IN_PROGRESS = "in_progress"
    IMPLEMENTED = "implemented"
    TESTING = "testing"
    REJECTED = "rejected"
    ARCHIVED = "archived"

@dataclass
class AnalysisRequest:
    """Request for onboarding analysis"""
    request_id: str
    scope: AnalysisScope
    goals: List[OptimizationGoal]
    
    # Scope parameters
    flow_ids: List[str]
    user_segments: List[UserSegment]
    date_range: Tuple[datetime, datetime]
    
    # Analysis settings
    include_friction_detection: bool = True
    include_recommendation_generation: bool = True
    include_trend_analysis: bool = True
    include_segment_analysis: bool = True
    
    # Quality filters
    min_session_count: int = 100
    confidence_threshold: float = 0.7
    significance_threshold: float = 0.05
    
    # Processing settings
    priority: str = "normal"  # low, normal, high, urgent
    async_processing: bool = True
    notify_on_completion: bool = True
    
    # Metadata
    requested_by: str = "system"
    requested_at: datetime = field(default_factory=datetime.utcnow)
    tags: List[str] = field(default_factory=list)

@dataclass
class AnalysisResult:
    """Results of onboarding analysis"""
    request_id: str
    analysis_id: str
    status: AnalysisStatus
    
    # Timing
    started_at: datetime
    completed_at: Optional[datetime] = None
    processing_time_seconds: float = 0.0
    
    # Data processed
    sessions_analyzed: int = 0
    flows_analyzed: int = 0
    friction_points_detected: int = 0
    recommendations_generated: int = 0
    
    # Results
    friction_analysis: List[FrictionPoint] = field(default_factory=list)
    friction_clusters: List[FrictionCluster] = field(default_factory=list)
    recommendation_suites: List[RecommendationSuite] = field(default_factory=list)
    trend_insights: Dict[str, Any] = field(default_factory=dict)
    segment_insights: Dict[str, Any] = field(default_factory=dict)
    
    # Quality metrics
    analysis_confidence: float = 0.0
    data_quality_score: float = 0.0
    statistical_significance: Dict[str, float] = field(default_factory=dict)
    
    # Errors and warnings
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Output artifacts
    generated_reports: List[str] = field(default_factory=list)
    exported_data: Dict[str, str] = field(default_factory=dict)

@dataclass
class OptimizationPlan:
    """Comprehensive optimization plan"""
    plan_id: str
    flow_id: str
    created_from_analysis: str
    
    # Goals and targets
    optimization_goals: List[OptimizationGoal]
    target_metrics: Dict[str, float]
    success_criteria: Dict[str, Any]
    
    # Implementation phases
    phases: List[Dict[str, Any]] = field(default_factory=list)
    quick_wins: List[str] = field(default_factory=list)  # Recommendation IDs
    major_initiatives: List[str] = field(default_factory=list)
    long_term_improvements: List[str] = field(default_factory=list)
    
    # Resource planning
    estimated_effort_hours: float = 0.0
    required_skills: List[str] = field(default_factory=list)
    stakeholders: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    
    # Timeline
    estimated_duration_weeks: int = 0
    start_date: Optional[datetime] = None
    target_completion_date: Optional[datetime] = None
    
    # Impact projections
    projected_conversion_lift: float = 0.0
    projected_users_recovered: int = 0
    projected_roi: Optional[float] = None
    
    # Risk assessment
    implementation_risks: List[Dict[str, Any]] = field(default_factory=list)
    mitigation_strategies: List[Dict[str, Any]] = field(default_factory=list)
    
    # Tracking
    implementation_status: str = "planned"
    progress_percentage: float = 0.0
    last_updated: datetime = field(default_factory=datetime.utcnow)

class BusinessLogicOrchestrator:
    """Main orchestrator for business logic operations"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.session_reconstructor = SessionReconstructor(config.get('session_reconstruction', {}))
        self.feature_engineer = FeatureEngineer(config.get('feature_engineering', {}))
        self.friction_detector = FrictionPatternDetector(config.get('friction_detection', {}))
        self.friction_clusterer = FrictionClustering(config.get('friction_clustering', {}))
        self.advanced_friction_analyzer = AdvancedFrictionAnalyzer(config.get('advanced_friction', {}))
        
        # Analysis storage
        self.active_analyses: Dict[str, AnalysisResult] = {}
        self.optimization_plans: Dict[str, OptimizationPlan] = {}
        
        # Thread pool for async processing
        self.executor = ThreadPoolExecutor(max_workers=config.get('max_workers', 4))
        
        # Business rules
        self.business_rules = config.get('business_rules', {})
        
        # Quality thresholds
        self.quality_thresholds = {
            'min_sessions_for_analysis': config.get('min_sessions_for_analysis', 100),
            'min_confidence_for_recommendations': config.get('min_confidence_for_recommendations', 0.7),
            'max_processing_time_minutes': config.get('max_processing_time_minutes', 30)
        }
    
    def submit_analysis_request(self, request: AnalysisRequest) -> str:
        """Submit a new analysis request"""
        analysis_id = f"analysis_{uuid.uuid4().hex[:8]}"
        
        # Validate request
        validation_errors = self._validate_analysis_request(request)
        if validation_errors:
            raise ValueError(f"Invalid analysis request: {validation_errors}")
        
        # Create analysis result placeholder
        analysis_result = AnalysisResult(
            request_id=request.request_id,
            analysis_id=analysis_id,
            status=AnalysisStatus.PENDING,
            started_at=datetime.utcnow()
        )
        
        self.active_analyses[analysis_id] = analysis_result
        
        # Submit for processing
        if request.async_processing:
            future = self.executor.submit(self._process_analysis_request, request, analysis_result)
            # Store future for potential cancellation
        else:
            self._process_analysis_request(request, analysis_result)
        
        self.logger.info(f"Analysis request submitted: {analysis_id}")
        return analysis_id
    
    def get_analysis_status(self, analysis_id: str) -> Optional[AnalysisResult]:
        """Get the status of an analysis"""
        return self.active_analyses.get(analysis_id)
    
    def cancel_analysis(self, analysis_id: str) -> bool:
        """Cancel a running analysis"""
        if analysis_id in self.active_analyses:
            analysis = self.active_analyses[analysis_id]
            if analysis.status == AnalysisStatus.RUNNING:
                analysis.status = AnalysisStatus.CANCELLED
                return True
        return False
    
    def _validate_analysis_request(self, request: AnalysisRequest) -> List[str]:
        """Validate analysis request parameters"""
        errors = []
        
        # Check date range
        if request.date_range[0] >= request.date_range[1]:
            errors.append("Invalid date range: start date must be before end date")
        
        # Check if date range is too large
        date_span = request.date_range[1] - request.date_range[0]
        if date_span.days > 365:
            errors.append("Date range too large: maximum 365 days allowed")
        
        # Check flow IDs
        if request.scope in [AnalysisScope.SINGLE_FLOW, AnalysisScope.MULTIPLE_FLOWS] and not request.flow_ids:
            errors.append("Flow IDs required for flow-based analysis")
        
        # Check user segments
        if request.scope == AnalysisScope.USER_SEGMENT and not request.user_segments:
            errors.append("User segments required for segment-based analysis")
        
        # Check minimum session count
        if request.min_session_count < 10:
            errors.append("Minimum session count must be at least 10")
        
        return errors
    
    def _process_analysis_request(self, request: AnalysisRequest, analysis_result: AnalysisResult):
        """Process an analysis request"""
        try:
            analysis_result.status = AnalysisStatus.RUNNING
            start_time = time.time()
            
            self.logger.info(f"Starting analysis: {analysis_result.analysis_id}")
            
            # Step 1: Collect and reconstruct sessions
            sessions = self._collect_sessions(request)
            analysis_result.sessions_analyzed = len(sessions)
            
            if len(sessions) < request.min_session_count:
                analysis_result.warnings.append(
                    f"Low session count: {len(sessions)} (minimum recommended: {request.min_session_count})"
                )
            
            # Step 2: Feature engineering
            if sessions:
                features = self._engineer_features(sessions, request)
            
            # Step 3: Friction detection
            if request.include_friction_detection and sessions:
                friction_points, friction_clusters = self._detect_friction(sessions, request)
                analysis_result.friction_analysis = friction_points
                analysis_result.friction_clusters = friction_clusters
                analysis_result.friction_points_detected = len(friction_points)
            
            # Step 4: Generate recommendations
            if request.include_recommendation_generation and analysis_result.friction_analysis:
                recommendation_suites = self._generate_recommendations(
                    analysis_result.friction_analysis,
                    analysis_result.friction_clusters,
                    sessions,
                    request
                )
                analysis_result.recommendation_suites = recommendation_suites
                analysis_result.recommendations_generated = sum(len(suite.recommendations) for suite in recommendation_suites)
            
            # Step 5: Trend analysis
            if request.include_trend_analysis:
                trend_insights = self._analyze_trends(sessions, request)
                analysis_result.trend_insights = trend_insights
            
            # Step 6: Segment analysis
            if request.include_segment_analysis:
                segment_insights = self._analyze_segments(sessions, request)
                analysis_result.segment_insights = segment_insights
            
            # Step 7: Calculate quality metrics
            self._calculate_quality_metrics(analysis_result, request)
            
            # Step 8: Apply business rules
            self._apply_business_rules(analysis_result, request)
            
            # Complete analysis
            analysis_result.status = AnalysisStatus.COMPLETED
            analysis_result.completed_at = datetime.utcnow()
            analysis_result.processing_time_seconds = time.time() - start_time
            
            self.logger.info(f"Analysis completed: {analysis_result.analysis_id}")
            
        except Exception as e:
            analysis_result.status = AnalysisStatus.FAILED
            analysis_result.errors.append(str(e))
            analysis_result.completed_at = datetime.utcnow()
            self.logger.error(f"Analysis failed: {analysis_result.analysis_id}, Error: {e}")
    
    def _collect_sessions(self, request: AnalysisRequest) -> List[OnboardingSession]:
        """Collect onboarding sessions based on request scope"""
        sessions = []
        
        # In real implementation, this would query the actual data sources
        # For now, create sample sessions
        for i in range(request.min_session_count):
            session = OnboardingSession(
                session_id=f"session_{i}",
                user_id=f"user_{i}",
                flow_id=request.flow_ids[0] if request.flow_ids else "default_flow",
                status=OnboardingStatus.COMPLETED if i % 3 == 0 else OnboardingStatus.ABANDONED,
                started_at=request.date_range[0] + timedelta(minutes=i * 10),
                current_step_id=f"step_{(i % 5) + 1}",
                completion_percentage=100.0 if i % 3 == 0 else (i % 4) * 25,
                total_time_spent=300.0 + (i % 10) * 30,
                steps_completed=5 if i % 3 == 0 else (i % 4) + 1,
                device_type="desktop" if i % 2 == 0 else "mobile",
                user_agent="Mozilla/5.0...",
                session_data={}
            )
            sessions.append(session)
        
        return sessions
    
    def _engineer_features(self, sessions: List[OnboardingSession], request: AnalysisRequest) -> Dict[str, Any]:
        """Engineer features from sessions"""
        try:
            features = {}
            
            # Basic completion metrics
            total_sessions = len(sessions)
            completed_sessions = [s for s in sessions if s.status == OnboardingStatus.COMPLETED]
            completion_rate = len(completed_sessions) / total_sessions if total_sessions > 0 else 0
            
            features['completion_rate'] = completion_rate
            features['total_sessions'] = total_sessions
            features['completed_sessions'] = len(completed_sessions)
            
            # Time-based features
            completion_times = [s.total_time_spent for s in completed_sessions if s.total_time_spent]
            if completion_times:
                features['avg_completion_time'] = statistics.mean(completion_times)
                features['median_completion_time'] = statistics.median(completion_times)
                features['completion_time_std'] = statistics.stdev(completion_times) if len(completion_times) > 1 else 0
            
            # Step progression features
            step_completions = defaultdict(int)
            for session in sessions:
                for i in range(session.steps_completed):
                    step_completions[f"step_{i+1}"] += 1
            
            features['step_completion_rates'] = {
                step: count / total_sessions for step, count in step_completions.items()
            }
            
            # Device and segment features
            device_completion = defaultdict(lambda: {'total': 0, 'completed': 0})
            for session in sessions:
                device_completion[session.device_type]['total'] += 1
                if session.status == OnboardingStatus.COMPLETED:
                    device_completion[session.device_type]['completed'] += 1
            
            features['device_performance'] = {
                device: stats['completed'] / stats['total'] if stats['total'] > 0 else 0
                for device, stats in device_completion.items()
            }
            
            return features
            
        except Exception as e:
            self.logger.error(f"Feature engineering failed: {e}")
            return {}
    
    def _detect_friction(self, sessions: List[OnboardingSession], 
                        request: AnalysisRequest) -> Tuple[List[FrictionPoint], List[FrictionCluster]]:
        """Detect friction points and clusters"""
        try:
            friction_points = []
            friction_clusters = []
            
            # Analyze each flow
            flows = set(s.flow_id for s in sessions)
            
            for flow_id in flows:
                flow_sessions = [s for s in sessions if s.flow_id == flow_id]
                
                if len(flow_sessions) < 10:  # Skip flows with too few sessions
                    continue
                
                # Basic friction detection
                flow_friction_points = self._detect_flow_friction_points(flow_sessions, flow_id)
                friction_points.extend(flow_friction_points)
                
                # Clustering analysis
                if len(flow_sessions) >= 50:  # Only cluster if enough data
                    flow_clusters = self._cluster_friction_patterns(flow_sessions, flow_id)
                    friction_clusters.extend(flow_clusters)
            
            return friction_points, friction_clusters
            
        except Exception as e:
            self.logger.error(f"Friction detection failed: {e}")
            return [], []
    
    def _detect_flow_friction_points(self, sessions: List[OnboardingSession], flow_id: str) -> List[FrictionPoint]:
        """Detect friction points for a specific flow"""
        friction_points = []
        
        # Analyze each step
        steps = set()
        for session in sessions:
            for i in range(1, session.steps_completed + 1):
                steps.add(f"step_{i}")
        
        for step_id in steps:
            # Calculate step metrics
            step_sessions = [s for s in sessions if s.steps_completed >= int(step_id.split('_')[1])]
            abandoned_at_step = [s for s in sessions if s.current_step_id == step_id and s.status == OnboardingStatus.ABANDONED]
            
            abandonment_rate = len(abandoned_at_step) / len(step_sessions) if step_sessions else 0
            
            # Check for high abandonment
            if abandonment_rate > 0.3:  # 30% abandonment threshold
                friction_point = FrictionPoint(
                    friction_id=f"friction_{flow_id}_{step_id}_{int(time.time())}",
                    step_id=step_id,
                    flow_id=flow_id,
                    friction_type=FrictionType.HIGH_ABANDONMENT,
                    severity=FrictionSeverity.HIGH if abandonment_rate > 0.5 else FrictionSeverity.MEDIUM,
                    abandonment_rate=abandonment_rate,
                    avg_time_spent=statistics.mean([s.total_time_spent for s in step_sessions if s.total_time_spent]) or 0,
                    affected_users=len(abandoned_at_step),
                    confidence=0.8,  # High confidence for abandonment rate calculation
                    detected_at=datetime.utcnow(),
                    behavioral_indicators={}
                )
                friction_points.append(friction_point)
        
        return friction_points
    
    def _cluster_friction_patterns(self, sessions: List[OnboardingSession], flow_id: str) -> List[FrictionCluster]:
        """Cluster friction patterns for a flow"""
        # Simplified clustering implementation
        # In real implementation, this would use the FrictionClustering class
        clusters = []
        
        # Group sessions by completion pattern
        completion_patterns = defaultdict(list)
        for session in sessions:
            pattern_key = f"{session.status.value}_{session.steps_completed}"
            completion_patterns[pattern_key].append(session)
        
        # Create clusters for significant patterns
        for pattern, pattern_sessions in completion_patterns.items():
            if len(pattern_sessions) >= 10:  # Minimum cluster size
                cluster = FrictionCluster(
                    cluster_id=f"cluster_{flow_id}_{pattern}_{int(time.time())}",
                    cluster_name=f"Pattern: {pattern.replace('_', ' ').title()}",
                    friction_points=[],  # Would be populated with actual friction points
                    user_count=len(pattern_sessions),
                    primary_segment=UserSegment.GENERAL,  # Would be determined from sessions
                    cluster_characteristics={
                        'completion_pattern': pattern,
                        'avg_completion_time': statistics.mean([s.total_time_spent for s in pattern_sessions if s.total_time_spent]) or 0,
                        'device_distribution': Counter([s.device_type for s in pattern_sessions])
                    },
                    user_personas={},
                    intervention_opportunities=[],
                    confidence_score=0.7,
                    detected_at=datetime.utcnow()
                )
                clusters.append(cluster)
        
        return clusters

class RecommendationOrchestrator:
    """Orchestrates recommendation generation and prioritization"""
    
    def __init__(self):
        from src.recommendation_engine import RecommendationEngine
        self.recommendation_engine = RecommendationEngine()
        self.logger = logging.getLogger(__name__)
    
    def generate_comprehensive_recommendations(self, 
                                             friction_points: List[FrictionPoint],
                                             sessions: List[OnboardingSession],
                                             clusters: List[Dict[str, Any]]) -> List[Recommendation]:
        """Generate comprehensive recommendations from multiple sources"""
        all_recommendations = []
        
        try:
            # Generate friction-specific recommendations
            friction_recommendations = self.recommendation_engine.generate_friction_recommendations(friction_points)
            all_recommendations.extend(friction_recommendations.recommendations)
            
            # Generate cluster-based recommendations
            cluster_recommendations = self.recommendation_engine.generate_cluster_recommendations(clusters, sessions)
            all_recommendations.extend(cluster_recommendations.recommendations)
            
            # Generate flow optimization recommendations
            flow_recommendations = self.recommendation_engine.generate_flow_recommendations(sessions)
            all_recommendations.extend(flow_recommendations.recommendations)
            
            # Deduplicate and prioritize recommendations
            deduplicated_recommendations = self._deduplicate_recommendations(all_recommendations)
            prioritized_recommendations = self._prioritize_recommendations(deduplicated_recommendations)
            
            return prioritized_recommendations
            
        except Exception as e:
            self.logger.error(f"Failed to generate comprehensive recommendations: {e}")
            return []
    
    def _deduplicate_recommendations(self, recommendations: List[Recommendation]) -> List[Recommendation]:
        """Remove duplicate recommendations based on similarity"""
        unique_recommendations = []
        seen_titles = set()
        
        for rec in recommendations:
            # Create a normalized title for comparison
            normalized_title = rec.title.lower().strip()
            
            if normalized_title not in seen_titles:
                unique_recommendations.append(rec)
                seen_titles.add(normalized_title)
        
        return unique_recommendations
    
    def _prioritize_recommendations(self, recommendations: List[Recommendation]) -> List[Recommendation]:
        """Prioritize recommendations based on business value and effort"""
        
        # Calculate priority score: business value / effort ratio
        for rec in recommendations:
            effort_factor = max(rec.implementation.estimated_effort_hours, 1)
            rec.priority_score = rec.business_value_score / effort_factor
        
        # Sort by priority score and business impact
        prioritized = sorted(
            recommendations,
            key=lambda r: (r.priority.value, -r.priority_score, -r.impact.estimated_conversion_lift),
            reverse=False  # Lower priority enum values come first
        )
        
        return prioritized

class TrendAnalyzer:
    """Analyzes trends in onboarding data"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def analyze_completion_trends(self, sessions: List[OnboardingSession]) -> Dict[str, Any]:
        """Analyze completion rate trends over time"""
        
        # Group sessions by day
        daily_stats = defaultdict(lambda: {'total': 0, 'completed': 0})
        
        for session in sessions:
            day = session.started_at.date()
            daily_stats[day]['total'] += 1
            if session.status == OnboardingStatus.COMPLETED:
                daily_stats[day]['completed'] += 1
        
        # Calculate daily completion rates
        daily_rates = {}
        for day, stats in daily_stats.items():
            daily_rates[day] = stats['completed'] / max(stats['total'], 1)
        
        # Calculate trend metrics
        dates = sorted(daily_rates.keys())
        rates = [daily_rates[date] for date in dates]
        
        if len(rates) >= 2:
            # Calculate trend direction
            recent_avg = statistics.mean(rates[-7:]) if len(rates) >= 7 else statistics.mean(rates[-3:])
            overall_avg = statistics.mean(rates)
            trend_direction = 'improving' if recent_avg > overall_avg * 1.05 else 'declining' if recent_avg < overall_avg * 0.95 else 'stable'
            
            # Calculate trend strength
            if len(rates) >= 7:
                correlation = self._calculate_trend_correlation(list(range(len(rates))), rates)
                trend_strength = abs(correlation)
            else:
                trend_strength = 0.0
        else:
            trend_direction = 'insufficient_data'
            trend_strength = 0.0
        
        return {
            'trend_direction': trend_direction,
            'trend_strength': trend_strength,
            'current_rate': rates[-1] if rates else 0.0,
            'average_rate': statistics.mean(rates) if rates else 0.0,
            'best_day': max(dates, key=lambda d: daily_rates[d]) if dates else None,
            'worst_day': min(dates, key=lambda d: daily_rates[d]) if dates else None,
            'daily_data': dict(zip(dates, rates))
        }
    
    def analyze_friction_trends(self, friction_points: List[FrictionPoint]) -> Dict[str, Any]:
        """Analyze friction point trends"""
        
        # Group friction points by type and day
        friction_by_day = defaultdict(lambda: defaultdict(int))
        
        for fp in friction_points:
            day = fp.detected_at.date()
            friction_by_day[day][fp.friction_type.value] += 1
        
        # Find trending friction types
        friction_trends = {}
        for friction_type in FrictionType:
            type_counts = []
            dates = sorted(friction_by_day.keys())
            
            for date in dates:
                count = friction_by_day[date].get(friction_type.value, 0)
                type_counts.append(count)
            
            if len(type_counts) >= 3:
                recent_avg = statistics.mean(type_counts[-3:])
                overall_avg = statistics.mean(type_counts) if type_counts else 0
                
                if recent_avg > overall_avg * 1.2:
                    trend = 'increasing'
                elif recent_avg < overall_avg * 0.8:
                    trend = 'decreasing'
                else:
                    trend = 'stable'
                
                friction_trends[friction_type.value] = {
                    'trend': trend,
                    'recent_average': recent_avg,
                    'overall_average': overall_avg,
                    'total_occurrences': sum(type_counts)
                }
        
        return friction_trends
    
    def _calculate_trend_correlation(self, x: List[float], y: List[float]) -> float:
        """Calculate correlation coefficient for trend analysis"""
        if len(x) != len(y) or len(x) < 2:
            return 0.0
        
        n = len(x)
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(x[i] * y[i] for i in range(n))
        sum_x2 = sum(xi * xi for xi in x)
        sum_y2 = sum(yi * yi for yi in y)
        
        numerator = n * sum_xy - sum_x * sum_y
        denominator = ((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)) ** 0.5
        
        return numerator / denominator if denominator != 0 else 0.0

class SegmentAnalyzer:
    """Analyzes user segments and their onboarding performance"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def analyze_device_segments(self, sessions: List[OnboardingSession]) -> Dict[str, Any]:
        """Analyze performance by device type"""
        
        device_stats = defaultdict(lambda: {
            'total_sessions': 0,
            'completed_sessions': 0,
            'average_completion_time': 0,
            'average_steps_completed': 0,
            'friction_points': 0
        })
        
        for session in sessions:
            device = session.device_type
            device_stats[device]['total_sessions'] += 1
            
            if session.status == OnboardingStatus.COMPLETED:
                device_stats[device]['completed_sessions'] += 1
                if session.completed_at:
                    completion_time = (session.completed_at - session.started_at).total_seconds()
                    device_stats[device]['average_completion_time'] += completion_time
            
            device_stats[device]['average_steps_completed'] += session.steps_completed
        
        # Calculate averages and rates
        device_analysis = {}
        for device, stats in device_stats.items():
            if stats['total_sessions'] > 0:
                completion_rate = stats['completed_sessions'] / stats['total_sessions']
                avg_completion_time = stats['average_completion_time'] / max(stats['completed_sessions'], 1)
                avg_steps = stats['average_steps_completed'] / stats['total_sessions']
                
                device_analysis[device] = {
                    'completion_rate': completion_rate,
                    'average_completion_time_minutes': avg_completion_time / 60,
                    'average_steps_completed': avg_steps,
                    'total_sessions': stats['total_sessions'],
                    'performance_score': completion_rate * 0.7 + (avg_steps / 10) * 0.3  # Weighted score
                }
        
        # Rank devices by performance
        ranked_devices = sorted(
            device_analysis.items(),
            key=lambda x: x[1]['performance_score'],
            reverse=True
        )
        
        return {
            'device_performance': device_analysis,
            'best_performing_device': ranked_devices[0][0] if ranked_devices else None,
            'worst_performing_device': ranked_devices[-1][0] if ranked_devices else None,
            'performance_gap': ranked_devices[0][1]['performance_score'] - ranked_devices[-1][1]['performance_score'] if len(ranked_devices) > 1 else 0
        }
    
    def analyze_temporal_segments(self, sessions: List[OnboardingSession]) -> Dict[str, Any]:
        """Analyze performance by time periods"""
        
        hourly_stats = defaultdict(lambda: {'total': 0, 'completed': 0})
        daily_stats = defaultdict(lambda: {'total': 0, 'completed': 0})
        
        for session in sessions:
            hour = session.started_at.hour
            day_of_week = session.started_at.strftime('%A')
            
            hourly_stats[hour]['total'] += 1
            daily_stats[day_of_week]['total'] += 1
            
            if session.status == OnboardingStatus.COMPLETED:
                hourly_stats[hour]['completed'] += 1
                daily_stats[day_of_week]['completed'] += 1
        
        # Calculate completion rates
        hourly_rates = {
            hour: stats['completed'] / max(stats['total'], 1)
            for hour, stats in hourly_stats.items()
        }
        
        daily_rates = {
            day: stats['completed'] / max(stats['total'], 1)
            for day, stats in daily_stats.items()
        }
        
        return {
            'hourly_performance': hourly_rates,
            'daily_performance': daily_rates,
            'peak_hour': max(hourly_rates.keys(), key=lambda h: hourly_rates[h]) if hourly_rates else None,
            'best_day': max(daily_rates.keys(), key=lambda d: daily_rates[d]) if daily_rates else None,
            'peak_hour_rate': max(hourly_rates.values()) if hourly_rates else 0,
            'best_day_rate': max(daily_rates.values()) if daily_rates else 0
        }

class QualityMetricsCalculator:
    """Calculates various quality metrics for onboarding analysis"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def calculate_data_quality_score(self, sessions: List[OnboardingSession]) -> float:
        """Calculate overall data quality score"""
        
        if not sessions:
            return 0.0
        
        quality_factors = {
            'completeness': self._calculate_completeness_score(sessions),
            'consistency': self._calculate_consistency_score(sessions),
            'validity': self._calculate_validity_score(sessions),
            'timeliness': self._calculate_timeliness_score(sessions)
        }
        
        # Weighted average
        weights = {'completeness': 0.3, 'consistency': 0.3, 'validity': 0.3, 'timeliness': 0.1}
        weighted_score = sum(score * weights[factor] for factor, score in quality_factors.items())
        
        return min(1.0, max(0.0, weighted_score))
    
    def _calculate_completeness_score(self, sessions: List[OnboardingSession]) -> float:
        """Calculate data completeness score"""
        total_sessions = len(sessions)
        complete_sessions = 0
        
        for session in sessions:
            # Check if session has all required fields
            required_fields = ['session_id', 'user_id', 'flow_id', 'started_at', 'device_type', 'steps_completed']
            missing_fields = sum(1 for field in required_fields if not getattr(session, field, None))
            
            if missing_fields == 0:
                complete_sessions += 1
        
        return complete_sessions / max(total_sessions, 1)
    
    def _calculate_consistency_score(self, sessions: List[OnboardingSession]) -> float:
        """Calculate data consistency score"""
        consistent_sessions = 0
        
        for session in sessions:
            consistency_checks = [
                # Check if steps_completed <= total_steps
                session.steps_completed <= session.total_steps,
                # Check if completed_at > started_at (when both exist)
                not (session.completed_at and session.completed_at <= session.started_at),
                # Check if last_activity >= started_at
                session.last_activity >= session.started_at,
                # Check status consistency
                (session.status == OnboardingStatus.COMPLETED and session.completed_at is not None) or 
                (session.status != OnboardingStatus.COMPLETED)
            ]
            
            if all(consistency_checks):
                consistent_sessions += 1
        
        return consistent_sessions / max(len(sessions), 1)
    
    def _calculate_validity_score(self, sessions: List[OnboardingSession]) -> float:
        """Calculate data validity score"""
        valid_sessions = 0
        
        for session in sessions:
            validity_checks = [
                # Check valid device types
                session.device_type in ['mobile', 'desktop', 'tablet'],
                # Check valid status
                session.status in [status for status in OnboardingStatus],
                # Check reasonable completion times (less than 24 hours)
                not session.completed_at or (session.completed_at - session.started_at).total_seconds() < 86400,
                # Check reasonable step counts
                0 <= session.steps_completed <= 50  # Assuming max 50 steps
            ]
            
            if all(validity_checks):
                valid_sessions += 1
        
        return valid_sessions / max(len(sessions), 1)
    
    def _calculate_timeliness_score(self, sessions: List[OnboardingSession]) -> float:
        """Calculate data timeliness score"""
        current_time = datetime.utcnow()
        timely_sessions = 0
        
        for session in sessions:
            # Consider data timely if it's within the last 30 days
            age_days = (current_time - session.started_at).days
            if age_days <= 30:
                timely_sessions += 1
        
        return timely_sessions / max(len(sessions), 1)
    
    def calculate_analysis_confidence(self, 
                                   total_sessions: int,
                                   data_quality_score: float,
                                   friction_detection_confidence: float) -> float:
        """Calculate overall analysis confidence score"""
        
        # Sample size factor
        sample_size_factor = min(1.0, total_sessions / 1000)  # Normalize to 1000 sessions
        
        # Combine factors
        confidence_factors = {
            'sample_size': sample_size_factor,
            'data_quality': data_quality_score,
            'friction_detection': friction_detection_confidence
        }
        
        # Weighted combination
        weights = {'sample_size': 0.4, 'data_quality': 0.3, 'friction_detection': 0.3}
        overall_confidence = sum(score * weights[factor] for factor, score in confidence_factors.items())
        
        return min(1.0, max(0.0, overall_confidence))
