"""
Point 14: Friction Pattern Detection Engine (Part 1/2)
ML-based friction pattern detection and analysis for onboarding flows.
"""

import os
import json
import time
import threading
import asyncio
from typing import Dict, List, Optional, Any, Union, Set, Tuple, NamedTuple
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, Counter
import logging
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
import sqlite3
from pathlib import Path
import statistics
import math

# Import from our onboarding models
from .onboarding_models import (
    OnboardingSession, OnboardingStepAttempt, OnboardingFlowDefinition,
    OnboardingStatus, OnboardingStepType, UserSegment, FeatureEngineer
)

class FrictionType(Enum):
    """Types of friction patterns"""
    HIGH_ABANDONMENT = "high_abandonment"
    SLOW_PROGRESSION = "slow_progression"
    REPEATED_FAILURES = "repeated_failures"
    STEP_CONFUSION = "step_confusion"
    FORM_ERRORS = "form_errors"
    NAVIGATION_ISSUES = "navigation_issues"
    PERFORMANCE_ISSUES = "performance_issues"
    CONTENT_CLARITY = "content_clarity"
    TECHNICAL_BARRIERS = "technical_barriers"
    COGNITIVE_OVERLOAD = "cognitive_overload"

class FrictionSeverity(Enum):
    """Friction severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class PatternConfidence(Enum):
    """Pattern detection confidence levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"

@dataclass
class FrictionPoint:
    """Individual friction point in onboarding flow"""
    friction_id: str
    step_id: str
    flow_id: str
    friction_type: FrictionType
    severity: FrictionSeverity
    confidence: PatternConfidence
    affected_users: int
    total_users: int
    abandonment_rate: float
    avg_time_spent: float
    retry_rate: float
    error_rate: float
    description: str
    detected_at: datetime = field(default_factory=datetime.utcnow)
    patterns: List[str] = field(default_factory=list)
    user_segments_affected: List[UserSegment] = field(default_factory=list)
    device_types_affected: List[str] = field(default_factory=list)
    behavioral_indicators: Dict[str, Any] = field(default_factory=dict)
    statistical_significance: float = 0.0
    recommendation_priority: int = 1  # 1=highest, 5=lowest
    estimated_impact: Dict[str, float] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class FrictionCluster:
    """Cluster of related friction patterns"""
    cluster_id: str
    cluster_name: str
    friction_points: List[FrictionPoint]
    dominant_friction_type: FrictionType
    cluster_severity: FrictionSeverity
    total_affected_users: int
    cluster_abandonment_rate: float
    cluster_description: str
    user_personas: List[Dict[str, Any]] = field(default_factory=list)
    common_patterns: List[str] = field(default_factory=list)
    intervention_opportunities: List[str] = field(default_factory=list)
    business_impact: Dict[str, float] = field(default_factory=dict)

@dataclass
class DropoffAnalysis:
    """Detailed drop-off analysis for a step"""
    step_id: str
    flow_id: str
    total_attempts: int
    successful_completions: int
    abandonment_count: int
    failure_count: int
    completion_rate: float
    abandonment_rate: float
    failure_rate: float
    avg_completion_time: float
    median_completion_time: float
    time_distribution_percentiles: Dict[str, float]
    retry_analysis: Dict[str, Any]
    error_analysis: Dict[str, Any]
    user_segment_breakdown: Dict[UserSegment, Dict[str, float]]
    device_breakdown: Dict[str, Dict[str, float]]
    temporal_patterns: Dict[str, Any]
    cohort_analysis: Dict[str, Any]

class FrictionDetectionConfig:
    """Configuration for friction detection"""
    
    def __init__(self, config: Dict[str, Any]):
        # Thresholds for pattern detection
        self.high_abandonment_threshold = config.get('high_abandonment_threshold', 0.3)
        self.slow_progression_threshold_minutes = config.get('slow_progression_threshold_minutes', 5.0)
        self.high_retry_threshold = config.get('high_retry_threshold', 0.2)
        self.high_error_threshold = config.get('high_error_threshold', 0.15)
        
        # Statistical significance
        self.min_sample_size = config.get('min_sample_size', 50)
        self.significance_level = config.get('significance_level', 0.05)
        
        # Pattern clustering
        self.max_clusters = config.get('max_clusters', 10)
        self.cluster_min_size = config.get('cluster_min_size', 5)
        
        # Time windows for analysis
        self.analysis_window_days = config.get('analysis_window_days', 30)
        self.cohort_window_days = config.get('cohort_window_days', 7)
        
        # Severity calculation weights
        self.severity_weights = config.get('severity_weights', {
            'abandonment_rate': 0.4,
            'affected_users': 0.3,
            'time_impact': 0.2,
            'error_rate': 0.1
        })

class DropoffAnalyzer:
    """Analyzes drop-off patterns in onboarding steps"""
    
    def __init__(self, config: FrictionDetectionConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def analyze_step_dropoffs(self, sessions: List[OnboardingSession], 
                            flow_def: OnboardingFlowDefinition) -> Dict[str, DropoffAnalysis]:
        """Analyze drop-offs for each step in the flow"""
        try:
            step_analyses = {}
            
            for step in flow_def.steps:
                analysis = self._analyze_single_step(step.step_id, sessions, flow_def)
                if analysis:
                    step_analyses[step.step_id] = analysis
            
            self.logger.info(f"Analyzed drop-offs for {len(step_analyses)} steps")
            return step_analyses
            
        except Exception as e:
            self.logger.error(f"Error analyzing step drop-offs: {e}")
            return {}
    
    def _analyze_single_step(self, step_id: str, sessions: List[OnboardingSession],
                           flow_def: OnboardingFlowDefinition) -> Optional[DropoffAnalysis]:
        """Analyze drop-offs for a single step"""
        try:
            # Get all attempts for this step
            attempts = []
            for session in sessions:
                if step_id in session.step_attempts:
                    attempts.extend(session.step_attempts[step_id])
            
            if len(attempts) < self.config.min_sample_size:
                self.logger.warning(f"Insufficient data for step {step_id}: {len(attempts)} attempts")
                return None
            
            # Calculate basic metrics
            total_attempts = len(attempts)
            successful_completions = len([a for a in attempts if a.status == "completed"])
            abandonment_count = len([a for a in attempts if a.status == "abandoned"])
            failure_count = len([a for a in attempts if a.status == "failed"])
            
            completion_rate = successful_completions / total_attempts
            abandonment_rate = abandonment_count / total_attempts
            failure_rate = failure_count / total_attempts
            
            # Calculate timing metrics
            completed_attempts = [a for a in attempts if a.status == "completed" and a.time_spent_seconds]
            
            if completed_attempts:
                completion_times = [a.time_spent_seconds for a in completed_attempts]
                avg_completion_time = statistics.mean(completion_times)
                median_completion_time = statistics.median(completion_times)
                
                # Calculate percentiles
                time_percentiles = {
                    'p25': np.percentile(completion_times, 25),
                    'p50': np.percentile(completion_times, 50),
                    'p75': np.percentile(completion_times, 75),
                    'p90': np.percentile(completion_times, 90),
                    'p95': np.percentile(completion_times, 95),
                    'p99': np.percentile(completion_times, 99)
                }
            else:
                avg_completion_time = 0.0
                median_completion_time = 0.0
                time_percentiles = {}
            
            # Analyze retries
            retry_analysis = self._analyze_retries(attempts)
            
            # Analyze errors
            error_analysis = self._analyze_errors(attempts)
            
            # Segment breakdown
            user_segment_breakdown = self._analyze_by_user_segment(attempts, sessions)
            
            # Device breakdown
            device_breakdown = self._analyze_by_device(attempts, sessions)
            
            # Temporal patterns
            temporal_patterns = self._analyze_temporal_patterns(attempts)
            
            # Cohort analysis
            cohort_analysis = self._analyze_cohorts(attempts, sessions)
            
            return DropoffAnalysis(
                step_id=step_id,
                flow_id=flow_def.flow_id,
                total_attempts=total_attempts,
                successful_completions=successful_completions,
                abandonment_count=abandonment_count,
                failure_count=failure_count,
                completion_rate=completion_rate,
                abandonment_rate=abandonment_rate,
                failure_rate=failure_rate,
                avg_completion_time=avg_completion_time,
                median_completion_time=median_completion_time,
                time_distribution_percentiles=time_percentiles,
                retry_analysis=retry_analysis,
                error_analysis=error_analysis,
                user_segment_breakdown=user_segment_breakdown,
                device_breakdown=device_breakdown,
                temporal_patterns=temporal_patterns,
                cohort_analysis=cohort_analysis
            )
            
        except Exception as e:
            self.logger.error(f"Error analyzing step {step_id}: {e}")
            return None
    
    def _analyze_retries(self, attempts: List[OnboardingStepAttempt]) -> Dict[str, Any]:
        """Analyze retry patterns"""
        retry_counts = [a.retry_count for a in attempts]
        
        total_retries = sum(retry_counts)
        attempts_with_retries = len([c for c in retry_counts if c > 0])
        retry_rate = attempts_with_retries / len(attempts) if attempts else 0
        
        return {
            'total_retries': total_retries,
            'attempts_with_retries': attempts_with_retries,
            'retry_rate': retry_rate,
            'avg_retries_per_attempt': statistics.mean(retry_counts) if retry_counts else 0,
            'max_retries': max(retry_counts) if retry_counts else 0,
            'retry_distribution': Counter(retry_counts)
        }
    
    def _analyze_errors(self, attempts: List[OnboardingStepAttempt]) -> Dict[str, Any]:
        """Analyze error patterns"""
        error_counts = [len(a.error_events) for a in attempts]
        
        total_errors = sum(error_counts)
        attempts_with_errors = len([c for c in error_counts if c > 0])
        error_rate = attempts_with_errors / len(attempts) if attempts else 0
        
        # Collect error details
        error_types = []
        for attempt in attempts:
            for event in attempt.events:
                if event.is_error_event and event.error_details:
                    error_types.append(event.error_details)
        
        error_type_distribution = Counter(error_types)
        
        return {
            'total_errors': total_errors,
            'attempts_with_errors': attempts_with_errors,
            'error_rate': error_rate,
            'avg_errors_per_attempt': statistics.mean(error_counts) if error_counts else 0,
            'error_type_distribution': dict(error_type_distribution.most_common(10))
        }
    
    def _analyze_by_user_segment(self, attempts: List[OnboardingStepAttempt], 
                               sessions: List[OnboardingSession]) -> Dict[UserSegment, Dict[str, float]]:
        """Analyze performance by user segment"""
        segment_data = defaultdict(lambda: {'total': 0, 'completed': 0, 'abandoned': 0, 'failed': 0})
        
        # Create session lookup
        session_lookup = {s.session_id: s for s in sessions}
        
        for attempt in attempts:
            session = session_lookup.get(attempt.session_id)
            if session:
                segment = session.user_segment
                segment_data[segment]['total'] += 1
                
                if attempt.status == 'completed':
                    segment_data[segment]['completed'] += 1
                elif attempt.status == 'abandoned':
                    segment_data[segment]['abandoned'] += 1
                elif attempt.status == 'failed':
                    segment_data[segment]['failed'] += 1
        
        # Calculate rates
        segment_breakdown = {}
        for segment, data in segment_data.items():
            if data['total'] > 0:
                segment_breakdown[segment] = {
                    'completion_rate': data['completed'] / data['total'],
                    'abandonment_rate': data['abandoned'] / data['total'],
                    'failure_rate': data['failed'] / data['total'],
                    'total_attempts': data['total']
                }
        
        return segment_breakdown
    
    def _analyze_by_device(self, attempts: List[OnboardingStepAttempt], 
                         sessions: List[OnboardingSession]) -> Dict[str, Dict[str, float]]:
        """Analyze performance by device type"""
        device_data = defaultdict(lambda: {'total': 0, 'completed': 0, 'abandoned': 0, 'failed': 0})
        
        # Create session lookup
        session_lookup = {s.session_id: s for s in sessions}
        
        for attempt in attempts:
            session = session_lookup.get(attempt.session_id)
            if session:
                device_type = session.device_info.get('device_type', 'unknown')
                device_data[device_type]['total'] += 1
                
                if attempt.status == 'completed':
                    device_data[device_type]['completed'] += 1
                elif attempt.status == 'abandoned':
                    device_data[device_type]['abandoned'] += 1
                elif attempt.status == 'failed':
                    device_data[device_type]['failed'] += 1
        
        # Calculate rates
        device_breakdown = {}
        for device_type, data in device_data.items():
            if data['total'] > 0:
                device_breakdown[device_type] = {
                    'completion_rate': data['completed'] / data['total'],
                    'abandonment_rate': data['abandoned'] / data['total'],
                    'failure_rate': data['failed'] / data['total'],
                    'total_attempts': data['total']
                }
        
        return device_breakdown
    
    def _analyze_temporal_patterns(self, attempts: List[OnboardingStepAttempt]) -> Dict[str, Any]:
        """Analyze temporal patterns in step attempts"""
        if not attempts:
            return {}
        
        # Group by hour of day
        hourly_data = defaultdict(lambda: {'total': 0, 'completed': 0})
        
        # Group by day of week
        daily_data = defaultdict(lambda: {'total': 0, 'completed': 0})
        
        for attempt in attempts:
            hour = attempt.started_at.hour
            day_of_week = attempt.started_at.weekday()  # 0=Monday, 6=Sunday
            
            hourly_data[hour]['total'] += 1
            daily_data[day_of_week]['total'] += 1
            
            if attempt.status == 'completed':
                hourly_data[hour]['completed'] += 1
                daily_data[day_of_week]['completed'] += 1
        
        # Calculate completion rates
        hourly_completion_rates = {}
        for hour, data in hourly_data.items():
            hourly_completion_rates[hour] = data['completed'] / data['total'] if data['total'] > 0 else 0
        
        daily_completion_rates = {}
        for day, data in daily_data.items():
            daily_completion_rates[day] = data['completed'] / data['total'] if data['total'] > 0 else 0
        
        return {
            'hourly_patterns': {
                'completion_rates': hourly_completion_rates,
                'attempt_distribution': {hour: data['total'] for hour, data in hourly_data.items()}
            },
            'daily_patterns': {
                'completion_rates': daily_completion_rates,
                'attempt_distribution': {day: data['total'] for day, data in daily_data.items()}
            }
        }
    
    def _analyze_cohorts(self, attempts: List[OnboardingStepAttempt], 
                       sessions: List[OnboardingSession]) -> Dict[str, Any]:
        """Analyze cohort-based patterns"""
        if not attempts:
            return {}
        
        # Create session lookup
        session_lookup = {s.session_id: s for s in sessions}
        
        # Group by weekly cohorts
        cohort_data = defaultdict(lambda: {'total': 0, 'completed': 0})
        
        for attempt in attempts:
            session = session_lookup.get(attempt.session_id)
            if session:
                # Get week start date
                week_start = session.started_at.date() - timedelta(days=session.started_at.weekday())
                
                cohort_data[week_start]['total'] += 1
                if attempt.status == 'completed':
                    cohort_data[week_start]['completed'] += 1
        
        # Calculate completion rates by cohort
        cohort_completion_rates = {}
        for week, data in cohort_data.items():
            cohort_completion_rates[week.isoformat()] = {
                'completion_rate': data['completed'] / data['total'] if data['total'] > 0 else 0,
                'total_attempts': data['total']
            }
        
        return {
            'weekly_cohorts': cohort_completion_rates
        }

class FrictionPatternDetector:
    """Detects friction patterns using statistical analysis and ML"""
    
    def __init__(self, config: FrictionDetectionConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.dropoff_analyzer = DropoffAnalyzer(config)
    
    def detect_friction_patterns(self, sessions: List[OnboardingSession], 
                               flow_def: OnboardingFlowDefinition) -> List[FrictionPoint]:
        """Detect friction patterns in onboarding flow"""
        try:
            # Analyze drop-offs for each step
            step_analyses = self.dropoff_analyzer.analyze_step_dropoffs(sessions, flow_def)
            
            # Detect friction patterns
            friction_points = []
            
            for step_id, analysis in step_analyses.items():
                step_friction_points = self._detect_step_friction_patterns(step_id, analysis, sessions)
                friction_points.extend(step_friction_points)
            
            # Calculate statistical significance
            for friction_point in friction_points:
                friction_point.statistical_significance = self._calculate_statistical_significance(
                    friction_point, step_analyses.get(friction_point.step_id)
                )
            
            # Sort by severity and confidence
            friction_points.sort(key=lambda fp: (fp.severity.value, fp.confidence.value), reverse=True)
            
            self.logger.info(f"Detected {len(friction_points)} friction patterns")
            return friction_points
            
        except Exception as e:
            self.logger.error(f"Error detecting friction patterns: {e}")
            return []
    
    def _detect_step_friction_patterns(self, step_id: str, analysis: DropoffAnalysis, 
                                     sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Detect friction patterns for a specific step"""
        friction_points = []
        
        # High abandonment pattern
        if analysis.abandonment_rate > self.config.high_abandonment_threshold:
            friction_point = self._create_abandonment_friction_point(step_id, analysis, sessions)
            if friction_point:
                friction_points.append(friction_point)
        
        # Slow progression pattern
        if analysis.avg_completion_time > self.config.slow_progression_threshold_minutes * 60:
            friction_point = self._create_slow_progression_friction_point(step_id, analysis, sessions)
            if friction_point:
                friction_points.append(friction_point)
        
        # High retry pattern
        retry_rate = analysis.retry_analysis.get('retry_rate', 0)
        if retry_rate > self.config.high_retry_threshold:
            friction_point = self._create_retry_friction_point(step_id, analysis, sessions)
            if friction_point:
                friction_points.append(friction_point)
        
        # High error pattern
        error_rate = analysis.error_analysis.get('error_rate', 0)
        if error_rate > self.config.high_error_threshold:
            friction_point = self._create_error_friction_point(step_id, analysis, sessions)
            if friction_point:
                friction_points.append(friction_point)
        
        # Form-specific patterns
        form_friction_points = self._detect_form_friction_patterns(step_id, analysis, sessions)
        friction_points.extend(form_friction_points)
        
        # Navigation patterns
        navigation_friction_points = self._detect_navigation_friction_patterns(step_id, analysis, sessions)
        friction_points.extend(navigation_friction_points)
        
        return friction_points
    
    def _create_abandonment_friction_point(self, step_id: str, analysis: DropoffAnalysis, 
                                         sessions: List[OnboardingSession]) -> Optional[FrictionPoint]:
        """Create friction point for high abandonment"""
        try:
            affected_users = analysis.abandonment_count
            total_users = analysis.total_attempts
            
            # Determine severity
            if analysis.abandonment_rate > 0.7:
                severity = FrictionSeverity.CRITICAL
            elif analysis.abandonment_rate > 0.5:
                severity = FrictionSeverity.HIGH
            elif analysis.abandonment_rate > 0.3:
                severity = FrictionSeverity.MEDIUM
            else:
                severity = FrictionSeverity.LOW
            
            # Determine confidence based on sample size and rate
            if total_users >= 200 and analysis.abandonment_rate > 0.5:
                confidence = PatternConfidence.VERY_HIGH
            elif total_users >= 100 and analysis.abandonment_rate > 0.4:
                confidence = PatternConfidence.HIGH
            elif total_users >= 50:
                confidence = PatternConfidence.MEDIUM
            else:
                confidence = PatternConfidence.LOW
            
            # Analyze affected segments
            affected_segments = []
            for segment, data in analysis.user_segment_breakdown.items():
                if data['abandonment_rate'] > self.config.high_abandonment_threshold:
                    affected_segments.append(segment)
            
            # Analyze affected devices
            affected_devices = []
            for device, data in analysis.device_breakdown.items():
                if data['abandonment_rate'] > self.config.high_abandonment_threshold:
                    affected_devices.append(device)
            
            # Behavioral indicators
            behavioral_indicators = {
                'avg_time_before_abandon': self._calculate_avg_abandon_time(step_id, sessions),
                'common_last_events': self._get_common_last_events(step_id, sessions),
                'temporal_patterns': analysis.temporal_patterns,
                'retry_before_abandon': analysis.retry_analysis['retry_rate']
            }
            
            friction_point = FrictionPoint(
                friction_id=f"abandon_{step_id}_{int(time.time())}",
                step_id=step_id,
                flow_id=analysis.flow_id,
                friction_type=FrictionType.HIGH_ABANDONMENT,
                severity=severity,
                confidence=confidence,
                affected_users=affected_users,
                total_users=total_users,
                abandonment_rate=analysis.abandonment_rate,
                avg_time_spent=analysis.avg_completion_time,
                retry_rate=analysis.retry_analysis.get('retry_rate', 0),
                error_rate=analysis.error_analysis.get('error_rate', 0),
                description=f"High abandonment rate ({analysis.abandonment_rate:.1%}) detected at step {step_id}",
                patterns=[
                    f"abandonment_rate:{analysis.abandonment_rate:.3f}",
                    f"affected_users:{affected_users}",
                    f"sample_size:{total_users}"
                ],
                user_segments_affected=affected_segments,
                device_types_affected=affected_devices,
                behavioral_indicators=behavioral_indicators,
                recommendation_priority=1 if severity in [FrictionSeverity.CRITICAL, FrictionSeverity.HIGH] else 2
            )
            
            return friction_point
            
        except Exception as e:
            self.logger.error(f"Error creating abandonment friction point: {e}")
            return None
    
    def _create_slow_progression_friction_point(self, step_id: str, analysis: DropoffAnalysis, 
                                              sessions: List[OnboardingSession]) -> Optional[FrictionPoint]:
        """Create friction point for slow progression"""
        try:
            avg_time_minutes = analysis.avg_completion_time / 60
            threshold_minutes = self.config.slow_progression_threshold_minutes
            
            # Determine severity based on how much slower than threshold
            time_ratio = avg_time_minutes / threshold_minutes
            if time_ratio > 4:
                severity = FrictionSeverity.CRITICAL
            elif time_ratio > 3:
                severity = FrictionSeverity.HIGH
            elif time_ratio > 2:
                severity = FrictionSeverity.MEDIUM
            else:
                severity = FrictionSeverity.LOW
            
            # Confidence based on sample size and consistency
            time_std = np.std([t for t in analysis.time_distribution_percentiles.values() if t > 0])
            if analysis.total_attempts >= 100 and time_std < avg_time_minutes * 0.5:
                confidence = PatternConfidence.HIGH
            elif analysis.total_attempts >= 50:
                confidence = PatternConfidence.MEDIUM
            else:
                confidence = PatternConfidence.LOW
            
            behavioral_indicators = {
                'avg_time_minutes': avg_time_minutes,
                'median_time_minutes': analysis.median_completion_time / 60,
                'time_percentiles': {k: v/60 for k, v in analysis.time_distribution_percentiles.items()},
                'time_variance': time_std / 60 if time_std else 0,
                'slow_completion_correlation': self._analyze_slow_completion_correlation(step_id, sessions)
            }
            
            friction_point = FrictionPoint(
                friction_id=f"slow_{step_id}_{int(time.time())}",
                step_id=step_id,
                flow_id=analysis.flow_id,
                friction_type=FrictionType.SLOW_PROGRESSION,
                severity=severity,
                confidence=confidence,
                affected_users=analysis.successful_completions,
                total_users=analysis.total_attempts,
                abandonment_rate=analysis.abandonment_rate,
                avg_time_spent=analysis.avg_completion_time,
                retry_rate=analysis.retry_analysis.get('retry_rate', 0),
                error_rate=analysis.error_analysis.get('error_rate', 0),
                description=f"Slow progression detected at step {step_id} (avg: {avg_time_minutes:.1f} min)",
                patterns=[
                    f"avg_time_minutes:{avg_time_minutes:.2f}",
                    f"threshold_ratio:{time_ratio:.2f}",
                    f"median_time_minutes:{analysis.median_completion_time/60:.2f}"
                ],
                behavioral_indicators=behavioral_indicators,
                recommendation_priority=2 if severity in [FrictionSeverity.CRITICAL, FrictionSeverity.HIGH] else 3
            )
            
            return friction_point
            
        except Exception as e:
            self.logger.error(f"Error creating slow progression friction point: {e}")
            return None
    
    def _create_retry_friction_point(self, step_id: str, analysis: DropoffAnalysis, 
                                   sessions: List[OnboardingSession]) -> Optional[FrictionPoint]:
        """Create friction point for high retry rate"""
        try:
            retry_rate = analysis.retry_analysis.get('retry_rate', 0)
            
            # Determine severity
            if retry_rate > 0.5:
                severity = FrictionSeverity.CRITICAL
            elif retry_rate > 0.3:
                severity = FrictionSeverity.HIGH
            elif retry_rate > 0.2:
                severity = FrictionSeverity.MEDIUM
            else:
                severity = FrictionSeverity.LOW
            
            confidence = PatternConfidence.HIGH if analysis.total_attempts >= 100 else PatternConfidence.MEDIUM
            
            # Analyze retry patterns
            retry_distribution = analysis.retry_analysis.get('retry_distribution', {})
            max_retries = analysis.retry_analysis.get('max_retries', 0)
            
            behavioral_indicators = {
                'retry_rate': retry_rate,
                'avg_retries_per_attempt': analysis.retry_analysis.get('avg_retries_per_attempt', 0),
                'max_retries_observed': max_retries,
                'retry_distribution': dict(retry_distribution),
                'retry_success_correlation': self._analyze_retry_success_correlation(step_id, sessions)
            }
            
            friction_point = FrictionPoint(
                friction_id=f"retry_{step_id}_{int(time.time())}",
                step_id=step_id,
                flow_id=analysis.flow_id,
                friction_type=FrictionType.REPEATED_FAILURES,
                severity=severity,
                confidence=confidence,
                affected_users=analysis.retry_analysis.get('attempts_with_retries', 0),
                total_users=analysis.total_attempts,
                abandonment_rate=analysis.abandonment_rate,
                avg_time_spent=analysis.avg_completion_time,
                retry_rate=retry_rate,
                error_rate=analysis.error_analysis.get('error_rate', 0),
                description=f"High retry rate ({retry_rate:.1%}) detected at step {step_id}",
                patterns=[
                    f"retry_rate:{retry_rate:.3f}",
                    f"max_retries:{max_retries}",
                    f"retry_correlation:failure"
                ],
                behavioral_indicators=behavioral_indicators,
                recommendation_priority=1 if severity in [FrictionSeverity.CRITICAL, FrictionSeverity.HIGH] else 2
            )
            
            return friction_point
            
        except Exception as e:
            self.logger.error(f"Error creating retry friction point: {e}")
            return None
    
    def _create_error_friction_point(self, step_id: str, analysis: DropoffAnalysis, 
                                   sessions: List[OnboardingSession]) -> Optional[FrictionPoint]:
        """Create friction point for high error rate"""
        try:
            error_rate = analysis.error_analysis.get('error_rate', 0)
            
            # Determine severity
            if error_rate > 0.3:
                severity = FrictionSeverity.CRITICAL
            elif error_rate > 0.2:
                severity = FrictionSeverity.HIGH
            elif error_rate > 0.15:
                severity = FrictionSeverity.MEDIUM
            else:
                severity = FrictionSeverity.LOW
            
            confidence = PatternConfidence.HIGH if analysis.total_attempts >= 50 else PatternConfidence.MEDIUM
            
            # Analyze error types
            error_types = analysis.error_analysis.get('error_type_distribution', {})
            dominant_error = max(error_types.items(), key=lambda x: x[1])[0] if error_types else "unknown"
            
            behavioral_indicators = {
                'error_rate': error_rate,
                'total_errors': analysis.error_analysis.get('total_errors', 0),
                'avg_errors_per_attempt': analysis.error_analysis.get('avg_errors_per_attempt', 0),
                'error_type_distribution': error_types,
                'dominant_error_type': dominant_error
            }
            
            friction_point = FrictionPoint(
                friction_id=f"error_{step_id}_{int(time.time())}",
                step_id=step_id,
                flow_id=analysis.flow_id,
                friction_type=FrictionType.FORM_ERRORS if 'form' in dominant_error.lower() else FrictionType.TECHNICAL_BARRIERS,
                severity=severity,
                confidence=confidence,
                affected_users=analysis.error_analysis.get('attempts_with_errors', 0),
                total_users=analysis.total_attempts,
                abandonment_rate=analysis.abandonment_rate,
                avg_time_spent=analysis.avg_completion_time,
                retry_rate=analysis.retry_analysis.get('retry_rate', 0),
                error_rate=error_rate,
                description=f"High error rate ({error_rate:.1%}) detected at step {step_id}",
                patterns=[
                    f"error_rate:{error_rate:.3f}",
                    f"dominant_error:{dominant_error}",
                    f"error_variety:{len(error_types)}"
                ],
                behavioral_indicators=behavioral_indicators,
                recommendation_priority=1
            )
            
            return friction_point
            
        except Exception as e:
            self.logger.error(f"Error creating error friction point: {e}")
            return None
    
    def _detect_form_friction_patterns(self, step_id: str, analysis: DropoffAnalysis, 
                                     sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Detect form-specific friction patterns"""
        # This would analyze form-specific events and errors
        # For now, return empty list - would be expanded in part 2
        return []
    
    def _detect_navigation_friction_patterns(self, step_id: str, analysis: DropoffAnalysis, 
                                           sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Detect navigation-specific friction patterns"""
        # This would analyze navigation patterns and confusion
        # For now, return empty list - would be expanded in part 2
        return []
    
    def _calculate_avg_abandon_time(self, step_id: str, sessions: List[OnboardingSession]) -> float:
        """Calculate average time before abandonment"""
        abandon_times = []
        
        for session in sessions:
            if session.status == OnboardingStatus.ABANDONED and step_id in session.step_attempts:
                attempts = session.step_attempts[step_id]
                for attempt in attempts:
                    if attempt.time_spent_seconds:
                        abandon_times.append(attempt.time_spent_seconds)
        
        return statistics.mean(abandon_times) if abandon_times else 0.0
    
    def _get_common_last_events(self, step_id: str, sessions: List[OnboardingSession]) -> List[str]:
        """Get common last events before abandonment"""
        last_events = []
        
        for session in sessions:
            if session.status == OnboardingStatus.ABANDONED and step_id in session.step_attempts:
                attempts = session.step_attempts[step_id]
                for attempt in attempts:
                    if attempt.events:
                        last_event = attempt.events[-1]
                        last_events.append(last_event.event_name)
        
        # Return top 5 most common
        event_counts = Counter(last_events)
        return [event for event, count in event_counts.most_common(5)]
    
    def _analyze_slow_completion_correlation(self, step_id: str, sessions: List[OnboardingSession]) -> Dict[str, Any]:
        """Analyze what correlates with slow completion"""
        # Simplified analysis - would be expanded in part 2
        return {
            'device_correlation': {},
            'segment_correlation': {},
            'time_of_day_correlation': {}
        }
    
    def _analyze_retry_success_correlation(self, step_id: str, sessions: List[OnboardingSession]) -> Dict[str, Any]:
        """Analyze correlation between retries and eventual success"""
        # Simplified analysis - would be expanded in part 2
        return {
            'retry_success_rate': 0.0,
            'optimal_retry_count': 1
        }
    
    def _calculate_statistical_significance(self, friction_point: FrictionPoint, 
                                          analysis: Optional[DropoffAnalysis]) -> float:
        """Calculate statistical significance of detected pattern"""
        if not analysis or friction_point.total_users < self.config.min_sample_size:
            return 0.0
        
        # Simplified significance calculation
        # In production, would use proper statistical tests (chi-square, t-test, etc.)
        
        sample_size_factor = min(1.0, friction_point.total_users / (self.config.min_sample_size * 2))
        
        if friction_point.friction_type == FrictionType.HIGH_ABANDONMENT:
            effect_size = abs(friction_point.abandonment_rate - 0.1)  # Compare to 10% baseline
        elif friction_point.friction_type == FrictionType.REPEATED_FAILURES:
            effect_size = abs(friction_point.retry_rate - 0.05)  # Compare to 5% baseline
        else:
            effect_size = 0.5  # Default effect size
        
        # Combine sample size and effect size
        significance = sample_size_factor * effect_size
        
        return min(1.0, significance)

class FrictionClustering:
    """Advanced ML clustering for friction patterns"""
    
    def __init__(self, config: FrictionDetectionConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def cluster_friction_patterns(self, friction_points: List[FrictionPoint]) -> List[FrictionCluster]:
        """Cluster related friction patterns using ML"""
        try:
            if len(friction_points) < self.config.cluster_min_size:
                return []
            
            # Extract features for clustering
            features = self._extract_clustering_features(friction_points)
            
            # Perform clustering
            cluster_labels = self._perform_clustering(features)
            
            # Create friction clusters
            clusters = self._create_friction_clusters(friction_points, cluster_labels)
            
            self.logger.info(f"Created {len(clusters)} friction clusters")
            return clusters
            
        except Exception as e:
            self.logger.error(f"Error clustering friction patterns: {e}")
            return []
    
    def _extract_clustering_features(self, friction_points: List[FrictionPoint]) -> np.ndarray:
        """Extract features for clustering friction patterns"""
        features = []
        
        for fp in friction_points:
            feature_vector = [
                # Pattern characteristics
                fp.abandonment_rate,
                fp.avg_time_spent / 300,  # Normalize to 5-minute units
                fp.retry_rate,
                fp.error_rate,
                fp.affected_users / max(fp.total_users, 1),
                
                # Friction type encoding
                self._encode_friction_type(fp.friction_type),
                
                # Severity encoding
                self._encode_severity(fp.severity),
                
                # Confidence encoding
                self._encode_confidence(fp.confidence),
                
                # Statistical significance
                fp.statistical_significance,
                
                # Behavioral indicators
                len(fp.user_segments_affected),
                len(fp.device_types_affected),
                len(fp.patterns)
            ]
            
            features.append(feature_vector)
        
        return np.array(features)
    
    def _encode_friction_type(self, friction_type: FrictionType) -> float:
        """Encode friction type as numeric value"""
        encoding = {
            FrictionType.HIGH_ABANDONMENT: 0.1,
            FrictionType.SLOW_PROGRESSION: 0.2,
            FrictionType.REPEATED_FAILURES: 0.3,
            FrictionType.STEP_CONFUSION: 0.4,
            FrictionType.FORM_ERRORS: 0.5,
            FrictionType.NAVIGATION_ISSUES: 0.6,
            FrictionType.PERFORMANCE_ISSUES: 0.7,
            FrictionType.CONTENT_CLARITY: 0.8,
            FrictionType.TECHNICAL_BARRIERS: 0.9,
            FrictionType.COGNITIVE_OVERLOAD: 1.0
        }
        return encoding.get(friction_type, 0.5)
    
    def _encode_severity(self, severity: FrictionSeverity) -> float:
        """Encode severity as numeric value"""
        encoding = {
            FrictionSeverity.LOW: 0.25,
            FrictionSeverity.MEDIUM: 0.5,
            FrictionSeverity.HIGH: 0.75,
            FrictionSeverity.CRITICAL: 1.0
        }
        return encoding.get(severity, 0.5)
    
    def _encode_confidence(self, confidence: PatternConfidence) -> float:
        """Encode confidence as numeric value"""
        encoding = {
            PatternConfidence.LOW: 0.25,
            PatternConfidence.MEDIUM: 0.5,
            PatternConfidence.HIGH: 0.75,
            PatternConfidence.VERY_HIGH: 1.0
        }
        return encoding.get(confidence, 0.5)
    
    def _perform_clustering(self, features: np.ndarray) -> np.ndarray:
        """Perform K-means clustering"""
        try:
            from sklearn.cluster import KMeans
            from sklearn.preprocessing import StandardScaler
            
            # Standardize features
            scaler = StandardScaler()
            scaled_features = scaler.fit_transform(features)
            
            # Determine optimal number of clusters
            n_clusters = min(self.config.max_clusters, len(features) // self.config.cluster_min_size)
            n_clusters = max(2, n_clusters)
            
            # Perform clustering
            kmeans = KMeans(n_clusters=n_clusters, random_state=42)
            cluster_labels = kmeans.fit_predict(scaled_features)
            
            return cluster_labels
            
        except ImportError:
            # Fallback to simple clustering if sklearn not available
            return self._simple_clustering(features)
        except Exception as e:
            self.logger.error(f"Error in ML clustering: {e}")
            return self._simple_clustering(features)
    
    def _simple_clustering(self, features: np.ndarray) -> np.ndarray:
        """Simple clustering fallback without sklearn"""
        n_points = len(features)
        n_clusters = min(self.config.max_clusters, n_points // self.config.cluster_min_size)
        n_clusters = max(2, n_clusters)
        
        # Simple clustering based on abandonment rate and severity
        cluster_labels = np.zeros(n_points, dtype=int)
        
        for i, feature_vector in enumerate(features):
            abandonment_rate = feature_vector[0]
            severity_encoded = feature_vector[6]
            
            # Simple rule-based clustering
            if abandonment_rate > 0.7:
                cluster_labels[i] = 0  # Critical abandonment cluster
            elif abandonment_rate > 0.4:
                cluster_labels[i] = 1  # High abandonment cluster
            elif severity_encoded > 0.75:
                cluster_labels[i] = 2  # High severity cluster
            else:
                cluster_labels[i] = 3  # Lower priority cluster
        
        return cluster_labels
    
    def _create_friction_clusters(self, friction_points: List[FrictionPoint], 
                                cluster_labels: np.ndarray) -> List[FrictionCluster]:
        """Create friction clusters from clustering results"""
        clusters_dict = defaultdict(list)
        
        # Group friction points by cluster
        for fp, label in zip(friction_points, cluster_labels):
            clusters_dict[label].append(fp)
        
        clusters = []
        for cluster_id, fps in clusters_dict.items():
            if len(fps) >= self.config.cluster_min_size:
                cluster = self._create_single_cluster(cluster_id, fps)
                if cluster:
                    clusters.append(cluster)
        
        return clusters
    
    def _create_single_cluster(self, cluster_id: int, friction_points: List[FrictionPoint]) -> Optional[FrictionCluster]:
        """Create a single friction cluster"""
        try:
            # Determine dominant friction type
            friction_types = [fp.friction_type for fp in friction_points]
            dominant_type = Counter(friction_types).most_common(1)[0][0]
            
            # Determine cluster severity (highest severity in cluster)
            severities = [fp.severity for fp in friction_points]
            cluster_severity = max(severities, key=lambda s: self._encode_severity(s))
            
            # Calculate cluster metrics
            total_affected = sum(fp.affected_users for fp in friction_points)
            total_users = sum(fp.total_users for fp in friction_points)
            cluster_abandonment_rate = sum(fp.abandonment_rate * fp.total_users for fp in friction_points) / total_users if total_users > 0 else 0
            
            # Extract common patterns
            all_patterns = []
            for fp in friction_points:
                all_patterns.extend(fp.patterns)
            common_patterns = [pattern for pattern, count in Counter(all_patterns).most_common(5)]
            
            # Generate cluster description
            step_ids = list(set(fp.step_id for fp in friction_points))
            cluster_description = f"Cluster of {len(friction_points)} friction points affecting {len(step_ids)} steps"
            
            # Analyze user personas
            user_personas = self._analyze_cluster_personas(friction_points)
            
            # Identify intervention opportunities
            intervention_opportunities = self._identify_intervention_opportunities(friction_points)
            
            # Calculate business impact
            business_impact = self._calculate_cluster_business_impact(friction_points)
            
            cluster = FrictionCluster(
                cluster_id=f"cluster_{cluster_id}",
                cluster_name=f"{dominant_type.value.title()} Issues",
                friction_points=friction_points,
                dominant_friction_type=dominant_type,
                cluster_severity=cluster_severity,
                total_affected_users=total_affected,
                cluster_abandonment_rate=cluster_abandonment_rate,
                cluster_description=cluster_description,
                user_personas=user_personas,
                common_patterns=common_patterns,
                intervention_opportunities=intervention_opportunities,
                business_impact=business_impact
            )
            
            return cluster
            
        except Exception as e:
            self.logger.error(f"Error creating cluster {cluster_id}: {e}")
            return None
    
    def _analyze_cluster_personas(self, friction_points: List[FrictionPoint]) -> List[Dict[str, Any]]:
        """Analyze user personas affected by cluster"""
        personas = []
        
        # Aggregate user segments
        segment_data = defaultdict(int)
        device_data = defaultdict(int)
        
        for fp in friction_points:
            for segment in fp.user_segments_affected:
                segment_data[segment] += fp.affected_users
            for device in fp.device_types_affected:
                device_data[device] += fp.affected_users
        
        # Create persona for top segments
        for segment, affected_count in Counter(segment_data).most_common(3):
            persona = {
                'segment': segment.value,
                'affected_users': affected_count,
                'primary_devices': [device for device, count in Counter(device_data).most_common(2)],
                'friction_types': list(set(fp.friction_type.value for fp in friction_points)),
                'avg_severity': statistics.mean([self._encode_severity(fp.severity) for fp in friction_points])
            }
            personas.append(persona)
        
        return personas
    
    def _identify_intervention_opportunities(self, friction_points: List[FrictionPoint]) -> List[str]:
        """Identify intervention opportunities for cluster"""
        opportunities = []
        
        # Analyze friction types for interventions
        friction_types = Counter([fp.friction_type for fp in friction_points])
        
        for friction_type, count in friction_types.most_common():
            if friction_type == FrictionType.HIGH_ABANDONMENT:
                opportunities.append("Implement exit-intent popups with assistance offers")
                opportunities.append("Add progress indicators to show remaining steps")
            elif friction_type == FrictionType.SLOW_PROGRESSION:
                opportunities.append("Simplify step content and reduce cognitive load")
                opportunities.append("Add contextual help and tooltips")
            elif friction_type == FrictionType.REPEATED_FAILURES:
                opportunities.append("Improve error messaging and recovery flows")
                opportunities.append("Add step-by-step guidance and examples")
            elif friction_type == FrictionType.FORM_ERRORS:
                opportunities.append("Implement real-time form validation")
                opportunities.append("Improve field labels and help text")
            elif friction_type == FrictionType.TECHNICAL_BARRIERS:
                opportunities.append("Optimize performance and loading times")
                opportunities.append("Improve cross-browser compatibility")
        
        return opportunities[:5]  # Top 5 opportunities
    
    def _calculate_cluster_business_impact(self, friction_points: List[FrictionPoint]) -> Dict[str, float]:
        """Calculate business impact of cluster"""
        total_affected = sum(fp.affected_users for fp in friction_points)
        total_users = sum(fp.total_users for fp in friction_points)
        avg_abandonment_rate = sum(fp.abandonment_rate * fp.total_users for fp in friction_points) / total_users if total_users > 0 else 0
        
        # Estimate potential improvements (conservative estimates)
        potential_improvement = min(0.5, avg_abandonment_rate * 0.3)  # 30% reduction in abandonment
        
        return {
            'affected_users': total_affected,
            'current_abandonment_rate': avg_abandonment_rate,
            'potential_improvement_rate': potential_improvement,
            'estimated_recovered_users': total_affected * potential_improvement,
            'priority_score': total_affected * avg_abandonment_rate  # Higher = more important
        }

class AdvancedFrictionAnalyzer:
    """Advanced friction analysis with ML and behavioral insights"""
    
    def __init__(self, config: FrictionDetectionConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.clustering = FrictionClustering(config)
    
    def analyze_form_friction(self, step_id: str, sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Analyze form-specific friction patterns"""
        try:
            friction_points = []
            
            # Get form events for this step
            form_events = self._extract_form_events(step_id, sessions)
            
            if not form_events:
                return friction_points
            
            # Analyze field-specific issues
            field_friction = self._analyze_field_friction(step_id, form_events, sessions)
            friction_points.extend(field_friction)
            
            # Analyze validation errors
            validation_friction = self._analyze_validation_friction(step_id, form_events, sessions)
            friction_points.extend(validation_friction)
            
            # Analyze form abandonment patterns
            abandonment_friction = self._analyze_form_abandonment(step_id, form_events, sessions)
            friction_points.extend(abandonment_friction)
            
            return friction_points
            
        except Exception as e:
            self.logger.error(f"Error analyzing form friction for step {step_id}: {e}")
            return []
    
    def analyze_navigation_friction(self, step_id: str, sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Analyze navigation-specific friction patterns"""
        try:
            friction_points = []
            
            # Get navigation events
            nav_events = self._extract_navigation_events(step_id, sessions)
            
            if not nav_events:
                return friction_points
            
            # Analyze back/forward patterns
            navigation_confusion = self._analyze_navigation_confusion(step_id, nav_events, sessions)
            friction_points.extend(navigation_confusion)
            
            # Analyze page switching patterns
            page_switching = self._analyze_excessive_page_switching(step_id, nav_events, sessions)
            friction_points.extend(page_switching)
            
            return friction_points
            
        except Exception as e:
            self.logger.error(f"Error analyzing navigation friction for step {step_id}: {e}")
            return []
    
    def analyze_performance_friction(self, step_id: str, sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Analyze performance-related friction"""
        try:
            friction_points = []
            
            # Analyze loading times
            loading_friction = self._analyze_loading_performance(step_id, sessions)
            friction_points.extend(loading_friction)
            
            # Analyze interaction delays
            interaction_friction = self._analyze_interaction_delays(step_id, sessions)
            friction_points.extend(interaction_friction)
            
            return friction_points
            
        except Exception as e:
            self.logger.error(f"Error analyzing performance friction for step {step_id}: {e}")
            return []
    
    def analyze_cognitive_load(self, step_id: str, sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Analyze cognitive load and content clarity issues"""
        try:
            friction_points = []
            
            # Analyze time spent vs. content complexity
            cognitive_overload = self._analyze_cognitive_overload(step_id, sessions)
            friction_points.extend(cognitive_overload)
            
            # Analyze help-seeking behavior
            help_seeking = self._analyze_help_seeking_patterns(step_id, sessions)
            friction_points.extend(help_seeking)
            
            return friction_points
            
        except Exception as e:
            self.logger.error(f"Error analyzing cognitive load for step {step_id}: {e}")
            return []
    
    def _extract_form_events(self, step_id: str, sessions: List[OnboardingSession]) -> List[Dict[str, Any]]:
        """Extract form-related events"""
        form_events = []
        
        form_event_types = ['input', 'focus', 'blur', 'submit', 'validation_error', 'field_error']
        
        for session in sessions:
            if step_id in session.step_attempts:
                for attempt in session.step_attempts[step_id]:
                    for event in attempt.events:
                        if any(event_type in event.event_name.lower() for event_type in form_event_types):
                            form_events.append({
                                'event': event,
                                'session_id': session.session_id,
                                'user_id': session.user_id,
                                'attempt': attempt
                            })
        
        return form_events
    
    def _analyze_field_friction(self, step_id: str, form_events: List[Dict[str, Any]], 
                              sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Analyze friction at the field level"""
        friction_points = []
        
        # Group events by field
        field_events = defaultdict(list)
        for event_data in form_events:
            field_name = event_data['event'].properties.get('field_name')
            if field_name:
                field_events[field_name].append(event_data)
        
        # Analyze each field
        for field_name, events in field_events.items():
            if len(events) < 10:  # Minimum events for analysis
                continue
            
            # Calculate field-specific metrics
            focus_events = [e for e in events if 'focus' in e['event'].event_name.lower()]
            blur_events = [e for e in events if 'blur' in e['event'].event_name.lower()]
            error_events = [e for e in events if 'error' in e['event'].event_name.lower()]
            
            if focus_events:
                # Calculate average time spent on field
                field_times = []
                for focus_event in focus_events:
                    matching_blur = None
                    for blur_event in blur_events:
                        if (blur_event['session_id'] == focus_event['session_id'] and 
                            blur_event['event'].timestamp > focus_event['event'].timestamp):
                            matching_blur = blur_event
                            break
                    
                    if matching_blur:
                        time_spent = (matching_blur['event'].timestamp - focus_event['event'].timestamp).total_seconds()
                        field_times.append(time_spent)
                
                if field_times:
                    avg_time = statistics.mean(field_times)
                    error_rate = len(error_events) / len(focus_events)
                    
                    # Detect high time spent (potential confusion)
                    if avg_time > 30:  # More than 30 seconds on a field
                        friction_point = FrictionPoint(
                            friction_id=f"field_{field_name}_{step_id}_{int(time.time())}",
                            step_id=step_id,
                            flow_id=sessions[0].flow_id if sessions else 'unknown',
                            friction_type=FrictionType.CONTENT_CLARITY if avg_time > 60 else FrictionType.FORM_ERRORS,
                            severity=FrictionSeverity.HIGH if avg_time > 60 else FrictionSeverity.MEDIUM,
                            confidence=PatternConfidence.HIGH if len(field_times) > 50 else PatternConfidence.MEDIUM,
                            affected_users=len(set(e['user_id'] for e in focus_events)),
                            total_users=len(set(s.user_id for s in sessions)),
                            abandonment_rate=0.0,  # Field-specific, calculate separately
                            avg_time_spent=avg_time,
                            retry_rate=0.0,
                            error_rate=error_rate,
                            description=f"High time spent on field '{field_name}' (avg: {avg_time:.1f}s)",
                            patterns=[
                                f"field_name:{field_name}",
                                f"avg_time_seconds:{avg_time:.1f}",
                                f"error_rate:{error_rate:.3f}"
                            ],
                            behavioral_indicators={
                                'field_name': field_name,
                                'avg_time_seconds': avg_time,
                                'median_time_seconds': statistics.median(field_times),
                                'error_rate': error_rate,
                                'focus_count': len(focus_events),
                                'total_interactions': len(events)
                            }
                        )
                        friction_points.append(friction_point)
        
        return friction_points
    
    def _analyze_validation_friction(self, step_id: str, form_events: List[Dict[str, Any]], 
                                   sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Analyze validation error patterns"""
        friction_points = []
        
        # Extract validation errors
        validation_errors = [e for e in form_events if 'validation' in e['event'].event_name.lower() or 'error' in e['event'].event_name.lower()]
        
        if not validation_errors:
            return friction_points
        
        # Group by error type
        error_types = defaultdict(list)
        for error_event in validation_errors:
            error_type = error_event['event'].properties.get('error_type', 'unknown_error')
            error_types[error_type].append(error_event)
        
        # Analyze each error type
        for error_type, errors in error_types.items():
            if len(errors) < 5:  # Minimum errors for analysis
                continue
            
            affected_users = len(set(e['user_id'] for e in errors))
            total_users = len(set(s.user_id for s in sessions))
            error_rate = len(errors) / len(form_events) if form_events else 0
            
            if error_rate > 0.1:  # More than 10% error rate
                friction_point = FrictionPoint(
                    friction_id=f"validation_{error_type}_{step_id}_{int(time.time())}",
                    step_id=step_id,
                    flow_id=sessions[0].flow_id if sessions else 'unknown',
                    friction_type=FrictionType.FORM_ERRORS,
                    severity=FrictionSeverity.HIGH if error_rate > 0.2 else FrictionSeverity.MEDIUM,
                    confidence=PatternConfidence.HIGH if len(errors) > 20 else PatternConfidence.MEDIUM,
                    affected_users=affected_users,
                    total_users=total_users,
                    abandonment_rate=0.0,
                    avg_time_spent=0.0,
                    retry_rate=0.0,
                    error_rate=error_rate,
                    description=f"High validation error rate for {error_type} ({error_rate:.1%})",
                    patterns=[
                        f"error_type:{error_type}",
                        f"error_rate:{error_rate:.3f}",
                        f"affected_users:{affected_users}"
                    ],
                    behavioral_indicators={
                        'error_type': error_type,
                        'error_count': len(errors),
                        'error_rate': error_rate,
                        'affected_users': affected_users,
                        'avg_errors_per_user': len(errors) / affected_users if affected_users > 0 else 0
                    }
                )
                friction_points.append(friction_point)
        
        return friction_points
    
    def _analyze_form_abandonment(self, step_id: str, form_events: List[Dict[str, Any]], 
                                sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Analyze form abandonment patterns"""
        friction_points = []
        
        # Find sessions that started form but didn't complete
        form_starters = set()
        form_completers = set()
        
        for event_data in form_events:
            session_id = event_data['session_id']
            event_name = event_data['event'].event_name.lower()
            
            if 'focus' in event_name or 'input' in event_name:
                form_starters.add(session_id)
            
            if 'submit' in event_name and event_data['event'].is_success_event:
                form_completers.add(session_id)
        
        form_abandoners = form_starters - form_completers
        
        if len(form_starters) > 0:
            abandonment_rate = len(form_abandoners) / len(form_starters)
            
            if abandonment_rate > 0.3:  # More than 30% form abandonment
                friction_point = FrictionPoint(
                    friction_id=f"form_abandon_{step_id}_{int(time.time())}",
                    step_id=step_id,
                    flow_id=sessions[0].flow_id if sessions else 'unknown',
                    friction_type=FrictionType.HIGH_ABANDONMENT,
                    severity=FrictionSeverity.HIGH if abandonment_rate > 0.5 else FrictionSeverity.MEDIUM,
                    confidence=PatternConfidence.HIGH if len(form_starters) > 50 else PatternConfidence.MEDIUM,
                    affected_users=len(form_abandoners),
                    total_users=len(form_starters),
                    abandonment_rate=abandonment_rate,
                    avg_time_spent=0.0,
                    retry_rate=0.0,
                    error_rate=0.0,
                    description=f"High form abandonment rate ({abandonment_rate:.1%})",
                    patterns=[
                        f"form_abandonment_rate:{abandonment_rate:.3f}",
                        f"form_starters:{len(form_starters)}",
                        f"form_abandoners:{len(form_abandoners)}"
                    ],
                    behavioral_indicators={
                        'form_starters': len(form_starters),
                        'form_completers': len(form_completers),
                        'form_abandoners': len(form_abandoners),
                        'abandonment_rate': abandonment_rate
                    }
                )
                friction_points.append(friction_point)
        
        return friction_points
    
    def _extract_navigation_events(self, step_id: str, sessions: List[OnboardingSession]) -> List[Dict[str, Any]]:
        """Extract navigation-related events"""
        nav_events = []
        
        nav_event_types = ['page_view', 'click', 'navigation', 'back', 'forward', 'url_change']
        
        for session in sessions:
            if step_id in session.step_attempts:
                for attempt in session.step_attempts[step_id]:
                    for event in attempt.events:
                        if any(event_type in event.event_name.lower() for event_type in nav_event_types):
                            nav_events.append({
                                'event': event,
                                'session_id': session.session_id,
                                'user_id': session.user_id,
                                'attempt': attempt
                            })
        
        return nav_events
    
    def _analyze_navigation_confusion(self, step_id: str, nav_events: List[Dict[str, Any]], 
                                    sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Analyze navigation confusion patterns"""
        friction_points = []
        
        # Group by session to analyze navigation patterns
        session_nav_patterns = defaultdict(list)
        for event_data in nav_events:
            session_nav_patterns[event_data['session_id']].append(event_data)
        
        # Analyze back/forward patterns
        confused_sessions = 0
        total_sessions = len(session_nav_patterns)
        
        for session_id, events in session_nav_patterns.items():
            # Sort events by timestamp
            events.sort(key=lambda x: x['event'].timestamp)
            
            # Count back/forward events
            back_forward_count = 0
            for event_data in events:
                event_name = event_data['event'].event_name.lower()
                if 'back' in event_name or 'forward' in event_name:
                    back_forward_count += 1
            
            # Consider confused if more than 3 back/forward actions
            if back_forward_count > 3:
                confused_sessions += 1
        
        if total_sessions > 0:
            confusion_rate = confused_sessions / total_sessions
            
            if confusion_rate > 0.2:  # More than 20% showing navigation confusion
                friction_point = FrictionPoint(
                    friction_id=f"nav_confusion_{step_id}_{int(time.time())}",
                    step_id=step_id,
                    flow_id=sessions[0].flow_id if sessions else 'unknown',
                    friction_type=FrictionType.NAVIGATION_ISSUES,
                    severity=FrictionSeverity.HIGH if confusion_rate > 0.4 else FrictionSeverity.MEDIUM,
                    confidence=PatternConfidence.HIGH if total_sessions > 50 else PatternConfidence.MEDIUM,
                    affected_users=confused_sessions,
                    total_users=total_sessions,
                    abandonment_rate=0.0,
                    avg_time_spent=0.0,
                    retry_rate=0.0,
                    error_rate=0.0,
                    description=f"Navigation confusion detected ({confusion_rate:.1%} of users)",
                    patterns=[
                        f"navigation_confusion_rate:{confusion_rate:.3f}",
                        f"confused_sessions:{confused_sessions}",
                        f"total_sessions:{total_sessions}"
                    ],
                    behavioral_indicators={
                        'confusion_rate': confusion_rate,
                        'confused_sessions': confused_sessions,
                        'total_navigation_events': len(nav_events),
                        'avg_nav_events_per_session': len(nav_events) / total_sessions if total_sessions > 0 else 0
                    }
                )
                friction_points.append(friction_point)
        
        return friction_points
    
    def _analyze_excessive_page_switching(self, step_id: str, nav_events: List[Dict[str, Any]], 
                                        sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Analyze excessive page switching patterns"""
        friction_points = []
        
        # Group by session
        session_page_switches = defaultdict(int)
        
        for event_data in nav_events:
            event_name = event_data['event'].event_name.lower()
            if 'page_view' in event_name or 'url_change' in event_name:
                session_page_switches[event_data['session_id']] += 1
        
        if session_page_switches:
            avg_page_switches = statistics.mean(session_page_switches.values())
            excessive_switching_sessions = len([s for s in session_page_switches.values() if s > avg_page_switches * 2])
            
            if excessive_switching_sessions > 0:
                excessive_rate = excessive_switching_sessions / len(session_page_switches)
                
                if excessive_rate > 0.15:  # More than 15% showing excessive switching
                    friction_point = FrictionPoint(
                        friction_id=f"page_switching_{step_id}_{int(time.time())}",
                        step_id=step_id,
                        flow_id=sessions[0].flow_id if sessions else 'unknown',
                        friction_type=FrictionType.STEP_CONFUSION,
                        severity=FrictionSeverity.MEDIUM,
                        confidence=PatternConfidence.MEDIUM,
                        affected_users=excessive_switching_sessions,
                        total_users=len(session_page_switches),
                        abandonment_rate=0.0,
                        avg_time_spent=0.0,
                        retry_rate=0.0,
                        error_rate=0.0,
                        description=f"Excessive page switching detected ({excessive_rate:.1%} of users)",
                        patterns=[
                            f"excessive_switching_rate:{excessive_rate:.3f}",
                            f"avg_page_switches:{avg_page_switches:.1f}",
                            f"excessive_sessions:{excessive_switching_sessions}"
                        ],
                        behavioral_indicators={
                            'excessive_switching_rate': excessive_rate,
                            'avg_page_switches': avg_page_switches,
                            'max_page_switches': max(session_page_switches.values()),
                            'total_sessions_analyzed': len(session_page_switches)
                        }
                    )
                    friction_points.append(friction_point)
        
        return friction_points
    
    def _analyze_loading_performance(self, step_id: str, sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Analyze loading performance issues"""
        friction_points = []
        
        # Extract loading times from events
        loading_times = []
        
        for session in sessions:
            if step_id in session.step_attempts:
                for attempt in session.step_attempts[step_id]:
                    for event in attempt.events:
                        load_time = event.properties.get('load_time')
                        page_load_time = event.properties.get('page_load_time')
                        
                        if load_time:
                            loading_times.append(float(load_time))
                        elif page_load_time:
                            loading_times.append(float(page_load_time))
        
        if loading_times and len(loading_times) > 10:
            avg_load_time = statistics.mean(loading_times)
            p95_load_time = np.percentile(loading_times, 95)
            
            # Consider slow if average > 3 seconds or p95 > 10 seconds
            if avg_load_time > 3.0 or p95_load_time > 10.0:
                severity = FrictionSeverity.HIGH if avg_load_time > 5.0 else FrictionSeverity.MEDIUM
                
                friction_point = FrictionPoint(
                    friction_id=f"performance_{step_id}_{int(time.time())}",
                    step_id=step_id,
                    flow_id=sessions[0].flow_id if sessions else 'unknown',
                    friction_type=FrictionType.PERFORMANCE_ISSUES,
                    severity=severity,
                    confidence=PatternConfidence.HIGH if len(loading_times) > 100 else PatternConfidence.MEDIUM,
                    affected_users=len(set(s.user_id for s in sessions)),
                    total_users=len(set(s.user_id for s in sessions)),
                    abandonment_rate=0.0,
                    avg_time_spent=avg_load_time,
                    retry_rate=0.0,
                    error_rate=0.0,
                    description=f"Slow loading performance (avg: {avg_load_time:.1f}s, p95: {p95_load_time:.1f}s)",
                    patterns=[
                        f"avg_load_time:{avg_load_time:.2f}",
                        f"p95_load_time:{p95_load_time:.2f}",
                        f"sample_size:{len(loading_times)}"
                    ],
                    behavioral_indicators={
                        'avg_load_time_seconds': avg_load_time,
                        'median_load_time_seconds': statistics.median(loading_times),
                        'p95_load_time_seconds': p95_load_time,
                        'p99_load_time_seconds': np.percentile(loading_times, 99),
                        'total_measurements': len(loading_times)
                    }
                )
                friction_points.append(friction_point)
        
        return friction_points
    
    def _analyze_interaction_delays(self, step_id: str, sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Analyze interaction delay patterns"""
        friction_points = []
        
        # Extract interaction delays between events
        interaction_delays = []
        
        for session in sessions:
            if step_id in session.step_attempts:
                for attempt in session.step_attempts[step_id]:
                    events = sorted(attempt.events, key=lambda e: e.timestamp)
                    
                    for i in range(1, len(events)):
                        delay = (events[i].timestamp - events[i-1].timestamp).total_seconds()
                        if 1 < delay < 300:  # Between 1 second and 5 minutes
                            interaction_delays.append(delay)
        
        if interaction_delays and len(interaction_delays) > 20:
            avg_delay = statistics.mean(interaction_delays)
            
            # Consider problematic if average delay > 30 seconds
            if avg_delay > 30:
                friction_point = FrictionPoint(
                    friction_id=f"interaction_delay_{step_id}_{int(time.time())}",
                    step_id=step_id,
                    flow_id=sessions[0].flow_id if sessions else 'unknown',
                    friction_type=FrictionType.COGNITIVE_OVERLOAD,
                    severity=FrictionSeverity.MEDIUM,
                    confidence=PatternConfidence.MEDIUM,
                    affected_users=len(set(s.user_id for s in sessions)),
                    total_users=len(set(s.user_id for s in sessions)),
                    abandonment_rate=0.0,
                    avg_time_spent=avg_delay,
                    retry_rate=0.0,
                    error_rate=0.0,
                    description=f"Long interaction delays detected (avg: {avg_delay:.1f}s)",
                    patterns=[
                        f"avg_interaction_delay:{avg_delay:.2f}",
                        f"delay_measurements:{len(interaction_delays)}"
                    ],
                    behavioral_indicators={
                        'avg_interaction_delay_seconds': avg_delay,
                        'median_delay_seconds': statistics.median(interaction_delays),
                        'total_delay_measurements': len(interaction_delays)
                    }
                )
                friction_points.append(friction_point)
        
        return friction_points
    
    def _analyze_cognitive_overload(self, step_id: str, sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Analyze cognitive overload patterns"""
        friction_points = []
        
        # Analyze time spent vs step complexity
        step_times = []
        help_requests = 0
        total_attempts = 0
        
        for session in sessions:
            if step_id in session.step_attempts:
                for attempt in session.step_attempts[step_id]:
                    total_attempts += 1
                    
                    if attempt.time_spent_seconds:
                        step_times.append(attempt.time_spent_seconds)
                    
                    # Count help-related events
                    for event in attempt.events:
                        if any(keyword in event.event_name.lower() for keyword in ['help', 'tooltip', 'hint', 'support']):
                            help_requests += 1
        
        if step_times and len(step_times) > 10:
            avg_time = statistics.mean(step_times)
            help_rate = help_requests / total_attempts if total_attempts > 0 else 0
            
            # Consider overloaded if average time > 5 minutes or high help request rate
            if avg_time > 300 or help_rate > 0.3:
                friction_point = FrictionPoint(
                    friction_id=f"cognitive_load_{step_id}_{int(time.time())}",
                    step_id=step_id,
                    flow_id=sessions[0].flow_id if sessions else 'unknown',
                    friction_type=FrictionType.COGNITIVE_OVERLOAD,
                    severity=FrictionSeverity.HIGH if avg_time > 600 else FrictionSeverity.MEDIUM,
                    confidence=PatternConfidence.MEDIUM,
                    affected_users=len(set(s.user_id for s in sessions)),
                    total_users=len(set(s.user_id for s in sessions)),
                    abandonment_rate=0.0,
                    avg_time_spent=avg_time,
                    retry_rate=0.0,
                    error_rate=0.0,
                    description=f"Cognitive overload detected (avg time: {avg_time/60:.1f} min, help rate: {help_rate:.1%})",
                    patterns=[
                        f"avg_time_minutes:{avg_time/60:.2f}",
                        f"help_request_rate:{help_rate:.3f}",
                        f"total_attempts:{total_attempts}"
                    ],
                    behavioral_indicators={
                        'avg_time_minutes': avg_time / 60,
                        'median_time_minutes': statistics.median(step_times) / 60,
                        'help_request_rate': help_rate,
                        'total_help_requests': help_requests,
                        'total_attempts': total_attempts
                    }
                )
                friction_points.append(friction_point)
        
        return friction_points
    
    def _analyze_help_seeking_patterns(self, step_id: str, sessions: List[OnboardingSession]) -> List[FrictionPoint]:
        """Analyze help-seeking behavior patterns"""
        # This would be implemented with more detailed help event analysis
        # For now, this is covered in the cognitive overload analysis
        return []

# Global friction detector
friction_detector = None

def initialize_friction_detector(config: Dict[str, Any]):
    """Initialize global friction detector"""
    global friction_detector
    friction_config = FrictionDetectionConfig(config)
    friction_detector = FrictionPatternDetector(friction_config)

def get_friction_detector() -> FrictionPatternDetector:
    """Get global friction detector instance"""
    if not friction_detector:
        raise RuntimeError("Friction detector not initialized. Call initialize_friction_detector() first.")
    
    return friction_detector

def detect_comprehensive_friction_patterns(sessions: List[OnboardingSession], 
                                         flow_def: OnboardingFlowDefinition) -> Tuple[List[FrictionPoint], List[FrictionCluster]]:
    """Detect comprehensive friction patterns and clusters"""
    detector = get_friction_detector()
    
    # Detect basic friction patterns
    friction_points = detector.detect_friction_patterns(sessions, flow_def)
    
    # Add advanced friction analysis
    advanced_analyzer = AdvancedFrictionAnalyzer(detector.config)
    
    for step in flow_def.steps:
        step_id = step.step_id
        
        # Analyze form friction
        form_friction = advanced_analyzer.analyze_form_friction(step_id, sessions)
        friction_points.extend(form_friction)
        
        # Analyze navigation friction
        nav_friction = advanced_analyzer.analyze_navigation_friction(step_id, sessions)
        friction_points.extend(nav_friction)
        
        # Analyze performance friction
        perf_friction = advanced_analyzer.analyze_performance_friction(step_id, sessions)
        friction_points.extend(perf_friction)
        
        # Analyze cognitive load
        cognitive_friction = advanced_analyzer.analyze_cognitive_load(step_id, sessions)
        friction_points.extend(cognitive_friction)
    
    # Cluster friction patterns
    clusters = detector.clustering.cluster_friction_patterns(friction_points)
    
    return friction_points, clusters
