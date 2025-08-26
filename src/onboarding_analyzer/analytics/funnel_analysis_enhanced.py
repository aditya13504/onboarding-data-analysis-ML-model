"""
Advanced Funnel Analysis System

Provides comprehensive funnel analysis including multi-path funnels, conversion optimization,
attribution analysis, drop-off analysis, and funnel performance tracking.
"""

import logging
import asyncio
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple, Union, Set
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from collections import defaultdict, Counter, OrderedDict
import json
import itertools
from pathlib import Path

# Analysis libraries
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from scipy import stats
from scipy.stats import chi2_contingency
import networkx as nx

# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge

# Initialize metrics
funnel_analyses = Counter('analytics_funnel_analyses_total', 'Funnel analyses performed', ['funnel_type'])
conversion_calculations = Counter('analytics_conversion_calculations_total', 'Conversion calculations performed')
drop_off_analyses = Counter('analytics_drop_off_analyses_total', 'Drop-off analyses performed')
funnel_optimizations = Counter('analytics_funnel_optimizations_total', 'Funnel optimizations performed')

logger = logging.getLogger(__name__)

@dataclass
class FunnelStep:
    """Individual funnel step definition."""
    step_id: str
    step_name: str
    step_order: int
    event_criteria: Dict[str, Any]  # Criteria to match events for this step
    is_required: bool
    alternative_events: List[str]  # Alternative events that satisfy this step
    time_constraints: Optional[Dict[str, Any]]  # Time-based constraints

@dataclass
class FunnelDefinition:
    """Complete funnel definition."""
    funnel_id: str
    funnel_name: str
    funnel_type: str  # conversion, onboarding, engagement, etc.
    steps: List[FunnelStep]
    time_window_hours: int
    attribution_model: str  # first_touch, last_touch, linear, time_decay
    success_criteria: Dict[str, Any]
    segment_filters: Optional[Dict[str, Any]]

@dataclass
class UserFunnelJourney:
    """Individual user's journey through a funnel."""
    user_id: str
    funnel_id: str
    journey_start: datetime
    journey_end: Optional[datetime]
    completed_steps: List[str]
    step_timestamps: Dict[str, datetime]
    conversion_achieved: bool
    drop_off_step: Optional[str]
    journey_duration_minutes: Optional[int]
    attribution_touchpoints: List[Dict[str, Any]]
    user_segments: List[str]

@dataclass
class FunnelAnalysisResult:
    """Funnel analysis result."""
    funnel_id: str
    analysis_timestamp: datetime
    total_users: int
    step_conversions: Dict[str, int]  # step_id -> user_count
    step_conversion_rates: Dict[str, float]  # step_id -> conversion_rate
    drop_off_rates: Dict[str, float]  # step_id -> drop_off_rate
    avg_time_between_steps: Dict[str, float]  # step_transition -> avg_minutes
    conversion_paths: List[Dict[str, Any]]  # Most common conversion paths
    segment_performance: Dict[str, Dict[str, float]]  # segment -> metrics
    bottlenecks: List[Dict[str, Any]]  # Identified bottlenecks
    optimization_opportunities: List[Dict[str, Any]]

@dataclass
class MultiPathFunnelResult:
    """Multi-path funnel analysis result."""
    funnel_id: str
    path_analysis: Dict[str, Dict[str, Any]]  # path_signature -> analysis
    path_performance: Dict[str, float]  # path_signature -> conversion_rate
    alternative_paths: List[Dict[str, Any]]  # Alternative successful paths
    path_efficiency: Dict[str, float]  # path_signature -> efficiency_score
    recommended_paths: List[str]  # Most efficient paths

@dataclass
class AttributionAnalysisResult:
    """Attribution analysis result."""
    funnel_id: str
    attribution_model: str
    touchpoint_attribution: Dict[str, float]  # touchpoint -> attribution_score
    channel_attribution: Dict[str, float]  # channel -> attribution_score
    campaign_attribution: Dict[str, float]  # campaign -> attribution_score
    time_to_conversion: Dict[str, float]  # touchpoint -> avg_time_to_conversion
    attribution_paths: List[Dict[str, Any]]  # Detailed attribution paths

class FunnelProcessor:
    """Processes events into funnel journeys."""
    
    def __init__(self):
        self.journey_cache = {}
        
    def process_funnel_journeys(self, events_df: pd.DataFrame, 
                               funnel_definition: FunnelDefinition) -> List[UserFunnelJourney]:
        """Process events into user funnel journeys."""
        try:
            funnel_analyses.labels(funnel_type='journey_processing').inc()
            
            # Filter events relevant to this funnel
            relevant_events = self._filter_relevant_events(events_df, funnel_definition)
            
            if relevant_events.empty:
                logger.warning(f"No relevant events found for funnel {funnel_definition.funnel_id}")
                return []
            
            # Group events by user
            user_journeys = []
            for user_id, user_events in relevant_events.groupby('user_id'):
                journey = self._process_user_journey(user_id, user_events, funnel_definition)
                if journey:
                    user_journeys.append(journey)
            
            logger.info(f"Processed {len(user_journeys)} user journeys for funnel {funnel_definition.funnel_id}")
            return user_journeys
            
        except Exception as e:
            logger.error(f"Funnel journey processing failed: {e}")
            return []
    
    def _filter_relevant_events(self, events_df: pd.DataFrame, 
                               funnel_definition: FunnelDefinition) -> pd.DataFrame:
        """Filter events relevant to the funnel."""
        try:
            # Get all event types used in funnel steps
            relevant_event_types = set()
            
            for step in funnel_definition.steps:
                # Add primary event criteria
                if 'event_type' in step.event_criteria:
                    relevant_event_types.add(step.event_criteria['event_type'])
                
                # Add alternative events
                relevant_event_types.update(step.alternative_events)
            
            # Filter events
            relevant_events = events_df[
                events_df['event_type'].isin(relevant_event_types)
            ].copy()
            
            # Apply segment filters if specified
            if funnel_definition.segment_filters:
                for filter_key, filter_value in funnel_definition.segment_filters.items():
                    if filter_key in relevant_events.columns:
                        if isinstance(filter_value, list):
                            relevant_events = relevant_events[
                                relevant_events[filter_key].isin(filter_value)
                            ]
                        else:
                            relevant_events = relevant_events[
                                relevant_events[filter_key] == filter_value
                            ]
            
            # Sort by timestamp
            relevant_events['timestamp'] = pd.to_datetime(relevant_events['timestamp'])
            relevant_events = relevant_events.sort_values(['user_id', 'timestamp'])
            
            return relevant_events
            
        except Exception as e:
            logger.error(f"Event filtering failed: {e}")
            return pd.DataFrame()
    
    def _process_user_journey(self, user_id: str, user_events: pd.DataFrame,
                             funnel_definition: FunnelDefinition) -> Optional[UserFunnelJourney]:
        """Process a single user's journey through the funnel."""
        try:
            user_events = user_events.sort_values('timestamp')
            
            # Track completed steps and timestamps
            completed_steps = []
            step_timestamps = {}
            attribution_touchpoints = []
            
            # Process events in chronological order
            current_step_index = 0
            journey_start = None
            
            for _, event in user_events.iterrows():
                event_time = pd.to_datetime(event['timestamp'])
                
                # Initialize journey start
                if journey_start is None:
                    journey_start = event_time
                
                # Check if event matches current expected step
                if current_step_index < len(funnel_definition.steps):
                    current_step = funnel_definition.steps[current_step_index]
                    
                    if self._event_matches_step(event, current_step):
                        # User completed this step
                        completed_steps.append(current_step.step_id)
                        step_timestamps[current_step.step_id] = event_time
                        
                        # Add attribution touchpoint
                        touchpoint = {
                            'step_id': current_step.step_id,
                            'timestamp': event_time,
                            'event_type': event.get('event_type'),
                            'channel': event.get('traffic_source', 'unknown'),
                            'campaign': event.get('campaign', 'unknown')
                        }
                        attribution_touchpoints.append(touchpoint)
                        
                        current_step_index += 1
                    
                    # Check time window constraint
                    time_window = timedelta(hours=funnel_definition.time_window_hours)
                    if event_time - journey_start > time_window:
                        break
            
            # Determine journey outcome
            conversion_achieved = len(completed_steps) == len(funnel_definition.steps)
            drop_off_step = None
            
            if not conversion_achieved and completed_steps:
                # Find drop-off step (first incomplete step)
                completed_step_orders = [
                    step.step_order for step in funnel_definition.steps 
                    if step.step_id in completed_steps
                ]
                if completed_step_orders:
                    last_completed_order = max(completed_step_orders)
                    next_steps = [
                        step for step in funnel_definition.steps 
                        if step.step_order > last_completed_order
                    ]
                    if next_steps:
                        drop_off_step = min(next_steps, key=lambda s: s.step_order).step_id
            
            # Calculate journey duration
            journey_end = max(step_timestamps.values()) if step_timestamps else journey_start
            journey_duration = (journey_end - journey_start).total_seconds() / 60  # minutes
            
            # Extract user segments (placeholder - would come from user profile)
            user_segments = self._extract_user_segments(user_events)
            
            return UserFunnelJourney(
                user_id=user_id,
                funnel_id=funnel_definition.funnel_id,
                journey_start=journey_start,
                journey_end=journey_end,
                completed_steps=completed_steps,
                step_timestamps=step_timestamps,
                conversion_achieved=conversion_achieved,
                drop_off_step=drop_off_step,
                journey_duration_minutes=journey_duration,
                attribution_touchpoints=attribution_touchpoints,
                user_segments=user_segments
            )
            
        except Exception as e:
            logger.error(f"User journey processing failed: {e}")
            return None
    
    def _event_matches_step(self, event: pd.Series, step: FunnelStep) -> bool:
        """Check if an event matches a funnel step."""
        try:
            # Check primary event criteria
            for criterion_key, criterion_value in step.event_criteria.items():
                if criterion_key not in event:
                    continue
                
                event_value = event[criterion_key]
                
                if isinstance(criterion_value, list):
                    if event_value not in criterion_value:
                        return False
                elif isinstance(criterion_value, dict):
                    # Handle complex criteria (e.g., range, regex)
                    if 'contains' in criterion_value:
                        if criterion_value['contains'] not in str(event_value):
                            return False
                    elif 'min' in criterion_value or 'max' in criterion_value:
                        try:
                            numeric_value = float(event_value)
                            if 'min' in criterion_value and numeric_value < criterion_value['min']:
                                return False
                            if 'max' in criterion_value and numeric_value > criterion_value['max']:
                                return False
                        except (ValueError, TypeError):
                            return False
                else:
                    if event_value != criterion_value:
                        return False
            
            # Check alternative events
            if step.alternative_events and event.get('event_type') in step.alternative_events:
                return True
            
            # Check time constraints if specified
            if step.time_constraints:
                event_time = pd.to_datetime(event['timestamp'])
                
                if 'hour_range' in step.time_constraints:
                    hour_range = step.time_constraints['hour_range']
                    if not (hour_range[0] <= event_time.hour <= hour_range[1]):
                        return False
                
                if 'weekday_range' in step.time_constraints:
                    weekday_range = step.time_constraints['weekday_range']
                    if not (weekday_range[0] <= event_time.weekday() <= weekday_range[1]):
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"Event-step matching failed: {e}")
            return False
    
    def _extract_user_segments(self, user_events: pd.DataFrame) -> List[str]:
        """Extract user segments from events."""
        segments = []
        
        try:
            # Device type
            devices = user_events['device_type'].dropna().unique()
            if len(devices) > 0:
                segments.append(f"device_{devices[0]}")
            
            # Traffic source
            sources = user_events['traffic_source'].dropna().unique()
            if len(sources) > 0:
                segments.append(f"source_{sources[0]}")
            
            # User type (new vs returning)
            if len(user_events) == 1:
                segments.append("new_user")
            else:
                segments.append("returning_user")
            
            return segments
            
        except Exception as e:
            logger.error(f"User segment extraction failed: {e}")
            return []

class FunnelAnalyzer:
    """Analyzes funnel performance and identifies optimization opportunities."""
    
    def __init__(self):
        self.processor = FunnelProcessor()
    
    def analyze_funnel(self, events_df: pd.DataFrame, 
                      funnel_definition: FunnelDefinition) -> FunnelAnalysisResult:
        """Perform comprehensive funnel analysis."""
        try:
            conversion_calculations.inc()
            
            # Process user journeys
            user_journeys = self.processor.process_funnel_journeys(events_df, funnel_definition)
            
            if not user_journeys:
                logger.warning(f"No user journeys found for funnel {funnel_definition.funnel_id}")
                return self._create_empty_analysis_result(funnel_definition.funnel_id)
            
            # Calculate step conversions
            step_conversions = self._calculate_step_conversions(user_journeys, funnel_definition)
            step_conversion_rates = self._calculate_conversion_rates(step_conversions)
            drop_off_rates = self._calculate_drop_off_rates(step_conversion_rates)
            
            # Calculate timing metrics
            avg_time_between_steps = self._calculate_step_timing(user_journeys, funnel_definition)
            
            # Analyze conversion paths
            conversion_paths = self._analyze_conversion_paths(user_journeys)
            
            # Segment performance analysis
            segment_performance = self._analyze_segment_performance(user_journeys, funnel_definition)
            
            # Identify bottlenecks
            bottlenecks = self._identify_bottlenecks(step_conversion_rates, avg_time_between_steps)
            
            # Generate optimization opportunities
            optimization_opportunities = self._generate_optimization_opportunities(
                step_conversion_rates, bottlenecks, segment_performance
            )
            
            return FunnelAnalysisResult(
                funnel_id=funnel_definition.funnel_id,
                analysis_timestamp=datetime.now(),
                total_users=len(user_journeys),
                step_conversions=step_conversions,
                step_conversion_rates=step_conversion_rates,
                drop_off_rates=drop_off_rates,
                avg_time_between_steps=avg_time_between_steps,
                conversion_paths=conversion_paths,
                segment_performance=segment_performance,
                bottlenecks=bottlenecks,
                optimization_opportunities=optimization_opportunities
            )
            
        except Exception as e:
            logger.error(f"Funnel analysis failed: {e}")
            return self._create_empty_analysis_result(funnel_definition.funnel_id)
    
    def analyze_multi_path_funnel(self, events_df: pd.DataFrame,
                                 funnel_definition: FunnelDefinition) -> MultiPathFunnelResult:
        """Analyze multiple paths through the funnel."""
        try:
            funnel_analyses.labels(funnel_type='multi_path').inc()
            
            # Process user journeys
            user_journeys = self.processor.process_funnel_journeys(events_df, funnel_definition)
            
            if not user_journeys:
                return MultiPathFunnelResult(
                    funnel_id=funnel_definition.funnel_id,
                    path_analysis={},
                    path_performance={},
                    alternative_paths=[],
                    path_efficiency={},
                    recommended_paths=[]
                )
            
            # Group journeys by path signature
            path_groups = defaultdict(list)
            for journey in user_journeys:
                path_signature = self._generate_path_signature(journey)
                path_groups[path_signature].append(journey)
            
            # Analyze each path
            path_analysis = {}
            path_performance = {}
            path_efficiency = {}
            
            for path_signature, path_journeys in path_groups.items():
                analysis = self._analyze_single_path(path_journeys, funnel_definition)
                path_analysis[path_signature] = analysis
                
                # Calculate path performance
                conversion_rate = sum(1 for j in path_journeys if j.conversion_achieved) / len(path_journeys)
                path_performance[path_signature] = conversion_rate
                
                # Calculate path efficiency (conversion rate / avg time)
                avg_duration = np.mean([j.journey_duration_minutes or 0 for j in path_journeys])
                efficiency = conversion_rate / max(avg_duration, 1)  # Avoid division by zero
                path_efficiency[path_signature] = efficiency
            
            # Find alternative successful paths
            alternative_paths = self._find_alternative_paths(path_analysis, path_performance)
            
            # Recommend most efficient paths
            sorted_paths = sorted(path_efficiency.items(), key=lambda x: x[1], reverse=True)
            recommended_paths = [path for path, efficiency in sorted_paths[:3] if efficiency > 0]
            
            return MultiPathFunnelResult(
                funnel_id=funnel_definition.funnel_id,
                path_analysis=path_analysis,
                path_performance=path_performance,
                alternative_paths=alternative_paths,
                path_efficiency=path_efficiency,
                recommended_paths=recommended_paths
            )
            
        except Exception as e:
            logger.error(f"Multi-path funnel analysis failed: {e}")
            return MultiPathFunnelResult(
                funnel_id=funnel_definition.funnel_id,
                path_analysis={},
                path_performance={},
                alternative_paths=[],
                path_efficiency={},
                recommended_paths=[]
            )
    
    def analyze_attribution(self, user_journeys: List[UserFunnelJourney],
                           attribution_model: str = 'linear') -> AttributionAnalysisResult:
        """Perform attribution analysis on funnel journeys."""
        try:
            funnel_analyses.labels(funnel_type='attribution').inc()
            
            # Extract all touchpoints
            all_touchpoints = []
            for journey in user_journeys:
                if journey.conversion_achieved:
                    all_touchpoints.extend(journey.attribution_touchpoints)
            
            if not all_touchpoints:
                return AttributionAnalysisResult(
                    funnel_id=user_journeys[0].funnel_id if user_journeys else 'unknown',
                    attribution_model=attribution_model,
                    touchpoint_attribution={},
                    channel_attribution={},
                    campaign_attribution={},
                    time_to_conversion={},
                    attribution_paths=[]
                )
            
            # Calculate attribution based on model
            if attribution_model == 'first_touch':
                attribution_weights = self._calculate_first_touch_attribution(user_journeys)
            elif attribution_model == 'last_touch':
                attribution_weights = self._calculate_last_touch_attribution(user_journeys)
            elif attribution_model == 'linear':
                attribution_weights = self._calculate_linear_attribution(user_journeys)
            elif attribution_model == 'time_decay':
                attribution_weights = self._calculate_time_decay_attribution(user_journeys)
            else:
                attribution_weights = self._calculate_linear_attribution(user_journeys)
            
            # Aggregate attribution by touchpoint, channel, and campaign
            touchpoint_attribution = defaultdict(float)
            channel_attribution = defaultdict(float)
            campaign_attribution = defaultdict(float)
            
            for journey_id, touchpoint_weights in attribution_weights.items():
                for touchpoint_id, weight in touchpoint_weights.items():
                    # Find touchpoint details
                    touchpoint = self._find_touchpoint_by_id(user_journeys, journey_id, touchpoint_id)
                    if touchpoint:
                        touchpoint_key = f"{touchpoint['step_id']}_{touchpoint['event_type']}"
                        touchpoint_attribution[touchpoint_key] += weight
                        
                        channel_attribution[touchpoint['channel']] += weight
                        campaign_attribution[touchpoint['campaign']] += weight
            
            # Calculate time to conversion
            time_to_conversion = self._calculate_time_to_conversion(user_journeys)
            
            # Generate attribution paths
            attribution_paths = self._generate_attribution_paths(user_journeys, attribution_weights)
            
            return AttributionAnalysisResult(
                funnel_id=user_journeys[0].funnel_id,
                attribution_model=attribution_model,
                touchpoint_attribution=dict(touchpoint_attribution),
                channel_attribution=dict(channel_attribution),
                campaign_attribution=dict(campaign_attribution),
                time_to_conversion=time_to_conversion,
                attribution_paths=attribution_paths
            )
            
        except Exception as e:
            logger.error(f"Attribution analysis failed: {e}")
            return AttributionAnalysisResult(
                funnel_id='unknown',
                attribution_model=attribution_model,
                touchpoint_attribution={},
                channel_attribution={},
                campaign_attribution={},
                time_to_conversion={},
                attribution_paths=[]
            )
    
    def _calculate_step_conversions(self, user_journeys: List[UserFunnelJourney],
                                   funnel_definition: FunnelDefinition) -> Dict[str, int]:
        """Calculate user count for each step."""
        step_conversions = {}
        
        for step in funnel_definition.steps:
            step_conversions[step.step_id] = sum(
                1 for journey in user_journeys 
                if step.step_id in journey.completed_steps
            )
        
        return step_conversions
    
    def _calculate_conversion_rates(self, step_conversions: Dict[str, int]) -> Dict[str, float]:
        """Calculate conversion rates between steps."""
        conversion_rates = {}
        
        if not step_conversions:
            return conversion_rates
        
        # Get total users at funnel entrance
        total_users = max(step_conversions.values()) if step_conversions else 0
        
        if total_users > 0:
            for step_id, user_count in step_conversions.items():
                conversion_rates[step_id] = user_count / total_users
        
        return conversion_rates
    
    def _calculate_drop_off_rates(self, conversion_rates: Dict[str, float]) -> Dict[str, float]:
        """Calculate drop-off rates for each step."""
        drop_off_rates = {}
        
        sorted_steps = sorted(conversion_rates.items(), key=lambda x: x[1], reverse=True)
        
        for i, (step_id, conversion_rate) in enumerate(sorted_steps):
            if i == 0:
                drop_off_rates[step_id] = 0.0  # No drop-off at first step
            else:
                prev_rate = sorted_steps[i-1][1]
                drop_off_rate = (prev_rate - conversion_rate) / prev_rate if prev_rate > 0 else 0
                drop_off_rates[step_id] = drop_off_rate
        
        return drop_off_rates
    
    def _calculate_step_timing(self, user_journeys: List[UserFunnelJourney],
                              funnel_definition: FunnelDefinition) -> Dict[str, float]:
        """Calculate average time between steps."""
        step_timings = defaultdict(list)
        
        for journey in user_journeys:
            if len(journey.completed_steps) < 2:
                continue
            
            sorted_steps = sorted(
                [(step_id, journey.step_timestamps[step_id]) 
                 for step_id in journey.completed_steps],
                key=lambda x: x[1]
            )
            
            for i in range(1, len(sorted_steps)):
                prev_step, prev_time = sorted_steps[i-1]
                curr_step, curr_time = sorted_steps[i]
                
                time_diff = (curr_time - prev_time).total_seconds() / 60  # minutes
                transition_key = f"{prev_step}_to_{curr_step}"
                step_timings[transition_key].append(time_diff)
        
        # Calculate averages
        avg_timings = {}
        for transition, times in step_timings.items():
            avg_timings[transition] = np.mean(times) if times else 0
        
        return avg_timings
    
    def _analyze_conversion_paths(self, user_journeys: List[UserFunnelJourney]) -> List[Dict[str, Any]]:
        """Analyze most common conversion paths."""
        path_counter = Counter()
        
        for journey in user_journeys:
            if journey.conversion_achieved:
                path = " -> ".join(journey.completed_steps)
                path_counter[path] += 1
        
        # Get top 10 most common paths
        top_paths = []
        total_conversions = sum(path_counter.values())
        
        for path, count in path_counter.most_common(10):
            percentage = (count / total_conversions) * 100 if total_conversions > 0 else 0
            top_paths.append({
                'path': path,
                'user_count': count,
                'percentage': percentage
            })
        
        return top_paths
    
    def _analyze_segment_performance(self, user_journeys: List[UserFunnelJourney],
                                   funnel_definition: FunnelDefinition) -> Dict[str, Dict[str, float]]:
        """Analyze funnel performance by user segments."""
        segment_performance = defaultdict(lambda: defaultdict(list))
        
        # Group journeys by segments
        for journey in user_journeys:
            for segment in journey.user_segments:
                segment_performance[segment]['conversions'].append(journey.conversion_achieved)
                segment_performance[segment]['durations'].append(journey.journey_duration_minutes or 0)
                segment_performance[segment]['steps_completed'].append(len(journey.completed_steps))
        
        # Calculate segment metrics
        segment_metrics = {}
        for segment, metrics in segment_performance.items():
            conversions = metrics['conversions']
            durations = metrics['durations']
            steps_completed = metrics['steps_completed']
            
            segment_metrics[segment] = {
                'user_count': len(conversions),
                'conversion_rate': np.mean(conversions) if conversions else 0,
                'avg_duration_minutes': np.mean(durations) if durations else 0,
                'avg_steps_completed': np.mean(steps_completed) if steps_completed else 0
            }
        
        return segment_metrics
    
    def _identify_bottlenecks(self, step_conversion_rates: Dict[str, float],
                             avg_time_between_steps: Dict[str, float]) -> List[Dict[str, Any]]:
        """Identify funnel bottlenecks."""
        drop_off_analyses.inc()
        
        bottlenecks = []
        
        # Identify steps with high drop-off rates
        sorted_steps = sorted(step_conversion_rates.items(), key=lambda x: x[1])
        
        for i, (step_id, conversion_rate) in enumerate(sorted_steps):
            if i > 0:
                prev_rate = sorted_steps[i-1][1]
                drop_off = (prev_rate - conversion_rate) / prev_rate if prev_rate > 0 else 0
                
                if drop_off > 0.3:  # More than 30% drop-off
                    bottlenecks.append({
                        'type': 'high_drop_off',
                        'step_id': step_id,
                        'drop_off_rate': drop_off,
                        'severity': 'high' if drop_off > 0.5 else 'medium'
                    })
        
        # Identify steps with long completion times
        for transition, avg_time in avg_time_between_steps.items():
            if avg_time > 60:  # More than 1 hour
                bottlenecks.append({
                    'type': 'long_completion_time',
                    'transition': transition,
                    'avg_time_minutes': avg_time,
                    'severity': 'high' if avg_time > 180 else 'medium'
                })
        
        return bottlenecks
    
    def _generate_optimization_opportunities(self, step_conversion_rates: Dict[str, float],
                                           bottlenecks: List[Dict[str, Any]],
                                           segment_performance: Dict[str, Dict[str, float]]) -> List[Dict[str, Any]]:
        """Generate funnel optimization opportunities."""
        funnel_optimizations.inc()
        
        opportunities = []
        
        # Opportunities based on bottlenecks
        for bottleneck in bottlenecks:
            if bottleneck['type'] == 'high_drop_off':
                opportunities.append({
                    'type': 'reduce_drop_off',
                    'step_id': bottleneck['step_id'],
                    'description': f"Optimize step {bottleneck['step_id']} to reduce {bottleneck['drop_off_rate']:.1%} drop-off",
                    'priority': bottleneck['severity'],
                    'recommendations': [
                        'Simplify user interface and reduce friction',
                        'Add progressive disclosure to avoid overwhelming users',
                        'Implement exit-intent interventions',
                        'A/B test different step designs'
                    ]
                })
            
            elif bottleneck['type'] == 'long_completion_time':
                opportunities.append({
                    'type': 'reduce_completion_time',
                    'transition': bottleneck['transition'],
                    'description': f"Reduce completion time for {bottleneck['transition']} (currently {bottleneck['avg_time_minutes']:.1f} minutes)",
                    'priority': bottleneck['severity'],
                    'recommendations': [
                        'Streamline the process flow',
                        'Add progress indicators and guidance',
                        'Implement auto-save and resume functionality',
                        'Reduce form fields and simplify inputs'
                    ]
                })
        
        # Opportunities based on segment performance
        best_segments = sorted(
            segment_performance.items(), 
            key=lambda x: x[1]['conversion_rate'], 
            reverse=True
        )
        
        if len(best_segments) > 1:
            best_segment = best_segments[0]
            worst_segment = best_segments[-1]
            
            if best_segment[1]['conversion_rate'] - worst_segment[1]['conversion_rate'] > 0.2:
                opportunities.append({
                    'type': 'segment_optimization',
                    'description': f"Improve performance for {worst_segment[0]} segment (currently {worst_segment[1]['conversion_rate']:.1%} vs {best_segment[1]['conversion_rate']:.1%} for {best_segment[0]})",
                    'priority': 'medium',
                    'recommendations': [
                        f'Study successful patterns from {best_segment[0]} segment',
                        f'Create targeted experience for {worst_segment[0]} users',
                        'Implement personalization based on user segments',
                        'Run cohort-specific optimization experiments'
                    ]
                })
        
        return opportunities
    
    def _generate_path_signature(self, journey: UserFunnelJourney) -> str:
        """Generate a unique signature for a user journey path."""
        return " -> ".join(journey.completed_steps)
    
    def _analyze_single_path(self, path_journeys: List[UserFunnelJourney],
                            funnel_definition: FunnelDefinition) -> Dict[str, Any]:
        """Analyze a single path through the funnel."""
        total_users = len(path_journeys)
        conversions = sum(1 for j in path_journeys if j.conversion_achieved)
        
        analysis = {
            'user_count': total_users,
            'conversions': conversions,
            'conversion_rate': conversions / total_users if total_users > 0 else 0,
            'avg_duration_minutes': np.mean([j.journey_duration_minutes or 0 for j in path_journeys]),
            'step_completion_order': path_journeys[0].completed_steps if path_journeys else []
        }
        
        return analysis
    
    def _find_alternative_paths(self, path_analysis: Dict[str, Dict[str, Any]],
                               path_performance: Dict[str, float]) -> List[Dict[str, Any]]:
        """Find alternative successful paths."""
        alternative_paths = []
        
        # Find paths with good conversion rates but different step orders
        successful_paths = [
            (path, performance) for path, performance in path_performance.items()
            if performance > 0.5  # More than 50% conversion rate
        ]
        
        for path, performance in successful_paths:
            analysis = path_analysis[path]
            alternative_paths.append({
                'path': path,
                'conversion_rate': performance,
                'user_count': analysis['user_count'],
                'avg_duration': analysis['avg_duration_minutes'],
                'efficiency_score': performance / max(analysis['avg_duration_minutes'], 1)
            })
        
        # Sort by efficiency
        alternative_paths.sort(key=lambda x: x['efficiency_score'], reverse=True)
        
        return alternative_paths[:5]  # Top 5 alternative paths
    
    def _calculate_first_touch_attribution(self, user_journeys: List[UserFunnelJourney]) -> Dict[str, Dict[str, float]]:
        """Calculate first-touch attribution."""
        attribution_weights = {}
        
        for journey in user_journeys:
            if journey.conversion_achieved and journey.attribution_touchpoints:
                journey_weights = {}
                first_touchpoint = journey.attribution_touchpoints[0]
                touchpoint_id = f"{first_touchpoint['step_id']}_{first_touchpoint['timestamp']}"
                journey_weights[touchpoint_id] = 1.0
                attribution_weights[journey.user_id] = journey_weights
        
        return attribution_weights
    
    def _calculate_last_touch_attribution(self, user_journeys: List[UserFunnelJourney]) -> Dict[str, Dict[str, float]]:
        """Calculate last-touch attribution."""
        attribution_weights = {}
        
        for journey in user_journeys:
            if journey.conversion_achieved and journey.attribution_touchpoints:
                journey_weights = {}
                last_touchpoint = journey.attribution_touchpoints[-1]
                touchpoint_id = f"{last_touchpoint['step_id']}_{last_touchpoint['timestamp']}"
                journey_weights[touchpoint_id] = 1.0
                attribution_weights[journey.user_id] = journey_weights
        
        return attribution_weights
    
    def _calculate_linear_attribution(self, user_journeys: List[UserFunnelJourney]) -> Dict[str, Dict[str, float]]:
        """Calculate linear attribution."""
        attribution_weights = {}
        
        for journey in user_journeys:
            if journey.conversion_achieved and journey.attribution_touchpoints:
                journey_weights = {}
                weight_per_touchpoint = 1.0 / len(journey.attribution_touchpoints)
                
                for touchpoint in journey.attribution_touchpoints:
                    touchpoint_id = f"{touchpoint['step_id']}_{touchpoint['timestamp']}"
                    journey_weights[touchpoint_id] = weight_per_touchpoint
                
                attribution_weights[journey.user_id] = journey_weights
        
        return attribution_weights
    
    def _calculate_time_decay_attribution(self, user_journeys: List[UserFunnelJourney]) -> Dict[str, Dict[str, float]]:
        """Calculate time-decay attribution."""
        attribution_weights = {}
        
        for journey in user_journeys:
            if journey.conversion_achieved and journey.attribution_touchpoints:
                journey_weights = {}
                conversion_time = journey.journey_end or journey.journey_start
                
                # Calculate decay weights (more recent touchpoints get higher weight)
                total_weight = 0
                touchpoint_weights = []
                
                for touchpoint in journey.attribution_touchpoints:
                    touchpoint_time = touchpoint['timestamp']
                    time_diff_hours = (conversion_time - touchpoint_time).total_seconds() / 3600
                    
                    # Exponential decay (half-life of 24 hours)
                    weight = 0.5 ** (time_diff_hours / 24)
                    touchpoint_weights.append(weight)
                    total_weight += weight
                
                # Normalize weights
                for i, touchpoint in enumerate(journey.attribution_touchpoints):
                    touchpoint_id = f"{touchpoint['step_id']}_{touchpoint['timestamp']}"
                    normalized_weight = touchpoint_weights[i] / total_weight if total_weight > 0 else 0
                    journey_weights[touchpoint_id] = normalized_weight
                
                attribution_weights[journey.user_id] = journey_weights
        
        return attribution_weights
    
    def _find_touchpoint_by_id(self, user_journeys: List[UserFunnelJourney],
                              journey_id: str, touchpoint_id: str) -> Optional[Dict[str, Any]]:
        """Find touchpoint details by ID."""
        for journey in user_journeys:
            if journey.user_id == journey_id:
                for touchpoint in journey.attribution_touchpoints:
                    current_id = f"{touchpoint['step_id']}_{touchpoint['timestamp']}"
                    if current_id == touchpoint_id:
                        return touchpoint
        return None
    
    def _calculate_time_to_conversion(self, user_journeys: List[UserFunnelJourney]) -> Dict[str, float]:
        """Calculate average time to conversion for each touchpoint."""
        touchpoint_times = defaultdict(list)
        
        for journey in user_journeys:
            if journey.conversion_achieved:
                for touchpoint in journey.attribution_touchpoints:
                    touchpoint_key = f"{touchpoint['step_id']}_{touchpoint['event_type']}"
                    time_to_conversion = (journey.journey_end - touchpoint['timestamp']).total_seconds() / 3600  # hours
                    touchpoint_times[touchpoint_key].append(time_to_conversion)
        
        # Calculate averages
        avg_times = {}
        for touchpoint, times in touchpoint_times.items():
            avg_times[touchpoint] = np.mean(times) if times else 0
        
        return avg_times
    
    def _generate_attribution_paths(self, user_journeys: List[UserFunnelJourney],
                                  attribution_weights: Dict[str, Dict[str, float]]) -> List[Dict[str, Any]]:
        """Generate detailed attribution paths."""
        attribution_paths = []
        
        for journey in user_journeys:
            if journey.conversion_achieved and journey.user_id in attribution_weights:
                path_info = {
                    'user_id': journey.user_id,
                    'journey_duration_hours': journey.journey_duration_minutes / 60 if journey.journey_duration_minutes else 0,
                    'touchpoints': []
                }
                
                for touchpoint in journey.attribution_touchpoints:
                    touchpoint_id = f"{touchpoint['step_id']}_{touchpoint['timestamp']}"
                    weight = attribution_weights[journey.user_id].get(touchpoint_id, 0)
                    
                    path_info['touchpoints'].append({
                        'step_id': touchpoint['step_id'],
                        'event_type': touchpoint['event_type'],
                        'channel': touchpoint['channel'],
                        'campaign': touchpoint['campaign'],
                        'timestamp': touchpoint['timestamp'].isoformat(),
                        'attribution_weight': weight
                    })
                
                attribution_paths.append(path_info)
        
        return attribution_paths
    
    def _create_empty_analysis_result(self, funnel_id: str) -> FunnelAnalysisResult:
        """Create empty analysis result for error cases."""
        return FunnelAnalysisResult(
            funnel_id=funnel_id,
            analysis_timestamp=datetime.now(),
            total_users=0,
            step_conversions={},
            step_conversion_rates={},
            drop_off_rates={},
            avg_time_between_steps={},
            conversion_paths=[],
            segment_performance={},
            bottlenecks=[],
            optimization_opportunities=[]
        )

class AdvancedFunnelAnalyzer:
    """Main coordinator for advanced funnel analysis."""
    
    def __init__(self):
        self.funnel_analyzer = FunnelAnalyzer()
        
        # Cache for analysis results
        self.latest_funnel_results: Dict[str, FunnelAnalysisResult] = {}
        self.latest_multipath_results: Dict[str, MultiPathFunnelResult] = {}
        self.latest_attribution_results: Dict[str, AttributionAnalysisResult] = {}
    
    async def analyze_advanced_funnel(self, events_df: pd.DataFrame,
                                    funnel_definitions: List[FunnelDefinition],
                                    analysis_types: List[str] = None) -> Dict[str, Any]:
        """Perform comprehensive advanced funnel analysis."""
        try:
            if analysis_types is None:
                analysis_types = ['standard', 'multi_path', 'attribution']
            
            analysis_results = {}
            
            for funnel_definition in funnel_definitions:
                funnel_id = funnel_definition.funnel_id
                funnel_results = {}
                
                # Standard funnel analysis
                if 'standard' in analysis_types:
                    logger.info(f"Performing standard funnel analysis for {funnel_id}...")
                    standard_result = self.funnel_analyzer.analyze_funnel(events_df, funnel_definition)
                    funnel_results['standard'] = asdict(standard_result)
                    self.latest_funnel_results[funnel_id] = standard_result
                
                # Multi-path analysis
                if 'multi_path' in analysis_types:
                    logger.info(f"Performing multi-path analysis for {funnel_id}...")
                    multipath_result = self.funnel_analyzer.analyze_multi_path_funnel(events_df, funnel_definition)
                    funnel_results['multi_path'] = asdict(multipath_result)
                    self.latest_multipath_results[funnel_id] = multipath_result
                
                # Attribution analysis
                if 'attribution' in analysis_types:
                    logger.info(f"Performing attribution analysis for {funnel_id}...")
                    user_journeys = self.funnel_analyzer.processor.process_funnel_journeys(events_df, funnel_definition)
                    attribution_result = self.funnel_analyzer.analyze_attribution(
                        user_journeys, funnel_definition.attribution_model
                    )
                    funnel_results['attribution'] = asdict(attribution_result)
                    self.latest_attribution_results[funnel_id] = attribution_result
                
                analysis_results[funnel_id] = funnel_results
            
            # Generate cross-funnel insights
            analysis_results['cross_funnel_insights'] = self._generate_cross_funnel_insights()
            
            # Calculate funnel benchmarks
            analysis_results['funnel_benchmarks'] = self._calculate_funnel_benchmarks()
            
            analysis_results['analysis_timestamp'] = datetime.now().isoformat()
            
            logger.info("Advanced funnel analysis completed successfully")
            return analysis_results
            
        except Exception as e:
            logger.error(f"Advanced funnel analysis failed: {e}")
            return {'error': str(e)}
    
    def compare_funnels(self, funnel_ids: List[str], 
                       comparison_metrics: List[str] = None) -> Dict[str, Any]:
        """Compare multiple funnels across specified metrics."""
        try:
            if comparison_metrics is None:
                comparison_metrics = ['overall_conversion_rate', 'avg_completion_time', 'drop_off_rate']
            
            comparison_result = {}
            
            # Extract metrics for comparison
            for metric in comparison_metrics:
                metric_values = {}
                
                for funnel_id in funnel_ids:
                    if funnel_id in self.latest_funnel_results:
                        result = self.latest_funnel_results[funnel_id]
                        
                        if metric == 'overall_conversion_rate':
                            # Calculate overall conversion rate (users completing all steps)
                            if result.step_conversion_rates:
                                metric_values[funnel_id] = min(result.step_conversion_rates.values())
                        
                        elif metric == 'avg_completion_time':
                            # Calculate average completion time
                            if result.avg_time_between_steps:
                                metric_values[funnel_id] = sum(result.avg_time_between_steps.values())
                        
                        elif metric == 'drop_off_rate':
                            # Calculate highest drop-off rate
                            if result.drop_off_rates:
                                metric_values[funnel_id] = max(result.drop_off_rates.values())
                
                comparison_result[metric] = metric_values
            
            # Generate comparison insights
            comparison_result['insights'] = self._generate_funnel_comparison_insights(comparison_result)
            
            return comparison_result
            
        except Exception as e:
            logger.error(f"Funnel comparison failed: {e}")
            return {'error': str(e)}
    
    def _generate_cross_funnel_insights(self) -> List[Dict[str, Any]]:
        """Generate insights across all analyzed funnels."""
        insights = []
        
        try:
            if not self.latest_funnel_results:
                return insights
            
            # Compare funnel performance
            funnel_performance = {}
            for funnel_id, result in self.latest_funnel_results.items():
                if result.step_conversion_rates:
                    overall_rate = min(result.step_conversion_rates.values())
                    funnel_performance[funnel_id] = overall_rate
            
            if len(funnel_performance) > 1:
                best_funnel = max(funnel_performance.items(), key=lambda x: x[1])
                worst_funnel = min(funnel_performance.items(), key=lambda x: x[1])
                
                insights.append({
                    'type': 'funnel_performance_comparison',
                    'title': 'Funnel Performance Variation',
                    'description': f'{best_funnel[0]} has {best_funnel[1]:.1%} conversion vs {worst_funnel[0]} at {worst_funnel[1]:.1%}',
                    'priority': 'high',
                    'recommendations': [
                        f'Study successful patterns from {best_funnel[0]}',
                        f'Apply best practices to improve {worst_funnel[0]}',
                        'Standardize high-performing funnel elements'
                    ]
                })
            
            # Identify common bottlenecks
            all_bottlenecks = []
            for result in self.latest_funnel_results.values():
                all_bottlenecks.extend(result.bottlenecks)
            
            if all_bottlenecks:
                bottleneck_types = Counter([b['type'] for b in all_bottlenecks])
                most_common_bottleneck = bottleneck_types.most_common(1)[0]
                
                insights.append({
                    'type': 'common_bottleneck',
                    'title': 'Common Bottleneck Pattern',
                    'description': f'{most_common_bottleneck[0]} appears in {most_common_bottleneck[1]} funnels',
                    'priority': 'medium',
                    'recommendations': [
                        'Address systematic UX issues causing this bottleneck',
                        'Implement consistent solutions across all funnels',
                        'Monitor bottleneck resolution effectiveness'
                    ]
                })
            
            return insights
            
        except Exception as e:
            logger.error(f"Cross-funnel insights generation failed: {e}")
            return []
    
    def _calculate_funnel_benchmarks(self) -> Dict[str, Any]:
        """Calculate benchmarks across all funnels."""
        try:
            benchmarks = {}
            
            if not self.latest_funnel_results:
                return benchmarks
            
            # Conversion rate benchmarks
            conversion_rates = []
            for result in self.latest_funnel_results.values():
                if result.step_conversion_rates:
                    overall_rate = min(result.step_conversion_rates.values())
                    conversion_rates.append(overall_rate)
            
            if conversion_rates:
                benchmarks['conversion_rate'] = {
                    'mean': np.mean(conversion_rates),
                    'median': np.median(conversion_rates),
                    'p25': np.percentile(conversion_rates, 25),
                    'p75': np.percentile(conversion_rates, 75)
                }
            
            # Completion time benchmarks
            completion_times = []
            for result in self.latest_funnel_results.values():
                if result.avg_time_between_steps:
                    total_time = sum(result.avg_time_between_steps.values())
                    completion_times.append(total_time)
            
            if completion_times:
                benchmarks['completion_time_minutes'] = {
                    'mean': np.mean(completion_times),
                    'median': np.median(completion_times),
                    'p25': np.percentile(completion_times, 25),
                    'p75': np.percentile(completion_times, 75)
                }
            
            # Drop-off rate benchmarks
            drop_off_rates = []
            for result in self.latest_funnel_results.values():
                if result.drop_off_rates:
                    max_drop_off = max(result.drop_off_rates.values())
                    drop_off_rates.append(max_drop_off)
            
            if drop_off_rates:
                benchmarks['drop_off_rate'] = {
                    'mean': np.mean(drop_off_rates),
                    'median': np.median(drop_off_rates),
                    'p25': np.percentile(drop_off_rates, 25),
                    'p75': np.percentile(drop_off_rates, 75)
                }
            
            return benchmarks
            
        except Exception as e:
            logger.error(f"Funnel benchmarks calculation failed: {e}")
            return {}
    
    def _generate_funnel_comparison_insights(self, comparison_result: Dict[str, Any]) -> List[str]:
        """Generate insights from funnel comparison."""
        insights = []
        
        try:
            for metric, values in comparison_result.items():
                if metric == 'insights':  # Skip the insights key itself
                    continue
                
                if len(values) >= 2:
                    sorted_funnels = sorted(values.items(), key=lambda x: x[1], reverse=True)
                    best_funnel = sorted_funnels[0]
                    worst_funnel = sorted_funnels[-1]
                    
                    if best_funnel[1] > worst_funnel[1]:
                        if metric == 'overall_conversion_rate':
                            insights.append(
                                f"{best_funnel[0]} has {best_funnel[1]:.1%} conversion rate vs {worst_funnel[0]} at {worst_funnel[1]:.1%}"
                            )
                        elif metric == 'avg_completion_time':
                            insights.append(
                                f"{worst_funnel[0]} completes fastest at {worst_funnel[1]:.1f} minutes vs {best_funnel[0]} at {best_funnel[1]:.1f} minutes"
                            )
                        elif metric == 'drop_off_rate':
                            insights.append(
                                f"{worst_funnel[0]} has lowest drop-off at {worst_funnel[1]:.1%} vs {best_funnel[0]} at {best_funnel[1]:.1%}"
                            )
            
            return insights
            
        except Exception as e:
            logger.error(f"Funnel comparison insights generation failed: {e}")
            return []
    
    def get_funnel_details(self, funnel_id: str, analysis_type: str = 'standard') -> Optional[Dict[str, Any]]:
        """Get detailed analysis results for a specific funnel."""
        try:
            if analysis_type == 'standard' and funnel_id in self.latest_funnel_results:
                return asdict(self.latest_funnel_results[funnel_id])
            elif analysis_type == 'multi_path' and funnel_id in self.latest_multipath_results:
                return asdict(self.latest_multipath_results[funnel_id])
            elif analysis_type == 'attribution' and funnel_id in self.latest_attribution_results:
                return asdict(self.latest_attribution_results[funnel_id])
            
            return None
            
        except Exception as e:
            logger.error(f"Funnel details retrieval failed: {e}")
            return None
