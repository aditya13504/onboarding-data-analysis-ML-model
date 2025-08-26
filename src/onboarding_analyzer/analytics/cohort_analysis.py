"""
Cohort Analysis System

Provides comprehensive cohort analysis including user retention cohorts, 
revenue cohorts, behavioral cohorts, and lifecycle cohort tracking.
"""

import logging
import asyncio
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import json
from pathlib import Path

# Analysis libraries
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from scipy import stats
import seaborn as sns
import matplotlib.pyplot as plt

# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge

# Initialize metrics
cohort_analyses = Counter('analytics_cohort_analyses_total', 'Cohort analyses performed', ['cohort_type'])
cohorts_generated = Gauge('analytics_cohorts_count', 'Number of cohorts generated')
retention_calculations = Counter('analytics_retention_calculations_total', 'Retention calculations performed')
revenue_cohort_analyses = Counter('analytics_revenue_cohort_analyses_total', 'Revenue cohort analyses performed')

logger = logging.getLogger(__name__)

@dataclass
class CohortUser:
    """User data for cohort analysis."""
    user_id: str
    cohort_period: str  # e.g., "2024-01", "2024-Q1", "2024-W01"
    first_activity_date: datetime
    cohort_size: int
    lifetime_value: float
    total_sessions: int
    conversion_events: int
    last_activity_date: datetime
    is_active: bool
    days_since_first_activity: int

@dataclass
class RetentionCohort:
    """Retention cohort data structure."""
    cohort_id: str
    cohort_period: str
    cohort_type: str  # daily, weekly, monthly, quarterly
    cohort_size: int
    first_activity_date: datetime
    users: List[str]
    retention_rates: Dict[str, float]  # period -> retention rate
    avg_lifetime_value: float
    conversion_rate: float
    characteristics: Dict[str, Any]

@dataclass
class RevenueCohort:
    """Revenue cohort data structure."""
    cohort_id: str
    cohort_period: str
    cohort_size: int
    total_revenue: float
    avg_revenue_per_user: float
    revenue_by_period: Dict[str, float]  # period -> cumulative revenue
    ltv_segments: Dict[str, int]  # ltv_range -> user_count
    revenue_retention: Dict[str, float]  # period -> revenue retention rate
    paying_users: int
    conversion_to_paid: float

@dataclass
class BehavioralCohort:
    """Behavioral cohort data structure."""
    cohort_id: str
    cohort_name: str
    cohort_type: str  # engagement_based, feature_based, journey_based
    user_count: int
    defining_behaviors: List[str]
    avg_metrics: Dict[str, float]
    retention_patterns: Dict[str, float]
    conversion_patterns: Dict[str, float]
    lifecycle_progression: Dict[str, int]

@dataclass
class CohortComparison:
    """Cohort comparison analysis result."""
    comparison_id: str
    cohort_ids: List[str]
    comparison_type: str
    metrics_comparison: Dict[str, Dict[str, float]]
    statistical_significance: Dict[str, Dict[str, Any]]
    insights: List[str]
    recommendations: List[str]

class CohortBuilder:
    """Builds cohorts from user activity data."""
    
    def __init__(self):
        self.cohort_types = ['daily', 'weekly', 'monthly', 'quarterly']
        
    def build_retention_cohorts(self, events_df: pd.DataFrame, 
                               cohort_type: str = 'monthly') -> List[RetentionCohort]:
        """Build retention cohorts from event data."""
        try:
            cohort_analyses.labels(cohort_type='retention').inc()
            
            # Prepare user activity data
            user_activity = self._prepare_user_activity_data(events_df)
            
            if user_activity.empty:
                logger.warning("No user activity data available for cohort analysis")
                return []
            
            # Define cohort periods
            cohort_periods = self._define_cohort_periods(user_activity, cohort_type)
            
            # Build cohorts for each period
            cohorts = []
            for period in cohort_periods:
                cohort = self._build_single_retention_cohort(
                    user_activity, period, cohort_type
                )
                if cohort:
                    cohorts.append(cohort)
            
            cohorts_generated.set(len(cohorts))
            logger.info(f"Built {len(cohorts)} retention cohorts")
            
            return cohorts
            
        except Exception as e:
            logger.error(f"Retention cohort building failed: {e}")
            return []
    
    def build_revenue_cohorts(self, events_df: pd.DataFrame, 
                             revenue_events: List[str] = None,
                             cohort_type: str = 'monthly') -> List[RevenueCohort]:
        """Build revenue cohorts from event data."""
        try:
            cohort_analyses.labels(cohort_type='revenue').inc()
            
            if revenue_events is None:
                revenue_events = ['purchase', 'subscription', 'payment']
            
            # Prepare revenue data
            revenue_data = self._prepare_revenue_data(events_df, revenue_events)
            
            if revenue_data.empty:
                logger.warning("No revenue data available for cohort analysis")
                return []
            
            # Define cohort periods
            cohort_periods = self._define_cohort_periods(revenue_data, cohort_type)
            
            # Build revenue cohorts
            cohorts = []
            for period in cohort_periods:
                cohort = self._build_single_revenue_cohort(
                    revenue_data, period, cohort_type
                )
                if cohort:
                    cohorts.append(cohort)
            
            revenue_cohort_analyses.inc()
            logger.info(f"Built {len(cohorts)} revenue cohorts")
            
            return cohorts
            
        except Exception as e:
            logger.error(f"Revenue cohort building failed: {e}")
            return []
    
    def build_behavioral_cohorts(self, events_df: pd.DataFrame,
                               user_profiles: List[Dict] = None) -> List[BehavioralCohort]:
        """Build behavioral cohorts based on user behavior patterns."""
        try:
            cohort_analyses.labels(cohort_type='behavioral').inc()
            
            # Prepare behavioral data
            behavioral_data = self._prepare_behavioral_data(events_df, user_profiles)
            
            if behavioral_data.empty:
                logger.warning("No behavioral data available for cohort analysis")
                return []
            
            # Define behavioral cohorts
            cohorts = []
            
            # Engagement-based cohorts
            engagement_cohorts = self._build_engagement_cohorts(behavioral_data)
            cohorts.extend(engagement_cohorts)
            
            # Feature-based cohorts
            feature_cohorts = self._build_feature_usage_cohorts(behavioral_data)
            cohorts.extend(feature_cohorts)
            
            # Journey-based cohorts
            journey_cohorts = self._build_journey_cohorts(behavioral_data)
            cohorts.extend(journey_cohorts)
            
            logger.info(f"Built {len(cohorts)} behavioral cohorts")
            
            return cohorts
            
        except Exception as e:
            logger.error(f"Behavioral cohort building failed: {e}")
            return []
    
    def _prepare_user_activity_data(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """Prepare user activity data for cohort analysis."""
        try:
            # Convert timestamp to datetime
            events_df['timestamp'] = pd.to_datetime(events_df['timestamp'])
            events_df['date'] = events_df['timestamp'].dt.date
            
            # Get first activity date for each user
            first_activity = events_df.groupby('user_id')['timestamp'].min().reset_index()
            first_activity.columns = ['user_id', 'first_activity_date']
            
            # Get user activity summary
            user_summary = events_df.groupby('user_id').agg({
                'timestamp': ['min', 'max', 'count'],
                'event_type': 'nunique'
            }).round(2)
            
            # Flatten column names
            user_summary.columns = ['first_activity', 'last_activity', 'total_events', 'unique_event_types']
            user_summary = user_summary.reset_index()
            
            # Merge with first activity
            user_activity = user_summary.merge(first_activity, on='user_id', how='left')
            
            # Add derived metrics
            user_activity['days_active'] = (
                user_activity['last_activity'] - user_activity['first_activity']
            ).dt.days + 1
            
            # Add conversion information
            conversion_events = ['purchase', 'signup', 'conversion', 'subscribe']
            conversions = events_df[events_df['event_type'].isin(conversion_events)]
            user_conversions = conversions.groupby('user_id').size().reset_index()
            user_conversions.columns = ['user_id', 'conversion_count']
            
            user_activity = user_activity.merge(user_conversions, on='user_id', how='left')
            user_activity['conversion_count'] = user_activity['conversion_count'].fillna(0)
            
            return user_activity
            
        except Exception as e:
            logger.error(f"User activity data preparation failed: {e}")
            return pd.DataFrame()
    
    def _prepare_revenue_data(self, events_df: pd.DataFrame, 
                             revenue_events: List[str]) -> pd.DataFrame:
        """Prepare revenue data for cohort analysis."""
        try:
            # Filter revenue events
            revenue_df = events_df[events_df['event_type'].isin(revenue_events)].copy()
            
            if revenue_df.empty:
                return pd.DataFrame()
            
            # Extract revenue amount (try multiple possible field names)
            revenue_fields = ['revenue', 'amount', 'value', 'price', 'total']
            revenue_df['revenue_amount'] = 0
            
            for field in revenue_fields:
                if field in revenue_df.columns:
                    revenue_df['revenue_amount'] = pd.to_numeric(
                        revenue_df[field], errors='coerce'
                    ).fillna(0)
                    break
            
            # Convert timestamp
            revenue_df['timestamp'] = pd.to_datetime(revenue_df['timestamp'])
            revenue_df['date'] = revenue_df['timestamp'].dt.date
            
            # Get first purchase date for each user
            first_purchase = revenue_df.groupby('user_id')['timestamp'].min().reset_index()
            first_purchase.columns = ['user_id', 'first_purchase_date']
            
            # Calculate user revenue metrics
            user_revenue = revenue_df.groupby('user_id').agg({
                'revenue_amount': ['sum', 'count', 'mean'],
                'timestamp': ['min', 'max']
            }).round(2)
            
            # Flatten column names
            user_revenue.columns = ['total_revenue', 'purchase_count', 'avg_purchase_amount', 
                                   'first_purchase', 'last_purchase']
            user_revenue = user_revenue.reset_index()
            
            # Merge with first purchase
            user_revenue = user_revenue.merge(first_purchase, on='user_id', how='left')
            
            # Add derived metrics
            user_revenue['purchase_frequency'] = user_revenue['purchase_count'] / (
                (user_revenue['last_purchase'] - user_revenue['first_purchase']).dt.days + 1
            )
            user_revenue['purchase_frequency'] = user_revenue['purchase_frequency'].fillna(0)
            
            return user_revenue
            
        except Exception as e:
            logger.error(f"Revenue data preparation failed: {e}")
            return pd.DataFrame()
    
    def _prepare_behavioral_data(self, events_df: pd.DataFrame, 
                               user_profiles: List[Dict] = None) -> pd.DataFrame:
        """Prepare behavioral data for cohort analysis."""
        try:
            # Convert timestamp
            events_df['timestamp'] = pd.to_datetime(events_df['timestamp'])
            
            # Calculate behavioral metrics per user
            behavioral_metrics = []
            
            for user_id, user_events in events_df.groupby('user_id'):
                user_events = user_events.sort_values('timestamp')
                
                # Basic activity metrics
                total_events = len(user_events)
                unique_event_types = user_events['event_type'].nunique()
                first_activity = user_events['timestamp'].min()
                last_activity = user_events['timestamp'].max()
                activity_span = (last_activity - first_activity).days + 1
                
                # Session-based metrics (approximate)
                session_threshold = timedelta(minutes=30)
                session_count = 1
                last_event_time = user_events.iloc[0]['timestamp']
                
                for _, event in user_events.iterrows():
                    if event['timestamp'] - last_event_time > session_threshold:
                        session_count += 1
                    last_event_time = event['timestamp']
                
                # Feature usage (based on event types)
                feature_usage = user_events['event_type'].value_counts().to_dict()
                
                # Page views and engagement
                page_views = len(user_events[user_events['event_type'] == 'page_view'])
                
                # Conversion events
                conversion_events = ['purchase', 'signup', 'conversion', 'subscribe']
                conversions = len(user_events[user_events['event_type'].isin(conversion_events)])
                
                behavioral_metrics.append({
                    'user_id': user_id,
                    'total_events': total_events,
                    'unique_event_types': unique_event_types,
                    'first_activity': first_activity,
                    'last_activity': last_activity,
                    'activity_span_days': activity_span,
                    'session_count': session_count,
                    'avg_events_per_session': total_events / session_count,
                    'page_views': page_views,
                    'conversions': conversions,
                    'feature_usage': feature_usage
                })
            
            return pd.DataFrame(behavioral_metrics)
            
        except Exception as e:
            logger.error(f"Behavioral data preparation failed: {e}")
            return pd.DataFrame()
    
    def _define_cohort_periods(self, data_df: pd.DataFrame, 
                              cohort_type: str) -> List[str]:
        """Define cohort periods based on cohort type."""
        try:
            # Get date range from data
            if 'first_activity_date' in data_df.columns:
                date_col = 'first_activity_date'
            elif 'first_purchase_date' in data_df.columns:
                date_col = 'first_purchase_date'
            else:
                date_col = 'first_activity'
            
            min_date = data_df[date_col].min()
            max_date = data_df[date_col].max()
            
            periods = []
            
            if cohort_type == 'daily':
                current_date = min_date.date() if hasattr(min_date, 'date') else min_date
                end_date = max_date.date() if hasattr(max_date, 'date') else max_date
                
                while current_date <= end_date:
                    periods.append(current_date.strftime('%Y-%m-%d'))
                    current_date += timedelta(days=1)
            
            elif cohort_type == 'weekly':
                # Week-based periods
                current_date = min_date - timedelta(days=min_date.weekday())
                end_date = max_date
                
                while current_date <= end_date:
                    week_start = current_date.date() if hasattr(current_date, 'date') else current_date
                    periods.append(f"{week_start.year}-W{week_start.isocalendar()[1]:02d}")
                    current_date += timedelta(weeks=1)
            
            elif cohort_type == 'monthly':
                # Month-based periods
                current_date = min_date.replace(day=1)
                end_date = max_date.replace(day=1)
                
                while current_date <= end_date:
                    periods.append(current_date.strftime('%Y-%m'))
                    # Move to next month
                    if current_date.month == 12:
                        current_date = current_date.replace(year=current_date.year + 1, month=1)
                    else:
                        current_date = current_date.replace(month=current_date.month + 1)
            
            elif cohort_type == 'quarterly':
                # Quarter-based periods
                current_date = min_date.replace(month=((min_date.month - 1) // 3) * 3 + 1, day=1)
                end_date = max_date.replace(month=((max_date.month - 1) // 3) * 3 + 1, day=1)
                
                while current_date <= end_date:
                    quarter = (current_date.month - 1) // 3 + 1
                    periods.append(f"{current_date.year}-Q{quarter}")
                    # Move to next quarter
                    if quarter == 4:
                        current_date = current_date.replace(year=current_date.year + 1, month=1)
                    else:
                        current_date = current_date.replace(month=current_date.month + 3)
            
            return sorted(periods)
            
        except Exception as e:
            logger.error(f"Cohort period definition failed: {e}")
            return []
    
    def _build_single_retention_cohort(self, user_activity: pd.DataFrame,
                                      cohort_period: str, cohort_type: str) -> Optional[RetentionCohort]:
        """Build a single retention cohort."""
        try:
            # Filter users for this cohort period
            cohort_users = self._filter_users_by_period(
                user_activity, cohort_period, cohort_type, 'first_activity_date'
            )
            
            if cohort_users.empty:
                return None
            
            # Calculate retention rates
            retention_rates = self._calculate_retention_rates(
                cohort_users, cohort_period, cohort_type
            )
            
            # Calculate cohort characteristics
            characteristics = {
                'avg_total_events': cohort_users['total_events'].mean(),
                'avg_unique_event_types': cohort_users['unique_event_types'].mean(),
                'avg_days_active': cohort_users['days_active'].mean(),
                'conversion_rate': (cohort_users['conversion_count'] > 0).mean()
            }
            
            # Calculate average lifetime value (placeholder - would need actual revenue data)
            avg_ltv = cohort_users['total_events'].mean() * 0.1  # Placeholder calculation
            
            cohort = RetentionCohort(
                cohort_id=f"retention_{cohort_type}_{cohort_period}",
                cohort_period=cohort_period,
                cohort_type=cohort_type,
                cohort_size=len(cohort_users),
                first_activity_date=cohort_users['first_activity_date'].min(),
                users=cohort_users['user_id'].tolist(),
                retention_rates=retention_rates,
                avg_lifetime_value=avg_ltv,
                conversion_rate=characteristics['conversion_rate'],
                characteristics=characteristics
            )
            
            return cohort
            
        except Exception as e:
            logger.error(f"Single retention cohort building failed: {e}")
            return None
    
    def _build_single_revenue_cohort(self, revenue_data: pd.DataFrame,
                                   cohort_period: str, cohort_type: str) -> Optional[RevenueCohort]:
        """Build a single revenue cohort."""
        try:
            # Filter users for this cohort period
            cohort_users = self._filter_users_by_period(
                revenue_data, cohort_period, cohort_type, 'first_purchase_date'
            )
            
            if cohort_users.empty:
                return None
            
            # Calculate revenue metrics
            total_revenue = cohort_users['total_revenue'].sum()
            avg_revenue_per_user = cohort_users['total_revenue'].mean()
            paying_users = len(cohort_users)
            
            # Calculate revenue by period (would need time-series revenue data)
            revenue_by_period = {cohort_period: total_revenue}
            
            # Calculate LTV segments
            ltv_ranges = [(0, 50), (50, 200), (200, 500), (500, float('inf'))]
            ltv_segments = {}
            
            for low, high in ltv_ranges:
                count = len(cohort_users[
                    (cohort_users['total_revenue'] >= low) & 
                    (cohort_users['total_revenue'] < high)
                ])
                ltv_segments[f"${low}-${high if high != float('inf') else '∞'}"] = count
            
            # Revenue retention (placeholder - would need time-series data)
            revenue_retention = {cohort_period: 1.0}
            
            cohort = RevenueCohort(
                cohort_id=f"revenue_{cohort_type}_{cohort_period}",
                cohort_period=cohort_period,
                cohort_size=len(cohort_users),
                total_revenue=total_revenue,
                avg_revenue_per_user=avg_revenue_per_user,
                revenue_by_period=revenue_by_period,
                ltv_segments=ltv_segments,
                revenue_retention=revenue_retention,
                paying_users=paying_users,
                conversion_to_paid=1.0  # All users in revenue cohort are paying
            )
            
            return cohort
            
        except Exception as e:
            logger.error(f"Single revenue cohort building failed: {e}")
            return None
    
    def _build_engagement_cohorts(self, behavioral_data: pd.DataFrame) -> List[BehavioralCohort]:
        """Build engagement-based behavioral cohorts."""
        cohorts = []
        
        try:
            if behavioral_data.empty:
                return cohorts
            
            # Define engagement levels based on activity
            behavioral_data['engagement_score'] = (
                behavioral_data['total_events'] * 0.3 +
                behavioral_data['session_count'] * 0.4 +
                behavioral_data['unique_event_types'] * 0.3
            )
            
            # Normalize engagement score
            max_score = behavioral_data['engagement_score'].max()
            if max_score > 0:
                behavioral_data['engagement_score'] = behavioral_data['engagement_score'] / max_score
            
            # Define engagement cohorts
            engagement_ranges = [
                (0.8, 1.0, 'High Engagement'),
                (0.5, 0.8, 'Medium Engagement'),
                (0.2, 0.5, 'Low Engagement'),
                (0.0, 0.2, 'Minimal Engagement')
            ]
            
            for low, high, name in engagement_ranges:
                cohort_users = behavioral_data[
                    (behavioral_data['engagement_score'] >= low) & 
                    (behavioral_data['engagement_score'] < high)
                ]
                
                if len(cohort_users) > 0:
                    # Calculate average metrics
                    avg_metrics = {
                        'avg_total_events': cohort_users['total_events'].mean(),
                        'avg_session_count': cohort_users['session_count'].mean(),
                        'avg_unique_event_types': cohort_users['unique_event_types'].mean(),
                        'avg_activity_span': cohort_users['activity_span_days'].mean(),
                        'conversion_rate': (cohort_users['conversions'] > 0).mean()
                    }
                    
                    # Define behaviors
                    defining_behaviors = [
                        f"Engagement score: {low:.1f}-{high:.1f}",
                        f"Avg events: {avg_metrics['avg_total_events']:.1f}",
                        f"Avg sessions: {avg_metrics['avg_session_count']:.1f}"
                    ]
                    
                    cohort = BehavioralCohort(
                        cohort_id=f"engagement_{name.lower().replace(' ', '_')}",
                        cohort_name=name,
                        cohort_type='engagement_based',
                        user_count=len(cohort_users),
                        defining_behaviors=defining_behaviors,
                        avg_metrics=avg_metrics,
                        retention_patterns={},  # Would need time-series data
                        conversion_patterns={'conversion_rate': avg_metrics['conversion_rate']},
                        lifecycle_progression={}  # Would need lifecycle data
                    )
                    
                    cohorts.append(cohort)
            
            return cohorts
            
        except Exception as e:
            logger.error(f"Engagement cohort building failed: {e}")
            return []
    
    def _build_feature_usage_cohorts(self, behavioral_data: pd.DataFrame) -> List[BehavioralCohort]:
        """Build feature usage-based behavioral cohorts."""
        cohorts = []
        
        try:
            if behavioral_data.empty:
                return cohorts
            
            # Analyze feature usage patterns
            all_features = set()
            for _, user in behavioral_data.iterrows():
                if isinstance(user['feature_usage'], dict):
                    all_features.update(user['feature_usage'].keys())
            
            # Define feature-based cohorts
            if 'page_view' in all_features:
                # Content consumers
                content_users = behavioral_data[
                    behavioral_data['feature_usage'].apply(
                        lambda x: isinstance(x, dict) and x.get('page_view', 0) > 10
                    )
                ]
                
                if len(content_users) > 0:
                    cohort = self._create_feature_cohort(
                        content_users, 'content_consumers', 'High Content Consumption'
                    )
                    cohorts.append(cohort)
            
            # Power users (high feature diversity)
            power_users = behavioral_data[behavioral_data['unique_event_types'] >= 5]
            if len(power_users) > 0:
                cohort = self._create_feature_cohort(
                    power_users, 'power_users', 'Power Users'
                )
                cohorts.append(cohort)
            
            # Basic users (low feature diversity)
            basic_users = behavioral_data[behavioral_data['unique_event_types'] <= 2]
            if len(basic_users) > 0:
                cohort = self._create_feature_cohort(
                    basic_users, 'basic_users', 'Basic Users'
                )
                cohorts.append(cohort)
            
            return cohorts
            
        except Exception as e:
            logger.error(f"Feature usage cohort building failed: {e}")
            return []
    
    def _build_journey_cohorts(self, behavioral_data: pd.DataFrame) -> List[BehavioralCohort]:
        """Build journey-based behavioral cohorts."""
        cohorts = []
        
        try:
            if behavioral_data.empty:
                return cohorts
            
            # Journey-based segmentation
            # Quick converters
            quick_converters = behavioral_data[
                (behavioral_data['conversions'] > 0) & 
                (behavioral_data['activity_span_days'] <= 7)
            ]
            
            if len(quick_converters) > 0:
                cohort = self._create_journey_cohort(
                    quick_converters, 'quick_converters', 'Quick Converters'
                )
                cohorts.append(cohort)
            
            # Long-term researchers
            researchers = behavioral_data[
                (behavioral_data['activity_span_days'] > 30) & 
                (behavioral_data['session_count'] > 10) &
                (behavioral_data['conversions'] == 0)
            ]
            
            if len(researchers) > 0:
                cohort = self._create_journey_cohort(
                    researchers, 'researchers', 'Long-term Researchers'
                )
                cohorts.append(cohort)
            
            # One-time visitors
            one_time_visitors = behavioral_data[
                (behavioral_data['session_count'] == 1) & 
                (behavioral_data['total_events'] <= 5)
            ]
            
            if len(one_time_visitors) > 0:
                cohort = self._create_journey_cohort(
                    one_time_visitors, 'one_time_visitors', 'One-time Visitors'
                )
                cohorts.append(cohort)
            
            return cohorts
            
        except Exception as e:
            logger.error(f"Journey cohort building failed: {e}")
            return []
    
    def _create_feature_cohort(self, cohort_users: pd.DataFrame, 
                              cohort_id: str, cohort_name: str) -> BehavioralCohort:
        """Create a feature-based cohort."""
        avg_metrics = {
            'avg_total_events': cohort_users['total_events'].mean(),
            'avg_session_count': cohort_users['session_count'].mean(),
            'avg_unique_event_types': cohort_users['unique_event_types'].mean(),
            'conversion_rate': (cohort_users['conversions'] > 0).mean()
        }
        
        defining_behaviors = [
            f"Avg unique features: {avg_metrics['avg_unique_event_types']:.1f}",
            f"Avg total events: {avg_metrics['avg_total_events']:.1f}",
            f"Conversion rate: {avg_metrics['conversion_rate']:.2%}"
        ]
        
        return BehavioralCohort(
            cohort_id=cohort_id,
            cohort_name=cohort_name,
            cohort_type='feature_based',
            user_count=len(cohort_users),
            defining_behaviors=defining_behaviors,
            avg_metrics=avg_metrics,
            retention_patterns={},
            conversion_patterns={'conversion_rate': avg_metrics['conversion_rate']},
            lifecycle_progression={}
        )
    
    def _create_journey_cohort(self, cohort_users: pd.DataFrame, 
                              cohort_id: str, cohort_name: str) -> BehavioralCohort:
        """Create a journey-based cohort."""
        avg_metrics = {
            'avg_activity_span': cohort_users['activity_span_days'].mean(),
            'avg_session_count': cohort_users['session_count'].mean(),
            'avg_total_events': cohort_users['total_events'].mean(),
            'conversion_rate': (cohort_users['conversions'] > 0).mean()
        }
        
        defining_behaviors = [
            f"Avg journey length: {avg_metrics['avg_activity_span']:.1f} days",
            f"Avg sessions: {avg_metrics['avg_session_count']:.1f}",
            f"Conversion rate: {avg_metrics['conversion_rate']:.2%}"
        ]
        
        return BehavioralCohort(
            cohort_id=cohort_id,
            cohort_name=cohort_name,
            cohort_type='journey_based',
            user_count=len(cohort_users),
            defining_behaviors=defining_behaviors,
            avg_metrics=avg_metrics,
            retention_patterns={},
            conversion_patterns={'conversion_rate': avg_metrics['conversion_rate']},
            lifecycle_progression={}
        )
    
    def _filter_users_by_period(self, data_df: pd.DataFrame, cohort_period: str,
                               cohort_type: str, date_column: str) -> pd.DataFrame:
        """Filter users by cohort period."""
        try:
            data_df[date_column] = pd.to_datetime(data_df[date_column])
            
            if cohort_type == 'daily':
                target_date = pd.to_datetime(cohort_period).date()
                return data_df[data_df[date_column].dt.date == target_date]
            
            elif cohort_type == 'weekly':
                year, week = cohort_period.split('-W')
                year, week = int(year), int(week)
                
                # Filter by ISO week
                return data_df[
                    (data_df[date_column].dt.isocalendar().year == year) &
                    (data_df[date_column].dt.isocalendar().week == week)
                ]
            
            elif cohort_type == 'monthly':
                year, month = cohort_period.split('-')
                year, month = int(year), int(month)
                
                return data_df[
                    (data_df[date_column].dt.year == year) &
                    (data_df[date_column].dt.month == month)
                ]
            
            elif cohort_type == 'quarterly':
                year, quarter = cohort_period.split('-Q')
                year, quarter = int(year), int(quarter)
                
                quarter_months = {
                    1: [1, 2, 3],
                    2: [4, 5, 6],
                    3: [7, 8, 9],
                    4: [10, 11, 12]
                }
                
                return data_df[
                    (data_df[date_column].dt.year == year) &
                    (data_df[date_column].dt.month.isin(quarter_months[quarter]))
                ]
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"User filtering by period failed: {e}")
            return pd.DataFrame()
    
    def _calculate_retention_rates(self, cohort_users: pd.DataFrame,
                                  cohort_period: str, cohort_type: str) -> Dict[str, float]:
        """Calculate retention rates for cohort (placeholder implementation)."""
        try:
            retention_calculations.inc()
            
            # This is a simplified calculation
            # In practice, you'd need actual activity data across multiple periods
            
            retention_rates = {}
            
            # Calculate retention based on activity span
            avg_activity_span = cohort_users['days_active'].mean()
            
            if cohort_type == 'daily':
                periods = ['Day 1', 'Day 7', 'Day 30']
                base_retention = [1.0, 0.7, 0.4]
            elif cohort_type == 'weekly':
                periods = ['Week 1', 'Week 4', 'Week 12']
                base_retention = [1.0, 0.6, 0.3]
            elif cohort_type == 'monthly':
                periods = ['Month 1', 'Month 3', 'Month 6', 'Month 12']
                base_retention = [1.0, 0.5, 0.3, 0.2]
            else:  # quarterly
                periods = ['Q1', 'Q2', 'Q3', 'Q4']
                base_retention = [1.0, 0.4, 0.25, 0.15]
            
            # Adjust retention based on cohort characteristics
            engagement_factor = min(avg_activity_span / 30, 1.0)  # Normalize to 30 days
            
            for i, period in enumerate(periods):
                adjusted_retention = base_retention[i] * (0.5 + 0.5 * engagement_factor)
                retention_rates[period] = min(adjusted_retention, 1.0)
            
            return retention_rates
            
        except Exception as e:
            logger.error(f"Retention rate calculation failed: {e}")
            return {}

class CohortAnalyzer:
    """Main cohort analysis coordinator."""
    
    def __init__(self):
        self.cohort_builder = CohortBuilder()
        
        # Cache for analysis results
        self.latest_retention_cohorts: List[RetentionCohort] = []
        self.latest_revenue_cohorts: List[RevenueCohort] = []
        self.latest_behavioral_cohorts: List[BehavioralCohort] = []
        
    async def analyze_cohorts(self, events_df: pd.DataFrame,
                            cohort_types: List[str] = None,
                            time_periods: List[str] = None) -> Dict[str, Any]:
        """Perform comprehensive cohort analysis."""
        try:
            if cohort_types is None:
                cohort_types = ['retention', 'revenue', 'behavioral']
            
            if time_periods is None:
                time_periods = ['monthly']
            
            analysis_results = {}
            
            # Retention cohort analysis
            if 'retention' in cohort_types:
                logger.info("Performing retention cohort analysis...")
                retention_results = {}
                
                for period in time_periods:
                    cohorts = self.cohort_builder.build_retention_cohorts(events_df, period)
                    retention_results[period] = [asdict(cohort) for cohort in cohorts]
                    self.latest_retention_cohorts.extend(cohorts)
                
                analysis_results['retention_cohorts'] = retention_results
            
            # Revenue cohort analysis
            if 'revenue' in cohort_types:
                logger.info("Performing revenue cohort analysis...")
                revenue_results = {}
                
                for period in time_periods:
                    cohorts = self.cohort_builder.build_revenue_cohorts(events_df, None, period)
                    revenue_results[period] = [asdict(cohort) for cohort in cohorts]
                    self.latest_revenue_cohorts.extend(cohorts)
                
                analysis_results['revenue_cohorts'] = revenue_results
            
            # Behavioral cohort analysis
            if 'behavioral' in cohort_types:
                logger.info("Performing behavioral cohort analysis...")
                cohorts = self.cohort_builder.build_behavioral_cohorts(events_df)
                analysis_results['behavioral_cohorts'] = [asdict(cohort) for cohort in cohorts]
                self.latest_behavioral_cohorts = cohorts
            
            # Generate cohort insights
            analysis_results['cohort_insights'] = self._generate_cohort_insights()
            
            # Calculate cross-cohort metrics
            analysis_results['cross_cohort_metrics'] = self._calculate_cross_cohort_metrics()
            
            analysis_results['analysis_timestamp'] = datetime.now().isoformat()
            
            logger.info("Cohort analysis completed successfully")
            return analysis_results
            
        except Exception as e:
            logger.error(f"Cohort analysis failed: {e}")
            return {'error': str(e)}
    
    def compare_cohorts(self, cohort_ids: List[str], 
                       comparison_metrics: List[str] = None) -> CohortComparison:
        """Compare multiple cohorts across specified metrics."""
        try:
            if comparison_metrics is None:
                comparison_metrics = ['retention_rate', 'conversion_rate', 'avg_revenue']
            
            # Find cohorts to compare
            cohorts_to_compare = []
            
            # Search in retention cohorts
            for cohort in self.latest_retention_cohorts:
                if cohort.cohort_id in cohort_ids:
                    cohorts_to_compare.append(('retention', cohort))
            
            # Search in revenue cohorts
            for cohort in self.latest_revenue_cohorts:
                if cohort.cohort_id in cohort_ids:
                    cohorts_to_compare.append(('revenue', cohort))
            
            # Search in behavioral cohorts
            for cohort in self.latest_behavioral_cohorts:
                if cohort.cohort_id in cohort_ids:
                    cohorts_to_compare.append(('behavioral', cohort))
            
            if len(cohorts_to_compare) < 2:
                raise ValueError("At least 2 cohorts required for comparison")
            
            # Perform comparison
            metrics_comparison = {}
            statistical_significance = {}
            
            for metric in comparison_metrics:
                metric_values = {}
                
                for cohort_type, cohort in cohorts_to_compare:
                    value = self._extract_metric_value(cohort, metric)
                    metric_values[cohort.cohort_id] = value
                
                metrics_comparison[metric] = metric_values
                
                # Calculate statistical significance (simplified)
                values = list(metric_values.values())
                if len(values) >= 2 and all(v is not None for v in values):
                    try:
                        stat, p_value = stats.kruskal(*[[v] for v in values])
                        statistical_significance[metric] = {
                            'statistic': stat,
                            'p_value': p_value,
                            'significant': p_value < 0.05
                        }
                    except Exception:
                        statistical_significance[metric] = {'error': 'Cannot calculate significance'}
            
            # Generate insights and recommendations
            insights = self._generate_comparison_insights(metrics_comparison, statistical_significance)
            recommendations = self._generate_comparison_recommendations(metrics_comparison)
            
            return CohortComparison(
                comparison_id=f"comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                cohort_ids=cohort_ids,
                comparison_type='multi_cohort',
                metrics_comparison=metrics_comparison,
                statistical_significance=statistical_significance,
                insights=insights,
                recommendations=recommendations
            )
            
        except Exception as e:
            logger.error(f"Cohort comparison failed: {e}")
            return CohortComparison(
                comparison_id='error',
                cohort_ids=cohort_ids,
                comparison_type='error',
                metrics_comparison={},
                statistical_significance={},
                insights=[f"Comparison failed: {str(e)}"],
                recommendations=[]
            )
    
    def _generate_cohort_insights(self) -> List[Dict[str, Any]]:
        """Generate insights from cohort analysis."""
        insights = []
        
        try:
            # Retention insights
            if self.latest_retention_cohorts:
                avg_retention = np.mean([
                    list(cohort.retention_rates.values())[-1] 
                    for cohort in self.latest_retention_cohorts 
                    if cohort.retention_rates
                ])
                
                insights.append({
                    'type': 'retention_insight',
                    'title': 'Retention Performance',
                    'description': f'Average long-term retention rate: {avg_retention:.1%}',
                    'priority': 'high' if avg_retention < 0.2 else 'medium'
                })
            
            # Revenue insights
            if self.latest_revenue_cohorts:
                high_value_cohorts = sum(1 for cohort in self.latest_revenue_cohorts 
                                       if cohort.avg_revenue_per_user > 100)
                
                insights.append({
                    'type': 'revenue_insight',
                    'title': 'High-Value Cohorts',
                    'description': f'{high_value_cohorts} cohorts show high revenue per user',
                    'priority': 'high'
                })
            
            # Behavioral insights
            if self.latest_behavioral_cohorts:
                power_user_cohort = next((c for c in self.latest_behavioral_cohorts 
                                        if 'power' in c.cohort_name.lower()), None)
                
                if power_user_cohort:
                    insights.append({
                        'type': 'behavioral_insight',
                        'title': 'Power User Segment',
                        'description': f'Power users represent {power_user_cohort.user_count} users with high feature usage',
                        'priority': 'high'
                    })
            
            return insights
            
        except Exception as e:
            logger.error(f"Cohort insights generation failed: {e}")
            return []
    
    def _calculate_cross_cohort_metrics(self) -> Dict[str, Any]:
        """Calculate metrics across all cohorts."""
        try:
            metrics = {}
            
            # Retention metrics
            if self.latest_retention_cohorts:
                retention_rates = []
                for cohort in self.latest_retention_cohorts:
                    if cohort.retention_rates:
                        retention_rates.append(list(cohort.retention_rates.values())[-1])
                
                if retention_rates:
                    metrics['avg_retention_rate'] = np.mean(retention_rates)
                    metrics['retention_rate_std'] = np.std(retention_rates)
            
            # Revenue metrics
            if self.latest_revenue_cohorts:
                arpu_values = [cohort.avg_revenue_per_user for cohort in self.latest_revenue_cohorts]
                metrics['avg_arpu'] = np.mean(arpu_values)
                metrics['arpu_std'] = np.std(arpu_values)
            
            # Behavioral metrics
            if self.latest_behavioral_cohorts:
                cohort_sizes = [cohort.user_count for cohort in self.latest_behavioral_cohorts]
                metrics['avg_behavioral_cohort_size'] = np.mean(cohort_sizes)
                metrics['behavioral_cohort_count'] = len(self.latest_behavioral_cohorts)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Cross-cohort metrics calculation failed: {e}")
            return {}
    
    def _extract_metric_value(self, cohort: Any, metric: str) -> Optional[float]:
        """Extract metric value from cohort object."""
        try:
            if hasattr(cohort, 'retention_rates') and metric == 'retention_rate':
                retention_rates = cohort.retention_rates
                return list(retention_rates.values())[-1] if retention_rates else None
            
            elif hasattr(cohort, 'conversion_rate') and metric == 'conversion_rate':
                return cohort.conversion_rate
            
            elif hasattr(cohort, 'avg_revenue_per_user') and metric == 'avg_revenue':
                return cohort.avg_revenue_per_user
            
            elif hasattr(cohort, 'avg_metrics') and metric in cohort.avg_metrics:
                return cohort.avg_metrics[metric]
            
            return None
            
        except Exception as e:
            logger.error(f"Metric extraction failed: {e}")
            return None
    
    def _generate_comparison_insights(self, metrics_comparison: Dict[str, Dict[str, float]],
                                    statistical_significance: Dict[str, Dict[str, Any]]) -> List[str]:
        """Generate insights from cohort comparison."""
        insights = []
        
        try:
            for metric, values in metrics_comparison.items():
                if len(values) >= 2:
                    max_cohort = max(values.keys(), key=lambda k: values[k] or 0)
                    min_cohort = min(values.keys(), key=lambda k: values[k] or float('inf'))
                    
                    max_value = values[max_cohort]
                    min_value = values[min_cohort]
                    
                    if max_value and min_value and max_value > min_value:
                        improvement = ((max_value - min_value) / min_value) * 100
                        insights.append(
                            f"{max_cohort} outperforms {min_cohort} in {metric} by {improvement:.1f}%"
                        )
                    
                    # Add significance insight
                    if metric in statistical_significance:
                        sig_info = statistical_significance[metric]
                        if sig_info.get('significant'):
                            insights.append(f"Difference in {metric} is statistically significant")
            
            return insights
            
        except Exception as e:
            logger.error(f"Comparison insights generation failed: {e}")
            return []
    
    def _generate_comparison_recommendations(self, metrics_comparison: Dict[str, Dict[str, float]]) -> List[str]:
        """Generate recommendations from cohort comparison."""
        recommendations = []
        
        try:
            # Find best performing cohorts
            best_performers = {}
            
            for metric, values in metrics_comparison.items():
                if values:
                    best_cohort = max(values.keys(), key=lambda k: values[k] or 0)
                    best_performers[metric] = best_cohort
            
            # Generate recommendations
            if best_performers:
                recommendations.append("Focus acquisition efforts on segments similar to top-performing cohorts")
                recommendations.append("Analyze characteristics of high-performing cohorts for optimization insights")
                recommendations.append("Implement retention strategies from successful cohorts across all segments")
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Comparison recommendations generation failed: {e}")
            return []
    
    def get_cohort_details(self, cohort_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific cohort."""
        # Search across all cohort types
        all_cohorts = (
            self.latest_retention_cohorts + 
            self.latest_revenue_cohorts + 
            self.latest_behavioral_cohorts
        )
        
        for cohort in all_cohorts:
            if cohort.cohort_id == cohort_id:
                return asdict(cohort)
        
        return None
