"""
User Behavior Analysis System

Provides comprehensive analysis of user behavior patterns, engagement metrics,
behavioral segmentation, journey analysis, and user lifecycle management.
"""

import logging
import asyncio
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple, Union, Set
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from collections import defaultdict, Counter, deque
import json
import pickle
from pathlib import Path

# Analysis libraries
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
from scipy import stats
from scipy.spatial.distance import pdist, squareform

# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge

# Initialize metrics
behavior_analyses = Counter('analytics_behavior_analyses_total', 'Behavior analyses performed', ['analysis_type'])
user_segments_identified = Gauge('analytics_user_segments_count', 'Number of user segments identified')
journey_analyses = Counter('analytics_journey_analyses_total', 'Journey analyses performed')
engagement_scores_computed = Counter('analytics_engagement_scores_total', 'Engagement scores computed')

logger = logging.getLogger(__name__)

@dataclass
class UserSession:
    """User session data structure."""
    session_id: str
    user_id: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_seconds: Optional[int]
    page_views: int
    events: List[Dict[str, Any]]
    entry_page: Optional[str]
    exit_page: Optional[str]
    conversion_events: List[str]
    device_type: Optional[str]
    traffic_source: Optional[str]

@dataclass
class UserProfile:
    """Comprehensive user profile."""
    user_id: str
    first_seen: datetime
    last_seen: datetime
    total_sessions: int
    total_time_spent: int  # seconds
    total_page_views: int
    total_events: int
    conversion_count: int
    avg_session_duration: float
    bounce_rate: float
    engagement_score: float
    segment: Optional[str]
    lifecycle_stage: str
    preferences: Dict[str, Any]
    behavioral_features: Dict[str, float]

@dataclass
class BehavioralSegment:
    """User behavioral segment."""
    segment_id: str
    segment_name: str
    description: str
    user_count: int
    characteristics: Dict[str, Any]
    avg_metrics: Dict[str, float]
    behavior_patterns: List[str]
    recommendations: List[str]

@dataclass
class UserJourney:
    """User journey analysis result."""
    journey_id: str
    user_id: str
    journey_type: str  # conversion, exploration, research, etc.
    start_time: datetime
    end_time: datetime
    touchpoints: List[Dict[str, Any]]
    conversion_achieved: bool
    journey_value: float
    drop_off_points: List[str]
    optimization_opportunities: List[str]

@dataclass
class EngagementMetrics:
    """User engagement metrics."""
    user_id: str
    time_period: Tuple[datetime, datetime]
    session_frequency: float
    avg_session_duration: float
    page_depth: float
    interaction_rate: float
    return_visitor_rate: float
    content_consumption_rate: float
    social_sharing_rate: float
    overall_engagement_score: float

class SessionAnalyzer:
    """Analyzes user sessions for behavior patterns."""
    
    def __init__(self):
        self.session_threshold_minutes = 30  # Session timeout
        
    def process_events_to_sessions(self, events_df: pd.DataFrame) -> List[UserSession]:
        """Convert raw events into user sessions."""
        try:
            sessions = []
            
            # Group events by user
            for user_id, user_events in events_df.groupby('user_id'):
                user_events = user_events.sort_values('timestamp')
                user_sessions = self._extract_user_sessions(user_id, user_events)
                sessions.extend(user_sessions)
            
            logger.info(f"Processed {len(events_df)} events into {len(sessions)} sessions")
            return sessions
            
        except Exception as e:
            logger.error(f"Session processing failed: {e}")
            return []
    
    def _extract_user_sessions(self, user_id: str, events: pd.DataFrame) -> List[UserSession]:
        """Extract sessions for a single user."""
        sessions = []
        current_session_events = []
        session_start = None
        session_id_counter = 0
        
        threshold = timedelta(minutes=self.session_threshold_minutes)
        
        for _, event in events.iterrows():
            event_time = pd.to_datetime(event['timestamp'])
            
            if session_start is None:
                # Start new session
                session_start = event_time
                current_session_events = [event.to_dict()]
                session_id_counter += 1
            else:
                # Check if event belongs to current session
                time_gap = event_time - session_start
                last_event_time = pd.to_datetime(current_session_events[-1]['timestamp'])
                time_since_last = event_time - last_event_time
                
                if time_since_last <= threshold:
                    # Add to current session
                    current_session_events.append(event.to_dict())
                else:
                    # End current session and start new one
                    session = self._create_session_object(
                        user_id, f"{user_id}_{session_id_counter}", 
                        current_session_events
                    )
                    sessions.append(session)
                    
                    # Start new session
                    session_start = event_time
                    current_session_events = [event.to_dict()]
                    session_id_counter += 1
        
        # Don't forget the last session
        if current_session_events:
            session = self._create_session_object(
                user_id, f"{user_id}_{session_id_counter}", 
                current_session_events
            )
            sessions.append(session)
        
        return sessions
    
    def _create_session_object(self, user_id: str, session_id: str, 
                              events: List[Dict]) -> UserSession:
        """Create UserSession object from event list."""
        try:
            start_time = pd.to_datetime(events[0]['timestamp'])
            end_time = pd.to_datetime(events[-1]['timestamp'])
            duration_seconds = int((end_time - start_time).total_seconds())
            
            # Extract page views
            page_views = len([e for e in events if e.get('event_type') == 'page_view'])
            
            # Extract entry and exit pages
            page_view_events = [e for e in events if e.get('event_type') == 'page_view']
            entry_page = page_view_events[0].get('page_url') if page_view_events else None
            exit_page = page_view_events[-1].get('page_url') if page_view_events else None
            
            # Extract conversion events
            conversion_events = [
                e.get('event_type') for e in events 
                if e.get('event_type') in ['purchase', 'signup', 'conversion', 'subscribe']
            ]
            
            # Extract device and traffic source from first event
            first_event = events[0]
            device_type = first_event.get('device_type')
            traffic_source = first_event.get('traffic_source') or first_event.get('referrer')
            
            return UserSession(
                session_id=session_id,
                user_id=user_id,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration_seconds,
                page_views=page_views,
                events=events,
                entry_page=entry_page,
                exit_page=exit_page,
                conversion_events=conversion_events,
                device_type=device_type,
                traffic_source=traffic_source
            )
            
        except Exception as e:
            logger.error(f"Session object creation failed: {e}")
            return UserSession(
                session_id=session_id,
                user_id=user_id,
                start_time=datetime.now(),
                end_time=None,
                duration_seconds=0,
                page_views=0,
                events=events,
                entry_page=None,
                exit_page=None,
                conversion_events=[],
                device_type=None,
                traffic_source=None
            )

class UserProfileBuilder:
    """Builds comprehensive user profiles from session data."""
    
    def __init__(self):
        self.engagement_weights = {
            'session_frequency': 0.25,
            'avg_session_duration': 0.20,
            'page_depth': 0.15,
            'conversion_rate': 0.25,
            'recency': 0.15
        }
    
    def build_user_profiles(self, sessions: List[UserSession]) -> List[UserProfile]:
        """Build user profiles from session data."""
        try:
            user_profiles = []
            
            # Group sessions by user
            user_sessions = defaultdict(list)
            for session in sessions:
                user_sessions[session.user_id].append(session)
            
            for user_id, user_session_list in user_sessions.items():
                profile = self._build_single_user_profile(user_id, user_session_list)
                user_profiles.append(profile)
            
            logger.info(f"Built profiles for {len(user_profiles)} users")
            return user_profiles
            
        except Exception as e:
            logger.error(f"User profile building failed: {e}")
            return []
    
    def _build_single_user_profile(self, user_id: str, 
                                  sessions: List[UserSession]) -> UserProfile:
        """Build profile for a single user."""
        try:
            # Basic metrics
            first_seen = min(s.start_time for s in sessions)
            last_seen = max(s.end_time or s.start_time for s in sessions)
            total_sessions = len(sessions)
            
            # Time and activity metrics
            total_time_spent = sum(s.duration_seconds or 0 for s in sessions)
            total_page_views = sum(s.page_views for s in sessions)
            total_events = sum(len(s.events) for s in sessions)
            
            # Conversion metrics
            conversion_count = sum(len(s.conversion_events) for s in sessions)
            
            # Calculated metrics
            avg_session_duration = total_time_spent / total_sessions if total_sessions > 0 else 0
            
            # Bounce rate (sessions with only one page view)
            single_page_sessions = sum(1 for s in sessions if s.page_views <= 1)
            bounce_rate = single_page_sessions / total_sessions if total_sessions > 0 else 0
            
            # Calculate engagement score
            engagement_score = self._calculate_engagement_score(sessions)
            
            # Determine lifecycle stage
            lifecycle_stage = self._determine_lifecycle_stage(
                first_seen, last_seen, total_sessions, conversion_count
            )
            
            # Extract preferences
            preferences = self._extract_user_preferences(sessions)
            
            # Calculate behavioral features
            behavioral_features = self._calculate_behavioral_features(sessions)
            
            return UserProfile(
                user_id=user_id,
                first_seen=first_seen,
                last_seen=last_seen,
                total_sessions=total_sessions,
                total_time_spent=total_time_spent,
                total_page_views=total_page_views,
                total_events=total_events,
                conversion_count=conversion_count,
                avg_session_duration=avg_session_duration,
                bounce_rate=bounce_rate,
                engagement_score=engagement_score,
                segment=None,  # Will be assigned by segmentation
                lifecycle_stage=lifecycle_stage,
                preferences=preferences,
                behavioral_features=behavioral_features
            )
            
        except Exception as e:
            logger.error(f"Single user profile building failed: {e}")
            return UserProfile(
                user_id=user_id,
                first_seen=datetime.now(),
                last_seen=datetime.now(),
                total_sessions=0,
                total_time_spent=0,
                total_page_views=0,
                total_events=0,
                conversion_count=0,
                avg_session_duration=0.0,
                bounce_rate=0.0,
                engagement_score=0.0,
                segment=None,
                lifecycle_stage="new",
                preferences={},
                behavioral_features={}
            )
    
    def _calculate_engagement_score(self, sessions: List[UserSession]) -> float:
        """Calculate user engagement score."""
        try:
            if not sessions:
                return 0.0
            
            # Session frequency (sessions per week)
            time_span = (max(s.end_time or s.start_time for s in sessions) - 
                        min(s.start_time for s in sessions))
            weeks = max(time_span.days / 7, 1)
            session_frequency = len(sessions) / weeks
            
            # Average session duration (normalized)
            avg_duration = np.mean([s.duration_seconds or 0 for s in sessions])
            duration_score = min(avg_duration / 600, 1.0)  # Normalize to 10 minutes max
            
            # Page depth
            avg_page_views = np.mean([s.page_views for s in sessions])
            page_depth_score = min(avg_page_views / 10, 1.0)  # Normalize to 10 pages max
            
            # Conversion rate
            total_conversions = sum(len(s.conversion_events) for s in sessions)
            conversion_rate = total_conversions / len(sessions)
            conversion_score = min(conversion_rate, 1.0)
            
            # Recency (how recently user was active)
            days_since_last = (datetime.now() - max(s.end_time or s.start_time for s in sessions)).days
            recency_score = max(1 - days_since_last / 30, 0)  # Decays over 30 days
            
            # Weighted combination
            engagement_score = (
                self.engagement_weights['session_frequency'] * min(session_frequency / 2, 1.0) +
                self.engagement_weights['avg_session_duration'] * duration_score +
                self.engagement_weights['page_depth'] * page_depth_score +
                self.engagement_weights['conversion_rate'] * conversion_score +
                self.engagement_weights['recency'] * recency_score
            )
            
            return min(max(engagement_score, 0.0), 1.0)
            
        except Exception as e:
            logger.error(f"Engagement score calculation failed: {e}")
            return 0.0
    
    def _determine_lifecycle_stage(self, first_seen: datetime, last_seen: datetime,
                                  total_sessions: int, conversion_count: int) -> str:
        """Determine user lifecycle stage."""
        try:
            days_since_first = (datetime.now() - first_seen).days
            days_since_last = (datetime.now() - last_seen).days
            
            # New users (first seen within 7 days)
            if days_since_first <= 7:
                return "new"
            
            # Churned users (not seen in 30+ days)
            if days_since_last >= 30:
                if conversion_count > 0:
                    return "churned_customer"
                else:
                    return "churned_prospect"
            
            # Active users
            if days_since_last <= 7:
                if conversion_count > 0:
                    if conversion_count >= 3:
                        return "loyal_customer"
                    else:
                        return "active_customer"
                else:
                    if total_sessions >= 5:
                        return "engaged_prospect"
                    else:
                        return "active_prospect"
            
            # At-risk users (7-30 days since last seen)
            if conversion_count > 0:
                return "at_risk_customer"
            else:
                return "at_risk_prospect"
                
        except Exception as e:
            logger.error(f"Lifecycle stage determination failed: {e}")
            return "unknown"
    
    def _extract_user_preferences(self, sessions: List[UserSession]) -> Dict[str, Any]:
        """Extract user preferences from session data."""
        try:
            preferences = {}
            
            # Device preference
            devices = [s.device_type for s in sessions if s.device_type]
            if devices:
                device_counts = Counter(devices)
                preferences['preferred_device'] = device_counts.most_common(1)[0][0]
            
            # Traffic source preference
            sources = [s.traffic_source for s in sessions if s.traffic_source]
            if sources:
                source_counts = Counter(sources)
                preferences['primary_traffic_source'] = source_counts.most_common(1)[0][0]
            
            # Time of day preference
            hours = [s.start_time.hour for s in sessions]
            if hours:
                hour_counts = Counter(hours)
                preferences['preferred_hour'] = hour_counts.most_common(1)[0][0]
            
            # Day of week preference
            weekdays = [s.start_time.weekday() for s in sessions]
            if weekdays:
                day_counts = Counter(weekdays)
                preferences['preferred_weekday'] = day_counts.most_common(1)[0][0]
            
            # Page preferences (most visited pages)
            all_pages = []
            for session in sessions:
                page_events = [e for e in session.events if e.get('event_type') == 'page_view']
                all_pages.extend([e.get('page_url') for e in page_events if e.get('page_url')])
            
            if all_pages:
                page_counts = Counter(all_pages)
                preferences['top_pages'] = [page for page, count in page_counts.most_common(5)]
            
            return preferences
            
        except Exception as e:
            logger.error(f"Preference extraction failed: {e}")
            return {}
    
    def _calculate_behavioral_features(self, sessions: List[UserSession]) -> Dict[str, float]:
        """Calculate behavioral features for ML models."""
        try:
            features = {}
            
            if not sessions:
                return features
            
            # Temporal features
            features['session_count'] = len(sessions)
            features['avg_session_duration'] = np.mean([s.duration_seconds or 0 for s in sessions])
            features['total_time_spent'] = sum(s.duration_seconds or 0 for s in sessions)
            
            # Engagement features
            features['avg_page_views'] = np.mean([s.page_views for s in sessions])
            features['total_page_views'] = sum(s.page_views for s in sessions)
            features['bounce_rate'] = sum(1 for s in sessions if s.page_views <= 1) / len(sessions)
            
            # Conversion features
            features['conversion_count'] = sum(len(s.conversion_events) for s in sessions)
            features['conversion_rate'] = features['conversion_count'] / len(sessions)
            
            # Frequency features
            time_span_days = (max(s.end_time or s.start_time for s in sessions) - 
                             min(s.start_time for s in sessions)).days + 1
            features['session_frequency'] = len(sessions) / time_span_days
            
            # Diversity features
            unique_pages = set()
            for session in sessions:
                page_events = [e for e in session.events if e.get('event_type') == 'page_view']
                unique_pages.update([e.get('page_url') for e in page_events if e.get('page_url')])
            features['page_diversity'] = len(unique_pages)
            
            # Recency feature
            days_since_last = (datetime.now() - max(s.end_time or s.start_time for s in sessions)).days
            features['recency_days'] = days_since_last
            
            # Consistency features
            session_durations = [s.duration_seconds or 0 for s in sessions]
            features['session_duration_std'] = np.std(session_durations)
            
            page_view_counts = [s.page_views for s in sessions]
            features['page_views_std'] = np.std(page_view_counts)
            
            return features
            
        except Exception as e:
            logger.error(f"Behavioral feature calculation failed: {e}")
            return {}

class BehavioralSegmentation:
    """Performs user behavioral segmentation using clustering."""
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.clusterer = None
        self.segment_profiles = {}
        
    def segment_users(self, user_profiles: List[UserProfile], 
                     n_segments: Optional[int] = None) -> List[BehavioralSegment]:
        """Segment users based on behavioral patterns."""
        try:
            behavior_analyses.labels(analysis_type='segmentation').inc()
            
            # Extract features for clustering
            feature_data, user_ids = self._extract_features_for_clustering(user_profiles)
            
            if len(feature_data) < 10:
                logger.warning("Insufficient data for segmentation")
                return []
            
            # Perform clustering
            if n_segments is None:
                n_segments = self._determine_optimal_segments(feature_data)
            
            cluster_labels = self._perform_clustering(feature_data, n_segments)
            
            # Assign segments to users
            for i, user_profile in enumerate(user_profiles):
                if i < len(cluster_labels):
                    user_profile.segment = f"segment_{cluster_labels[i]}"
            
            # Create segment profiles
            segments = self._create_segment_profiles(user_profiles, cluster_labels, n_segments)
            
            user_segments_identified.set(len(segments))
            logger.info(f"Created {len(segments)} user segments")
            
            return segments
            
        except Exception as e:
            logger.error(f"User segmentation failed: {e}")
            return []
    
    def _extract_features_for_clustering(self, user_profiles: List[UserProfile]) -> Tuple[np.ndarray, List[str]]:
        """Extract features for clustering."""
        features = []
        user_ids = []
        
        for profile in user_profiles:
            # Use behavioral features if available, otherwise calculate from profile
            if profile.behavioral_features:
                feature_vector = [
                    profile.behavioral_features.get('session_count', 0),
                    profile.behavioral_features.get('avg_session_duration', 0),
                    profile.behavioral_features.get('avg_page_views', 0),
                    profile.behavioral_features.get('conversion_rate', 0),
                    profile.behavioral_features.get('bounce_rate', 0),
                    profile.behavioral_features.get('session_frequency', 0),
                    profile.behavioral_features.get('recency_days', 0),
                    profile.engagement_score
                ]
            else:
                # Fallback to profile-level features
                time_span_days = max((profile.last_seen - profile.first_seen).days, 1)
                feature_vector = [
                    profile.total_sessions,
                    profile.avg_session_duration,
                    profile.total_page_views / max(profile.total_sessions, 1),
                    profile.conversion_count / max(profile.total_sessions, 1),
                    profile.bounce_rate,
                    profile.total_sessions / time_span_days,
                    (datetime.now() - profile.last_seen).days,
                    profile.engagement_score
                ]
            
            features.append(feature_vector)
            user_ids.append(profile.user_id)
        
        return np.array(features), user_ids
    
    def _determine_optimal_segments(self, feature_data: np.ndarray) -> int:
        """Determine optimal number of segments using elbow method."""
        try:
            # Normalize features
            normalized_features = self.scaler.fit_transform(feature_data)
            
            # Try different numbers of clusters
            max_clusters = min(10, len(feature_data) // 5)
            silhouette_scores = []
            
            for n_clusters in range(2, max_clusters + 1):
                try:
                    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
                    cluster_labels = kmeans.fit_predict(normalized_features)
                    
                    if len(set(cluster_labels)) > 1:  # Valid clustering
                        score = silhouette_score(normalized_features, cluster_labels)
                        silhouette_scores.append((n_clusters, score))
                except Exception:
                    continue
            
            if silhouette_scores:
                # Choose number of clusters with highest silhouette score
                best_n_clusters = max(silhouette_scores, key=lambda x: x[1])[0]
                return best_n_clusters
            else:
                return 3  # Default fallback
                
        except Exception as e:
            logger.error(f"Optimal segment determination failed: {e}")
            return 3
    
    def _perform_clustering(self, feature_data: np.ndarray, n_segments: int) -> np.ndarray:
        """Perform clustering with specified number of segments."""
        try:
            # Normalize features
            normalized_features = self.scaler.fit_transform(feature_data)
            
            # Perform K-means clustering
            self.clusterer = KMeans(n_clusters=n_segments, random_state=42, n_init=10)
            cluster_labels = self.clusterer.fit_predict(normalized_features)
            
            return cluster_labels
            
        except Exception as e:
            logger.error(f"Clustering failed: {e}")
            return np.zeros(len(feature_data))
    
    def _create_segment_profiles(self, user_profiles: List[UserProfile], 
                               cluster_labels: np.ndarray, n_segments: int) -> List[BehavioralSegment]:
        """Create behavioral segment profiles."""
        segments = []
        
        for segment_id in range(n_segments):
            # Get users in this segment
            segment_users = [
                profile for i, profile in enumerate(user_profiles)
                if i < len(cluster_labels) and cluster_labels[i] == segment_id
            ]
            
            if not segment_users:
                continue
            
            # Calculate segment characteristics
            characteristics = self._calculate_segment_characteristics(segment_users)
            avg_metrics = self._calculate_segment_metrics(segment_users)
            behavior_patterns = self._identify_behavior_patterns(segment_users)
            recommendations = self._generate_segment_recommendations(
                segment_id, characteristics, behavior_patterns
            )
            
            # Generate segment name and description
            segment_name, description = self._generate_segment_name_description(
                segment_id, characteristics, behavior_patterns
            )
            
            segment = BehavioralSegment(
                segment_id=f"segment_{segment_id}",
                segment_name=segment_name,
                description=description,
                user_count=len(segment_users),
                characteristics=characteristics,
                avg_metrics=avg_metrics,
                behavior_patterns=behavior_patterns,
                recommendations=recommendations
            )
            
            segments.append(segment)
        
        return segments
    
    def _calculate_segment_characteristics(self, segment_users: List[UserProfile]) -> Dict[str, Any]:
        """Calculate characteristics of a segment."""
        try:
            characteristics = {}
            
            # Basic demographics
            characteristics['user_count'] = len(segment_users)
            characteristics['avg_engagement_score'] = np.mean([u.engagement_score for u in segment_users])
            characteristics['avg_sessions'] = np.mean([u.total_sessions for u in segment_users])
            characteristics['avg_session_duration'] = np.mean([u.avg_session_duration for u in segment_users])
            characteristics['avg_bounce_rate'] = np.mean([u.bounce_rate for u in segment_users])
            characteristics['avg_conversion_count'] = np.mean([u.conversion_count for u in segment_users])
            
            # Lifecycle distribution
            lifecycle_distribution = Counter([u.lifecycle_stage for u in segment_users])
            characteristics['lifecycle_distribution'] = dict(lifecycle_distribution)
            characteristics['dominant_lifecycle'] = lifecycle_distribution.most_common(1)[0][0]
            
            # Device preferences
            devices = []
            for user in segment_users:
                if user.preferences.get('preferred_device'):
                    devices.append(user.preferences['preferred_device'])
            
            if devices:
                device_distribution = Counter(devices)
                characteristics['device_distribution'] = dict(device_distribution)
                characteristics['dominant_device'] = device_distribution.most_common(1)[0][0]
            
            # Recency analysis
            recency_days = [(datetime.now() - u.last_seen).days for u in segment_users]
            characteristics['avg_recency_days'] = np.mean(recency_days)
            characteristics['recency_distribution'] = {
                'recent': sum(1 for d in recency_days if d <= 7),
                'moderate': sum(1 for d in recency_days if 7 < d <= 30),
                'old': sum(1 for d in recency_days if d > 30)
            }
            
            return characteristics
            
        except Exception as e:
            logger.error(f"Segment characteristics calculation failed: {e}")
            return {}
    
    def _calculate_segment_metrics(self, segment_users: List[UserProfile]) -> Dict[str, float]:
        """Calculate average metrics for segment."""
        try:
            metrics = {}
            
            if not segment_users:
                return metrics
            
            metrics['engagement_score'] = np.mean([u.engagement_score for u in segment_users])
            metrics['session_count'] = np.mean([u.total_sessions for u in segment_users])
            metrics['avg_session_duration'] = np.mean([u.avg_session_duration for u in segment_users])
            metrics['page_views_per_session'] = np.mean([
                u.total_page_views / max(u.total_sessions, 1) for u in segment_users
            ])
            metrics['bounce_rate'] = np.mean([u.bounce_rate for u in segment_users])
            metrics['conversion_rate'] = np.mean([
                u.conversion_count / max(u.total_sessions, 1) for u in segment_users
            ])
            
            return metrics
            
        except Exception as e:
            logger.error(f"Segment metrics calculation failed: {e}")
            return {}
    
    def _identify_behavior_patterns(self, segment_users: List[UserProfile]) -> List[str]:
        """Identify behavior patterns for segment."""
        patterns = []
        
        try:
            if not segment_users:
                return patterns
            
            # High engagement pattern
            avg_engagement = np.mean([u.engagement_score for u in segment_users])
            if avg_engagement > 0.7:
                patterns.append("high_engagement")
            elif avg_engagement < 0.3:
                patterns.append("low_engagement")
            
            # Session behavior patterns
            avg_sessions = np.mean([u.total_sessions for u in segment_users])
            if avg_sessions > 10:
                patterns.append("frequent_visitor")
            elif avg_sessions < 3:
                patterns.append("infrequent_visitor")
            
            # Duration patterns
            avg_duration = np.mean([u.avg_session_duration for u in segment_users])
            if avg_duration > 600:  # 10 minutes
                patterns.append("long_session_duration")
            elif avg_duration < 60:  # 1 minute
                patterns.append("short_session_duration")
            
            # Bounce rate patterns
            avg_bounce = np.mean([u.bounce_rate for u in segment_users])
            if avg_bounce > 0.7:
                patterns.append("high_bounce_rate")
            elif avg_bounce < 0.3:
                patterns.append("low_bounce_rate")
            
            # Conversion patterns
            conversion_users = sum(1 for u in segment_users if u.conversion_count > 0)
            conversion_rate = conversion_users / len(segment_users)
            
            if conversion_rate > 0.5:
                patterns.append("high_conversion_propensity")
            elif conversion_rate < 0.1:
                patterns.append("low_conversion_propensity")
            
            # Lifecycle patterns
            lifecycle_counts = Counter([u.lifecycle_stage for u in segment_users])
            dominant_lifecycle = lifecycle_counts.most_common(1)[0][0]
            patterns.append(f"predominantly_{dominant_lifecycle}")
            
            return patterns
            
        except Exception as e:
            logger.error(f"Behavior pattern identification failed: {e}")
            return []
    
    def _generate_segment_recommendations(self, segment_id: int, 
                                        characteristics: Dict[str, Any],
                                        patterns: List[str]) -> List[str]:
        """Generate recommendations for segment."""
        recommendations = []
        
        # High engagement segments
        if "high_engagement" in patterns:
            recommendations.extend([
                "Leverage as brand advocates and referral sources",
                "Offer premium features or loyalty programs",
                "Request feedback and testimonials"
            ])
        
        # Low engagement segments
        if "low_engagement" in patterns:
            recommendations.extend([
                "Implement re-engagement campaigns",
                "Simplify user experience and onboarding",
                "Provide more targeted and relevant content"
            ])
        
        # High bounce rate
        if "high_bounce_rate" in patterns:
            recommendations.extend([
                "Improve landing page relevance and loading speed",
                "Enhance content quality and user experience",
                "Review traffic sources for quality"
            ])
        
        # Conversion-focused recommendations
        if "low_conversion_propensity" in patterns:
            recommendations.extend([
                "Implement conversion optimization campaigns",
                "Provide incentives and offers",
                "Improve conversion funnel and reduce friction"
            ])
        elif "high_conversion_propensity" in patterns:
            recommendations.extend([
                "Increase marketing spend on similar audiences",
                "Develop upselling and cross-selling strategies",
                "Focus on customer retention and lifetime value"
            ])
        
        # Lifecycle-specific recommendations
        if "predominantly_new" in patterns:
            recommendations.extend([
                "Focus on onboarding and first-time user experience",
                "Provide educational content and tutorials",
                "Monitor early engagement signals"
            ])
        elif "predominantly_churned" in patterns:
            recommendations.extend([
                "Implement win-back campaigns",
                "Investigate churn reasons and address pain points",
                "Offer special incentives to return"
            ])
        
        return recommendations
    
    def _generate_segment_name_description(self, segment_id: int, 
                                          characteristics: Dict[str, Any],
                                          patterns: List[str]) -> Tuple[str, str]:
        """Generate human-readable name and description for segment."""
        try:
            # Generate name based on dominant characteristics
            dominant_lifecycle = characteristics.get('dominant_lifecycle', 'unknown')
            avg_engagement = characteristics.get('avg_engagement_score', 0)
            
            if avg_engagement > 0.7:
                engagement_level = "Highly Engaged"
            elif avg_engagement > 0.4:
                engagement_level = "Moderately Engaged"
            else:
                engagement_level = "Low Engagement"
            
            # Combine lifecycle and engagement
            if "customer" in dominant_lifecycle:
                name = f"{engagement_level} Customers"
            elif "prospect" in dominant_lifecycle:
                name = f"{engagement_level} Prospects"
            else:
                name = f"{engagement_level} Users"
            
            # Generate description
            user_count = characteristics.get('user_count', 0)
            avg_sessions = characteristics.get('avg_sessions', 0)
            avg_duration = characteristics.get('avg_session_duration', 0)
            
            description = f"Segment of {user_count} users with {avg_engagement:.1%} average engagement. " \
                         f"Typically have {avg_sessions:.1f} sessions with {avg_duration:.0f} second duration. " \
                         f"Dominant lifecycle stage: {dominant_lifecycle}."
            
            return name, description
            
        except Exception as e:
            logger.error(f"Segment name generation failed: {e}")
            return f"Segment {segment_id}", f"User segment {segment_id}"

class UserBehaviorAnalyzer:
    """Main analyzer for user behavior patterns."""
    
    def __init__(self):
        self.session_analyzer = SessionAnalyzer()
        self.profile_builder = UserProfileBuilder()
        self.segmentation = BehavioralSegmentation()
        
        # Cache for analysis results
        self.latest_user_profiles: List[UserProfile] = []
        self.latest_segments: List[BehavioralSegment] = []
        
    async def analyze_user_behavior(self, events_df: pd.DataFrame) -> Dict[str, Any]:
        """Perform comprehensive user behavior analysis."""
        try:
            behavior_analyses.labels(analysis_type='comprehensive').inc()
            
            # Step 1: Convert events to sessions
            logger.info("Processing events into sessions...")
            sessions = self.session_analyzer.process_events_to_sessions(events_df)
            
            # Step 2: Build user profiles
            logger.info("Building user profiles...")
            user_profiles = self.profile_builder.build_user_profiles(sessions)
            self.latest_user_profiles = user_profiles
            
            # Step 3: Perform behavioral segmentation
            logger.info("Performing behavioral segmentation...")
            segments = self.segmentation.segment_users(user_profiles)
            self.latest_segments = segments
            
            # Step 4: Calculate engagement metrics
            logger.info("Computing engagement metrics...")
            engagement_summary = self._calculate_engagement_summary(user_profiles)
            
            # Step 5: Analyze user journeys
            logger.info("Analyzing user journeys...")
            journey_insights = self._analyze_user_journeys(sessions)
            
            # Step 6: Generate behavior insights
            behavior_insights = self._generate_behavior_insights(
                user_profiles, segments, engagement_summary
            )
            
            analysis_result = {
                'total_users': len(user_profiles),
                'total_sessions': len(sessions),
                'user_segments': [asdict(segment) for segment in segments],
                'engagement_summary': engagement_summary,
                'journey_insights': journey_insights,
                'behavior_insights': behavior_insights,
                'lifecycle_distribution': self._get_lifecycle_distribution(user_profiles),
                'top_user_patterns': self._identify_top_patterns(user_profiles),
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            logger.info("User behavior analysis completed successfully")
            return analysis_result
            
        except Exception as e:
            logger.error(f"User behavior analysis failed: {e}")
            return {'error': str(e)}
    
    def _calculate_engagement_summary(self, user_profiles: List[UserProfile]) -> Dict[str, Any]:
        """Calculate engagement summary statistics."""
        try:
            if not user_profiles:
                return {}
            
            engagement_scores = [u.engagement_score for u in user_profiles]
            session_counts = [u.total_sessions for u in user_profiles]
            durations = [u.avg_session_duration for u in user_profiles]
            bounce_rates = [u.bounce_rate for u in user_profiles]
            
            # Count users by engagement level
            engagement_scores_computed.inc(len(engagement_scores))
            
            summary = {
                'avg_engagement_score': np.mean(engagement_scores),
                'median_engagement_score': np.median(engagement_scores),
                'engagement_distribution': {
                    'high': sum(1 for score in engagement_scores if score > 0.7),
                    'medium': sum(1 for score in engagement_scores if 0.3 <= score <= 0.7),
                    'low': sum(1 for score in engagement_scores if score < 0.3)
                },
                'avg_sessions_per_user': np.mean(session_counts),
                'avg_session_duration': np.mean(durations),
                'avg_bounce_rate': np.mean(bounce_rates),
                'total_engaged_users': sum(1 for score in engagement_scores if score > 0.5)
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Engagement summary calculation failed: {e}")
            return {}
    
    def _analyze_user_journeys(self, sessions: List[UserSession]) -> Dict[str, Any]:
        """Analyze user journeys and paths."""
        try:
            journey_analyses.inc()
            
            # Group sessions by user to analyze journeys
            user_sessions = defaultdict(list)
            for session in sessions:
                user_sessions[session.user_id].append(session)
            
            # Analyze conversion journeys
            conversion_journeys = []
            non_conversion_journeys = []
            
            for user_id, user_session_list in user_sessions.items():
                has_conversion = any(session.conversion_events for session in user_session_list)
                
                if has_conversion:
                    conversion_journeys.append(user_session_list)
                else:
                    non_conversion_journeys.append(user_session_list)
            
            # Common entry points
            entry_pages = [s.entry_page for s in sessions if s.entry_page]
            entry_page_counts = Counter(entry_pages)
            
            # Common exit points
            exit_pages = [s.exit_page for s in sessions if s.exit_page]
            exit_page_counts = Counter(exit_pages)
            
            # Journey length analysis
            journey_lengths = [len(user_session_list) for user_session_list in user_sessions.values()]
            
            insights = {
                'total_journeys': len(user_sessions),
                'conversion_journeys': len(conversion_journeys),
                'conversion_rate': len(conversion_journeys) / len(user_sessions) if user_sessions else 0,
                'avg_journey_length': np.mean(journey_lengths) if journey_lengths else 0,
                'top_entry_pages': dict(entry_page_counts.most_common(5)),
                'top_exit_pages': dict(exit_page_counts.most_common(5)),
                'journey_length_distribution': {
                    'single_session': sum(1 for length in journey_lengths if length == 1),
                    'short': sum(1 for length in journey_lengths if 2 <= length <= 3),
                    'medium': sum(1 for length in journey_lengths if 4 <= length <= 7),
                    'long': sum(1 for length in journey_lengths if length > 7)
                }
            }
            
            return insights
            
        except Exception as e:
            logger.error(f"User journey analysis failed: {e}")
            return {}
    
    def _generate_behavior_insights(self, user_profiles: List[UserProfile],
                                  segments: List[BehavioralSegment],
                                  engagement_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate actionable behavior insights."""
        insights = []
        
        try:
            # Segment-specific insights
            for segment in segments:
                if segment.user_count > 0:
                    insight = {
                        'type': 'segment_insight',
                        'segment_name': segment.segment_name,
                        'user_count': segment.user_count,
                        'key_characteristics': segment.behavior_patterns[:3],
                        'recommendations': segment.recommendations[:3],
                        'priority': 'high' if segment.user_count > len(user_profiles) * 0.2 else 'medium'
                    }
                    insights.append(insight)
            
            # Engagement insights
            high_engagement_users = sum(1 for u in user_profiles if u.engagement_score > 0.7)
            if high_engagement_users > 0:
                insights.append({
                    'type': 'engagement_insight',
                    'title': 'High-Value User Segment Identified',
                    'description': f'{high_engagement_users} users show high engagement patterns',
                    'recommendations': [
                        'Focus retention efforts on high-engagement users',
                        'Develop loyalty programs for engaged users',
                        'Use engaged users for referral programs'
                    ],
                    'priority': 'high'
                })
            
            # Churn risk insights
            at_risk_users = sum(1 for u in user_profiles if 'at_risk' in u.lifecycle_stage)
            if at_risk_users > len(user_profiles) * 0.1:  # More than 10% at risk
                insights.append({
                    'type': 'churn_risk_insight',
                    'title': 'Significant Churn Risk Detected',
                    'description': f'{at_risk_users} users are at risk of churning',
                    'recommendations': [
                        'Implement re-engagement campaigns',
                        'Conduct user feedback surveys',
                        'Offer targeted incentives'
                    ],
                    'priority': 'critical'
                })
            
            # Bounce rate insights
            avg_bounce_rate = engagement_summary.get('avg_bounce_rate', 0)
            if avg_bounce_rate > 0.6:  # High bounce rate
                insights.append({
                    'type': 'bounce_rate_insight',
                    'title': 'High Bounce Rate Alert',
                    'description': f'Average bounce rate is {avg_bounce_rate:.1%}',
                    'recommendations': [
                        'Improve landing page relevance',
                        'Optimize page loading speed',
                        'Review content quality and user experience'
                    ],
                    'priority': 'high'
                })
            
            return insights
            
        except Exception as e:
            logger.error(f"Behavior insights generation failed: {e}")
            return []
    
    def _get_lifecycle_distribution(self, user_profiles: List[UserProfile]) -> Dict[str, int]:
        """Get distribution of users across lifecycle stages."""
        lifecycle_counts = Counter([u.lifecycle_stage for u in user_profiles])
        return dict(lifecycle_counts)
    
    def _identify_top_patterns(self, user_profiles: List[UserProfile]) -> List[Dict[str, Any]]:
        """Identify top behavior patterns across all users."""
        patterns = []
        
        try:
            # Device usage patterns
            devices = [u.preferences.get('preferred_device') for u in user_profiles 
                      if u.preferences.get('preferred_device')]
            if devices:
                device_counts = Counter(devices)
                patterns.append({
                    'pattern_type': 'device_preference',
                    'top_values': dict(device_counts.most_common(3))
                })
            
            # Traffic source patterns
            sources = [u.preferences.get('primary_traffic_source') for u in user_profiles 
                      if u.preferences.get('primary_traffic_source')]
            if sources:
                source_counts = Counter(sources)
                patterns.append({
                    'pattern_type': 'traffic_source',
                    'top_values': dict(source_counts.most_common(3))
                })
            
            # Time preference patterns
            hours = [u.preferences.get('preferred_hour') for u in user_profiles 
                    if u.preferences.get('preferred_hour') is not None]
            if hours:
                hour_counts = Counter(hours)
                patterns.append({
                    'pattern_type': 'time_preference',
                    'top_values': dict(hour_counts.most_common(3))
                })
            
            return patterns
            
        except Exception as e:
            logger.error(f"Pattern identification failed: {e}")
            return []
    
    def get_user_profile(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed profile for a specific user."""
        for profile in self.latest_user_profiles:
            if profile.user_id == user_id:
                return asdict(profile)
        return None
    
    def get_segment_details(self, segment_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific segment."""
        for segment in self.latest_segments:
            if segment.segment_id == segment_id:
                return asdict(segment)
        return None
