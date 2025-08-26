"""
Automated Insight Generation System

Analyzes data patterns, detects anomalies, and generates actionable business insights
automatically using statistical analysis and machine learning techniques.
"""

import logging
import asyncio
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple, Union, Callable
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import json
import pickle
from pathlib import Path
import statistics

# Statistical libraries
from scipy import stats
from scipy.signal import find_peaks
from sklearn.cluster import DBSCAN, KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LinearRegression
from sklearn.metrics import silhouette_score

# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge

# Initialize metrics
insights_generated = Counter('analytics_insights_generated_total', 'Insights generated', ['insight_type', 'priority'])
insight_generation_time = Histogram('analytics_insight_generation_duration_seconds', 'Insight generation time')
anomalies_detected = Counter('analytics_anomalies_detected_total', 'Anomalies detected', ['anomaly_type'])
pattern_discoveries = Counter('analytics_pattern_discoveries_total', 'Patterns discovered', ['pattern_type'])

logger = logging.getLogger(__name__)

@dataclass
class Insight:
    """Business insight with metadata."""
    insight_id: str
    title: str
    description: str
    insight_type: str  # trend, anomaly, pattern, opportunity, risk
    priority: str  # low, medium, high, critical
    confidence: float  # 0.0 to 1.0
    impact_score: float  # 0.0 to 1.0
    metrics_involved: List[str]
    time_period: Tuple[datetime, datetime]
    supporting_data: Dict[str, Any]
    recommendations: List[str]
    tags: List[str]
    created_at: datetime
    expires_at: Optional[datetime] = None

@dataclass
class AnomalyDetection:
    """Anomaly detection result."""
    anomaly_id: str
    metric_name: str
    detected_at: datetime
    anomaly_type: str  # spike, drop, drift, outlier
    severity: str  # low, medium, high
    expected_range: Tuple[float, float]
    actual_value: float
    deviation_score: float
    context: Dict[str, Any]

@dataclass
class TrendAnalysis:
    """Trend analysis result."""
    trend_id: str
    metric_name: str
    trend_type: str  # increasing, decreasing, seasonal, cyclical
    trend_strength: float  # 0.0 to 1.0
    trend_direction: float  # positive/negative
    confidence_interval: Tuple[float, float]
    seasonality_period: Optional[int]
    forecast: Optional[List[float]]
    time_period: Tuple[datetime, datetime]

@dataclass
class PatternDiscovery:
    """Pattern discovery result."""
    pattern_id: str
    pattern_type: str  # correlation, segment, behavior, funnel
    description: str
    strength: float  # 0.0 to 1.0
    entities_involved: List[str]
    supporting_evidence: Dict[str, Any]
    business_impact: str

class StatisticalAnalyzer:
    """Performs statistical analysis on data."""
    
    def __init__(self, significance_level: float = 0.05):
        self.significance_level = significance_level
        
    def detect_trend(self, data: pd.Series, window_size: int = 30) -> TrendAnalysis:
        """Detect trend in time series data."""
        try:
            if len(data) < window_size:
                return self._create_no_trend_result(data.name)
            
            # Remove NaN values
            clean_data = data.dropna()
            if len(clean_data) < window_size:
                return self._create_no_trend_result(data.name)
            
            # Linear regression for trend detection
            x = np.arange(len(clean_data))
            y = clean_data.values
            
            slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
            
            # Determine trend type and strength
            trend_strength = abs(r_value)
            trend_direction = np.sign(slope)
            
            if p_value > self.significance_level:
                trend_type = "no_trend"
                trend_strength = 0.0
            elif trend_direction > 0:
                trend_type = "increasing"
            else:
                trend_type = "decreasing"
            
            # Check for seasonality
            seasonality_period = self._detect_seasonality(clean_data)
            if seasonality_period:
                if trend_type == "no_trend":
                    trend_type = "seasonal"
                else:
                    trend_type = f"{trend_type}_seasonal"
            
            # Generate confidence interval
            confidence_interval = (
                intercept + slope * len(clean_data) - 1.96 * std_err,
                intercept + slope * len(clean_data) + 1.96 * std_err
            )
            
            # Simple forecast (linear extrapolation)
            forecast_steps = min(30, len(clean_data) // 4)
            forecast_x = np.arange(len(clean_data), len(clean_data) + forecast_steps)
            forecast = [intercept + slope * x for x in forecast_x]
            
            return TrendAnalysis(
                trend_id=f"trend_{data.name}_{int(datetime.now().timestamp())}",
                metric_name=data.name,
                trend_type=trend_type,
                trend_strength=trend_strength,
                trend_direction=trend_direction,
                confidence_interval=confidence_interval,
                seasonality_period=seasonality_period,
                forecast=forecast,
                time_period=(clean_data.index[0], clean_data.index[-1])
            )
            
        except Exception as e:
            logger.error(f"Trend detection failed for {data.name}: {e}")
            return self._create_no_trend_result(data.name)
    
    def _detect_seasonality(self, data: pd.Series, max_period: int = 50) -> Optional[int]:
        """Detect seasonality in time series."""
        if len(data) < max_period * 2:
            return None
        
        try:
            # Autocorrelation-based seasonality detection
            autocorr_values = []
            periods = range(2, min(max_period, len(data) // 2))
            
            for period in periods:
                # Calculate autocorrelation at this lag
                shifted = data.shift(period)
                correlation = data.corr(shifted)
                if not np.isnan(correlation):
                    autocorr_values.append((period, abs(correlation)))
            
            if not autocorr_values:
                return None
            
            # Find period with highest autocorrelation
            best_period, best_corr = max(autocorr_values, key=lambda x: x[1])
            
            # Only consider significant seasonality
            if best_corr > 0.3:
                return best_period
                
            return None
            
        except Exception as e:
            logger.error(f"Seasonality detection failed: {e}")
            return None
    
    def _create_no_trend_result(self, metric_name: str) -> TrendAnalysis:
        """Create result for no trend detected."""
        return TrendAnalysis(
            trend_id=f"no_trend_{metric_name}_{int(datetime.now().timestamp())}",
            metric_name=metric_name,
            trend_type="no_trend",
            trend_strength=0.0,
            trend_direction=0.0,
            confidence_interval=(0.0, 0.0),
            seasonality_period=None,
            forecast=None,
            time_period=(datetime.now() - timedelta(days=1), datetime.now())
        )
    
    def detect_anomalies(self, data: pd.Series, method: str = 'isolation_forest') -> List[AnomalyDetection]:
        """Detect anomalies in time series data."""
        try:
            clean_data = data.dropna()
            if len(clean_data) < 10:
                return []
            
            anomalies = []
            
            if method == 'statistical':
                anomalies.extend(self._detect_statistical_anomalies(clean_data))
            elif method == 'isolation_forest':
                anomalies.extend(self._detect_isolation_forest_anomalies(clean_data))
            elif method == 'zscore':
                anomalies.extend(self._detect_zscore_anomalies(clean_data))
            else:
                # Use combination of methods
                anomalies.extend(self._detect_statistical_anomalies(clean_data))
                anomalies.extend(self._detect_isolation_forest_anomalies(clean_data))
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Anomaly detection failed for {data.name}: {e}")
            return []
    
    def _detect_statistical_anomalies(self, data: pd.Series) -> List[AnomalyDetection]:
        """Detect anomalies using statistical methods."""
        anomalies = []
        
        # Calculate statistical bounds
        mean_val = data.mean()
        std_val = data.std()
        lower_bound = mean_val - 3 * std_val
        upper_bound = mean_val + 3 * std_val
        
        for timestamp, value in data.items():
            if value < lower_bound or value > upper_bound:
                deviation_score = abs(value - mean_val) / std_val
                
                if value > upper_bound:
                    anomaly_type = "spike"
                else:
                    anomaly_type = "drop"
                
                severity = "high" if deviation_score > 4 else "medium"
                
                anomaly = AnomalyDetection(
                    anomaly_id=f"anomaly_{data.name}_{int(timestamp.timestamp())}",
                    metric_name=data.name,
                    detected_at=timestamp,
                    anomaly_type=anomaly_type,
                    severity=severity,
                    expected_range=(lower_bound, upper_bound),
                    actual_value=value,
                    deviation_score=deviation_score,
                    context={"method": "statistical", "mean": mean_val, "std": std_val}
                )
                anomalies.append(anomaly)
        
        return anomalies
    
    def _detect_isolation_forest_anomalies(self, data: pd.Series) -> List[AnomalyDetection]:
        """Detect anomalies using Isolation Forest."""
        try:
            # Prepare data for Isolation Forest
            values = data.values.reshape(-1, 1)
            
            # Fit Isolation Forest
            iso_forest = IsolationForest(contamination=0.1, random_state=42)
            outlier_labels = iso_forest.fit_predict(values)
            outlier_scores = iso_forest.decision_function(values)
            
            anomalies = []
            
            for i, (timestamp, value) in enumerate(data.items()):
                if outlier_labels[i] == -1:  # Anomaly detected
                    # Calculate expected range (approximate)
                    median_val = data.median()
                    mad = np.median(np.abs(data - median_val))
                    lower_bound = median_val - 2.5 * mad
                    upper_bound = median_val + 2.5 * mad
                    
                    # Determine anomaly type
                    if value > upper_bound:
                        anomaly_type = "spike"
                    elif value < lower_bound:
                        anomaly_type = "drop"
                    else:
                        anomaly_type = "outlier"
                    
                    # Calculate severity based on isolation score
                    deviation_score = abs(outlier_scores[i])
                    if deviation_score > 0.2:
                        severity = "high"
                    elif deviation_score > 0.1:
                        severity = "medium"
                    else:
                        severity = "low"
                    
                    anomaly = AnomalyDetection(
                        anomaly_id=f"anomaly_if_{data.name}_{int(timestamp.timestamp())}",
                        metric_name=data.name,
                        detected_at=timestamp,
                        anomaly_type=anomaly_type,
                        severity=severity,
                        expected_range=(lower_bound, upper_bound),
                        actual_value=value,
                        deviation_score=deviation_score,
                        context={"method": "isolation_forest", "score": outlier_scores[i]}
                    )
                    anomalies.append(anomaly)
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Isolation Forest anomaly detection failed: {e}")
            return []
    
    def _detect_zscore_anomalies(self, data: pd.Series, threshold: float = 3.0) -> List[AnomalyDetection]:
        """Detect anomalies using Z-score method."""
        anomalies = []
        
        # Calculate rolling statistics
        rolling_mean = data.rolling(window=min(30, len(data)//2), center=True).mean()
        rolling_std = data.rolling(window=min(30, len(data)//2), center=True).std()
        
        for timestamp, value in data.items():
            expected_mean = rolling_mean.get(timestamp, data.mean())
            expected_std = rolling_std.get(timestamp, data.std())
            
            if expected_std > 0:
                z_score = abs(value - expected_mean) / expected_std
                
                if z_score > threshold:
                    lower_bound = expected_mean - threshold * expected_std
                    upper_bound = expected_mean + threshold * expected_std
                    
                    if value > upper_bound:
                        anomaly_type = "spike"
                    else:
                        anomaly_type = "drop"
                    
                    severity = "high" if z_score > 4 else "medium"
                    
                    anomaly = AnomalyDetection(
                        anomaly_id=f"anomaly_z_{data.name}_{int(timestamp.timestamp())}",
                        metric_name=data.name,
                        detected_at=timestamp,
                        anomaly_type=anomaly_type,
                        severity=severity,
                        expected_range=(lower_bound, upper_bound),
                        actual_value=value,
                        deviation_score=z_score,
                        context={"method": "zscore", "rolling_mean": expected_mean, "rolling_std": expected_std}
                    )
                    anomalies.append(anomaly)
        
        return anomalies

class PatternDetector:
    """Detects patterns in user behavior and business metrics."""
    
    def __init__(self):
        self.correlation_threshold = 0.7
        self.cluster_min_samples = 10
    
    def detect_correlations(self, data: pd.DataFrame) -> List[PatternDiscovery]:
        """Detect correlations between metrics."""
        patterns = []
        
        try:
            # Calculate correlation matrix
            numeric_data = data.select_dtypes(include=[np.number])
            if len(numeric_data.columns) < 2:
                return patterns
            
            correlation_matrix = numeric_data.corr()
            
            # Find strong correlations
            for i, metric1 in enumerate(correlation_matrix.columns):
                for j, metric2 in enumerate(correlation_matrix.columns):
                    if i < j:  # Avoid duplicates
                        correlation = correlation_matrix.loc[metric1, metric2]
                        
                        if abs(correlation) >= self.correlation_threshold and not np.isnan(correlation):
                            pattern_type = "positive_correlation" if correlation > 0 else "negative_correlation"
                            
                            pattern = PatternDiscovery(
                                pattern_id=f"corr_{metric1}_{metric2}_{int(datetime.now().timestamp())}",
                                pattern_type=pattern_type,
                                description=f"Strong {pattern_type.replace('_', ' ')} between {metric1} and {metric2}",
                                strength=abs(correlation),
                                entities_involved=[metric1, metric2],
                                supporting_evidence={
                                    "correlation_coefficient": correlation,
                                    "p_value": self._calculate_correlation_pvalue(
                                        numeric_data[metric1], numeric_data[metric2]
                                    ),
                                    "sample_size": len(numeric_data)
                                },
                                business_impact=self._assess_correlation_impact(metric1, metric2, correlation)
                            )
                            patterns.append(pattern)
            
            return patterns
            
        except Exception as e:
            logger.error(f"Correlation detection failed: {e}")
            return []
    
    def _calculate_correlation_pvalue(self, x: pd.Series, y: pd.Series) -> float:
        """Calculate p-value for correlation."""
        try:
            clean_x = x.dropna()
            clean_y = y.dropna()
            common_index = clean_x.index.intersection(clean_y.index)
            
            if len(common_index) < 3:
                return 1.0
            
            correlation, p_value = stats.pearsonr(
                clean_x.loc[common_index], 
                clean_y.loc[common_index]
            )
            return p_value
        except Exception:
            return 1.0
    
    def _assess_correlation_impact(self, metric1: str, metric2: str, correlation: float) -> str:
        """Assess business impact of correlation."""
        # Define business-critical metrics
        revenue_metrics = ['revenue', 'conversion_rate', 'purchase_value', 'sales']
        engagement_metrics = ['session_duration', 'page_views', 'user_actions', 'retention']
        performance_metrics = ['page_load_time', 'error_rate', 'bounce_rate']
        
        impact_level = "low"
        
        # Check if either metric is business-critical
        if any(metric in metric1.lower() or metric in metric2.lower() for metric in revenue_metrics):
            impact_level = "high"
        elif any(metric in metric1.lower() or metric in metric2.lower() for metric in engagement_metrics):
            impact_level = "medium"
        elif any(metric in metric1.lower() or metric in metric2.lower() for metric in performance_metrics):
            impact_level = "medium"
        
        # Adjust based on correlation strength
        if abs(correlation) > 0.9:
            impact_level = "high"
        elif abs(correlation) > 0.8 and impact_level == "low":
            impact_level = "medium"
        
        return impact_level
    
    def detect_user_segments(self, user_data: pd.DataFrame, 
                           features: List[str]) -> List[PatternDiscovery]:
        """Detect user segments using clustering."""
        patterns = []
        
        try:
            if len(user_data) < self.cluster_min_samples:
                return patterns
            
            # Prepare feature data
            feature_data = user_data[features].fillna(0)
            if feature_data.empty:
                return patterns
            
            # Standardize features
            scaler = StandardScaler()
            scaled_features = scaler.fit_transform(feature_data)
            
            # Try different numbers of clusters
            best_clusters = None
            best_score = -1
            best_n_clusters = 2
            
            for n_clusters in range(2, min(8, len(user_data) // 5)):
                try:
                    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
                    cluster_labels = kmeans.fit_predict(scaled_features)
                    
                    if len(set(cluster_labels)) > 1:  # Valid clustering
                        score = silhouette_score(scaled_features, cluster_labels)
                        if score > best_score:
                            best_score = score
                            best_clusters = cluster_labels
                            best_n_clusters = n_clusters
                except Exception:
                    continue
            
            if best_clusters is not None and best_score > 0.3:
                # Analyze clusters
                user_data_with_clusters = user_data.copy()
                user_data_with_clusters['cluster'] = best_clusters
                
                cluster_analysis = {}
                for cluster_id in range(best_n_clusters):
                    cluster_data = user_data_with_clusters[user_data_with_clusters['cluster'] == cluster_id]
                    
                    # Calculate cluster characteristics
                    cluster_profile = {}
                    for feature in features:
                        if feature in cluster_data.columns:
                            cluster_profile[feature] = {
                                'mean': cluster_data[feature].mean(),
                                'median': cluster_data[feature].median(),
                                'count': len(cluster_data)
                            }
                    
                    cluster_analysis[f'cluster_{cluster_id}'] = cluster_profile
                
                pattern = PatternDiscovery(
                    pattern_id=f"segments_{int(datetime.now().timestamp())}",
                    pattern_type="user_segments",
                    description=f"Discovered {best_n_clusters} distinct user segments",
                    strength=best_score,
                    entities_involved=features,
                    supporting_evidence={
                        "n_clusters": best_n_clusters,
                        "silhouette_score": best_score,
                        "cluster_analysis": cluster_analysis,
                        "sample_size": len(user_data)
                    },
                    business_impact="medium"
                )
                patterns.append(pattern)
            
            return patterns
            
        except Exception as e:
            logger.error(f"User segmentation failed: {e}")
            return []
    
    def detect_behavioral_patterns(self, event_data: pd.DataFrame) -> List[PatternDiscovery]:
        """Detect behavioral patterns in user events."""
        patterns = []
        
        try:
            if 'user_id' not in event_data.columns or 'event_type' not in event_data.columns:
                return patterns
            
            # Analyze event sequences
            user_sequences = event_data.groupby('user_id')['event_type'].apply(list)
            
            # Find common sequences
            sequence_counter = Counter()
            for user_id, events in user_sequences.items():
                # Extract sequences of length 2-4
                for seq_length in range(2, min(5, len(events) + 1)):
                    for i in range(len(events) - seq_length + 1):
                        sequence = tuple(events[i:i + seq_length])
                        sequence_counter[sequence] += 1
            
            # Find significant sequences
            total_users = len(user_sequences)
            min_support = max(3, total_users * 0.05)  # 5% minimum support
            
            significant_sequences = {
                seq: count for seq, count in sequence_counter.items() 
                if count >= min_support
            }
            
            if significant_sequences:
                # Create pattern for most common sequences
                top_sequences = sorted(
                    significant_sequences.items(), 
                    key=lambda x: x[1], 
                    reverse=True
                )[:5]
                
                pattern = PatternDiscovery(
                    pattern_id=f"behavior_{int(datetime.now().timestamp())}",
                    pattern_type="behavioral_sequence",
                    description=f"Common behavioral sequences discovered",
                    strength=top_sequences[0][1] / total_users,  # Support of top sequence
                    entities_involved=['user_behavior'],
                    supporting_evidence={
                        "top_sequences": [
                            {"sequence": list(seq), "count": count, "support": count / total_users}
                            for seq, count in top_sequences
                        ],
                        "total_users": total_users
                    },
                    business_impact="medium"
                )
                patterns.append(pattern)
            
            return patterns
            
        except Exception as e:
            logger.error(f"Behavioral pattern detection failed: {e}")
            return []

class InsightGenerator:
    """Generates business insights from analysis results."""
    
    def __init__(self):
        self.statistical_analyzer = StatisticalAnalyzer()
        self.pattern_detector = PatternDetector()
        self.insight_cache: Dict[str, List[Insight]] = {}
        
    def generate_insights(self, data: Dict[str, pd.DataFrame],
                         metrics: Dict[str, Any]) -> List[Insight]:
        """Generate comprehensive insights from data."""
        insights = []
        
        with insight_generation_time.time():
            try:
                # Generate trend insights
                insights.extend(self._generate_trend_insights(data, metrics))
                
                # Generate anomaly insights
                insights.extend(self._generate_anomaly_insights(data, metrics))
                
                # Generate pattern insights
                insights.extend(self._generate_pattern_insights(data, metrics))
                
                # Generate opportunity insights
                insights.extend(self._generate_opportunity_insights(data, metrics))
                
                # Generate risk insights
                insights.extend(self._generate_risk_insights(data, metrics))
                
                # Rank and filter insights
                insights = self._rank_insights(insights)
                
                # Record metrics
                for insight in insights:
                    insights_generated.labels(
                        insight_type=insight.insight_type,
                        priority=insight.priority
                    ).inc()
                
                return insights
                
            except Exception as e:
                logger.error(f"Insight generation failed: {e}")
                return []
    
    def _generate_trend_insights(self, data: Dict[str, pd.DataFrame],
                               metrics: Dict[str, Any]) -> List[Insight]:
        """Generate insights from trend analysis."""
        insights = []
        
        try:
            for metric_name, metric_data in data.items():
                if isinstance(metric_data, pd.Series):
                    trend_analysis = self.statistical_analyzer.detect_trend(metric_data)
                    
                    if trend_analysis.trend_strength > 0.3:  # Significant trend
                        insight = self._create_trend_insight(trend_analysis)
                        if insight:
                            insights.append(insight)
            
            return insights
            
        except Exception as e:
            logger.error(f"Trend insight generation failed: {e}")
            return []
    
    def _create_trend_insight(self, trend_analysis: TrendAnalysis) -> Optional[Insight]:
        """Create insight from trend analysis."""
        try:
            # Determine priority based on trend strength and type
            if trend_analysis.trend_strength > 0.8:
                priority = "high"
            elif trend_analysis.trend_strength > 0.5:
                priority = "medium"
            else:
                priority = "low"
            
            # Create title and description
            direction = "increasing" if trend_analysis.trend_direction > 0 else "decreasing"
            
            if "conversion" in trend_analysis.metric_name.lower():
                if direction == "increasing":
                    title = f"🎯 Conversion Rate Trending Upward"
                    impact_score = 0.9
                else:
                    title = f"⚠️ Conversion Rate Declining"
                    impact_score = 0.8
                    priority = "high"
            elif "revenue" in trend_analysis.metric_name.lower():
                if direction == "increasing":
                    title = f"💰 Revenue Growth Detected"
                    impact_score = 0.9
                else:
                    title = f"📉 Revenue Decline Alert"
                    impact_score = 0.9
                    priority = "high"
            elif "error" in trend_analysis.metric_name.lower():
                if direction == "increasing":
                    title = f"🚨 Error Rate Increasing"
                    impact_score = 0.7
                    priority = "high"
                else:
                    title = f"✅ Error Rate Improving"
                    impact_score = 0.6
            else:
                title = f"📊 {trend_analysis.metric_name.title()} Trend: {direction.title()}"
                impact_score = 0.5
            
            description = f"The metric '{trend_analysis.metric_name}' shows a {direction} trend " \
                         f"with {trend_analysis.trend_strength:.1%} confidence. "
            
            if trend_analysis.seasonality_period:
                description += f"Seasonal pattern detected with {trend_analysis.seasonality_period}-period cycle. "
            
            # Generate recommendations
            recommendations = self._generate_trend_recommendations(trend_analysis)
            
            # Create tags
            tags = [trend_analysis.trend_type, trend_analysis.metric_name]
            if trend_analysis.seasonality_period:
                tags.append("seasonal")
            
            insight = Insight(
                insight_id=f"trend_{trend_analysis.trend_id}",
                title=title,
                description=description,
                insight_type="trend",
                priority=priority,
                confidence=trend_analysis.trend_strength,
                impact_score=impact_score,
                metrics_involved=[trend_analysis.metric_name],
                time_period=trend_analysis.time_period,
                supporting_data=asdict(trend_analysis),
                recommendations=recommendations,
                tags=tags,
                created_at=datetime.now(),
                expires_at=datetime.now() + timedelta(days=7)
            )
            
            return insight
            
        except Exception as e:
            logger.error(f"Trend insight creation failed: {e}")
            return None
    
    def _generate_trend_recommendations(self, trend_analysis: TrendAnalysis) -> List[str]:
        """Generate recommendations based on trend analysis."""
        recommendations = []
        metric_name = trend_analysis.metric_name.lower()
        direction = trend_analysis.trend_direction
        
        if "conversion" in metric_name:
            if direction > 0:
                recommendations.extend([
                    "Continue current optimization strategies",
                    "Scale successful marketing campaigns",
                    "Document and replicate successful practices"
                ])
            else:
                recommendations.extend([
                    "Review recent changes to conversion funnel",
                    "Conduct A/B tests on checkout process",
                    "Analyze user feedback for friction points"
                ])
        
        elif "revenue" in metric_name:
            if direction > 0:
                recommendations.extend([
                    "Increase marketing budget for high-performing channels",
                    "Expand successful product lines",
                    "Optimize pricing strategies"
                ])
            else:
                recommendations.extend([
                    "Review pricing strategy",
                    "Analyze competitor activities",
                    "Implement customer retention programs"
                ])
        
        elif "error" in metric_name:
            if direction > 0:
                recommendations.extend([
                    "Investigate root causes of increasing errors",
                    "Implement additional monitoring",
                    "Review recent deployments"
                ])
            else:
                recommendations.extend([
                    "Document successful error reduction strategies",
                    "Continue monitoring for sustained improvement"
                ])
        
        # Add seasonal recommendations
        if trend_analysis.seasonality_period:
            recommendations.append(f"Plan for seasonal variation (cycle: {trend_analysis.seasonality_period} periods)")
        
        return recommendations
    
    def _generate_anomaly_insights(self, data: Dict[str, pd.DataFrame],
                                 metrics: Dict[str, Any]) -> List[Insight]:
        """Generate insights from anomaly detection."""
        insights = []
        
        try:
            for metric_name, metric_data in data.items():
                if isinstance(metric_data, pd.Series):
                    anomalies = self.statistical_analyzer.detect_anomalies(metric_data)
                    
                    for anomaly in anomalies:
                        if anomaly.severity in ['high', 'medium']:
                            insight = self._create_anomaly_insight(anomaly)
                            if insight:
                                insights.append(insight)
                                
                                anomalies_detected.labels(
                                    anomaly_type=anomaly.anomaly_type
                                ).inc()
            
            return insights
            
        except Exception as e:
            logger.error(f"Anomaly insight generation failed: {e}")
            return []
    
    def _create_anomaly_insight(self, anomaly: AnomalyDetection) -> Optional[Insight]:
        """Create insight from anomaly detection."""
        try:
            # Determine priority and impact
            priority_map = {"high": "critical", "medium": "high", "low": "medium"}
            priority = priority_map.get(anomaly.severity, "medium")
            
            impact_score = 0.8 if anomaly.severity == "high" else 0.6
            
            # Create title based on anomaly type and metric
            if anomaly.anomaly_type == "spike":
                title = f"📈 Unusual Spike in {anomaly.metric_name.title()}"
            elif anomaly.anomaly_type == "drop":
                title = f"📉 Significant Drop in {anomaly.metric_name.title()}"
            else:
                title = f"⚠️ Anomaly Detected in {anomaly.metric_name.title()}"
            
            description = f"An anomaly was detected in '{anomaly.metric_name}' at {anomaly.detected_at.strftime('%Y-%m-%d %H:%M')}. " \
                         f"Expected range: {anomaly.expected_range[0]:.2f} - {anomaly.expected_range[1]:.2f}, " \
                         f"but observed: {anomaly.actual_value:.2f} " \
                         f"(deviation score: {anomaly.deviation_score:.2f})"
            
            # Generate recommendations based on anomaly type and metric
            recommendations = self._generate_anomaly_recommendations(anomaly)
            
            insight = Insight(
                insight_id=f"anomaly_{anomaly.anomaly_id}",
                title=title,
                description=description,
                insight_type="anomaly",
                priority=priority,
                confidence=min(anomaly.deviation_score / 5.0, 1.0),  # Scale confidence
                impact_score=impact_score,
                metrics_involved=[anomaly.metric_name],
                time_period=(anomaly.detected_at, anomaly.detected_at),
                supporting_data=asdict(anomaly),
                recommendations=recommendations,
                tags=[anomaly.anomaly_type, anomaly.metric_name, "urgent"],
                created_at=datetime.now(),
                expires_at=datetime.now() + timedelta(days=3)
            )
            
            return insight
            
        except Exception as e:
            logger.error(f"Anomaly insight creation failed: {e}")
            return None
    
    def _generate_anomaly_recommendations(self, anomaly: AnomalyDetection) -> List[str]:
        """Generate recommendations for anomaly."""
        recommendations = []
        metric_name = anomaly.metric_name.lower()
        
        if anomaly.anomaly_type in ["spike", "drop"]:
            recommendations.extend([
                f"Investigate events around {anomaly.detected_at.strftime('%Y-%m-%d %H:%M')}",
                "Check for system changes or deployments",
                "Review marketing campaigns or external factors"
            ])
        
        if "error" in metric_name and anomaly.anomaly_type == "spike":
            recommendations.extend([
                "Check application logs for error details",
                "Verify system resources and performance",
                "Consider rolling back recent changes"
            ])
        
        elif "traffic" in metric_name or "user" in metric_name:
            if anomaly.anomaly_type == "spike":
                recommendations.extend([
                    "Ensure system can handle increased load",
                    "Monitor conversion rates during traffic spike"
                ])
            else:
                recommendations.extend([
                    "Check marketing campaigns and referral sources",
                    "Investigate potential technical issues"
                ])
        
        return recommendations
    
    def _generate_pattern_insights(self, data: Dict[str, pd.DataFrame],
                                 metrics: Dict[str, Any]) -> List[Insight]:
        """Generate insights from pattern detection."""
        insights = []
        
        try:
            # Combine all numeric data for correlation analysis
            numeric_data = pd.DataFrame()
            for name, df in data.items():
                if isinstance(df, pd.DataFrame):
                    numeric_cols = df.select_dtypes(include=[np.number])
                    for col in numeric_cols.columns:
                        numeric_data[f"{name}_{col}"] = numeric_cols[col]
                elif isinstance(df, pd.Series):
                    numeric_data[name] = df
            
            if not numeric_data.empty:
                # Detect correlations
                correlation_patterns = self.pattern_detector.detect_correlations(numeric_data)
                
                for pattern in correlation_patterns:
                    if pattern.strength > 0.7:  # Strong patterns only
                        insight = self._create_pattern_insight(pattern)
                        if insight:
                            insights.append(insight)
                            
                            pattern_discoveries.labels(
                                pattern_type=pattern.pattern_type
                            ).inc()
            
            return insights
            
        except Exception as e:
            logger.error(f"Pattern insight generation failed: {e}")
            return []
    
    def _create_pattern_insight(self, pattern: PatternDiscovery) -> Optional[Insight]:
        """Create insight from pattern discovery."""
        try:
            # Determine priority based on business impact and strength
            if pattern.business_impact == "high" and pattern.strength > 0.8:
                priority = "high"
                impact_score = 0.8
            elif pattern.business_impact == "medium" or pattern.strength > 0.7:
                priority = "medium"
                impact_score = 0.6
            else:
                priority = "low"
                impact_score = 0.4
            
            # Create title based on pattern type
            if pattern.pattern_type == "positive_correlation":
                title = f"🔗 Strong Positive Correlation Discovered"
            elif pattern.pattern_type == "negative_correlation":
                title = f"🔗 Strong Negative Correlation Discovered"
            else:
                title = f"🔍 Pattern Discovered: {pattern.pattern_type.title()}"
            
            description = pattern.description + f" (strength: {pattern.strength:.1%})"
            
            # Generate recommendations based on pattern
            recommendations = self._generate_pattern_recommendations(pattern)
            
            insight = Insight(
                insight_id=f"pattern_{pattern.pattern_id}",
                title=title,
                description=description,
                insight_type="pattern",
                priority=priority,
                confidence=pattern.strength,
                impact_score=impact_score,
                metrics_involved=pattern.entities_involved,
                time_period=(datetime.now() - timedelta(days=30), datetime.now()),
                supporting_data=asdict(pattern),
                recommendations=recommendations,
                tags=[pattern.pattern_type] + pattern.entities_involved,
                created_at=datetime.now(),
                expires_at=datetime.now() + timedelta(days=14)
            )
            
            return insight
            
        except Exception as e:
            logger.error(f"Pattern insight creation failed: {e}")
            return None
    
    def _generate_pattern_recommendations(self, pattern: PatternDiscovery) -> List[str]:
        """Generate recommendations for pattern."""
        recommendations = []
        
        if pattern.pattern_type == "positive_correlation":
            recommendations.extend([
                f"Leverage the relationship between {' and '.join(pattern.entities_involved)}",
                "Consider optimizing both metrics simultaneously",
                "Monitor this relationship for future insights"
            ])
        elif pattern.pattern_type == "negative_correlation":
            recommendations.extend([
                f"Investigate the trade-off between {' and '.join(pattern.entities_involved)}",
                "Consider balancing strategies for both metrics",
                "Monitor for potential optimization opportunities"
            ])
        
        return recommendations
    
    def _generate_opportunity_insights(self, data: Dict[str, pd.DataFrame],
                                     metrics: Dict[str, Any]) -> List[Insight]:
        """Generate opportunity insights."""
        insights = []
        
        # Example opportunity detection (simplified)
        try:
            for metric_name, metric_data in data.items():
                if isinstance(metric_data, pd.Series) and len(metric_data) > 0:
                    recent_avg = metric_data.tail(7).mean()
                    historical_avg = metric_data.head(30).mean()
                    
                    if "conversion" in metric_name.lower():
                        if recent_avg > historical_avg * 1.1:  # 10% improvement
                            insight = Insight(
                                insight_id=f"opp_{metric_name}_{int(datetime.now().timestamp())}",
                                title="🚀 Conversion Rate Optimization Opportunity",
                                description=f"Recent {metric_name} performance ({recent_avg:.2%}) is {((recent_avg/historical_avg-1)*100):.1f}% above historical average. Consider scaling successful strategies.",
                                insight_type="opportunity",
                                priority="medium",
                                confidence=0.7,
                                impact_score=0.7,
                                metrics_involved=[metric_name],
                                time_period=(datetime.now() - timedelta(days=7), datetime.now()),
                                supporting_data={"recent_avg": recent_avg, "historical_avg": historical_avg},
                                recommendations=[
                                    "Analyze what drove recent improvements",
                                    "Scale successful tactics",
                                    "Document best practices"
                                ],
                                tags=["opportunity", "optimization"],
                                created_at=datetime.now(),
                                expires_at=datetime.now() + timedelta(days=5)
                            )
                            insights.append(insight)
        
            return insights
            
        except Exception as e:
            logger.error(f"Opportunity insight generation failed: {e}")
            return []
    
    def _generate_risk_insights(self, data: Dict[str, pd.DataFrame],
                              metrics: Dict[str, Any]) -> List[Insight]:
        """Generate risk insights."""
        insights = []
        
        # Example risk detection (simplified)
        try:
            for metric_name, metric_data in data.items():
                if isinstance(metric_data, pd.Series) and len(metric_data) > 0:
                    recent_trend = metric_data.tail(5).mean() - metric_data.tail(10).head(5).mean()
                    
                    if "error" in metric_name.lower() and recent_trend > 0:
                        insight = Insight(
                            insight_id=f"risk_{metric_name}_{int(datetime.now().timestamp())}",
                            title="⚠️ Increasing Error Rate Risk",
                            description=f"Error rate trend is increasing. Immediate attention may be required to prevent service degradation.",
                            insight_type="risk",
                            priority="high",
                            confidence=0.8,
                            impact_score=0.8,
                            metrics_involved=[metric_name],
                            time_period=(datetime.now() - timedelta(days=5), datetime.now()),
                            supporting_data={"trend": recent_trend},
                            recommendations=[
                                "Monitor error logs closely",
                                "Prepare rollback procedures",
                                "Alert engineering team"
                            ],
                            tags=["risk", "urgent"],
                            created_at=datetime.now(),
                            expires_at=datetime.now() + timedelta(days=1)
                        )
                        insights.append(insight)
        
            return insights
            
        except Exception as e:
            logger.error(f"Risk insight generation failed: {e}")
            return []
    
    def _rank_insights(self, insights: List[Insight]) -> List[Insight]:
        """Rank insights by importance and relevance."""
        try:
            # Priority weights
            priority_weights = {"critical": 10, "high": 7, "medium": 4, "low": 1}
            
            # Type weights
            type_weights = {"risk": 9, "anomaly": 8, "opportunity": 6, "trend": 5, "pattern": 4}
            
            def calculate_score(insight: Insight) -> float:
                priority_score = priority_weights.get(insight.priority, 1)
                type_score = type_weights.get(insight.insight_type, 1)
                
                # Combine factors
                total_score = (
                    priority_score * 0.4 +
                    type_score * 0.3 +
                    insight.confidence * 10 * 0.2 +
                    insight.impact_score * 10 * 0.1
                )
                
                return total_score
            
            # Sort by calculated score
            ranked_insights = sorted(insights, key=calculate_score, reverse=True)
            
            # Limit to top 50 insights to avoid overwhelming
            return ranked_insights[:50]
            
        except Exception as e:
            logger.error(f"Insight ranking failed: {e}")
            return insights
