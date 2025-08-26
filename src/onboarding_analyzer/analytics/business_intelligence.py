"""
Business Intelligence Reporting System

Provides comprehensive BI capabilities including automated report generation,
executive dashboards, KPI monitoring, data visualization, and strategic insights.
"""

import logging
import asyncio
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple, Union, Set
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import json
import io
import base64
from pathlib import Path

# Analysis and visualization libraries
from sklearn.preprocessing import StandardScaler
from scipy import stats
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt

# Optional visualization libraries with fallbacks
try:
    import seaborn as sns
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False
    sns = None

try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False
    go = None
    px = None
    make_subplots = None

# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry

# Initialize metrics with conditional registration
try:
    bi_reports_generated = Counter('analytics_bi_reports_total', 'BI reports generated', ['report_type'])
    dashboard_updates = Counter('analytics_dashboard_updates_total', 'Dashboard updates performed')
    kpi_calculations = Counter('analytics_kpi_calculations_total', 'KPI calculations performed')
    executive_reports = Counter('analytics_executive_reports_total', 'Executive reports generated')
except ValueError:
    # Metrics already registered, use existing ones
    from prometheus_client import REGISTRY
    bi_reports_generated = None
    dashboard_updates = None
    kpi_calculations = None
    executive_reports = None

logger = logging.getLogger(__name__)

@dataclass
class ReportDefinition:
    """Definition for a BI report."""
    report_id: str
    report_name: str
    report_type: str  # 'operational', 'executive', 'analytical', 'regulatory'
    description: str
    kpi_ids: List[str]
    time_period: str
    filters: Dict[str, Any]
    schedule: Optional[str] = None
    recipients: List[str] = None
    auto_refresh: bool = False
    refresh_interval: int = 3600  # seconds

@dataclass
class GeneratedReport:
    """Generated BI report with all components."""
    report_id: str
    generation_timestamp: datetime
    report_data: Dict[str, Any]
    visualizations: Dict[str, str]  # Base64 encoded charts
    insights: List[Dict[str, Any]]
    recommendations: List[str]
    kpi_summary: Dict[str, 'KPIResult']
    executive_summary: str
    export_formats: List[str]

@dataclass
class DashboardWidget:
    """Dashboard widget configuration."""
    widget_id: str
    widget_type: str  # 'kpi_card', 'chart', 'table', 'metric'
    title: str
    config: Dict[str, Any]
    position: Dict[str, int]  # x, y, width, height
    
@dataclass
class Dashboard:
    """Dashboard configuration."""
    dashboard_id: str
    dashboard_name: str
    description: str
    widgets: List[DashboardWidget]
    layout: str  # 'grid', 'flex'
    auto_refresh: bool = True
    refresh_interval: int = 300  # seconds

logger = logging.getLogger(__name__)

@dataclass
class KPIDefinition:
    """Key Performance Indicator definition."""
    kpi_id: str
    kpi_name: str
    description: str
    category: str  # business, operational, financial, customer
    calculation_method: str  # sum, average, ratio, percentage, count
    data_source: str
    target_value: Optional[float]
    warning_threshold: Optional[float]
    critical_threshold: Optional[float]
    time_period: str  # daily, weekly, monthly, quarterly
    format_type: str  # percentage, currency, number, count

@dataclass
class KPIResult:
    """KPI calculation result."""
    kpi_id: str
    value: float
    previous_value: Optional[float]
    change_percentage: Optional[float]
    target_value: Optional[float]
    target_achievement: Optional[float]
    status: str  # on_target, warning, critical, improving, declining
    trend: str  # up, down, stable
    calculation_timestamp: datetime
    data_points: List[Dict[str, Any]]

@dataclass
class ReportDefinition:
    """Report definition structure."""
    report_id: str
    report_name: str
    report_type: str  # executive, operational, analytical, performance
    description: str
    frequency: str  # daily, weekly, monthly, quarterly, ad_hoc
    recipients: List[str]
    kpi_ids: List[str]
    chart_types: List[str]
    filters: Dict[str, Any]
    layout_config: Dict[str, Any]
    auto_insights: bool

@dataclass
class DashboardWidget:
    """Dashboard widget definition."""
    widget_id: str
    widget_type: str  # kpi_card, chart, table, metric, trend
    title: str
    data_source: str
    config: Dict[str, Any]
    position: Dict[str, int]  # row, col, width, height
    refresh_interval: int  # seconds
    access_level: str  # public, private, restricted

@dataclass
class Dashboard:
    """Complete dashboard definition."""
    dashboard_id: str
    dashboard_name: str
    description: str
    dashboard_type: str  # executive, operational, analytical
    widgets: List[DashboardWidget]
    layout: Dict[str, Any]
    access_permissions: List[str]
    auto_refresh: bool
    refresh_interval: int

@dataclass
class GeneratedReport:
    """Generated report result."""
    report_id: str
    generation_timestamp: datetime
    report_data: Dict[str, Any]
    visualizations: Dict[str, str]  # chart_id -> base64_encoded_image
    insights: List[Dict[str, Any]]
    recommendations: List[str]
    kpi_summary: Dict[str, KPIResult]
    executive_summary: str
    export_formats: List[str]  # pdf, excel, json

class KPICalculator:
    """Calculates Key Performance Indicators."""
    
    def __init__(self):
        self.kpi_cache = {}
        self.historical_data = defaultdict(list)
    
    def calculate_kpis(self, data_df: pd.DataFrame, 
                      kpi_definitions: List[KPIDefinition],
                      time_period: str = 'current') -> Dict[str, KPIResult]:
        """Calculate all KPIs for the specified time period."""
        try:
            if kpi_calculations:
                kpi_calculations.inc()
            
            kpi_results = {}
            
            for kpi_def in kpi_definitions:
                try:
                    result = self._calculate_single_kpi(data_df, kpi_def, time_period)
                    if result:
                        kpi_results[kpi_def.kpi_id] = result
                        
                        # Cache result for trending
                        self.historical_data[kpi_def.kpi_id].append({
                            'timestamp': result.calculation_timestamp,
                            'value': result.value
                        })
                        
                except Exception as e:
                    logger.error(f"Failed to calculate KPI {kpi_def.kpi_id}: {e}")
                    continue
            
            logger.info(f"Calculated {len(kpi_results)} KPIs successfully")
            return kpi_results
            
        except Exception as e:
            logger.error(f"KPI calculation failed: {e}")
            return {}
    
    def _calculate_single_kpi(self, data_df: pd.DataFrame, 
                             kpi_def: KPIDefinition, 
                             time_period: str) -> Optional[KPIResult]:
        """Calculate a single KPI."""
        try:
            # Filter data for time period
            filtered_data = self._filter_data_by_period(data_df, time_period)
            
            if filtered_data.empty:
                logger.warning(f"No data available for KPI {kpi_def.kpi_id}")
                return None
            
            # Calculate current value
            current_value = self._perform_calculation(filtered_data, kpi_def)
            
            # Calculate previous period value for comparison
            previous_value = self._calculate_previous_period_value(
                data_df, kpi_def, time_period
            )
            
            # Calculate change percentage
            change_percentage = None
            if previous_value is not None and previous_value != 0:
                change_percentage = ((current_value - previous_value) / previous_value) * 100
            
            # Calculate target achievement
            target_achievement = None
            if kpi_def.target_value is not None and kpi_def.target_value != 0:
                target_achievement = (current_value / kpi_def.target_value) * 100
            
            # Determine status
            status = self._determine_kpi_status(kpi_def, current_value, target_achievement)
            
            # Determine trend
            trend = self._determine_trend(current_value, previous_value, change_percentage)
            
            # Extract data points for visualization
            data_points = self._extract_data_points(filtered_data, kpi_def)
            
            return KPIResult(
                kpi_id=kpi_def.kpi_id,
                value=current_value,
                previous_value=previous_value,
                change_percentage=change_percentage,
                target_value=kpi_def.target_value,
                target_achievement=target_achievement,
                status=status,
                trend=trend,
                calculation_timestamp=datetime.now(),
                data_points=data_points
            )
            
        except Exception as e:
            logger.error(f"Single KPI calculation failed for {kpi_def.kpi_id}: {e}")
            return None
    
    def _filter_data_by_period(self, data_df: pd.DataFrame, time_period: str) -> pd.DataFrame:
        """Filter data based on time period."""
        try:
            if 'timestamp' not in data_df.columns:
                return data_df
            
            data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])
            current_time = datetime.now()
            
            if time_period == 'current_day':
                start_date = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
                return data_df[data_df['timestamp'] >= start_date]
            
            elif time_period == 'current_week':
                start_date = current_time - timedelta(days=current_time.weekday())
                start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                return data_df[data_df['timestamp'] >= start_date]
            
            elif time_period == 'current_month':
                start_date = current_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                return data_df[data_df['timestamp'] >= start_date]
            
            elif time_period == 'current_quarter':
                quarter_start_month = ((current_time.month - 1) // 3) * 3 + 1
                start_date = current_time.replace(
                    month=quarter_start_month, day=1, 
                    hour=0, minute=0, second=0, microsecond=0
                )
                return data_df[data_df['timestamp'] >= start_date]
            
            elif time_period == 'last_7_days':
                start_date = current_time - timedelta(days=7)
                return data_df[data_df['timestamp'] >= start_date]
            
            elif time_period == 'last_30_days':
                start_date = current_time - timedelta(days=30)
                return data_df[data_df['timestamp'] >= start_date]
            
            else:  # current or default
                return data_df
                
        except Exception as e:
            logger.error(f"Data filtering by period failed: {e}")
            return data_df
    
    def _perform_calculation(self, data_df: pd.DataFrame, kpi_def: KPIDefinition) -> float:
        """Perform the actual KPI calculation."""
        try:
            if kpi_def.calculation_method == 'count':
                return len(data_df)
            
            elif kpi_def.calculation_method == 'sum':
                # Sum numeric columns
                numeric_cols = data_df.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) > 0:
                    return data_df[numeric_cols[0]].sum()
                return len(data_df)
            
            elif kpi_def.calculation_method == 'average':
                # Average of numeric columns
                numeric_cols = data_df.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) > 0:
                    return data_df[numeric_cols[0]].mean()
                return 0
            
            elif kpi_def.calculation_method == 'ratio':
                # Calculate ratio based on KPI configuration
                return self._calculate_ratio_kpi(data_df, kpi_def)
            
            elif kpi_def.calculation_method == 'percentage':
                # Calculate percentage based on KPI configuration
                return self._calculate_percentage_kpi(data_df, kpi_def)
            
            elif kpi_def.calculation_method == 'unique_count':
                # Count unique values in first string column
                string_cols = data_df.select_dtypes(include=['object']).columns
                if len(string_cols) > 0:
                    return data_df[string_cols[0]].nunique()
                return len(data_df)
            
            else:
                # Default to count
                return len(data_df)
                
        except Exception as e:
            logger.error(f"KPI calculation failed: {e}")
            return 0
    
    def _calculate_ratio_kpi(self, data_df: pd.DataFrame, kpi_def: KPIDefinition) -> float:
        """Calculate ratio-based KPIs."""
        try:
            # Common ratio calculations
            if 'conversion' in kpi_def.kpi_id.lower():
                # Conversion rate: conversions / total users
                conversion_events = ['purchase', 'signup', 'conversion', 'subscribe']
                conversions = len(data_df[data_df['event_type'].isin(conversion_events)])
                total_users = data_df['user_id'].nunique() if 'user_id' in data_df.columns else len(data_df)
                return (conversions / total_users) * 100 if total_users > 0 else 0
            
            elif 'bounce' in kpi_def.kpi_id.lower():
                # Bounce rate: single-page sessions / total sessions
                if 'session_id' in data_df.columns:
                    session_page_counts = data_df.groupby('session_id').size()
                    single_page_sessions = (session_page_counts == 1).sum()
                    total_sessions = len(session_page_counts)
                    return (single_page_sessions / total_sessions) * 100 if total_sessions > 0 else 0
                return 0
            
            elif 'retention' in kpi_def.kpi_id.lower():
                # User retention rate (simplified)
                if 'user_id' in data_df.columns and 'timestamp' in data_df.columns:
                    data_df['date'] = pd.to_datetime(data_df['timestamp']).dt.date
                    user_activity_days = data_df.groupby('user_id')['date'].nunique()
                    retained_users = (user_activity_days > 1).sum()
                    total_users = len(user_activity_days)
                    return (retained_users / total_users) * 100 if total_users > 0 else 0
                return 0
            
            else:
                # Generic ratio calculation
                numeric_cols = data_df.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) >= 2:
                    return (data_df[numeric_cols[0]].sum() / data_df[numeric_cols[1]].sum()) * 100
                return 0
                
        except Exception as e:
            logger.error(f"Ratio KPI calculation failed: {e}")
            return 0
    
    def _calculate_percentage_kpi(self, data_df: pd.DataFrame, kpi_def: KPIDefinition) -> float:
        """Calculate percentage-based KPIs."""
        try:
            # Common percentage calculations
            if 'growth' in kpi_def.kpi_id.lower():
                # Growth rate calculation (requires historical data)
                return self._calculate_growth_rate(data_df, kpi_def)
            
            elif 'engagement' in kpi_def.kpi_id.lower():
                # Engagement rate
                engaged_users = len(data_df[data_df.get('events_per_session', 0) > 3])
                total_users = data_df['user_id'].nunique() if 'user_id' in data_df.columns else len(data_df)
                return (engaged_users / total_users) * 100 if total_users > 0 else 0
            
            else:
                # Generic percentage calculation
                total_count = len(data_df)
                condition_count = len(data_df[data_df.iloc[:, 0].notna()]) if not data_df.empty else 0
                return (condition_count / total_count) * 100 if total_count > 0 else 0
                
        except Exception as e:
            logger.error(f"Percentage KPI calculation failed: {e}")
            return 0
    
    def _calculate_growth_rate(self, data_df: pd.DataFrame, kpi_def: KPIDefinition) -> float:
        """Calculate growth rate for a KPI."""
        try:
            if 'timestamp' not in data_df.columns:
                return 0
            
            data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])
            data_df['date'] = data_df['timestamp'].dt.date
            
            # Group by date and count
            daily_counts = data_df.groupby('date').size().reset_index(name='count')
            
            if len(daily_counts) < 2:
                return 0
            
            # Calculate period-over-period growth
            current_period = daily_counts['count'].iloc[-7:].sum()  # Last 7 days
            previous_period = daily_counts['count'].iloc[-14:-7].sum()  # Previous 7 days
            
            if previous_period > 0:
                return ((current_period - previous_period) / previous_period) * 100
            else:
                return 0
                
        except Exception as e:
            logger.error(f"Growth rate calculation failed: {e}")
            return 0
    
    def _calculate_previous_period_value(self, data_df: pd.DataFrame, 
                                       kpi_def: KPIDefinition, 
                                       time_period: str) -> Optional[float]:
        """Calculate KPI value for previous period."""
        try:
            if 'timestamp' not in data_df.columns:
                return None
            
            data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])
            current_time = datetime.now()
            
            # Define previous period based on current period
            if time_period == 'current_day':
                start_date = (current_time - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
            
            elif time_period == 'current_week':
                current_week_start = current_time - timedelta(days=current_time.weekday())
                start_date = current_week_start - timedelta(days=7)
                end_date = current_week_start
            
            elif time_period == 'current_month':
                current_month_start = current_time.replace(day=1)
                if current_month_start.month == 1:
                    start_date = current_month_start.replace(year=current_month_start.year - 1, month=12)
                else:
                    start_date = current_month_start.replace(month=current_month_start.month - 1)
                end_date = current_month_start
            
            else:
                # For other periods, use last 30 days as previous period
                start_date = current_time - timedelta(days=60)
                end_date = current_time - timedelta(days=30)
            
            # Filter data for previous period
            previous_data = data_df[
                (data_df['timestamp'] >= start_date) & 
                (data_df['timestamp'] < end_date)
            ]
            
            if previous_data.empty:
                return None
            
            # Calculate value for previous period
            return self._perform_calculation(previous_data, kpi_def)
            
        except Exception as e:
            logger.error(f"Previous period calculation failed: {e}")
            return None
    
    def _determine_kpi_status(self, kpi_def: KPIDefinition, 
                             current_value: float, 
                             target_achievement: Optional[float]) -> str:
        """Determine KPI status based on thresholds."""
        try:
            # Check critical threshold
            if kpi_def.critical_threshold is not None:
                if current_value <= kpi_def.critical_threshold:
                    return 'critical'
            
            # Check warning threshold
            if kpi_def.warning_threshold is not None:
                if current_value <= kpi_def.warning_threshold:
                    return 'warning'
            
            # Check target achievement
            if target_achievement is not None:
                if target_achievement >= 100:
                    return 'on_target'
                elif target_achievement >= 80:
                    return 'approaching_target'
                else:
                    return 'below_target'
            
            return 'normal'
            
        except Exception as e:
            logger.error(f"KPI status determination failed: {e}")
            return 'unknown'
    
    def _determine_trend(self, current_value: float, 
                        previous_value: Optional[float], 
                        change_percentage: Optional[float]) -> str:
        """Determine trend direction."""
        try:
            if change_percentage is None or previous_value is None:
                return 'stable'
            
            if abs(change_percentage) < 2:  # Less than 2% change
                return 'stable'
            elif change_percentage > 0:
                return 'up'
            else:
                return 'down'
                
        except Exception as e:
            logger.error(f"Trend determination failed: {e}")
            return 'stable'
    
    def _extract_data_points(self, data_df: pd.DataFrame, 
                           kpi_def: KPIDefinition) -> List[Dict[str, Any]]:
        """Extract data points for visualization."""
        try:
            data_points = []
            
            if 'timestamp' in data_df.columns:
                data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])
                data_df['date'] = data_df['timestamp'].dt.date
                
                # Group by date for trend visualization
                daily_data = data_df.groupby('date').agg({
                    'user_id': 'nunique' if 'user_id' in data_df.columns else 'count',
                    'event_type': 'count'
                }).reset_index()
                
                for _, row in daily_data.iterrows():
                    data_points.append({
                        'date': row['date'].isoformat(),
                        'value': row.iloc[1],  # First aggregated column
                        'count': row.iloc[2] if len(row) > 2 else row.iloc[1]
                    })
            
            return data_points[:30]  # Limit to last 30 points
            
        except Exception as e:
            logger.error(f"Data points extraction failed: {e}")
            return []

class VisualizationGenerator:
    """Generates charts and visualizations for BI reports."""
    
    def __init__(self):
        self.color_palette = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', 
                             '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
        
        # Set style
        plt.style.use('seaborn-v0_8')
        sns.set_palette(self.color_palette)
    
    def generate_kpi_card(self, kpi_result: KPIResult, kpi_def: KPIDefinition) -> str:
        """Generate KPI card visualization."""
        try:
            fig, ax = plt.subplots(figsize=(6, 4))
            
            # Format value based on KPI type
            formatted_value = self._format_kpi_value(kpi_result.value, kpi_def.format_type)
            
            # Create card layout
            ax.text(0.5, 0.7, kpi_def.kpi_name, ha='center', va='center', 
                   fontsize=14, fontweight='bold', transform=ax.transAxes)
            
            ax.text(0.5, 0.5, formatted_value, ha='center', va='center', 
                   fontsize=24, fontweight='bold', transform=ax.transAxes)
            
            # Add change indicator
            if kpi_result.change_percentage is not None:
                change_color = 'green' if kpi_result.change_percentage > 0 else 'red'
                change_text = f"{kpi_result.change_percentage:+.1f}%"
                ax.text(0.5, 0.3, change_text, ha='center', va='center', 
                       fontsize=12, color=change_color, transform=ax.transAxes)
            
            # Add status indicator
            status_color = self._get_status_color(kpi_result.status)
            ax.add_patch(plt.Rectangle((0.02, 0.02), 0.96, 0.96, 
                                     fill=False, edgecolor=status_color, linewidth=3))
            
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            ax.axis('off')
            
            # Convert to base64
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', bbox_inches='tight', dpi=150)
            buffer.seek(0)
            plot_data = buffer.getvalue()
            buffer.close()
            plt.close(fig)
            
            return base64.b64encode(plot_data).decode()
            
        except Exception as e:
            logger.error(f"KPI card generation failed: {e}")
            return ""
    
    def generate_trend_chart(self, kpi_result: KPIResult, kpi_def: KPIDefinition) -> str:
        """Generate trend chart for KPI."""
        try:
            if not kpi_result.data_points:
                return ""
            
            # Extract data for plotting
            dates = [pd.to_datetime(dp['date']) for dp in kpi_result.data_points]
            values = [dp['value'] for dp in kpi_result.data_points]
            
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # Plot trend line
            ax.plot(dates, values, marker='o', linewidth=2, markersize=4)
            
            # Add target line if available
            if kpi_def.target_value is not None:
                ax.axhline(y=kpi_def.target_value, color='red', linestyle='--', 
                          alpha=0.7, label='Target')
            
            # Formatting
            ax.set_title(f'{kpi_def.kpi_name} Trend', fontsize=14, fontweight='bold')
            ax.set_xlabel('Date')
            ax.set_ylabel(self._get_ylabel(kpi_def.format_type))
            ax.grid(True, alpha=0.3)
            
            # Format y-axis based on KPI type
            if kpi_def.format_type == 'percentage':
                ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.1f}%'))
            elif kpi_def.format_type == 'currency':
                ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
            
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            # Convert to base64
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', bbox_inches='tight', dpi=150)
            buffer.seek(0)
            plot_data = buffer.getvalue()
            buffer.close()
            plt.close(fig)
            
            return base64.b64encode(plot_data).decode()
            
        except Exception as e:
            logger.error(f"Trend chart generation failed: {e}")
            return ""
    
    def generate_comparison_chart(self, kpi_results: Dict[str, KPIResult], 
                                chart_type: str = 'bar') -> str:
        """Generate comparison chart for multiple KPIs."""
        try:
            if not kpi_results:
                return ""
            
            # Extract data
            kpi_names = list(kpi_results.keys())
            kpi_values = [result.value for result in kpi_results.values()]
            
            fig, ax = plt.subplots(figsize=(12, 6))
            
            if chart_type == 'bar':
                bars = ax.bar(kpi_names, kpi_values, color=self.color_palette[:len(kpi_names)])
                
                # Add value labels on bars
                for bar, value in zip(bars, kpi_values):
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                           f'{value:.1f}', ha='center', va='bottom')
            
            elif chart_type == 'horizontal_bar':
                bars = ax.barh(kpi_names, kpi_values, color=self.color_palette[:len(kpi_names)])
                
                # Add value labels
                for bar, value in zip(bars, kpi_values):
                    width = bar.get_width()
                    ax.text(width, bar.get_y() + bar.get_height()/2.,
                           f'{value:.1f}', ha='left', va='center')
            
            ax.set_title('KPI Comparison', fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3)
            
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            # Convert to base64
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', bbox_inches='tight', dpi=150)
            buffer.seek(0)
            plot_data = buffer.getvalue()
            buffer.close()
            plt.close(fig)
            
            return base64.b64encode(plot_data).decode()
            
        except Exception as e:
            logger.error(f"Comparison chart generation failed: {e}")
            return ""
    
    def generate_dashboard_overview(self, kpi_results: Dict[str, KPIResult]) -> str:
        """Generate comprehensive dashboard overview."""
        try:
            # Create subplot grid
            n_kpis = len(kpi_results)
            n_cols = min(3, n_kpis)
            n_rows = (n_kpis + n_cols - 1) // n_cols
            
            fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 5 * n_rows))
            
            if n_kpis == 1:
                axes = [axes]
            elif n_rows == 1:
                axes = [axes]
            else:
                axes = axes.flatten()
            
            for i, (kpi_id, kpi_result) in enumerate(kpi_results.items()):
                if i < len(axes):
                    ax = axes[i]
                    
                    # Create mini trend chart
                    if kpi_result.data_points:
                        dates = [pd.to_datetime(dp['date']) for dp in kpi_result.data_points[-7:]]  # Last 7 days
                        values = [dp['value'] for dp in kpi_result.data_points[-7:]]
                        
                        ax.plot(dates, values, marker='o', linewidth=2)
                        ax.set_title(f'{kpi_id}: {kpi_result.value:.1f}', fontweight='bold')
                        ax.grid(True, alpha=0.3)
                        
                        # Color based on trend
                        if kpi_result.trend == 'up':
                            ax.plot(dates, values, color='green', marker='o', linewidth=2)
                        elif kpi_result.trend == 'down':
                            ax.plot(dates, values, color='red', marker='o', linewidth=2)
                        else:
                            ax.plot(dates, values, color='blue', marker='o', linewidth=2)
                    else:
                        # Show just the value
                        ax.text(0.5, 0.5, f'{kpi_result.value:.1f}', 
                               ha='center', va='center', fontsize=20, fontweight='bold')
                        ax.set_title(kpi_id, fontweight='bold')
                        ax.axis('off')
            
            # Hide unused subplots
            for i in range(n_kpis, len(axes)):
                axes[i].axis('off')
            
            plt.suptitle('Executive Dashboard Overview', fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            # Convert to base64
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', bbox_inches='tight', dpi=150)
            buffer.seek(0)
            plot_data = buffer.getvalue()
            buffer.close()
            plt.close(fig)
            
            return base64.b64encode(plot_data).decode()
            
        except Exception as e:
            logger.error(f"Dashboard overview generation failed: {e}")
            return ""
    
    def _format_kpi_value(self, value: float, format_type: str) -> str:
        """Format KPI value based on type."""
        try:
            if format_type == 'percentage':
                return f"{value:.1f}%"
            elif format_type == 'currency':
                return f"${value:,.0f}"
            elif format_type == 'count':
                return f"{int(value):,}"
            else:
                return f"{value:.1f}"
        except Exception:
            return str(value)
    
    def _get_status_color(self, status: str) -> str:
        """Get color based on KPI status."""
        status_colors = {
            'on_target': 'green',
            'approaching_target': 'orange',
            'below_target': 'red',
            'warning': 'orange',
            'critical': 'red',
            'normal': 'blue'
        }
        return status_colors.get(status, 'gray')
    
    def _get_ylabel(self, format_type: str) -> str:
        """Get y-axis label based on format type."""
        if format_type == 'percentage':
            return 'Percentage (%)'
        elif format_type == 'currency':
            return 'Amount ($)'
        elif format_type == 'count':
            return 'Count'
        else:
            return 'Value'

class ReportGenerator:
    """Generates comprehensive BI reports with insights and recommendations."""
    
    def __init__(self):
        self.kpi_calculator = KPICalculator()
        self.visualization_generator = VisualizationGenerator()
        self.insight_engine = InsightEngine()
        
    def generate_report(self, data_df: pd.DataFrame, 
                       report_definition: ReportDefinition,
                       kpi_definitions: List[KPIDefinition]) -> GeneratedReport:
        """Generate a complete BI report."""
        try:
            if bi_reports_generated:
                bi_reports_generated.labels(report_type=report_definition.report_type).inc()
            
            # Calculate KPIs
            logger.info(f"Calculating KPIs for report {report_definition.report_id}...")
            kpi_results = self.kpi_calculator.calculate_kpis(
                data_df, 
                [kpi for kpi in kpi_definitions if kpi.kpi_id in report_definition.kpi_ids]
            )
            
            # Generate visualizations
            logger.info("Generating visualizations...")
            visualizations = self._generate_report_visualizations(kpi_results, kpi_definitions)
            
            # Generate insights
            logger.info("Generating insights...")
            insights = self.insight_engine.generate_insights(kpi_results, data_df)
            
            # Generate recommendations
            recommendations = self.insight_engine.generate_recommendations(kpi_results, insights)
            
            # Create executive summary
            executive_summary = self._create_executive_summary(kpi_results, insights)
            
            # Compile report data
            report_data = self._compile_report_data(
                data_df, kpi_results, report_definition
            )
            
            generated_report = GeneratedReport(
                report_id=report_definition.report_id,
                generation_timestamp=datetime.now(),
                report_data=report_data,
                visualizations=visualizations,
                insights=insights,
                recommendations=recommendations,
                kpi_summary=kpi_results,
                executive_summary=executive_summary,
                export_formats=['pdf', 'excel', 'json']
            )
            
            logger.info(f"Report {report_definition.report_id} generated successfully")
            return generated_report
            
        except Exception as e:
            logger.error(f"Report generation failed: {e}")
            return GeneratedReport(
                report_id=report_definition.report_id,
                generation_timestamp=datetime.now(),
                report_data={},
                visualizations={},
                insights=[],
                recommendations=[],
                kpi_summary={},
                executive_summary="Report generation failed",
                export_formats=[]
            )
    
    def generate_executive_report(self, data_df: pd.DataFrame,
                                 kpi_definitions: List[KPIDefinition]) -> Dict[str, Any]:
        """Generate executive-level summary report."""
        try:
            if executive_reports:
                executive_reports.inc()
            
            # Calculate all KPIs
            kpi_results = self.kpi_calculator.calculate_kpis(data_df, kpi_definitions)
            
            # Categorize KPIs by business area
            categorized_kpis = self._categorize_kpis(kpi_results, kpi_definitions)
            
            # Generate executive dashboard
            executive_dashboard = self.visualization_generator.generate_dashboard_overview(kpi_results)
            
            # Generate high-level insights
            executive_insights = self.insight_engine.generate_executive_insights(
                kpi_results, categorized_kpis
            )
            
            # Create strategic recommendations
            strategic_recommendations = self.insight_engine.generate_strategic_recommendations(
                kpi_results, executive_insights
            )
            
            # Calculate business health score
            business_health_score = self._calculate_business_health_score(kpi_results)
            
            # Create executive summary
            executive_summary = self._create_detailed_executive_summary(
                kpi_results, executive_insights, business_health_score
            )
            
            executive_report = {
                'report_type': 'executive_summary',
                'generation_timestamp': datetime.now().isoformat(),
                'business_health_score': business_health_score,
                'executive_summary': executive_summary,
                'key_metrics': self._extract_key_metrics(kpi_results),
                'categorized_performance': categorized_kpis,
                'executive_dashboard': executive_dashboard,
                'strategic_insights': executive_insights,
                'strategic_recommendations': strategic_recommendations,
                'performance_alerts': self._identify_performance_alerts(kpi_results),
                'growth_opportunities': self._identify_growth_opportunities(kpi_results)
            }
            
            logger.info("Executive report generated successfully")
            return executive_report
            
        except Exception as e:
            logger.error(f"Executive report generation failed: {e}")
            return {'error': str(e)}
    
    def _generate_report_visualizations(self, kpi_results: Dict[str, KPIResult],
                                      kpi_definitions: List[KPIDefinition]) -> Dict[str, str]:
        """Generate all visualizations for the report."""
        visualizations = {}
        
        try:
            # Generate individual KPI cards
            for kpi_id, kpi_result in kpi_results.items():
                kpi_def = next((kpi for kpi in kpi_definitions if kpi.kpi_id == kpi_id), None)
                if kpi_def:
                    card_viz = self.visualization_generator.generate_kpi_card(kpi_result, kpi_def)
                    if card_viz:
                        visualizations[f"kpi_card_{kpi_id}"] = card_viz
                    
                    trend_viz = self.visualization_generator.generate_trend_chart(kpi_result, kpi_def)
                    if trend_viz:
                        visualizations[f"trend_{kpi_id}"] = trend_viz
            
            # Generate comparison charts
            if len(kpi_results) > 1:
                comparison_viz = self.visualization_generator.generate_comparison_chart(kpi_results)
                if comparison_viz:
                    visualizations["kpi_comparison"] = comparison_viz
            
            # Generate dashboard overview
            dashboard_viz = self.visualization_generator.generate_dashboard_overview(kpi_results)
            if dashboard_viz:
                visualizations["dashboard_overview"] = dashboard_viz
            
            return visualizations
            
        except Exception as e:
            logger.error(f"Report visualization generation failed: {e}")
            return {}
    
    def _compile_report_data(self, data_df: pd.DataFrame, 
                           kpi_results: Dict[str, KPIResult],
                           report_definition: ReportDefinition) -> Dict[str, Any]:
        """Compile comprehensive report data."""
        try:
            # Basic statistics
            total_records = len(data_df)
            date_range = self._get_date_range(data_df)
            
            # User metrics
            user_metrics = self._calculate_user_metrics(data_df)
            
            # Performance summary
            performance_summary = self._create_performance_summary(kpi_results)
            
            # Trend analysis
            trend_analysis = self._analyze_trends(kpi_results)
            
            report_data = {
                'report_metadata': {
                    'report_id': report_definition.report_id,
                    'report_name': report_definition.report_name,
                    'report_type': report_definition.report_type,
                    'generation_time': datetime.now().isoformat(),
                    'data_period': date_range,
                    'total_records': total_records
                },
                'user_metrics': user_metrics,
                'performance_summary': performance_summary,
                'trend_analysis': trend_analysis,
                'kpi_details': {kpi_id: asdict(result) for kpi_id, result in kpi_results.items()},
                'data_quality': self._assess_data_quality(data_df)
            }
            
            return report_data
            
        except Exception as e:
            logger.error(f"Report data compilation failed: {e}")
            return {}
    
    def _categorize_kpis(self, kpi_results: Dict[str, KPIResult],
                        kpi_definitions: List[KPIDefinition]) -> Dict[str, Dict[str, Any]]:
        """Categorize KPIs by business area."""
        categorized = defaultdict(dict)
        
        try:
            for kpi_id, kpi_result in kpi_results.items():
                kpi_def = next((kpi for kpi in kpi_definitions if kpi.kpi_id == kpi_id), None)
                if kpi_def:
                    category = kpi_def.category
                    categorized[category][kpi_id] = {
                        'name': kpi_def.kpi_name,
                        'value': kpi_result.value,
                        'status': kpi_result.status,
                        'trend': kpi_result.trend,
                        'change_percentage': kpi_result.change_percentage,
                        'target_achievement': kpi_result.target_achievement
                    }
            
            return dict(categorized)
            
        except Exception as e:
            logger.error(f"KPI categorization failed: {e}")
            return {}
    
    def _calculate_business_health_score(self, kpi_results: Dict[str, KPIResult]) -> Dict[str, Any]:
        """Calculate overall business health score."""
        try:
            if not kpi_results:
                return {'score': 0, 'status': 'unknown', 'factors': []}
            
            # Score factors
            target_achievements = []
            status_scores = []
            trend_scores = []
            
            for kpi_result in kpi_results.values():
                # Target achievement factor
                if kpi_result.target_achievement is not None:
                    target_achievements.append(min(kpi_result.target_achievement / 100, 1.0))
                
                # Status factor
                status_score = {
                    'on_target': 1.0,
                    'approaching_target': 0.8,
                    'below_target': 0.4,
                    'warning': 0.6,
                    'critical': 0.2,
                    'normal': 0.7
                }.get(kpi_result.status, 0.5)
                status_scores.append(status_score)
                
                # Trend factor
                trend_score = {
                    'up': 1.0,
                    'stable': 0.7,
                    'down': 0.3
                }.get(kpi_result.trend, 0.5)
                trend_scores.append(trend_score)
            
            # Calculate weighted score
            weights = {'target': 0.4, 'status': 0.4, 'trend': 0.2}
            
            target_score = np.mean(target_achievements) if target_achievements else 0.7
            status_score = np.mean(status_scores) if status_scores else 0.7
            trend_score = np.mean(trend_scores) if trend_scores else 0.7
            
            overall_score = (
                weights['target'] * target_score +
                weights['status'] * status_score +
                weights['trend'] * trend_score
            )
            
            # Determine health status
            if overall_score >= 0.8:
                health_status = 'excellent'
            elif overall_score >= 0.6:
                health_status = 'good'
            elif overall_score >= 0.4:
                health_status = 'fair'
            else:
                health_status = 'poor'
            
            return {
                'score': overall_score * 100,  # Convert to percentage
                'status': health_status,
                'factors': {
                    'target_achievement': target_score * 100,
                    'status_performance': status_score * 100,
                    'trend_momentum': trend_score * 100
                }
            }
            
        except Exception as e:
            logger.error(f"Business health score calculation failed: {e}")
            return {'score': 0, 'status': 'unknown', 'factors': []}
    
    def _create_executive_summary(self, kpi_results: Dict[str, KPIResult],
                                 insights: List[Dict[str, Any]]) -> str:
        """Create executive summary text."""
        try:
            summary_parts = []
            
            # Business performance overview
            total_kpis = len(kpi_results)
            on_target_kpis = sum(1 for result in kpi_results.values() 
                               if result.status in ['on_target', 'normal'])
            
            summary_parts.append(
                f"Business Performance Overview: {on_target_kpis}/{total_kpis} key metrics are performing on target."
            )
            
            # Trending insights
            trending_up = sum(1 for result in kpi_results.values() if result.trend == 'up')
            trending_down = sum(1 for result in kpi_results.values() if result.trend == 'down')
            
            if trending_up > trending_down:
                summary_parts.append(
                    f"Positive momentum observed with {trending_up} metrics trending upward."
                )
            elif trending_down > trending_up:
                summary_parts.append(
                    f"Caution advised: {trending_down} metrics showing downward trends."
                )
            
            # Key insights
            high_priority_insights = [insight for insight in insights 
                                    if insight.get('priority') == 'high']
            
            if high_priority_insights:
                summary_parts.append(
                    f"Key Focus Areas: {len(high_priority_insights)} high-priority insights identified requiring immediate attention."
                )
            
            return " ".join(summary_parts)
            
        except Exception as e:
            logger.error(f"Executive summary creation failed: {e}")
            return "Executive summary generation encountered an error."
    
    def _create_detailed_executive_summary(self, kpi_results: Dict[str, KPIResult],
                                         executive_insights: List[Dict[str, Any]],
                                         business_health_score: Dict[str, Any]) -> str:
        """Create detailed executive summary."""
        try:
            summary_sections = []
            
            # Business health overview
            health_score = business_health_score.get('score', 0)
            health_status = business_health_score.get('status', 'unknown')
            
            summary_sections.append(
                f"BUSINESS HEALTH: Overall business health score is {health_score:.0f}% ({health_status.upper()}). "
            )
            
            # Performance highlights
            top_performers = [
                (kpi_id, result) for kpi_id, result in kpi_results.items()
                if result.status == 'on_target' and result.trend == 'up'
            ]
            
            if top_performers:
                summary_sections.append(
                    f"PERFORMANCE HIGHLIGHTS: {len(top_performers)} metrics are exceeding targets with positive momentum. "
                )
            
            # Areas of concern
            concerns = [
                (kpi_id, result) for kpi_id, result in kpi_results.items()
                if result.status in ['critical', 'warning'] or result.trend == 'down'
            ]
            
            if concerns:
                summary_sections.append(
                    f"AREAS OF CONCERN: {len(concerns)} metrics require immediate attention due to performance issues or negative trends. "
                )
            
            # Strategic insights
            strategic_insights = [insight for insight in executive_insights 
                                if insight.get('type') == 'strategic']
            
            if strategic_insights:
                summary_sections.append(
                    f"STRATEGIC INSIGHTS: {len(strategic_insights)} strategic opportunities identified for business growth and optimization. "
                )
            
            # Growth trajectory
            growth_metrics = [
                result for result in kpi_results.values()
                if result.change_percentage is not None and result.change_percentage > 10
            ]
            
            if growth_metrics:
                avg_growth = np.mean([result.change_percentage for result in growth_metrics])
                summary_sections.append(
                    f"GROWTH TRAJECTORY: Strong growth momentum with {len(growth_metrics)} metrics showing >10% improvement (avg: {avg_growth:.1f}%). "
                )
            
            return "".join(summary_sections)
            
        except Exception as e:
            logger.error(f"Detailed executive summary creation failed: {e}")
            return "Executive summary generation encountered an error."
    
    def _extract_key_metrics(self, kpi_results: Dict[str, KPIResult]) -> Dict[str, Any]:
        """Extract key metrics for executive view."""
        try:
            key_metrics = {}
            
            # Top performing metrics
            top_performers = sorted(
                kpi_results.items(),
                key=lambda x: x[1].target_achievement or 0,
                reverse=True
            )[:5]
            
            key_metrics['top_performers'] = [
                {
                    'kpi_id': kpi_id,
                    'value': result.value,
                    'target_achievement': result.target_achievement,
                    'trend': result.trend
                }
                for kpi_id, result in top_performers
            ]
            
            # Concerning metrics
            concerning = [
                (kpi_id, result) for kpi_id, result in kpi_results.items()
                if result.status in ['critical', 'warning']
            ]
            
            key_metrics['concerning_metrics'] = [
                {
                    'kpi_id': kpi_id,
                    'value': result.value,
                    'status': result.status,
                    'trend': result.trend
                }
                for kpi_id, result in concerning[:5]
            ]
            
            # Trending metrics
            trending = sorted(
                [(kpi_id, result) for kpi_id, result in kpi_results.items()
                 if result.change_percentage is not None],
                key=lambda x: abs(x[1].change_percentage),
                reverse=True
            )[:5]
            
            key_metrics['trending_metrics'] = [
                {
                    'kpi_id': kpi_id,
                    'value': result.value,
                    'change_percentage': result.change_percentage,
                    'trend': result.trend
                }
                for kpi_id, result in trending
            ]
            
            return key_metrics
            
        except Exception as e:
            logger.error(f"Key metrics extraction failed: {e}")
            return {}
    
    def _identify_performance_alerts(self, kpi_results: Dict[str, KPIResult]) -> List[Dict[str, Any]]:
        """Identify performance alerts requiring immediate attention."""
        alerts = []
        
        try:
            for kpi_id, result in kpi_results.items():
                # Critical status alert
                if result.status == 'critical':
                    alerts.append({
                        'type': 'critical_performance',
                        'kpi_id': kpi_id,
                        'message': f"{kpi_id} is in critical state with value {result.value:.1f}",
                        'severity': 'high',
                        'action_required': 'immediate'
                    })
                
                # Large negative change alert
                if (result.change_percentage is not None and 
                    result.change_percentage < -20):
                    alerts.append({
                        'type': 'significant_decline',
                        'kpi_id': kpi_id,
                        'message': f"{kpi_id} declined by {abs(result.change_percentage):.1f}%",
                        'severity': 'medium',
                        'action_required': 'investigation'
                    })
                
                # Target miss alert
                if (result.target_achievement is not None and 
                    result.target_achievement < 70):
                    alerts.append({
                        'type': 'target_miss',
                        'kpi_id': kpi_id,
                        'message': f"{kpi_id} is at {result.target_achievement:.1f}% of target",
                        'severity': 'medium',
                        'action_required': 'optimization'
                    })
            
            # Sort by severity
            severity_order = {'high': 3, 'medium': 2, 'low': 1}
            alerts.sort(key=lambda x: severity_order.get(x['severity'], 0), reverse=True)
            
            return alerts[:10]  # Top 10 alerts
            
        except Exception as e:
            logger.error(f"Performance alerts identification failed: {e}")
            return []
    
    def _identify_growth_opportunities(self, kpi_results: Dict[str, KPIResult]) -> List[Dict[str, Any]]:
        """Identify growth opportunities."""
        opportunities = []
        
        try:
            for kpi_id, result in kpi_results.items():
                # Strong positive trend opportunity
                if (result.trend == 'up' and result.change_percentage is not None and 
                    result.change_percentage > 15):
                    opportunities.append({
                        'type': 'momentum_opportunity',
                        'kpi_id': kpi_id,
                        'description': f"{kpi_id} showing strong growth ({result.change_percentage:.1f}%)",
                        'recommendation': 'Increase investment to capitalize on momentum',
                        'impact_potential': 'high'
                    })
                
                # Near-target opportunity
                if (result.target_achievement is not None and 
                    85 <= result.target_achievement < 100):
                    opportunities.append({
                        'type': 'target_opportunity',
                        'kpi_id': kpi_id,
                        'description': f"{kpi_id} is close to target ({result.target_achievement:.1f}%)",
                        'recommendation': 'Small optimization could achieve target',
                        'impact_potential': 'medium'
                    })
                
                # Improvement from low base opportunity
                if (result.status in ['below_target', 'warning'] and 
                    result.trend == 'up'):
                    opportunities.append({
                        'type': 'recovery_opportunity',
                        'kpi_id': kpi_id,
                        'description': f"{kpi_id} recovering from low performance",
                        'recommendation': 'Continue current improvement strategies',
                        'impact_potential': 'medium'
                    })
            
            return opportunities[:10]  # Top 10 opportunities
            
        except Exception as e:
            logger.error(f"Growth opportunities identification failed: {e}")
            return []
    
    def _get_date_range(self, data_df: pd.DataFrame) -> Dict[str, str]:
        """Get date range from data."""
        try:
            if 'timestamp' in data_df.columns:
                data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])
                return {
                    'start_date': data_df['timestamp'].min().isoformat(),
                    'end_date': data_df['timestamp'].max().isoformat()
                }
            return {'start_date': 'unknown', 'end_date': 'unknown'}
        except Exception:
            return {'start_date': 'unknown', 'end_date': 'unknown'}
    
    def _calculate_user_metrics(self, data_df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate user-related metrics."""
        try:
            metrics = {}
            
            if 'user_id' in data_df.columns:
                metrics['total_users'] = data_df['user_id'].nunique()
                metrics['total_events'] = len(data_df)
                metrics['events_per_user'] = metrics['total_events'] / metrics['total_users']
            
            if 'event_type' in data_df.columns:
                metrics['unique_event_types'] = data_df['event_type'].nunique()
                metrics['top_events'] = data_df['event_type'].value_counts().head(5).to_dict()
            
            return metrics
        except Exception as e:
            logger.error(f"User metrics calculation failed: {e}")
            return {}
    
    def _create_performance_summary(self, kpi_results: Dict[str, KPIResult]) -> Dict[str, Any]:
        """Create performance summary."""
        try:
            summary = {
                'total_kpis': len(kpi_results),
                'on_target': sum(1 for r in kpi_results.values() if r.status == 'on_target'),
                'warning': sum(1 for r in kpi_results.values() if r.status == 'warning'),
                'critical': sum(1 for r in kpi_results.values() if r.status == 'critical'),
                'trending_up': sum(1 for r in kpi_results.values() if r.trend == 'up'),
                'trending_down': sum(1 for r in kpi_results.values() if r.trend == 'down'),
                'stable': sum(1 for r in kpi_results.values() if r.trend == 'stable')
            }
            
            return summary
        except Exception as e:
            logger.error(f"Performance summary creation failed: {e}")
            return {}
    
    def _analyze_trends(self, kpi_results: Dict[str, KPIResult]) -> Dict[str, Any]:
        """Analyze trends across KPIs."""
        try:
            trends = {}
            
            # Overall trend direction
            trend_counts = Counter([result.trend for result in kpi_results.values()])
            trends['trend_distribution'] = dict(trend_counts)
            
            # Average change percentage
            changes = [result.change_percentage for result in kpi_results.values() 
                      if result.change_percentage is not None]
            if changes:
                trends['avg_change_percentage'] = np.mean(changes)
                trends['median_change_percentage'] = np.median(changes)
            
            # Target achievement analysis
            achievements = [result.target_achievement for result in kpi_results.values() 
                          if result.target_achievement is not None]
            if achievements:
                trends['avg_target_achievement'] = np.mean(achievements)
                trends['targets_exceeded'] = sum(1 for a in achievements if a >= 100)
            
            return trends
        except Exception as e:
            logger.error(f"Trends analysis failed: {e}")
            return {}
    
    def _assess_data_quality(self, data_df: pd.DataFrame) -> Dict[str, Any]:
        """Assess data quality metrics."""
        try:
            quality = {}
            
            # Basic quality metrics
            quality['total_records'] = len(data_df)
            quality['missing_values'] = data_df.isnull().sum().to_dict()
            quality['duplicate_records'] = data_df.duplicated().sum()
            
            # Data completeness
            completeness = ((data_df.count() / len(data_df)) * 100).to_dict()
            quality['completeness_percentage'] = completeness
            
            # Data freshness
            if 'timestamp' in data_df.columns:
                data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])
                latest_record = data_df['timestamp'].max()
                data_age_hours = (datetime.now() - latest_record).total_seconds() / 3600
                quality['data_freshness_hours'] = data_age_hours
            
            return quality
        except Exception as e:
            logger.error(f"Data quality assessment failed: {e}")
            return {}

class InsightEngine:
    """Generates insights and recommendations from KPI analysis."""
    
    def __init__(self):
        self.insight_threshold_high = 0.15  # 15% change threshold for high priority
        self.insight_threshold_medium = 0.10  # 10% change threshold for medium priority
    
    def generate_insights(self, kpi_results: Dict[str, KPIResult], 
                         data_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Generate actionable insights from KPI results."""
        try:
            insights = []
            
            # Performance insights
            insights.extend(self._generate_performance_insights(kpi_results))
            
            # Trend insights
            insights.extend(self._generate_trend_insights(kpi_results))
            
            # Comparative insights
            insights.extend(self._generate_comparative_insights(kpi_results))
            
            # Data-driven insights
            insights.extend(self._generate_data_insights(data_df))
            
            # Prioritize insights
            insights = self._prioritize_insights(insights)
            
            return insights[:20]  # Top 20 insights
            
        except Exception as e:
            logger.error(f"Insight generation failed: {e}")
            return []
    
    def generate_executive_insights(self, kpi_results: Dict[str, KPIResult],
                                  categorized_kpis: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate executive-level strategic insights."""
        try:
            insights = []
            
            # Strategic performance insights
            insights.extend(self._generate_strategic_insights(categorized_kpis))
            
            # Cross-functional insights
            insights.extend(self._generate_cross_functional_insights(categorized_kpis))
            
            # Market opportunity insights
            insights.extend(self._generate_opportunity_insights(kpi_results))
            
            # Risk assessment insights
            insights.extend(self._generate_risk_insights(kpi_results))
            
            return insights
            
        except Exception as e:
            logger.error(f"Executive insight generation failed: {e}")
            return []
    
    def generate_recommendations(self, kpi_results: Dict[str, KPIResult],
                               insights: List[Dict[str, Any]]) -> List[str]:
        """Generate actionable recommendations."""
        try:
            recommendations = []
            
            # KPI-based recommendations
            for kpi_id, result in kpi_results.items():
                if result.status == 'critical':
                    recommendations.append(
                        f"URGENT: Address critical performance in {kpi_id} - implement immediate corrective actions"
                    )
                elif result.status == 'warning':
                    recommendations.append(
                        f"ATTENTION: Monitor {kpi_id} closely and implement preventive measures"
                    )
                elif result.trend == 'up' and result.status == 'on_target':
                    recommendations.append(
                        f"OPPORTUNITY: Leverage positive momentum in {kpi_id} - consider scaling successful strategies"
                    )
            
            # Insight-based recommendations
            high_priority_insights = [i for i in insights if i.get('priority') == 'high']
            for insight in high_priority_insights[:5]:
                if 'recommendation' in insight:
                    recommendations.append(insight['recommendation'])
            
            return recommendations[:15]  # Top 15 recommendations
            
        except Exception as e:
            logger.error(f"Recommendations generation failed: {e}")
            return []
    
    def generate_strategic_recommendations(self, kpi_results: Dict[str, KPIResult],
                                         executive_insights: List[Dict[str, Any]]) -> List[str]:
        """Generate strategic-level recommendations."""
        try:
            recommendations = []
            
            # Strategic growth recommendations
            growth_opportunities = [kpi for kpi, result in kpi_results.items() 
                                  if result.trend == 'up' and result.change_percentage and result.change_percentage > 20]
            
            if growth_opportunities:
                recommendations.append(
                    f"STRATEGIC GROWTH: Capitalize on high-growth areas ({', '.join(growth_opportunities[:3])}) by reallocating resources and scaling successful initiatives"
                )
            
            # Risk mitigation recommendations
            risk_areas = [kpi for kpi, result in kpi_results.items() 
                         if result.status in ['critical', 'warning']]
            
            if len(risk_areas) > 2:
                recommendations.append(
                    f"RISK MITIGATION: Develop comprehensive improvement plan for underperforming areas - consider organizational restructuring if needed"
                )
            
            # Innovation recommendations
            stable_metrics = [kpi for kpi, result in kpi_results.items() 
                            if result.trend == 'stable' and result.status == 'on_target']
            
            if len(stable_metrics) > 3:
                recommendations.append(
                    "INNOVATION OPPORTUNITY: Stable performance areas present opportunities for breakthrough improvements through innovation and process optimization"
                )
            
            # Market positioning recommendations
            top_performers = sorted(kpi_results.items(), 
                                  key=lambda x: x[1].target_achievement or 0, 
                                  reverse=True)[:3]
            
            if top_performers and top_performers[0][1].target_achievement and top_performers[0][1].target_achievement > 120:
                recommendations.append(
                    f"MARKET LEADERSHIP: Exceptional performance in {top_performers[0][0]} positions company for market leadership - consider competitive expansion"
                )
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Strategic recommendations generation failed: {e}")
            return []
    
    def _generate_performance_insights(self, kpi_results: Dict[str, KPIResult]) -> List[Dict[str, Any]]:
        """Generate performance-related insights."""
        insights = []
        
        try:
            # Outstanding performance insight
            exceptional_performers = [
                (kpi_id, result) for kpi_id, result in kpi_results.items()
                if (result.target_achievement and result.target_achievement > 130) or
                   (result.change_percentage and result.change_percentage > 25)
            ]
            
            if exceptional_performers:
                insights.append({
                    'type': 'exceptional_performance',
                    'title': 'Outstanding Performance Detected',
                    'description': f"{len(exceptional_performers)} metrics showing exceptional performance",
                    'details': [f"{kpi}: {result.value:.1f}" for kpi, result in exceptional_performers[:3]],
                    'priority': 'high',
                    'category': 'performance',
                    'recommendation': 'Analyze success factors and replicate across other areas'
                })
            
            # Performance consistency insight
            consistent_performers = [
                result for result in kpi_results.values()
                if result.status == 'on_target' and abs(result.change_percentage or 0) < 5
            ]
            
            if len(consistent_performers) > len(kpi_results) * 0.6:
                insights.append({
                    'type': 'consistent_performance',
                    'title': 'Strong Performance Consistency',
                    'description': f"{len(consistent_performers)} metrics showing consistent performance",
                    'priority': 'medium',
                    'category': 'performance',
                    'recommendation': 'Maintain current strategies while exploring growth opportunities'
                })
            
            return insights
            
        except Exception as e:
            logger.error(f"Performance insights generation failed: {e}")
            return []
    
    def _generate_trend_insights(self, kpi_results: Dict[str, KPIResult]) -> List[Dict[str, Any]]:
        """Generate trend-related insights."""
        insights = []
        
        try:
            # Strong positive trend insight
            strong_growth = [
                (kpi_id, result) for kpi_id, result in kpi_results.items()
                if result.change_percentage and result.change_percentage > self.insight_threshold_high * 100
            ]
            
            if strong_growth:
                avg_growth = np.mean([result.change_percentage for _, result in strong_growth])
                insights.append({
                    'type': 'strong_growth_trend',
                    'title': 'Strong Growth Momentum',
                    'description': f"{len(strong_growth)} metrics showing strong growth (avg: {avg_growth:.1f}%)",
                    'details': [f"{kpi}: +{result.change_percentage:.1f}%" for kpi, result in strong_growth[:3]],
                    'priority': 'high',
                    'category': 'trend',
                    'recommendation': 'Invest more resources in high-growth areas'
                })
            
            # Concerning downward trend insight
            declining_metrics = [
                (kpi_id, result) for kpi_id, result in kpi_results.items()
                if result.change_percentage and result.change_percentage < -self.insight_threshold_high * 100
            ]
            
            if declining_metrics:
                insights.append({
                    'type': 'declining_trend',
                    'title': 'Concerning Downward Trends',
                    'description': f"{len(declining_metrics)} metrics showing significant decline",
                    'details': [f"{kpi}: {result.change_percentage:.1f}%" for kpi, result in declining_metrics[:3]],
                    'priority': 'high',
                    'category': 'trend',
                    'recommendation': 'Immediate investigation and corrective action required'
                })
            
            return insights
            
        except Exception as e:
            logger.error(f"Trend insights generation failed: {e}")
            return []
    
    def _generate_comparative_insights(self, kpi_results: Dict[str, KPIResult]) -> List[Dict[str, Any]]:
        """Generate comparative insights between KPIs."""
        insights = []
        
        try:
            if len(kpi_results) < 2:
                return insights
            
            # Performance gap insight
            target_achievements = [
                (kpi_id, result.target_achievement) for kpi_id, result in kpi_results.items()
                if result.target_achievement is not None
            ]
            
            if len(target_achievements) > 1:
                sorted_achievements = sorted(target_achievements, key=lambda x: x[1], reverse=True)
                top_performer = sorted_achievements[0]
                bottom_performer = sorted_achievements[-1]
                
                gap = top_performer[1] - bottom_performer[1]
                if gap > 50:  # More than 50% gap
                    insights.append({
                        'type': 'performance_gap',
                        'title': 'Significant Performance Gap',
                        'description': f"Large gap between best ({top_performer[0]}: {top_performer[1]:.1f}%) and worst ({bottom_performer[0]}: {bottom_performer[1]:.1f}%) performing metrics",
                        'priority': 'medium',
                        'category': 'comparative',
                        'recommendation': f'Focus improvement efforts on {bottom_performer[0]} using best practices from {top_performer[0]}'
                    })
            
            return insights
            
        except Exception as e:
            logger.error(f"Comparative insights generation failed: {e}")
            return []
    
    def _generate_data_insights(self, data_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Generate insights from raw data patterns."""
        insights = []
        
        try:
            # Data volume insight
            total_records = len(data_df)
            if total_records > 10000:
                insights.append({
                    'type': 'data_volume',
                    'title': 'High Data Volume',
                    'description': f"Processing {total_records:,} records indicates strong business activity",
                    'priority': 'low',
                    'category': 'data',
                    'recommendation': 'Consider implementing data sampling for faster processing'
                })
            
            # Data freshness insight
            if 'timestamp' in data_df.columns:
                data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])
                latest_record = data_df['timestamp'].max()
                hours_old = (datetime.now() - latest_record).total_seconds() / 3600
                
                if hours_old > 24:
                    insights.append({
                        'type': 'data_freshness',
                        'title': 'Data Freshness Alert',
                        'description': f"Latest data is {hours_old:.1f} hours old",
                        'priority': 'medium',
                        'category': 'data',
                        'recommendation': 'Review data ingestion processes to ensure timely updates'
                    })
            
            return insights
            
        except Exception as e:
            logger.error(f"Data insights generation failed: {e}")
            return []
    
    def _generate_strategic_insights(self, categorized_kpis: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate strategic insights from categorized KPIs."""
        insights = []
        
        try:
            # Cross-category performance analysis
            category_performance = {}
            for category, kpis in categorized_kpis.items():
                if kpis:
                    avg_achievement = np.mean([
                        kpi_data['target_achievement'] for kpi_data in kpis.values()
                        if kpi_data['target_achievement'] is not None
                    ])
                    category_performance[category] = avg_achievement
            
            if category_performance:
                best_category = max(category_performance.items(), key=lambda x: x[1])
                worst_category = min(category_performance.items(), key=lambda x: x[1])
                
                if best_category[1] - worst_category[1] > 30:  # 30% difference
                    insights.append({
                        'type': 'strategic_category_gap',
                        'title': 'Strategic Performance Imbalance',
                        'description': f"{best_category[0]} significantly outperforming {worst_category[0]} ({best_category[1]:.1f}% vs {worst_category[1]:.1f}%)",
                        'priority': 'high',
                        'category': 'strategic',
                        'recommendation': f'Realign resources and strategies to improve {worst_category[0]} performance'
                    })
            
            return insights
            
        except Exception as e:
            logger.error(f"Strategic insights generation failed: {e}")
            return []
    
    def _generate_cross_functional_insights(self, categorized_kpis: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate cross-functional insights."""
        insights = []
        
        try:
            # Look for correlations between categories
            business_categories = ['business', 'operational', 'financial', 'customer']
            available_categories = [cat for cat in business_categories if cat in categorized_kpis]
            
            if len(available_categories) >= 2:
                insights.append({
                    'type': 'cross_functional_opportunity',
                    'title': 'Cross-Functional Optimization Opportunity',
                    'description': f"Performance data available across {len(available_categories)} business functions",
                    'priority': 'medium',
                    'category': 'strategic',
                    'recommendation': 'Implement cross-functional KPI alignment initiatives'
                })
            
            return insights
            
        except Exception as e:
            logger.error(f"Cross-functional insights generation failed: {e}")
            return []
    
    def _generate_opportunity_insights(self, kpi_results: Dict[str, KPIResult]) -> List[Dict[str, Any]]:
        """Generate market opportunity insights."""
        insights = []
        
        try:
            # High-growth opportunity insight
            momentum_metrics = [
                (kpi_id, result) for kpi_id, result in kpi_results.items()
                if result.trend == 'up' and result.change_percentage and result.change_percentage > 20
            ]
            
            if momentum_metrics:
                insights.append({
                    'type': 'market_opportunity',
                    'title': 'High-Growth Market Opportunity',
                    'description': f"{len(momentum_metrics)} metrics showing exceptional growth momentum",
                    'priority': 'high',
                    'category': 'opportunity',
                    'recommendation': 'Increase market investment to capitalize on growth trends'
                })
            
            return insights
            
        except Exception as e:
            logger.error(f"Opportunity insights generation failed: {e}")
            return []
    
    def _generate_risk_insights(self, kpi_results: Dict[str, KPIResult]) -> List[Dict[str, Any]]:
        """Generate risk assessment insights."""
        insights = []
        
        try:
            # High-risk metrics
            risk_metrics = [
                (kpi_id, result) for kpi_id, result in kpi_results.items()
                if result.status in ['critical', 'warning'] or 
                   (result.change_percentage and result.change_percentage < -15)
            ]
            
            if len(risk_metrics) > len(kpi_results) * 0.3:  # More than 30% at risk
                insights.append({
                    'type': 'business_risk',
                    'title': 'Elevated Business Risk',
                    'description': f"{len(risk_metrics)} metrics indicating potential business risks",
                    'priority': 'critical',
                    'category': 'risk',
                    'recommendation': 'Implement comprehensive risk mitigation strategy'
                })
            
            return insights
            
        except Exception as e:
            logger.error(f"Risk insights generation failed: {e}")
            return []
    
    def _prioritize_insights(self, insights: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prioritize insights by importance and urgency."""
        try:
            priority_order = {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}
            
            return sorted(insights, 
                         key=lambda x: priority_order.get(x.get('priority', 'low'), 1), 
                         reverse=True)
            
        except Exception as e:
            logger.error(f"Insight prioritization failed: {e}")
            return insights

class BusinessIntelligenceEngine:
    """Main coordinator for Business Intelligence reporting system."""
    
    def __init__(self):
        self.report_generator = ReportGenerator()
        self.kpi_calculator = KPICalculator()
        self.visualization_generator = VisualizationGenerator()
        self.insight_engine = InsightEngine()
        
        # Cache for results
        self.latest_reports: Dict[str, GeneratedReport] = {}
        self.latest_kpi_results: Dict[str, KPIResult] = {}
        
    async def generate_comprehensive_report(self, data_df: pd.DataFrame,
                                          report_definition: ReportDefinition,
                                          kpi_definitions: List[KPIDefinition]) -> Dict[str, Any]:
        """Generate comprehensive BI report with all components."""
        try:
            # Generate main report
            logger.info("Generating comprehensive BI report...")
            report = self.report_generator.generate_report(data_df, report_definition, kpi_definitions)
            self.latest_reports[report_definition.report_id] = report
            
            # Update KPI cache
            self.latest_kpi_results.update(report.kpi_summary)
            
            # Generate executive summary if requested
            executive_summary = None
            if report_definition.report_type == 'executive':
                executive_summary = self.report_generator.generate_executive_report(
                    data_df, kpi_definitions
                )
            
            comprehensive_report = {
                'main_report': asdict(report),
                'executive_summary': executive_summary,
                'dashboard_components': self._create_dashboard_components(report.kpi_summary),
                'export_options': self._create_export_options(report),
                'metadata': {
                    'generation_timestamp': datetime.now().isoformat(),
                    'report_id': report_definition.report_id,
                    'data_records_processed': len(data_df),
                    'kpis_calculated': len(report.kpi_summary)
                }
            }
            
            logger.info("Comprehensive BI report generated successfully")
            return comprehensive_report
            
        except Exception as e:
            logger.error(f"Comprehensive report generation failed: {e}")
            return {'error': str(e)}
    
    def create_dashboard(self, dashboard_definition: Dashboard,
                        data_df: pd.DataFrame,
                        kpi_definitions: List[KPIDefinition]) -> Dict[str, Any]:
        """Create interactive dashboard."""
        try:
            if dashboard_updates:
                dashboard_updates.inc()
            
            # Calculate KPIs for dashboard
            kpi_results = self.kpi_calculator.calculate_kpis(data_df, kpi_definitions)
            
            # Generate widgets
            dashboard_widgets = {}
            for widget in dashboard_definition.widgets:
                widget_data = self._generate_widget_data(widget, kpi_results, data_df)
                dashboard_widgets[widget.widget_id] = widget_data
            
            # Create dashboard layout
            dashboard_layout = self._create_dashboard_layout(dashboard_definition, dashboard_widgets)
            
            dashboard_result = {
                'dashboard_id': dashboard_definition.dashboard_id,
                'dashboard_name': dashboard_definition.dashboard_name,
                'widgets': dashboard_widgets,
                'layout': dashboard_layout,
                'last_updated': datetime.now().isoformat(),
                'auto_refresh': dashboard_definition.auto_refresh,
                'refresh_interval': dashboard_definition.refresh_interval
            }
            
            logger.info(f"Dashboard {dashboard_definition.dashboard_id} created successfully")
            return dashboard_result
            
        except Exception as e:
            logger.error(f"Dashboard creation failed: {e}")
            return {'error': str(e)}
    
    def _create_dashboard_components(self, kpi_results: Dict[str, KPIResult]) -> Dict[str, Any]:
        """Create dashboard components for display."""
        try:
            components = {}
            
            # KPI cards
            components['kpi_cards'] = []
            for kpi_id, result in kpi_results.items():
                components['kpi_cards'].append({
                    'kpi_id': kpi_id,
                    'value': result.value,
                    'status': result.status,
                    'trend': result.trend,
                    'change_percentage': result.change_percentage
                })
            
            # Status summary
            status_counts = Counter([result.status for result in kpi_results.values()])
            components['status_summary'] = dict(status_counts)
            
            # Trend summary
            trend_counts = Counter([result.trend for result in kpi_results.values()])
            components['trend_summary'] = dict(trend_counts)
            
            return components
            
        except Exception as e:
            logger.error(f"Dashboard components creation failed: {e}")
            return {}
    
    def _create_export_options(self, report: GeneratedReport) -> Dict[str, Any]:
        """Create export options for the report."""
        try:
            export_options = {
                'available_formats': report.export_formats,
                'export_metadata': {
                    'report_id': report.report_id,
                    'generation_timestamp': report.generation_timestamp.isoformat(),
                    'contains_visualizations': bool(report.visualizations),
                    'kpi_count': len(report.kpi_summary)
                },
                'download_links': {
                    format_type: f"/api/reports/{report.report_id}/export/{format_type}"
                    for format_type in report.export_formats
                }
            }
            
            return export_options
            
        except Exception as e:
            logger.error(f"Export options creation failed: {e}")
            return {}
    
    def _generate_widget_data(self, widget: DashboardWidget,
                            kpi_results: Dict[str, KPIResult],
                            data_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate data for a dashboard widget."""
        try:
            widget_data = {
                'widget_id': widget.widget_id,
                'widget_type': widget.widget_type,
                'title': widget.title,
                'config': widget.config,
                'position': widget.position,
                'last_updated': datetime.now().isoformat()
            }
            
            if widget.widget_type == 'kpi_card':
                # Generate KPI card data
                kpi_id = widget.config.get('kpi_id')
                if kpi_id in kpi_results:
                    result = kpi_results[kpi_id]
                    widget_data['data'] = {
                        'value': result.value,
                        'status': result.status,
                        'trend': result.trend,
                        'change_percentage': result.change_percentage,
                        'target_achievement': result.target_achievement
                    }
            
            elif widget.widget_type == 'chart':
                # Generate chart data
                chart_type = widget.config.get('chart_type', 'line')
                widget_data['data'] = self._generate_chart_data(chart_type, kpi_results, data_df)
            
            elif widget.widget_type == 'table':
                # Generate table data
                widget_data['data'] = self._generate_table_data(kpi_results, widget.config)
            
            elif widget.widget_type == 'metric':
                # Generate single metric data
                metric_type = widget.config.get('metric_type')
                widget_data['data'] = self._calculate_single_metric(data_df, metric_type)
            
            return widget_data
            
        except Exception as e:
            logger.error(f"Widget data generation failed: {e}")
            return {'error': str(e)}
    
    def _create_dashboard_layout(self, dashboard_definition: Dashboard,
                               dashboard_widgets: Dict[str, Any]) -> Dict[str, Any]:
        """Create dashboard layout configuration."""
        try:
            layout = {
                'type': 'grid',
                'columns': 12,
                'rows': 'auto',
                'widgets': []
            }
            
            for widget in dashboard_definition.widgets:
                if widget.widget_id in dashboard_widgets:
                    layout['widgets'].append({
                        'widget_id': widget.widget_id,
                        'position': widget.position,
                        'size': {
                            'width': widget.position.get('width', 4),
                            'height': widget.position.get('height', 4)
                        }
                    })
            
            return layout
            
        except Exception as e:
            logger.error(f"Dashboard layout creation failed: {e}")
            return {}
    
    def _generate_chart_data(self, chart_type: str,
                           kpi_results: Dict[str, KPIResult],
                           data_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate chart data for widgets."""
        try:
            if chart_type == 'line':
                # Time series data
                if 'timestamp' in data_df.columns:
                    data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])
                    daily_counts = data_df.groupby(data_df['timestamp'].dt.date).size()
                    
                    return {
                        'labels': [date.isoformat() for date in daily_counts.index],
                        'values': daily_counts.values.tolist()
                    }
            
            elif chart_type == 'bar':
                # KPI comparison
                return {
                    'labels': list(kpi_results.keys()),
                    'values': [result.value for result in kpi_results.values()]
                }
            
            elif chart_type == 'pie':
                # Status distribution
                status_counts = Counter([result.status for result in kpi_results.values()])
                return {
                    'labels': list(status_counts.keys()),
                    'values': list(status_counts.values())
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"Chart data generation failed: {e}")
            return {}
    
    def _generate_table_data(self, kpi_results: Dict[str, KPIResult],
                           config: Dict[str, Any]) -> Dict[str, Any]:
        """Generate table data for widgets."""
        try:
            headers = ['KPI', 'Value', 'Status', 'Trend', 'Change %']
            rows = []
            
            for kpi_id, result in kpi_results.items():
                rows.append([
                    kpi_id,
                    f"{result.value:.1f}",
                    result.status,
                    result.trend,
                    f"{result.change_percentage:.1f}%" if result.change_percentage else "N/A"
                ])
            
            return {
                'headers': headers,
                'rows': rows
            }
            
        except Exception as e:
            logger.error(f"Table data generation failed: {e}")
            return {}
    
    def _calculate_single_metric(self, data_df: pd.DataFrame, metric_type: str) -> Dict[str, Any]:
        """Calculate single metric for widgets."""
        try:
            if metric_type == 'total_records':
                return {'value': len(data_df), 'label': 'Total Records'}
            
            elif metric_type == 'unique_users' and 'user_id' in data_df.columns:
                return {'value': data_df['user_id'].nunique(), 'label': 'Unique Users'}
            
            elif metric_type == 'date_range' and 'timestamp' in data_df.columns:
                data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])
                date_range = f"{data_df['timestamp'].min().date()} to {data_df['timestamp'].max().date()}"
                return {'value': date_range, 'label': 'Date Range'}
            
            return {'value': 'N/A', 'label': metric_type}
            
        except Exception as e:
            logger.error(f"Single metric calculation failed: {e}")
            return {'value': 'Error', 'label': metric_type}
    
    def get_report_status(self, report_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a generated report."""
        if report_id in self.latest_reports:
            report = self.latest_reports[report_id]
            return {
                'report_id': report_id,
                'status': 'completed',
                'generation_timestamp': report.generation_timestamp.isoformat(),
                'kpi_count': len(report.kpi_summary),
                'visualization_count': len(report.visualizations),
                'insight_count': len(report.insights)
            }
        return None
    
    def get_kpi_details(self, kpi_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific KPI."""
        if kpi_id in self.latest_kpi_results:
            return asdict(self.latest_kpi_results[kpi_id])
        return None
