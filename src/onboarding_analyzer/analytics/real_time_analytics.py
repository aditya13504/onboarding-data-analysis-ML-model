"""
Real-time Analytics Engine

Provides real-time analytics capabilities including streaming data processing,
live dashboards, real-time metrics computation, and instant alerting.
"""

import logging
import asyncio
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any, Callable, Tuple, Union
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
import json
import threading
from concurrent.futures import ThreadPoolExecutor
import pickle
from pathlib import Path
import queue
import time

# Redis for real-time data
import redis
from redis.exceptions import ConnectionError as RedisConnectionError

# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge, Summary

# Initialize metrics
realtime_events_processed = Counter('analytics_realtime_events_processed_total', 'Real-time events processed', ['event_type', 'status'])
realtime_processing_latency = Histogram('analytics_realtime_processing_duration_seconds', 'Real-time processing latency')
active_streams = Gauge('analytics_active_streams', 'Number of active real-time streams')
dashboard_updates = Counter('analytics_dashboard_updates_total', 'Dashboard updates sent', ['dashboard_type'])
alert_triggers = Counter('analytics_alert_triggers_total', 'Alerts triggered', ['alert_type', 'severity'])
metrics_computation_time = Summary('analytics_metrics_computation_seconds', 'Time spent computing metrics')

logger = logging.getLogger(__name__)

@dataclass
class StreamingEvent:
    """Event for real-time streaming."""
    event_id: str
    event_type: str
    user_id: str
    session_id: str
    timestamp: datetime
    properties: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class RealTimeMetric:
    """Real-time metric definition."""
    name: str
    value: float
    timestamp: datetime
    dimensions: Dict[str, str]
    aggregation_type: str  # sum, count, avg, min, max
    time_window: timedelta
    confidence_interval: Optional[Tuple[float, float]] = None

@dataclass
class Alert:
    """Alert definition and state."""
    alert_id: str
    name: str
    condition: str
    threshold: float
    severity: str  # low, medium, high, critical
    is_active: bool
    last_triggered: Optional[datetime]
    trigger_count: int
    message: str
    channels: List[str]  # email, slack, webhook

@dataclass
class DashboardWidget:
    """Dashboard widget configuration."""
    widget_id: str
    widget_type: str  # metric, chart, table, gauge
    title: str
    metrics: List[str]
    refresh_interval: int  # seconds
    configuration: Dict[str, Any]
    last_updated: datetime

class StreamProcessor:
    """Processes streaming events in real-time."""
    
    def __init__(self, buffer_size: int = 10000):
        self.buffer_size = buffer_size
        self.event_buffer = deque(maxlen=buffer_size)
        self.event_queue = queue.Queue()
        self.processors: Dict[str, Callable] = {}
        self.is_running = False
        self.worker_thread = None
        
    def register_processor(self, event_type: str, processor: Callable):
        """Register an event processor for specific event type."""
        self.processors[event_type] = processor
        logger.info(f"Registered processor for event type: {event_type}")
    
    def start_processing(self):
        """Start the stream processing worker."""
        if self.is_running:
            return
        
        self.is_running = True
        self.worker_thread = threading.Thread(target=self._process_events)
        self.worker_thread.daemon = True
        self.worker_thread.start()
        logger.info("Stream processor started")
    
    def stop_processing(self):
        """Stop the stream processing worker."""
        self.is_running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=5)
        logger.info("Stream processor stopped")
    
    def add_event(self, event: StreamingEvent):
        """Add event to processing queue."""
        try:
            self.event_queue.put_nowait(event)
            self.event_buffer.append(event)
        except queue.Full:
            logger.warning("Event queue full, dropping event")
    
    def _process_events(self):
        """Process events from the queue."""
        while self.is_running:
            try:
                # Get event with timeout
                event = self.event_queue.get(timeout=1.0)
                
                with realtime_processing_latency.time():
                    self._process_single_event(event)
                
                realtime_events_processed.labels(
                    event_type=event.event_type, 
                    status='success'
                ).inc()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Event processing failed: {e}")
                realtime_events_processed.labels(
                    event_type='unknown', 
                    status='error'
                ).inc()
    
    def _process_single_event(self, event: StreamingEvent):
        """Process a single event."""
        processor = self.processors.get(event.event_type)
        if processor:
            try:
                processor(event)
            except Exception as e:
                logger.error(f"Processor for {event.event_type} failed: {e}")
                raise
        else:
            logger.warning(f"No processor found for event type: {event.event_type}")
    
    def get_recent_events(self, event_type: Optional[str] = None, 
                         limit: int = 100) -> List[StreamingEvent]:
        """Get recent events from buffer."""
        events = list(self.event_buffer)
        
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        
        return events[-limit:]

class MetricsCalculator:
    """Calculates real-time metrics from streaming data."""
    
    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self.redis_client = redis_client or redis.Redis(host='localhost', port=6379, db=0)
        self.metric_definitions: Dict[str, Dict] = {}
        self.time_windows: Dict[str, deque] = defaultdict(lambda: deque())
        self.aggregated_metrics: Dict[str, RealTimeMetric] = {}
        
    def define_metric(self, name: str, aggregation_type: str, 
                     time_window_minutes: int = 5,
                     dimensions: List[str] = None):
        """Define a real-time metric."""
        self.metric_definitions[name] = {
            'aggregation_type': aggregation_type,
            'time_window': timedelta(minutes=time_window_minutes),
            'dimensions': dimensions or []
        }
        logger.info(f"Defined metric: {name} ({aggregation_type}, {time_window_minutes}m window)")
    
    @metrics_computation_time.time()
    def update_metrics(self, event: StreamingEvent):
        """Update metrics based on incoming event."""
        current_time = datetime.now()
        
        for metric_name, definition in self.metric_definitions.items():
            try:
                # Extract metric value from event
                metric_value = self._extract_metric_value(event, metric_name)
                if metric_value is not None:
                    
                    # Update time window
                    time_window = self.time_windows[metric_name]
                    time_window.append((current_time, metric_value, event))
                    
                    # Clean old data
                    window_duration = definition['time_window']
                    cutoff_time = current_time - window_duration
                    
                    while time_window and time_window[0][0] < cutoff_time:
                        time_window.popleft()
                    
                    # Calculate aggregated metric
                    aggregated_value = self._calculate_aggregation(
                        time_window, definition['aggregation_type']
                    )
                    
                    # Extract dimensions
                    dimensions = self._extract_dimensions(event, definition['dimensions'])
                    
                    # Create metric object
                    metric = RealTimeMetric(
                        name=metric_name,
                        value=aggregated_value,
                        timestamp=current_time,
                        dimensions=dimensions,
                        aggregation_type=definition['aggregation_type'],
                        time_window=definition['time_window']
                    )
                    
                    self.aggregated_metrics[metric_name] = metric
                    
                    # Store in Redis for real-time access
                    self._store_metric_in_redis(metric)
                    
            except Exception as e:
                logger.error(f"Metric calculation failed for {metric_name}: {e}")
    
    def _extract_metric_value(self, event: StreamingEvent, metric_name: str) -> Optional[float]:
        """Extract metric value from event."""
        # Define how to extract different metrics from events
        metric_extractors = {
            'event_count': lambda e: 1.0,
            'session_duration': lambda e: e.properties.get('duration', 0.0),
            'page_load_time': lambda e: e.properties.get('load_time', 0.0),
            'conversion_rate': lambda e: 1.0 if e.properties.get('converted', False) else 0.0,
            'bounce_rate': lambda e: 1.0 if e.properties.get('bounced', False) else 0.0,
            'revenue': lambda e: e.properties.get('revenue', 0.0),
            'error_count': lambda e: 1.0 if e.event_type == 'error' else 0.0,
            'unique_users': lambda e: 1.0,  # Will be deduplicated in aggregation
        }
        
        extractor = metric_extractors.get(metric_name)
        if extractor:
            try:
                return extractor(event)
            except Exception as e:
                logger.error(f"Value extraction failed for {metric_name}: {e}")
                return None
        
        # Try to extract from properties directly
        return event.properties.get(metric_name)
    
    def _calculate_aggregation(self, time_window: deque, aggregation_type: str) -> float:
        """Calculate aggregated value from time window data."""
        if not time_window:
            return 0.0
        
        values = [item[1] for item in time_window]
        events = [item[2] for item in time_window]
        
        if aggregation_type == 'sum':
            return sum(values)
        elif aggregation_type == 'count':
            return len(values)
        elif aggregation_type == 'avg':
            return np.mean(values) if values else 0.0
        elif aggregation_type == 'min':
            return min(values) if values else 0.0
        elif aggregation_type == 'max':
            return max(values) if values else 0.0
        elif aggregation_type == 'unique_count':
            # Count unique users
            unique_users = set(event.user_id for event in events)
            return len(unique_users)
        elif aggregation_type == 'rate':
            # Calculate rate (events per minute)
            if len(values) < 2:
                return 0.0
            time_span = (time_window[-1][0] - time_window[0][0]).total_seconds() / 60.0
            return len(values) / time_span if time_span > 0 else 0.0
        else:
            logger.warning(f"Unknown aggregation type: {aggregation_type}")
            return 0.0
    
    def _extract_dimensions(self, event: StreamingEvent, dimension_names: List[str]) -> Dict[str, str]:
        """Extract dimensions from event."""
        dimensions = {}
        
        for dim_name in dimension_names:
            if dim_name == 'user_id':
                dimensions[dim_name] = event.user_id
            elif dim_name == 'session_id':
                dimensions[dim_name] = event.session_id
            elif dim_name == 'event_type':
                dimensions[dim_name] = event.event_type
            else:
                # Try to extract from properties
                value = event.properties.get(dim_name, 'unknown')
                dimensions[dim_name] = str(value)
        
        return dimensions
    
    def _store_metric_in_redis(self, metric: RealTimeMetric):
        """Store metric in Redis for real-time access."""
        try:
            key = f"realtime_metric:{metric.name}"
            data = {
                'value': metric.value,
                'timestamp': metric.timestamp.isoformat(),
                'dimensions': metric.dimensions,
                'aggregation_type': metric.aggregation_type
            }
            
            # Store with expiration
            self.redis_client.setex(key, 3600, json.dumps(data))
            
            # Also store in time series for historical data
            ts_key = f"metric_timeseries:{metric.name}"
            timestamp = int(metric.timestamp.timestamp())
            self.redis_client.zadd(ts_key, {json.dumps(data): timestamp})
            
            # Keep only recent data (last 24 hours)
            cutoff = timestamp - (24 * 3600)
            self.redis_client.zremrangebyscore(ts_key, 0, cutoff)
            
        except RedisConnectionError:
            logger.warning("Redis not available for metric storage")
        except Exception as e:
            logger.error(f"Redis metric storage failed: {e}")
    
    def get_metric(self, metric_name: str) -> Optional[RealTimeMetric]:
        """Get current metric value."""
        return self.aggregated_metrics.get(metric_name)
    
    def get_metric_history(self, metric_name: str, 
                          hours: int = 1) -> List[RealTimeMetric]:
        """Get metric history from Redis."""
        try:
            ts_key = f"metric_timeseries:{metric_name}"
            cutoff = int((datetime.now() - timedelta(hours=hours)).timestamp())
            
            # Get recent metrics
            results = self.redis_client.zrangebyscore(ts_key, cutoff, '+inf', withscores=True)
            
            metrics = []
            for data_json, timestamp in results:
                try:
                    data = json.loads(data_json)
                    metric = RealTimeMetric(
                        name=metric_name,
                        value=data['value'],
                        timestamp=datetime.fromisoformat(data['timestamp']),
                        dimensions=data['dimensions'],
                        aggregation_type=data['aggregation_type'],
                        time_window=timedelta(minutes=5)  # Default
                    )
                    metrics.append(metric)
                except Exception as e:
                    logger.error(f"Failed to parse metric data: {e}")
            
            return sorted(metrics, key=lambda m: m.timestamp)
            
        except RedisConnectionError:
            logger.warning("Redis not available for metric history")
            return []
        except Exception as e:
            logger.error(f"Redis metric history retrieval failed: {e}")
            return []

class AlertManager:
    """Manages real-time alerts and notifications."""
    
    def __init__(self):
        self.alerts: Dict[str, Alert] = {}
        self.alert_history: List[Dict] = []
        self.notification_channels: Dict[str, Callable] = {}
        
    def define_alert(self, alert_id: str, name: str, condition: str,
                    threshold: float, severity: str = 'medium',
                    channels: List[str] = None):
        """Define a new alert."""
        alert = Alert(
            alert_id=alert_id,
            name=name,
            condition=condition,
            threshold=threshold,
            severity=severity,
            is_active=False,
            last_triggered=None,
            trigger_count=0,
            message="",
            channels=channels or ['log']
        )
        
        self.alerts[alert_id] = alert
        logger.info(f"Defined alert: {name} ({condition} > {threshold})")
    
    def register_notification_channel(self, channel_name: str, handler: Callable):
        """Register a notification channel handler."""
        self.notification_channels[channel_name] = handler
        logger.info(f"Registered notification channel: {channel_name}")
    
    def check_alerts(self, metrics: Dict[str, RealTimeMetric]):
        """Check all alerts against current metrics."""
        for alert_id, alert in self.alerts.items():
            try:
                triggered = self._evaluate_alert_condition(alert, metrics)
                
                if triggered and not alert.is_active:
                    self._trigger_alert(alert, metrics)
                elif not triggered and alert.is_active:
                    self._resolve_alert(alert)
                    
            except Exception as e:
                logger.error(f"Alert evaluation failed for {alert_id}: {e}")
    
    def _evaluate_alert_condition(self, alert: Alert, 
                                metrics: Dict[str, RealTimeMetric]) -> bool:
        """Evaluate alert condition against metrics."""
        # Simple condition parsing (metric_name > threshold)
        parts = alert.condition.split()
        if len(parts) != 3:
            logger.error(f"Invalid alert condition: {alert.condition}")
            return False
        
        metric_name, operator, threshold_str = parts
        
        try:
            threshold = float(threshold_str)
        except ValueError:
            threshold = alert.threshold
        
        metric = metrics.get(metric_name)
        if not metric:
            return False
        
        value = metric.value
        
        if operator == '>':
            return value > threshold
        elif operator == '<':
            return value < threshold
        elif operator == '>=':
            return value >= threshold
        elif operator == '<=':
            return value <= threshold
        elif operator == '==':
            return abs(value - threshold) < 1e-6
        elif operator == '!=':
            return abs(value - threshold) >= 1e-6
        else:
            logger.error(f"Unknown operator in alert condition: {operator}")
            return False
    
    def _trigger_alert(self, alert: Alert, metrics: Dict[str, RealTimeMetric]):
        """Trigger an alert."""
        current_time = datetime.now()
        alert.is_active = True
        alert.last_triggered = current_time
        alert.trigger_count += 1
        
        # Extract relevant metric value
        metric_name = alert.condition.split()[0]
        metric_value = metrics.get(metric_name, {}).value if metrics.get(metric_name) else 'unknown'
        
        alert.message = f"Alert '{alert.name}' triggered: {alert.condition} (current value: {metric_value})"
        
        # Send notifications
        for channel in alert.channels:
            self._send_notification(channel, alert)
        
        # Record in history
        self.alert_history.append({
            'alert_id': alert.alert_id,
            'name': alert.name,
            'action': 'triggered',
            'timestamp': current_time.isoformat(),
            'message': alert.message,
            'metric_value': metric_value
        })
        
        alert_triggers.labels(
            alert_type=alert.alert_id,
            severity=alert.severity
        ).inc()
        
        logger.warning(f"Alert triggered: {alert.message}")
    
    def _resolve_alert(self, alert: Alert):
        """Resolve an alert."""
        current_time = datetime.now()
        alert.is_active = False
        
        resolve_message = f"Alert '{alert.name}' resolved"
        
        # Send resolution notifications
        for channel in alert.channels:
            self._send_notification(channel, alert, is_resolution=True)
        
        # Record in history
        self.alert_history.append({
            'alert_id': alert.alert_id,
            'name': alert.name,
            'action': 'resolved',
            'timestamp': current_time.isoformat(),
            'message': resolve_message
        })
        
        logger.info(f"Alert resolved: {resolve_message}")
    
    def _send_notification(self, channel: str, alert: Alert, is_resolution: bool = False):
        """Send notification through specified channel."""
        handler = self.notification_channels.get(channel)
        if handler:
            try:
                notification_data = {
                    'alert': alert,
                    'is_resolution': is_resolution,
                    'timestamp': datetime.now().isoformat()
                }
                handler(notification_data)
            except Exception as e:
                logger.error(f"Notification failed for channel {channel}: {e}")
        else:
            # Default to logging
            action = "resolved" if is_resolution else "triggered"
            logger.info(f"ALERT {action.upper()}: {alert.message}")

class RealTimeDashboard:
    """Manages real-time dashboard widgets and updates."""
    
    def __init__(self, metrics_calculator: MetricsCalculator):
        self.metrics_calculator = metrics_calculator
        self.widgets: Dict[str, DashboardWidget] = {}
        self.dashboard_data: Dict[str, Any] = {}
        self.update_subscribers: List[Callable] = []
        
    def add_widget(self, widget_id: str, widget_type: str, title: str,
                  metrics: List[str], refresh_interval: int = 30,
                  configuration: Dict[str, Any] = None):
        """Add a widget to the dashboard."""
        widget = DashboardWidget(
            widget_id=widget_id,
            widget_type=widget_type,
            title=title,
            metrics=metrics,
            refresh_interval=refresh_interval,
            configuration=configuration or {},
            last_updated=datetime.now()
        )
        
        self.widgets[widget_id] = widget
        logger.info(f"Added dashboard widget: {title} ({widget_type})")
    
    def subscribe_to_updates(self, callback: Callable):
        """Subscribe to dashboard updates."""
        self.update_subscribers.append(callback)
    
    async def update_dashboard(self):
        """Update all dashboard widgets."""
        current_time = datetime.now()
        updated_widgets = []
        
        for widget_id, widget in self.widgets.items():
            try:
                # Check if widget needs update
                time_since_update = current_time - widget.last_updated
                if time_since_update.total_seconds() >= widget.refresh_interval:
                    
                    # Update widget data
                    widget_data = await self._update_widget_data(widget)
                    self.dashboard_data[widget_id] = widget_data
                    widget.last_updated = current_time
                    updated_widgets.append(widget_id)
                    
            except Exception as e:
                logger.error(f"Widget update failed for {widget_id}: {e}")
        
        # Notify subscribers if any widgets were updated
        if updated_widgets:
            await self._notify_subscribers(updated_widgets)
            dashboard_updates.labels(dashboard_type='realtime').inc()
    
    async def _update_widget_data(self, widget: DashboardWidget) -> Dict[str, Any]:
        """Update data for a specific widget."""
        widget_data = {
            'widget_id': widget.widget_id,
            'title': widget.title,
            'type': widget.widget_type,
            'last_updated': widget.last_updated.isoformat(),
            'data': {}
        }
        
        # Get current metric values
        for metric_name in widget.metrics:
            metric = self.metrics_calculator.get_metric(metric_name)
            if metric:
                widget_data['data'][metric_name] = {
                    'value': metric.value,
                    'timestamp': metric.timestamp.isoformat(),
                    'dimensions': metric.dimensions
                }
        
        # Add historical data for chart widgets
        if widget.widget_type in ['line_chart', 'area_chart']:
            historical_hours = widget.configuration.get('historical_hours', 1)
            for metric_name in widget.metrics:
                history = self.metrics_calculator.get_metric_history(
                    metric_name, hours=historical_hours
                )
                widget_data['data'][f'{metric_name}_history'] = [
                    {
                        'timestamp': m.timestamp.isoformat(),
                        'value': m.value
                    } for m in history
                ]
        
        return widget_data
    
    async def _notify_subscribers(self, updated_widget_ids: List[str]):
        """Notify subscribers of dashboard updates."""
        update_data = {
            'updated_widgets': updated_widget_ids,
            'dashboard_data': {
                widget_id: self.dashboard_data[widget_id] 
                for widget_id in updated_widget_ids
            },
            'timestamp': datetime.now().isoformat()
        }
        
        for callback in self.update_subscribers:
            try:
                await callback(update_data)
            except Exception as e:
                logger.error(f"Dashboard subscriber notification failed: {e}")
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get current dashboard data."""
        return {
            'widgets': {
                widget_id: asdict(widget) 
                for widget_id, widget in self.widgets.items()
            },
            'data': self.dashboard_data,
            'last_updated': datetime.now().isoformat()
        }

class RealTimeAnalyticsEngine:
    """Main real-time analytics engine."""
    
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379):
        # Initialize components
        self.stream_processor = StreamProcessor()
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
        self.metrics_calculator = MetricsCalculator(self.redis_client)
        self.alert_manager = AlertManager()
        self.dashboard = RealTimeDashboard(self.metrics_calculator)
        
        # Configuration
        self.update_interval = 5  # seconds
        self.is_running = False
        self.update_task = None
        
        # Setup default metrics
        self._setup_default_metrics()
        self._setup_default_alerts()
        self._setup_default_dashboard()
        
        # Register event processors
        self._register_event_processors()
        
        active_streams.set(1)
        logger.info("RealTimeAnalyticsEngine initialized")
    
    def _setup_default_metrics(self):
        """Setup default real-time metrics."""
        metrics_config = [
            ('event_count', 'count', 5),
            ('unique_users', 'unique_count', 15),
            ('session_duration', 'avg', 10),
            ('conversion_rate', 'avg', 30),
            ('bounce_rate', 'avg', 15),
            ('error_count', 'count', 5),
            ('page_load_time', 'avg', 10),
            ('revenue', 'sum', 60),
        ]
        
        for name, agg_type, window_minutes in metrics_config:
            self.metrics_calculator.define_metric(
                name=name,
                aggregation_type=agg_type,
                time_window_minutes=window_minutes,
                dimensions=['event_type', 'user_segment']
            )
    
    def _setup_default_alerts(self):
        """Setup default alerts."""
        alerts_config = [
            ('high_error_rate', 'Error Rate High', 'error_count > 10', 10.0, 'high'),
            ('low_conversion', 'Low Conversion Rate', 'conversion_rate < 0.05', 0.05, 'medium'),
            ('high_bounce_rate', 'High Bounce Rate', 'bounce_rate > 0.8', 0.8, 'medium'),
            ('slow_page_load', 'Slow Page Load', 'page_load_time > 5.0', 5.0, 'low'),
            ('traffic_spike', 'Traffic Spike', 'event_count > 1000', 1000.0, 'low'),
        ]
        
        for alert_id, name, condition, threshold, severity in alerts_config:
            self.alert_manager.define_alert(
                alert_id=alert_id,
                name=name,
                condition=condition,
                threshold=threshold,
                severity=severity,
                channels=['log', 'email']
            )
    
    def _setup_default_dashboard(self):
        """Setup default dashboard widgets."""
        widgets_config = [
            ('event_count_metric', 'metric', 'Event Count', ['event_count'], 10),
            ('unique_users_metric', 'metric', 'Active Users', ['unique_users'], 30),
            ('conversion_gauge', 'gauge', 'Conversion Rate', ['conversion_rate'], 60),
            ('error_count_chart', 'line_chart', 'Error Rate', ['error_count'], 15),
            ('performance_chart', 'line_chart', 'Page Load Time', ['page_load_time'], 30),
        ]
        
        for widget_id, widget_type, title, metrics, refresh_interval in widgets_config:
            self.dashboard.add_widget(
                widget_id=widget_id,
                widget_type=widget_type,
                title=title,
                metrics=metrics,
                refresh_interval=refresh_interval,
                configuration={'historical_hours': 2}
            )
    
    def _register_event_processors(self):
        """Register event processors for different event types."""
        
        def process_page_view(event: StreamingEvent):
            """Process page view events."""
            self.metrics_calculator.update_metrics(event)
        
        def process_conversion(event: StreamingEvent):
            """Process conversion events."""
            self.metrics_calculator.update_metrics(event)
        
        def process_error(event: StreamingEvent):
            """Process error events."""
            self.metrics_calculator.update_metrics(event)
            
            # Log error details
            error_type = event.properties.get('error_type', 'unknown')
            error_message = event.properties.get('message', 'No message')
            logger.warning(f"Error event: {error_type} - {error_message}")
        
        def process_user_action(event: StreamingEvent):
            """Process user action events."""
            self.metrics_calculator.update_metrics(event)
        
        # Register processors
        self.stream_processor.register_processor('page_view', process_page_view)
        self.stream_processor.register_processor('conversion', process_conversion)
        self.stream_processor.register_processor('error', process_error)
        self.stream_processor.register_processor('user_action', process_user_action)
        self.stream_processor.register_processor('session_start', process_user_action)
        self.stream_processor.register_processor('session_end', process_user_action)
    
    async def start(self):
        """Start the real-time analytics engine."""
        if self.is_running:
            return
        
        self.is_running = True
        
        # Start stream processor
        self.stream_processor.start_processing()
        
        # Start update loop
        self.update_task = asyncio.create_task(self._update_loop())
        
        logger.info("Real-time analytics engine started")
    
    async def stop(self):
        """Stop the real-time analytics engine."""
        self.is_running = False
        
        # Stop stream processor
        self.stream_processor.stop_processing()
        
        # Cancel update task
        if self.update_task:
            self.update_task.cancel()
            try:
                await self.update_task
            except asyncio.CancelledError:
                pass
        
        active_streams.set(0)
        logger.info("Real-time analytics engine stopped")
    
    async def _update_loop(self):
        """Main update loop for metrics, alerts, and dashboard."""
        while self.is_running:
            try:
                # Check alerts
                current_metrics = self.metrics_calculator.aggregated_metrics
                self.alert_manager.check_alerts(current_metrics)
                
                # Update dashboard
                await self.dashboard.update_dashboard()
                
                # Wait for next update
                await asyncio.sleep(self.update_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Update loop error: {e}")
                await asyncio.sleep(1)
    
    def process_event(self, event_data: Dict[str, Any]):
        """Process a real-time event."""
        try:
            # Create streaming event
            event = StreamingEvent(
                event_id=event_data.get('event_id', f"evt_{int(time.time() * 1000)}"),
                event_type=event_data['event_type'],
                user_id=event_data['user_id'],
                session_id=event_data.get('session_id', 'unknown'),
                timestamp=datetime.fromisoformat(event_data.get('timestamp', datetime.now().isoformat())),
                properties=event_data.get('properties', {}),
                metadata=event_data.get('metadata', {})
            )
            
            # Add to stream processor
            self.stream_processor.add_event(event)
            
        except Exception as e:
            logger.error(f"Event processing failed: {e}")
            realtime_events_processed.labels(
                event_type='unknown', 
                status='error'
            ).inc()
    
    def get_metrics_snapshot(self) -> Dict[str, Any]:
        """Get current metrics snapshot."""
        metrics_data = {}
        
        for name, metric in self.metrics_calculator.aggregated_metrics.items():
            metrics_data[name] = {
                'value': metric.value,
                'timestamp': metric.timestamp.isoformat(),
                'dimensions': metric.dimensions,
                'aggregation_type': metric.aggregation_type
            }
        
        return {
            'metrics': metrics_data,
            'timestamp': datetime.now().isoformat(),
            'active_alerts': [
                alert.alert_id for alert in self.alert_manager.alerts.values() 
                if alert.is_active
            ]
        }
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get current dashboard data."""
        return self.dashboard.get_dashboard_data()
    
    def register_notification_handler(self, channel: str, handler: Callable):
        """Register notification handler for alerts."""
        self.alert_manager.register_notification_channel(channel, handler)
    
    def subscribe_to_dashboard_updates(self, callback: Callable):
        """Subscribe to dashboard updates."""
        self.dashboard.subscribe_to_updates(callback)
