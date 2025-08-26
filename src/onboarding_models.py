"""
Point 13: Onboarding-Specific Data Models
Domain models and session reconstruction for onboarding flow analysis.
"""

import os
import json
import uuid
import time
import threading
import asyncio
from typing import Dict, List, Optional, Any, Union, Set, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, deque
import logging
from pathlib import Path
import sqlite3
from abc import ABC, abstractmethod
import numpy as np
import pandas as pd

class OnboardingStepType(Enum):
    """Types of onboarding steps"""
    LANDING = "landing"
    SIGNUP = "signup"
    EMAIL_VERIFICATION = "email_verification"
    PROFILE_SETUP = "profile_setup"
    PREFERENCES = "preferences"
    TUTORIAL = "tutorial"
    FIRST_ACTION = "first_action"
    COMPLETION = "completion"
    CUSTOM = "custom"

class OnboardingStatus(Enum):
    """Onboarding completion status"""
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    ABANDONED = "abandoned"
    FAILED = "failed"

class SessionStatus(Enum):
    """Session status"""
    ACTIVE = "active"
    COMPLETED = "completed"
    ABANDONED = "abandoned"
    TIMEOUT = "timeout"

class UserSegment(Enum):
    """User segmentation types"""
    ORGANIC = "organic"
    PAID = "paid"
    REFERRAL = "referral"
    DIRECT = "direct"
    SOCIAL = "social"
    EMAIL = "email"
    UNKNOWN = "unknown"

class DeviceType(Enum):
    """Device type classification"""
    DESKTOP = "desktop"
    MOBILE = "mobile"
    TABLET = "tablet"
    UNKNOWN = "unknown"

@dataclass
class OnboardingStepDefinition:
    """Definition of an onboarding step"""
    step_id: str
    step_name: str
    step_type: OnboardingStepType
    order: int
    required: bool = True
    description: str = ""
    expected_events: List[str] = field(default_factory=list)
    success_criteria: Dict[str, Any] = field(default_factory=dict)
    timeout_minutes: Optional[int] = None
    retry_allowed: bool = True
    dependencies: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class OnboardingFlowDefinition:
    """Complete onboarding flow definition"""
    flow_id: str
    flow_name: str
    description: str
    version: str
    steps: List[OnboardingStepDefinition]
    total_timeout_hours: Optional[int] = 24
    abandonment_threshold_minutes: int = 30
    success_step_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    
    def get_step(self, step_id: str) -> Optional[OnboardingStepDefinition]:
        """Get step by ID"""
        for step in self.steps:
            if step.step_id == step_id:
                return step
        return None
    
    def get_step_order(self, step_id: str) -> Optional[int]:
        """Get step order"""
        step = self.get_step(step_id)
        return step.order if step else None
    
    def get_next_step(self, current_step_id: str) -> Optional[OnboardingStepDefinition]:
        """Get next step in flow"""
        current_order = self.get_step_order(current_step_id)
        if current_order is None:
            return None
        
        next_steps = [step for step in self.steps if step.order == current_order + 1]
        return next_steps[0] if next_steps else None

@dataclass
class OnboardingEvent:
    """Onboarding-specific event"""
    event_id: str
    user_id: str
    session_id: str
    flow_id: str
    step_id: Optional[str]
    event_name: str
    event_type: str
    timestamp: datetime
    properties: Dict[str, Any] = field(default_factory=dict)
    user_properties: Dict[str, Any] = field(default_factory=dict)
    device_info: Dict[str, Any] = field(default_factory=dict)
    location_info: Dict[str, Any] = field(default_factory=dict)
    platform: str = ""
    is_success_event: bool = False
    is_error_event: bool = False
    error_details: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class OnboardingStepAttempt:
    """Individual step attempt within a session"""
    attempt_id: str
    step_id: str
    user_id: str
    session_id: str
    flow_id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str = "started"
    events: List[OnboardingEvent] = field(default_factory=list)
    success_events: List[str] = field(default_factory=list)
    error_events: List[str] = field(default_factory=list)
    time_spent_seconds: Optional[float] = None
    retry_count: int = 0
    abandonment_reason: Optional[str] = None
    conversion_data: Dict[str, Any] = field(default_factory=dict)
    
    def add_event(self, event: OnboardingEvent):
        """Add event to step attempt"""
        self.events.append(event)
        
        if event.is_success_event:
            self.success_events.append(event.event_id)
            if self.status == "started":
                self.status = "completed"
                self.completed_at = event.timestamp
        
        if event.is_error_event:
            self.error_events.append(event.event_id)
    
    def calculate_time_spent(self):
        """Calculate time spent on step"""
        if self.completed_at:
            self.time_spent_seconds = (self.completed_at - self.started_at).total_seconds()
        elif self.events:
            last_event_time = max(event.timestamp for event in self.events)
            self.time_spent_seconds = (last_event_time - self.started_at).total_seconds()

@dataclass
class OnboardingSession:
    """Complete onboarding session for a user"""
    session_id: str
    user_id: str
    flow_id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: OnboardingStatus = OnboardingStatus.IN_PROGRESS
    current_step_id: Optional[str] = None
    completed_steps: List[str] = field(default_factory=list)
    failed_steps: List[str] = field(default_factory=list)
    step_attempts: Dict[str, List[OnboardingStepAttempt]] = field(default_factory=dict)
    total_events: int = 0
    conversion_rate: float = 0.0
    abandonment_point: Optional[str] = None
    abandonment_reason: Optional[str] = None
    device_info: Dict[str, Any] = field(default_factory=dict)
    user_segment: UserSegment = UserSegment.UNKNOWN
    acquisition_channel: Optional[str] = None
    referrer: Optional[str] = None
    utm_data: Dict[str, str] = field(default_factory=dict)
    location_info: Dict[str, Any] = field(default_factory=dict)
    session_metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_step_attempt(self, attempt: OnboardingStepAttempt):
        """Add step attempt to session"""
        if attempt.step_id not in self.step_attempts:
            self.step_attempts[attempt.step_id] = []
        
        self.step_attempts[attempt.step_id].append(attempt)
        self.current_step_id = attempt.step_id
        
        if attempt.status == "completed" and attempt.step_id not in self.completed_steps:
            self.completed_steps.append(attempt.step_id)
        elif attempt.status == "failed" and attempt.step_id not in self.failed_steps:
            self.failed_steps.append(attempt.step_id)
    
    def get_current_attempt(self, step_id: str) -> Optional[OnboardingStepAttempt]:
        """Get current attempt for step"""
        attempts = self.step_attempts.get(step_id, [])
        return attempts[-1] if attempts else None
    
    def calculate_conversion_rate(self, flow_def: OnboardingFlowDefinition) -> float:
        """Calculate session conversion rate"""
        if not flow_def.steps:
            return 0.0
        
        completed_count = len(self.completed_steps)
        total_count = len(flow_def.steps)
        
        self.conversion_rate = completed_count / total_count
        return self.conversion_rate
    
    def get_time_to_complete(self) -> Optional[timedelta]:
        """Get time to complete onboarding"""
        if self.completed_at:
            return self.completed_at - self.started_at
        return None
    
    def get_step_completion_times(self) -> Dict[str, float]:
        """Get completion time for each step"""
        completion_times = {}
        
        for step_id, attempts in self.step_attempts.items():
            completed_attempts = [a for a in attempts if a.status == "completed"]
            if completed_attempts:
                # Use the first successful attempt
                attempt = completed_attempts[0]
                if attempt.time_spent_seconds:
                    completion_times[step_id] = attempt.time_spent_seconds
        
        return completion_times

@dataclass
class UserProfile:
    """Enhanced user profile for onboarding analysis"""
    user_id: str
    distinct_id: Optional[str]
    first_seen: datetime
    last_seen: datetime
    total_sessions: int = 0
    completed_onboardings: int = 0
    abandoned_onboardings: int = 0
    primary_device: DeviceType = DeviceType.UNKNOWN
    primary_segment: UserSegment = UserSegment.UNKNOWN
    acquisition_channel: Optional[str] = None
    first_referrer: Optional[str] = None
    location_country: Optional[str] = None
    location_city: Optional[str] = None
    user_properties: Dict[str, Any] = field(default_factory=dict)
    behavioral_traits: Dict[str, Any] = field(default_factory=dict)
    lifetime_value: Optional[float] = None
    risk_score: Optional[float] = None
    tags: Set[str] = field(default_factory=set)
    
    def add_session(self, session: OnboardingSession):
        """Add session data to user profile"""
        self.total_sessions += 1
        
        if session.status == OnboardingStatus.COMPLETED:
            self.completed_onboardings += 1
        elif session.status == OnboardingStatus.ABANDONED:
            self.abandoned_onboardings += 1
        
        # Update device info
        if session.device_info:
            device_type = session.device_info.get('device_type', 'unknown').lower()
            if device_type in ['mobile', 'tablet', 'desktop']:
                self.primary_device = DeviceType(device_type)
        
        # Update segment
        self.primary_segment = session.user_segment
        
        # Update location
        if session.location_info:
            self.location_country = session.location_info.get('country')
            self.location_city = session.location_info.get('city')
    
    def calculate_conversion_rate(self) -> float:
        """Calculate user's onboarding conversion rate"""
        if self.total_sessions == 0:
            return 0.0
        return self.completed_onboardings / self.total_sessions

class SessionReconstructor:
    """Reconstruct user sessions from events"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Session configuration
        self.session_timeout_minutes = config.get('session_timeout_minutes', 30)
        self.min_session_events = config.get('min_session_events', 1)
        
        # Flow definitions
        self.flow_definitions = {}
        
        # Event classification
        self.onboarding_event_patterns = config.get('onboarding_event_patterns', [])
        
        # User tracking
        self.user_sessions = defaultdict(list)
        self.active_sessions = {}
    
    def add_flow_definition(self, flow_def: OnboardingFlowDefinition):
        """Add onboarding flow definition"""
        self.flow_definitions[flow_def.flow_id] = flow_def
        self.logger.info(f"Added flow definition: {flow_def.flow_name}")
    
    def reconstruct_sessions(self, events: List[Dict[str, Any]]) -> List[OnboardingSession]:
        """Reconstruct onboarding sessions from events"""
        try:
            # Convert to onboarding events
            onboarding_events = []
            for event in events:
                onboarding_event = self._convert_to_onboarding_event(event)
                if onboarding_event:
                    onboarding_events.append(onboarding_event)
            
            # Sort events by timestamp
            onboarding_events.sort(key=lambda e: e.timestamp)
            
            # Group events by user and session
            sessions = self._group_events_into_sessions(onboarding_events)
            
            # Reconstruct session details
            reconstructed_sessions = []
            for session_data in sessions:
                session = self._reconstruct_session_details(session_data)
                if session:
                    reconstructed_sessions.append(session)
            
            self.logger.info(f"Reconstructed {len(reconstructed_sessions)} onboarding sessions")
            return reconstructed_sessions
            
        except Exception as e:
            self.logger.error(f"Error reconstructing sessions: {e}")
            return []
    
    def _convert_to_onboarding_event(self, event: Dict[str, Any]) -> Optional[OnboardingEvent]:
        """Convert normalized event to onboarding event"""
        try:
            # Check if event is onboarding-related
            if not self._is_onboarding_event(event):
                return None
            
            # Extract onboarding-specific data
            flow_id = self._determine_flow_id(event)
            step_id = self._determine_step_id(event)
            
            onboarding_event = OnboardingEvent(
                event_id=event.get('event_id', str(uuid.uuid4())),
                user_id=event.get('user_id', ''),
                session_id=event.get('session_id') or self._generate_session_id(event),
                flow_id=flow_id,
                step_id=step_id,
                event_name=event.get('event_name', ''),
                event_type=event.get('event_type', ''),
                timestamp=datetime.fromisoformat(event.get('timestamp', datetime.utcnow().isoformat())),
                properties=event.get('properties', {}),
                user_properties=event.get('user_properties', {}),
                device_info=event.get('device', {}),
                location_info=event.get('location', {}),
                platform=event.get('platform', ''),
                is_success_event=self._is_success_event(event),
                is_error_event=self._is_error_event(event),
                error_details=event.get('properties', {}).get('error_message'),
                metadata=event.get('metadata', {})
            )
            
            return onboarding_event
            
        except Exception as e:
            self.logger.error(f"Error converting event to onboarding event: {e}")
            return None
    
    def _is_onboarding_event(self, event: Dict[str, Any]) -> bool:
        """Check if event is onboarding-related"""
        event_name = event.get('event_name', '').lower()
        event_type = event.get('event_type', '').lower()
        
        # Check for onboarding keywords
        onboarding_keywords = [
            'onboarding', 'signup', 'sign_up', 'registration', 'welcome',
            'tutorial', 'getting_started', 'first_time', 'setup', 'profile',
            'preferences', 'verification', 'activation', 'complete'
        ]
        
        for keyword in onboarding_keywords:
            if keyword in event_name or keyword in event_type:
                return True
        
        # Check URL patterns
        page_url = event.get('properties', {}).get('page_url', '')
        if any(pattern in page_url.lower() for pattern in ['onboard', 'signup', 'welcome', 'setup']):
            return True
        
        # Check custom patterns
        for pattern in self.onboarding_event_patterns:
            if self._matches_pattern(event, pattern):
                return True
        
        return False
    
    def _determine_flow_id(self, event: Dict[str, Any]) -> str:
        """Determine onboarding flow ID from event"""
        # Check event properties for flow ID
        flow_id = event.get('properties', {}).get('flow_id')
        if flow_id:
            return flow_id
        
        # Determine from URL or context
        page_url = event.get('properties', {}).get('page_url', '')
        
        if 'signup' in page_url.lower():
            return 'user_signup_flow'
        elif 'onboard' in page_url.lower():
            return 'user_onboarding_flow'
        else:
            return 'default_onboarding_flow'
    
    def _determine_step_id(self, event: Dict[str, Any]) -> Optional[str]:
        """Determine onboarding step ID from event"""
        # Check event properties for step ID
        step_id = event.get('properties', {}).get('step_id')
        if step_id:
            return step_id
        
        # Infer from event name
        event_name = event.get('event_name', '').lower()
        
        if 'signup' in event_name or 'registration' in event_name:
            return 'signup'
        elif 'verification' in event_name or 'verify' in event_name:
            return 'email_verification'
        elif 'profile' in event_name:
            return 'profile_setup'
        elif 'preferences' in event_name or 'settings' in event_name:
            return 'preferences'
        elif 'tutorial' in event_name or 'walkthrough' in event_name:
            return 'tutorial'
        elif 'complete' in event_name or 'finish' in event_name:
            return 'completion'
        
        # Check URL patterns
        page_url = event.get('properties', {}).get('page_url', '')
        
        if '/signup' in page_url:
            return 'signup'
        elif '/verify' in page_url:
            return 'email_verification'
        elif '/profile' in page_url:
            return 'profile_setup'
        elif '/preferences' in page_url:
            return 'preferences'
        elif '/tutorial' in page_url or '/welcome' in page_url:
            return 'tutorial'
        
        return None
    
    def _generate_session_id(self, event: Dict[str, Any]) -> str:
        """Generate session ID for event"""
        user_id = event.get('user_id', '')
        timestamp = event.get('timestamp', '')
        
        # Simple session ID based on user and time window
        if timestamp:
            try:
                event_time = datetime.fromisoformat(timestamp)
                # Round to nearest 30-minute window
                rounded_time = event_time.replace(minute=(event_time.minute // 30) * 30, second=0, microsecond=0)
                return f"{user_id}_{rounded_time.isoformat()}"
            except:
                pass
        
        return f"{user_id}_{str(uuid.uuid4())[:8]}"
    
    def _is_success_event(self, event: Dict[str, Any]) -> bool:
        """Check if event represents a successful action"""
        event_name = event.get('event_name', '').lower()
        
        success_indicators = [
            'complete', 'success', 'finish', 'submit', 'confirm',
            'verified', 'activated', 'created', 'saved'
        ]
        
        for indicator in success_indicators:
            if indicator in event_name:
                return True
        
        # Check properties
        properties = event.get('properties', {})
        if properties.get('success') or properties.get('completed'):
            return True
        
        return False
    
    def _is_error_event(self, event: Dict[str, Any]) -> bool:
        """Check if event represents an error"""
        event_name = event.get('event_name', '').lower()
        event_type = event.get('event_type', '').lower()
        
        if 'error' in event_name or 'error' in event_type:
            return True
        
        # Check properties
        properties = event.get('properties', {})
        if properties.get('error') or properties.get('error_message'):
            return True
        
        return False
    
    def _matches_pattern(self, event: Dict[str, Any], pattern: Dict[str, Any]) -> bool:
        """Check if event matches custom pattern"""
        # Simple pattern matching implementation
        for key, value in pattern.items():
            if key == 'event_name':
                if value.lower() not in event.get('event_name', '').lower():
                    return False
            elif key == 'properties':
                event_props = event.get('properties', {})
                for prop_key, prop_value in value.items():
                    if event_props.get(prop_key) != prop_value:
                        return False
        
        return True
    
    def _group_events_into_sessions(self, events: List[OnboardingEvent]) -> List[List[OnboardingEvent]]:
        """Group events into sessions"""
        sessions = []
        current_session = []
        last_timestamp = None
        last_user = None
        last_session_id = None
        
        for event in events:
            # Check for session boundary
            session_boundary = False
            
            if (last_user and event.user_id != last_user) or (last_session_id and event.session_id != last_session_id):
                session_boundary = True
            elif last_timestamp:
                time_gap = (event.timestamp - last_timestamp).total_seconds() / 60
                if time_gap > self.session_timeout_minutes:
                    session_boundary = True
            
            if session_boundary and current_session:
                if len(current_session) >= self.min_session_events:
                    sessions.append(current_session)
                current_session = []
            
            current_session.append(event)
            last_timestamp = event.timestamp
            last_user = event.user_id
            last_session_id = event.session_id
        
        # Add final session
        if current_session and len(current_session) >= self.min_session_events:
            sessions.append(current_session)
        
        return sessions
    
    def _reconstruct_session_details(self, session_events: List[OnboardingEvent]) -> Optional[OnboardingSession]:
        """Reconstruct detailed session from events"""
        try:
            if not session_events:
                return None
            
            first_event = session_events[0]
            last_event = session_events[-1]
            
            # Create session
            session = OnboardingSession(
                session_id=first_event.session_id,
                user_id=first_event.user_id,
                flow_id=first_event.flow_id,
                started_at=first_event.timestamp,
                total_events=len(session_events),
                device_info=first_event.device_info,
                location_info=first_event.location_info
            )
            
            # Determine user segment
            session.user_segment = self._determine_user_segment(first_event)
            session.acquisition_channel = self._determine_acquisition_channel(first_event)
            session.referrer = first_event.properties.get('referrer')
            session.utm_data = self._extract_utm_data(first_event)
            
            # Reconstruct step attempts
            step_attempts = self._reconstruct_step_attempts(session_events)
            for attempt in step_attempts:
                session.add_step_attempt(attempt)
            
            # Determine session status
            session.status = self._determine_session_status(session, session_events)
            
            if session.status == OnboardingStatus.COMPLETED:
                session.completed_at = last_event.timestamp
            elif session.status == OnboardingStatus.ABANDONED:
                session.abandonment_point = session.current_step_id
                session.abandonment_reason = self._determine_abandonment_reason(session_events)
            
            # Calculate conversion rate
            flow_def = self.flow_definitions.get(session.flow_id)
            if flow_def:
                session.calculate_conversion_rate(flow_def)
            
            return session
            
        except Exception as e:
            self.logger.error(f"Error reconstructing session details: {e}")
            return None
    
    def _reconstruct_step_attempts(self, events: List[OnboardingEvent]) -> List[OnboardingStepAttempt]:
        """Reconstruct step attempts from events"""
        attempts = []
        current_attempts = {}
        
        for event in events:
            step_id = event.step_id
            if not step_id:
                continue
            
            # Check if this is a new attempt for the step
            if step_id not in current_attempts:
                attempt = OnboardingStepAttempt(
                    attempt_id=str(uuid.uuid4()),
                    step_id=step_id,
                    user_id=event.user_id,
                    session_id=event.session_id,
                    flow_id=event.flow_id,
                    started_at=event.timestamp
                )
                current_attempts[step_id] = attempt
                attempts.append(attempt)
            
            # Add event to current attempt
            attempt = current_attempts[step_id]
            attempt.add_event(event)
            
            # Check if this completes the step
            if event.is_success_event and attempt.status != "completed":
                attempt.status = "completed"
                attempt.completed_at = event.timestamp
                attempt.calculate_time_spent()
            elif event.is_error_event:
                attempt.status = "failed"
        
        # Calculate time spent for incomplete attempts
        for attempt in attempts:
            if not attempt.completed_at:
                attempt.calculate_time_spent()
        
        return attempts
    
    def _determine_user_segment(self, event: OnboardingEvent) -> UserSegment:
        """Determine user segment from first event"""
        # Check UTM source
        utm_source = event.properties.get('utm_source', '').lower()
        
        if utm_source in ['google', 'facebook', 'linkedin', 'twitter']:
            return UserSegment.PAID
        elif utm_source in ['organic', 'search']:
            return UserSegment.ORGANIC
        elif utm_source == 'referral':
            return UserSegment.REFERRAL
        elif utm_source == 'email':
            return UserSegment.EMAIL
        elif utm_source in ['facebook', 'twitter', 'linkedin', 'instagram']:
            return UserSegment.SOCIAL
        
        # Check referrer
        referrer = event.properties.get('referrer', '').lower()
        if referrer:
            if any(domain in referrer for domain in ['google.com', 'bing.com', 'yahoo.com']):
                return UserSegment.ORGANIC
            elif any(domain in referrer for domain in ['facebook.com', 'twitter.com', 'linkedin.com']):
                return UserSegment.SOCIAL
            else:
                return UserSegment.REFERRAL
        
        return UserSegment.DIRECT
    
    def _determine_acquisition_channel(self, event: OnboardingEvent) -> Optional[str]:
        """Determine acquisition channel"""
        utm_medium = event.properties.get('utm_medium')
        utm_source = event.properties.get('utm_source')
        
        if utm_medium and utm_source:
            return f"{utm_source}/{utm_medium}"
        elif utm_source:
            return utm_source
        
        referrer = event.properties.get('referrer')
        if referrer:
            return f"referral/{referrer}"
        
        return "direct"
    
    def _extract_utm_data(self, event: OnboardingEvent) -> Dict[str, str]:
        """Extract UTM parameters"""
        utm_data = {}
        
        for key in ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content']:
            value = event.properties.get(key)
            if value:
                utm_data[key] = value
        
        return utm_data
    
    def _determine_session_status(self, session: OnboardingSession, 
                                events: List[OnboardingEvent]) -> OnboardingStatus:
        """Determine session completion status"""
        # Check for completion events
        completion_events = [e for e in events if 'complete' in e.event_name.lower() or e.is_success_event]
        
        # Get flow definition
        flow_def = self.flow_definitions.get(session.flow_id)
        if flow_def:
            # Check if all required steps are completed
            required_steps = [step.step_id for step in flow_def.steps if step.required]
            completed_required = [step_id for step_id in session.completed_steps if step_id in required_steps]
            
            if len(completed_required) == len(required_steps):
                return OnboardingStatus.COMPLETED
            
            # Check for success step
            if flow_def.success_step_id and flow_def.success_step_id in session.completed_steps:
                return OnboardingStatus.COMPLETED
        
        # Check for explicit completion events
        if completion_events:
            return OnboardingStatus.COMPLETED
        
        # Check for errors
        error_events = [e for e in events if e.is_error_event]
        if error_events:
            return OnboardingStatus.FAILED
        
        # Check for abandonment (no events for threshold period)
        last_event_time = max(e.timestamp for e in events)
        time_since_last = (datetime.utcnow() - last_event_time).total_seconds() / 60
        
        abandonment_threshold = flow_def.abandonment_threshold_minutes if flow_def else 30
        
        if time_since_last > abandonment_threshold:
            return OnboardingStatus.ABANDONED
        
        return OnboardingStatus.IN_PROGRESS
    
    def _determine_abandonment_reason(self, events: List[OnboardingEvent]) -> Optional[str]:
        """Determine reason for abandonment"""
        # Check for error events
        error_events = [e for e in events if e.is_error_event]
        if error_events:
            return f"error: {error_events[-1].error_details or 'unknown error'}"
        
        # Check for long idle time on specific step
        if len(events) >= 2:
            last_events = events[-2:]
            time_gap = (last_events[1].timestamp - last_events[0].timestamp).total_seconds()
            
            if time_gap > 300:  # 5 minutes
                return f"idle_timeout: {last_events[0].step_id}"
        
        # Check for repeated failed attempts
        step_attempts = defaultdict(int)
        for event in events:
            if event.step_id:
                step_attempts[event.step_id] += 1
        
        for step_id, count in step_attempts.items():
            if count > 3:  # More than 3 attempts
                return f"repeated_failure: {step_id}"
        
        return "unknown"

class FeatureEngineer:
    """Extract features for ML analysis"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def extract_session_features(self, session: OnboardingSession, 
                                flow_def: OnboardingFlowDefinition) -> Dict[str, Any]:
        """Extract features from onboarding session"""
        try:
            features = {
                # Basic session info
                'session_id': session.session_id,
                'user_id': session.user_id,
                'flow_id': session.flow_id,
                'total_events': session.total_events,
                'conversion_rate': session.conversion_rate,
                'status': session.status.value,
                
                # Timing features
                'session_duration_minutes': self._get_session_duration_minutes(session),
                'time_to_complete_minutes': self._get_time_to_complete_minutes(session),
                
                # Step completion features
                'steps_completed': len(session.completed_steps),
                'steps_failed': len(session.failed_steps),
                'total_steps': len(flow_def.steps),
                'completion_percentage': len(session.completed_steps) / len(flow_def.steps) if flow_def.steps else 0,
                
                # Step timing features
                'avg_step_time_seconds': self._get_avg_step_time(session),
                'max_step_time_seconds': self._get_max_step_time(session),
                'min_step_time_seconds': self._get_min_step_time(session),
                
                # User characteristics
                'user_segment': session.user_segment.value,
                'device_type': self._get_device_type(session),
                'browser': self._get_browser(session),
                'operating_system': self._get_operating_system(session),
                'country': session.location_info.get('country', 'unknown'),
                
                # Acquisition features
                'acquisition_channel': session.acquisition_channel or 'unknown',
                'has_referrer': bool(session.referrer),
                'has_utm_data': bool(session.utm_data),
                'utm_source': session.utm_data.get('utm_source', 'unknown'),
                'utm_medium': session.utm_data.get('utm_medium', 'unknown'),
                
                # Behavioral features
                'retry_count': self._get_total_retry_count(session),
                'error_count': self._get_total_error_count(session),
                'success_rate': self._get_success_rate(session),
                
                # Abandonment features
                'abandonment_point': session.abandonment_point,
                'abandonment_reason': session.abandonment_reason,
                'abandoned_at_step_order': self._get_abandonment_step_order(session, flow_def),
                
                # Advanced behavioral patterns
                'has_backtracking': self._has_backtracking(session),
                'session_linearity': self._calculate_session_linearity(session, flow_def),
                'event_frequency': self._calculate_event_frequency(session),
                'interaction_depth': self._calculate_interaction_depth(session)
            }
            
            # Add step-specific features
            step_features = self._extract_step_features(session, flow_def)
            features.update(step_features)
            
            return features
            
        except Exception as e:
            self.logger.error(f"Error extracting session features: {e}")
            return {}
    
    def _get_session_duration_minutes(self, session: OnboardingSession) -> float:
        """Get session duration in minutes"""
        if session.completed_at:
            return (session.completed_at - session.started_at).total_seconds() / 60
        return 0.0
    
    def _get_time_to_complete_minutes(self, session: OnboardingSession) -> Optional[float]:
        """Get time to complete in minutes"""
        completion_time = session.get_time_to_complete()
        return completion_time.total_seconds() / 60 if completion_time else None
    
    def _get_avg_step_time(self, session: OnboardingSession) -> float:
        """Get average step completion time"""
        completion_times = session.get_step_completion_times()
        return np.mean(list(completion_times.values())) if completion_times else 0.0
    
    def _get_max_step_time(self, session: OnboardingSession) -> float:
        """Get maximum step completion time"""
        completion_times = session.get_step_completion_times()
        return max(completion_times.values()) if completion_times else 0.0
    
    def _get_min_step_time(self, session: OnboardingSession) -> float:
        """Get minimum step completion time"""
        completion_times = session.get_step_completion_times()
        return min(completion_times.values()) if completion_times else 0.0
    
    def _get_device_type(self, session: OnboardingSession) -> str:
        """Get device type"""
        return session.device_info.get('device_type', 'unknown').lower()
    
    def _get_browser(self, session: OnboardingSession) -> str:
        """Get browser"""
        return session.device_info.get('browser', 'unknown').lower()
    
    def _get_operating_system(self, session: OnboardingSession) -> str:
        """Get operating system"""
        return session.device_info.get('operating_system', 'unknown').lower()
    
    def _get_total_retry_count(self, session: OnboardingSession) -> int:
        """Get total retry count across all steps"""
        total_retries = 0
        for attempts_list in session.step_attempts.values():
            total_retries += sum(attempt.retry_count for attempt in attempts_list)
        return total_retries
    
    def _get_total_error_count(self, session: OnboardingSession) -> int:
        """Get total error count"""
        total_errors = 0
        for attempts_list in session.step_attempts.values():
            for attempt in attempts_list:
                total_errors += len(attempt.error_events)
        return total_errors
    
    def _get_success_rate(self, session: OnboardingSession) -> float:
        """Get overall success rate"""
        total_attempts = sum(len(attempts) for attempts in session.step_attempts.values())
        if total_attempts == 0:
            return 0.0
        
        successful_attempts = 0
        for attempts_list in session.step_attempts.values():
            successful_attempts += sum(1 for attempt in attempts_list if attempt.status == "completed")
        
        return successful_attempts / total_attempts
    
    def _get_abandonment_step_order(self, session: OnboardingSession, 
                                  flow_def: OnboardingFlowDefinition) -> Optional[int]:
        """Get order of abandonment step"""
        if session.abandonment_point:
            return flow_def.get_step_order(session.abandonment_point)
        return None
    
    def _has_backtracking(self, session: OnboardingSession) -> bool:
        """Check if user backtracked through steps"""
        if not session.step_attempts:
            return False
        
        # Check if user returned to earlier steps
        step_first_times = {}
        for step_id, attempts in session.step_attempts.items():
            if attempts:
                step_first_times[step_id] = attempts[0].started_at
        
        if len(step_first_times) < 2:
            return False
        
        # Sort by first attempt time
        sorted_steps = sorted(step_first_times.items(), key=lambda x: x[1])
        
        # Check if any step was attempted again after a later step
        for i, (step_id, _) in enumerate(sorted_steps[:-1]):
            attempts = session.step_attempts[step_id]
            if len(attempts) > 1:
                last_attempt_time = attempts[-1].started_at
                # Check if any later step was attempted before this repeat
                for j in range(i + 1, len(sorted_steps)):
                    later_step_id, later_time = sorted_steps[j]
                    if later_time < last_attempt_time:
                        return True
        
        return False
    
    def _calculate_session_linearity(self, session: OnboardingSession, 
                                   flow_def: OnboardingFlowDefinition) -> float:
        """Calculate how linear the session progression was"""
        if not session.step_attempts or not flow_def.steps:
            return 0.0
        
        # Get expected order vs actual order
        expected_order = {step.step_id: step.order for step in flow_def.steps}
        
        actual_order = []
        for step_id, attempts in session.step_attempts.items():
            if attempts and step_id in expected_order:
                actual_order.append((expected_order[step_id], attempts[0].started_at))
        
        if len(actual_order) < 2:
            return 1.0
        
        # Sort by actual time
        actual_order.sort(key=lambda x: x[1])
        
        # Calculate how many steps are in correct order
        correct_order_count = 0
        for i in range(len(actual_order) - 1):
            if actual_order[i][0] < actual_order[i + 1][0]:
                correct_order_count += 1
        
        return correct_order_count / (len(actual_order) - 1)
    
    def _calculate_event_frequency(self, session: OnboardingSession) -> float:
        """Calculate events per minute"""
        duration = self._get_session_duration_minutes(session)
        if duration == 0:
            return 0.0
        return session.total_events / duration
    
    def _calculate_interaction_depth(self, session: OnboardingSession) -> float:
        """Calculate average events per step"""
        if not session.step_attempts:
            return 0.0
        
        total_events = sum(len(attempts[0].events) for attempts in session.step_attempts.values() if attempts)
        return total_events / len(session.step_attempts)
    
    def _extract_step_features(self, session: OnboardingSession, 
                             flow_def: OnboardingFlowDefinition) -> Dict[str, Any]:
        """Extract step-specific features"""
        step_features = {}
        
        for step in flow_def.steps:
            step_id = step.step_id
            prefix = f"step_{step_id}"
            
            # Default features
            step_features[f"{prefix}_attempted"] = step_id in session.step_attempts
            step_features[f"{prefix}_completed"] = step_id in session.completed_steps
            step_features[f"{prefix}_failed"] = step_id in session.failed_steps
            
            if step_id in session.step_attempts:
                attempts = session.step_attempts[step_id]
                first_attempt = attempts[0]
                
                step_features[f"{prefix}_attempt_count"] = len(attempts)
                step_features[f"{prefix}_time_spent"] = first_attempt.time_spent_seconds or 0
                step_features[f"{prefix}_event_count"] = len(first_attempt.events)
                step_features[f"{prefix}_success_events"] = len(first_attempt.success_events)
                step_features[f"{prefix}_error_events"] = len(first_attempt.error_events)
                step_features[f"{prefix}_retry_count"] = first_attempt.retry_count
                
                # Time to reach step
                step_features[f"{prefix}_time_to_reach"] = (
                    first_attempt.started_at - session.started_at
                ).total_seconds()
            else:
                step_features[f"{prefix}_attempt_count"] = 0
                step_features[f"{prefix}_time_spent"] = 0
                step_features[f"{prefix}_event_count"] = 0
                step_features[f"{prefix}_success_events"] = 0
                step_features[f"{prefix}_error_events"] = 0
                step_features[f"{prefix}_retry_count"] = 0
                step_features[f"{prefix}_time_to_reach"] = 0
        
        return step_features

# Global session reconstructor
session_reconstructor = None

def initialize_session_reconstructor(config: Dict[str, Any]):
    """Initialize global session reconstructor"""
    global session_reconstructor
    session_reconstructor = SessionReconstructor(config)

def get_session_reconstructor() -> SessionReconstructor:
    """Get global session reconstructor instance"""
    if not session_reconstructor:
        raise RuntimeError("Session reconstructor not initialized. Call initialize_session_reconstructor() first.")
    
    return session_reconstructor

def create_onboarding_flow_definition(flow_id: str, flow_name: str, 
                                    steps: List[Dict[str, Any]]) -> OnboardingFlowDefinition:
    """Create onboarding flow definition"""
    step_definitions = []
    
    for i, step_data in enumerate(steps):
        step_def = OnboardingStepDefinition(
            step_id=step_data['step_id'],
            step_name=step_data['step_name'],
            step_type=OnboardingStepType(step_data.get('step_type', 'custom')),
            order=i,
            required=step_data.get('required', True),
            description=step_data.get('description', ''),
            expected_events=step_data.get('expected_events', []),
            success_criteria=step_data.get('success_criteria', {}),
            timeout_minutes=step_data.get('timeout_minutes'),
            retry_allowed=step_data.get('retry_allowed', True),
            dependencies=step_data.get('dependencies', []),
            metadata=step_data.get('metadata', {})
        )
        step_definitions.append(step_def)
    
    return OnboardingFlowDefinition(
        flow_id=flow_id,
        flow_name=flow_name,
        description="",
        version="1.0",
        steps=step_definitions
    )

def reconstruct_onboarding_sessions(events: List[Dict[str, Any]]) -> List[OnboardingSession]:
    """Reconstruct onboarding sessions from events"""
    reconstructor = get_session_reconstructor()
    return reconstructor.reconstruct_sessions(events)
