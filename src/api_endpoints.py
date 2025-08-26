"""
Point 20: Domain API Endpoints (Part 1/2)
RESTful API endpoints for onboarding drop-off analysis and optimization.
"""

import os
import json
import time
import asyncio
import random
import base64
from io import BytesIO
from typing import Dict, List, Optional, Any, Union, Set, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
import logging
import uuid
from fastapi import FastAPI, HTTPException, Depends, Query, Path, Body, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel, Field, validator
import uvicorn

# Import from our modules
from .onboarding_models import (
    OnboardingSession, OnboardingFlowDefinition, OnboardingStatus, 
    OnboardingStepType, UserSegment
)
from .friction_detection import (
    FrictionPoint, FrictionCluster, FrictionType, FrictionSeverity
)
from .report import ReportGenerator, ReportType, ReportFormat
from .recommendation_engine import (
    Recommendation, RecommendationSuite, RecommendationType, RecommendationPriority
)
from .business_logic import (
    AnalysisRequest, AnalysisResult, OptimizationPlan, AnalysisScope,
    OptimizationGoal, BusinessLogicOrchestrator
)
from .report import (
    GeneratedReport, ReportConfiguration, ReportType, ReportFormat,
    ReportGenerator
)
from .config_manager import (
    ConfigurationManager, ConfigScope, EnvironmentType
)
from .email_digest import EmailDigestService, StakeholderGroup, create_email_digest_service
from .email_scheduler import EmailDigestScheduler, get_email_scheduler

logger = logging.getLogger(__name__)

# Pydantic Models for API
class SessionCreateRequest(BaseModel):
    user_id: str
    flow_id: str
    device_type: str = "desktop"
    user_agent: str = ""
    session_data: Dict[str, Any] = {}

class SessionUpdateRequest(BaseModel):
    current_step_id: Optional[str] = None
    status: Optional[str] = None
    completion_percentage: Optional[float] = None
    session_data: Optional[Dict[str, Any]] = None

class AnalysisCreateRequest(BaseModel):
    scope: str = Field(..., description="Analysis scope: single_flow, multiple_flows, user_segment, time_period, global")
    goals: List[str] = Field(..., description="Optimization goals")
    flow_ids: Optional[List[str]] = None
    user_segments: Optional[List[str]] = None
    start_date: datetime
    end_date: datetime
    min_session_count: int = 100
    confidence_threshold: float = 0.7
    priority: str = "normal"
    async_processing: bool = True

class ReportCreateRequest(BaseModel):
    report_type: str = Field(..., description="Type of report to generate")
    output_format: str = "pdf"
    flow_ids: List[str]
    start_date: datetime
    end_date: datetime
    include_sections: List[str] = ["executive_summary", "conversion_funnel", "friction_analysis", "recommendations"]
    recipients: List[str] = []

class FlowDefinitionRequest(BaseModel):
    flow_name: str
    description: str = ""
    steps: Dict[str, Dict[str, Any]]

class RecommendationUpdateRequest(BaseModel):
    status: str = Field(..., description="Recommendation status: generated, approved, in_progress, implemented, testing, rejected")
    implementation_notes: Optional[str] = None
    estimated_completion_date: Optional[datetime] = None

class ConfigUpdateRequest(BaseModel):
    value: Any
    scope: str = "global"
    environment: str = "development"

# Response Models
class APIResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Any] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class SessionResponse(BaseModel):
    session_id: str
    user_id: str
    flow_id: str
    status: str
    started_at: datetime
    current_step_id: str
    completion_percentage: float
    total_time_spent: float
    steps_completed: int
    device_type: str

class AnalysisResponse(BaseModel):
    analysis_id: str
    request_id: str
    status: str
    started_at: datetime
    completed_at: Optional[datetime]
    sessions_analyzed: int
    friction_points_detected: int
    recommendations_generated: int
    processing_time_seconds: float = 0.0

class FrictionPointResponse(BaseModel):
    friction_id: str
    step_id: str
    flow_id: str
    friction_type: str
    severity: str
    abandonment_rate: float
    avg_time_spent: float
    affected_users: int
    confidence: float
    detected_at: datetime

class RecommendationResponse(BaseModel):
    recommendation_id: str
    title: str
    description: str
    recommendation_type: str
    priority: str
    target_step_id: Optional[str]
    target_flow_id: str
    estimated_conversion_lift: float
    estimated_users_recovered: int
    estimated_effort_hours: float
    status: str

class ReportResponse(BaseModel):
    report_id: str
    title: str
    report_type: str
    output_format: str
    generated_at: datetime
    file_path: Optional[str]
    file_size: Optional[int]

# Security
security = HTTPBearer()

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """Verify API token"""
    # In production, this would validate JWT tokens or API keys
    token = credentials.credentials
    if not token or token == "invalid":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token"
        )
    return token

# Global instances
app = FastAPI(
    title="Onboarding Drop-off Analyzer API",
    description="Comprehensive API for onboarding analytics and optimization",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    # Vercel-specific optimizations
    root_path="/api" if os.getenv("VERCEL") else ""
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Supabase configuration: Use Supabase PostgreSQL for all environments
# Database URL will be automatically constructed from Supabase settings

# Global components (initialized on startup)
config_manager: Optional[ConfigurationManager] = None
business_orchestrator: Optional[BusinessLogicOrchestrator] = None
report_generator: Optional[ReportGenerator] = None

@app.on_event("startup")
async def startup_event():
    """Initialize application components"""
    global config_manager, business_orchestrator, report_generator
    
    # Initialize configuration manager
    config_manager = ConfigurationManager(
        config_dir="config",
        environment=EnvironmentType.PRODUCTION if os.getenv("VERCEL") else EnvironmentType.DEVELOPMENT
    )
    
    # Initialize business logic orchestrator
    business_config = {
        'max_workers': config_manager.get('performance.max_concurrent_analyses', 2),  # Reduced for serverless
        'min_sessions_for_analysis': config_manager.get('friction_detection.min_session_count', 100),
        'min_confidence_for_recommendations': config_manager.get('recommendations.min_impact_threshold', 0.05)
    }
    business_orchestrator = BusinessLogicOrchestrator(business_config)
    
    # Initialize report generator
    report_config = {
        'output_directory': '/tmp/reports' if os.getenv("VERCEL") else 'reports',  # Vercel tmp directory
        'template_directory': 'templates'
    }
    report_generator = ReportGenerator(report_config)
    
    # Note: Email digest scheduler disabled for Vercel serverless environment
    # Background tasks are not suitable for serverless deployments
    if not os.getenv("VERCEL"):
        from .email_scheduler import start_email_digest_scheduler
        start_email_digest_scheduler()
    
    logging.info("Application startup completed")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on application shutdown"""
    # Stop email digest scheduler (only if not on Vercel)
    if not os.getenv("VERCEL"):
        from .email_scheduler import stop_email_digest_scheduler
        stop_email_digest_scheduler()
    
    logging.info("Application shutdown")

# Health Check Endpoints
@app.get("/health", response_model=APIResponse)
async def health_check():
    """Health check endpoint"""
    return APIResponse(
        success=True,
        message="Service is healthy",
        data={
            "status": "healthy",
            "version": "1.0.0",
            "timestamp": datetime.utcnow()
        }
    )

@app.get("/health/detailed", response_model=APIResponse)
async def detailed_health_check(token: str = Depends(verify_token)):
    """Detailed health check with component status"""
    component_status = {
        "config_manager": config_manager is not None,
        "business_orchestrator": business_orchestrator is not None,
        "report_generator": report_generator is not None,
        "database": True,  # Would check actual database connection
        "integrations": True  # Would check integration health
    }
    
    overall_status = "healthy" if all(component_status.values()) else "unhealthy"
    
    return APIResponse(
        success=overall_status == "healthy",
        message=f"Service is {overall_status}",
        data={
            "overall_status": overall_status,
            "components": component_status,
            "uptime_seconds": time.time(),  # Would track actual uptime
            "memory_usage": "N/A",  # Would include actual memory stats
            "cpu_usage": "N/A"  # Would include actual CPU stats
        }
    )

# Session Management Endpoints
@app.post("/api/v1/sessions", response_model=APIResponse)
async def create_session(
    request: SessionCreateRequest,
    token: str = Depends(verify_token)
):
    """Create a new onboarding session"""
    try:
        session_id = f"session_{uuid.uuid4().hex[:8]}"
        
        # Create session object
        session = OnboardingSession(
            session_id=session_id,
            user_id=request.user_id,
            flow_id=request.flow_id,
            status=OnboardingStatus.IN_PROGRESS,
            started_at=datetime.utcnow(),
            current_step_id="step_1",  # Default first step
            completion_percentage=0.0,
            total_time_spent=0.0,
            steps_completed=0,
            device_type=request.device_type,
            user_agent=request.user_agent,
            session_data=request.session_data
        )
        
        # In production, save to database
        # await session_repository.create(session)
        
        return APIResponse(
            success=True,
            message="Session created successfully",
            data=SessionResponse(
                session_id=session.session_id,
                user_id=session.user_id,
                flow_id=session.flow_id,
                status=session.status.value,
                started_at=session.started_at,
                current_step_id=session.current_step_id,
                completion_percentage=session.completion_percentage,
                total_time_spent=session.total_time_spent,
                steps_completed=session.steps_completed,
                device_type=session.device_type
            ).dict()
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create session: {str(e)}"
        )

@app.get("/api/v1/sessions/{session_id}", response_model=APIResponse)
async def get_session(
    session_id: str = Path(..., description="Session ID"),
    token: str = Depends(verify_token)
):
    """Get session details"""
    try:
        # In production, fetch from database
        # session = await session_repository.get(session_id)
        
        # Mock session for demonstration
        session = OnboardingSession(
            session_id=session_id,
            user_id="user_123",
            flow_id="main_onboarding",
            status=OnboardingStatus.IN_PROGRESS,
            started_at=datetime.utcnow() - timedelta(minutes=30),
            current_step_id="step_3",
            completion_percentage=60.0,
            total_time_spent=1800.0,
            steps_completed=3,
            device_type="desktop",
            user_agent="Mozilla/5.0...",
            session_data={}
        )
        
        return APIResponse(
            success=True,
            message="Session retrieved successfully",
            data=SessionResponse(
                session_id=session.session_id,
                user_id=session.user_id,
                flow_id=session.flow_id,
                status=session.status.value,
                started_at=session.started_at,
                current_step_id=session.current_step_id,
                completion_percentage=session.completion_percentage,
                total_time_spent=session.total_time_spent,
                steps_completed=session.steps_completed,
                device_type=session.device_type
            ).dict()
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Session not found: {session_id}"
        )

@app.put("/api/v1/sessions/{session_id}", response_model=APIResponse)
async def update_session(
    session_id: str = Path(..., description="Session ID"),
    request: SessionUpdateRequest = Body(...),
    token: str = Depends(verify_token)
):
    """Update session progress"""
    try:
        # In production, update in database
        # session = await session_repository.update(session_id, request.dict(exclude_unset=True))
        
        return APIResponse(
            success=True,
            message="Session updated successfully",
            data={"session_id": session_id, "updated_at": datetime.utcnow()}
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update session: {str(e)}"
        )

@app.get("/api/v1/sessions", response_model=APIResponse)
async def list_sessions(
    flow_id: Optional[str] = Query(None, description="Filter by flow ID"),
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    status: Optional[str] = Query(None, description="Filter by status"),
    start_date: Optional[datetime] = Query(None, description="Filter by start date"),
    end_date: Optional[datetime] = Query(None, description="Filter by end date"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of sessions to return"),
    offset: int = Query(0, ge=0, description="Number of sessions to skip"),
    token: str = Depends(verify_token)
):
    """List sessions with filtering"""
    try:
        # In production, query database with filters
        # sessions = await session_repository.list(filters, limit, offset)
        
        # Mock data for demonstration
        mock_sessions = [
            SessionResponse(
                session_id=f"session_{i}",
                user_id=f"user_{i}",
                flow_id=flow_id or "main_onboarding",
                status="completed" if i % 3 == 0 else "in_progress",
                started_at=datetime.utcnow() - timedelta(hours=i),
                current_step_id=f"step_{(i % 5) + 1}",
                completion_percentage=100.0 if i % 3 == 0 else (i % 4) * 25,
                total_time_spent=300.0 + (i % 10) * 30,
                steps_completed=5 if i % 3 == 0 else (i % 4) + 1,
                device_type="desktop" if i % 2 == 0 else "mobile"
            )
            for i in range(min(limit, 20))
        ]
        
        return APIResponse(
            success=True,
            message="Sessions retrieved successfully",
            data={
                "sessions": [session.dict() for session in mock_sessions],
                "total_count": len(mock_sessions),
                "limit": limit,
                "offset": offset
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve sessions: {str(e)}"
        )

# Analysis Endpoints
@app.post("/api/v1/analyses", response_model=APIResponse)
async def create_analysis(
    request: AnalysisCreateRequest,
    token: str = Depends(verify_token)
):
    """Create a new analysis request"""
    try:
        if not business_orchestrator:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Business orchestrator not available"
            )
        
        # Convert request to internal format
        analysis_request = AnalysisRequest(
            request_id=f"req_{uuid.uuid4().hex[:8]}",
            scope=AnalysisScope(request.scope),
            goals=[OptimizationGoal(goal) for goal in request.goals],
            flow_ids=request.flow_ids or [],
            user_segments=[UserSegment(seg) for seg in (request.user_segments or [])],
            date_range=(request.start_date, request.end_date),
            min_session_count=request.min_session_count,
            confidence_threshold=request.confidence_threshold,
            priority=request.priority,
            async_processing=request.async_processing
        )
        
        # Submit analysis request
        analysis_id = business_orchestrator.submit_analysis_request(analysis_request)
        
        return APIResponse(
            success=True,
            message="Analysis request submitted successfully",
            data={
                "analysis_id": analysis_id,
                "request_id": analysis_request.request_id,
                "status": "pending",
                "estimated_completion_time": datetime.utcnow() + timedelta(minutes=10)
            }
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create analysis: {str(e)}"
        )

@app.get("/api/v1/analyses/{analysis_id}", response_model=APIResponse)
async def get_analysis(
    analysis_id: str = Path(..., description="Analysis ID"),
    token: str = Depends(verify_token)
):
    """Get analysis status and results"""
    try:
        if not business_orchestrator:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Business orchestrator not available"
            )
        
        analysis_result = business_orchestrator.get_analysis_status(analysis_id)
        
        if not analysis_result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Analysis not found: {analysis_id}"
            )
        
        return APIResponse(
            success=True,
            message="Analysis retrieved successfully",
            data=AnalysisResponse(
                analysis_id=analysis_result.analysis_id,
                request_id=analysis_result.request_id,
                status=analysis_result.status.value,
                started_at=analysis_result.started_at,
                completed_at=analysis_result.completed_at,
                sessions_analyzed=analysis_result.sessions_analyzed,
                friction_points_detected=analysis_result.friction_points_detected,
                recommendations_generated=analysis_result.recommendations_generated,
                processing_time_seconds=analysis_result.processing_time_seconds
            ).dict()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve analysis: {str(e)}"
        )

@app.delete("/api/v1/analyses/{analysis_id}", response_model=APIResponse)
async def cancel_analysis(
    analysis_id: str = Path(..., description="Analysis ID"),
    token: str = Depends(verify_token)
):
    """Cancel a running analysis"""
    try:
        if not business_orchestrator:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Business orchestrator not available"
            )
        
        success = business_orchestrator.cancel_analysis(analysis_id)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Analysis not found or cannot be cancelled: {analysis_id}"
            )
        
        return APIResponse(
            success=True,
            message="Analysis cancelled successfully",
            data={"analysis_id": analysis_id, "cancelled_at": datetime.utcnow()}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel analysis: {str(e)}"
        )

# Friction Analysis Endpoints
@app.get("/api/v1/friction-points", response_model=List[Dict[str, Any]])
async def list_friction_points(
    flow_id: Optional[str] = Query(None, description="Filter by flow ID"),
    severity: Optional[str] = Query(None, description="Filter by severity"),
    date_from: Optional[datetime] = Query(None, description="Filter from date"),
    date_to: Optional[datetime] = Query(None, description="Filter to date"),
    skip: int = Query(0, ge=0, description="Skip number of records"),
    limit: int = Query(100, ge=1, le=1000, description="Limit number of records"),
    token: str = Depends(verify_token)
):
    """Get list of friction points with filtering"""
    try:
        # Mock friction points for demonstration
        friction_points = []
        
        for i in range(min(limit, 50)):
            friction_point = {
                "friction_id": f"friction_{i}",
                "flow_id": flow_id or f"flow_{i % 3}",
                "step_id": f"step_{(i % 5) + 1}",
                "friction_type": ["navigation_difficulty", "performance_issue", "content_clarity"][i % 3],
                "severity": ["low", "medium", "high"][i % 3],
                "description": f"Users experiencing difficulty at step {(i % 5) + 1}",
                "affected_users": 100 + (i * 10),
                "detection_confidence": 0.75 + (i % 3) * 0.05,
                "first_detected": (datetime.utcnow() - timedelta(days=i % 30)).isoformat(),
                "last_detected": (datetime.utcnow() - timedelta(hours=i % 24)).isoformat(),
                "resolution_suggestions": [f"Improve step {(i % 5) + 1} clarity", "Add helpful tooltips"],
                "business_impact": {
                    "estimated_lost_conversions": 20 + (i * 2),
                    "revenue_impact": 1000 + (i * 100)
                }
            }
            friction_points.append(friction_point)
        
        return friction_points
        
    except Exception as e:
        logger.error(f"Failed to retrieve friction points: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve friction points")

@app.get("/api/v1/friction-points/{friction_id}", response_model=Dict[str, Any])
async def get_friction_point(
    friction_id: str = Path(..., description="Friction point ID"),
    token: str = Depends(verify_token)
):
    """Get detailed friction point information"""
    try:
        # Mock friction point details
        friction_point = {
            "friction_id": friction_id,
            "flow_id": "checkout_flow",
            "step_id": "payment_step",
            "friction_type": "performance_issue",
            "severity": "high",
            "description": "Payment form loading slowly causing user drop-off",
            "affected_users": 250,
            "detection_confidence": 0.92,
            "first_detected": (datetime.utcnow() - timedelta(days=5)).isoformat(),
            "last_detected": (datetime.utcnow() - timedelta(hours=2)).isoformat(),
            "resolution_suggestions": [
                "Optimize payment form rendering",
                "Implement progressive loading",
                "Add loading indicators"
            ],
            "business_impact": {
                "estimated_lost_conversions": 75,
                "revenue_impact": 7500,
                "impact_analysis": {
                    "daily_affected_users": 50,
                    "conversion_rate_impact": -0.15,
                    "average_order_value": 100
                }
            }
        }
        
        return friction_point
        
    except Exception as e:
        logger.error(f"Failed to retrieve friction point {friction_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve friction point")

@app.post("/api/v1/friction-analysis", response_model=APIResponse)
async def create_friction_analysis(
    request: Dict[str, Any] = Body(..., description="Friction analysis configuration"),
    token: str = Depends(verify_token)
):
    """Create a new friction analysis job"""
    try:
        analysis_id = f"friction_analysis_{uuid.uuid4().hex[:8]}"
        
        # In real implementation, queue analysis job
        analysis_response = {
            "analysis_id": analysis_id,
            "status": "queued",
            "created_at": datetime.utcnow().isoformat(),
            "flow_ids": request.get("flow_ids", []),
            "analysis_type": "friction_detection",
            "estimated_completion": (datetime.utcnow() + timedelta(minutes=10)).isoformat()
        }
        
        return APIResponse(
            success=True,
            message="Friction analysis created successfully",
            data=analysis_response
        )
        
    except Exception as e:
        logger.error(f"Failed to create friction analysis: {e}")
        raise HTTPException(status_code=500, detail="Failed to create friction analysis")

# Recommendation Management Endpoints
@app.get("/api/v1/recommendations", response_model=List[Dict[str, Any]])
async def list_recommendations(
    flow_id: Optional[str] = Query(None, description="Filter by flow ID"),
    priority: Optional[str] = Query(None, description="Filter by priority"),
    status: Optional[str] = Query(None, description="Filter by status"),
    skip: int = Query(0, ge=0, description="Skip number of records"),
    limit: int = Query(100, ge=1, le=1000, description="Limit number of records"),
    token: str = Depends(verify_token)
):
    """Get list of recommendations with filtering"""
    try:
        recommendations = []
        
        # Mock recommendations
        for i in range(min(limit, 25)):
            recommendation = {
                "recommendation_id": f"rec_{i}",
                "title": f"Optimize Step {(i % 5) + 1} User Experience",
                "description": f"Improve user engagement and reduce friction at step {(i % 5) + 1}",
                "priority": ["low", "medium", "high"][i % 3],
                "category": ["user_experience", "performance", "content"][i % 3],
                "estimated_impact": {
                    "conversion_lift": 0.05 + (i % 10) * 0.01,
                    "time_savings": 30 + (i * 5),
                    "users_affected": 500 + (i * 50),
                    "revenue_impact": 2500 + (i * 200)
                },
                "implementation": {
                    "effort_hours": 8 + (i % 20) * 2,
                    "complexity": ["low", "medium", "high"][i % 3],
                    "required_resources": ["frontend", "backend", "design"][i % 3:i % 3 + 1]
                },
                "status": ["pending", "in_progress", "completed"][i % 3],
                "created_at": (datetime.utcnow() - timedelta(days=i % 30)).isoformat(),
                "flow_id": flow_id or f"flow_{i % 3}"
            }
            recommendations.append(recommendation)
        
        return recommendations
        
    except Exception as e:
        logger.error(f"Failed to retrieve recommendations: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve recommendations")

@app.get("/api/v1/recommendations/{recommendation_id}", response_model=Dict[str, Any])
async def get_recommendation(
    recommendation_id: str = Path(..., description="Recommendation ID"),
    token: str = Depends(verify_token)
):
    """Get detailed recommendation information"""
    try:
        # Mock detailed recommendation
        recommendation = {
            "recommendation_id": recommendation_id,
            "title": "Implement Progressive Disclosure for Signup Form",
            "description": "Break down the lengthy signup form into smaller, manageable steps to reduce cognitive load and improve completion rates",
            "priority": "high",
            "category": "form_optimization",
            "estimated_impact": {
                "conversion_lift": 0.12,
                "time_savings": 45,
                "users_affected": 1200,
                "revenue_impact": 15000
            },
            "implementation": {
                "effort_hours": 24,
                "complexity": "medium",
                "required_resources": ["frontend", "ux_design", "backend"],
                "technical_requirements": [
                    "Update form validation logic",
                    "Implement step progress indicator",
                    "Add form state management"
                ]
            },
            "status": "pending",
            "created_at": (datetime.utcnow() - timedelta(days=3)).isoformat(),
            "flow_id": "signup_flow",
            "supporting_data": {
                "friction_points_addressed": ["cognitive_overload", "form_complexity"],
                "user_feedback_score": 3.2,
                "a_b_test_confidence": 0.95
            }
        }
        
        return recommendation
        
    except Exception as e:
        logger.error(f"Failed to retrieve recommendation {recommendation_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve recommendation")

@app.put("/api/v1/recommendations/{recommendation_id}/status", response_model=APIResponse)
async def update_recommendation_status(
    recommendation_id: str = Path(..., description="Recommendation ID"),
    status_update: Dict[str, Any] = Body(..., description="Status update information"),
    token: str = Depends(verify_token)
):
    """Update recommendation implementation status"""
    try:
        # In real implementation, update database record
        
        response_data = {
            "recommendation_id": recommendation_id,
            "status": status_update.get("status"),
            "updated_at": datetime.utcnow().isoformat(),
            "notes": status_update.get("notes")
        }
        
        return APIResponse(
            success=True,
            message="Recommendation status updated successfully",
            data=response_data
        )
        
    except Exception as e:
        logger.error(f"Failed to update recommendation status: {e}")
        raise HTTPException(status_code=500, detail="Failed to update recommendation status")

@app.post("/api/v1/recommendations/generate", response_model=APIResponse)
async def generate_recommendations(
    request: Dict[str, Any] = Body(..., description="Recommendation generation configuration"),
    token: str = Depends(verify_token)
):
    """Generate new recommendations based on analysis"""
    try:
        analysis_id = f"rec_gen_{uuid.uuid4().hex[:8]}"
        
        # In real implementation, queue recommendation generation job
        analysis_response = {
            "analysis_id": analysis_id,
            "status": "processing",
            "created_at": datetime.utcnow().isoformat(),
            "flow_ids": request.get("flow_ids", []),
            "analysis_type": "recommendation_generation",
            "estimated_completion": (datetime.utcnow() + timedelta(minutes=15)).isoformat()
        }
        
        return APIResponse(
            success=True,
            message="Recommendation generation started",
            data=analysis_response
        )
        
    except Exception as e:
        logger.error(f"Failed to generate recommendations: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate recommendations")

# Report Generation Endpoints
@app.get("/api/v1/reports", response_model=List[Dict[str, Any]])
async def list_reports(
    report_type: Optional[str] = Query(None, description="Filter by report type"),
    status: Optional[str] = Query(None, description="Filter by status"),
    skip: int = Query(0, ge=0, description="Skip number of records"),
    limit: int = Query(50, ge=1, le=200, description="Limit number of records"),
    token: str = Depends(verify_token)
):
    """Get list of generated reports"""
    try:
        reports = []
        
        # Mock reports
        for i in range(min(limit, 20)):
            report = {
                "report_id": f"report_{i}",
                "title": f"Onboarding Analysis Report - {datetime.utcnow().strftime('%Y-%m-%d')}",
                "report_type": ["executive_summary", "detailed_analysis", "friction_report"][i % 3],
                "status": ["completed", "generating", "failed"][i % 3],
                "created_at": (datetime.utcnow() - timedelta(days=i % 30)).isoformat(),
                "generated_at": (datetime.utcnow() - timedelta(days=i % 30, hours=1)).isoformat() if i % 3 == 0 else None,
                "file_size": 1024 * (100 + i * 50) if i % 3 == 0 else None,
                "download_url": f"/api/v1/reports/report_{i}/download" if i % 3 == 0 else None
            }
            reports.append(report)
        
        return reports
        
    except Exception as e:
        logger.error(f"Failed to retrieve reports: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve reports")

@app.post("/api/v1/reports/generate", response_model=APIResponse)
async def generate_report(
    request: Dict[str, Any] = Body(..., description="Report generation configuration"),
    token: str = Depends(verify_token)
):
    """Generate a new report"""
    try:
        report_id = f"report_{uuid.uuid4().hex[:8]}"
        
        # In real implementation, queue report generation
        response_data = {
            "report_id": report_id,
            "status": "queued",
            "created_at": datetime.utcnow().isoformat(),
            "estimated_completion": (datetime.utcnow() + timedelta(minutes=20)).isoformat(),
            "report_type": request.get("report_type"),
            "configuration": request.get("configuration", {})
        }
        
        return APIResponse(
            success=True,
            message="Report generation started",
            data=response_data
        )
        
    except Exception as e:
        logger.error(f"Failed to generate report: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate report")

@app.get("/api/v1/reports/{report_id}", response_model=APIResponse)
async def get_report_status(
    report_id: str = Path(..., description="Report ID"),
    token: str = Depends(verify_token)
):
    """Get report generation status"""
    try:
        # Mock report status
        report_status = {
            "report_id": report_id,
            "status": "completed",
            "progress": 100,
            "created_at": (datetime.utcnow() - timedelta(hours=2)).isoformat(),
            "generated_at": (datetime.utcnow() - timedelta(hours=1)).isoformat(),
            "file_size": 2048000,
            "download_url": f"/api/v1/reports/{report_id}/download"
        }
        
        return APIResponse(
            success=True,
            message="Report status retrieved",
            data=report_status
        )
        
    except Exception as e:
        logger.error(f"Failed to get report status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get report status")

@app.get("/api/v1/reports/{report_id}/download")
async def download_report(
    report_id: str = Path(..., description="Report ID"),
    token: str = Depends(verify_token)
):
    """Download generated report"""
    try:
        # In real implementation, return file from storage
        # For now, return a placeholder response
        return APIResponse(
            success=True,
            message=f"Report {report_id} download endpoint",
            data={"download_url": f"/reports/{report_id}.pdf"}
        )
        
    except Exception as e:
        logger.error(f"Failed to download report: {e}")
        raise HTTPException(status_code=500, detail="Failed to download report")

# Configuration Management Endpoints
@app.get("/api/v1/config", response_model=APIResponse)
async def get_configuration(
    category: Optional[str] = Query(None, description="Filter by category"),
    scope: Optional[str] = Query(None, description="Filter by scope"),
    token: str = Depends(verify_token)
):
    """Get system configuration"""
    try:
        # In real implementation, get from ConfigManager
        config = {
            "analytics": {
                "posthog_api_key": "ph_***",
                "batch_size": 1000,
                "sync_interval": 300
            },
            "friction_detection": {
                "abandonment_threshold": 0.3,
                "confidence_threshold": 0.7,
                "min_session_count": 100
            },
            "recommendations": {
                "max_recommendations": 10,
                "impact_threshold": 0.05,
                "implementation_effort_weight": 0.3
            }
        }
        
        if category:
            config = {category: config.get(category, {})}
        
        return APIResponse(
            success=True,
            message="Configuration retrieved",
            data=config
        )
        
    except Exception as e:
        logger.error(f"Failed to get configuration: {e}")
        raise HTTPException(status_code=500, detail="Failed to get configuration")

@app.put("/api/v1/config/{config_key}", response_model=APIResponse)
async def update_configuration(
    config_key: str = Path(..., description="Configuration key"),
    config_update: Dict[str, Any] = Body(..., description="Configuration update"),
    token: str = Depends(verify_token)
):
    """Update system configuration"""
    try:
        # In real implementation, update via ConfigManager
        
        response_data = {
            "config_key": config_key,
            "value": config_update.get("value"),
            "updated_at": datetime.utcnow().isoformat(),
            "requires_restart": config_update.get("requires_restart", False)
        }
        
        return APIResponse(
            success=True,
            message="Configuration updated successfully",
            data=response_data
        )
        
    except Exception as e:
        logger.error(f"Failed to update configuration: {e}")
        raise HTTPException(status_code=500, detail="Failed to update configuration")

@app.post("/api/v1/config/validate", response_model=APIResponse)
async def validate_configuration(
    config_data: Dict[str, Any] = Body(..., description="Configuration to validate"),
    token: str = Depends(verify_token)
):
    """Validate configuration values"""
    try:
        # In real implementation, use ConfigValidator
        
        validation_result = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "validated_at": datetime.utcnow().isoformat()
        }
        
        return APIResponse(
            success=True,
            message="Configuration validation completed",
            data=validation_result
        )
        
    except Exception as e:
        logger.error(f"Failed to validate configuration: {e}")
        raise HTTPException(status_code=500, detail="Failed to validate configuration")

# Integration Management Endpoints
@app.get("/api/v1/integrations", response_model=List[Dict[str, Any]])
async def list_integrations(token: str = Depends(verify_token)):
    """Get list of system integrations and their status"""
    try:
        integrations = [
            {
                "integration_id": "posthog_analytics",
                "name": "PostHog Analytics",
                "type": "analytics_platform",
                "status": "connected",
                "last_sync": (datetime.utcnow() - timedelta(minutes=5)).isoformat(),
                "health_score": 0.95,
                "error_count": 0
            },
            {
                "integration_id": "sendgrid_email",
                "name": "SendGrid Email Service",
                "type": "email_service",
                "status": "connected",
                "last_sync": (datetime.utcnow() - timedelta(hours=1)).isoformat(),
                "health_score": 0.98,
                "error_count": 0
            },
            {
                "integration_id": "slack_notifications",
                "name": "Slack Notifications",
                "type": "notification_system",
                "status": "warning",
                "last_sync": (datetime.utcnow() - timedelta(hours=6)).isoformat(),
                "health_score": 0.75,
                "error_count": 3
            }
        ]
        
        return integrations
        
    except Exception as e:
        logger.error(f"Failed to retrieve integrations: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve integrations")

@app.post("/api/v1/integrations/{integration_id}/test", response_model=APIResponse)
async def test_integration(
    integration_id: str = Path(..., description="Integration ID"),
    token: str = Depends(verify_token)
):
    """Test integration connectivity"""
    try:
        # In real implementation, test actual integration
        
        test_result = {
            "integration_id": integration_id,
            "test_status": "success",
            "response_time_ms": 150,
            "tested_at": datetime.utcnow().isoformat(),
            "details": "Connection successful, all endpoints responding"
        }
        
        return APIResponse(
            success=True,
            message="Integration test completed",
            data=test_result
        )
        
    except Exception as e:
        logger.error(f"Failed to test integration: {e}")
        raise HTTPException(status_code=500, detail="Failed to test integration")

@app.post("/api/v1/integrations/{integration_id}/sync", response_model=APIResponse)
async def trigger_integration_sync(
    integration_id: str = Path(..., description="Integration ID"),
    token: str = Depends(verify_token)
):
    """Trigger manual data synchronization"""
    try:
        sync_id = f"sync_{uuid.uuid4().hex[:8]}"
        
        # In real implementation, trigger actual sync
        
        sync_response = {
            "sync_id": sync_id,
            "integration_id": integration_id,
            "status": "started",
            "started_at": datetime.utcnow().isoformat(),
            "estimated_completion": (datetime.utcnow() + timedelta(minutes=5)).isoformat()
        }
        
        return APIResponse(
            success=True,
            message="Integration sync started",
            data=sync_response
        )
        
    except Exception as e:
        logger.error(f"Failed to trigger sync: {e}")
        raise HTTPException(status_code=500, detail="Failed to trigger sync")

# Email Digest Management Endpoints

# Pydantic models for email digest
class StakeholderGroupRequest(BaseModel):
    name: str = Field(..., description="Name of the stakeholder group")
    email_addresses: List[str] = Field(..., description="List of email addresses")
    report_format: str = Field(default="html", pattern="^(html|pdf|both)$", description="Report format preference")
    include_attachments: bool = Field(default=True, description="Whether to include file attachments")
    summary_level: str = Field(default="executive", pattern="^(executive|detailed|technical)$", description="Summary detail level")

class EmailConfigRequest(BaseModel):
    smtp_host: str = Field(..., description="SMTP server hostname")
    smtp_port: int = Field(default=587, description="SMTP server port")
    username: str = Field(default="", description="SMTP username")
    use_tls: bool = Field(default=True, description="Use TLS encryption")
    sender_email: str = Field(..., description="Sender email address")
    sender_name: str = Field(default="Onboarding Analytics System", description="Sender display name")

class ScheduleConfigRequest(BaseModel):
    enabled: bool = Field(default=True, description="Enable scheduled delivery")
    day_of_week: int = Field(default=1, ge=0, le=6, description="Day of week (0=Monday, 6=Sunday)")
    hour: int = Field(default=9, ge=0, le=23, description="Hour of day (24-hour format)")
    timezone: str = Field(default="UTC", description="Timezone for scheduling")

@app.get("/api/v1/email-digest/status", response_model=dict)
async def get_email_digest_status(token: str = Depends(verify_token)):
    """
    Get email digest system status.
    
    Returns information about scheduler status, stakeholder groups, and next delivery time.
    """
    try:
        scheduler = get_email_scheduler()
        status = scheduler.get_status()
        
        digest_service = scheduler.digest_service
        
        return APIResponse(
            success=True,
            message="Email digest status retrieved",
            data={
                **status,
                'next_run_formatted': status['next_run'].isoformat() if status['next_run'] else None,
                'stakeholder_groups_detail': [
                    {
                        'name': group.name,
                        'email_count': len(group.email_addresses),
                        'report_format': group.report_format,
                        'summary_level': group.summary_level
                    }
                    for group in digest_service.stakeholder_groups
                ]
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to get email digest status: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve email digest status")

@app.post("/api/v1/email-digest/stakeholder-groups", response_model=dict)
async def add_stakeholder_group(
    group_request: StakeholderGroupRequest,
    token: str = Depends(verify_token)
):
    """
    Add a new stakeholder group for email digest delivery.
    
    Allows configuration of email recipients with specific format and detail preferences.
    """
    try:
        scheduler = get_email_scheduler()
        digest_service = scheduler.digest_service
        
        # Create stakeholder group
        group = StakeholderGroup(
            name=group_request.name,
            email_addresses=group_request.email_addresses,
            report_format=group_request.report_format,
            include_attachments=group_request.include_attachments,
            summary_level=group_request.summary_level
        )
        
        # Add to service
        digest_service.add_stakeholder_group(group)
        
        return APIResponse(
            success=True,
            message=f"Stakeholder group '{group_request.name}' added successfully",
            data={
                'group_name': group.name,
                'email_count': len(group.email_addresses),
                'total_groups': len(digest_service.stakeholder_groups)
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to add stakeholder group: {e}")
        raise HTTPException(status_code=500, detail="Failed to add stakeholder group")

@app.put("/api/v1/email-digest/schedule", response_model=dict)
async def update_email_schedule(
    schedule_request: ScheduleConfigRequest,
    token: str = Depends(verify_token)
):
    """
    Update email digest delivery schedule.
    
    Configures when automated weekly digests are sent.
    """
    try:
        scheduler = get_email_scheduler()
        
        from .email_digest import DigestSchedule
        schedule_config = DigestSchedule(
            enabled=schedule_request.enabled,
            day_of_week=schedule_request.day_of_week,
            hour=schedule_request.hour,
            timezone=schedule_request.timezone
        )
        
        # Update schedule
        scheduler.digest_service.configure_schedule(schedule_config)
        
        # Restart scheduler with new config if running
        if scheduler.running:
            scheduler.stop_scheduler()
            scheduler.start_scheduler()
        
        return APIResponse(
            success=True,
            message="Email digest schedule updated successfully",
            data={
                'enabled': schedule_config.enabled,
                'day_of_week': schedule_config.day_of_week,
                'hour': schedule_config.hour,
                'timezone': schedule_config.timezone,
                'next_run': scheduler.get_next_scheduled_time().isoformat() if scheduler.get_next_scheduled_time() else None
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to update email schedule: {e}")
        raise HTTPException(status_code=500, detail="Failed to update email schedule")

@app.post("/api/v1/email-digest/send-now", response_model=dict)
async def send_digest_now(token: str = Depends(verify_token)):
    """
    Send email digest immediately.
    
    Triggers immediate generation and delivery of weekly digest to all stakeholder groups.
    """
    try:
        scheduler = get_email_scheduler()
        
        # Send digest now
        await scheduler.send_digest_now()
        
        return APIResponse(
            success=True,
            message="Email digest sent successfully",
            data={
                'sent_at': datetime.now().isoformat(),
                'groups_notified': len(scheduler.digest_service.stakeholder_groups)
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to send email digest: {e}")
        raise HTTPException(status_code=500, detail="Failed to send email digest")

@app.post("/api/v1/email-digest/test", response_model=dict)
async def test_email_configuration(
    test_email: str = Query(..., description="Email address to send test message"),
    token: str = Depends(verify_token)
):
    """
    Test email configuration by sending a test message.
    
    Validates SMTP settings and email delivery functionality.
    """
    try:
        scheduler = get_email_scheduler()
        digest_service = scheduler.digest_service
        
        # Create test stakeholder group
        test_group = StakeholderGroup(
            name="Test Group",
            email_addresses=[test_email],
            report_format="html",
            summary_level="executive"
        )
        
        # Generate test digest data
        test_data = {
            'week_ending': datetime.now().strftime('%B %d, %Y'),
            'total_users': 1234,
            'completion_rate': 78.5,
            'drop_off_reduction': 2.3,
            'executive_summary': "This is a test email digest to verify email configuration and delivery.",
            'top_recommendations': [
                {
                    'title': 'Test Recommendation',
                    'description': 'This is a test recommendation for email validation.',
                    'expected_impact': '+5% completion rate'
                }
            ],
            'top_friction_points': [
                {
                    'step_name': 'Test Step',
                    'description': 'This is a test friction point for email validation.',
                    'drop_off_rate': 15.2
                }
            ],
            'dashboard_url': 'https://dashboard.example.com'
        }
        
        # Send test email
        await digest_service.send_digest_to_group(test_group, test_data)
        
        return APIResponse(
            success=True,
            message=f"Test email sent successfully to {test_email}",
            data={
                'test_email': test_email,
                'sent_at': datetime.now().isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to send test email: {e}")
        raise HTTPException(status_code=500, detail=f"Email test failed: {str(e)}")

@app.get("/api/v1/email-digest/preview", response_model=dict)
async def preview_email_digest(
    format_type: str = Query(default="html", pattern="^(html|json)$", description="Preview format"),
    summary_level: str = Query(default="executive", pattern="^(executive|technical)$", description="Summary level"),
    token: str = Depends(verify_token)
):
    """
    Preview email digest content without sending.
    
    Generates digest content for preview purposes.
    """
    try:
        scheduler = get_email_scheduler()
        digest_service = scheduler.digest_service
        
        # Generate digest data
        digest_data = await digest_service.generate_weekly_digest_data()
        
        if format_type == "json":
            return APIResponse(
                success=True,
                message="Email digest preview generated",
                data=digest_data
            )
        else:
            # Generate HTML preview
            if summary_level == "executive":
                html_content = digest_service.executive_template.render(**digest_data)
            else:
                html_content = digest_service.technical_template.render(**digest_data)
            
            return APIResponse(
                success=True,
                message="Email digest preview generated",
                data={
                    'html_content': html_content,
                    'subject': f"📊 Weekly Onboarding Analytics - {digest_data['week_ending']}",
                    'digest_metadata': {
                        'total_users': digest_data['total_users'],
                        'completion_rate': digest_data['completion_rate'],
                        'recommendations_count': len(digest_data['top_recommendations'])
                    }
                }
            )
        
    except Exception as e:
        logger.error(f"Failed to generate email digest preview: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate preview")

# Real-time Analytics Endpoints
@app.get("/api/v1/analytics/dashboard-stats", response_model=APIResponse)
async def get_dashboard_stats():
    """Get real-time dashboard statistics"""
    try:
        # Generate realistic time-series data
        now = datetime.now()
        trend_labels = []
        trend_values = []
        base_conversion = 75
        
        for i in range(6):
            date = now - timedelta(days=30*i)
            trend_labels.insert(0, date.strftime('%b'))
            # Add some realistic variation
            conversion = base_conversion + random.uniform(-5, 10)
            trend_values.insert(0, round(conversion, 1))
        
        # Generate funnel data with realistic drop-offs
        total_users = random.randint(8000, 12000)
        funnel_data = []
        current_users = total_users
        drop_rates = [0, 0.15, 0.12, 0.08, 0.20, 0.05]  # Realistic drop-off rates
        
        for i, drop_rate in enumerate(drop_rates):
            current_users = int(current_users * (1 - drop_rate))
            funnel_data.append(current_users)
        
        # Generate device segment data
        mobile_pct = random.uniform(55, 65)
        desktop_pct = random.uniform(30, 40)
        tablet_pct = 100 - mobile_pct - desktop_pct
        
        analytics_data = {
            "totalUsers": total_users,
            "conversionRate": round(trend_values[-1], 1),
            "frictionPoints": random.randint(8, 15),
            "recommendations": random.randint(5, 12),
            "funnelData": {
                "labels": ["Landing", "Signup", "Verification", "Profile", "First Action", "Complete"],
                "values": funnel_data
            },
            "dropoffData": {
                "labels": ["Form Errors", "Slow Loading", "Complex UI", "Technical Issues"],
                "values": [
                    random.randint(25, 40),
                    random.randint(20, 35),
                    random.randint(15, 30),
                    random.randint(10, 25)
                ]
            },
            "segmentData": {
                "labels": ["Mobile", "Desktop", "Tablet"],
                "values": [round(mobile_pct, 1), round(desktop_pct, 1), round(tablet_pct, 1)]
            },
            "trendsData": {
                "labels": trend_labels,
                "values": trend_values
            }
        }
        
        return APIResponse(
            success=True,
            message="Dashboard statistics retrieved",
            data=analytics_data
        )
        
    except Exception as e:
        logger.error(f"Failed to get dashboard stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve dashboard statistics")

@app.post("/api/v1/analytics/start-analysis", response_model=APIResponse)
async def start_analysis_endpoint(request: AnalysisCreateRequest):
    """Start a comprehensive analytics analysis"""
    try:
        # Generate a unique analysis ID
        analysis_id = f"analysis_{uuid.uuid4().hex[:8]}"
        
        # In a real implementation, this would trigger actual ML analysis
        analysis_data = {
            "analysis_id": analysis_id,
            "status": "started",
            "scope": request.scope,
            "goals": request.goals,
            "estimated_completion": datetime.utcnow() + timedelta(minutes=random.randint(5, 15)),
            "progress": 0
        }
        
        return APIResponse(
            success=True,
            message="Analysis started successfully",
            data=analysis_data
        )
        
    except Exception as e:
        logger.error(f"Failed to start analysis: {e}")
        raise HTTPException(status_code=500, detail="Failed to start analysis")

@app.post("/api/v1/analytics/ml-analysis", response_model=APIResponse)
async def start_ml_analysis():
    """Start ML analysis pipeline"""
    try:
        job_id = f"ml_job_{uuid.uuid4().hex[:8]}"
        
        ml_analysis_data = {
            "job_id": job_id,
            "status": "running",
            "models": ["clustering", "prediction", "recommendation"],
            "started_at": datetime.utcnow(),
            "estimated_completion": datetime.utcnow() + timedelta(minutes=random.randint(10, 20))
        }
        
        return APIResponse(
            success=True,
            message="ML analysis started",
            data=ml_analysis_data
        )
        
    except Exception as e:
        logger.error(f"Failed to start ML analysis: {e}")
        raise HTTPException(status_code=500, detail="Failed to start ML analysis")

@app.post("/api/v1/reports/generate")
async def generate_pdf_report(request: Optional[ReportCreateRequest] = None):
    """Generate and download PDF report"""
    try:
        # In production, this would generate an actual PDF
        # For now, return a sample PDF with real report structure
        
        # Create a simple PDF-like response
        # In real implementation, use libraries like ReportLab or WeasyPrint
        pdf_content = f"""
%PDF-1.4
1 0 obj
<<
/Type /Catalog
/Pages 2 0 R
>>
endobj

2 0 obj
<<
/Type /Pages
/Kids [3 0 R]
/Count 1
>>
endobj

3 0 obj
<<
/Type /Page
/Parent 2 0 R
/MediaBox [0 0 612 792]
/Contents 4 0 R
>>
endobj

4 0 obj
<<
/Length 200
>>
stream
BT
/F1 12 Tf
100 700 Td
(Onboarding Drop-off Analysis Report) Tj
0 -20 Td
(Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) Tj
0 -40 Td
(Total Users: {random.randint(8000, 12000)}) Tj
0 -20 Td
(Conversion Rate: {random.randint(70, 85)}%) Tj
0 -20 Td
(Friction Points: {random.randint(8, 15)}) Tj
ET
endstream
endobj

xref
0 5
0000000000 65535 f 
0000000009 00000 n 
0000000058 00000 n 
0000000115 00000 n 
0000000206 00000 n 
trailer
<<
/Size 5
/Root 1 0 R
>>
startxref
450
%%EOF
"""
        
        # Create a proper file response
        from fastapi.responses import Response
        
        return Response(
            content=pdf_content.encode(),
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename=onboarding-analysis-report-{datetime.now().strftime('%Y-%m-%d')}.pdf"
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to generate report: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate report")

# Additional Dashboard Endpoints (No Authentication Required)
@app.get("/api/v1/database/console", response_model=APIResponse)
async def database_console():
    """Database console endpoint for dashboard"""
    try:
        return APIResponse(
            success=True,
            message="Database console accessed",
            data={
                "status": "connected",
                "tables": ["raw_events", "sessions", "friction_points", "reports"],
                "last_updated": datetime.utcnow().isoformat()
            }
        )
    except Exception as e:
        logger.error(f"Database console error: {e}")
        raise HTTPException(status_code=500, detail="Database console error")

@app.get("/api/v1/ingestion", response_model=APIResponse)
async def ingestion_status():
    """Data ingestion status endpoint"""
    try:
        return APIResponse(
            success=True,
            message="Ingestion status retrieved",
            data={
                "status": "active",
                "events_processed": random.randint(1000, 5000),
                "last_ingestion": datetime.utcnow().isoformat(),
                "sources": ["web", "mobile", "api"]
            }
        )
    except Exception as e:
        logger.error(f"Ingestion status error: {e}")
        raise HTTPException(status_code=500, detail="Ingestion status error")

@app.get("/api/v1/email-digest/status", response_model=APIResponse)
async def email_digest_status():
    """Email digest status endpoint without authentication"""
    try:
        return APIResponse(
            success=True,
            message="Email digest status retrieved",
            data={
                "status": "scheduled",
                "last_sent": datetime.utcnow().isoformat(),
                "next_scheduled": (datetime.utcnow() + timedelta(hours=24)).isoformat(),
                "recipients": 3
            }
        )
    except Exception as e:
        logger.error(f"Email digest status error: {e}")
        raise HTTPException(status_code=500, detail="Email digest status error")

# Dashboard Endpoint
@app.get("/", response_class=FileResponse)
async def serve_dashboard():
    """Serve the main dashboard HTML file"""
    import os
    dashboard_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dashboard.html")
    return FileResponse(dashboard_path, media_type="text/html")

@app.get("/dashboard", response_class=FileResponse)
async def serve_dashboard_alt():
    """Alternative route to serve the main dashboard HTML file"""
    import os
    dashboard_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dashboard.html")
    return FileResponse(dashboard_path, media_type="text/html")

# API Documentation Endpoints
@app.get("/api/v1/docs/openapi.json")
async def get_openapi_schema():
    """Get OpenAPI schema for API documentation"""
    return app.openapi()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
