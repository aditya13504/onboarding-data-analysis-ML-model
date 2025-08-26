"""
Security Audit Logging and Compliance System

Enterprise-grade security audit logging, compliance monitoring, forensic analysis,
and comprehensive audit trail management for regulatory compliance.
"""

import asyncio
import logging
import json
import hashlib
import gzip
import csv
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Set, Union, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
import queue
import threading
from collections import defaultdict

# Database for audit storage
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import Column, String, Integer, DateTime, Boolean, Text, JSON, Index

# Elasticsearch for audit search (optional)
try:
    from elasticsearch import Elasticsearch
    ELASTICSEARCH_AVAILABLE = True
except ImportError:
    ELASTICSEARCH_AVAILABLE = False

# Monitoring
from prometheus_client import Counter, Histogram, Gauge

# Import security components
from .authentication import SecurityEvent, SecurityEventType
from .encryption import EncryptionService

# Initialize metrics
audit_logs_total = Counter('audit_logs_total', 'Total audit logs', ['event_type', 'severity'])
audit_log_failures = Counter('audit_log_failures_total', 'Audit log failures', ['reason'])
audit_search_queries = Counter('audit_search_queries_total', 'Audit search queries', ['query_type'])
audit_storage_size = Gauge('audit_storage_size_bytes', 'Audit storage size in bytes')
audit_retention_cleanups = Counter('audit_retention_cleanups_total', 'Audit retention cleanups')

logger = logging.getLogger(__name__)

Base = declarative_base()

class ComplianceStandard(Enum):
    """Supported compliance standards."""
    SOX = "sox"
    GDPR = "gdpr"
    HIPAA = "hipaa"
    PCI_DSS = "pci_dss"
    ISO_27001 = "iso_27001"
    NIST_CSF = "nist_csf"
    SOC2 = "soc2"

class AuditLevel(Enum):
    """Audit logging levels."""
    MINIMAL = "minimal"
    STANDARD = "standard"
    DETAILED = "detailed"
    COMPREHENSIVE = "comprehensive"
    FORENSIC = "forensic"

class RetentionPolicy(Enum):
    """Data retention policies."""
    SHORT_TERM = "short_term"  # 30 days
    MEDIUM_TERM = "medium_term"  # 1 year
    LONG_TERM = "long_term"  # 7 years
    PERMANENT = "permanent"

@dataclass
class AuditRecord:
    """Comprehensive audit record."""
    record_id: str
    timestamp: datetime
    event_type: str
    severity: str
    user_id: Optional[str]
    username: Optional[str]
    session_id: Optional[str]
    ip_address: str
    user_agent: str
    resource: Optional[str]
    action: str
    outcome: str  # success, failure, error
    details: Dict[str, Any]
    risk_score: int = 0
    compliance_tags: List[str] = field(default_factory=list)
    retention_policy: RetentionPolicy = RetentionPolicy.MEDIUM_TERM
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ComplianceRule:
    """Compliance monitoring rule."""
    rule_id: str
    name: str
    description: str
    compliance_standard: ComplianceStandard
    event_types: List[str]
    conditions: Dict[str, Any]
    alert_threshold: int
    time_window: timedelta
    is_active: bool = True
    severity: str = "medium"

@dataclass
class AuditQuery:
    """Audit log query parameters."""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    user_ids: List[str] = field(default_factory=list)
    event_types: List[str] = field(default_factory=list)
    ip_addresses: List[str] = field(default_factory=list)
    resources: List[str] = field(default_factory=list)
    actions: List[str] = field(default_factory=list)
    outcomes: List[str] = field(default_factory=list)
    min_risk_score: int = 0
    max_risk_score: int = 100
    compliance_tags: List[str] = field(default_factory=list)
    text_search: Optional[str] = None
    limit: int = 1000
    offset: int = 0

@dataclass
class AuditReport:
    """Audit analysis report."""
    report_id: str
    title: str
    description: str
    generated_at: datetime
    time_range: Dict[str, datetime]
    total_events: int
    event_breakdown: Dict[str, int]
    risk_analysis: Dict[str, Any]
    compliance_status: Dict[str, Any]
    findings: List[Dict[str, Any]]
    recommendations: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)

# SQLAlchemy models for audit storage
class AuditLogModel(Base):
    """Audit log database model."""
    __tablename__ = 'audit_logs'
    
    id = Column(Integer, primary_key=True)
    record_id = Column(String(64), unique=True, nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    event_type = Column(String(100), nullable=False, index=True)
    severity = Column(String(20), nullable=False, index=True)
    user_id = Column(String(100), index=True)
    username = Column(String(100), index=True)
    session_id = Column(String(100), index=True)
    ip_address = Column(String(45), nullable=False, index=True)
    user_agent = Column(Text)
    resource = Column(String(500), index=True)
    action = Column(String(100), nullable=False, index=True)
    outcome = Column(String(20), nullable=False, index=True)
    details = Column(JSON)
    risk_score = Column(Integer, default=0, index=True)
    compliance_tags = Column(JSON)
    retention_policy = Column(String(20), nullable=False)
    metadata = Column(JSON)
    
    # Indexes for common queries
    __table_args__ = (
        Index('idx_audit_user_time', 'user_id', 'timestamp'),
        Index('idx_audit_event_time', 'event_type', 'timestamp'),
        Index('idx_audit_ip_time', 'ip_address', 'timestamp'),
        Index('idx_audit_risk_time', 'risk_score', 'timestamp'),
    )

class ComplianceViolationModel(Base):
    """Compliance violation tracking."""
    __tablename__ = 'compliance_violations'
    
    id = Column(Integer, primary_key=True)
    violation_id = Column(String(64), unique=True, nullable=False)
    rule_id = Column(String(100), nullable=False)
    compliance_standard = Column(String(50), nullable=False)
    detected_at = Column(DateTime(timezone=True), nullable=False)
    severity = Column(String(20), nullable=False)
    description = Column(Text, nullable=False)
    affected_records = Column(JSON)
    status = Column(String(20), default='open')  # open, investigating, resolved
    resolution_notes = Column(Text)
    resolved_at = Column(DateTime(timezone=True))

class AuditLogProcessor:
    """Processes and enriches audit logs."""
    
    def __init__(self, encryption_service: Optional[EncryptionService] = None):
        self.encryption_service = encryption_service
        self.risk_calculators: List[Callable] = []
        self.enrichers: List[Callable] = []
        
        # Initialize default processors
        self._initialize_default_processors()
    
    def _initialize_default_processors(self):
        """Initialize default audit processors."""
        self.risk_calculators.append(self._calculate_basic_risk)
        self.enrichers.append(self._enrich_geolocation)
        self.enrichers.append(self._enrich_user_context)
    
    def process_audit_record(self, record: AuditRecord) -> AuditRecord:
        """Process and enrich audit record."""
        try:
            # Calculate risk score
            for calculator in self.risk_calculators:
                record.risk_score = max(record.risk_score, calculator(record))
            
            # Apply enrichers
            for enricher in self.enrichers:
                record = enricher(record)
            
            # Add compliance tags
            record.compliance_tags = self._determine_compliance_tags(record)
            
            # Encrypt sensitive data if encryption is available
            if self.encryption_service:
                record = self._encrypt_sensitive_fields(record)
            
            return record
            
        except Exception as e:
            logger.error(f"Audit record processing failed: {e}")
            return record
    
    def _calculate_basic_risk(self, record: AuditRecord) -> int:
        """Calculate basic risk score."""
        risk_score = 0
        
        # High risk event types
        high_risk_events = [
            'login_failure', 'permission_denied', 'account_locked',
            'suspicious_activity', 'data_access', 'admin_action'
        ]
        
        if record.event_type in high_risk_events:
            risk_score += 30
        
        # Failed outcomes
        if record.outcome == 'failure':
            risk_score += 20
        
        # Admin actions
        if 'admin' in record.action.lower():
            risk_score += 15
        
        # Sensitive resources
        sensitive_resources = ['user_data', 'financial_data', 'personal_info']
        if any(resource in record.resource.lower() for resource in sensitive_resources if record.resource):
            risk_score += 25
        
        # Multiple failures from same IP
        if 'failed_attempts' in record.details:
            attempts = record.details.get('failed_attempts', 0)
            if attempts > 3:
                risk_score += min(attempts * 5, 40)
        
        return min(risk_score, 100)
    
    def _enrich_geolocation(self, record: AuditRecord) -> AuditRecord:
        """Enrich with geolocation data."""
        # This would typically integrate with a geolocation service
        # For now, adding placeholder logic
        if record.ip_address and not record.ip_address.startswith('192.168.'):
            record.metadata['geo_enriched'] = True
            record.metadata['suspected_country'] = 'Unknown'
        
        return record
    
    def _enrich_user_context(self, record: AuditRecord) -> AuditRecord:
        """Enrich with user context."""
        if record.user_id:
            # This would typically look up user information
            record.metadata['user_context_enriched'] = True
        
        return record
    
    def _determine_compliance_tags(self, record: AuditRecord) -> List[str]:
        """Determine applicable compliance tags."""
        tags = []
        
        # GDPR - data access/modification
        if any(term in record.action.lower() for term in ['read', 'update', 'delete', 'export']):
            if record.resource and 'personal' in record.resource.lower():
                tags.append('gdpr')
        
        # SOX - financial data access
        if record.resource and 'financial' in record.resource.lower():
            tags.append('sox')
        
        # HIPAA - health data
        if record.resource and 'health' in record.resource.lower():
            tags.append('hipaa')
        
        # PCI DSS - payment data
        if record.resource and any(term in record.resource.lower() for term in ['payment', 'card', 'transaction']):
            tags.append('pci_dss')
        
        # SOC2 - system access
        if record.event_type in ['login_success', 'login_failure', 'system_access']:
            tags.append('soc2')
        
        return tags
    
    def _encrypt_sensitive_fields(self, record: AuditRecord) -> AuditRecord:
        """Encrypt sensitive fields in audit record."""
        # This would encrypt sensitive data in the details field
        # For now, just marking as encrypted
        if 'sensitive_data' in record.details:
            record.metadata['encryption_applied'] = True
        
        return record

class AuditStorage:
    """Audit log storage backend."""
    
    def __init__(self, database_url: str, elasticsearch_url: Optional[str] = None):
        self.database_url = database_url
        self.engine = sa.create_engine(database_url)
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        # Create tables
        Base.metadata.create_all(bind=self.engine)
        
        # Initialize Elasticsearch if available
        self.elasticsearch = None
        if elasticsearch_url and ELASTICSEARCH_AVAILABLE:
            try:
                self.elasticsearch = Elasticsearch([elasticsearch_url])
                logger.info("Elasticsearch connected for audit search")
            except Exception as e:
                logger.warning(f"Elasticsearch connection failed: {e}")
    
    async def store_audit_record(self, record: AuditRecord) -> bool:
        """Store audit record in database."""
        try:
            session = self.SessionLocal()
            
            audit_log = AuditLogModel(
                record_id=record.record_id,
                timestamp=record.timestamp,
                event_type=record.event_type,
                severity=record.severity,
                user_id=record.user_id,
                username=record.username,
                session_id=record.session_id,
                ip_address=record.ip_address,
                user_agent=record.user_agent,
                resource=record.resource,
                action=record.action,
                outcome=record.outcome,
                details=record.details,
                risk_score=record.risk_score,
                compliance_tags=record.compliance_tags,
                retention_policy=record.retention_policy.value,
                metadata=record.metadata
            )
            
            session.add(audit_log)
            session.commit()
            
            # Store in Elasticsearch for search
            if self.elasticsearch:
                await self._store_in_elasticsearch(record)
            
            audit_logs_total.labels(
                event_type=record.event_type,
                severity=record.severity
            ).inc()
            
            return True
            
        except Exception as e:
            logger.error(f"Audit record storage failed: {e}")
            audit_log_failures.labels(reason='storage_error').inc()
            return False
        finally:
            session.close()
    
    async def _store_in_elasticsearch(self, record: AuditRecord):
        """Store audit record in Elasticsearch."""
        try:
            doc = asdict(record)
            doc['timestamp'] = record.timestamp.isoformat()
            
            await self.elasticsearch.index(
                index=f"audit-logs-{record.timestamp.strftime('%Y-%m')}",
                body=doc
            )
        except Exception as e:
            logger.error(f"Elasticsearch storage failed: {e}")
    
    async def query_audit_logs(self, query: AuditQuery) -> List[AuditRecord]:
        """Query audit logs."""
        try:
            session = self.SessionLocal()
            
            # Build SQL query
            sql_query = session.query(AuditLogModel)
            
            if query.start_time:
                sql_query = sql_query.filter(AuditLogModel.timestamp >= query.start_time)
            
            if query.end_time:
                sql_query = sql_query.filter(AuditLogModel.timestamp <= query.end_time)
            
            if query.user_ids:
                sql_query = sql_query.filter(AuditLogModel.user_id.in_(query.user_ids))
            
            if query.event_types:
                sql_query = sql_query.filter(AuditLogModel.event_type.in_(query.event_types))
            
            if query.ip_addresses:
                sql_query = sql_query.filter(AuditLogModel.ip_address.in_(query.ip_addresses))
            
            if query.resources:
                sql_query = sql_query.filter(AuditLogModel.resource.in_(query.resources))
            
            if query.actions:
                sql_query = sql_query.filter(AuditLogModel.action.in_(query.actions))
            
            if query.outcomes:
                sql_query = sql_query.filter(AuditLogModel.outcome.in_(query.outcomes))
            
            if query.min_risk_score > 0:
                sql_query = sql_query.filter(AuditLogModel.risk_score >= query.min_risk_score)
            
            if query.max_risk_score < 100:
                sql_query = sql_query.filter(AuditLogModel.risk_score <= query.max_risk_score)
            
            # Order by timestamp
            sql_query = sql_query.order_by(AuditLogModel.timestamp.desc())
            
            # Apply limit and offset
            sql_query = sql_query.offset(query.offset).limit(query.limit)
            
            results = sql_query.all()
            
            # Convert to AuditRecord objects
            audit_records = []
            for result in results:
                record = AuditRecord(
                    record_id=result.record_id,
                    timestamp=result.timestamp,
                    event_type=result.event_type,
                    severity=result.severity,
                    user_id=result.user_id,
                    username=result.username,
                    session_id=result.session_id,
                    ip_address=result.ip_address,
                    user_agent=result.user_agent,
                    resource=result.resource,
                    action=result.action,
                    outcome=result.outcome,
                    details=result.details or {},
                    risk_score=result.risk_score,
                    compliance_tags=result.compliance_tags or [],
                    retention_policy=RetentionPolicy(result.retention_policy),
                    metadata=result.metadata or {}
                )
                audit_records.append(record)
            
            audit_search_queries.labels(query_type='sql').inc()
            
            return audit_records
            
        except Exception as e:
            logger.error(f"Audit query failed: {e}")
            return []
        finally:
            session.close()
    
    async def cleanup_expired_records(self):
        """Clean up expired audit records based on retention policy."""
        try:
            session = self.SessionLocal()
            current_time = datetime.now(timezone.utc)
            cleanup_count = 0
            
            # Define retention periods
            retention_periods = {
                RetentionPolicy.SHORT_TERM.value: timedelta(days=30),
                RetentionPolicy.MEDIUM_TERM.value: timedelta(days=365),
                RetentionPolicy.LONG_TERM.value: timedelta(days=2555),  # 7 years
                # PERMANENT records are never deleted
            }
            
            for policy, period in retention_periods.items():
                cutoff_time = current_time - period
                
                deleted = session.query(AuditLogModel).filter(
                    AuditLogModel.retention_policy == policy,
                    AuditLogModel.timestamp < cutoff_time
                ).delete()
                
                cleanup_count += deleted
            
            session.commit()
            
            if cleanup_count > 0:
                audit_retention_cleanups.inc()
                logger.info(f"Cleaned up {cleanup_count} expired audit records")
            
        except Exception as e:
            logger.error(f"Audit cleanup failed: {e}")
        finally:
            session.close()

class ComplianceMonitor:
    """Compliance monitoring and violation detection."""
    
    def __init__(self, audit_storage: AuditStorage):
        self.audit_storage = audit_storage
        self.compliance_rules: Dict[str, ComplianceRule] = {}
        self.violation_counters: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        # Initialize default compliance rules
        self._initialize_default_rules()
    
    def _initialize_default_rules(self):
        """Initialize default compliance monitoring rules."""
        # GDPR data access monitoring
        gdpr_rule = ComplianceRule(
            rule_id="gdpr_data_access",
            name="GDPR Data Access Monitoring",
            description="Monitor access to personal data for GDPR compliance",
            compliance_standard=ComplianceStandard.GDPR,
            event_types=["data_access", "data_export"],
            conditions={"resource_contains": "personal"},
            alert_threshold=10,
            time_window=timedelta(hours=1)
        )
        self.compliance_rules[gdpr_rule.rule_id] = gdpr_rule
        
        # SOX financial data access
        sox_rule = ComplianceRule(
            rule_id="sox_financial_access",
            name="SOX Financial Data Access",
            description="Monitor access to financial data for SOX compliance",
            compliance_standard=ComplianceStandard.SOX,
            event_types=["data_access", "data_modification"],
            conditions={"resource_contains": "financial"},
            alert_threshold=5,
            time_window=timedelta(hours=1)
        )
        self.compliance_rules[sox_rule.rule_id] = sox_rule
        
        # Failed login attempts
        security_rule = ComplianceRule(
            rule_id="failed_login_monitoring",
            name="Failed Login Monitoring",
            description="Monitor failed login attempts",
            compliance_standard=ComplianceStandard.SOC2,
            event_types=["login_failure"],
            conditions={},
            alert_threshold=5,
            time_window=timedelta(minutes=15)
        )
        self.compliance_rules[security_rule.rule_id] = security_rule
    
    async def check_compliance(self, record: AuditRecord) -> List[str]:
        """Check record against compliance rules."""
        violations = []
        
        for rule in self.compliance_rules.values():
            if not rule.is_active:
                continue
            
            if record.event_type not in rule.event_types:
                continue
            
            # Check conditions
            if not self._check_rule_conditions(rule, record):
                continue
            
            # Update counters
            key = f"{rule.rule_id}:{record.ip_address}"
            self.violation_counters[key][record.timestamp.strftime('%Y-%m-%d %H')] += 1
            
            # Check threshold
            current_hour = record.timestamp.strftime('%Y-%m-%d %H')
            count = self.violation_counters[key][current_hour]
            
            if count >= rule.alert_threshold:
                violation_id = f"{rule.rule_id}_{record.timestamp.strftime('%Y%m%d_%H%M%S')}_{record.ip_address}"
                violations.append(violation_id)
                
                # Log compliance violation
                await self._log_compliance_violation(rule, record, violation_id, count)
        
        return violations
    
    def _check_rule_conditions(self, rule: ComplianceRule, record: AuditRecord) -> bool:
        """Check if record matches rule conditions."""
        for condition, value in rule.conditions.items():
            if condition == "resource_contains":
                if not record.resource or value.lower() not in record.resource.lower():
                    return False
            elif condition == "user_id":
                if record.user_id != value:
                    return False
            elif condition == "action":
                if record.action != value:
                    return False
            # Add more condition types as needed
        
        return True
    
    async def _log_compliance_violation(self, rule: ComplianceRule, record: AuditRecord,
                                      violation_id: str, count: int):
        """Log compliance violation."""
        try:
            session = self.audit_storage.SessionLocal()
            
            violation = ComplianceViolationModel(
                violation_id=violation_id,
                rule_id=rule.rule_id,
                compliance_standard=rule.compliance_standard.value,
                detected_at=record.timestamp,
                severity=rule.severity,
                description=f"{rule.name}: {count} events in {rule.time_window}",
                affected_records=[record.record_id],
                status='open'
            )
            
            session.add(violation)
            session.commit()
            
            logger.warning(f"Compliance violation detected: {violation_id}")
            
        except Exception as e:
            logger.error(f"Failed to log compliance violation: {e}")
        finally:
            session.close()

class AuditReportGenerator:
    """Generate audit analysis reports."""
    
    def __init__(self, audit_storage: AuditStorage):
        self.audit_storage = audit_storage
    
    async def generate_security_report(self, start_time: datetime, end_time: datetime) -> AuditReport:
        """Generate security analysis report."""
        query = AuditQuery(
            start_time=start_time,
            end_time=end_time,
            limit=10000
        )
        
        records = await self.audit_storage.query_audit_logs(query)
        
        # Analyze events
        event_breakdown = defaultdict(int)
        risk_distribution = defaultdict(int)
        ip_analysis = defaultdict(int)
        user_analysis = defaultdict(int)
        
        high_risk_events = []
        
        for record in records:
            event_breakdown[record.event_type] += 1
            
            # Risk distribution
            if record.risk_score >= 80:
                risk_distribution['critical'] += 1
                high_risk_events.append(record)
            elif record.risk_score >= 60:
                risk_distribution['high'] += 1
            elif record.risk_score >= 40:
                risk_distribution['medium'] += 1
            else:
                risk_distribution['low'] += 1
            
            # IP analysis
            ip_analysis[record.ip_address] += 1
            
            # User analysis
            if record.user_id:
                user_analysis[record.user_id] += 1
        
        # Generate findings
        findings = []
        
        # High risk events
        if high_risk_events:
            findings.append({
                'type': 'high_risk_events',
                'count': len(high_risk_events),
                'description': f'Found {len(high_risk_events)} high-risk security events',
                'severity': 'high'
            })
        
        # Suspicious IP activity
        suspicious_ips = [ip for ip, count in ip_analysis.items() if count > 100]
        if suspicious_ips:
            findings.append({
                'type': 'suspicious_ip_activity',
                'count': len(suspicious_ips),
                'description': f'Found {len(suspicious_ips)} IPs with high activity',
                'details': suspicious_ips[:10],  # Top 10
                'severity': 'medium'
            })
        
        # Generate recommendations
        recommendations = []
        if high_risk_events:
            recommendations.append("Review and investigate high-risk security events")
        if suspicious_ips:
            recommendations.append("Monitor or restrict access from high-activity IP addresses")
        
        report = AuditReport(
            report_id=f"security_report_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
            title="Security Analysis Report",
            description=f"Security analysis for period {start_time} to {end_time}",
            generated_at=datetime.now(timezone.utc),
            time_range={'start': start_time, 'end': end_time},
            total_events=len(records),
            event_breakdown=dict(event_breakdown),
            risk_analysis={
                'distribution': dict(risk_distribution),
                'high_risk_events': len(high_risk_events)
            },
            compliance_status={'status': 'analyzed'},
            findings=findings,
            recommendations=recommendations
        )
        
        return report

class SecurityAuditSystem:
    """Main security audit system."""
    
    def __init__(self, database_url: str, elasticsearch_url: Optional[str] = None,
                 encryption_service: Optional[EncryptionService] = None):
        self.processor = AuditLogProcessor(encryption_service)
        self.storage = AuditStorage(database_url, elasticsearch_url)
        self.compliance_monitor = ComplianceMonitor(self.storage)
        self.report_generator = AuditReportGenerator(self.storage)
        
        # Audit queue for async processing
        self.audit_queue = queue.Queue(maxsize=10000)
        self.processing_thread = None
        self.is_running = False
        
        # Start processing
        self._start_processing()
    
    def _start_processing(self):
        """Start audit log processing."""
        self.is_running = True
        self.processing_thread = threading.Thread(target=self._process_audit_queue)
        self.processing_thread.daemon = True
        self.processing_thread.start()
        
        # Start cleanup task
        asyncio.create_task(self._cleanup_loop())
    
    def _process_audit_queue(self):
        """Process audit logs from queue."""
        while self.is_running:
            try:
                record = self.audit_queue.get(timeout=1)
                
                # Process record
                processed_record = self.processor.process_audit_record(record)
                
                # Store record
                asyncio.run(self.storage.store_audit_record(processed_record))
                
                # Check compliance
                asyncio.run(self.compliance_monitor.check_compliance(processed_record))
                
                self.audit_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Audit processing error: {e}")
    
    async def _cleanup_loop(self):
        """Periodic cleanup of expired records."""
        while self.is_running:
            try:
                await self.storage.cleanup_expired_records()
                await asyncio.sleep(3600)  # Clean up every hour
            except Exception as e:
                logger.error(f"Audit cleanup error: {e}")
                await asyncio.sleep(300)
    
    def log_audit_event(self, event_type: str, user_id: Optional[str], username: Optional[str],
                       session_id: Optional[str], ip_address: str, user_agent: str,
                       resource: Optional[str], action: str, outcome: str,
                       details: Dict[str, Any], severity: str = "info") -> bool:
        """Log audit event."""
        try:
            record = AuditRecord(
                record_id=f"audit_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{secrets.token_urlsafe(8)}",
                timestamp=datetime.now(timezone.utc),
                event_type=event_type,
                severity=severity,
                user_id=user_id,
                username=username,
                session_id=session_id,
                ip_address=ip_address,
                user_agent=user_agent,
                resource=resource,
                action=action,
                outcome=outcome,
                details=details
            )
            
            # Add to queue for processing
            self.audit_queue.put_nowait(record)
            return True
            
        except queue.Full:
            logger.error("Audit queue is full, dropping event")
            audit_log_failures.labels(reason='queue_full').inc()
            return False
        except Exception as e:
            logger.error(f"Audit logging failed: {e}")
            audit_log_failures.labels(reason='logging_error').inc()
            return False
    
    async def search_audit_logs(self, query: AuditQuery) -> List[AuditRecord]:
        """Search audit logs."""
        return await self.storage.query_audit_logs(query)
    
    async def generate_report(self, report_type: str, start_time: datetime, 
                            end_time: datetime) -> AuditReport:
        """Generate audit report."""
        if report_type == "security":
            return await self.report_generator.generate_security_report(start_time, end_time)
        else:
            raise ValueError(f"Unknown report type: {report_type}")
    
    def stop(self):
        """Stop audit system."""
        self.is_running = False
        if self.processing_thread:
            self.processing_thread.join(timeout=5)

# Global instance
_audit_system: Optional[SecurityAuditSystem] = None

def initialize_audit_system(database_url: str, elasticsearch_url: Optional[str] = None,
                           encryption_service: Optional[EncryptionService] = None) -> SecurityAuditSystem:
    """Initialize security audit system."""
    global _audit_system
    
    _audit_system = SecurityAuditSystem(database_url, elasticsearch_url, encryption_service)
    
    logger.info("Security audit system initialized")
    return _audit_system

def get_audit_system() -> SecurityAuditSystem:
    """Get audit system instance."""
    if _audit_system is None:
        raise RuntimeError("Audit system not initialized")
    return _audit_system
