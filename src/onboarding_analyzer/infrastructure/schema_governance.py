"""Schema evolution governance with automated diff approval and shadow validation.

Provides governance controls for event schema changes, diff analysis, and validation workflows.
"""
from __future__ import annotations
import json
import logging
from typing import Dict, List, Any, Optional, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from prometheus_client import Counter, Histogram, Gauge
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.models.tables import EventSchemaVersion, DataQualityAlert

logger = logging.getLogger(__name__)

class SchemaChangeType(Enum):
    FIELD_ADDED = "field_added"
    FIELD_REMOVED = "field_removed"  
    FIELD_TYPE_CHANGED = "field_type_changed"
    FIELD_RENAMED = "field_renamed"
    CONSTRAINT_ADDED = "constraint_added"
    CONSTRAINT_REMOVED = "constraint_removed"
    ENUM_VALUE_ADDED = "enum_value_added"
    ENUM_VALUE_REMOVED = "enum_value_removed"

class SchemaChangeRisk(Enum):
    LOW = "low"           # Additive changes
    MEDIUM = "medium"     # Constraint changes  
    HIGH = "high"         # Breaking changes
    CRITICAL = "critical" # Data loss risk

@dataclass
class SchemaChange:
    change_type: SchemaChangeType
    field_path: str
    old_value: Any
    new_value: Any
    risk_level: SchemaChangeRisk
    description: str

@dataclass
class SchemaDiff:
    event_name: str
    old_version: int
    new_version: int
    changes: List[SchemaChange]
    overall_risk: SchemaChangeRisk
    requires_approval: bool
    breaking_changes: bool

# Metrics
SCHEMA_CHANGES_TOTAL = Counter('schema_changes_total', 'Total schema changes', ['event_name', 'change_type', 'risk'])
SCHEMA_APPROVALS_TOTAL = Counter('schema_approvals_total', 'Schema change approvals', ['event_name', 'status'])
SCHEMA_VALIDATION_DURATION = Histogram('schema_validation_duration_seconds', 'Schema validation time', ['event_name'])
SCHEMA_VALIDATION_ERRORS = Counter('schema_validation_errors_total', 'Schema validation errors', ['event_name', 'error_type'])

class SchemaGovernance:
    """Manages schema evolution governance and approval workflows."""
    
    def __init__(self):
        self._approval_rules = {
            SchemaChangeRisk.LOW: {'auto_approve': True, 'reviewers_required': 0},
            SchemaChangeRisk.MEDIUM: {'auto_approve': False, 'reviewers_required': 1},
            SchemaChangeRisk.HIGH: {'auto_approve': False, 'reviewers_required': 2},
            SchemaChangeRisk.CRITICAL: {'auto_approve': False, 'reviewers_required': 3}
        }
        
    def analyze_schema_diff(self, event_name: str, old_schema: Dict, new_schema: Dict) -> SchemaDiff:
        """Analyze differences between schema versions."""
        changes = []
        
        # Compare fields
        old_fields = old_schema.get('properties', {})
        new_fields = new_schema.get('properties', {})
        
        # Field additions
        for field, spec in new_fields.items():
            if field not in old_fields:
                risk = SchemaChangeRisk.LOW  # Additive changes are low risk
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.FIELD_ADDED,
                    field_path=field,
                    old_value=None,
                    new_value=spec,
                    risk_level=risk,
                    description=f"Added field '{field}' with type {spec.get('type', 'unknown')}"
                ))
        
        # Field removals  
        for field, spec in old_fields.items():
            if field not in new_fields:
                risk = SchemaChangeRisk.HIGH  # Removals are high risk
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.FIELD_REMOVED,
                    field_path=field,
                    old_value=spec,
                    new_value=None,
                    risk_level=risk,
                    description=f"Removed field '{field}'"
                ))
        
        # Field type changes
        for field in old_fields.keys() & new_fields.keys():
            old_type = old_fields[field].get('type')
            new_type = new_fields[field].get('type')
            
            if old_type != new_type:
                # Determine risk based on type compatibility
                risk = self._assess_type_change_risk(old_type, new_type)
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.FIELD_TYPE_CHANGED,
                    field_path=field,
                    old_value=old_type,
                    new_value=new_type,
                    risk_level=risk,
                    description=f"Changed field '{field}' type from {old_type} to {new_type}"
                ))
        
        # Compare required fields
        old_required = set(old_schema.get('required', []))
        new_required = set(new_schema.get('required', []))
        
        # New required fields
        for field in new_required - old_required:
            risk = SchemaChangeRisk.HIGH  # New required fields are high risk
            changes.append(SchemaChange(
                change_type=SchemaChangeType.CONSTRAINT_ADDED,
                field_path=field,
                old_value=False,
                new_value=True,
                risk_level=risk,
                description=f"Made field '{field}' required"
            ))
        
        # Removed required fields
        for field in old_required - new_required:
            risk = SchemaChangeRisk.MEDIUM  # Relaxing constraints is medium risk
            changes.append(SchemaChange(
                change_type=SchemaChangeType.CONSTRAINT_REMOVED,
                field_path=field,
                old_value=True,
                new_value=False,
                risk_level=risk,
                description=f"Made field '{field}' optional"
            ))
        
        # Determine overall risk and approval requirements
        overall_risk = self._calculate_overall_risk(changes)
        breaking_changes = any(c.risk_level in [SchemaChangeRisk.HIGH, SchemaChangeRisk.CRITICAL] for c in changes)
        requires_approval = not self._approval_rules[overall_risk]['auto_approve']
        
        # Record metrics
        for change in changes:
            SCHEMA_CHANGES_TOTAL.labels(
                event_name=event_name,
                change_type=change.change_type.value,
                risk=change.risk_level.value
            ).inc()
        
        return SchemaDiff(
            event_name=event_name,
            old_version=old_schema.get('version', 1),
            new_version=new_schema.get('version', 2),
            changes=changes,
            overall_risk=overall_risk,
            requires_approval=requires_approval,
            breaking_changes=breaking_changes
        )
    
    def _assess_type_change_risk(self, old_type: str, new_type: str) -> SchemaChangeRisk:
        """Assess risk level of type changes."""
        # Compatible type changes (low risk)
        compatible_changes = {
            ('integer', 'number'),
            ('number', 'string'),  # Can always stringify
            ('boolean', 'string'),
        }
        
        # Lossy changes (high risk)
        lossy_changes = {
            ('string', 'integer'),
            ('string', 'number'),
            ('string', 'boolean'),
            ('number', 'integer'),
            ('array', 'object'),
            ('object', 'array'),
        }
        
        change_pair = (old_type, new_type)
        
        if change_pair in compatible_changes:
            return SchemaChangeRisk.LOW
        elif change_pair in lossy_changes:
            return SchemaChangeRisk.HIGH
        else:
            return SchemaChangeRisk.MEDIUM
    
    def _calculate_overall_risk(self, changes: List[SchemaChange]) -> SchemaChangeRisk:
        """Calculate overall risk from individual changes."""
        if not changes:
            return SchemaChangeRisk.LOW
        
        risk_levels = [c.risk_level for c in changes]
        
        if SchemaChangeRisk.CRITICAL in risk_levels:
            return SchemaChangeRisk.CRITICAL
        elif SchemaChangeRisk.HIGH in risk_levels:
            return SchemaChangeRisk.HIGH
        elif SchemaChangeRisk.MEDIUM in risk_levels:
            return SchemaChangeRisk.MEDIUM
        else:
            return SchemaChangeRisk.LOW
    
    def create_schema_version(self, event_name: str, schema: Dict, author: str = None) -> int:
        """Create a new schema version."""
        session = SessionLocal()
        try:
            # Get latest version
            latest = session.query(EventSchemaVersion).filter_by(
                event_name=event_name
            ).order_by(EventSchemaVersion.version.desc()).first()
            
            new_version = (latest.version + 1) if latest else 1
            
            schema_version = EventSchemaVersion(
                event_name=event_name,
                version=new_version,
                spec=schema,
                status='draft',  # Start as draft
                author=author or 'system'
            )
            
            session.add(schema_version)
            session.commit()
            
            logger.info(f"Created schema version {new_version} for {event_name}")
            return new_version
            
        finally:
            session.close()
    
    def approve_schema_version(self, event_name: str, version: int, approver: str) -> bool:
        """Approve a schema version for production use."""
        session = SessionLocal()
        try:
            schema_version = session.query(EventSchemaVersion).filter_by(
                event_name=event_name,
                version=version
            ).first()
            
            if not schema_version:
                logger.error(f"Schema version {version} not found for {event_name}")
                return False
            
            if schema_version.status != 'draft':
                logger.warning(f"Schema version {version} for {event_name} is not in draft status")
                return False
            
            schema_version.status = 'approved'
            session.commit()
            
            SCHEMA_APPROVALS_TOTAL.labels(event_name=event_name, status='approved').inc()
            logger.info(f"Approved schema version {version} for {event_name} by {approver}")
            
            return True
            
        finally:
            session.close()

class ShadowValidator:
    """Validates events against multiple schema versions in shadow mode."""
    
    def __init__(self):
        self._validation_cache = {}
        
    def validate_event_shadow(self, event_name: str, event_data: Dict) -> Dict[str, bool]:
        """Validate event against current and draft schemas."""
        session = SessionLocal()
        try:
            # Get current approved schema
            current_schema = session.query(EventSchemaVersion).filter_by(
                event_name=event_name,
                status='approved'
            ).order_by(EventSchemaVersion.version.desc()).first()
            
            # Get latest draft schema
            draft_schema = session.query(EventSchemaVersion).filter_by(
                event_name=event_name,
                status='draft'
            ).order_by(EventSchemaVersion.version.desc()).first()
            
            results = {}
            
            # Validate against current schema
            if current_schema:
                results['current'] = self._validate_against_schema(
                    event_data, current_schema.spec, f"{event_name}_v{current_schema.version}"
                )
            
            # Validate against draft schema (shadow validation)
            if draft_schema:
                results['draft'] = self._validate_against_schema(
                    event_data, draft_schema.spec, f"{event_name}_v{draft_schema.version}"
                )
                
                # Log validation discrepancies
                if 'current' in results and results['current'] != results['draft']:
                    if results['current'] and not results['draft']:
                        logger.warning(f"Event {event_name} passes current schema but fails draft schema")
                        SCHEMA_VALIDATION_ERRORS.labels(
                            event_name=event_name,
                            error_type='draft_failure'
                        ).inc()
                    elif not results['current'] and results['draft']:
                        logger.info(f"Event {event_name} fails current schema but passes draft schema")
                        SCHEMA_VALIDATION_ERRORS.labels(
                            event_name=event_name,
                            error_type='current_failure'
                        ).inc()
            
            return results
            
        finally:
            session.close()
    
    def _validate_against_schema(self, event_data: Dict, schema: Dict, schema_id: str) -> bool:
        """Validate event data against a specific schema."""
        import time
        start_time = time.time()
        
        try:
            # Basic validation - check required fields
            required_fields = schema.get('required', [])
            for field in required_fields:
                if field not in event_data:
                    return False
            
            # Type validation
            properties = schema.get('properties', {})
            for field, value in event_data.items():
                if field in properties:
                    expected_type = properties[field].get('type')
                    if not self._validate_field_type(value, expected_type):
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"Schema validation error for {schema_id}: {e}")
            return False
        finally:
            duration = time.time() - start_time
            SCHEMA_VALIDATION_DURATION.labels(event_name=schema_id).observe(duration)
    
    def _validate_field_type(self, value: Any, expected_type: str) -> bool:
        """Validate field type."""
        if expected_type == 'string':
            return isinstance(value, str)
        elif expected_type == 'integer':
            return isinstance(value, int)
        elif expected_type == 'number':
            return isinstance(value, (int, float))
        elif expected_type == 'boolean':
            return isinstance(value, bool)
        elif expected_type == 'array':
            return isinstance(value, list)
        elif expected_type == 'object':
            return isinstance(value, dict)
        else:
            return True  # Unknown type, allow it

# Global instances
schema_governance = SchemaGovernance()
shadow_validator = ShadowValidator()

def validate_schema_change(event_name: str, new_schema: Dict, author: str = None) -> SchemaDiff:
    """Convenience function to validate and create schema changes."""
    session = SessionLocal()
    try:
        # Get current schema
        current = session.query(EventSchemaVersion).filter_by(
            event_name=event_name,
            status='approved'
        ).order_by(EventSchemaVersion.version.desc()).first()
        
        old_schema = current.spec if current else {}
        
        # Analyze diff
        diff = schema_governance.analyze_schema_diff(event_name, old_schema, new_schema)
        
        # Create new version if changes exist
        if diff.changes:
            new_version = schema_governance.create_schema_version(event_name, new_schema, author)
            logger.info(f"Created schema version {new_version} for {event_name} with {len(diff.changes)} changes")
        
        return diff
        
    finally:
        session.close()
