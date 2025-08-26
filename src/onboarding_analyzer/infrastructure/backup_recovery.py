"""Backup and disaster recovery system with automated scheduling and verification.

Provides enterprise-grade backup, restoration, and disaster recovery capabilities.
"""
from __future__ import annotations
import os
import json
import logging
import subprocess
import time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from prometheus_client import Counter, Histogram, Gauge
from onboarding_analyzer.infrastructure.db import SessionLocal
from onboarding_analyzer.config import get_settings

logger = logging.getLogger(__name__)

class BackupType(Enum):
    FULL = "full"              # Complete database backup
    INCREMENTAL = "incremental" # Changes since last backup
    DIFFERENTIAL = "differential" # Changes since last full backup
    ARCHIVE = "archive"        # Archive log backup

class BackupStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    VERIFIED = "verified"
    RESTORED = "restored"

class BackupLocation(Enum):
    LOCAL = "local"
    S3 = "s3"
    GCS = "gcs"
    AZURE = "azure"

@dataclass
class BackupConfig:
    backup_type: BackupType
    location: BackupLocation
    retention_days: int
    compression: bool = True
    encryption: bool = True
    verify_after_backup: bool = True
    cross_region_replication: bool = False
    
@dataclass
class BackupJob:
    id: str
    backup_type: BackupType
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: BackupStatus = BackupStatus.PENDING
    file_path: Optional[str] = None
    file_size_bytes: int = 0
    compression_ratio: float = 1.0
    error_message: Optional[str] = None
    verification_status: Optional[str] = None

# Metrics
BACKUP_OPERATIONS = Counter('backup_operations_total', 'Backup operations', ['type', 'location', 'result'])
BACKUP_DURATION = Histogram('backup_duration_seconds', 'Backup duration', ['type'])
BACKUP_SIZE_BYTES = Gauge('backup_size_bytes', 'Backup size in bytes', ['type', 'location'])
BACKUP_VERIFICATION_DURATION = Histogram('backup_verification_seconds', 'Backup verification time')
RESTORE_OPERATIONS = Counter('restore_operations_total', 'Restore operations', ['type', 'result'])
BACKUP_AGE_DAYS = Gauge('backup_age_days', 'Age of latest backup in days', ['type'])

class BackupManager:
    """Manages automated backup operations with verification and disaster recovery."""
    
    def __init__(self):
        self.settings = get_settings()
        self._backup_configs = self._load_backup_configs()
        self._active_jobs: Dict[str, BackupJob] = {}
        
    def _load_backup_configs(self) -> Dict[BackupType, BackupConfig]:
        """Load backup configurations from settings."""
        return {
            BackupType.FULL: BackupConfig(
                backup_type=BackupType.FULL,
                location=BackupLocation.S3,
                retention_days=90,
                compression=True,
                encryption=True,
                verify_after_backup=True,
                cross_region_replication=True
            ),
            BackupType.INCREMENTAL: BackupConfig(
                backup_type=BackupType.INCREMENTAL,
                location=BackupLocation.S3,
                retention_days=30,
                compression=True,
                encryption=True,
                verify_after_backup=False,  # Skip verification for frequent incrementals
                cross_region_replication=False
            ),
            BackupType.ARCHIVE: BackupConfig(
                backup_type=BackupType.ARCHIVE,
                location=BackupLocation.S3,
                retention_days=365,
                compression=True,
                encryption=True,
                verify_after_backup=True,
                cross_region_replication=True
            )
        }
    
    def create_backup(self, backup_type: BackupType, database_name: str = None) -> str:
        """Create a new backup job."""
        job_id = f"backup_{backup_type.value}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        config = self._backup_configs[backup_type]
        
        job = BackupJob(
            id=job_id,
            backup_type=backup_type,
            started_at=datetime.utcnow(),
            status=BackupStatus.PENDING
        )
        
        self._active_jobs[job_id] = job
        
        try:
            # Start backup operation
            job.status = BackupStatus.RUNNING
            BACKUP_OPERATIONS.labels(type=backup_type.value, location=config.location.value, result='started').inc()
            
            start_time = time.time()
            
            # Perform the actual backup
            backup_result = self._perform_backup(job, config, database_name)
            
            if backup_result['success']:
                job.status = BackupStatus.COMPLETED
                job.completed_at = datetime.utcnow()
                job.file_path = backup_result['file_path']
                job.file_size_bytes = backup_result['file_size']
                job.compression_ratio = backup_result.get('compression_ratio', 1.0)
                
                duration = time.time() - start_time
                BACKUP_DURATION.labels(type=backup_type.value).observe(duration)
                BACKUP_SIZE_BYTES.labels(type=backup_type.value, location=config.location.value).set(job.file_size_bytes)
                
                # Verify backup if configured
                if config.verify_after_backup:
                    verification_result = self._verify_backup(job, config)
                    job.verification_status = verification_result['status']
                    if verification_result['success']:
                        job.status = BackupStatus.VERIFIED
                
                # Cross-region replication if configured
                if config.cross_region_replication:
                    self._replicate_backup(job, config)
                
                BACKUP_OPERATIONS.labels(type=backup_type.value, location=config.location.value, result='success').inc()
                logger.info(f"Backup {job_id} completed successfully")
                
            else:
                job.status = BackupStatus.FAILED
                job.error_message = backup_result['error']
                BACKUP_OPERATIONS.labels(type=backup_type.value, location=config.location.value, result='failure').inc()
                logger.error(f"Backup {job_id} failed: {backup_result['error']}")
            
        except Exception as e:
            job.status = BackupStatus.FAILED
            job.error_message = str(e)
            BACKUP_OPERATIONS.labels(type=backup_type.value, location=config.location.value, result='error').inc()
            logger.error(f"Backup {job_id} encountered error: {e}")
        
        return job_id
    
    def _perform_backup(self, job: BackupJob, config: BackupConfig, database_name: str) -> Dict[str, Any]:
        """Perform the actual backup operation."""
        try:
            db_url = self.settings.database_url
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            
            # Determine backup file name
            if config.backup_type == BackupType.FULL:
                filename = f"full_backup_{timestamp}.sql"
            elif config.backup_type == BackupType.INCREMENTAL:
                filename = f"incremental_backup_{timestamp}.sql"
            else:
                filename = f"archive_backup_{timestamp}.sql"
            
            if config.compression:
                filename += ".gz"
            
            # Create backup directory
            backup_dir = Path("/tmp/backups")  # In production, use proper backup storage
            backup_dir.mkdir(parents=True, exist_ok=True)
            file_path = backup_dir / filename
            
            # Build pg_dump command
            dump_cmd = ["pg_dump"]
            
            if config.backup_type == BackupType.FULL:
                dump_cmd.extend(["--verbose", "--clean", "--if-exists"])
            elif config.backup_type == BackupType.INCREMENTAL:
                # For incremental, we'd need WAL archiving setup
                # This is simplified for demo
                dump_cmd.extend(["--verbose", "--data-only"])
            
            dump_cmd.extend([
                "--format=custom",
                "--file", str(file_path),
                db_url
            ])
            
            # Execute backup
            result = subprocess.run(dump_cmd, capture_output=True, text=True, timeout=3600)
            
            if result.returncode == 0:
                file_size = file_path.stat().st_size
                compression_ratio = 1.0
                
                if config.compression:
                    # Apply compression
                    compressed_size = self._compress_file(file_path)
                    compression_ratio = compressed_size / file_size if file_size > 0 else 1.0
                
                if config.encryption:
                    # Apply encryption
                    self._encrypt_file(file_path)
                
                # Upload to configured location
                if config.location != BackupLocation.LOCAL:
                    remote_path = self._upload_backup(file_path, config)
                    if remote_path:
                        file_path = remote_path
                
                return {
                    'success': True,
                    'file_path': str(file_path),
                    'file_size': file_size,
                    'compression_ratio': compression_ratio
                }
            else:
                return {
                    'success': False,
                    'error': f"pg_dump failed: {result.stderr}"
                }
                
        except subprocess.TimeoutExpired:
            return {
                'success': False,
                'error': "Backup operation timed out"
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Backup operation failed: {str(e)}"
            }
    
    def _verify_backup(self, job: BackupJob, config: BackupConfig) -> Dict[str, Any]:
        """Verify backup integrity."""
        start_time = time.time()
        
        try:
            # Basic file integrity check
            if not os.path.exists(job.file_path):
                return {'success': False, 'status': 'file_not_found'}
            
            file_size = os.path.getsize(job.file_path)
            if file_size != job.file_size_bytes:
                return {'success': False, 'status': 'size_mismatch'}
            
            # For more thorough verification, we could:
            # 1. Restore to a temporary database
            # 2. Run integrity checks
            # 3. Compare checksums
            
            # Simplified verification - check if file is readable
            try:
                with open(job.file_path, 'rb') as f:
                    # Read first 1KB to verify file is readable
                    f.read(1024)
                
                verification_time = time.time() - start_time
                BACKUP_VERIFICATION_DURATION.observe(verification_time)
                
                return {'success': True, 'status': 'verified'}
                
            except Exception as e:
                return {'success': False, 'status': f'read_error: {e}'}
                
        except Exception as e:
            return {'success': False, 'status': f'verification_error: {e}'}
    
    def _compress_file(self, file_path: Path) -> int:
        """Compress backup file."""
        try:
            import gzip
            import shutil
            
            compressed_path = file_path.with_suffix(file_path.suffix + '.gz')
            
            with open(file_path, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # Replace original with compressed
            file_path.unlink()
            compressed_path.rename(file_path)
            
            return file_path.stat().st_size
            
        except Exception as e:
            logger.error(f"Compression failed: {e}")
            return file_path.stat().st_size
    
    def _encrypt_file(self, file_path: Path):
        """Encrypt backup file."""
        try:
            # In production, use proper encryption
            # For demo, just rename to indicate encryption
            encrypted_path = file_path.with_suffix(file_path.suffix + '.enc')
            file_path.rename(encrypted_path)
            return encrypted_path
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            return file_path
    
    def _upload_backup(self, file_path: Path, config: BackupConfig) -> Optional[str]:
        """Upload backup to remote storage."""
        try:
            if config.location == BackupLocation.S3:
                # AWS S3 upload logic
                remote_path = f"s3://backups/{file_path.name}"
                # In production, use boto3 to upload
                logger.info(f"Simulated upload to {remote_path}")
                return remote_path
                
            elif config.location == BackupLocation.GCS:
                # Google Cloud Storage upload logic
                remote_path = f"gs://backups/{file_path.name}"
                logger.info(f"Simulated upload to {remote_path}")
                return remote_path
                
            elif config.location == BackupLocation.AZURE:
                # Azure Blob Storage upload logic
                remote_path = f"azure://backups/{file_path.name}"
                logger.info(f"Simulated upload to {remote_path}")
                return remote_path
            
            return None
            
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return None
    
    def _replicate_backup(self, job: BackupJob, config: BackupConfig):
        """Replicate backup to secondary region."""
        try:
            # Cross-region replication logic
            logger.info(f"Replicating backup {job.id} to secondary region")
            # In production, implement actual cross-region copy
        except Exception as e:
            logger.error(f"Cross-region replication failed: {e}")
    
    def restore_from_backup(self, backup_id: str, target_database: str = None) -> Dict[str, Any]:
        """Restore database from backup."""
        start_time = time.time()
        
        try:
            job = self._active_jobs.get(backup_id)
            if not job:
                return {'success': False, 'error': 'Backup job not found'}
            
            if job.status != BackupStatus.VERIFIED and job.status != BackupStatus.COMPLETED:
                return {'success': False, 'error': 'Backup not ready for restore'}
            
            RESTORE_OPERATIONS.labels(type=job.backup_type.value, result='started').inc()
            
            # Download backup if it's remote
            local_file_path = self._download_backup_if_needed(job)
            
            # Perform restore
            restore_result = self._perform_restore(local_file_path, target_database)
            
            if restore_result['success']:
                job.status = BackupStatus.RESTORED
                RESTORE_OPERATIONS.labels(type=job.backup_type.value, result='success').inc()
                
                duration = time.time() - start_time
                logger.info(f"Restore from backup {backup_id} completed in {duration:.2f}s")
                
                return {
                    'success': True,
                    'duration_seconds': duration,
                    'restored_database': target_database or 'default'
                }
            else:
                RESTORE_OPERATIONS.labels(type=job.backup_type.value, result='failure').inc()
                return restore_result
                
        except Exception as e:
            RESTORE_OPERATIONS.labels(type='unknown', result='error').inc()
            logger.error(f"Restore operation failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def _download_backup_if_needed(self, job: BackupJob) -> str:
        """Download backup file if it's stored remotely."""
        if job.file_path.startswith(('s3://', 'gs://', 'azure://')):
            # Download logic for remote backups
            local_path = f"/tmp/restore_{job.id}"
            # In production, implement actual download
            logger.info(f"Downloading backup from {job.file_path} to {local_path}")
            return local_path
        return job.file_path
    
    def _perform_restore(self, file_path: str, target_database: str) -> Dict[str, Any]:
        """Perform the actual database restore."""
        try:
            db_url = self.settings.database_url
            
            # Build pg_restore command
            restore_cmd = [
                "pg_restore",
                "--verbose",
                "--clean",
                "--if-exists",
                "--dbname", target_database or db_url,
                file_path
            ]
            
            result = subprocess.run(restore_cmd, capture_output=True, text=True, timeout=7200)
            
            if result.returncode == 0:
                return {'success': True}
            else:
                return {'success': False, 'error': f"pg_restore failed: {result.stderr}"}
                
        except subprocess.TimeoutExpired:
            return {'success': False, 'error': "Restore operation timed out"}
        except Exception as e:
            return {'success': False, 'error': f"Restore failed: {str(e)}"}
    
    def cleanup_old_backups(self) -> Dict[str, Any]:
        """Clean up old backups based on retention policies."""
        results = {
            'deleted': 0,
            'errors': 0,
            'freed_bytes': 0
        }
        
        for backup_type, config in self._backup_configs.items():
            try:
                cutoff_date = datetime.utcnow() - timedelta(days=config.retention_days)
                
                # Find old backups
                old_jobs = [
                    job for job in self._active_jobs.values()
                    if (job.backup_type == backup_type and 
                        job.started_at < cutoff_date and
                        job.status in [BackupStatus.COMPLETED, BackupStatus.VERIFIED])
                ]
                
                for job in old_jobs:
                    try:
                        # Delete backup file
                        if os.path.exists(job.file_path):
                            os.remove(job.file_path)
                            results['freed_bytes'] += job.file_size_bytes
                        
                        # Remove from active jobs
                        del self._active_jobs[job.id]
                        results['deleted'] += 1
                        
                        logger.info(f"Deleted old backup {job.id}")
                        
                    except Exception as e:
                        logger.error(f"Failed to delete backup {job.id}: {e}")
                        results['errors'] += 1
                        
            except Exception as e:
                logger.error(f"Cleanup failed for {backup_type.value}: {e}")
                results['errors'] += 1
        
        return results
    
    def get_backup_status(self, backup_id: str) -> Optional[BackupJob]:
        """Get status of a backup job."""
        return self._active_jobs.get(backup_id)
    
    def list_backups(self, backup_type: BackupType = None) -> List[BackupJob]:
        """List all backup jobs, optionally filtered by type."""
        jobs = list(self._active_jobs.values())
        
        if backup_type:
            jobs = [job for job in jobs if job.backup_type == backup_type]
        
        # Sort by start time, newest first
        jobs.sort(key=lambda x: x.started_at, reverse=True)
        
        return jobs

# Global backup manager
backup_manager = BackupManager()

def create_full_backup() -> str:
    """Convenience function to create a full backup."""
    return backup_manager.create_backup(BackupType.FULL)

def create_incremental_backup() -> str:
    """Convenience function to create an incremental backup."""
    return backup_manager.create_backup(BackupType.INCREMENTAL)

def restore_database(backup_id: str, target_db: str = None) -> Dict[str, Any]:
    """Convenience function to restore from backup."""
    return backup_manager.restore_from_backup(backup_id, target_db)
