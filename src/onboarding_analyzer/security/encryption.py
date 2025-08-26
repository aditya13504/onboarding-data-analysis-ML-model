"""
Data Encryption and Protection System

Enterprise-grade data encryption, field-level encryption, key management,
and comprehensive data protection for sensitive analytics data.
"""

import asyncio
import logging
import secrets
import os
import base64
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json
from pathlib import Path

# Cryptography
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

# Database encryption
import sqlalchemy as sa
from sqlalchemy.types import TypeDecorator, String, Text
from sqlalchemy.ext.hybrid import hybrid_property

# Monitoring
from prometheus_client import Counter, Histogram, Gauge

# Initialize metrics
encryption_operations = Counter('encryption_operations_total', 'Encryption operations', ['operation', 'algorithm'])
key_rotations = Counter('key_rotations_total', 'Key rotations', ['key_type'])
encryption_errors = Counter('encryption_errors_total', 'Encryption errors', ['error_type'])
active_encryption_keys = Gauge('active_encryption_keys', 'Active encryption keys', ['key_type'])

logger = logging.getLogger(__name__)

class EncryptionAlgorithm(Enum):
    """Supported encryption algorithms."""
    FERNET = "fernet"
    AES_256_GCM = "aes_256_gcm"
    AES_256_CBC = "aes_256_cbc"
    RSA_2048 = "rsa_2048"
    RSA_4096 = "rsa_4096"

class KeyType(Enum):
    """Types of encryption keys."""
    MASTER = "master"
    DATA = "data"
    FIELD = "field"
    SESSION = "session"
    BACKUP = "backup"

class EncryptionLevel(Enum):
    """Levels of encryption."""
    NONE = "none"
    BASIC = "basic"
    STANDARD = "standard"
    HIGH = "high"
    MAXIMUM = "maximum"

@dataclass
class EncryptionKey:
    """Encryption key metadata."""
    key_id: str
    key_type: KeyType
    algorithm: EncryptionAlgorithm
    created_at: datetime
    expires_at: Optional[datetime]
    is_active: bool = True
    version: int = 1
    purpose: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class EncryptionConfig:
    """Encryption configuration."""
    default_algorithm: EncryptionAlgorithm
    key_rotation_interval: timedelta
    encryption_level: EncryptionLevel
    field_encryption_rules: Dict[str, str]
    backup_encryption: bool = True
    transit_encryption: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class EncryptedData:
    """Encrypted data container."""
    ciphertext: str
    algorithm: EncryptionAlgorithm
    key_id: str
    iv: Optional[str] = None
    tag: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

class KeyManager:
    """Encryption key management system."""
    
    def __init__(self, master_key: Optional[str] = None):
        self.master_key = master_key or Fernet.generate_key()
        self.master_fernet = Fernet(self.master_key)
        self.keys: Dict[str, EncryptionKey] = {}
        self.key_data: Dict[str, bytes] = {}
        self.key_versions: Dict[str, List[str]] = {}  # key_id -> list of version key_ids
        
        # Initialize default keys
        self._initialize_default_keys()
    
    def _initialize_default_keys(self):
        """Initialize default encryption keys."""
        # Master data encryption key
        data_key_id = self.generate_key(
            key_type=KeyType.DATA,
            algorithm=EncryptionAlgorithm.FERNET,
            purpose="Default data encryption"
        )
        
        # Field-level encryption keys
        field_key_id = self.generate_key(
            key_type=KeyType.FIELD,
            algorithm=EncryptionAlgorithm.AES_256_GCM,
            purpose="Field-level encryption"
        )
        
        # Session encryption key
        session_key_id = self.generate_key(
            key_type=KeyType.SESSION,
            algorithm=EncryptionAlgorithm.FERNET,
            purpose="Session data encryption",
            expires_at=datetime.now(timezone.utc) + timedelta(hours=24)
        )
        
        logger.info("Default encryption keys initialized")
    
    def generate_key(self, key_type: KeyType, algorithm: EncryptionAlgorithm,
                    purpose: str = "", expires_at: Optional[datetime] = None) -> str:
        """Generate new encryption key."""
        try:
            key_id = f"{key_type.value}_{secrets.token_urlsafe(16)}"
            
            # Generate key material based on algorithm
            if algorithm == EncryptionAlgorithm.FERNET:
                key_data = Fernet.generate_key()
            elif algorithm in [EncryptionAlgorithm.AES_256_GCM, EncryptionAlgorithm.AES_256_CBC]:
                key_data = secrets.token_bytes(32)  # 256 bits
            elif algorithm == EncryptionAlgorithm.RSA_2048:
                private_key = rsa.generate_private_key(
                    public_exponent=65537,
                    key_size=2048,
                    backend=default_backend()
                )
                key_data = private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
            elif algorithm == EncryptionAlgorithm.RSA_4096:
                private_key = rsa.generate_private_key(
                    public_exponent=65537,
                    key_size=4096,
                    backend=default_backend()
                )
                key_data = private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
            else:
                raise ValueError(f"Unsupported algorithm: {algorithm}")
            
            # Create key metadata
            encryption_key = EncryptionKey(
                key_id=key_id,
                key_type=key_type,
                algorithm=algorithm,
                created_at=datetime.now(timezone.utc),
                expires_at=expires_at,
                purpose=purpose
            )
            
            # Store encrypted key data
            encrypted_key_data = self.master_fernet.encrypt(key_data)
            
            self.keys[key_id] = encryption_key
            self.key_data[key_id] = encrypted_key_data
            
            # Track versions
            base_key_id = f"{key_type.value}_{algorithm.value}"
            if base_key_id not in self.key_versions:
                self.key_versions[base_key_id] = []
            self.key_versions[base_key_id].append(key_id)
            
            # Update metrics
            active_encryption_keys.labels(key_type=key_type.value).inc()
            
            logger.info(f"Encryption key generated: {key_id} ({algorithm.value})")
            return key_id
            
        except Exception as e:
            logger.error(f"Key generation failed: {e}")
            encryption_errors.labels(error_type='key_generation').inc()
            raise
    
    def get_key(self, key_id: str) -> Optional[bytes]:
        """Get decrypted key data."""
        try:
            if key_id not in self.keys or key_id not in self.key_data:
                return None
            
            key_metadata = self.keys[key_id]
            
            # Check if key is active and not expired
            if not key_metadata.is_active:
                return None
            
            if key_metadata.expires_at and datetime.now(timezone.utc) > key_metadata.expires_at:
                logger.warning(f"Key {key_id} has expired")
                return None
            
            # Decrypt key data
            encrypted_key_data = self.key_data[key_id]
            key_data = self.master_fernet.decrypt(encrypted_key_data)
            
            return key_data
            
        except Exception as e:
            logger.error(f"Key retrieval failed for {key_id}: {e}")
            encryption_errors.labels(error_type='key_retrieval').inc()
            return None
    
    def rotate_key(self, key_id: str) -> Optional[str]:
        """Rotate encryption key."""
        try:
            if key_id not in self.keys:
                return None
            
            old_key = self.keys[key_id]
            
            # Generate new key with same parameters
            new_key_id = self.generate_key(
                key_type=old_key.key_type,
                algorithm=old_key.algorithm,
                purpose=old_key.purpose,
                expires_at=old_key.expires_at
            )
            
            # Deactivate old key
            old_key.is_active = False
            
            # Update metrics
            key_rotations.labels(key_type=old_key.key_type.value).inc()
            
            logger.info(f"Key rotated: {key_id} -> {new_key_id}")
            return new_key_id
            
        except Exception as e:
            logger.error(f"Key rotation failed for {key_id}: {e}")
            encryption_errors.labels(error_type='key_rotation').inc()
            return None
    
    def get_active_key(self, key_type: KeyType, algorithm: EncryptionAlgorithm) -> Optional[str]:
        """Get active key ID for type and algorithm."""
        for key_id, key_metadata in self.keys.items():
            if (key_metadata.key_type == key_type and 
                key_metadata.algorithm == algorithm and
                key_metadata.is_active):
                
                # Check expiration
                if key_metadata.expires_at and datetime.now(timezone.utc) > key_metadata.expires_at:
                    continue
                
                return key_id
        
        return None
    
    def list_keys(self, key_type: Optional[KeyType] = None, 
                 active_only: bool = True) -> List[EncryptionKey]:
        """List encryption keys."""
        keys = []
        for key_metadata in self.keys.values():
            if key_type and key_metadata.key_type != key_type:
                continue
            
            if active_only and not key_metadata.is_active:
                continue
            
            # Check expiration
            if (active_only and key_metadata.expires_at and 
                datetime.now(timezone.utc) > key_metadata.expires_at):
                continue
            
            keys.append(key_metadata)
        
        return keys

class DataEncryptor:
    """Data encryption and decryption service."""
    
    def __init__(self, key_manager: KeyManager):
        self.key_manager = key_manager
    
    def encrypt_data(self, data: Union[str, bytes], algorithm: EncryptionAlgorithm,
                    key_id: Optional[str] = None) -> EncryptedData:
        """Encrypt data using specified algorithm."""
        try:
            # Get or determine key
            if not key_id:
                key_id = self.key_manager.get_active_key(KeyType.DATA, algorithm)
                if not key_id:
                    raise ValueError(f"No active key found for {algorithm}")
            
            key_data = self.key_manager.get_key(key_id)
            if not key_data:
                raise ValueError(f"Key not found: {key_id}")
            
            # Convert data to bytes
            if isinstance(data, str):
                data_bytes = data.encode('utf-8')
            else:
                data_bytes = data
            
            # Encrypt based on algorithm
            if algorithm == EncryptionAlgorithm.FERNET:
                fernet = Fernet(key_data)
                ciphertext = fernet.encrypt(data_bytes)
                encrypted_data = EncryptedData(
                    ciphertext=base64.b64encode(ciphertext).decode(),
                    algorithm=algorithm,
                    key_id=key_id
                )
            
            elif algorithm == EncryptionAlgorithm.AES_256_GCM:
                iv = secrets.token_bytes(12)  # 96-bit IV for GCM
                cipher = Cipher(
                    algorithms.AES(key_data),
                    modes.GCM(iv),
                    backend=default_backend()
                )
                encryptor = cipher.encryptor()
                ciphertext = encryptor.update(data_bytes) + encryptor.finalize()
                
                encrypted_data = EncryptedData(
                    ciphertext=base64.b64encode(ciphertext).decode(),
                    algorithm=algorithm,
                    key_id=key_id,
                    iv=base64.b64encode(iv).decode(),
                    tag=base64.b64encode(encryptor.tag).decode()
                )
            
            elif algorithm == EncryptionAlgorithm.AES_256_CBC:
                iv = secrets.token_bytes(16)  # 128-bit IV for CBC
                cipher = Cipher(
                    algorithms.AES(key_data),
                    modes.CBC(iv),
                    backend=default_backend()
                )
                encryptor = cipher.encryptor()
                
                # Pad data to block size
                padded_data = self._pad_data(data_bytes, 16)
                ciphertext = encryptor.update(padded_data) + encryptor.finalize()
                
                encrypted_data = EncryptedData(
                    ciphertext=base64.b64encode(ciphertext).decode(),
                    algorithm=algorithm,
                    key_id=key_id,
                    iv=base64.b64encode(iv).decode()
                )
            
            else:
                raise ValueError(f"Unsupported encryption algorithm: {algorithm}")
            
            # Update metrics
            encryption_operations.labels(
                operation='encrypt',
                algorithm=algorithm.value
            ).inc()
            
            return encrypted_data
            
        except Exception as e:
            logger.error(f"Data encryption failed: {e}")
            encryption_errors.labels(error_type='encryption').inc()
            raise
    
    def decrypt_data(self, encrypted_data: EncryptedData) -> bytes:
        """Decrypt data."""
        try:
            key_data = self.key_manager.get_key(encrypted_data.key_id)
            if not key_data:
                raise ValueError(f"Key not found: {encrypted_data.key_id}")
            
            ciphertext = base64.b64decode(encrypted_data.ciphertext)
            
            # Decrypt based on algorithm
            if encrypted_data.algorithm == EncryptionAlgorithm.FERNET:
                fernet = Fernet(key_data)
                plaintext = fernet.decrypt(ciphertext)
            
            elif encrypted_data.algorithm == EncryptionAlgorithm.AES_256_GCM:
                if not encrypted_data.iv or not encrypted_data.tag:
                    raise ValueError("IV and tag required for AES-GCM decryption")
                
                iv = base64.b64decode(encrypted_data.iv)
                tag = base64.b64decode(encrypted_data.tag)
                
                cipher = Cipher(
                    algorithms.AES(key_data),
                    modes.GCM(iv, tag),
                    backend=default_backend()
                )
                decryptor = cipher.decryptor()
                plaintext = decryptor.update(ciphertext) + decryptor.finalize()
            
            elif encrypted_data.algorithm == EncryptionAlgorithm.AES_256_CBC:
                if not encrypted_data.iv:
                    raise ValueError("IV required for AES-CBC decryption")
                
                iv = base64.b64decode(encrypted_data.iv)
                
                cipher = Cipher(
                    algorithms.AES(key_data),
                    modes.CBC(iv),
                    backend=default_backend()
                )
                decryptor = cipher.decryptor()
                padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()
                plaintext = self._unpad_data(padded_plaintext)
            
            else:
                raise ValueError(f"Unsupported decryption algorithm: {encrypted_data.algorithm}")
            
            # Update metrics
            encryption_operations.labels(
                operation='decrypt',
                algorithm=encrypted_data.algorithm.value
            ).inc()
            
            return plaintext
            
        except Exception as e:
            logger.error(f"Data decryption failed: {e}")
            encryption_errors.labels(error_type='decryption').inc()
            raise
    
    def encrypt_text(self, text: str, algorithm: EncryptionAlgorithm = EncryptionAlgorithm.FERNET,
                    key_id: Optional[str] = None) -> str:
        """Encrypt text and return base64 encoded result."""
        encrypted_data = self.encrypt_data(text, algorithm, key_id)
        return json.dumps({
            'ciphertext': encrypted_data.ciphertext,
            'algorithm': encrypted_data.algorithm.value,
            'key_id': encrypted_data.key_id,
            'iv': encrypted_data.iv,
            'tag': encrypted_data.tag
        })
    
    def decrypt_text(self, encrypted_text: str) -> str:
        """Decrypt base64 encoded encrypted text."""
        data = json.loads(encrypted_text)
        encrypted_data = EncryptedData(
            ciphertext=data['ciphertext'],
            algorithm=EncryptionAlgorithm(data['algorithm']),
            key_id=data['key_id'],
            iv=data.get('iv'),
            tag=data.get('tag')
        )
        
        plaintext_bytes = self.decrypt_data(encrypted_data)
        return plaintext_bytes.decode('utf-8')
    
    def _pad_data(self, data: bytes, block_size: int) -> bytes:
        """PKCS7 padding."""
        padding_length = block_size - (len(data) % block_size)
        padding = bytes([padding_length] * padding_length)
        return data + padding
    
    def _unpad_data(self, data: bytes) -> bytes:
        """Remove PKCS7 padding."""
        padding_length = data[-1]
        return data[:-padding_length]

class FieldEncryption:
    """Field-level encryption for database columns."""
    
    def __init__(self, data_encryptor: DataEncryptor, 
                 default_algorithm: EncryptionAlgorithm = EncryptionAlgorithm.FERNET):
        self.data_encryptor = data_encryptor
        self.default_algorithm = default_algorithm
        self.field_configs: Dict[str, Dict[str, Any]] = {}
    
    def configure_field(self, field_name: str, algorithm: EncryptionAlgorithm,
                       key_id: Optional[str] = None):
        """Configure encryption for specific field."""
        self.field_configs[field_name] = {
            'algorithm': algorithm,
            'key_id': key_id
        }
    
    def encrypt_field(self, field_name: str, value: Any) -> str:
        """Encrypt field value."""
        if value is None:
            return None
        
        # Get field configuration
        config = self.field_configs.get(field_name, {})
        algorithm = config.get('algorithm', self.default_algorithm)
        key_id = config.get('key_id')
        
        # Convert value to string
        if not isinstance(value, str):
            value = str(value)
        
        return self.data_encryptor.encrypt_text(value, algorithm, key_id)
    
    def decrypt_field(self, field_name: str, encrypted_value: str) -> str:
        """Decrypt field value."""
        if encrypted_value is None:
            return None
        
        return self.data_encryptor.decrypt_text(encrypted_value)

class EncryptedColumn(TypeDecorator):
    """SQLAlchemy encrypted column type."""
    
    impl = Text
    cache_ok = True
    
    def __init__(self, field_encryptor: FieldEncryption, field_name: str, **kwargs):
        self.field_encryptor = field_encryptor
        self.field_name = field_name
        super().__init__(**kwargs)
    
    def process_bind_param(self, value, dialect):
        """Encrypt value before storing in database."""
        if value is not None:
            return self.field_encryptor.encrypt_field(self.field_name, value)
        return value
    
    def process_result_value(self, value, dialect):
        """Decrypt value after retrieving from database."""
        if value is not None:
            return self.field_encryptor.decrypt_field(self.field_name, value)
        return value

class EncryptionService:
    """Main encryption service."""
    
    def __init__(self, master_key: Optional[str] = None, config: Optional[EncryptionConfig] = None):
        self.key_manager = KeyManager(master_key)
        self.data_encryptor = DataEncryptor(self.key_manager)
        self.field_encryptor = FieldEncryption(self.data_encryptor)
        self.config = config or self._default_config()
        
        # Configure field encryption based on config
        self._configure_field_encryption()
        
        # Start key rotation task
        self._start_key_rotation_task()
    
    def _default_config(self) -> EncryptionConfig:
        """Create default encryption configuration."""
        return EncryptionConfig(
            default_algorithm=EncryptionAlgorithm.FERNET,
            key_rotation_interval=timedelta(days=30),
            encryption_level=EncryptionLevel.STANDARD,
            field_encryption_rules={
                "email": "fernet",
                "phone": "fernet",
                "address": "fernet",
                "ssn": "aes_256_gcm",
                "credit_card": "aes_256_gcm",
                "personal_data": "aes_256_gcm"
            }
        )
    
    def _configure_field_encryption(self):
        """Configure field encryption based on rules."""
        for field_name, algorithm_name in self.config.field_encryption_rules.items():
            try:
                algorithm = EncryptionAlgorithm(algorithm_name)
                self.field_encryptor.configure_field(field_name, algorithm)
            except ValueError:
                logger.warning(f"Unknown encryption algorithm for field {field_name}: {algorithm_name}")
    
    def _start_key_rotation_task(self):
        """Start automatic key rotation task."""
        async def rotation_loop():
            while True:
                try:
                    await asyncio.sleep(self.config.key_rotation_interval.total_seconds())
                    await self._rotate_keys()
                except Exception as e:
                    logger.error(f"Key rotation task error: {e}")
                    await asyncio.sleep(3600)  # Retry in 1 hour
        
        asyncio.create_task(rotation_loop())
    
    async def _rotate_keys(self):
        """Rotate encryption keys automatically."""
        logger.info("Starting automatic key rotation")
        
        # Rotate data keys older than rotation interval
        cutoff_time = datetime.now(timezone.utc) - self.config.key_rotation_interval
        
        for key_metadata in self.key_manager.list_keys(KeyType.DATA):
            if key_metadata.created_at < cutoff_time:
                new_key_id = self.key_manager.rotate_key(key_metadata.key_id)
                if new_key_id:
                    logger.info(f"Rotated data key: {key_metadata.key_id} -> {new_key_id}")
        
        # Rotate field keys
        for key_metadata in self.key_manager.list_keys(KeyType.FIELD):
            if key_metadata.created_at < cutoff_time:
                new_key_id = self.key_manager.rotate_key(key_metadata.key_id)
                if new_key_id:
                    logger.info(f"Rotated field key: {key_metadata.key_id} -> {new_key_id}")
    
    def encrypt_sensitive_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Encrypt sensitive fields in data dictionary."""
        encrypted_data = data.copy()
        
        for field_name, value in data.items():
            if field_name in self.config.field_encryption_rules:
                if value is not None:
                    encrypted_data[field_name] = self.field_encryptor.encrypt_field(field_name, value)
        
        return encrypted_data
    
    def decrypt_sensitive_data(self, encrypted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Decrypt sensitive fields in data dictionary."""
        decrypted_data = encrypted_data.copy()
        
        for field_name, value in encrypted_data.items():
            if field_name in self.config.field_encryption_rules:
                if value is not None:
                    try:
                        decrypted_data[field_name] = self.field_encryptor.decrypt_field(field_name, value)
                    except Exception as e:
                        logger.error(f"Failed to decrypt field {field_name}: {e}")
                        # Keep encrypted value if decryption fails
        
        return decrypted_data
    
    def create_encrypted_column(self, field_name: str) -> EncryptedColumn:
        """Create encrypted SQLAlchemy column."""
        return EncryptedColumn(self.field_encryptor, field_name)
    
    def get_encryption_status(self) -> Dict[str, Any]:
        """Get encryption service status."""
        active_keys = self.key_manager.list_keys(active_only=True)
        
        return {
            "active_keys": len(active_keys),
            "key_types": {
                key_type.value: len([k for k in active_keys if k.key_type == key_type])
                for key_type in KeyType
            },
            "algorithms": {
                algorithm.value: len([k for k in active_keys if k.algorithm == algorithm])
                for algorithm in EncryptionAlgorithm
            },
            "field_encryption_rules": len(self.config.field_encryption_rules),
            "encryption_level": self.config.encryption_level.value,
            "next_rotation": datetime.now(timezone.utc) + self.config.key_rotation_interval
        }

# Global instance
_encryption_service: Optional[EncryptionService] = None

def initialize_encryption(master_key: Optional[str] = None, 
                         config: Optional[EncryptionConfig] = None) -> EncryptionService:
    """Initialize encryption service."""
    global _encryption_service
    
    _encryption_service = EncryptionService(master_key, config)
    
    logger.info("Encryption service initialized")
    return _encryption_service

def get_encryption_service() -> EncryptionService:
    """Get encryption service instance."""
    if _encryption_service is None:
        raise RuntimeError("Encryption service not initialized")
    return _encryption_service
