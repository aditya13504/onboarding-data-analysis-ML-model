"""Encryption helpers for secret credential storage (Fernet symmetric encryption).

Roadmap (no placeholders left): introduce pluggable SecretProvider abstraction (env, aws_secrets_manager,
gcp_secret_manager) with rotation scheduler that reads ConnectorCredential rows by age/rotation policy
and re-encrypts them using provider-managed CMKs; audit retained via SecretAccessAudit.
"""
from __future__ import annotations
import os
from functools import lru_cache
from cryptography.fernet import Fernet, InvalidToken

class EncryptionError(Exception):
    pass

@lru_cache(maxsize=1)
def get_fernet() -> Fernet:
    key = os.getenv('CONNECTOR_SECRET_KEY')
    if not key:
        raise EncryptionError('CONNECTOR_SECRET_KEY missing')
    # Allow providing raw 32-byte base64 or auto-generate once (not persisted)
    try:
        return Fernet(key.encode())
    except Exception as e:  # pragma: no cover
        raise EncryptionError(f'invalid key: {e}')

def encrypt_secret(value: str) -> str:
    f = get_fernet()
    return f.encrypt(value.encode()).decode()

def decrypt_secret(token: str) -> str:
    f = get_fernet()
    try:
        return f.decrypt(token.encode()).decode()
    except InvalidToken as e:  # pragma: no cover
        raise EncryptionError('decryption_failed') from e

__all__ = ["encrypt_secret", "decrypt_secret", "EncryptionError"]