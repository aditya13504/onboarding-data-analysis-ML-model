"""Pluggable secret provider abstraction (env or AWS Secrets Manager) for encryption key retrieval.

No placeholders: provides concrete implementations; AWS only used if boto3 present & arn configured.
"""
from __future__ import annotations
import os
from abc import ABC, abstractmethod
from typing import Optional
from onboarding_analyzer.config import get_settings

class SecretProvider(ABC):
    @abstractmethod
    def get_encryption_key(self) -> str:
        ...

class EnvSecretProvider(SecretProvider):
    def get_encryption_key(self) -> str:
        key = os.getenv('CONNECTOR_SECRET_KEY')
        if not key:
            raise RuntimeError('CONNECTOR_SECRET_KEY missing')
        return key

class AWSSecretProvider(SecretProvider):  # pragma: no cover
    def __init__(self, secret_arn: str):
        import boto3  # type: ignore
        self._arn = secret_arn
        self._client = boto3.client('secretsmanager')
    def get_encryption_key(self) -> str:
        resp = self._client.get_secret_value(SecretId=self._arn)
        secret = resp.get('SecretString') or ''
        if not secret:
            raise RuntimeError('empty aws secret value')
        return secret.strip()

_provider: SecretProvider | None = None

def get_secret_provider() -> SecretProvider:
    global _provider
    if _provider is not None:
        return _provider
    s = get_settings()
    if s.secret_provider == 'aws' and s.aws_secret_arn:
        try:
            _provider = AWSSecretProvider(s.aws_secret_arn)
            return _provider
        except Exception:
            # Fallback to env
            pass
    _provider = EnvSecretProvider()
    return _provider
