from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class EncryptionProvider(Protocol):
    provider_id: str

    def encrypt(self, payload: bytes, *, context: dict[str, Any]) -> bytes: ...
    def decrypt(self, payload: bytes, *, context: dict[str, Any]) -> bytes: ...
    def rewrap(self, payload: bytes, *, context: dict[str, Any]) -> bytes: ...


@runtime_checkable
class ArtifactAuthorizer(Protocol):
    def authorize(self, artifact: Any, *, context: dict[str, Any]) -> bool: ...


class ArtifactPolicyError(RuntimeError):
    pass


class ArtifactAccessDenied(ArtifactPolicyError):
    pass
