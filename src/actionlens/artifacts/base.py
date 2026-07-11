from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class EncryptionProvider(Protocol):
    provider_id: str

    def encrypt(self, payload: bytes, *, context: dict[str, Any]) -> bytes: ...


class ArtifactPolicyError(RuntimeError):
    pass
