from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Protocol, runtime_checkable

from actionlens.models import MediaMetadata


@runtime_checkable
class EncryptionProvider(Protocol):
    provider_id: str

    def encrypt(self, payload: bytes, *, context: dict[str, Any]) -> bytes: ...
    def decrypt(self, payload: bytes, *, context: dict[str, Any]) -> bytes: ...
    def rewrap(self, payload: bytes, *, context: dict[str, Any]) -> bytes: ...


@dataclass(frozen=True)
class EncryptionMetadata:
    """Non-secret metadata required to interpret a streamed ciphertext.

    Providers must never place key material in these fields. ``nonce`` and
    ``authentication_tag`` are expected to be text-safe encodings chosen by
    the provider (for example, base64url or hexadecimal).
    """

    algorithm: str
    key_version: str | None = None
    nonce: str | None = None
    authentication_tag: str | None = None

    def __post_init__(self) -> None:
        if not self.algorithm.strip():
            raise ValueError("encryption algorithm must not be empty")

    def as_dict(self) -> dict[str, str]:
        values = {
            "algorithm": self.algorithm,
            "key_version": self.key_version,
            "nonce": self.nonce,
            "authentication_tag": self.authentication_tag,
        }
        return {name: value for name, value in values.items() if value is not None}


@runtime_checkable
class StreamingEncryptionProvider(Protocol):
    """Optional v2 artifact encryption capability.

    Implementations must process bounded reads from ``source`` and must not
    close either stream. The returned metadata is persisted with the artifact;
    it is deliberately limited to non-secret values.
    """

    provider_id: str
    algorithm: str

    def encrypt_stream(
        self, source: BinaryIO, destination: BinaryIO, *, context: dict[str, Any]
    ) -> EncryptionMetadata: ...

    def decrypt_stream(
        self, source: BinaryIO, destination: BinaryIO, *, context: dict[str, Any]
    ) -> None: ...


@runtime_checkable
class ArtifactAuthorizer(Protocol):
    def authorize(self, artifact: Any, *, context: dict[str, Any]) -> bool: ...


@runtime_checkable
class MediaMetadataExtractor(Protocol):
    """Optional host-provided extractor for image, audio, and video facts.

    The path is a local plaintext artifact managed by the store and must be
    treated as read-only. Encrypted artifacts are deliberately not passed to
    this SPI; callers can provide trusted metadata explicitly when extraction
    must happen before encryption.
    """

    def extract(self, path: Path, media_type: str) -> MediaMetadata | None: ...


class ArtifactPolicyError(RuntimeError):
    pass


class ArtifactAccessDenied(ArtifactPolicyError):
    pass
