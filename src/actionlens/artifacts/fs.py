from __future__ import annotations

import functools
import io
import json
import logging
import os
import re
import stat
import tempfile
import threading
import time
import weakref
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import unquote, urlsplit
from uuid import uuid4

from actionlens.models import (
    ArtifactPolicy,
    ArtifactProvenance,
    ArtifactRef,
    MediaMetadata,
)

from .base import (
    ArtifactAccessDenied,
    ArtifactAuthorizer,
    ArtifactPolicyError,
    EncryptionMetadata,
    EncryptionProvider,
    MediaMetadataExtractor,
)

_STREAM_CHUNK_BYTES = 256 * 1024
_LEASE_SECONDS = 300
_MAX_PROVENANCE_RECORDS_PER_ARTIFACT = 64
logger = logging.getLogger(__name__)


_STORE_LOCKS: weakref.WeakValueDictionary[str, _StoreLock] = weakref.WeakValueDictionary()
_STORE_LOCKS_GUARD = threading.Lock()


class _StoreLock:
    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        self.local = threading.local()

    @contextmanager
    def acquire(self):
        # One OS lock coordinates writes, readers and GC. The OS releases it
        # on process death; active readers cannot lose a time-based lease.
        if not self.lock.acquire(timeout=5):
            raise ArtifactPolicyError("timed out waiting for artifact store access")
        try:
            if getattr(self.local, "depth", 0):
                self.local.depth += 1
                try:
                    yield
                finally:
                    self.local.depth -= 1
                return
            with self.path.open("a+b") as handle:
                deadline = time.monotonic() + 5
                if os.name == "nt":
                    import msvcrt
                    if handle.seek(0, 2) == 0:
                        handle.write(b"0")
                        handle.flush()
                    while True:
                        handle.seek(0)
                        try:
                            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                            break
                        except OSError:
                            if time.monotonic() >= deadline:
                                raise ArtifactPolicyError("timed out waiting for artifact store access") from None
                            time.sleep(0.01)
                else:
                    import fcntl
                    while True:
                        try:
                            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                            break
                        except BlockingIOError:
                            if time.monotonic() >= deadline:
                                raise ArtifactPolicyError("timed out waiting for artifact store access") from None
                            time.sleep(0.01)
                self.local.depth = 1
                try:
                    yield
                finally:
                    self.local.depth = 0
                    if os.name == "nt":
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.lock.release()


def _store_operation(method):
    @functools.wraps(method)
    def guarded(self, *args, **kwargs):
        with self._store_lock.acquire():
            return method(self, *args, **kwargs)
    return guarded


class FileArtifactStore:
    """Filesystem artifact store with atomic writes and optional streaming crypto.

    ``put()`` and ``read()`` preserve the stable 1.x bytes contract. Callers
    handling large bodies should use ``put_stream()`` and ``read_stream()``;
    those methods never intentionally retain the complete payload in Python
    memory and require a provider with the v2 streaming capability when
    encryption is enabled.
    """

    def __init__(
        self,
        root: str | Path,
        default_ttl_days: int | None = None,
        *,
        policy: ArtifactPolicy | None = None,
        encryption_provider: EncryptionProvider | None = None,
        authorizer: ArtifactAuthorizer | None = None,
        media_metadata_extractor: MediaMetadataExtractor | None = None,
    ):
        self.root = Path(root).absolute()
        self.policy = policy or ArtifactPolicy()
        self.default_ttl_days = (
            self.policy.retention_days
            if self.policy.retention_days is not None
            else default_ttl_days
        )
        self.encryption_provider = encryption_provider
        self.authorizer = authorizer
        self.media_metadata_extractor = media_metadata_extractor
        if self.policy.encryption == "provider" and encryption_provider is None:
            raise ValueError("artifact policy requires an encryption_provider")
        if media_metadata_extractor is not None and not callable(
            getattr(media_metadata_extractor, "extract", None)
        ):
            raise TypeError("media_metadata_extractor must provide extract(path, media_type)")

        self.root.mkdir(parents=True, exist_ok=True)
        _reject_link_or_reparse(self.root, "artifact root")
        self.artifacts_dir = self.root / "artifacts"
        self.leases_dir = self.root / "artifact-leases"
        self._mkdir_secure(self.artifacts_dir)
        self._mkdir_secure(self.leases_dir)

        identity = str(self.root.resolve())
        with _STORE_LOCKS_GUARD:
            self._store_lock = _STORE_LOCKS.setdefault(identity, _StoreLock(self.leases_dir / "store.lock"))
        self._usage_by_run: dict[str, int] = {}
        self._usage_lock = threading.Lock()

    @_store_operation
    def put(
        self,
        value: Any,
        *,
        media_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        preview: str | None = None,
        redacted: bool = False,
        media_metadata: MediaMetadata | dict[str, Any] | None = None,
        provenance: ArtifactProvenance | dict[str, Any] | None = None,
    ) -> ArtifactRef:
        """Persist a complete value through the stable 1.x bytes provider SPI."""

        payload, inferred_media_type, suffix = _serialize_artifact(value, media_type)
        self._validate_write_policy(inferred_media_type)
        normalized_media_metadata = _coerce_media_metadata(media_metadata)
        normalized_provenance = _coerce_provenance(provenance)
        run_id = self._run_id_from_metadata(metadata)
        reserved = len(payload)
        self._reserve_usage(run_id, reserved)
        try:
            plaintext_digest = sha256(payload).hexdigest()
            stored_payload = payload
            confidentiality: dict[str, Any] = self._base_confidentiality()
            if self.encryption_provider is not None:
                stored_payload = self.encryption_provider.encrypt(
                    payload, context=dict(metadata or {})
                )
                confidentiality.update(
                    {
                        "encrypted": True,
                        "provider_id": self.encryption_provider.provider_id,
                        "key_version": getattr(self.encryption_provider, "key_version", None),
                        "streaming": False,
                    }
                )
            return self._persist_bytes(
                stored_payload,
                media_type=inferred_media_type,
                suffix=suffix,
                plaintext_digest=plaintext_digest,
                plaintext_size=len(payload),
                metadata=metadata,
                # Artifact sidecars are not encrypted by an encryption provider.
                # Never put plaintext-derived previews in them when ciphertext is
                # the requested storage mode; callers can still return a bounded,
                # redacted preview directly to the current model invocation.
                preview=(
                    None
                    if self.encryption_provider is not None
                    else preview if preview is not None else _preview_bytes(payload)
                ),
                redacted=redacted,
                confidentiality=confidentiality,
                media_metadata=normalized_media_metadata,
                provenance=normalized_provenance,
            )
        except Exception:
            self._release_usage(run_id, reserved)
            raise

    @_store_operation
    def put_stream(
        self,
        source: BinaryIO,
        *,
        media_type: str = "application/octet-stream",
        metadata: dict[str, Any] | None = None,
        preview: str | None = None,
        redacted: bool = False,
        suffix: str = ".bin",
        media_metadata: MediaMetadata | dict[str, Any] | None = None,
        provenance: ArtifactProvenance | dict[str, Any] | None = None,
    ) -> ArtifactRef:
        """Persist a bounded-read binary stream without materializing it in memory.

        When an encryption provider is configured, it must expose
        ``encrypt_stream()`` and ``algorithm``. The provider is responsible
        for authenticated encryption; ActionLens persists only non-secret
        metadata and independently hashes both plaintext and ciphertext.
        """

        if not callable(getattr(source, "read", None)):
            raise TypeError("source must be a binary stream with a read() method")
        self._validate_write_policy(media_type)
        suffix = _validate_suffix(suffix)
        normalized_media_metadata = _coerce_media_metadata(media_metadata)
        normalized_provenance = _coerce_provenance(provenance)
        provider = self._streaming_provider(required=self.encryption_provider is not None)
        run_id = self._run_id_from_metadata(metadata)
        reserved = 0

        def reserve_bytes(size: int) -> None:
            nonlocal reserved
            self._reserve_usage(run_id, size)
            reserved += size

        plaintext = _HashingReader(source, on_chunk=reserve_bytes, capture_limit=240)
        temporary: Path | None = None
        descriptor: int | None = None
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=".actionlens-stream-", suffix=".tmp", dir=self.artifacts_dir
            )
            temporary = Path(temporary_name)
            with os.fdopen(descriptor, "wb") as handle:
                descriptor = None
                ciphertext = _HashingWriter(handle)
                encryption_metadata: EncryptionMetadata | None = None
                if provider is None:
                    _copy_stream(plaintext, ciphertext)
                else:
                    result = provider.encrypt_stream(
                        plaintext, ciphertext, context=dict(metadata or {})
                    )
                    encryption_metadata = self._validate_stream_metadata(provider, result)
                _require_consumed(plaintext)
                handle.flush()
                os.fsync(handle.fileno())

            path = self._target_path(ciphertext.hexdigest, suffix)
            _promote_tempfile(temporary, path)
            confidentiality = self._base_confidentiality()
            if encryption_metadata is not None:
                confidentiality.update(
                    {
                        "encrypted": True,
                        "provider_id": provider.provider_id,
                        "streaming": True,
                        **encryption_metadata.as_dict(),
                    }
                )
            else:
                confidentiality["streaming"] = True
            resolved_media_metadata = self._resolve_media_metadata(
                path=path,
                media_type=media_type,
                provided=normalized_media_metadata,
                encrypted=provider is not None,
            )
            artifact = self._build_artifact(
                path=path,
                media_type=media_type,
                ciphertext_digest=ciphertext.hexdigest,
                ciphertext_size=ciphertext.size,
                plaintext_digest=plaintext.hexdigest,
                plaintext_size=plaintext.size,
                preview=(
                    None
                    if self.encryption_provider is not None
                    else (
                        preview
                        if preview is not None
                        else _stream_preview(
                            plaintext.preview, truncated=plaintext.preview_truncated
                        )
                    )
                ),
                redacted=redacted,
                confidentiality=confidentiality,
                media_metadata=resolved_media_metadata,
                provenance=normalized_provenance,
            )
            self._write_metadata(path, artifact, metadata)
            return artifact
        except Exception:
            self._release_usage(run_id, reserved)
            raise
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def validate_reference(self, artifact: ArtifactRef) -> None:
        parts = urlsplit(artifact.uri)
        if parts.username or parts.password or parts.query or parts.fragment:
            raise ArtifactPolicyError("artifact URI must not contain credentials, query, or fragment")
        if parts.scheme.lower() not in {item.lower() for item in self.policy.reference_schemes}:
            raise ArtifactPolicyError(f"artifact URI scheme {parts.scheme!r} is not allowed")

    def read(
        self, artifact: ArtifactRef, *, context: dict[str, Any] | None = None
    ) -> bytes:
        """Read an artifact into memory through the stable 1.x convenience API."""

        destination = io.BytesIO()
        self.read_stream(artifact, destination, context=context)
        return destination.getvalue()

    @_store_operation
    def read_range(
        self,
        artifact: ArtifactRef,
        *,
        offset: int = 0,
        limit: int = 4096,
        context: dict[str, Any] | None = None,
    ) -> bytes:
        """Return a verified, bounded plaintext range from an artifact.

        Plain local artifacts are checksummed while being streamed, so callers
        can page through a large result without materializing its full body.
        Encrypted legacy providers retain their existing whole-payload provider
        contract; streaming providers continue to use the verified streaming
        path and only retain the requested range in memory.
        """

        if offset < 0:
            raise ValueError("offset must not be negative")
        if limit < 0:
            raise ValueError("limit must not be negative")
        access_context = dict(context or {})
        artifact = self._trusted_artifact(artifact)
        self._authorize_read(artifact, access_context)
        path = self._local_path(artifact)
        if (
            not artifact.confidentiality.get("encrypted")
            and not artifact.confidentiality.get("streaming")
        ):
            return self._read_plain_range(path, artifact, offset=offset, limit=limit)

        destination = _RangeWriter(offset=offset, limit=limit)
        self.read_stream(artifact, destination, context=access_context)
        return destination.value

    @_store_operation
    def read_stream(
        self,
        artifact: ArtifactRef,
        destination: BinaryIO,
        *,
        context: dict[str, Any] | None = None,
    ) -> int:
        """Verify and copy an artifact to a binary destination.

        Streamed artifacts are first decrypted to a private temporary file and
        fully checksum-verified. This prevents plaintext from reaching the
        caller's destination when authentication or integrity checks fail.
        ``read_to_path()`` additionally provides atomic replacement for a
        filesystem destination.
        """

        if not callable(getattr(destination, "write", None)):
            raise TypeError("destination must be a binary stream with a write() method")
        access_context = dict(context or {})
        artifact = self._trusted_artifact(artifact)
        self._authorize_read(artifact, access_context)
        path = self._local_path(artifact)
        if not artifact.confidentiality.get("streaming"):
            payload = self._read_legacy(path, artifact, access_context)
            _write_chunk(destination, payload)
            return len(payload)
        return self._read_streamed(path, artifact, destination, access_context)

    def read_to_path(
        self,
        artifact: ArtifactRef,
        destination: str | Path,
        *,
        context: dict[str, Any] | None = None,
    ) -> int:
        """Write a verified artifact to a path using fsync plus atomic replace."""

        target = Path(destination).absolute()
        target.parent.mkdir(parents=True, exist_ok=True)
        _reject_link_or_reparse(target.parent, "artifact destination directory")
        if target.exists():
            _reject_link_or_reparse(target, "artifact destination")
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.", dir=target.parent
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                written = self.read_stream(artifact, handle, context=context)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
            _fsync_directory(target.parent)
            return written
        finally:
            temporary.unlink(missing_ok=True)

    def _authorize_read(self, artifact: ArtifactRef, access_context: dict[str, Any]) -> None:
        for field, value in artifact.access_scope.items():
            if field in {"project", "environment", "tenant_id"} and value is not None and access_context.get(field) != value:
                raise ArtifactAccessDenied("artifact ownership scope does not match the reader")
        if self.authorizer is None:
            raise ArtifactAccessDenied("artifact reads require an ArtifactAuthorizer")
        if not self.authorizer.authorize(artifact, context=access_context):
            raise ArtifactAccessDenied("artifact access was denied")
        if artifact.expires_at is not None and artifact.expires_at <= datetime.now(timezone.utc):
            raise ArtifactAccessDenied("artifact has expired")

    def _read_legacy(
        self, path: Path, artifact: ArtifactRef, access_context: dict[str, Any]
    ) -> bytes:
        with self._artifact_lease(path):
            payload = _read_file_no_follow(path)
        if sha256(payload).hexdigest() != artifact.sha256:
            raise ArtifactPolicyError("artifact ciphertext checksum mismatch")
        if artifact.confidentiality.get("encrypted"):
            decrypt = getattr(self.encryption_provider, "decrypt", None)
            if decrypt is None:
                raise ArtifactPolicyError("encryption provider does not support decrypt")
            payload = decrypt(payload, context=access_context)
        expected_plaintext = artifact.confidentiality.get("plaintext_sha256")
        if expected_plaintext and sha256(payload).hexdigest() != expected_plaintext:
            raise ArtifactPolicyError("artifact plaintext checksum mismatch")
        return payload

    def _read_plain_range(
        self, path: Path, artifact: ArtifactRef, *, offset: int, limit: int
    ) -> bytes:
        digest = sha256()
        captured = bytearray()
        position = 0
        with self._artifact_lease(path), _open_file_no_follow(path) as source:
            while chunk := source.read(_STREAM_CHUNK_BYTES):
                digest.update(chunk)
                chunk_start = position
                chunk_end = position + len(chunk)
                capture_start = max(offset, chunk_start)
                capture_end = min(offset + limit, chunk_end)
                if capture_start < capture_end:
                    start_index = capture_start - chunk_start
                    end_index = capture_end - chunk_start
                    captured.extend(chunk[start_index:end_index])
                position = chunk_end
        if digest.hexdigest() != artifact.sha256:
            raise ArtifactPolicyError("artifact ciphertext checksum mismatch")
        expected_plaintext = artifact.confidentiality.get("plaintext_sha256")
        if expected_plaintext and digest.hexdigest() != expected_plaintext:
            raise ArtifactPolicyError("artifact plaintext checksum mismatch")
        return bytes(captured)

    def _read_streamed(
        self,
        path: Path,
        artifact: ArtifactRef,
        destination: BinaryIO,
        access_context: dict[str, Any],
    ) -> int:
        encrypted = bool(artifact.confidentiality.get("encrypted"))
        provider = self._streaming_provider(required=encrypted)
        if encrypted:
            expected_algorithm = artifact.confidentiality.get("algorithm")
            if expected_algorithm != provider.algorithm:
                raise ArtifactPolicyError("streaming encryption provider algorithm does not match artifact")

        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".actionlens-read-", suffix=".tmp", dir=self.artifacts_dir
        )
        temporary = Path(temporary_name)
        try:
            with (
                self._artifact_lease(path),
                _open_file_no_follow(path) as source,
                os.fdopen(descriptor, "wb") as handle,
            ):
                ciphertext = _HashingReader(source)
                plaintext = _HashingWriter(handle)
                if encrypted:
                    assert provider is not None
                    provider.decrypt_stream(
                        ciphertext,
                        plaintext,
                        context=self._decryption_context(access_context, artifact),
                    )
                else:
                    _copy_stream(ciphertext, plaintext)
                _require_consumed(ciphertext)
                handle.flush()
                os.fsync(handle.fileno())

            if ciphertext.hexdigest != artifact.sha256:
                raise ArtifactPolicyError("artifact ciphertext checksum mismatch")
            expected_plaintext = artifact.confidentiality.get("plaintext_sha256")
            if expected_plaintext and plaintext.hexdigest != expected_plaintext:
                raise ArtifactPolicyError("artifact plaintext checksum mismatch")
            expected_size = artifact.confidentiality.get("plaintext_size_bytes")
            if expected_size is not None and int(expected_size) != plaintext.size:
                raise ArtifactPolicyError("artifact plaintext size mismatch")
            with temporary.open("rb") as verified:
                _copy_stream(verified, destination)
            return plaintext.size
        finally:
            temporary.unlink(missing_ok=True)

    def _decryption_context(
        self, access_context: dict[str, Any], artifact: ArtifactRef
    ) -> dict[str, Any]:
        context = dict(access_context)
        context["_actionlens_artifact"] = {
            "sha256": artifact.sha256,
            "confidentiality": dict(artifact.confidentiality),
        }
        return context

    def _streaming_provider(self, *, required: bool) -> Any | None:
        provider = self.encryption_provider
        if provider is None:
            return None
        has_capability = all(
            callable(getattr(provider, method, None))
            for method in ("encrypt_stream", "decrypt_stream")
        ) and isinstance(getattr(provider, "algorithm", None), str)
        if has_capability:
            return provider
        if required:
            raise ArtifactPolicyError(
                "put_stream/read_stream with encryption requires a StreamingEncryptionProvider"
            )
        return None

    def _validate_stream_metadata(
        self, provider: Any, result: Any
    ) -> EncryptionMetadata:
        if not isinstance(result, EncryptionMetadata):
            raise ArtifactPolicyError(
                "encrypt_stream must return an EncryptionMetadata instance"
            )
        if result.algorithm != provider.algorithm:
            raise ArtifactPolicyError(
                "encryption metadata algorithm must match the provider algorithm"
            )
        return result

    def _validate_write_policy(self, media_type: str) -> None:
        if self.policy.raw_mode == "deny":
            raise ArtifactPolicyError("artifact storage is denied by policy")
        if self.policy.raw_mode == "reference_only":
            raise ArtifactPolicyError("reference_only policy accepts ArtifactRef values only")
        if not media_type:
            raise ValueError("media_type must not be empty")
        if self.policy.allowed_media_types is not None and not any(
            media_type == allowed or media_type.startswith(allowed + ";")
            for allowed in self.policy.allowed_media_types
        ):
            raise ArtifactPolicyError(f"media type {media_type!r} is not allowed")

    def _base_confidentiality(self) -> dict[str, Any]:
        confidentiality = {
            "raw_mode": self.policy.raw_mode,
            "policy_id": self.policy.policy_id,
        }
        if self.encryption_provider is not None:
            confidentiality["preview_withheld"] = True
        return confidentiality

    def _persist_bytes(
        self,
        stored_payload: bytes,
        *,
        media_type: str,
        suffix: str,
        plaintext_digest: str,
        plaintext_size: int,
        metadata: dict[str, Any] | None,
        preview: str | None,
        redacted: bool,
        confidentiality: dict[str, Any],
        media_metadata: MediaMetadata | None,
        provenance: ArtifactProvenance | None,
    ) -> ArtifactRef:
        ciphertext_digest = sha256(stored_payload).hexdigest()
        path = self._target_path(ciphertext_digest, suffix)
        if not path.exists():
            _atomic_write_if_absent(path, stored_payload)
        resolved_media_metadata = self._resolve_media_metadata(
            path=path,
            media_type=media_type,
            provided=media_metadata,
            encrypted=self.encryption_provider is not None,
        )
        artifact = self._build_artifact(
            path=path,
            media_type=media_type,
            ciphertext_digest=ciphertext_digest,
            ciphertext_size=len(stored_payload),
            plaintext_digest=plaintext_digest,
            plaintext_size=plaintext_size,
            preview=preview,
            redacted=redacted,
            confidentiality=confidentiality,
            media_metadata=resolved_media_metadata,
            provenance=provenance,
        )
        self._write_metadata(path, artifact, metadata)
        return artifact

    def _build_artifact(
        self,
        *,
        path: Path,
        media_type: str,
        ciphertext_digest: str,
        ciphertext_size: int,
        plaintext_digest: str,
        plaintext_size: int,
        preview: str | None,
        redacted: bool,
        confidentiality: dict[str, Any],
        media_metadata: MediaMetadata | None,
        provenance: ArtifactProvenance | None,
    ) -> ArtifactRef:
        created_at = datetime.now(timezone.utc)
        expires_at = None
        if self.default_ttl_days is not None:
            expires_at = created_at + timedelta(days=self.default_ttl_days)
        return ArtifactRef(
            uri=str(path.resolve()),
            media_type=media_type,
            size_bytes=ciphertext_size,
            sha256=ciphertext_digest,
            preview=preview,
            redacted=redacted,
            created_at=created_at,
            expires_at=expires_at,
            confidentiality={
                **confidentiality,
                "plaintext_sha256": plaintext_digest,
                "plaintext_size_bytes": plaintext_size,
                "ciphertext_size_bytes": ciphertext_size,
            },
            media_metadata=media_metadata,
            provenance=provenance,
            descriptor_id=uuid4().hex,
        )

    def _resolve_media_metadata(
        self,
        *,
        path: Path,
        media_type: str,
        provided: MediaMetadata | None,
        encrypted: bool,
    ) -> MediaMetadata | None:
        if provided is not None:
            return provided
        if (
            encrypted
            or self.media_metadata_extractor is None
            or not _is_extractable_media_type(media_type)
        ):
            return None
        try:
            extracted = self.media_metadata_extractor.extract(path, media_type)
            return _coerce_media_metadata(extracted)
        except Exception:
            # Metadata is auxiliary evidence. A faulty host extractor must not
            # turn a completed, governed artifact write into an orphaned file.
            logger.warning(
                "media metadata extractor %s failed for media type %s; "
                "the artifact was stored without extracted metadata",
                type(self.media_metadata_extractor).__qualname__,
                media_type,
                exc_info=True,
            )
            return None

    def _write_metadata(
        self, path: Path, artifact: ArtifactRef, metadata: dict[str, Any] | None
    ) -> None:
        if artifact.descriptor_id is not None:
            artifact.access_scope = {
                key: (None if value is None else str(value))
                for key, value in (metadata or {}).items()
                if key in {"project", "environment", "tenant_id"}
            }
            descriptor = path.with_name(f"{path.name}.{artifact.descriptor_id}.ref.meta.json")
            _atomic_write(descriptor, artifact.model_dump_json().encode("utf-8"))
        meta_path = path.with_suffix(path.suffix + ".meta.json")
        # A completed, ordinary sidecar needs no mutation. Keep that common
        # deduplication path lock-free, but serialize initialization and every
        # sidecar mutation below. Otherwise two workers can both observe a
        # missing sidecar and race on os.replace() on Windows.
        requires_mutation = artifact.provenance is not None or bool(
            artifact.confidentiality.get("preview_withheld")
        )
        if not requires_mutation and meta_path.exists():
            return
        with self._metadata_lease(path):
            if meta_path.exists():
                if artifact.provenance is None:
                    self._scrub_withheld_preview(meta_path, artifact)
                    return
                try:
                    existing_payload = json.loads(meta_path.read_text(encoding="utf-8"))
                except (OSError, UnicodeError, TypeError, ValueError):
                    # Do not overwrite an existing unreadable sidecar. The file is
                    # still valid content-addressed storage; replacing its evidence
                    # would be more surprising than omitting a later duplicate.
                    return
                if not isinstance(existing_payload, dict):
                    return
                changed = _append_sidecar_provenance(existing_payload, artifact.provenance)
                changed = self._withhold_preview(existing_payload, artifact) or changed
                if changed:
                    _atomic_write(
                        meta_path,
                        json.dumps(existing_payload, ensure_ascii=False, indent=2).encode(
                            "utf-8"
                        ),
                    )
                return
            meta_payload = artifact.model_dump(mode="json")
            if artifact.provenance is not None:
                meta_payload["provenance_records"] = [artifact.provenance.model_dump(mode="json")]
            if metadata:
                # Keep caller metadata out of the signed artifact identity fields.
                # The former flat merge allowed a caller to overwrite ``sha256``,
                # ``uri``, or newly added provenance fields in the sidecar.
                meta_payload["metadata"] = dict(metadata)
            _atomic_write(
                meta_path,
                json.dumps(meta_payload, ensure_ascii=False, indent=2).encode("utf-8"),
            )

    def _scrub_withheld_preview(self, meta_path: Path, artifact: ArtifactRef) -> None:
        """Remove a pre-v1.5 plaintext preview while holding the metadata lease."""

        if not artifact.confidentiality.get("preview_withheld"):
            return
        try:
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, TypeError, ValueError):
            return
        if not isinstance(payload, dict) or not self._withhold_preview(payload, artifact):
            return
        _atomic_write(
            meta_path,
            json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        )

    @staticmethod
    def _withhold_preview(payload: dict[str, Any], artifact: ArtifactRef) -> bool:
        if not artifact.confidentiality.get("preview_withheld"):
            return False
        changed = payload.get("preview") is not None
        if changed:
            payload["preview"] = None
        confidentiality = payload.get("confidentiality")
        if isinstance(confidentiality, dict) and not confidentiality.get("preview_withheld"):
            confidentiality["preview_withheld"] = True
            changed = True
        return changed

    def _target_path(self, digest: str, suffix: str) -> Path:
        suffix = _validate_suffix(suffix)
        self._mkdir_secure(self.artifacts_dir)
        target_dir = self.artifacts_dir / digest[:2]
        self._mkdir_secure(target_dir)
        path = target_dir / f"{digest}{suffix}"
        for candidate in (self.artifacts_dir, target_dir, path):
            if candidate.exists():
                _reject_link_or_reparse(candidate, "artifact storage path")
        return path

    def _mkdir_secure(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        _reject_link_or_reparse(path, "artifact storage path")

    def _run_id_from_metadata(self, metadata: dict[str, Any] | None) -> str:
        value = (metadata or {}).get("run_id")
        run_id = "" if value is None else str(value).strip()
        if self.policy.max_bytes_per_run is not None and not run_id:
            raise ArtifactPolicyError(
                "max_bytes_per_run requires a non-empty metadata['run_id']"
            )
        scope = metadata or {}
        if any(key in scope for key in ("project", "environment", "tenant_id")):
            return json.dumps([scope.get("project"), scope.get("environment"), scope.get("tenant_id"), run_id], separators=(",", ":"))
        return run_id

    def reset_run(self, run_id: str, *, metadata: dict[str, Any] | None = None) -> None:
        """Release in-memory byte accounting after the host completes a run.

        This never deletes stored artifacts. Call it only after all writers for
        the durable run have completed; a resumed run must continue to share
        its accumulated budget until its actual lifecycle has ended.
        """

        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("run_id must be a non-empty string")
        run_id = run_id.strip()
        with self._usage_lock:
            if metadata is not None:
                self._usage_by_run.pop(self._run_id_from_metadata({**metadata, "run_id": run_id}), None)
                return
            for key in list(self._usage_by_run):
                if key == run_id or (key.startswith("[") and json.loads(key)[-1] == run_id):
                    self._usage_by_run.pop(key, None)

    @property
    def tracked_run_count(self) -> int:
        """Return the number of runs currently retained for quota accounting."""

        with self._usage_lock:
            return len(self._usage_by_run)

    def _reserve_usage(self, run_id: str, size: int) -> None:
        if size < 0:
            raise ValueError("artifact byte count must not be negative")
        if size == 0 or self.policy.max_bytes_per_run is None:
            return
        with self._usage_lock:
            used = self._usage_by_run.get(run_id, 0)
            if (
                self.policy.max_bytes_per_run is not None
                and used + size > self.policy.max_bytes_per_run
            ):
                raise ArtifactPolicyError("artifact byte budget exceeded for run")
            self._usage_by_run[run_id] = used + size

    def _release_usage(self, run_id: str, size: int) -> None:
        if size == 0 or self.policy.max_bytes_per_run is None:
            return
        with self._usage_lock:
            remaining = self._usage_by_run.get(run_id, 0) - size
            if remaining > 0:
                self._usage_by_run[run_id] = remaining
            else:
                self._usage_by_run.pop(run_id, None)

    def _local_path(self, artifact: ArtifactRef) -> Path:
        parts = urlsplit(artifact.uri)
        windows_drive_path = len(artifact.uri) >= 3 and artifact.uri[1] == ":"
        if (
            not windows_drive_path
            and (parts.scheme not in {"", "file"} or parts.query or parts.fragment or parts.netloc)
        ):
            raise ArtifactPolicyError("artifact is not a safe local URI")
        path_text = artifact.uri if windows_drive_path or not parts.scheme else unquote(parts.path)
        if os.name == "nt" and parts.scheme == "file" and re.match(r"^/[A-Za-z]:[/\\]", path_text):
            path_text = path_text[1:]
        if "\x00" in path_text:
            raise ArtifactPolicyError("artifact path contains a null byte")
        path = Path(path_text)
        if ".." in path.parts:
            raise ArtifactPolicyError("artifact path must not contain parent segments")
        path = path.absolute()
        root = self.artifacts_dir.resolve()
        try:
            relative = path.relative_to(root)
        except ValueError as exc:
            raise ArtifactPolicyError("artifact path escapes the configured store") from exc
        components = [root]
        for part in relative.parts:
            components.append(components[-1] / part)
        for component in components:
            if component.exists():
                _reject_link_or_reparse(component, "artifact path")
        if path.resolve() != path:
            raise ArtifactPolicyError("artifact path changed during validation")
        return path

    def _trusted_artifact(self, artifact: ArtifactRef) -> ArtifactRef:
        path = self._local_path(artifact)
        if artifact.descriptor_id is not None:
            if not re.fullmatch(r"[0-9a-f]{32}", artifact.descriptor_id):
                raise ArtifactPolicyError("invalid artifact descriptor identity")
            descriptor = path.with_name(f"{path.name}.{artifact.descriptor_id}.ref.meta.json")
        else:
            descriptor = path.with_suffix(path.suffix + ".meta.json")
        try:
            trusted = ArtifactRef.model_validate_json(_read_file_no_follow(descriptor))
        except (OSError, ValueError) as exc:
            raise ArtifactPolicyError("trusted artifact descriptor is missing or invalid") from exc
        if (trusted.sha256 != artifact.sha256 or self._local_path(trusted) != path
                or trusted.descriptor_id != artifact.descriptor_id):
            raise ArtifactPolicyError("artifact reference does not match its stored identity")
        return trusted

    @contextmanager
    def _artifact_lease(self, path: Path) -> Iterator[None]:
        lease = self.leases_dir / f"{path.name}.{uuid4().hex}.lease"
        fd = os.open(lease, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        try:
            yield
        finally:
            lease.unlink(missing_ok=True)

    @contextmanager
    def _metadata_lease(self, path: Path) -> Iterator[None]:
        lease = self.leases_dir / f"{path.name}.metadata.lease"
        deadline = time.monotonic() + 5.0
        while True:
            try:
                fd = os.open(lease, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                break
            except FileExistsError as exc:
                try:
                    stale = time.time() - lease.stat().st_mtime > _LEASE_SECONDS
                except OSError:
                    stale = False
                if stale:
                    lease.unlink(missing_ok=True)
                    continue
                if time.monotonic() >= deadline:
                    raise ArtifactPolicyError(
                        "timed out waiting for artifact metadata lease"
                    ) from exc
                time.sleep(0.01)
        os.close(fd)
        try:
            yield
        finally:
            lease.unlink(missing_ok=True)

    @contextmanager
    def _gc_lease(self) -> Iterator[None]:
        lock = self.leases_dir / "gc.lock"
        try:
            fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            try:
                stale = time.time() - lock.stat().st_mtime > _LEASE_SECONDS
            except OSError:
                stale = False
            if not stale:
                raise ArtifactPolicyError("artifact GC is already running") from exc
            lock.unlink(missing_ok=True)
            try:
                fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError as retry_exc:
                raise ArtifactPolicyError("artifact GC is already running") from retry_exc
        os.close(fd)
        try:
            yield
        finally:
            lock.unlink(missing_ok=True)

    @_store_operation
    def gc(
        self,
        *,
        older_than_seconds: int,
        max_bytes: int | None = None,
        dry_run: bool = False,
        cascade_derived: bool = False,
    ) -> dict[str, int]:
        if older_than_seconds < 0:
            raise ValueError("older_than_seconds must not be negative")
        if max_bytes is not None and max_bytes < 0:
            raise ValueError("max_bytes must not be negative")
        with self._gc_lease():
            return self._gc_locked(
                older_than_seconds=older_than_seconds,
                max_bytes=max_bytes,
                dry_run=dry_run,
                cascade_derived=cascade_derived,
            )

    def _gc_locked(
        self,
        *,
        older_than_seconds: int,
        max_bytes: int | None,
        dry_run: bool,
        cascade_derived: bool,
    ) -> dict[str, int]:
        now = datetime.now(timezone.utc).timestamp()
        cutoff = now - older_than_seconds
        candidates: list[Path] = []
        total_size = 0
        files: list[tuple[float, int, Path]] = []
        stale_temporary_files = 0
        for path in self.artifacts_dir.glob("**/*"):
            try:
                info = path.lstat()
            except OSError:
                continue
            if _is_link_or_reparse(info):
                continue
            if not stat.S_ISREG(info.st_mode):
                continue
            if path.name.endswith(".meta.json"):
                continue
            if path.name.startswith(".actionlens-") and path.name.endswith(".tmp"):
                if now - info.st_mtime > _LEASE_SECONDS:
                    stale_temporary_files += 1
                    if not dry_run:
                        path.unlink(missing_ok=True)
                continue
            total_size += info.st_size
            files.append((info.st_mtime, info.st_size, path))
            if info.st_mtime < cutoff:
                candidates.append(path)
        if max_bytes is not None and total_size > max_bytes:
            selected = {path for path in candidates}
            remaining = total_size - sum(path.stat().st_size for path in selected if path.exists())
            for _, size, path in sorted(files):
                if remaining <= max_bytes:
                    break
                if path in selected:
                    continue
                candidates.append(path)
                selected.add(path)
                remaining -= size

        all_files = {path for _, _, path in files}
        candidate_paths = set(candidates)
        candidate_paths = {
            path
            for path in candidate_paths
            if not self._has_active_gc_lease(path, now=now, dry_run=dry_run)
        }
        lineage_cascaded = 0
        lineage_protected = 0
        if candidate_paths:
            lineage_parents = self._lineage_parents(all_files)
            if cascade_derived:
                candidate_paths, lineage_cascaded = self._cascade_derived_candidates(
                    candidate_paths, lineage_parents
                )
                candidate_paths = {
                    path
                    for path in candidate_paths
                    if not self._has_active_gc_lease(path, now=now, dry_run=dry_run)
                }
            candidate_paths, lineage_protected = self._protect_lineage_sources(
                candidate_paths, all_files, lineage_parents
            )

        deleted = 0
        bytes_deleted = 0
        for path in sorted(candidate_paths, key=lambda item: str(item)):
            if self._has_active_gc_lease(path, now=now, dry_run=dry_run):
                continue
            try:
                size = path.stat().st_size
            except OSError:
                continue
            bytes_deleted += size
            deleted += 1
            if dry_run:
                continue
            path.unlink(missing_ok=True)
            meta_path = path.with_suffix(path.suffix + ".meta.json")
            meta_path.unlink(missing_ok=True)
            for descriptor in path.parent.glob(f"{path.name}.*.ref.meta.json"):
                descriptor.unlink(missing_ok=True)
        for directory in sorted(self.artifacts_dir.glob("**/*"), reverse=True):
            if directory.is_dir():
                with suppress(OSError):
                    directory.rmdir()
        return {
            "deleted": 0 if dry_run else deleted,
            "would_delete": deleted if dry_run else 0,
            "bytes_deleted": 0 if dry_run else bytes_deleted,
            "bytes_would_delete": bytes_deleted if dry_run else 0,
            "temporary_deleted": 0 if dry_run else stale_temporary_files,
            "temporary_would_delete": stale_temporary_files if dry_run else 0,
            "lineage_protected": lineage_protected,
            "lineage_cascaded": lineage_cascaded,
        }

    def _has_active_gc_lease(self, path: Path, *, now: float, dry_run: bool) -> bool:
        active_lease = False
        for lease in self.leases_dir.glob(f"{path.name}.*.lease"):
            try:
                if now - lease.stat().st_mtime <= _LEASE_SECONDS:
                    active_lease = True
                elif not dry_run:
                    lease.unlink(missing_ok=True)
            except OSError:
                active_lease = True
        return active_lease

    def _lineage_parents(self, files: set[Path]) -> dict[Path, set[Path]]:
        parents: dict[Path, set[Path]] = {}
        for child in files:
            meta_path = child.with_suffix(child.suffix + ".meta.json")
            try:
                payload = json.loads(meta_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, TypeError, ValueError):
                continue
            if not isinstance(payload, dict):
                continue
            for provenance in _sidecar_provenance_records(payload):
                source_ref = provenance.get("source_ref")
                if not isinstance(source_ref, dict):
                    continue
                source_uri = source_ref.get("uri")
                source_sha256 = source_ref.get("sha256")
                if not isinstance(source_uri, str) or not isinstance(source_sha256, str):
                    continue
                source = self._local_provenance_path(source_uri)
                if (
                    source is None
                    or source == child
                    or source not in files
                    or not source.name.startswith(source_sha256 + ".")
                ):
                    continue
                parents.setdefault(child, set()).add(source)
        return parents

    def _local_provenance_path(self, uri: str) -> Path | None:
        parts = urlsplit(uri)
        windows_drive_path = len(uri) >= 3 and uri[1] == ":"
        if (
            not windows_drive_path
            and (parts.scheme not in {"", "file"} or parts.query or parts.fragment or parts.netloc)
        ):
            return None
        path = Path(uri if windows_drive_path or not parts.scheme else parts.path).resolve()
        try:
            path.relative_to(self.artifacts_dir.resolve())
        except ValueError:
            return None
        return path

    @staticmethod
    def _cascade_derived_candidates(
        candidates: set[Path], parents: dict[Path, set[Path]]
    ) -> tuple[set[Path], int]:
        children: dict[Path, set[Path]] = {}
        for child, sources in parents.items():
            for source in sources:
                children.setdefault(source, set()).add(child)
        result = set(candidates)
        pending = list(candidates)
        added = 0
        while pending:
            source = pending.pop()
            for child in children.get(source, set()):
                if child in result or not parents[child].issubset(result):
                    continue
                result.add(child)
                pending.append(child)
                added += 1
        return result, added

    @staticmethod
    def _protect_lineage_sources(
        candidates: set[Path], files: set[Path], parents: dict[Path, set[Path]]
    ) -> tuple[set[Path], int]:
        result = set(candidates)
        retained = files - result
        protected = 0
        pending = list(retained)
        visited: set[Path] = set()
        while pending:
            child = pending.pop()
            if child in visited:
                continue
            visited.add(child)
            for source in parents.get(child, set()):
                if source not in result:
                    continue
                result.remove(source)
                retained.add(source)
                pending.append(source)
                protected += 1
        return result, protected

    @_store_operation
    def inspect(self, artifact: ArtifactRef) -> dict[str, Any]:
        try:
            path = self._local_path(artifact)
        except ArtifactPolicyError:
            return {"status": "invalid_uri", "expected_sha256": artifact.sha256}
        try:
            digest = sha256()
            size = 0
            with self._artifact_lease(path), _open_file_no_follow(path) as source:
                while chunk := source.read(_STREAM_CHUNK_BYTES):
                    digest.update(chunk)
                    size += len(chunk)
        except FileNotFoundError:
            return {"status": "missing", "expected_sha256": artifact.sha256}
        except PermissionError:
            return {"status": "permission_denied", "expected_sha256": artifact.sha256}
        actual = digest.hexdigest()
        return {
            "status": "ok" if actual == artifact.sha256 else "checksum_mismatch",
            "expected_sha256": artifact.sha256,
            "actual_sha256": actual,
            "size_bytes": size,
        }


class _HashingReader:
    def __init__(
        self,
        source: BinaryIO,
        *,
        on_chunk: Callable[[int], None] | None = None,
        capture_limit: int = 0,
    ) -> None:
        self._source = source
        self._digest = sha256()
        self._on_chunk = on_chunk
        self._capture_limit = capture_limit
        self._preview = bytearray()
        self.preview_truncated = False
        self.size = 0

    @property
    def hexdigest(self) -> str:
        return self._digest.hexdigest()

    @property
    def preview(self) -> bytes:
        return bytes(self._preview)

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            raise ArtifactPolicyError(
                "streaming providers must use bounded read sizes to preserve memory limits"
            )
        chunk = self._source.read(size)
        if chunk is None:
            return b""
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise TypeError("artifact stream must yield bytes")
        payload = bytes(chunk)
        if not payload:
            return payload
        if self._on_chunk is not None:
            self._on_chunk(len(payload))
        self._digest.update(payload)
        self.size += len(payload)
        if len(self._preview) < self._capture_limit:
            remaining = self._capture_limit - len(self._preview)
            self._preview.extend(payload[:remaining])
            self.preview_truncated = len(payload) > remaining
        elif payload:
            self.preview_truncated = True
        return payload

    def __getattr__(self, name: str) -> Any:
        return getattr(self._source, name)


class _HashingWriter:
    def __init__(self, destination: BinaryIO) -> None:
        self._destination = destination
        self._digest = sha256()
        self.size = 0

    @property
    def hexdigest(self) -> str:
        return self._digest.hexdigest()

    def write(self, data: bytes) -> int:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("artifact stream destinations accept bytes only")
        payload = bytes(data)
        _write_chunk(self._destination, payload)
        self._digest.update(payload)
        self.size += len(payload)
        return len(payload)

    def flush(self) -> None:
        flush = getattr(self._destination, "flush", None)
        if flush is not None:
            flush()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._destination, name)


class _RangeWriter:
    """A write-only destination which retains one requested byte range."""

    def __init__(self, *, offset: int, limit: int) -> None:
        self._offset = offset
        self._limit = limit
        self._position = 0
        self._captured = bytearray()

    @property
    def value(self) -> bytes:
        return bytes(self._captured)

    def write(self, data: bytes) -> int:
        payload = bytes(data)
        chunk_start = self._position
        chunk_end = chunk_start + len(payload)
        capture_start = max(self._offset, chunk_start)
        capture_end = min(self._offset + self._limit, chunk_end)
        if capture_start < capture_end:
            self._captured.extend(
                payload[capture_start - chunk_start:capture_end - chunk_start]
            )
        self._position = chunk_end
        return len(payload)


def _copy_stream(source: Any, destination: Any) -> None:
    while True:
        chunk = source.read(_STREAM_CHUNK_BYTES)
        if not chunk:
            return
        destination.write(chunk)


def _require_consumed(source: _HashingReader) -> None:
    if source.read(1):
        raise ArtifactPolicyError("streaming provider returned before consuming the complete source")


def _write_chunk(destination: BinaryIO, payload: bytes) -> None:
    written = destination.write(payload)
    if written is not None and written != len(payload):
        raise OSError("artifact destination accepted a partial write")


@contextmanager
def _open_file_no_follow(path: Path) -> Iterator[BinaryIO]:
    path = path.absolute()
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    directory_fds = []
    fd = None
    try:
        if os.name != "nt" and os.open in os.supports_dir_fd:
            parent = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
            directory_fds.append(parent)
            for part in path.parts[1:-1]:
                parent = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                directory_fds.append(parent)
            fd = os.open(path.name, flags | os.O_NONBLOCK, dir_fd=parent)
        else:
            for component in (*reversed(path.parents), path):
                _reject_link_or_reparse(component, "artifact open path")
            fd = os.open(path, flags)
            if os.name == "nt":
                import ctypes
                import msvcrt
                final_path = ctypes.windll.kernel32.GetFinalPathNameByHandleW
                final_path.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32]
                final_path.restype = ctypes.c_uint32
                buffer = ctypes.create_unicode_buffer(32768)
                length = final_path(msvcrt.get_osfhandle(fd), buffer, len(buffer), 0)
                actual = buffer.value.removeprefix("\\\\?\\")
                if not length or length >= len(buffer) or os.path.normcase(actual) != os.path.normcase(str(path)):
                    raise ArtifactPolicyError("opened artifact target differs from the validated path")
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ArtifactPolicyError("artifact must be a regular file")
        with os.fdopen(fd, "rb") as handle:
            fd = None
            yield handle
    except (FileNotFoundError, PermissionError):
        raise
    except OSError as exc:
        raise ArtifactPolicyError("artifact could not be opened safely") from exc
    finally:
        if fd is not None:
            os.close(fd)
        for directory_fd in reversed(directory_fds):
            os.close(directory_fd)


def _read_file_no_follow(path: Path) -> bytes:
    with _open_file_no_follow(path) as handle:
        return handle.read()


def _is_link_or_reparse(info: os.stat_result) -> bool:
    return stat.S_ISLNK(info.st_mode) or bool(
        getattr(info, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    )


def _reject_link_or_reparse(path: Path, label: str) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if _is_link_or_reparse(info):
        raise ArtifactPolicyError(f"{label} must not contain a symlink or reparse point")


def _serialize_artifact(
    value: Any, media_type: str | None
) -> tuple[bytes, str, str]:
    if isinstance(value, bytes):
        return value, media_type or "application/octet-stream", ".bin"
    if isinstance(value, str):
        return value.encode("utf-8"), media_type or "text/plain; charset=utf-8", ".txt"
    text = json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
    return text.encode("utf-8"), media_type or "application/json", ".json"


def _coerce_media_metadata(value: Any) -> MediaMetadata | None:
    if value is None:
        return None
    if isinstance(value, MediaMetadata):
        return value
    return MediaMetadata.model_validate(value)


def _coerce_provenance(value: Any) -> ArtifactProvenance | None:
    if value is None:
        return None
    if isinstance(value, ArtifactProvenance):
        return value
    return ArtifactProvenance.model_validate(value)


def _sidecar_provenance_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    top_level = payload.get("provenance")
    if isinstance(top_level, dict):
        records.append(top_level)
    stored_records = payload.get("provenance_records")
    if isinstance(stored_records, list):
        records.extend(record for record in stored_records if isinstance(record, dict))
    unique: list[dict[str, Any]] = []
    seen: set[str] = set()
    for record in records:
        identity = _provenance_record_identity(record)
        if identity is None:
            continue
        if identity not in seen:
            seen.add(identity)
            unique.append(record)
    return unique


def _provenance_record_identity(record: dict[str, Any]) -> str | None:
    # A trajectory already records each invocation. The sidecar instead keeps
    # lineage identities, so retries do not consume the bounded record budget
    # merely because their created_at timestamps differ.
    identity_record = {key: value for key, value in record.items() if key != "created_at"}
    try:
        return json.dumps(identity_record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        return None


def _append_sidecar_provenance(
    payload: dict[str, Any], provenance: ArtifactProvenance | None
) -> bool:
    if provenance is None:
        return False
    record = provenance.model_dump(mode="json")
    records = _sidecar_provenance_records(payload)
    record_identity = _provenance_record_identity(record)
    if record_identity is None:
        return False
    identities = {identity for item in records if (identity := _provenance_record_identity(item))}
    if record_identity not in identities:
        if len(records) >= _MAX_PROVENANCE_RECORDS_PER_ARTIFACT:
            raise ArtifactPolicyError(
                "provenance record limit reached for one content-addressed artifact"
            )
        records.append(record)
    changed = payload.get("provenance_records") != records
    if not isinstance(payload.get("provenance"), dict):
        payload["provenance"] = record
        changed = True
    if changed:
        payload["provenance_records"] = records
    return changed


def _is_extractable_media_type(media_type: str) -> bool:
    base_type = media_type.split(";", 1)[0].strip().lower()
    return base_type.startswith(("image/", "audio/", "video/"))


def _preview_bytes(payload: bytes, limit: int = 240) -> str:
    text = payload[:limit].decode("utf-8", errors="replace")
    if len(payload) > limit:
        return text + "...(artifact preview truncated)"
    return text


def _stream_preview(payload: bytes, *, truncated: bool) -> str:
    text = payload.decode("utf-8", errors="replace")
    return text + "...(artifact preview truncated)" if truncated else text


def _validate_suffix(suffix: str) -> str:
    if not suffix.startswith(".") or len(suffix) > 32 or any(
        marker in suffix for marker in ("/", "\\", "\x00")
    ):
        raise ValueError("artifact suffix must be a short extension without path separators")
    return suffix


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _reject_link_or_reparse(path.parent, "artifact storage path")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_write_if_absent(path: Path, payload: bytes) -> None:
    """Atomically install a content-addressed payload without replacing a peer."""

    path.parent.mkdir(parents=True, exist_ok=True)
    _reject_link_or_reparse(path.parent, "artifact storage path")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        _promote_tempfile(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _promote_tempfile(temporary: Path, path: Path) -> None:
    """Install a staged content-addressed file, or reuse an existing peer write."""

    if not path.exists():
        try:
            os.replace(temporary, path)
        except (FileExistsError, PermissionError):
            if not path.exists():
                raise
        else:
            _fsync_directory(path.parent)


def _fsync_directory(path: Path) -> None:
    if not hasattr(os, "O_DIRECTORY"):
        return
    try:
        directory_fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    except OSError:
        return
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
