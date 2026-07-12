from __future__ import annotations

import json
import os
import stat
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

from actionlens.models import ArtifactPolicy, ArtifactRef
from .base import ArtifactAccessDenied, ArtifactAuthorizer, ArtifactPolicyError, EncryptionProvider


class FileArtifactStore:
    def __init__(
        self,
        root: str | Path,
        default_ttl_days: int | None = None,
        *,
        policy: ArtifactPolicy | None = None,
        encryption_provider: EncryptionProvider | None = None,
        authorizer: ArtifactAuthorizer | None = None,
    ):
        self.root = Path(root)
        self.policy = policy or ArtifactPolicy()
        self.default_ttl_days = (
            self.policy.retention_days
            if self.policy.retention_days is not None
            else default_ttl_days
        )
        self.encryption_provider = encryption_provider
        self.authorizer = authorizer
        if self.policy.encryption == "provider" and encryption_provider is None:
            raise ValueError("artifact policy requires an encryption_provider")
        self.artifacts_dir = self.root / "artifacts"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.leases_dir = self.root / "artifact-leases"
        self.leases_dir.mkdir(parents=True, exist_ok=True)
        self._usage_by_run: dict[str, int] = {}
        self._usage_lock = threading.Lock()

    def put(
        self,
        value: Any,
        *,
        media_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        preview: str | None = None,
        redacted: bool = False,
    ) -> ArtifactRef:
        if self.policy.raw_mode == "deny":
            raise ArtifactPolicyError("artifact storage is denied by policy")
        if self.policy.raw_mode == "reference_only":
            raise ArtifactPolicyError("reference_only policy accepts ArtifactRef values only")
        payload, inferred_media_type, suffix = _serialize_artifact(value, media_type)
        if self.policy.allowed_media_types is not None and not any(
            inferred_media_type == allowed or inferred_media_type.startswith(allowed + ";")
            for allowed in self.policy.allowed_media_types
        ):
            raise ArtifactPolicyError(f"media type {inferred_media_type!r} is not allowed")
        run_id = str((metadata or {}).get("run_id", ""))
        with self._usage_lock:
            used = self._usage_by_run.get(run_id, 0)
            if self.policy.max_bytes_per_run is not None and used + len(payload) > self.policy.max_bytes_per_run:
                raise ArtifactPolicyError("artifact byte budget exceeded for run")
            self._usage_by_run[run_id] = used + len(payload)
        plaintext_digest = sha256(payload).hexdigest()
        stored_payload = payload
        confidentiality: dict[str, Any] = {
            "raw_mode": self.policy.raw_mode,
            "policy_id": self.policy.policy_id,
        }
        if self.encryption_provider is not None:
            stored_payload = self.encryption_provider.encrypt(
                payload, context=dict(metadata or {})
            )
            confidentiality.update(
                {
                    "encrypted": True,
                    "provider_id": self.encryption_provider.provider_id,
                    "key_version": getattr(self.encryption_provider, "key_version", None),
                }
            )
        digest = sha256(stored_payload).hexdigest()
        target_dir = self.artifacts_dir / digest[:2]
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / f"{digest}{suffix}"
        for candidate in (self.artifacts_dir, target_dir, path):
            try:
                info = candidate.lstat()
            except FileNotFoundError:
                continue
            if _is_link_or_reparse(info):
                raise ArtifactPolicyError(
                    "artifact storage path must not contain a symlink or reparse point"
                )
        if not path.exists():
            _atomic_write(path, stored_payload)
        created_at = datetime.now(timezone.utc)
        expires_at = None
        if self.default_ttl_days is not None:
            expires_at = created_at + timedelta(days=self.default_ttl_days)
        artifact = ArtifactRef(
            uri=str(path.resolve()),
            media_type=inferred_media_type,
            size_bytes=len(stored_payload),
            sha256=digest,
            preview=preview if preview is not None else _preview_bytes(payload),
            redacted=redacted,
            created_at=created_at,
            expires_at=expires_at,
            confidentiality={**confidentiality, "plaintext_sha256": plaintext_digest},
        )
        meta_path = path.with_suffix(path.suffix + ".meta.json")
        if not meta_path.exists():
            meta_payload = artifact.model_dump(mode="json")
            meta_payload.update(metadata or {})
            _atomic_write(
                meta_path,
                json.dumps(meta_payload, ensure_ascii=False, indent=2).encode("utf-8"),
            )
        return artifact

    def validate_reference(self, artifact: ArtifactRef) -> None:
        parts = urlsplit(artifact.uri)
        if parts.username or parts.password or parts.query or parts.fragment:
            raise ArtifactPolicyError("artifact URI must not contain credentials, query, or fragment")
        if parts.scheme.lower() not in {item.lower() for item in self.policy.reference_schemes}:
            raise ArtifactPolicyError(f"artifact URI scheme {parts.scheme!r} is not allowed")

    def read(
        self, artifact: ArtifactRef, *, context: dict[str, Any] | None = None
    ) -> bytes:
        access_context = dict(context or {})
        if self.authorizer is None:
            raise ArtifactAccessDenied("artifact reads require an ArtifactAuthorizer")
        if not self.authorizer.authorize(artifact, context=access_context):
            raise ArtifactAccessDenied("artifact access was denied")
        if artifact.expires_at is not None and artifact.expires_at <= datetime.now(timezone.utc):
            raise ArtifactAccessDenied("artifact has expired")
        path = self._local_path(artifact)
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

    def _local_path(self, artifact: ArtifactRef) -> Path:
        parts = urlsplit(artifact.uri)
        windows_drive_path = len(artifact.uri) >= 3 and artifact.uri[1] == ":"
        if (
            not windows_drive_path
            and (parts.scheme not in {"", "file"} or parts.query or parts.fragment or parts.netloc)
        ):
            raise ArtifactPolicyError("artifact is not a safe local URI")
        path = Path(
            artifact.uri if windows_drive_path or not parts.scheme else parts.path
        ).absolute()
        root = self.artifacts_dir.resolve()
        try:
            relative = path.relative_to(root)
        except ValueError as exc:
            raise ArtifactPolicyError("artifact path escapes the configured store") from exc
        components = [root]
        for part in relative.parts:
            components.append(components[-1] / part)
        for component in components:
            try:
                info = component.lstat()
            except FileNotFoundError:
                continue
            if _is_link_or_reparse(info):
                raise ArtifactPolicyError(
                    "artifact path must not contain a symlink or reparse point"
                )
        return path

    @contextmanager
    def _artifact_lease(self, path: Path):
        lease = self.leases_dir / f"{path.name}.{uuid4().hex}.lease"
        fd = os.open(lease, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        try:
            yield
        finally:
            lease.unlink(missing_ok=True)

    @contextmanager
    def _gc_lease(self):
        lock = self.leases_dir / "gc.lock"
        try:
            fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            try:
                stale = time.time() - lock.stat().st_mtime > 300
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

    def gc(
        self,
        *,
        older_than_seconds: int,
        max_bytes: int | None = None,
        dry_run: bool = False,
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
            )

    def _gc_locked(
        self, *, older_than_seconds: int, max_bytes: int | None, dry_run: bool
    ) -> dict[str, int]:
        now = datetime.now(timezone.utc).timestamp()
        cutoff = now - older_than_seconds
        candidates: list[Path] = []
        total_size = 0
        files: list[tuple[float, int, Path]] = []
        for path in self.artifacts_dir.glob("**/*"):
            if not path.is_file() or path.name.endswith(".meta.json"):
                continue
            try:
                stat = path.stat()
            except OSError:
                continue
            total_size += stat.st_size
            files.append((stat.st_mtime, stat.st_size, path))
            if stat.st_mtime < cutoff:
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

        deleted = 0
        bytes_deleted = 0
        for path in candidates:
            leases = list(self.leases_dir.glob(f"{path.name}.*.lease"))
            active_lease = False
            for lease in leases:
                try:
                    if now - lease.stat().st_mtime <= 300:
                        active_lease = True
                    elif not dry_run:
                        lease.unlink(missing_ok=True)
                except OSError:
                    active_lease = True
            if active_lease:
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
        for directory in sorted(self.artifacts_dir.glob("**/*"), reverse=True):
            if directory.is_dir():
                try:
                    directory.rmdir()
                except OSError:
                    pass
        return {
            "deleted": 0 if dry_run else deleted,
            "would_delete": deleted if dry_run else 0,
            "bytes_deleted": 0 if dry_run else bytes_deleted,
            "bytes_would_delete": bytes_deleted if dry_run else 0,
        }

    def inspect(self, artifact: ArtifactRef) -> dict[str, Any]:
        try:
            path = self._local_path(artifact)
        except ArtifactPolicyError:
            return {"status": "invalid_uri", "expected_sha256": artifact.sha256}
        try:
            with self._artifact_lease(path):
                payload = _read_file_no_follow(path)
        except FileNotFoundError:
            return {"status": "missing", "expected_sha256": artifact.sha256}
        except PermissionError:
            return {"status": "permission_denied", "expected_sha256": artifact.sha256}
        actual = sha256(payload).hexdigest()
        return {
            "status": "ok" if actual == artifact.sha256 else "checksum_mismatch",
            "expected_sha256": artifact.sha256,
            "actual_sha256": actual,
            "size_bytes": len(payload),
        }


def _read_file_no_follow(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except (FileNotFoundError, PermissionError):
        raise
    except OSError as exc:
        raise ArtifactPolicyError(f"artifact could not be opened safely: {exc}") from exc
    with os.fdopen(fd, "rb") as handle:
        return handle.read()


def _is_link_or_reparse(info: os.stat_result) -> bool:
    return stat.S_ISLNK(info.st_mode) or bool(
        getattr(info, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    )


def _serialize_artifact(
    value: Any, media_type: str | None
) -> tuple[bytes, str, str]:
    if isinstance(value, bytes):
        return value, media_type or "application/octet-stream", ".bin"
    if isinstance(value, str):
        return value.encode("utf-8"), media_type or "text/plain; charset=utf-8", ".txt"
    text = json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
    return text.encode("utf-8"), media_type or "application/json", ".json"


def _preview_bytes(payload: bytes, limit: int = 240) -> str:
    text = payload[:limit].decode("utf-8", errors="replace")
    if len(payload) > limit:
        return text + "...(artifact preview truncated)"
    return text


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        if hasattr(os, "O_DIRECTORY"):
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
