from __future__ import annotations

import json
import os
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

from actionlens.models import ArtifactPolicy, ArtifactRef
from .base import ArtifactPolicyError, EncryptionProvider


class FileArtifactStore:
    def __init__(
        self,
        root: str | Path,
        default_ttl_days: int | None = None,
        *,
        policy: ArtifactPolicy | None = None,
        encryption_provider: EncryptionProvider | None = None,
    ):
        self.root = Path(root)
        self.policy = policy or ArtifactPolicy()
        self.default_ttl_days = (
            self.policy.retention_days
            if self.policy.retention_days is not None
            else default_ttl_days
        )
        self.encryption_provider = encryption_provider
        if self.policy.encryption == "provider" and encryption_provider is None:
            raise ValueError("artifact policy requires an encryption_provider")
        self.artifacts_dir = self.root / "artifacts"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
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
        confidentiality: dict[str, Any] = {"raw_mode": self.policy.raw_mode}
        if self.encryption_provider is not None:
            stored_payload = self.encryption_provider.encrypt(
                payload, context=dict(metadata or {})
            )
            confidentiality.update(
                {"encrypted": True, "provider_id": self.encryption_provider.provider_id}
            )
        digest = sha256(stored_payload).hexdigest()
        target_dir = self.artifacts_dir / digest[:2]
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / f"{digest}{suffix}"
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

    def gc(
        self,
        *,
        older_than_seconds: int,
        max_bytes: int | None = None,
        dry_run: bool = False,
    ) -> dict[str, int]:
        cutoff = datetime.now(timezone.utc).timestamp() - older_than_seconds
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
        path = Path(artifact.uri)
        try:
            payload = path.read_bytes()
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
