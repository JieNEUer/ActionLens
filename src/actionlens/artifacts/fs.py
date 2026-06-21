from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

from actionlens.models import ArtifactRef


class FileArtifactStore:
    def __init__(self, root: str | Path, default_ttl_days: int | None = None):
        self.root = Path(root)
        self.default_ttl_days = default_ttl_days
        self.artifacts_dir = self.root / "artifacts"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    def put(
        self,
        value: Any,
        *,
        media_type: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ArtifactRef:
        payload, inferred_media_type, suffix = _serialize_artifact(value, media_type)
        digest = sha256(payload).hexdigest()
        target_dir = self.artifacts_dir / digest[:2]
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / f"{digest}{suffix}"
        if not path.exists():
            path.write_bytes(payload)
        created_at = datetime.now(timezone.utc)
        expires_at = None
        if self.default_ttl_days is not None:
            expires_at = created_at + timedelta(days=self.default_ttl_days)
        artifact = ArtifactRef(
            uri=str(path.resolve()),
            media_type=inferred_media_type,
            size_bytes=len(payload),
            sha256=digest,
            preview=_preview_bytes(payload),
            created_at=created_at,
            expires_at=expires_at,
        )
        meta_path = path.with_suffix(path.suffix + ".meta.json")
        if not meta_path.exists():
            meta_payload = artifact.model_dump(mode="json")
            meta_payload.update(metadata or {})
            meta_path.write_text(
                json.dumps(meta_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
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
