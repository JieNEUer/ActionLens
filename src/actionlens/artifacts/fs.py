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

    def put(self, value: Any, *, media_type: str | None = None) -> ArtifactRef:
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
        return ArtifactRef(
            uri=str(path.resolve()),
            media_type=inferred_media_type,
            size_bytes=len(payload),
            sha256=digest,
            preview=_preview_bytes(payload),
            created_at=created_at,
            expires_at=expires_at,
        )

    def gc(self, *, older_than_seconds: int) -> dict[str, int]:
        cutoff = datetime.now(timezone.utc).timestamp() - older_than_seconds
        deleted = 0
        bytes_deleted = 0
        for path in self.artifacts_dir.glob("**/*"):
            if not path.is_file():
                continue
            try:
                stat = path.stat()
            except OSError:
                continue
            if stat.st_mtime < cutoff:
                bytes_deleted += stat.st_size
                path.unlink(missing_ok=True)
                deleted += 1
        for directory in sorted(self.artifacts_dir.glob("**/*"), reverse=True):
            if directory.is_dir():
                try:
                    directory.rmdir()
                except OSError:
                    pass
        return {"deleted": deleted, "bytes_deleted": bytes_deleted}


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
