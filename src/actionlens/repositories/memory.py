from __future__ import annotations

import tempfile
from pathlib import Path

from .sqlite import SQLiteGovernanceRepository


class MemoryGovernanceRepository(SQLiteGovernanceRepository):
    """Ephemeral repository with the same transactional semantics as SQLite."""

    def __init__(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory(prefix="actionlens-repository-")
        super().__init__(Path(self._temporary_directory.name) / "repository.sqlite3")

    def close(self) -> None:
        super().close()
        self._temporary_directory.cleanup()
