from __future__ import annotations

import hashlib
import hmac
import io
import time
import tracemalloc
from pathlib import Path

import pytest

import actionlens as al
from actionlens.artifacts import ArtifactPolicyError, FileArtifactStore
from actionlens.outbox import OutboxDispatcher
from actionlens.sinks import MemorySink


class _AllowArtifactRead:
    def authorize(self, artifact: object, *, context: dict[str, object]) -> bool:
        return True


class _PatternSource:
    """Deterministic source that never stores the requested payload in memory."""

    _pattern = bytes(range(256)) * 2048

    def __init__(self, size: int) -> None:
        self.remaining = size
        self.offset = 0
        self.bytes_read = 0
        self.digest = hashlib.sha256()

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            raise AssertionError("stream consumer attempted an unbounded read")
        count = min(size, self.remaining)
        if count == 0:
            return b""
        start = self.offset % len(self._pattern)
        end = start + count
        if end <= len(self._pattern):
            payload = self._pattern[start:end]
        else:
            payload = self._pattern[start:] + self._pattern[: end - len(self._pattern)]
        self.offset += count
        self.remaining -= count
        self.bytes_read += count
        self.digest.update(payload)
        return payload


class _DigestSink:
    def __init__(self) -> None:
        self.bytes_written = 0
        self.digest = hashlib.sha256()

    def write(self, payload: bytes) -> int:
        self.bytes_written += len(payload)
        self.digest.update(payload)
        return len(payload)


class _StreamingTestCipher:
    provider_id = "actionlens-test-streaming"
    algorithm = "xor-hmac-sha256-test-v1"
    key_version = "test-key-v1"
    _key = b"actionlens-streaming-test-key"
    _translation = bytes.maketrans(
        bytes(range(256)), bytes(value ^ 0xA5 for value in range(256))
    )

    def encrypt(self, payload: bytes, *, context: dict[str, object]) -> bytes:
        return payload.translate(self._translation)

    def decrypt(self, payload: bytes, *, context: dict[str, object]) -> bytes:
        return payload.translate(self._translation)

    def rewrap(self, payload: bytes, *, context: dict[str, object]) -> bytes:
        return payload

    def encrypt_stream(
        self, source: io.BufferedIOBase, destination: io.BufferedIOBase, *, context: dict[str, object]
    ) -> al.EncryptionMetadata:
        authenticator = hmac.new(self._key, digestmod=hashlib.sha256)
        while chunk := source.read(64 * 1024):
            authenticator.update(chunk)
            destination.write(chunk.translate(self._translation))
        return al.EncryptionMetadata(
            algorithm=self.algorithm,
            key_version=self.key_version,
            nonce="test-nonce-v1",
            authentication_tag=authenticator.hexdigest(),
        )

    def decrypt_stream(
        self, source: io.BufferedIOBase, destination: io.BufferedIOBase, *, context: dict[str, object]
    ) -> None:
        authenticator = hmac.new(self._key, digestmod=hashlib.sha256)
        while chunk := source.read(64 * 1024):
            plaintext = chunk.translate(self._translation)
            authenticator.update(plaintext)
            destination.write(plaintext)
        metadata = context["_actionlens_artifact"]["confidentiality"]
        expected_tag = metadata["authentication_tag"]
        if not hmac.compare_digest(authenticator.hexdigest(), expected_tag):
            raise ArtifactPolicyError("streaming artifact authentication tag mismatch")


def _encrypted_store(tmp_path: Path, *, max_bytes_per_run: int | None = None) -> FileArtifactStore:
    return FileArtifactStore(
        tmp_path,
        policy=al.ArtifactPolicy(encryption="provider", max_bytes_per_run=max_bytes_per_run),
        encryption_provider=_StreamingTestCipher(),
        authorizer=_AllowArtifactRead(),
    )


def test_streaming_encryption_round_trip_100mib_has_bounded_python_heap(tmp_path: Path) -> None:
    size = 100 * 1024 * 1024
    source = _PatternSource(size)
    store = _encrypted_store(tmp_path)

    tracemalloc.start()
    artifact = store.put_stream(
        source,
        media_type="application/octet-stream",
        metadata={"run_id": "streaming-100mib"},
    )
    destination = _DigestSink()
    assert store.read_stream(artifact, destination, context={"actor_id": "test"}) == size
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert source.bytes_read == size
    assert destination.bytes_written == size
    assert destination.digest.hexdigest() == source.digest.hexdigest()
    assert artifact.confidentiality["streaming"] is True
    assert artifact.confidentiality["algorithm"] == _StreamingTestCipher.algorithm
    assert artifact.confidentiality["plaintext_size_bytes"] == size
    assert artifact.confidentiality["ciphertext_size_bytes"] == size
    assert peak < 12 * 1024 * 1024
    assert not list((tmp_path / "artifacts").glob(".actionlens-*.tmp"))


def test_streaming_tamper_does_not_write_any_plaintext_to_destination(tmp_path: Path) -> None:
    store = _encrypted_store(tmp_path)
    artifact = store.put_stream(io.BytesIO(b"classified payload"), metadata={"run_id": "tamper"})
    path = Path(artifact.uri)
    with path.open("r+b") as handle:
        handle.seek(0)
        original = handle.read(1)
        handle.seek(0)
        handle.write(bytes([original[0] ^ 0x01]))

    destination = _DigestSink()
    with pytest.raises(ArtifactPolicyError, match="checksum|authentication"):
        store.read_stream(artifact, destination, context={"actor_id": "test"})
    assert destination.bytes_written == 0
    assert not list((tmp_path / "artifacts").glob(".actionlens-read-*.tmp"))


def test_streaming_provider_must_consume_source_and_failure_releases_byte_reservation(
    tmp_path: Path,
) -> None:
    class PartialProvider(_StreamingTestCipher):
        def encrypt_stream(
            self, source: io.BufferedIOBase, destination: io.BufferedIOBase, *, context: dict[str, object]
        ) -> al.EncryptionMetadata:
            destination.write(source.read(1))
            return al.EncryptionMetadata(algorithm=self.algorithm)

    store = FileArtifactStore(
        tmp_path,
        policy=al.ArtifactPolicy(encryption="provider", max_bytes_per_run=8),
        encryption_provider=PartialProvider(),
        authorizer=_AllowArtifactRead(),
    )
    with pytest.raises(ArtifactPolicyError, match="complete source"):
        store.put_stream(io.BytesIO(b"12345678"), metadata={"run_id": "same-run"})
    store.encryption_provider = _StreamingTestCipher()
    artifact = store.put_stream(io.BytesIO(b"12345678"), metadata={"run_id": "same-run"})
    assert artifact.confidentiality["streaming"] is True
    assert not list((tmp_path / "artifacts").glob(".actionlens-stream-*.tmp"))


def test_streaming_api_rejects_legacy_bytes_only_encryption_provider(tmp_path: Path) -> None:
    class LegacyProvider:
        provider_id = "legacy"

        def encrypt(self, payload: bytes, *, context: dict[str, object]) -> bytes:
            return payload

        def decrypt(self, payload: bytes, *, context: dict[str, object]) -> bytes:
            return payload

        def rewrap(self, payload: bytes, *, context: dict[str, object]) -> bytes:
            return payload

    source = _PatternSource(1024)
    store = FileArtifactStore(
        tmp_path,
        policy=al.ArtifactPolicy(encryption="provider"),
        encryption_provider=LegacyProvider(),
        authorizer=_AllowArtifactRead(),
    )
    with pytest.raises(ArtifactPolicyError, match="StreamingEncryptionProvider"):
        store.put_stream(source)
    assert source.bytes_read == 0


def test_read_to_path_replaces_destination_only_after_stream_verification(tmp_path: Path) -> None:
    store = _encrypted_store(tmp_path / "store")
    artifact = store.put_stream(io.BytesIO(b"new content"), metadata={"run_id": "path"})
    target = tmp_path / "output.bin"
    target.write_bytes(b"old content")
    assert store.read_to_path(artifact, target, context={"actor_id": "test"}) == len(b"new content")
    assert target.read_bytes() == b"new content"


def test_outbox_health_and_retry_state_do_not_expose_transport_credentials() -> None:
    class UnavailableRepository:
        def outbox_stats(self) -> dict[str, int]:
            raise OSError("postgresql://user:password@db.example/actionlens")

        def claim_outbox(self, **kwargs: object) -> list[object]:
            raise OSError("postgresql://user:password@db.example/actionlens")

    dispatcher = OutboxDispatcher(UnavailableRepository(), MemorySink())
    assert dispatcher.health()["stats_error"] == "OSError"
    dispatcher.start(interval_seconds=0.001, batch_size=1)
    try:
        for _ in range(100):
            if dispatcher.health()["last_error"] is not None:
                break
            time.sleep(0.001)
        health = dispatcher.health()
        assert health["last_error"] == "OSError"
        assert "password" not in str(health)
    finally:
        assert dispatcher.stop(timeout=1)
