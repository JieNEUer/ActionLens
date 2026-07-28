from __future__ import annotations

import hashlib
import hmac
import ipaddress
import socket
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from urllib.parse import urlsplit

from actionlens.models import TrajectoryEvent


class WebhookDeliveryError(RuntimeError):
    def __init__(self, message: str, *, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


def verify_webhook_signature(
    payload: bytes,
    headers: Mapping[str, str],
    *,
    secrets: Mapping[str, str | bytes],
    replay_window_seconds: int = 300,
    now: float | None = None,
    seen_event: Callable[[str], bool] | None = None,
) -> bool:
    normalized = {key.lower(): value for key, value in headers.items()}
    timestamp_text = normalized.get("x-actionlens-timestamp", "")
    event_id = normalized.get("x-actionlens-event-id", "")
    key_id = normalized.get("x-actionlens-key-id", "default")
    signature = normalized.get("x-actionlens-signature", "")
    try:
        timestamp = int(timestamp_text)
    except ValueError:
        return False
    if abs((now if now is not None else time.time()) - timestamp) > replay_window_seconds:
        return False
    if not event_id or (seen_event is not None and seen_event(event_id)):
        return False
    secret = secrets.get(key_id)
    if secret is None:
        return False
    key = secret.encode("utf-8") if isinstance(secret, str) else secret
    expected = hmac.new(
        key, timestamp_text.encode("ascii") + b"." + payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(signature, f"sha256={expected}")


class WebhookSink:
    """Single-attempt signed webhook sink; durable retries belong to OutboxDispatcher."""

    def __init__(
        self, endpoint: str, *, secret: Callable[[], str | bytes | tuple[str, str | bytes]],
        timeout_sec: float = 10.0, max_payload_bytes: int = 256_000,
        opener: Callable[..., object] | None = None,
        key_id: str = "default",
        host_allowlist: set[str] | None = None,
        allow_private_networks: bool = False,
    ) -> None:
        if timeout_sec <= 0 or max_payload_bytes <= 0:
            raise ValueError("timeout_sec and max_payload_bytes must be positive")
        self.endpoint = _validate_endpoint(endpoint, host_allowlist=host_allowlist)
        self._secret = secret
        self.key_id = key_id
        self.host_allowlist = host_allowlist
        self.allow_private_networks = allow_private_networks
        self.timeout_sec = timeout_sec
        self.max_payload_bytes = max_payload_bytes
        self._opener = opener or urllib.request.urlopen
        self._resolve_endpoint = opener is None

    def emit(self, event: TrajectoryEvent) -> None:
        payload = event.model_dump_json(exclude_none=True).encode("utf-8")
        if len(payload) > self.max_payload_bytes:
            raise WebhookDeliveryError("webhook payload exceeds configured limit")
        timestamp = str(int(time.time()))
        if self._resolve_endpoint:
            _validate_resolved_address(
                self.endpoint, allow_private_networks=self.allow_private_networks
            )
        secret_value = self._secret()
        if isinstance(secret_value, tuple):
            key_id, secret = secret_value
        else:
            key_id, secret = self.key_id, secret_value
        key = secret.encode("utf-8") if isinstance(secret, str) else secret
        signature = hmac.new(key, timestamp.encode("ascii") + b"." + payload, hashlib.sha256).hexdigest()
        request = urllib.request.Request(
            self.endpoint, data=payload, method="POST",
            headers={
                "Content-Type": "application/json",
                "X-ActionLens-Timestamp": timestamp,
                "X-ActionLens-Signature": f"sha256={signature}",
                "X-ActionLens-Event-Id": event.event_id,
                "X-ActionLens-Key-Id": key_id,
            },
        )
        retry_after = None
        try:
            response = self._opener(request, timeout=self.timeout_sec)
            status = int(getattr(response, "status", 200))
            close = getattr(response, "close", None)
            if close is not None:
                close()
        except urllib.error.HTTPError as exc:
            status = exc.code
            retry_after = _retry_after_seconds(exc.headers.get("Retry-After")) if exc.headers else None
        except (OSError, TimeoutError) as exc:
            raise WebhookDeliveryError(str(exc)) from exc
        if 200 <= status < 300:
            return
        if status == 429 or status >= 500:
            raise WebhookDeliveryError(
                f"retryable webhook response: HTTP {status}",
                retry_after=retry_after if status == 429 else None,
            )
        # Other 4xx responses are terminal and intentionally acknowledged.

    def flush(self) -> None: ...
    def close(self) -> None: ...


def _validate_endpoint(endpoint: str, *, host_allowlist: set[str] | None) -> str:
    parts = urlsplit(endpoint)
    if parts.scheme.lower() != "https":
        raise ValueError("webhook endpoint must use HTTPS")
    if not parts.hostname or parts.username or parts.password:
        raise ValueError("webhook endpoint must have a host and no embedded credentials")
    if parts.fragment:
        raise ValueError("webhook endpoint must not contain a fragment")
    if host_allowlist is not None and parts.hostname.lower() not in {
        host.lower() for host in host_allowlist
    }:
        raise ValueError("webhook endpoint host is not allowlisted")
    return endpoint


def _validate_resolved_address(endpoint: str, *, allow_private_networks: bool) -> None:
    if allow_private_networks:
        return
    host = urlsplit(endpoint).hostname
    assert host is not None
    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)}
    except OSError as exc:
        raise WebhookDeliveryError(f"webhook DNS resolution failed: {exc}") from exc
    for address in addresses:
        ip = ipaddress.ip_address(address)
        if not ip.is_global:
            raise WebhookDeliveryError("webhook endpoint resolved to a non-public address")


def _retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None
