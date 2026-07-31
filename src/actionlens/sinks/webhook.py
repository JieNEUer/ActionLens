from __future__ import annotations

import hashlib
import hmac
import http.client
import ipaddress
import socket
import ssl
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from actionlens.models import TrajectoryEvent

_DEFAULT_DNS_CACHE_TTL_SEC = 60.0


@dataclass(frozen=True)
class _ResolvedAddress:
    family: int
    socktype: int
    proto: int
    sockaddr: tuple[Any, ...]


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
        dns_cache_ttl_sec: float = _DEFAULT_DNS_CACHE_TTL_SEC,
    ) -> None:
        if timeout_sec <= 0 or max_payload_bytes <= 0:
            raise ValueError("timeout_sec and max_payload_bytes must be positive")
        if dns_cache_ttl_sec <= 0:
            raise ValueError("dns_cache_ttl_sec must be positive")
        self.endpoint = _validate_endpoint(endpoint, host_allowlist=host_allowlist)
        self._secret = secret
        self.key_id = key_id
        self.host_allowlist = host_allowlist
        self.allow_private_networks = allow_private_networks
        self.timeout_sec = timeout_sec
        self.max_payload_bytes = max_payload_bytes
        self._opener = opener
        self._dns_cache_ttl_sec = dns_cache_ttl_sec
        self._dns_cache_lock = threading.Lock()
        self._dns_cache: tuple[float, tuple[_ResolvedAddress, ...]] | None = None

    def emit(self, event: TrajectoryEvent) -> None:
        payload = event.model_dump_json(exclude_none=True).encode("utf-8")
        if len(payload) > self.max_payload_bytes:
            raise WebhookDeliveryError("webhook payload exceeds configured limit")
        timestamp = str(int(time.time()))
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
            response = (
                self._opener(request, timeout=self.timeout_sec)
                if self._opener is not None
                else self._open_pinned(request, timeout=self.timeout_sec)
            )
            status = int(getattr(response, "status", 200))
            headers = getattr(response, "headers", None)
            if status == 429 and headers is not None:
                retry_after = _retry_after_seconds(headers.get("Retry-After"))
            close = getattr(response, "close", None)
            if close is not None:
                close()
        except urllib.error.HTTPError as exc:
            status = exc.code
            retry_after = _retry_after_seconds(exc.headers.get("Retry-After")) if exc.headers else None
        except (OSError, TimeoutError, http.client.HTTPException) as exc:
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

    def _resolved_addresses(self) -> tuple[_ResolvedAddress, ...]:
        """Cache addresses that are both validated and used for the connection."""
        now = time.monotonic()
        with self._dns_cache_lock:
            if self._dns_cache is not None:
                cached_at, addresses = self._dns_cache
                if now - cached_at < self._dns_cache_ttl_sec:
                    return addresses
            addresses = _resolve_addresses(
                self.endpoint,
                allow_private_networks=self.allow_private_networks,
            )
            self._dns_cache = (time.monotonic(), addresses)
            return addresses

    def _open_pinned(
        self, request: urllib.request.Request, *, timeout: float
    ) -> _PinnedHTTPResponse:
        parts = urlsplit(request.full_url)
        host = parts.hostname
        assert host is not None
        path = parts.path or "/"
        if parts.query:
            path = f"{path}?{parts.query}"
        last_error: OSError | http.client.HTTPException | None = None
        addresses = self._resolved_addresses()
        for address in addresses:
            connection = _PinnedHTTPSConnection(
                host,
                port=parts.port or 443,
                timeout=timeout,
                resolved_address=address,
            )
            try:
                connection.request(
                    request.get_method(),
                    path,
                    body=request.data,
                    headers=dict(request.header_items()),
                )
                return _PinnedHTTPResponse(connection, connection.getresponse())
            except (OSError, http.client.HTTPException) as exc:
                last_error = exc
                connection.close()
        with self._dns_cache_lock:
            if self._dns_cache is not None and self._dns_cache[1] == addresses:
                self._dns_cache = None
        assert last_error is not None
        raise last_error


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


def _resolve_addresses(
    endpoint: str, *, allow_private_networks: bool
) -> tuple[_ResolvedAddress, ...]:
    parts = urlsplit(endpoint)
    host = parts.hostname
    assert host is not None
    try:
        resolved = socket.getaddrinfo(
            host,
            parts.port or 443,
            type=socket.SOCK_STREAM,
        )
    except OSError as exc:
        raise WebhookDeliveryError(f"webhook DNS resolution failed: {exc}") from exc
    addresses = tuple(
        dict.fromkeys(
            _ResolvedAddress(family, socktype, proto, tuple(sockaddr))
            for family, socktype, proto, _, sockaddr in resolved
        )
    )
    if not addresses:
        raise WebhookDeliveryError("webhook DNS resolution returned no addresses")
    if not allow_private_networks and any(
        not ipaddress.ip_address(address.sockaddr[0]).is_global
        for address in addresses
    ):
        raise WebhookDeliveryError("webhook endpoint resolved to a non-public address")
    return addresses


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(
        self,
        host: str,
        *,
        port: int,
        timeout: float,
        resolved_address: _ResolvedAddress,
    ) -> None:
        super().__init__(
            host,
            port=port,
            timeout=timeout,
            context=ssl.create_default_context(),
        )
        self._resolved_address = resolved_address

    def connect(self) -> None:
        address = self._resolved_address
        sock = socket.socket(address.family, address.socktype, address.proto)
        try:
            sock.settimeout(self.timeout)
            sock.connect(address.sockaddr)
            self.sock = self._context.wrap_socket(sock, server_hostname=self.host)
        except Exception:
            sock.close()
            raise


class _PinnedHTTPResponse:
    def __init__(
        self,
        connection: _PinnedHTTPSConnection,
        response: http.client.HTTPResponse,
    ) -> None:
        self._connection = connection
        self._response = response
        self.status = response.status
        self.headers = response.headers

    def close(self) -> None:
        try:
            self._response.close()
        finally:
            self._connection.close()


def _retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None
