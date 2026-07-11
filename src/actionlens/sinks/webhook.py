from __future__ import annotations

import hashlib
import hmac
import time
import urllib.error
import urllib.request
from collections.abc import Callable

from actionlens.models import TrajectoryEvent


class WebhookDeliveryError(RuntimeError):
    pass


class WebhookSink:
    """Single-attempt signed webhook sink; durable retries belong to OutboxDispatcher."""

    def __init__(
        self, endpoint: str, *, secret: Callable[[], str | bytes],
        timeout_sec: float = 10.0, max_payload_bytes: int = 256_000,
        opener: Callable[..., object] | None = None,
    ) -> None:
        if timeout_sec <= 0 or max_payload_bytes <= 0:
            raise ValueError("timeout_sec and max_payload_bytes must be positive")
        self.endpoint = endpoint
        self._secret = secret
        self.timeout_sec = timeout_sec
        self.max_payload_bytes = max_payload_bytes
        self._opener = opener or urllib.request.urlopen

    def emit(self, event: TrajectoryEvent) -> None:
        payload = event.model_dump_json(exclude_none=True).encode("utf-8")
        if len(payload) > self.max_payload_bytes:
            raise WebhookDeliveryError("webhook payload exceeds configured limit")
        timestamp = str(int(time.time()))
        secret = self._secret()
        key = secret.encode("utf-8") if isinstance(secret, str) else secret
        signature = hmac.new(key, timestamp.encode("ascii") + b"." + payload, hashlib.sha256).hexdigest()
        request = urllib.request.Request(
            self.endpoint, data=payload, method="POST",
            headers={
                "Content-Type": "application/json",
                "X-ActionLens-Timestamp": timestamp,
                "X-ActionLens-Signature": f"sha256={signature}",
                "X-ActionLens-Event-Id": event.event_id,
            },
        )
        try:
            response = self._opener(request, timeout=self.timeout_sec)
            status = int(getattr(response, "status", 200))
            close = getattr(response, "close", None)
            if close is not None:
                close()
        except urllib.error.HTTPError as exc:
            status = exc.code
        except (OSError, TimeoutError) as exc:
            raise WebhookDeliveryError(str(exc)) from exc
        if 200 <= status < 300:
            return
        if status == 429 or status >= 500:
            raise WebhookDeliveryError(f"retryable webhook response: HTTP {status}")
        # Other 4xx responses are terminal and intentionally acknowledged.

    def flush(self) -> None: ...
    def close(self) -> None: ...
