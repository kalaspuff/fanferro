"""AWS connection settings: credentials, SDK retries and timeouts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from fanferro._utils import Duration, seconds

__all__ = ["AwsRetry", "Credentials", "Timeouts"]


@dataclass(frozen=True, slots=True)
class Credentials:
    """Static AWS credentials (prefer the default provider chain in production).

    Example for MiniStack/LocalStack: ``Credentials("test", "test")``.
    """

    access_key_id: str
    secret_access_key: str
    session_token: str | None = None

    def __repr__(self) -> str:
        return f"Credentials(access_key_id={self.access_key_id!r}, secret_access_key='***')"

    def to_core(self) -> dict[str, Any]:
        """The core representation."""
        return {
            "access_key_id": self.access_key_id,
            "secret_access_key": self.secret_access_key,
            "session_token": self.session_token,
        }


@dataclass(frozen=True, slots=True)
class AwsRetry:
    """Retries of individual AWS API calls (performed by the AWS SDK for Rust).

    Attributes:
        max_attempts: Attempts per call including the first (``1`` disables retries).
        mode: ``"standard"`` (jittered exponential backoff with a retry quota) or ``"adaptive"``
            (additionally rate-limits the client when throttled).
        initial_backoff: Initial backoff.
        max_backoff: Maximum backoff.
    """

    max_attempts: int = 5
    mode: Literal["standard", "adaptive"] = "standard"
    initial_backoff: Duration = 0.2
    max_backoff: Duration = 20.0

    def to_core(self) -> dict[str, Any]:
        """The core representation."""
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        return {
            "max_attempts": self.max_attempts,
            "mode": self.mode,
            "initial_backoff": seconds(self.initial_backoff, "initial_backoff"),
            "max_backoff": seconds(self.max_backoff, "max_backoff"),
        }


@dataclass(frozen=True, slots=True)
class Timeouts:
    """Timeouts of AWS API calls (``None`` keeps the SDK default: no timeout, except for connecting).

    Long polling receives get at least their wait time plus 15 seconds for ``read``, ``attempt``
    and ``operation``, and requests that upload a large body (offloaded payloads, big messages)
    one more second per 64 KiB. Transfers that stall are cut off by the AWS SDK.

    Attributes:
        connect: TCP/TLS connect timeout.
        read: Time until the response starts to arrive, counted from when the request is sent. A
            call that gets no response (a dead connection) fails after it, and the SDK retries it.
        operation: Total time for a call including retries.
        attempt: Time for a single attempt.
    """

    connect: Duration | None = 3.1
    read: Duration | None = 10.0
    operation: Duration | None = None
    attempt: Duration | None = None

    def to_core(self) -> dict[str, Any]:
        """The core representation."""
        return {
            "connect": seconds(self.connect, "connect") if self.connect is not None else None,
            "read": seconds(self.read, "read") if self.read is not None else None,
            "operation": seconds(self.operation, "operation") if self.operation is not None else None,
            "attempt": seconds(self.attempt, "attempt") if self.attempt is not None else None,
        }
