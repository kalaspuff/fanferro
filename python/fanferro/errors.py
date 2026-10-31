"""Exceptions raised by fanferro.

Every exception derives from :class:`FanferroError` and carries a ``retryable`` attribute that
tells whether retrying the same operation unchanged may succeed. Errors raised by the Rust core
(AWS failures, validation, payload problems, leases) are defined in the native module and
re-exported here, the module they name as theirs (``fanferro.errors.AccessDeniedError``); the rest
are raised by the Python layer.

Hierarchy::

    FanferroError
    ├── ConfigurationError
    ├── ValidationError
    │   └── MessageTooLargeError
    ├── AwsError
    │   ├── ResourceNotFoundError
    │   ├── AccessDeniedError
    │   └── ThrottledError
    ├── TransportError
    ├── PayloadError
    │   └── PayloadIntegrityError
    ├── EnvelopeError
    ├── LeaseError
    ├── ShutdownError
    ├── ProvisioningError
    ├── ResourceUnavailableError
    ├── CodecError
    │   ├── EncodeError
    │   └── DecodeError
    ├── RoutingError
    ├── BatchPublishError
    ├── HandlerTimeoutError
    └── ConsumerStateError
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fanferro._core import (
    AccessDeniedError,
    AwsError,
    ConfigurationError,
    EnvelopeError,
    FanferroError,
    LeaseError,
    MessageTooLargeError,
    PayloadError,
    PayloadIntegrityError,
    ResourceNotFoundError,
    ShutdownError,
    ThrottledError,
    TransportError,
    ValidationError,
)

if TYPE_CHECKING:
    from fanferro.messages import BatchPublishResult
    from fanferro.provisioning import ProvisioningReport

__all__ = [
    "AccessDeniedError",
    "AwsError",
    "BatchPublishError",
    "CodecError",
    "ConfigurationError",
    "ConsumerStateError",
    "DecodeError",
    "EncodeError",
    "EnvelopeError",
    "FanferroError",
    "HandlerTimeoutError",
    "LeaseError",
    "MessageTooLargeError",
    "PayloadError",
    "PayloadIntegrityError",
    "ProvisioningError",
    "ResourceNotFoundError",
    "ResourceUnavailableError",
    "RoutingError",
    "ShutdownError",
    "ThrottledError",
    "TransportError",
    "ValidationError",
]


class ProvisioningError(FanferroError):
    """A queue, topic, subscription or bucket could not be provisioned as configured.

    Raised when the provisioning policy's failure mode is ``"raise"``. The full outcome is
    available as :attr:`report`.
    """

    retryable = False

    def __init__(self, message: str, report: ProvisioningReport) -> None:
        super().__init__(message)
        self.report = report
        error = report.error
        self.retryable = bool(getattr(error, "retryable", False))


class ResourceUnavailableError(FanferroError):
    """A resource is not available (missing, or provisioning failed in ``"log"``/``"ignore"`` mode).

    fanferro retries resolution on later use, so this error clears once the resource exists.
    """

    retryable = True


class CodecError(FanferroError):
    """A message body could not be encoded or decoded."""

    retryable = False


class EncodeError(CodecError):
    """A value could not be encoded into bytes."""


class DecodeError(CodecError):
    """Message bytes could not be decoded into the requested type."""


class RoutingError(FanferroError):
    """Invalid routing configuration (for example a handler signature that cannot be resolved)."""

    retryable = False


class BatchPublishError(FanferroError):
    """Some entries of a batch publish failed after retries.

    The successful entries were published. Inspect :attr:`result` for per-entry outcomes.
    """

    def __init__(self, message: str, result: BatchPublishResult) -> None:
        super().__init__(message)
        self.result = result
        self.retryable = all(getattr(failure.error, "retryable", False) for failure in result.failed)


class HandlerTimeoutError(FanferroError, TimeoutError):
    """A handler exceeded the consumer's ``handler_timeout``."""

    retryable = True


class ConsumerStateError(FanferroError):
    """An operation is not valid in the consumer's current state (e.g. starting it twice)."""

    retryable = False


def error_details(error: BaseException) -> dict[str, Any]:
    """Structured description of an exception (used in events and dead-letter metadata)."""
    details: dict[str, Any] = {
        "error_type": f"{type(error).__module__}.{type(error).__qualname__}",
        "error_message": str(error),
        "retryable": bool(getattr(error, "retryable", False)),
    }
    for name in ("code", "request_id", "kind", "category"):
        value = getattr(error, name, None)
        if isinstance(value, str):
            details[name] = value
    return details
