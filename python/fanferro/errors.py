"""Exceptions raised by fanferro.

Every exception derives from :class:`FanferroError` and carries a ``retryable`` attribute that
tells whether retrying the same operation unchanged may succeed. Errors raised by the Rust core
(AWS failures, validation, payload problems, leases) are defined in the native module and
re-exported here, the module they name as theirs (``fanferro.errors.AccessDeniedError``); the rest
are raised by the Python layer. Errors pickle with their attributes, leaving out values that do not
pickle (``None``), and copy with all of them.

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
    ├── ConsumerStateError
    └── BatchItemFailuresError
"""

from __future__ import annotations

import copy
import copyreg
import dataclasses
import pickle
from typing import TYPE_CHECKING, Any, cast

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
    "BatchItemFailuresError",
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


class BatchItemFailuresError(FanferroError):
    """Records of an SQS event failed, and the Lambda function does not report batch item failures.

    Raised by :mod:`fanferro.aws_lambda` handlers created with ``report_batch_item_failures=False``,
    so that Lambda returns the whole batch to the queue instead of deleting it. ``item_identifiers``
    are the SQS message ids of the records that failed.
    """

    retryable = True

    def __init__(self, message: str, item_identifiers: list[str]) -> None:
        super().__init__(message)
        self.item_identifiers = item_identifiers


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


def _round_trips(value: object) -> bool:
    try:
        pickle.loads(pickle.dumps(value))  # noqa: S301 - our own value, unpickled to check it
    except Exception:  # noqa: BLE001 - whatever does not pickle or unpickle
        return False
    return True


def _picklable(value: object) -> object:
    # Leaves out only what does not pickle: the fields of a dataclass (a failed message's body, a
    # batch failure's error) and the items of a tuple, list or dict, else the value itself.
    if _round_trips(value):
        return value
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        fields = {
            field.name: _picklable(getattr(value, field.name)) for field in dataclasses.fields(value) if field.init
        }
        try:
            rebuilt = dataclasses.replace(value, **fields)
        except Exception:  # noqa: BLE001 - a field the dataclass does not accept as None
            return None
        return rebuilt if _round_trips(rebuilt) else None  # the class itself may not pickle
    if isinstance(value, tuple) and type(value) is tuple:
        return tuple(_picklable(item) for item in value)
    if isinstance(value, list) and type(value) is list:
        return [_picklable(item) for item in value]
    if isinstance(value, dict) and type(value) is dict:
        return {key: _picklable(item) for key, item in value.items()}
    return None


def _reduce_error(error: BaseException) -> tuple[Any, ...]:
    # Unpickling creates the error without calling its constructor (ProvisioningError and
    # BatchPublishError take a second argument) and restores its attributes. A value that does not
    # pickle, or does not unpickle, is left out (None), so the error still reaches the caller of a
    # worker process or task queue as itself.
    state = vars(error)
    if not _round_trips(state):
        state = {name: _picklable(value) for name, value in state.items()}
    return (copyreg.__newobj__, (type(error), *error.args), state)  # pyrefly: ignore[missing-attribute]


def _blank[E: BaseException](error: E, args: tuple[Any, ...]) -> E:
    # A new instance of the error's class, created without calling its constructor, like unpickling.
    cls = type(error)
    return cast("E", cls.__new__(cls, *args))


def _copy_error[E: BaseException](error: E) -> E:
    copied = _blank(error, error.args)
    copied.__dict__.update(error.__dict__)
    return copied


def _deepcopy_error[E: BaseException](error: E, memo: dict[int, Any]) -> E:
    copied = _blank(error, copy.deepcopy(error.args, memo))
    memo[id(error)] = copied
    copied.__dict__.update(copy.deepcopy(error.__dict__, memo))
    return copied


# Every fanferro error, native or defined above, derives from the native base class. Copies keep
# every attribute: only pickles leave out what does not pickle.
setattr(FanferroError, "__reduce__", _reduce_error)  # noqa: B010
setattr(FanferroError, "__copy__", _copy_error)  # noqa: B010
setattr(FanferroError, "__deepcopy__", _deepcopy_error)  # noqa: B010
