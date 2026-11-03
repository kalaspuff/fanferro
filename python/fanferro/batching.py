"""Batch routes: handlers that process several messages in one call.

A route registered with ``batch=Batch(...)`` collects the messages it matches into batches and
calls its handler once per batch, with a list::

    @consumer.route(OrderPlaced, batch=fanferro.Batch(max_size=100, max_wait=0.5))
    async def store(orders: list[fanferro.Message[OrderPlaced]]) -> fanferro.BatchResult | None:
        failed = await db.insert_many([order.body for order in orders])
        return fanferro.BatchResult(failed=[orders[i] for i in failed])

* A batch is handed to the handler when it holds ``max_size`` messages, or ``max_wait`` seconds
  after its first message joined it, whichever comes first.
* Returning ``None`` acks every message of the batch; raising applies the retry policy to every
  message, as if each one's handler had raised; returning a :class:`BatchResult` fails only the
  messages it lists (each with its own error) and acks the others. Messages the handler settled
  itself (``message.retry()``, ``message.dead_letter()``...) are left as they are.
* Every message goes through routing, decoding and consume middleware on its own (idempotency
  middleware skips duplicates before they join a batch), and keeps its in-flight permit and its
  visibility heartbeat until it is settled.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from fanferro._utils import Duration, seconds
from fanferro.errors import ConfigurationError

if TYPE_CHECKING:
    from fanferro.messages import Message

__all__ = ["Batch", "BatchResult"]

_MAX_SIZE = 10_000
"""Largest batch: the in-flight messages of a consumer are bounded too (``concurrency + prefetch``)."""
_MAX_WAIT = 900.0
"""Longest collection time (seconds): heartbeats keep collected messages invisible meanwhile."""


@dataclass(frozen=True, slots=True, init=False)
class Batch:
    """How a batch route collects messages (``route(..., batch=Batch(...))``).

    Attributes:
        max_size: Messages of a batch at most (1-10,000). A consumer that does not set
            ``prefetch`` prefetches at least this many messages, so that a full batch fits in
            flight; one that sets it needs ``concurrency + prefetch >= max_size``.
        max_wait: Seconds (or a ``timedelta``) a batch waits for more messages after its first one
            joined it (0-900). ``0`` hands over the messages that are ready together.
    """

    max_size: int
    max_wait: float

    def __init__(self, max_size: int = 10, max_wait: Duration = 1.0) -> None:
        if isinstance(max_size, bool) or not isinstance(max_size, int):
            raise TypeError(f"max_size must be an integer, got {type(max_size).__name__}")
        if not 1 <= max_size <= _MAX_SIZE:
            raise ConfigurationError(f"max_size must be between 1 and {_MAX_SIZE:,}, got {max_size}")
        wait = seconds(max_wait, "max_wait")
        if not (math.isfinite(wait) and wait <= _MAX_WAIT):
            raise ConfigurationError(f"max_wait must be between 0 and {_MAX_WAIT:g} seconds, got {max_wait!r}")
        object.__setattr__(self, "max_size", max_size)
        object.__setattr__(self, "max_wait", wait)


class BatchResult:
    """What a batch handler reports: the messages of its batch that failed.

    The listed messages are settled as if their handler had raised their error (the retry and
    dead-letter policies apply; :class:`~fanferro.Reject`, :class:`~fanferro.Retry` and the other
    handler actions decide explicitly); a message listed without an error fails with
    :class:`~fanferro.errors.BatchItemFailedError`, which is retried. The other messages are acked.
    A message is named by itself or by its position in the batch (the only way for a handler that
    receives bodies).

    Example::

        result = fanferro.BatchResult()
        for message, outcome in zip(messages, await api.index([m.body for m in messages])):
            if not outcome.ok:
                result.fail(message, ValueError(outcome.reason))
        return result
    """

    __slots__ = ("_failures",)

    def __init__(
        self,
        failed: Iterable[Message[Any] | int] | Mapping[Message[Any] | int, Exception | None] = (),
    ) -> None:
        """Creates a result.

        Args:
            failed: The messages (or their positions in the batch) that failed, or a mapping of
                them to their errors (``None``: no particular error).
        """
        self._failures: dict[Any, Exception | None] = {}
        if isinstance(failed, Mapping):
            for item, error in failed.items():
                self.fail(item, error)
        else:
            for item in failed:
                self.fail(item)

    def fail(self, message: Message[Any] | int, error: Exception | None = None) -> None:
        """Reports ``message`` (or the message at that position of the batch) as failed with ``error``.

        Raises:
            TypeError: If ``error`` is not an exception.
        """
        if error is not None and not isinstance(error, Exception):
            raise TypeError(f"error must be an exception or None, got {type(error).__name__}")
        if isinstance(message, bool):
            raise TypeError("message must be a Message or its position in the batch, got a bool")
        self._failures[message if isinstance(message, int) else _Identity(message)] = error

    @property
    def failed(self) -> list[tuple[Message[Any] | int, Exception | None]]:
        """The failed messages (or positions) with their errors, in the order they were reported."""
        return [(key.value if isinstance(key, _Identity) else key, error) for key, error in self._failures.items()]

    def __len__(self) -> int:
        return len(self._failures)

    def __repr__(self) -> str:
        return f"BatchResult(failed={len(self._failures)})"

    def _errors_for(self, messages: list[Message[Any]]) -> list[tuple[bool, Exception | None]]:
        """``(failed, error)`` for each message of the batch, in order.

        Raises:
            ValueError: If the result names a message that is not in the batch.
        """
        positions = {id(message): index for index, message in enumerate(messages)}
        outcomes: list[tuple[bool, Exception | None]] = [(False, None)] * len(messages)
        for key, error in self._failures.items():
            if isinstance(key, _Identity):
                index = positions.get(id(key.value))
                if index is None:
                    raise ValueError(f"BatchResult names a message that is not in the batch: {key.value!r}")
            else:
                index = key + len(messages) if key < 0 else key
                if not 0 <= index < len(messages):
                    raise ValueError(f"BatchResult names position {key} of a batch of {len(messages)} messages")
            outcomes[index] = (True, error)
        return outcomes


class _Identity:
    """Keys a message by identity (whatever ``__eq__`` it has)."""

    __slots__ = ("value",)

    def __init__(self, value: Message[Any]) -> None:
        self.value = value

    def __hash__(self) -> int:
        return id(self.value)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Identity) and other.value is self.value
