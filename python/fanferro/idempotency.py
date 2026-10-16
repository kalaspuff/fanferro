"""Idempotent consumers (the "inbox" pattern).

SQS delivers at least once, and producers may publish the same message twice after a retry, so
handlers with side effects should be idempotent. The :class:`IdempotencyMiddleware` claims a key
(the message id by default - stable across producer retries) before the handler runs:

* new key: the handler runs; the key is marked completed on success, released on failure and when
  the handler hands the message back for another attempt (``message.retry()``, ``requeue()``,
  ``release()``, or ``dead_letter()``, which a redrive may undo; a message handed back without a
  delay may arrive again before the handler returned, and is then retried after ``retry_delay``);
* completed key: the message is acknowledged without running the handler (``message.duplicate``);
* key in progress elsewhere: the message is retried later (another consumer is working on it).

For exactly-once *effects*, record the key in the same database transaction as the handler's
writes (see ``docs/guides/patterns.md``); :class:`InMemoryIdempotencyStore` is for tests and
single-process workers.
"""

from __future__ import annotations

import time
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

from fanferro.actions import Retry
from fanferro.consumer import Duplicate

if TYPE_CHECKING:
    from collections.abc import Callable

    from fanferro.messages import Message
    from fanferro.middleware import HandlerNext

__all__ = ["Claim", "IdempotencyMiddleware", "IdempotencyStore", "InMemoryIdempotencyStore", "idempotent"]


class Claim(StrEnum):
    """Result of claiming an idempotency key."""

    ACQUIRED = "acquired"
    """New key: process the message."""
    COMPLETED = "completed"
    """Already processed: skip it."""
    IN_PROGRESS = "in_progress"
    """Being processed elsewhere: try again later."""


class IdempotencyStore(Protocol):
    """Storage for idempotency keys (implement it on Redis, DynamoDB or your SQL database)."""

    async def claim(self, key: str, *, lock_ttl: float) -> Claim:
        """Atomically claims ``key`` for ``lock_ttl`` seconds unless it is completed or claimed."""
        ...

    async def complete(self, key: str, *, ttl: float) -> None:
        """Marks ``key`` as processed for ``ttl`` seconds."""
        ...

    async def release(self, key: str) -> None:
        """Drops the claim on ``key`` so the message can be processed again."""
        ...


class InMemoryIdempotencyStore:
    """Process-local store (tests, single-process workers).

    Expired keys are forgotten (checked at most once per ``sweep_interval`` seconds), so a long-running
    worker keeps only the keys of its last ``ttl`` seconds in memory.
    """

    def __init__(self, *, clock: Callable[[], float] = time.monotonic, sweep_interval: float = 60.0) -> None:
        self._clock = clock
        self._entries: dict[str, tuple[Claim, float]] = {}
        self._sweep_interval = sweep_interval
        self._next_sweep = clock() + sweep_interval

    def _forget_expired(self, now: float) -> None:
        if now < self._next_sweep:
            return
        self._next_sweep = now + self._sweep_interval
        expired = [key for key, (_, until) in self._entries.items() if until <= now]
        for key in expired:
            del self._entries[key]

    async def claim(self, key: str, *, lock_ttl: float) -> Claim:
        """Claims ``key`` unless it is completed or claimed by someone else."""
        now = self._clock()
        self._forget_expired(now)
        entry = self._entries.get(key)
        if entry is not None and entry[1] > now:
            return entry[0] if entry[0] is Claim.COMPLETED else Claim.IN_PROGRESS
        self._entries[key] = (Claim.IN_PROGRESS, now + lock_ttl)
        return Claim.ACQUIRED

    async def complete(self, key: str, *, ttl: float) -> None:
        """Marks ``key`` as processed for ``ttl`` seconds."""
        self._entries[key] = (Claim.COMPLETED, self._clock() + ttl)

    async def release(self, key: str) -> None:
        """Drops the claim on ``key``."""
        self._entries.pop(key, None)

    def __len__(self) -> int:
        return len(self._entries)


_PROCESSED_AGAIN = frozenset({"retried", "requeued", "released", "dead_lettered"})
"""Outcomes of explicit settlements after which the message is delivered again (or may be)."""


def _default_key(message: Message[Any]) -> str:
    return f"{message.queue}:{message.id}"


class IdempotencyMiddleware:
    """Consume middleware that skips already processed messages.

    Args:
        store: Where keys are kept.
        key: Derives the key from a message (default: ``"<queue>:<message id>"``).
        ttl: How long completed keys are remembered (seconds; cover your maximum redelivery window).
        lock_ttl: How long an in-progress claim is held (should exceed the handler's run time).
        retry_delay: Delay before retrying a message that is in progress elsewhere.
    """

    def __init__(
        self,
        store: IdempotencyStore,
        *,
        key: Callable[[Message[Any]], str] = _default_key,
        ttl: float = 7 * 24 * 3600,
        lock_ttl: float = 15 * 60,
        retry_delay: float = 30.0,
    ) -> None:
        self.store = store
        self.key = key
        self.ttl = ttl
        self.lock_ttl = lock_ttl
        self.retry_delay = retry_delay

    async def __call__(self, message: Message[Any], call_next: HandlerNext) -> None:
        """Runs the handler unless the message was already processed."""
        key = self.key(message)
        claim = await self.store.claim(key, lock_ttl=self.lock_ttl)
        if claim is Claim.COMPLETED:
            raise Duplicate(f"message {message.id} was already processed")
        if claim is Claim.IN_PROGRESS:
            raise Retry(f"message {message.id} is being processed elsewhere", delay=self.retry_delay)
        try:
            await call_next(message)
        except BaseException:
            await self.store.release(key)
            raise
        if message.outcome in _PROCESSED_AGAIN:
            # The handler handed the message back (or to the dead-letter queue, from where it may be
            # redriven): its next delivery must run the handler again, not be skipped as a duplicate.
            await self.store.release(key)
            return
        await self.store.complete(key, ttl=self.ttl)


def idempotent(store: IdempotencyStore | None = None, **options: Any) -> IdempotencyMiddleware:
    """Creates an :class:`IdempotencyMiddleware` (in-memory store by default)."""
    return IdempotencyMiddleware(store or InMemoryIdempotencyStore(), **options)
