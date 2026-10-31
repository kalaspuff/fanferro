"""Transactional outbox: publish messages reliably together with database changes.

Writing to a database and publishing to SNS/SQS are two separate systems; doing both in a request
can lose messages (crash after commit) or publish phantom ones (rollback after publish). The
outbox pattern fixes this:

1. In the business transaction, insert the encoded message into an outbox table::

       prepared = await orders_topic.prepare(OrderCreated(...))  # no network I/O for the message
       await db.execute("INSERT INTO outbox (id, message) VALUES ($1, $2)", prepared.message_id, prepared.to_json())

2. A relay (:class:`OutboxRelay`) publishes pending rows and marks them published.

Delivery is at-least-once: a crash between publishing and marking can publish a row twice. The
message id is stable, so FIFO destinations deduplicate it and idempotent consumers
(:mod:`fanferro.idempotency`) skip it. A row that cannot be published (its destination was
deleted, the destination refuses it) is given up after ``OutboxRelay.max_attempts`` attempts, so
that it does not hold back the rows behind it. Implement :class:`OutboxStore` for your database; a
PostgreSQL implementation (``FOR UPDATE SKIP LOCKED``) is shown in ``docs/guides/patterns.md``.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

from fanferro.messages import BatchFailure, PreparedMessage, PublishResult

if TYPE_CHECKING:
    from collections.abc import Sequence

    from fanferro.client import Client

__all__ = ["InMemoryOutbox", "OutboxRecord", "OutboxRelay", "OutboxStore"]

_LOGGER = logging.getLogger("fanferro.outbox")


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    """A pending outbox entry."""

    id: str
    """Row id (often the message id)."""
    message: PreparedMessage
    """The message to publish."""
    attempts: int = 0
    """Previous publish attempts."""
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class OutboxStore(Protocol):
    """Persistence of outbox entries (implement it on your database)."""

    async def claim_batch(self, limit: int) -> Sequence[OutboxRecord]:
        """Returns up to ``limit`` unpublished records in insertion order.

        The records must be claimed so that concurrent relays do not publish them too (e.g.
        ``SELECT ... FOR UPDATE SKIP LOCKED`` or a lease column).
        """
        ...

    async def mark_published(self, ids: Sequence[str]) -> None:
        """Marks records as published (or deletes them)."""
        ...

    async def mark_failed(self, record: OutboxRecord, error: BaseException) -> None:
        """Records a failed attempt (the record stays pending; release any claim)."""
        ...

    async def mark_abandoned(self, record: OutboxRecord, error: BaseException) -> None:
        """Gives a record up: it failed ``OutboxRelay.max_attempts`` times, last for good.

        Keep the record for inspection and count the attempt, but never claim it again (release
        any claim). Without this method the relay retries such records forever, and they hold back
        the records behind them.
        """
        ...


_MAX_FAILING_RECORDS = 10_000
"""Records whose failures a relay counts (the oldest are forgotten first)."""


class _HeldBack(Exception):  # noqa: N818 - not an error of the record itself
    """A FIFO record not attempted because an earlier record of its group failed this round."""

    retryable = True


class InMemoryOutbox:
    """An in-memory :class:`OutboxStore` for tests and examples."""

    def __init__(self) -> None:
        self._pending: dict[str, OutboxRecord] = {}
        self._claimed: set[str] = set()
        self.published: list[str] = []
        self.abandoned: list[OutboxRecord] = []
        """Records the relay gave up (in the order it did)."""
        self._ids = itertools.count(1)

    def add(self, message: PreparedMessage, record_id: str | None = None) -> str:
        """Stores a message (call this inside your unit of work)."""
        identifier = record_id or f"{next(self._ids):012d}"
        self._pending[identifier] = OutboxRecord(identifier, message)
        return identifier

    async def claim_batch(self, limit: int) -> Sequence[OutboxRecord]:
        """Claims up to ``limit`` pending records."""
        records = [r for key, r in self._pending.items() if key not in self._claimed][:limit]
        self._claimed.update(r.id for r in records)
        return records

    async def mark_published(self, ids: Sequence[str]) -> None:
        """Removes published records."""
        for identifier in ids:
            self._pending.pop(identifier, None)
            self._claimed.discard(identifier)
            self.published.append(identifier)

    async def mark_failed(self, record: OutboxRecord, error: BaseException) -> None:
        """Releases the claim and counts the attempt."""
        self._claimed.discard(record.id)
        if record.id in self._pending:
            current = self._pending[record.id]
            self._pending[record.id] = OutboxRecord(
                current.id, current.message, current.attempts + 1, current.created_at
            )

    async def mark_abandoned(self, record: OutboxRecord, error: BaseException) -> None:
        """Moves the record to :attr:`abandoned`."""
        self._claimed.discard(record.id)
        current = self._pending.pop(record.id, None)
        if current is not None:
            self.abandoned.append(OutboxRecord(current.id, current.message, current.attempts + 1, current.created_at))

    @property
    def pending(self) -> list[OutboxRecord]:
        """Records not yet published (nor given up)."""
        return list(self._pending.values())


class OutboxRelay:
    """Publishes pending outbox records.

    Records for standard destinations are sent in batches. Records for FIFO destinations are sent
    one by one in order, and a failure stops the rest of that message group for the round, so the
    group's order is preserved.

    A record whose attempts failed ``max_attempts`` times with an error that retrying cannot fix
    (its destination does not exist, the destination refuses the message) is given up
    (:meth:`OutboxStore.mark_abandoned`): it no longer holds back the records behind it, and a FIFO
    group continues with its next record, as after dead-lettering. Errors that may pass (throttling,
    timeouts, network errors) and rounds spent behind an earlier record of a FIFO group do not
    count: such records are retried until they are published. The relay counts these failures
    itself (a restarted relay starts again from zero).

    Args:
        client: The fanferro client.
        store: The outbox store.
        batch_size: Records claimed per round.
        poll_interval: Pause after an empty round (seconds).
        max_attempts: Failed attempts for good before a record is given up (``None``: retry it
            forever).
    """

    def __init__(
        self,
        client: Client,
        store: OutboxStore,
        *,
        batch_size: int = 50,
        poll_interval: float = 1.0,
        max_attempts: int | None = 10,
    ) -> None:
        if max_attempts is not None and max_attempts < 1:
            raise ValueError("max_attempts must be at least 1 (or None)")
        self.client = client
        self.store = store
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self._stopped = asyncio.Event()
        self._warned_store = False
        # Failures for good per record id: rounds of temporary errors, or of waiting behind an earlier
        # record of a FIFO group, are attempts for the store but do not count towards giving up.
        self._failures: dict[str, int] = {}

    async def run_once(self) -> int:
        """Publishes one round of records; returns how many were published."""
        records = list(await self.store.claim_batch(self.batch_size))
        if not records:
            return 0
        standard = [r for r in records if not r.message.destination.get("fifo")]
        fifo = [r for r in records if r.message.destination.get("fifo")]
        published: list[str] = []
        if standard:
            batch = await self.client.publish_prepared_batch([r.message for r in standard])
            for record, result in zip(standard, batch.results, strict=True):
                if isinstance(result, PublishResult):
                    published.append(record.id)
                elif isinstance(result, BatchFailure):
                    await self._failed(record, result.error)
        blocked: set[tuple[str, str | None]] = set()
        for record in fifo:
            group = (record.message.destination_name, record.message.group_id)
            if group in blocked:
                await self._failed(record, _HeldBack("an earlier message of the group failed"))
                continue
            try:
                await self.client.publish_prepared(record.message)
            except Exception as error:  # noqa: BLE001 - record and continue with other groups
                blocked.add(group)
                await self._failed(record, error)
                continue
            published.append(record.id)
        if published:
            await self.store.mark_published(published)
            if self._failures:
                for identifier in published:
                    self._failures.pop(identifier, None)
        return len(published)

    async def _failed(self, record: OutboxRecord, error: BaseException) -> None:
        """Records a failed attempt, or gives the record up (see :class:`OutboxRelay`)."""
        failures = self._failures
        count = 0
        if self.max_attempts is not None and not getattr(error, "retryable", False):
            count = failures.pop(record.id, 0) + 1
            failures[record.id] = count  # most recent last
            if len(failures) > _MAX_FAILING_RECORDS:
                del failures[next(iter(failures))]
        if self.max_attempts is not None and count >= self.max_attempts:
            abandon = getattr(self.store, "mark_abandoned", None)
            if abandon is not None:
                _LOGGER.error(
                    "outbox record %s given up after %d attempts that failed for good (%s: %s)",
                    record.id,
                    count,
                    type(error).__name__,
                    error,
                )
                failures.pop(record.id, None)
                await abandon(record, error)
                return
            if not self._warned_store:
                self._warned_store = True
                _LOGGER.warning(
                    "outbox record %s keeps failing (%s), but %s has no mark_abandoned(): it is retried forever "
                    "and holds back the records behind it",
                    record.id,
                    error,
                    type(self.store).__name__,
                )
        await self.store.mark_failed(record, error)

    async def run(self) -> None:
        """Publishes continuously until :meth:`stop` is called."""
        self._stopped.clear()
        while not self._stopped.is_set():
            try:
                count = await self.run_once()
            except Exception:
                _LOGGER.exception("outbox relay round failed")
                count = 0
            if count == 0:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stopped.wait(), timeout=self.poll_interval)

    def stop(self) -> None:
        """Stops :meth:`run` after the current round."""
        self._stopped.set()
