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
PostgreSQL implementation (``FOR UPDATE SKIP LOCKED``, with claims exclusive per FIFO message
group, so that several relays keep each group in order) is shown in ``docs/guides/patterns.md``.
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
    """Persistence of outbox entries (implement it on your database).

    A store may also define ``async def release(self, records: Sequence[OutboxRecord]) -> None``
    (not part of the protocol, so that stores without it still type-check): the relay calls it
    when a round ends early (its task is cancelled, or an error stops it) for the records it had
    claimed but not attempted, and for records whose ``mark_published`` or ``mark_failed`` raised,
    to release their claims; they stay pending and are not counted as attempts. Without it, the
    relay calls :meth:`mark_failed` for them instead.
    """

    async def claim_batch(self, limit: int) -> Sequence[OutboxRecord]:
        """Returns up to ``limit`` unpublished records in insertion order.

        The records must be claimed so that concurrent relays do not publish them too (e.g.
        ``SELECT ... FOR UPDATE SKIP LOCKED`` or a lease column). When several relays share the
        store, the claims must also be exclusive per FIFO message group
        (:attr:`PreparedMessage.fifo_group <fanferro.PreparedMessage.fifo_group>`): skip every
        record of a group that has a record claimed by another relay, else a second relay sends the
        later records of the group while the first still sends the earlier ones, out of order.
        :class:`InMemoryOutbox` does this; ``docs/guides/patterns.md`` shows it for PostgreSQL.
        """
        ...

    async def mark_published(self, ids: Sequence[str]) -> None:
        """Marks records as published (or deletes them), and releases their claims.

        The relay marks records as soon as they are published, also when the round fails or is
        cancelled afterwards.
        """
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


class _NotAttempted(Exception):  # noqa: N818 - not an error of the record itself
    """Passed to ``mark_failed`` for a record the round did not attempt (stores without ``release``)."""

    retryable = True


class _Round:
    """One round of a relay: what it published, marked and still has to release."""

    def __init__(self, relay: OutboxRelay, records: list[OutboxRecord]) -> None:
        self.relay = relay
        self.records = records
        self.published: list[str] = []
        """Ids of the records published this round."""
        self._marked = 0
        """How many of :attr:`published` were passed to ``mark_published``."""
        self._records = {r.id: r for r in records}
        self._unsettled = dict(self._records)
        """Claimed records whose claims the store has not released yet."""
        self.failed: list[tuple[OutboxRecord, BaseException]] = []

    async def run(self) -> None:
        relay = self.relay
        standard = [r for r in self.records if not r.message.destination.get("fifo")]
        fifo = [r for r in self.records if r.message.destination.get("fifo")]
        if standard:
            failures: list[tuple[OutboxRecord, BaseException]] = []
            try:
                batch = await relay.client.publish_prepared_batch([r.message for r in standard])
            except Exception as error:  # noqa: BLE001 - every record of the batch failed
                failures = [(record, error) for record in standard]
            else:
                for record, result in zip(standard, batch.results, strict=True):
                    if isinstance(result, PublishResult):
                        self._published(record)
                    elif isinstance(result, BatchFailure):
                        failures.append((record, result.error))
            await self.mark_published()
            for record, error in failures:
                await self.failed_attempt(record, error)
        blocked: set[str | None] = set()
        for record in fifo:
            group = record.message.fifo_group
            if group in blocked:
                await self.failed_attempt(record, _HeldBack("an earlier message of the group failed"))
                continue
            try:
                await relay.client.publish_prepared(record.message)
            except Exception as error:  # noqa: BLE001 - record and continue with other groups
                blocked.add(group)
                await self.failed_attempt(record, error)
                continue
            self._published(record)

    def _published(self, record: OutboxRecord) -> None:
        self.published.append(record.id)
        self._unsettled.pop(record.id, None)

    async def failed_attempt(self, record: OutboxRecord, error: BaseException) -> None:
        """Tells the store (an error of the store is logged: the round goes on)."""
        self.failed.append((record, error))
        try:
            await self.relay._failed(record, error)
        except Exception:
            # The claim may still be held: settle() releases it with the records not attempted.
            _LOGGER.exception("outbox store failed to record the failed attempt of record %s", record.id)
            return
        # Not on cancellation either: the record is then released with the others not attempted.
        self._unsettled.pop(record.id, None)

    async def mark_published(self) -> None:
        """Marks the records published since the last call."""
        ids = self.published[self._marked :]
        if not ids:
            return
        try:
            await self.relay.store.mark_published(ids)
        except Exception:
            _LOGGER.exception(
                "outbox store failed to mark %d published records (%s): they will be published again", len(ids), ids[0]
            )
            self._marked += len(ids)
            # Released by settle(), so that a store that keeps claims does not hold them forever.
            self._unsettled.update((identifier, self._records[identifier]) for identifier in ids)
            return
        # Not on cancellation: settle() then marks them again (marking twice must be harmless).
        self._marked += len(ids)
        failures = self.relay._failures
        if failures:
            for identifier in ids:
                failures.pop(identifier, None)

    async def settle(self) -> None:
        """Marks what is published and releases the claims of the records not attempted."""
        await self.mark_published()
        unsettled_records = list(self._unsettled.values())
        self._unsettled.clear()
        store = self.relay.store
        if unsettled_records:
            _LOGGER.debug("outbox round ended early: releasing the claims of %d records", len(unsettled_records))
            release = getattr(store, "release", None)
            try:
                if release is not None:
                    await release(unsettled_records)
                else:
                    for record in unsettled_records:
                        await store.mark_failed(record, _NotAttempted("the relay round ended before this record"))
            except Exception:
                _LOGGER.exception("outbox store failed to release %d claimed records", len(unsettled_records))
        if self.failed:
            record, error = self.failed[0]
            _LOGGER.debug(
                "outbox round: %d published, %d failed (first: record %s, %s: %s)",
                len(self.published),
                len(self.failed),
                record.id,
                type(error).__name__,
                error,
            )


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
        """Claims up to ``limit`` pending records, skipping FIFO groups with a claimed record."""
        busy = {r.message.fifo_group for key, r in self._pending.items() if key in self._claimed}
        busy.discard(None)
        records: list[OutboxRecord] = []
        for key, record in self._pending.items():
            if len(records) >= limit:
                break
            if key not in self._claimed and record.message.fifo_group not in busy:
                records.append(record)
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

    async def release(self, records: Sequence[OutboxRecord]) -> None:
        """Releases the claims (the records stay pending, their attempts unchanged)."""
        for record in records:
            self._claimed.discard(record.id)

    @property
    def claimed(self) -> list[str]:
        """Ids of the records claimed by a relay and not yet marked."""
        return [key for key in self._pending if key in self._claimed]

    @property
    def pending(self) -> list[OutboxRecord]:
        """Records not yet published (nor given up)."""
        return list(self._pending.values())


class OutboxRelay:
    """Publishes pending outbox records.

    Records for standard destinations are sent in batches. Records for FIFO destinations are sent
    one by one in order, and a failure stops the rest of that message group for the round, so the
    group's order is preserved. Several relays keep that order only if the store claims the
    records of a FIFO group exclusively (see :meth:`OutboxStore.claim_batch`); otherwise run a
    single relay for FIFO destinations.

    A round marks the records it published as soon as it knows, and releases the claims of those it
    did not attempt, also when it fails or is cancelled (e.g. the task running :meth:`run` at
    shutdown). A store must therefore accept a record being marked published again.

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
        self._settling: set[asyncio.Future[None]] = set()

    async def run_once(self) -> int:
        """Publishes one round of records; returns how many were published.

        Published records are marked as soon as they are known, and the claims of records not
        attempted are released (``OutboxStore.release``), also when the round is cancelled or
        stopped by an error. An error of the store while recording one record's failure is logged
        and does not end the round.
        """
        records = list(await self.store.claim_batch(self.batch_size))
        if not records:
            return 0
        round_ = _Round(self, records)
        try:
            await round_.run()
        finally:
            # Shielded: a cancelled round (relay.run() cancelled at shutdown) still marks what it
            # published and releases the rest, and a second cancellation lets that finish.
            settle = asyncio.ensure_future(round_.settle())
            self._settling.add(settle)
            settle.add_done_callback(self._settling.discard)
            await asyncio.shield(settle)
        return len(round_.published)

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
