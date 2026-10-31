"""The consumer runtime: receive, route, decode, handle, settle.

A :class:`Consumer` pulls messages from one queue through the Rust engine (which owns long
polling, rate limiting, in-flight limits, visibility heartbeats and batched settlement) and runs
handlers on the asyncio event loop:

1. transport decoding (SNS unwrapping, claim-check download, base64) happens in Rust;
2. the envelope is unwrapped (first matching envelope);
3. messages past ``RetryPolicy.max_attempts`` get ``DeadLetterPolicy.on_exhausted`` without running
   a handler (dead-lettered by default), except with ``"retry"``, which keeps processing them;
4. the router picks a route; the body is decoded into the route's type;
5. middleware and the handler run (with an optional timeout) inside an OpenTelemetry span;
6. success acks the message; failures follow the :class:`~fanferro.RetryPolicy` and
   :class:`~fanferro.DeadLetterPolicy`; handlers may also settle explicitly.

FIFO queues are processed in order per message group (groups run concurrently).
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import functools
import signal
import time
from collections import deque
from typing import TYPE_CHECKING, Any, Literal, Self

from fanferro._core import Delivery, FanferroError, LeaseError, ShutdownError
from fanferro._utils import Duration, optional_seconds, seconds
from fanferro.actions import Discard, HandlerAction, Reject, Requeue, Retry
from fanferro.codecs import is_json_content_type, media_type
from fanferro.envelopes import CloudEvents, CloudEventsProtobuf, Envelope, Raw, default_envelopes, unwrap_first
from fanferro.errors import ConsumerStateError, HandlerTimeoutError, ResourceNotFoundError, error_details
from fanferro.events import EventKind
from fanferro.messages import Message, Outcome
from fanferro.middleware import ConsumeMiddleware, chain_consume
from fanferro.policies import DeadLetterPolicy, FailureAction, Heartbeat, RateLimit, RetryPolicy, ShutdownPolicy
from fanferro.telemetry import monotonic

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from fanferro.client import Client
    from fanferro.resources import Queue
    from fanferro.routing import Handler, Route, Router

__all__ = ["Consumer", "Duplicate"]

type ConsumerState = Literal["created", "starting", "running", "stopping", "stopped"]

_MAX_VISIBILITY_DELAY = 12 * 3600 - 60
_MAX_REQUEUE_DELAY = 900.0
_MAX_ATTRIBUTES = 10
"""Message attributes SQS allows (a requeued copy needs one for ``fanferro.attempt``)."""
_MAX_HANDED_BACK = 10_000
"""Messages whose hand-backs a consumer remembers (the oldest are forgotten first)."""
_DELIVERY_WAIT = 0.5
"""Seconds the dispatcher waits for deliveries per call (stopping wakes it at once)."""
_BUILTIN_ENVELOPES = (CloudEvents, CloudEventsProtobuf, Raw)


class Duplicate(Discard):
    """Raised by idempotency middleware for messages that were already processed."""

    def __init__(self, reason: str = "duplicate message") -> None:
        super().__init__(reason)


class _Postpone(HandlerAction):
    """Hands a message back unprocessed for ``delay`` seconds, without spending a processing attempt.

    Raised by the idempotency middleware for messages whose claim is held elsewhere.

    Standard queues get a copy that keeps the attempt number (which also resets SQS's receive count
    for a ``RedrivePolicy``); FIFO queues, and messages without room for ``fanferro.attempt``, change
    the visibility, and only the consumer that postponed the message knows it was no attempt.
    """

    def __init__(self, reason: str, *, delay: float) -> None:
        super().__init__(reason)
        self.delay = delay


def _report_error(message: Message[Any], text: str, error: BaseException) -> None:
    """Reports an error of middleware that does not fail the message (``consumer.error``)."""
    settler = message._settler
    if isinstance(settler, _Settler):
        settler._consumer._emit(EventKind.CONSUMER_ERROR, text, message=message, exception=error)


class _Stats:
    __slots__ = ("decode_failures", "duplicates", "failed", "handler_seconds", "processed", "unroutable")

    def __init__(self) -> None:
        self.processed = 0
        self.failed = 0
        self.duplicates = 0
        self.unroutable = 0
        self.decode_failures = 0
        self.handler_seconds = 0.0

    def snapshot(self) -> dict[str, Any]:
        return {
            "processed": self.processed,
            "failed": self.failed,
            "duplicates": self.duplicates,
            "unroutable": self.unroutable,
            "decode_failures": self.decode_failures,
            "handler_seconds": round(self.handler_seconds, 6),
        }


class _Slot:
    """A handler slot (one of ``concurrency``), held while decoding, middleware and the handler run.

    It is released before the message is settled (auto-ack, retries, dead-lettering), so the next
    message can start while the settlement's round trip completes.
    """

    __slots__ = ("_consumer", "_semaphore", "_waits", "held")

    def __init__(self, consumer: Consumer, semaphore: asyncio.Semaphore) -> None:
        self._consumer = consumer
        self._semaphore = semaphore
        self._waits = 0
        self.held = True

    def release(self) -> None:
        """Frees the slot (idempotent)."""
        if self.held:
            self.held = False
            self._consumer._active -= 1
            self._semaphore.release()

    def block(self) -> bool:
        """Notes that the handler waits for a settlement it requested (an explicit ``ack()``...).

        Returns whether every handler that could request another settlement is waiting now: then
        no other settlement can join the batch, unless some of these handlers wait for a
        settlement that was sent already (the engine checks that, see ``Delivery.mark_awaited``).
        """
        consumer = self._consumer
        self._waits += 1
        if self._waits == 1:
            consumer._blocked += 1
        return consumer._all_waiting()

    def unblock(self, _settlement: object = None) -> None:
        """Ends a wait started with :meth:`block` (usable as a done callback)."""
        self._waits -= 1
        if self._waits == 0:
            self._consumer._blocked -= 1


class _Settler:
    """Settles messages for the consumer (used by :class:`Message` methods)."""

    def __init__(self, consumer: Consumer) -> None:
        self._consumer = consumer

    def _delivery(self, message: Message[Any]) -> Delivery:
        if message.delivery is None:
            raise ConsumerStateError("message has no delivery")
        return message.delivery

    async def _call(
        self, message: Message[Any], operation: Callable[[Delivery], Awaitable[None]], *, settlement: bool = True
    ) -> None:
        """Runs one settlement call on the message's delivery.

        A handler that still holds its slot (an explicit ``await message.ack()``...) counts as
        waiting for the call until the call completes (see :meth:`_Slot.block`), and so does the
        message group of a sequenced message, whose next message waits for it. When everything
        that could request another settlement is waiting, the engine sends the batch as soon as
        it holds all their settlements instead of waiting to fill it.

        A settlement call that raises is noted on the message: its group's later messages are
        then handed back (see :class:`_GroupSequencer`). ``settlement=False`` (extensions) does not.
        """
        delivery = self._delivery(message)
        consumer = self._consumer
        slot: _Slot | None = message._slot
        try:
            if slot is not None and slot.held:
                delivery.mark_awaited(consumer._awaited_settlements() if slot.block() else None)
                future = asyncio.ensure_future(operation(delivery))
                # Not waiting any more once the call completed, even before the handler resumes.
                future.add_done_callback(slot.unblock)
                await future
            elif consumer._sequenced and message.group_id is not None:
                consumer._group_waits += 1
                try:
                    delivery.mark_awaited(consumer._awaited_settlements() if consumer._all_waiting() else None)
                    await operation(delivery)
                finally:
                    consumer._group_waits -= 1
            else:
                await operation(delivery)
        except Exception:
            if settlement:
                message._settlement_failed = True
            raise

    async def ack(self, message: Message[Any]) -> None:
        await self._call(message, lambda delivery: delivery.ack())
        await self._consumer._after_ack(message)

    async def retry(self, message: Message[Any], delay: float | None, error: BaseException | None) -> None:
        consumer = self._consumer
        policy = consumer.retry_policy
        if policy.exhausted(message.attempt):
            consumer._emit(
                EventKind.MESSAGE_FAILED,
                f"retries exhausted after {message.attempt} attempts; "
                f"applying {consumer.dead_letter_policy.on_exhausted!r}",
                message=message,
            )
            await self.perform(message, consumer.dead_letter_policy.on_exhausted, "retries exhausted", error)
            return
        if delay is None:
            delay = policy.backoff.delay(message.attempt)
        if policy.strategy == "requeue":
            await self._call(
                message, lambda delivery: delivery.requeue(message.attempt, min(delay, _MAX_REQUEUE_DELAY))
            )
            message.outcome = "requeued"
        else:
            await self._call(message, lambda delivery: delivery.nack(min(delay, _MAX_VISIBILITY_DELAY)))

    async def perform(
        self, message: Message[Any], action: FailureAction, reason: str, error: BaseException | None
    ) -> None:
        """Applies a failure action to an already-begun settlement (sets the final outcome)."""
        backoff = self._consumer.retry_policy.backoff
        if action == "dead_letter":
            message.outcome = "dead_lettered"
            await self.dead_letter(message, reason, error)
        elif action == "discard":
            message.outcome = "discarded"
            await self.discard(message, reason)
        elif action == "retry":
            message.outcome = "retried"
            await self._call(
                message, lambda delivery: delivery.nack(min(backoff.delay(message.attempt), _MAX_VISIBILITY_DELAY))
            )
        else:
            message.outcome = "retried"
            await self._call(message, lambda delivery: delivery.nack(min(backoff.maximum, _MAX_VISIBILITY_DELAY)))

    async def requeue(self, message: Message[Any], delay: float | None, error: BaseException | None) -> None:
        if delay is None:
            delay = self._consumer.retry_policy.backoff.delay(message.attempt)
        await self._call(message, lambda delivery: delivery.requeue(message.attempt, min(delay, _MAX_REQUEUE_DELAY)))

    async def dead_letter(self, message: Message[Any], reason: str, error: BaseException | None) -> None:
        consumer = self._consumer
        if not consumer._dead_letter_url:
            fallback = consumer.dead_letter_policy.on_missing_queue
            consumer._emit(
                EventKind.CONFIG_WARNING,
                f"cannot dead-letter message {message.id}: no dead-letter queue configured; applying {fallback!r}",
                message=message,
                reason=reason,
            )
            if fallback == "discard":
                message.outcome = "discarded"
                await self._call(message, lambda delivery: delivery.ack())
            elif fallback == "retry":
                message.outcome = "retried"
                delay = min(consumer.retry_policy.backoff.delay(message.attempt), _MAX_VISIBILITY_DELAY)
                await self._call(message, lambda delivery: delivery.nack(delay))
            else:
                message.outcome = "retried"
                await self._call(
                    message,
                    lambda delivery: delivery.nack(min(consumer.retry_policy.backoff.maximum, _MAX_VISIBILITY_DELAY)),
                )
            return
        details: dict[str, str] = {"queue": consumer.queue.name, "attempt": str(message.attempt)}
        if message.type:
            details["message_type"] = message.type
        if message.id:
            details["message_id"] = message.id
        if error is not None and consumer.dead_letter_policy.include_error:
            info = error_details(error)
            details["error_type"] = info["error_type"]
            details["error_message"] = info["error_message"]
        await self._call(message, lambda delivery: delivery.dead_letter(reason, details))

    async def release(self, message: Message[Any]) -> None:
        await self._call(message, lambda delivery: delivery.release())

    async def discard(self, message: Message[Any], reason: str) -> None:
        await self._call(message, lambda delivery: delivery.ack())
        self._consumer._emit(EventKind.MESSAGE_DROPPED, f"message dropped: {reason}", message=message, reason=reason)

    async def extend(self, message: Message[Any], timeout: float) -> None:
        await self._call(message, lambda delivery: delivery.extend(timeout), settlement=False)


class _GroupSequencer:
    """Processes messages of the same FIFO group one at a time, in order.

    A message whose settlement did not complete (it was retried or released, or a delete
    failed) hands the group's later messages back, so SQS redelivers the group in order. With
    ``strict_ordering``, the group's next message also waits until the automatic ack of the
    previous one is confirmed.
    """

    def __init__(self, consumer: Consumer) -> None:
        self._consumer = consumer
        self._pending: dict[str, deque[Delivery]] = {}

    def submit(self, group: str, delivery: Delivery) -> None:
        queue = self._pending.get(group)
        if queue is not None:
            queue.append(delivery)
            return
        self._pending[group] = deque([delivery])
        self._consumer._waiting += 1  # the group's first message competes for a slot right away
        self._consumer._spawn(self._drain(group))

    async def _drain(self, group: str) -> None:
        queue = self._pending[group]
        counted = True  # whether the next message is already counted as waiting for a slot
        try:
            while queue:
                delivery = queue.popleft()
                was_counted, counted = counted, False
                outcome = await self._consumer._run(delivery, counted=was_counted)
                if outcome in ("acked", "discarded", "dead_lettered", "requeued"):  # gone from the queue
                    self._consumer._forget_handed_back(delivery)
                if outcome not in ("acked", "discarded", "dead_lettered"):
                    # The message comes back: hand back the rest of its batch, so that SQS
                    # redelivers the group in order. What arrives meanwhile is that redelivery.
                    later = list(queue)
                    queue.clear()
                    for other in later:
                        self._consumer._note_handed_back(other)
                    await asyncio.gather(*(self._consumer._release_quietly(other) for other in later))
        except Exception as error:  # noqa: BLE001 - deliberate catch-all; the error is reported, never swallowed silently
            self._consumer._task_failed(error)
        finally:
            if counted:
                self._consumer._waiting -= 1
            self._pending.pop(group, None)
            self._consumer._task_ended()


class Consumer:
    """Consumes one queue. Create it with ``client.consumer(queue, handler_or_router, ...)``.

    Use it as ``await consumer.run()`` (until stopped or a signal arrives), as an async context
    manager, or with explicit ``await consumer.start()`` / ``await consumer.stop()``. Routes can be
    added directly on the consumer (``@consumer.route(...)``).
    """

    def __init__(
        self,
        client: Client,
        queue: Queue,
        router: Router,
        *,
        name: str | None = None,
        concurrency: int = 10,
        prefetch: int | None = None,
        batch_size: int = 10,
        min_receive_batch: int | None = None,
        receive_batch_wait: Duration = 0.1,
        wait_time: Duration = 20.0,
        pollers: int = 1,
        max_pollers: int | None = None,
        rate_limit: RateLimit | float | None = None,
        visibility_timeout: Duration | None = None,
        heartbeat: Heartbeat | bool = True,
        retry: RetryPolicy | None = None,
        dead_letter: DeadLetterPolicy | None = None,
        shutdown: ShutdownPolicy | None = None,
        handler_timeout: Duration | None = None,
        ordering: Literal["auto", "fifo", "none"] = "auto",
        strict_ordering: bool = True,
        envelopes: Sequence[Envelope] | None = None,
        middleware: Sequence[ConsumeMiddleware] = (),
        auto_ack: bool = True,
        settle_linger: Duration = 0.005,
        sns_unwrap: bool = True,
        auto_confirm_subscriptions: bool = False,
        delete_offloaded_payloads: bool = False,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        if prefetch is not None and prefetch < 0:
            raise ValueError("prefetch must be non-negative")
        if not 1 <= batch_size <= 10:
            raise ValueError("batch_size must be between 1 and 10")
        if min_receive_batch is not None and not 1 <= min_receive_batch <= batch_size:
            raise ValueError(f"min_receive_batch must be between 1 and batch_size ({batch_size})")
        receive_batch_wait = seconds(receive_batch_wait, "receive_batch_wait")
        if receive_batch_wait > 20:
            raise ValueError(f"receive_batch_wait must be between 0 and 20 seconds, got {receive_batch_wait}")
        wait_time = seconds(wait_time, "wait_time")
        if wait_time > 20:
            raise ValueError(f"wait_time must be between 0 and 20 seconds, got {wait_time}")
        visibility_timeout = optional_seconds(visibility_timeout, "visibility_timeout")
        if visibility_timeout is not None and visibility_timeout > 43200:
            raise ValueError(f"visibility_timeout must be between 0 and 43200 seconds, got {visibility_timeout}")
        handler_timeout = optional_seconds(handler_timeout, "handler_timeout")
        if handler_timeout is not None and handler_timeout <= 0:
            raise ValueError(f"handler_timeout must be positive (or None), got {handler_timeout}")
        if not 1 <= pollers <= 64:
            raise ValueError("pollers must be between 1 and 64")
        if max_pollers is not None and not pollers <= max_pollers <= 64:
            raise ValueError(f"max_pollers must be between pollers ({pollers}) and 64")
        settle_linger = seconds(settle_linger, "settle_linger")
        if settle_linger > 1:
            raise ValueError(f"settle_linger must be between 0 and 1 second, got {settle_linger}")
        self._client = client
        self.queue = queue
        self.router = router
        self.name = name or queue.name
        self.concurrency = concurrency
        # One receive batch per poller ahead of the handlers (the next receive overlaps with them),
        # but not more than the handlers can take at once; extra receive loops bring one each too.
        self.prefetch = min(batch_size, concurrency) * pollers if prefetch is None else prefetch
        self._extra_poller_slots = min(batch_size, concurrency) if prefetch is None else 0
        self.batch_size = batch_size
        self.min_receive_batch = min_receive_batch
        self.receive_batch_wait = receive_batch_wait
        self.wait_time = wait_time
        self.pollers = pollers
        self.max_pollers = max_pollers
        self.rate_limit = RateLimit(rate_limit) if isinstance(rate_limit, int | float) else rate_limit
        self.visibility_timeout = visibility_timeout
        self.heartbeat = heartbeat
        self.retry_policy = retry or RetryPolicy()
        self.dead_letter_policy = dead_letter or DeadLetterPolicy()
        self.shutdown_policy = shutdown or ShutdownPolicy()
        self.handler_timeout = handler_timeout
        self.ordering = ordering
        self.strict_ordering = strict_ordering
        self._envelopes = tuple(envelopes) if envelopes is not None else None
        self._middleware = tuple(middleware)
        self.auto_ack = auto_ack
        self.settle_linger = settle_linger
        self.sns_unwrap = sns_unwrap
        self.auto_confirm_subscriptions = auto_confirm_subscriptions
        self.delete_offloaded_payloads = delete_offloaded_payloads
        self._state: ConsumerState = "created"
        self._engine: Any = None
        self._dispatcher: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._semaphore: asyncio.Semaphore | None = None
        self._stopping = False
        self._stop_event: asyncio.Event | None = None
        self._force_stop = False
        self._dead_letter_url: str | None = None
        self._fifo = queue.fifo
        self._sequenced = False  # FIFO per-group sequencing (resolved on start)
        self._unwrap_with: tuple[Envelope, ...] = ()  # envelopes tried (resolved on start)
        self._envelope_names: tuple[str, ...] = ()
        self._sequencer = _GroupSequencer(self)
        # Receives of a FIFO message that were not processing attempts: handed back unprocessed
        # while an earlier message of its group was retried (by SQS message id).
        self._handed_back: dict[str, int] = {}
        self._settler = _Settler(self)
        self._stats = _Stats()
        self._active = 0  # handlers holding a slot
        self._waiting = 0  # messages scheduled for a handler that have no slot yet
        self._blocked = 0  # handlers waiting for a settlement they requested
        self._group_waits = 0  # settlements of sequenced messages whose group's next message waits
        self._last_activity = 0.0
        self._starting: asyncio.Future[None] | None = None  # resolved when a start ends
        self._stopped: asyncio.Future[None] | None = None  # the stop in progress (or the last one)
        self._drain_deadline: float | None = None  # when the stop in progress stops waiting for handlers
        self._hurry: asyncio.Event | None = None  # set when a later stop request moved the deadline
        self._aborting = False  # the stop in progress aborts the long poll in flight
        self._abort_receiving: asyncio.Future[None] | None = None
        self._slot_waiters: set[asyncio.Task[Any]] = set()  # tasks of messages waiting for a slot
        self._releasing: set[asyncio.Task[Any]] = set()  # waiters cancelled to release their message

    # -- routing shortcuts -----------------------------------------------------------------

    def route(self, *args: Any, **kwargs: Any) -> Any:
        """Registers a route on this consumer's router (see :meth:`fanferro.Router.route`)."""
        return self.router.route(*args, **kwargs)

    def default(self, handler: Handler) -> Handler:
        """Registers the fallback route (see :meth:`fanferro.Router.default`)."""
        return self.router.default(handler)

    # -- properties ------------------------------------------------------------------------

    @property
    def state(self) -> ConsumerState:
        """Lifecycle state."""
        return self._state

    @property
    def queue_name(self) -> str:
        """Name of the consumed queue."""
        return self.queue.name

    @property
    def envelopes(self) -> tuple[Envelope, ...]:
        """Envelopes tried when unwrapping, in order."""
        return self._envelopes or self._client.accept_envelopes or default_envelopes()

    @property
    def in_flight(self) -> int:
        """Messages currently being processed by handlers."""
        return self._active

    def stats(self) -> dict[str, Any]:
        """Processing statistics (Python outcomes plus the Rust engine's counters)."""
        engine: dict[str, Any] = {}
        if self._engine is not None:
            raw = self._engine.stats()
            engine = {**raw.get("metrics", {}), "state": raw.get("state"), "leases": raw.get("leases")}
        return {
            "name": self.name,
            "queue": self.queue.name,
            "state": self._state,
            "active": self._active,
            **self._stats.snapshot(),
            "engine": engine,
        }

    def pause(self) -> None:
        """Stops receiving new messages (in-flight messages continue)."""
        if self._engine is not None:
            self._engine.pause()

    def resume(self) -> None:
        """Resumes receiving."""
        if self._engine is not None:
            self._engine.resume()

    # -- lifecycle -------------------------------------------------------------------------

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    async def start(self) -> None:
        """Resolves the queue (and its subscriptions), starts receiving and dispatching."""
        if self._state not in ("created", "stopped"):
            raise ConsumerStateError(f"consumer {self.name!r} is {self._state}")
        if not len(self.router):
            raise ConsumerStateError(f"consumer {self.name!r} has no routes; add a handler or routes")
        self._state = "starting"
        starting = self._starting = asyncio.get_running_loop().create_future()
        # Registered right away: closing the client meanwhile stops the consumer once it started.
        self._client._register_consumer(self)
        self._emit(EventKind.CONSUMER_STARTING, f"consumer {self.name} starting")
        try:
            await self._start()
        except BaseException:
            self._state = "stopped"
            self._client._unregister_consumer(self)
            raise
        finally:
            self._starting = None
            starting.set_result(None)

    async def _start(self) -> None:
        client = self._client
        info = await self.queue.resolve()
        await client._provision_subscriptions_for(self.queue)
        self._dead_letter_url = await self._resolve_dead_letter_url(info)
        visibility = self.visibility_timeout or info.visibility_timeout or 30
        heartbeat: dict[str, Any]
        if self.heartbeat is False:
            heartbeat = {"enabled": False}
        elif isinstance(self.heartbeat, Heartbeat):
            heartbeat = self.heartbeat.to_core()
        else:
            heartbeat = {"enabled": True}
        config = {
            "queue_url": info.url,
            "queue_arn": info.arn,
            "fifo": info.fifo,
            "max_in_flight": self.concurrency + self.prefetch,
            "batch_size": self.batch_size,
            "min_receive_batch": self.min_receive_batch,
            "receive_batch_wait": float(self.receive_batch_wait),
            "wait_time": float(self.wait_time),
            "pollers": self.pollers,
            "max_pollers": self.max_pollers,
            "extra_poller_slots": self._extra_poller_slots,
            "rate_limit": (
                {"per_second": self.rate_limit.per_second, "burst": self.rate_limit.burst} if self.rate_limit else None
            ),
            "visibility_timeout": float(visibility),
            "heartbeat": heartbeat,
            "settle": {"linger": float(self.settle_linger)},
            "dead_letter_queue_url": self._dead_letter_url,
            # It may be of the other type (DeadLetterPolicy(queue=...)): FIFO needs a group id.
            "dead_letter_fifo": self._dead_letter_url.endswith(".fifo") if self._dead_letter_url else None,
            "sns_unwrap": "auto" if self.sns_unwrap else "never",
            "auto_confirm_subscriptions": self.auto_confirm_subscriptions,
            "verify_md5": client.verify_md5,
        }
        self._check_configuration(info)
        self._sequenced = self.ordering == "fifo" or (self.ordering == "auto" and self._fifo)
        self._unwrap_with = self.envelopes
        self._envelope_names = tuple(envelope.name for envelope in self._unwrap_with)
        core = await client._core()
        # Built-in envelopes are unwrapped by the engine while it decodes messages (off the event
        # loop); custom ones here, for each message.
        builtin = [envelope for envelope in self._unwrap_with if isinstance(envelope, _BUILTIN_ENVELOPES)]
        self._engine = core.consumer(config, builtin if len(builtin) == len(self._unwrap_with) else None)
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._stop_event = asyncio.Event()
        self._stopping = False
        self._force_stop = False
        self._active = self._waiting = self._blocked = self._group_waits = 0
        # A group task cancelled before it ran (stopping) left its group behind in the last one.
        self._sequencer = _GroupSequencer(self)
        client._acquire_pump()
        try:
            self._engine.start()
        except BaseException:
            client._release_pump()
            raise
        self._last_activity = time.monotonic()
        self._dispatcher = asyncio.get_running_loop().create_task(self._dispatch_loop(), name=f"fanferro-{self.name}")
        self._state = "running"

    def _check_configuration(self, info: Any) -> None:
        redrive = info.redrive_policy
        max_attempts = self.retry_policy.max_attempts
        if redrive and max_attempts is not None:
            try:
                redrive_count = int(redrive.get("maxReceiveCount", 0))
            except (TypeError, ValueError):
                redrive_count = 0
            if redrive_count and self.retry_policy.strategy == "visibility" and max_attempts > redrive_count:
                self._emit(
                    EventKind.CONFIG_WARNING,
                    f"RetryPolicy.max_attempts ({max_attempts}) exceeds the queue's maxReceiveCount ({redrive_count}); "
                    "SQS will move messages to the dead-letter queue before fanferro's retries are exhausted",
                )
        if not self._dead_letter_url and "dead_letter" in (
            self.dead_letter_policy.on_exhausted,
            self.dead_letter_policy.on_decode_error,
            self.dead_letter_policy.on_unroutable,
        ):
            self._emit(
                EventKind.CONFIG_WARNING,
                f"no dead-letter queue is configured for {self.queue.name}; messages that should be dead-lettered "
                f"will be handled with on_missing_queue={self.dead_letter_policy.on_missing_queue!r}",
            )
        if self.retry_policy.strategy == "requeue" and info.fifo:
            self._emit(
                EventKind.CONFIG_WARNING,
                "RetryPolicy(strategy='requeue') on a FIFO queue sends a failed message behind the rest of its "
                "message group, which then loses its order; strategy='visibility' keeps the group waiting instead",
            )
        visibility = self.visibility_timeout or info.visibility_timeout
        if self.heartbeat is False and self.handler_timeout and visibility and self.handler_timeout > visibility:
            self._emit(
                EventKind.CONFIG_WARNING,
                "handler_timeout exceeds the visibility timeout while heartbeats are disabled; "
                "messages may be redelivered while still being processed",
            )

    async def _resolve_dead_letter_url(self, info: Any) -> str | None:
        client = self._client
        if self.dead_letter_policy.queue is not None:
            return (await client.queue(self.dead_letter_policy.queue).resolve()).url
        if self.queue.dead_letter_queue is not None:
            return (await self.queue.dead_letter_queue.resolve()).url
        redrive = info.redrive_policy
        target = redrive.get("deadLetterTargetArn") if redrive else None
        if isinstance(target, str) and target.count(":") >= 5:
            parts = target.split(":")
            core = await client._core()
            report = await core.ensure_queue(
                {"name": parts[5], "owner_account_id": parts[4], "policy": {"lookup": True, "verify": False}}
            )
            resource = report.get("resource")
            if resource:
                return resource["url"]
        return None

    def request_stop(self) -> None:
        """Asks :meth:`run` to stop (safe to call from signal handlers).

        A second request skips the drain: a stop in progress stops waiting for handlers (they are
        cancelled) and aborts the long poll in flight.
        """
        if self._stop_event is not None:
            if self._stop_event.is_set():
                self._force_stop = True
                self._hurry_stop(0, True)
            self._stop_event.set()

    async def run(self, *, install_signal_handlers: bool = True) -> None:
        """Starts the consumer and processes messages until stopped, then shuts down gracefully.

        Stops on :meth:`request_stop`, :meth:`stop` or SIGINT/SIGTERM (a second signal skips the
        drain).
        """
        await self.start()
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        if install_signal_handlers:
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, self.request_stop)
                    installed.append(sig)
                except (NotImplementedError, RuntimeError, ValueError):
                    break
        try:
            assert self._stop_event is not None
            await self._stop_event.wait()
        finally:
            # The handlers stay installed while the consumer drains: a second signal cuts it short.
            try:
                if self._force_stop:
                    await self.stop(drain_timeout=0, abort_receives=True)
                else:
                    await self.stop()
            finally:
                for sig in installed:
                    loop.remove_signal_handler(sig)

    async def run_until_idle(self, *, idle_for: Duration = 0.3, timeout: Duration = 30.0) -> None:
        """Processes messages until nothing has happened for ``idle_for`` seconds, then stops.

        Intended for tests and batch jobs. Raises :class:`TimeoutError` if the queue does not go
        idle within ``timeout`` seconds (the consumer is stopped either way). Both take seconds or
        a ``timedelta``.
        """
        idle_for = seconds(idle_for, "idle_for")
        timeout = seconds(timeout, "timeout")
        if self._state != "running":
            await self.start()
        deadline = time.monotonic() + timeout
        try:
            while True:
                now = time.monotonic()
                self._forget_finished_tasks()
                engine_in_flight = 0
                if self._engine is not None:
                    engine_in_flight = int(self._engine.stats().get("metrics", {}).get("in_flight", 0))
                idle = not self._tasks and engine_in_flight == 0
                if idle and now - self._last_activity >= idle_for:
                    # Locally idle: make sure the queue has nothing visible left either.
                    counts = await self.queue.approximate_count()
                    if counts["visible"] == 0:
                        return
                    self._last_activity = time.monotonic()
                if now >= deadline:
                    raise TimeoutError(f"consumer {self.name!r} did not become idle within {timeout} seconds")
                if not idle:
                    self._last_activity = now
                await asyncio.sleep(min(0.05, idle_for / 4))
        finally:
            await self.stop()

    async def stop(self, *, drain_timeout: Duration | Any | None = ..., abort_receives: bool | None = None) -> None:
        """Gracefully stops the consumer (see :class:`~fanferro.ShutdownPolicy`).

        A consumer that is starting is stopped as soon as its start completed. While a stop is in
        progress, ``stop()`` waits for it; a shorter ``drain_timeout`` or ``abort_receives=True``
        then cuts it short (``drain_timeout=0`` stops waiting for handlers at once).

        Args:
            drain_timeout: Overrides the policy's drain timeout (seconds or a ``timedelta``; ``None``
                waits forever).
            abort_receives: Overrides the policy's ``abort_receives``.
        """
        if drain_timeout is not ...:
            drain_timeout = optional_seconds(drain_timeout, "drain_timeout")
        if self._state == "starting" and self._starting is not None:
            await asyncio.shield(self._starting)
        if self._state == "running":
            self._begin_stop(drain_timeout, abort_receives)
        elif self._state == "stopping":
            self._hurry_stop(drain_timeout, abort_receives)
        else:
            return
        assert self._stopped is not None
        # Shielded: a cancelled caller must not interrupt the shutdown (it releases messages).
        await asyncio.shield(self._stopped)

    def _begin_stop(self, drain_timeout: float | Any | None, abort_receives: bool | None) -> None:
        """Moves to ``stopping`` at once (later requests see it) and runs the shutdown as a task."""
        policy = self.shutdown_policy
        timeout = policy.drain_timeout if drain_timeout is ... else drain_timeout
        self._drain_deadline = None if timeout is None else time.monotonic() + timeout
        self._hurry = asyncio.Event()
        self._aborting = policy.abort_receives if abort_receives is None else abort_receives
        self._abort_receiving = None
        self._state = "stopping"
        self._stopping = True
        if self._stop_event is not None:
            self._stop_event.set()
        self._stopped = asyncio.ensure_future(self._stop())

    def _hurry_stop(self, drain_timeout: float | Any | None, abort_receives: bool | None) -> None:
        """Applies a later stop request to the stop in progress: an earlier deadline, an aborted poll."""
        if self._state != "stopping":
            return
        if drain_timeout is not ... and drain_timeout is not None:
            deadline = time.monotonic() + drain_timeout
            if self._drain_deadline is None or deadline < self._drain_deadline:
                self._drain_deadline = deadline
                if self._hurry is not None:
                    self._hurry.set()
        if abort_receives and not self._aborting and self._engine is not None:
            self._aborting = True
            self._abort_receiving = asyncio.ensure_future(self._engine.stop_receiving(True))

    async def _stop(self) -> None:
        policy = self.shutdown_policy
        self._emit(EventKind.CONSUMER_STOPPING, f"consumer {self.name} stopping", in_flight=self._active)
        engine = self._engine
        if not policy.release_unfinished:
            # Also the messages of cancelled tasks and of FIFO groups that never reached a handler.
            engine.leave_unsettled()
        stop_receiving = asyncio.ensure_future(engine.stop_receiving(self._aborting))
        try:
            # The dispatcher exits on its own: `next_batch` returns as soon as receiving stops.
            if self._dispatcher is not None:
                try:
                    await asyncio.wait_for(asyncio.shield(self._dispatcher), timeout=5.0)
                except TimeoutError:
                    self._dispatcher.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await self._dispatcher
            if policy.release_prefetched:
                self._release_waiting_messages()
            # Handlers keep running while a final long poll completes.
            await self._drain()
            await stop_receiving
            buffered = engine.drain_buffered()
            deadline = self._drain_deadline
            if not policy.release_prefetched and (deadline is None or time.monotonic() < deadline):
                for delivery in buffered:
                    self._schedule(delivery)
            elif policy.release_prefetched or policy.release_unfinished:
                # Past the drain deadline their tasks would be cancelled before they ran.
                await asyncio.gather(*(self._release_quietly(delivery) for delivery in buffered))
            # Otherwise they are left to time out, like unfinished messages (engine.leave_unsettled()).
            await self._drain()
            if self._tasks:
                for task in list(self._tasks):
                    task.cancel()
                await asyncio.wait(list(self._tasks), timeout=policy.cancel_timeout)
                self._forget_finished_tasks()
        finally:
            if not stop_receiving.done():
                with contextlib.suppress(Exception):
                    await stop_receiving
            if self._abort_receiving is not None:
                with contextlib.suppress(Exception):
                    await self._abort_receiving
            await engine.close(policy.release_unfinished)
            self._client._unregister_consumer(self)
            self._client._release_pump()
            self._state = "stopped"

    def _release_waiting_messages(self) -> None:
        """Releases the prefetched messages that wait for a handler slot (stopping: they never start)."""
        for task in list(self._slot_waiters):
            self._releasing.add(task)
            task.cancel()

    async def _drain(self) -> None:
        """Waits for the message tasks until they end or the drain deadline passes."""
        if not self._tasks:
            return
        started = reported = time.monotonic()
        interval = self.shutdown_policy.progress_interval
        hurry = self._hurry
        while self._tasks:
            now = time.monotonic()
            deadline = self._drain_deadline
            if deadline is not None and now >= deadline:
                return
            wait = interval - (now - reported) if deadline is None else min(interval - (now - reported), deadline - now)
            waiters: set[asyncio.Future[Any]] = set(self._tasks)
            hurried = asyncio.ensure_future(hurry.wait()) if hurry is not None else None
            if hurried is not None:
                waiters.add(hurried)
            try:
                done, _pending = await asyncio.wait(waiters, timeout=max(wait, 0), return_when=asyncio.FIRST_COMPLETED)
            finally:
                if hurried is not None:
                    hurried.cancel()
            if hurry is not None:
                hurry.clear()
            self._tasks.difference_update(done)  # also tasks cancelled before they started
            now = time.monotonic()
            if self._tasks and now - reported >= interval:
                reported = now
                self._emit(
                    EventKind.CONSUMER_DRAINING,
                    f"waiting for {len(self._tasks)} in-flight messages",
                    in_flight=len(self._tasks),
                    waited=round(now - started, 3),
                )

    # -- dispatching -----------------------------------------------------------------------

    async def _dispatch_loop(self) -> None:
        engine = self._engine
        while not self._stopping:
            try:
                deliveries = await engine.next_batch(self.concurrency + self.prefetch, _DELIVERY_WAIT)
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - deliberate catch-all; the error is reported, never swallowed silently
                self._emit(
                    EventKind.CONSUMER_ERROR, f"consumer {self.name} failed to fetch deliveries", exception=error
                )
                await asyncio.sleep(1.0)
                continue
            if not deliveries:
                continue
            self._last_activity = time.monotonic()
            self._client._telemetry.record_consumed(self.queue.name, len(deliveries))
            self._schedule_all(deliveries)
            # Not kept across the next wait for deliveries: a delivery nothing else holds must be
            # dropped (released) when its task ends.
            del deliveries

    def _schedule_all(self, deliveries: list[Delivery]) -> None:
        for delivery in deliveries:
            self._schedule(delivery)

    def _schedule(self, delivery: Delivery) -> None:
        if self._sequenced and (group := delivery.group_id) is not None:
            self._sequencer.submit(group, delivery)
        else:
            self._waiting += 1
            self._spawn(self._run(delivery, counted=True, task=True))

    def _spawn(self, coroutine: Any) -> None:
        """Runs ``coroutine`` as a task (it calls :meth:`_task_ended` when it ends: no done callback)."""
        self._tasks.add(asyncio.get_running_loop().create_task(coroutine))

    def _task_ended(self) -> None:
        """Forgets the running task (called by the task itself, at its very end)."""
        self._tasks.discard(asyncio.current_task())  # type: ignore[arg-type]
        self._last_activity = time.monotonic()

    def _forget_finished_tasks(self) -> None:
        """Forgets tasks that ended without :meth:`_task_ended` (cancelled before they ever ran)."""
        self._tasks.difference_update([task for task in self._tasks if task.done()])

    def _all_waiting(self) -> bool:
        """Whether every handler that could request another settlement waits for one."""
        return self._blocked >= self._active and (self._waiting == 0 or self._active >= self.concurrency)

    def _awaited_settlements(self) -> int:
        """Settlements that handlers and message groups wait for (see :meth:`_Settler._call`)."""
        return self._blocked + self._group_waits

    async def _acquire_slot(self, semaphore: asyncio.Semaphore) -> bool:
        """Waits for a handler slot; ``False`` when stopping released the message instead."""
        task = asyncio.current_task()
        assert task is not None
        self._slot_waiters.add(task)
        try:
            await semaphore.acquire()
        except asyncio.CancelledError:
            if task not in self._releasing:
                raise
            self._releasing.discard(task)
            task.uncancel()
            return False
        finally:
            self._slot_waiters.discard(task)
        return True

    def _note_handed_back(self, delivery: Delivery) -> None:
        """Counts a hand-back of ``delivery``: its next receive is not a processing attempt."""
        handed_back = self._handed_back
        count = handed_back.pop(delivery.sqs_message_id, 0) + 1
        handed_back[delivery.sqs_message_id] = count  # most recent last
        if len(handed_back) > _MAX_HANDED_BACK:
            del handed_back[next(iter(handed_back))]

    def _forget_handed_back(self, delivery: Delivery) -> None:
        """Forgets the hand-backs of a message that left the queue."""
        if self._handed_back:
            self._handed_back.pop(delivery.sqs_message_id, None)

    def _task_failed(self, error: Exception) -> None:
        self._emit(EventKind.CONSUMER_ERROR, f"unexpected error in consumer {self.name}", exception=error)

    async def _release_quietly(self, delivery: Delivery) -> None:
        with contextlib.suppress(FanferroError):
            await delivery.release()

    async def _run(self, delivery: Delivery, *, counted: bool = False, task: bool = False) -> Outcome | None:
        """Processes one delivery; ``task``: this call is the message's whole task (see :meth:`_spawn`)."""
        try:
            semaphore = self._semaphore
            assert semaphore is not None
            if self._stopping and self.shutdown_policy.release_prefetched:
                if counted:
                    self._waiting -= 1
                await self._release_quietly(delivery)
                return "released"
            if not counted:
                self._waiting += 1
            try:
                if not semaphore.locked():
                    await semaphore.acquire()  # a free slot: no wait
                elif not await self._acquire_slot(semaphore):
                    await self._release_quietly(delivery)
                    return "released"
            finally:
                self._waiting -= 1
            if self._stopping and self.shutdown_policy.release_prefetched:
                semaphore.release()  # the slot came free while the consumer began to stop
                await self._release_quietly(delivery)
                return "released"
            delivery.mark_active()
            self._active += 1
            slot = _Slot(self, semaphore)
            try:
                return await self._process(delivery, slot)
            finally:
                slot.release()
        except Exception as error:  # noqa: BLE001 - deliberate catch-all; the error is reported, never swallowed silently
            self._task_failed(error)
            await self._settle_failed_task(delivery)
            return None
        finally:
            if task:
                self._task_ended()

    async def _settle_failed_task(self, delivery: Delivery) -> None:
        """Settles a message whose task failed unexpectedly, instead of leaving it leased.

        A processed message is retried after the retry policy's backoff (its next receive counts
        as an attempt, so ``max_attempts`` and the dead-letter policy still apply); one that never
        started is released.
        """
        status = delivery.lease_status
        if status == "buffered":
            await self._release_quietly(delivery)
        elif status == "active":
            try:
                delay = self.retry_policy.backoff.delay(max(delivery.receive_count, 1))
                await delivery.nack(min(delay, _MAX_VISIBILITY_DELAY))
            except Exception:  # noqa: BLE001 - last resort: make it visible again
                await self._release_quietly(delivery)

    # -- processing ------------------------------------------------------------------------

    def _emit(
        self,
        kind: EventKind,
        text: str,
        *,
        message: Message[Any] | None = None,
        exception: BaseException | None = None,
        **fields: Any,
    ) -> None:
        events = self._client.events
        if not events.is_enabled(kind):
            return
        context: dict[str, Any] = {"consumer": self.name, "queue": self.queue.name}
        if message is not None:
            context.update(
                message_id=message.id,
                sqs_message_id=message.sqs_message_id,
                message_type=message.type,
                attempt=message.attempt,
                group_id=message.group_id,
            )
        context.update(fields)
        events.emit(kind, text, exception=exception, **context)

    def _build_message(self, delivery: Delivery) -> Message[Any]:
        # Raises decoding errors (transport, or the envelope that recognised the message).
        payload, index, attributes, visible = delivery.parts()
        if payload is None or index is None:  # custom envelopes: unwrapped here
            wire = delivery.wire
            assert wire is not None
            payload, envelope = unwrap_first(self._unwrap_with or self.envelopes, wire)
            name = envelope.name
        else:
            name = self._envelope_names[index]
        message = Message._received(payload, attributes, visible, name, delivery, self.queue.name, self._settler)
        if self._handed_back:
            message._handed_back = self._handed_back.get(delivery.sqs_message_id, 0)
        return message

    def _decode_body(self, message: Message[Any], route: Route) -> Any:
        target = route.body_type
        content_type = message.content_type
        if target is Any or target is object:
            if content_type is None:
                if message.data[:1] in (b"{", b"["):
                    return message.json()
                return message.data
            if _is_json(content_type):
                return message.json()
            essence = media_type(content_type)
            if essence is not None and essence.startswith("text/"):
                return self._client.codecs.decode(message.data, str, content_type)
            return message.data
        return self._client.codecs.decode(message.data, target, content_type)

    async def _process(self, delivery: Delivery, slot: _Slot) -> Outcome | None:
        started = monotonic()
        try:
            message = self._build_message(delivery)
        except Exception as error:  # noqa: BLE001 - deliberate catch-all; the error is reported, never swallowed silently
            slot.release()
            if self.delete_offloaded_payloads and _is_missing_payload(error):
                return await self._payload_gone(delivery)
            self._stats.decode_failures += 1
            return await self._undecodable(delivery, error)
        message._slot = slot
        telemetry = self._client._telemetry
        with telemetry.process_span(message, self.queue.name) as span:
            error: BaseException | None = None
            try:
                await self._handle(message, slot)
            except asyncio.CancelledError:
                if not message.settled and self.shutdown_policy.release_unfinished:
                    with contextlib.suppress(Exception):
                        message.outcome = "released"
                        await delivery.release()
                raise
            except Exception as exc:  # noqa: BLE001 - deliberate catch-all; the error is reported, never swallowed silently
                error = exc
                telemetry.mark_error(span, exc)
                await self._failed(message, exc)
            finally:
                telemetry.record_process(self.queue.name, monotonic() - started, error)
                # The traceback refers to this frame: no cycle that holds the delivery until the
                # garbage collector runs.
                error = None
        # None when a settlement failed: the message comes back, so a FIFO group must wait for it.
        return None if message._settlement_failed else message.outcome

    async def _handle(self, message: Message[Any], slot: _Slot) -> None:
        """Routes, decodes and runs the handler while holding ``slot``, then settles without it."""
        policy = self.retry_policy
        # With on_exhausted="retry", retries go on past max_attempts: the handler runs each time.
        if (
            policy.max_attempts is not None
            and message.attempt > policy.max_attempts
            and self.dead_letter_policy.on_exhausted != "retry"
        ):
            slot.release()
            await self._apply_action(message, self.dead_letter_policy.on_exhausted, "maximum attempts exceeded", None)
            return
        route = self.router.resolve(message, self._client.types)
        if route is None:
            self._stats.unroutable += 1
            self._emit(EventKind.MESSAGE_UNROUTABLE, "no route matched the message", message=message)
            slot.release()
            await self._apply_action(message, self.dead_letter_policy.on_unroutable, "no route matched", None)
            return
        try:
            message._set_body(self._decode_body(message, route))
        except Exception as error:  # noqa: BLE001 - deliberate catch-all; the error is reported, never swallowed silently
            self._stats.decode_failures += 1
            self._emit(
                EventKind.MESSAGE_DECODE_FAILED,
                f"cannot decode message body for route {route.name}",
                message=message,
                exception=error,
                route=route.name,
            )
            slot.release()
            await self._apply_action(message, self.dead_letter_policy.on_decode_error, "decode failed", error)
            return
        handler_started = monotonic()
        if self._client.consume_middleware or self._middleware:
            call = chain_consume((*self._client.consume_middleware, *self._middleware), route.invoke)
        else:
            call = route.invoke
        try:
            if self.handler_timeout is not None:
                try:
                    async with asyncio.timeout(self.handler_timeout):
                        await call(message)
                except TimeoutError as error:
                    raise HandlerTimeoutError(
                        f"handler {route.name} exceeded {self.handler_timeout} seconds"
                    ) from error
            else:
                await call(message)
        finally:
            self._stats.handler_seconds += monotonic() - handler_started
            # Settling (auto-ack below, or the failure handling in `_process`) needs no slot.
            slot.release()
        if not message.settled and self.auto_ack:
            try:
                if (
                    self.delete_offloaded_payloads
                    or message.delivery is None
                    or (self._sequenced and self.strict_ordering and message.group_id is not None)
                ):
                    # Awaited: a sequenced group's next message starts once the delete is confirmed.
                    await message.ack()
                else:
                    # The engine deletes the message in the background (batched, tracked until the
                    # consumer closes, failures reported as `message.ack_failed` events): no
                    # awaitable per message.
                    message._begin("acked")
                    message.delivery.ack_in_background()
            except (LeaseError, FanferroError) as error:
                self._emit(EventKind.MESSAGE_ACK_FAILED, "failed to ack message", message=message, exception=error)
                return
        elif not message.settled and message.delivery is not None:
            # Left to time out: heartbeats stop and the slot is free, but the message can still be
            # settled (by a task the handler started) until its visibility timeout runs out.
            message.delivery.leave()
        self._stats.processed += 1
        if self._client.events.is_enabled(EventKind.MESSAGE_PROCESSED):
            self._emit(
                EventKind.MESSAGE_PROCESSED,
                f"message processed by {route.name}",
                message=message,
                route=route.name,
                duration=round(monotonic() - handler_started, 6),
                outcome=message.outcome,
            )

    async def _failed(self, message: Message[Any], error: BaseException) -> None:
        if message.settled and not isinstance(error, HandlerAction):
            self._stats.failed += 1
            self._emit(
                EventKind.MESSAGE_FAILED, "handler failed after settling the message", message=message, exception=error
            )
            return
        if isinstance(error, _Postpone):
            await self._postpone(message, error.delay)
            return
        if isinstance(error, Duplicate):
            self._stats.duplicates += 1
            self._emit(EventKind.MESSAGE_DUPLICATE, "duplicate message skipped", message=message)
            await self._settle_safely(message, "ack", None)
            return
        if isinstance(error, Discard):
            await self._settle_safely(message, "discard", None, reason=str(error))
            return
        self._stats.failed += 1
        action: str
        if isinstance(error, Reject):
            action = "dead_letter"
        elif isinstance(error, Requeue):
            action = "requeue"
        elif isinstance(error, Retry):
            action = "retry"
        else:
            action = self._decide(message, error)
        exhausted = action in ("retry", "requeue") and self.retry_policy.exhausted(message.attempt)
        if exhausted:
            action = self.dead_letter_policy.on_exhausted
        self._emit(
            EventKind.MESSAGE_FAILED,
            f"handler failed ({action}{', retries exhausted' if exhausted else ''})",
            message=message,
            exception=None if isinstance(error, HandlerAction) else error,
            action=action,
            exhausted=exhausted,
            error=str(error),
            error_type=type(error).__qualname__,
        )
        try:
            delay = optional_seconds(getattr(error, "delay", None), "delay")
        except (TypeError, ValueError):
            delay = None  # an exception's own ``delay`` attribute that is not a delay: the backoff applies
        if exhausted:
            await self._apply_action(message, action, "retries exhausted", error)  # type: ignore[arg-type]
        elif action == "dead_letter":
            await self._settle_safely(message, "dead_letter", error, reason=str(error) or "rejected")
        elif action == "requeue":
            await self._settle_safely(message, "requeue", error, delay=delay)
        elif action == "discard":
            await self._settle_safely(message, "discard", error, reason=str(error) or "discarded")
        else:
            await self._settle_safely(message, "retry", error, delay=delay)

    def _decide(self, message: Message[Any], error: BaseException) -> Literal["retry", "dead_letter", "discard"]:
        """The retry policy's decision; one that raises (e.g. its ``classify``) is reported and skipped."""
        policy = self.retry_policy
        try:
            return policy.decide(error)
        except Exception as hook_error:  # noqa: BLE001 - deliberate catch-all; the error is reported, never swallowed silently
            self._emit(
                EventKind.CONSUMER_ERROR,
                f"retry policy of consumer {self.name} failed to classify {type(error).__qualname__}; "
                "the default classification applies",
                message=message,
                exception=hook_error,
            )
        try:
            return dataclasses.replace(policy, classify=None).decide(error)
        except Exception:  # noqa: BLE001 - a subclass whose decide() raises anyway: retry
            return "retry"

    async def _postpone(self, message: Message[Any], delay: float) -> None:
        """Hands ``message`` back for ``delay`` seconds without spending a processing attempt."""
        if message.settled or message.delivery is None:
            return
        delivery = message.delivery
        attributes = message.raw_attributes
        copy = not self._fifo and (len(attributes) < _MAX_ATTRIBUTES or "fanferro.attempt" in attributes)
        message._begin("requeued" if copy else "retried")
        try:
            if copy:
                # The copy's receive is attempt (attempt - 1) + 1: the same attempt as this one.
                attempt = message.attempt - 1
                await self._settler._call(
                    message, lambda delivery: delivery.requeue(attempt, min(delay, _MAX_REQUEUE_DELAY))
                )
            else:
                self._note_handed_back(delivery)  # its next receive here is not an attempt
                await self._settler._call(message, lambda delivery: delivery.nack(min(delay, _MAX_VISIBILITY_DELAY)))
        except (LeaseError, ShutdownError) as settle_error:
            self._emit(
                EventKind.MESSAGE_ACK_FAILED,
                "could not postpone message; it will be redelivered",
                message=message,
                exception=settle_error,
            )
        except FanferroError as settle_error:
            self._emit(
                EventKind.CONSUMER_ERROR,
                "failed to postpone message; it will be redelivered after its visibility timeout",
                message=message,
                exception=settle_error,
            )

    async def _apply_action(
        self, message: Message[Any], action: FailureAction, reason: str, error: BaseException | None
    ) -> None:
        if message.settled:
            return
        message._begin("retried")
        try:
            await self._settler.perform(message, action, reason, error)
        except (LeaseError, ShutdownError) as settle_error:
            self._emit(
                EventKind.MESSAGE_ACK_FAILED,
                f"could not settle message ({action}); it will be redelivered",
                message=message,
                exception=settle_error,
            )
        except FanferroError as settle_error:
            self._emit(
                EventKind.CONSUMER_ERROR,
                f"failed to settle message ({action}); it will be redelivered after its visibility timeout",
                message=message,
                exception=settle_error,
            )

    async def _settle_safely(
        self,
        message: Message[Any],
        operation: Literal["ack", "retry", "requeue", "dead_letter", "discard"],
        error: BaseException | None,
        *,
        reason: str = "",
        delay: float | None = None,
    ) -> None:
        if message.settled:
            return
        try:
            if operation == "ack":
                await message.ack()
            elif operation == "retry":
                await message.retry(delay=delay, error=error)
            elif operation == "requeue":
                await message.requeue(delay=delay, error=error)
            elif operation == "dead_letter":
                await message.dead_letter(reason, error=error)
            else:
                await message.discard(reason)
        except (LeaseError, ShutdownError) as settle_error:
            self._emit(
                EventKind.MESSAGE_ACK_FAILED,
                f"could not settle message ({operation}); it will be redelivered",
                message=message,
                exception=settle_error,
            )
        except FanferroError as settle_error:
            self._emit(
                EventKind.CONSUMER_ERROR,
                f"failed to settle message ({operation}); it will be redelivered after its visibility timeout",
                message=message,
                exception=settle_error,
            )

    async def _undecodable(self, delivery: Delivery, error: BaseException) -> Outcome | None:
        retryable = bool(getattr(error, "retryable", False))
        self._emit(
            EventKind.MESSAGE_DECODE_FAILED,
            "cannot decode message",
            exception=error,
            sqs_message_id=delivery.sqs_message_id,
            receive_count=delivery.receive_count,
            retryable=retryable,
        )
        message = self._undecoded_message(delivery)
        if retryable and not self.retry_policy.exhausted(message.attempt):
            await self._apply_action(message, "retry", "transient decode failure", error)
        else:
            await self._apply_action(message, self.dead_letter_policy.on_decode_error, "decode failed", error)
        return None if message._settlement_failed else message.outcome

    async def _payload_gone(self, delivery: Delivery) -> Outcome | None:
        # The offloaded payload no longer exists: with delete_offloaded_payloads, the ack of
        # another copy of this message (a duplicate delivery or a publisher retry) deleted it, so
        # this copy is skipped as already processed. A payload that expired for another reason (a
        # lifecycle rule) is acked this way too: that is the documented cost of the option.
        message = self._undecoded_message(delivery)
        self._stats.duplicates += 1
        self._emit(
            EventKind.MESSAGE_DUPLICATE,
            "duplicate message skipped: its offloaded payload was already deleted",
            message=message,
            reason="payload_deleted",
        )
        await self._settle_safely(message, "ack", None)
        return None if message._settlement_failed else message.outcome

    def _undecoded_message(self, delivery: Delivery) -> Message[Any]:
        message = Message(
            payload=_raw_payload(delivery),
            attributes=delivery.raw_attributes,
            envelope="undecodable",
            delivery=delivery,
            queue=self.queue.name,
            settler=self._settler,
        )
        message._handed_back = self._handed_back.get(delivery.sqs_message_id, 0)
        return message

    async def _after_ack(self, message: Message[Any]) -> None:
        if not self.delete_offloaded_payloads:
            return
        pointer = message.pointer
        if pointer is None or pointer.bucket is None or pointer.key is None:
            return
        try:
            core = await self._client._core()
            await core.delete_payload(pointer.bucket, pointer.key)
            self._emit(
                EventKind.PAYLOAD_DELETED,
                "offloaded payload deleted",
                message=message,
                bucket=pointer.bucket,
                key=pointer.key,
            )
        except FanferroError as error:
            self._emit(EventKind.CONSUMER_ERROR, "failed to delete offloaded payload", message=message, exception=error)

    def __repr__(self) -> str:
        return f"Consumer({self.name!r}, queue={self.queue.name!r}, state={self._state!r})"


@functools.lru_cache(maxsize=256)
def _is_json(content_type: str) -> bool:
    """:func:`is_json_content_type`, memoized for the few content types a consumer sees."""
    return is_json_content_type(content_type)


def _is_missing_payload(error: BaseException) -> bool:
    """Whether a delivery could not be decoded because S3 no longer has its offloaded payload."""
    return isinstance(error, ResourceNotFoundError) and getattr(error, "code", None) == "NoSuchKey"


def _raw_payload(delivery: Delivery) -> Any:
    from fanferro._core import Payload

    return Payload(delivery.raw_body)
