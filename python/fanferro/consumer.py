"""The consumer runtime: receive, route, decode, handle, settle.

A :class:`Consumer` pulls messages from one queue through the Rust engine (which owns long
polling, rate limiting, in-flight limits, visibility heartbeats and batched settlement) and runs
handlers on the asyncio event loop:

1. transport decoding (SNS unwrapping, claim-check download, base64) happens in Rust;
2. the envelope is unwrapped (first matching envelope);
3. messages past ``RetryPolicy.max_attempts`` are dead-lettered without running a handler;
4. the router picks a route; the body is decoded into the route's type;
5. middleware and the handler run (with an optional timeout) inside an OpenTelemetry span;
6. success acks the message; failures follow the :class:`~fanferro.RetryPolicy` and
   :class:`~fanferro.DeadLetterPolicy`; handlers may also settle explicitly.

FIFO queues are processed in order per message group (groups run concurrently).
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import time
from collections import deque
from typing import TYPE_CHECKING, Any, Literal, Self

from fanferro._core import Delivery, FanferroError, LeaseError, ShutdownError
from fanferro.actions import Discard, HandlerAction, Reject, Requeue, Retry
from fanferro.codecs import is_json_content_type, media_type
from fanferro.envelopes import Envelope, default_envelopes, unwrap_first
from fanferro.errors import ConsumerStateError, HandlerTimeoutError, error_details
from fanferro.events import EventKind
from fanferro.messages import Message, Outcome
from fanferro.middleware import ConsumeMiddleware, chain_consume
from fanferro.policies import DeadLetterPolicy, FailureAction, Heartbeat, RateLimit, RetryPolicy, ShutdownPolicy
from fanferro.telemetry import monotonic

if TYPE_CHECKING:
    from collections.abc import Sequence

    from fanferro.client import Client
    from fanferro.resources import Queue
    from fanferro.routing import Handler, Route, Router

__all__ = ["Consumer", "Duplicate"]

type ConsumerState = Literal["created", "starting", "running", "stopping", "stopped"]

_MAX_VISIBILITY_DELAY = 12 * 3600 - 60
_MAX_REQUEUE_DELAY = 900.0


class Duplicate(Discard):
    """Raised by idempotency middleware for messages that were already processed."""

    def __init__(self, reason: str = "duplicate message") -> None:
        super().__init__(reason)


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


class _Settler:
    """Settles messages for the consumer (used by :class:`Message` methods)."""

    def __init__(self, consumer: Consumer) -> None:
        self._consumer = consumer

    def _delivery(self, message: Message[Any]) -> Delivery:
        if message.delivery is None:
            raise ConsumerStateError("message has no delivery")
        return message.delivery

    async def ack(self, message: Message[Any]) -> None:
        await self._delivery(message).ack()
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
            await self._delivery(message).requeue(message.attempt, min(delay, _MAX_REQUEUE_DELAY))
            message.outcome = "requeued"
        else:
            await self._delivery(message).nack(min(delay, _MAX_VISIBILITY_DELAY))

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
            await self._delivery(message).nack(min(backoff.delay(message.attempt), _MAX_VISIBILITY_DELAY))
        else:
            message.outcome = "retried"
            await self._delivery(message).nack(min(backoff.maximum, _MAX_VISIBILITY_DELAY))

    async def requeue(self, message: Message[Any], delay: float | None, error: BaseException | None) -> None:
        if delay is None:
            delay = self._consumer.retry_policy.backoff.delay(message.attempt)
        await self._delivery(message).requeue(message.attempt, min(delay, _MAX_REQUEUE_DELAY))

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
                await self._delivery(message).ack()
            elif fallback == "retry":
                message.outcome = "retried"
                await self._delivery(message).nack(
                    min(consumer.retry_policy.backoff.delay(message.attempt), _MAX_VISIBILITY_DELAY)
                )
            else:
                message.outcome = "retried"
                await self._delivery(message).nack(min(consumer.retry_policy.backoff.maximum, _MAX_VISIBILITY_DELAY))
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
        await self._delivery(message).dead_letter(reason, details)

    async def release(self, message: Message[Any]) -> None:
        await self._delivery(message).release()

    async def discard(self, message: Message[Any], reason: str) -> None:
        await self._delivery(message).ack()
        self._consumer._emit(EventKind.MESSAGE_DROPPED, f"message dropped: {reason}", message=message, reason=reason)

    async def extend(self, message: Message[Any], timeout: float) -> None:
        await self._delivery(message).extend(timeout)


class _GroupSequencer:
    """Processes messages of the same FIFO group one at a time, in order."""

    def __init__(self, consumer: Consumer) -> None:
        self._consumer = consumer
        self._pending: dict[str, deque[Delivery]] = {}
        self._poisoned: set[str] = set()

    def submit(self, group: str, delivery: Delivery) -> None:
        queue = self._pending.get(group)
        if queue is not None:
            queue.append(delivery)
            return
        self._pending[group] = deque([delivery])
        self._consumer._spawn(self._drain(group))

    async def _drain(self, group: str) -> None:
        queue = self._pending[group]
        try:
            while queue:
                delivery = queue.popleft()
                if group in self._poisoned:
                    # An earlier message of this group was not completed: keep the order by
                    # handing this one back; SQS redelivers the group in sequence.
                    await self._consumer._release_quietly(delivery)
                    continue
                outcome = await self._consumer._run(delivery)
                if outcome not in ("acked", "discarded", "dead_lettered"):
                    self._poisoned.add(group)
        finally:
            self._pending.pop(group, None)
            self._poisoned.discard(group)


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
        prefetch: int = 0,
        batch_size: int = 10,
        wait_time: float = 20.0,
        pollers: int = 1,
        rate_limit: RateLimit | float | None = None,
        visibility_timeout: float | None = None,
        heartbeat: Heartbeat | bool = True,
        retry: RetryPolicy | None = None,
        dead_letter: DeadLetterPolicy | None = None,
        shutdown: ShutdownPolicy | None = None,
        handler_timeout: float | None = None,
        ordering: Literal["auto", "fifo", "none"] = "auto",
        envelopes: Sequence[Envelope] | None = None,
        middleware: Sequence[ConsumeMiddleware] = (),
        auto_ack: bool = True,
        sns_unwrap: bool = True,
        auto_confirm_subscriptions: bool = False,
        delete_offloaded_payloads: bool = False,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        if prefetch < 0:
            raise ValueError("prefetch must be non-negative")
        if not 1 <= batch_size <= 10:
            raise ValueError("batch_size must be between 1 and 10")
        if not 0 <= wait_time <= 20:
            raise ValueError("wait_time must be between 0 and 20 seconds")
        self._client = client
        self.queue = queue
        self.router = router
        self.name = name or queue.name
        self.concurrency = concurrency
        self.prefetch = prefetch
        self.batch_size = batch_size
        self.wait_time = wait_time
        self.pollers = pollers
        self.rate_limit = RateLimit(rate_limit) if isinstance(rate_limit, int | float) else rate_limit
        self.visibility_timeout = visibility_timeout
        self.heartbeat = heartbeat
        self.retry_policy = retry or RetryPolicy()
        self.dead_letter_policy = dead_letter or DeadLetterPolicy()
        self.shutdown_policy = shutdown or ShutdownPolicy()
        self.handler_timeout = handler_timeout
        self.ordering = ordering
        self._envelopes = tuple(envelopes) if envelopes is not None else None
        self._middleware = tuple(middleware)
        self.auto_ack = auto_ack
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
        self._sequencer = _GroupSequencer(self)
        self._settler = _Settler(self)
        self._stats = _Stats()
        self._active = 0
        self._last_activity = 0.0

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
        self._emit(EventKind.CONSUMER_STARTING, f"consumer {self.name} starting")
        try:
            await self._start()
        except BaseException:
            self._state = "stopped"
            raise

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
            "wait_time": float(self.wait_time),
            "pollers": self.pollers,
            "rate_limit": (
                {"per_second": self.rate_limit.per_second, "burst": self.rate_limit.burst} if self.rate_limit else None
            ),
            "visibility_timeout": float(visibility),
            "heartbeat": heartbeat,
            "dead_letter_queue_url": self._dead_letter_url,
            "sns_unwrap": "auto" if self.sns_unwrap else "never",
            "auto_confirm_subscriptions": self.auto_confirm_subscriptions,
            "verify_md5": client.verify_md5,
        }
        self._check_configuration(info)
        core = await client._core()
        self._engine = core.consumer(config)
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._stop_event = asyncio.Event()
        self._stopping = False
        self._force_stop = False
        client._acquire_pump()
        client._register_consumer(self)
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
        """Asks :meth:`run` to stop (safe to call from signal handlers)."""
        if self._stop_event is not None:
            if self._stop_event.is_set():
                self._force_stop = True
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
            for sig in installed:
                loop.remove_signal_handler(sig)
            if self._force_stop:
                await self.stop(drain_timeout=0, abort_receives=True)
            else:
                await self.stop()

    async def run_until_idle(self, *, idle_for: float = 0.3, timeout: float = 30.0) -> None:
        """Processes messages until nothing has happened for ``idle_for`` seconds, then stops.

        Intended for tests and batch jobs. Raises :class:`TimeoutError` if the queue does not go
        idle within ``timeout`` seconds (the consumer is stopped either way).
        """
        if self._state != "running":
            await self.start()
        deadline = time.monotonic() + timeout
        try:
            while True:
                now = time.monotonic()
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

    async def stop(self, *, drain_timeout: float | Any | None = ..., abort_receives: bool | None = None) -> None:
        """Gracefully stops the consumer (see :class:`~fanferro.ShutdownPolicy`).

        Args:
            drain_timeout: Overrides the policy's drain timeout (``None`` waits forever).
            abort_receives: Overrides the policy's ``abort_receives``.
        """
        if self._state not in ("running", "starting"):
            return
        policy = self.shutdown_policy
        timeout = policy.drain_timeout if drain_timeout is ... else drain_timeout
        deadline = None if timeout is None else time.monotonic() + timeout
        self._state = "stopping"
        self._stopping = True
        if self._stop_event is not None:
            self._stop_event.set()
        self._emit(EventKind.CONSUMER_STOPPING, f"consumer {self.name} stopping", in_flight=self._active)
        engine = self._engine
        abort = policy.abort_receives if abort_receives is None else abort_receives
        stop_receiving = asyncio.ensure_future(engine.stop_receiving(abort))
        try:
            # The dispatcher exits on its own: `next_batch` returns as soon as receiving stops.
            if self._dispatcher is not None:
                try:
                    await asyncio.wait_for(asyncio.shield(self._dispatcher), timeout=5.0)
                except TimeoutError:
                    self._dispatcher.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await self._dispatcher
            # Handlers keep running while a final long poll completes.
            await self._drain(deadline)
            await stop_receiving
            for delivery in engine.drain_buffered():
                if policy.release_prefetched:
                    await self._release_quietly(delivery)
                else:
                    self._schedule(delivery)
            await self._drain(deadline)
            if self._tasks:
                for task in list(self._tasks):
                    task.cancel()
                await asyncio.wait(list(self._tasks), timeout=policy.cancel_timeout)
        finally:
            if not stop_receiving.done():
                with contextlib.suppress(Exception):
                    await stop_receiving
            await engine.close(policy.release_unfinished)
            self._client._unregister_consumer(self)
            self._client._release_pump()
            self._state = "stopped"

    async def _drain(self, deadline: float | None) -> None:
        if not self._tasks:
            return
        started = time.monotonic()
        interval = self.shutdown_policy.progress_interval
        while self._tasks:
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                return
            wait = interval if remaining is None else min(interval, remaining)
            done, _pending = await asyncio.wait(list(self._tasks), timeout=wait)
            if not done and self._tasks:
                self._emit(
                    EventKind.CONSUMER_DRAINING,
                    f"waiting for {len(self._tasks)} in-flight messages",
                    in_flight=len(self._tasks),
                    waited=round(time.monotonic() - started, 3),
                )

    # -- dispatching -----------------------------------------------------------------------

    async def _dispatch_loop(self) -> None:
        engine = self._engine
        while not self._stopping:
            try:
                deliveries = await engine.next_batch(self.concurrency + self.prefetch, 0.5)
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
            for delivery in deliveries:
                self._schedule(delivery)

    def _schedule(self, delivery: Delivery) -> None:
        fifo = self.ordering == "fifo" or (self.ordering == "auto" and self._fifo)
        group = delivery.group_id
        if fifo and group is not None:
            self._sequencer.submit(group, delivery)
        else:
            self._spawn(self._run(delivery))

    def _spawn(self, coroutine: Any) -> None:
        task = asyncio.get_running_loop().create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        self._last_activity = time.monotonic()
        if not task.cancelled() and task.exception() is not None:
            self._emit(
                EventKind.CONSUMER_ERROR, f"unexpected error in consumer {self.name}", exception=task.exception()
            )

    async def _release_quietly(self, delivery: Delivery) -> None:
        with contextlib.suppress(FanferroError):
            await delivery.release()

    async def _run(self, delivery: Delivery) -> Outcome | None:
        assert self._semaphore is not None
        async with self._semaphore:
            if self._stopping and self.shutdown_policy.release_prefetched:
                await self._release_quietly(delivery)
                return "released"
            delivery.mark_active()
            self._active += 1
            try:
                return await self._process(delivery)
            finally:
                self._active -= 1

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
        error = delivery.decode_error
        if error is not None:
            raise error
        wire = delivery.wire
        assert wire is not None
        payload, envelope = unwrap_first(self.envelopes, wire)
        return Message(
            payload=payload,
            attributes=wire.attributes,
            envelope=envelope.name,
            delivery=delivery,
            queue=self.queue.name,
            settler=self._settler,
        )

    def _decode_body(self, message: Message[Any], route: Route) -> Any:
        target = route.body_type
        content_type = message.content_type
        codecs = self._client.codecs
        if target is Any or target is object:
            if is_json_content_type(content_type) or (content_type is None and message.data[:1] in (b"{", b"[")):
                return message.json()
            essence = media_type(content_type)
            if essence is not None and essence.startswith("text/"):
                return codecs.decode(message.data, str, content_type)
            return message.data
        return codecs.decode(message.data, target, content_type)

    async def _process(self, delivery: Delivery) -> Outcome | None:
        started = monotonic()
        try:
            message = self._build_message(delivery)
        except Exception as error:  # noqa: BLE001 - deliberate catch-all; the error is reported, never swallowed silently
            self._stats.decode_failures += 1
            return await self._undecodable(delivery, error)
        telemetry = self._client._telemetry
        with telemetry.process_span(message, self.queue.name) as span:
            error: BaseException | None = None
            try:
                await self._handle(message)
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
        return message.outcome

    async def _handle(self, message: Message[Any]) -> None:
        policy = self.retry_policy
        if policy.max_attempts is not None and message.attempt > policy.max_attempts:
            await self._apply_action(message, self.dead_letter_policy.on_exhausted, "maximum attempts exceeded", None)
            return
        route = self.router.resolve(message, self._client.types)
        if route is None:
            self._stats.unroutable += 1
            self._emit(EventKind.MESSAGE_UNROUTABLE, "no route matched the message", message=message)
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
            await self._apply_action(message, self.dead_letter_policy.on_decode_error, "decode failed", error)
            return
        handler_started = monotonic()

        async def invoke(msg: Message[Any]) -> None:
            await route.invoke(msg)

        call = chain_consume((*self._client.consume_middleware, *self._middleware), invoke)
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
        if not message.settled and self.auto_ack:
            try:
                await message.ack()
            except (LeaseError, FanferroError) as error:
                self._emit(EventKind.MESSAGE_ACK_FAILED, "failed to ack message", message=message, exception=error)
                return
        self._stats.processed += 1
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
            action = self.retry_policy.decide(error)
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
        delay = getattr(error, "delay", None)
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
        message = Message(
            payload=_raw_payload(delivery),
            attributes=delivery.raw_attributes,
            envelope="undecodable",
            delivery=delivery,
            queue=self.queue.name,
            settler=self._settler,
        )
        if retryable and not self.retry_policy.exhausted(message.attempt):
            await self._apply_action(message, "retry", "transient decode failure", error)
        else:
            await self._apply_action(message, self.dead_letter_policy.on_decode_error, "decode failed", error)
        return message.outcome

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


def _raw_payload(delivery: Delivery) -> Any:
    from fanferro._core import Payload

    return Payload(delivery.raw_body)
