"""Consume SQS messages in AWS Lambda with fanferro's decoding, routing and failure handling.

An SQS event source mapping receives the messages of a queue and invokes the function with them as
the ``Records`` of an event. :func:`sqs_handler` turns a router (or a single handler) into such a
function: each record goes through the same path as a message a :class:`~fanferro.Consumer`
received (SNS unwrapping, claim-check download, base64, envelopes, the attempts check, routing,
codecs, middleware, the handler, the retry and dead-letter policies), and the function reports
the records that are to come back as ``batchItemFailures``::

    client = fanferro.Client(source="urn:service:billing")
    router = fanferro.Router()


    @router.route(OrderPlaced)
    async def on_order(order: OrderPlaced) -> None: ...


    handler = fanferro.aws_lambda.sqs_handler(client, router)  # def handler(event, context) -> dict

How outcomes map to the response:

* acked (the handler returned), discarded (``Discard``, duplicates), dead-lettered (forwarded to
  the dead-letter queue by fanferro) and requeued (a copy sent to the queue) records are left out:
  Lambda deletes them;
* retried, released or unsettled records (``auto_ack=False``), records whose settlement failed and
  records still running when the invocation's time runs out are listed: they stay in the queue. A
  retry also sets the record's visibility timeout to the retry policy's backoff, as a consumer
  does (``ChangeMessageVisibility``; when that fails, the record comes back after the queue's
  visibility timeout);
* on FIFO queues, the records of a message group after one that is listed or requeued are listed
  too, unprocessed, so that the group is delivered again in order.

The event source mapping must have ``ReportBatchItemFailures`` in its ``FunctionResponseTypes``;
otherwise Lambda ignores the response and deletes every record of a successful invocation. Create
the handler with ``report_batch_item_failures=False`` for a mapping without it: an invocation with
failed records then raises :class:`~fanferro.errors.BatchItemFailuresError`, and Lambda returns the
whole batch to the queue.

The handler keeps its event loop, and with it the client's connections, between invocations of a
warm execution environment, and runs Rust events and event hooks to completion before each
invocation returns (Lambda freezes the environment afterwards).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Literal, Protocol

from fanferro._utils import Duration, optional_seconds, seconds
from fanferro.consumer import Consumer
from fanferro.errors import BatchItemFailuresError, ConfigurationError
from fanferro.provisioning import Provisioning
from fanferro.resources import Queue
from fanferro.routing import Router

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from fanferro.client import Client
    from fanferro.envelopes import Envelope
    from fanferro.middleware import ConsumeMiddleware
    from fanferro.policies import DeadLetterPolicy, RetryPolicy
    from fanferro.routing import Handler

__all__ = ["LambdaContext", "SqsEventHandler", "sqs_handler"]


class LambdaContext(Protocol):
    """The part of the Lambda context object a handler uses."""

    def get_remaining_time_in_millis(self) -> int:
        """Milliseconds until the invocation times out."""
        ...


class SqsEventHandler:
    """Processes the SQS events of an AWS Lambda event source mapping. Create it with :func:`sqs_handler`.

    Call it as the function's handler (``handler(event, context)``, on its own event loop, which it
    keeps between invocations), or await :meth:`handle` from code that runs an event loop already
    (tests, async Lambda adapters). Routes can be added on the handler (``@handler.route(...)``).
    """

    def __init__(
        self,
        client: Client,
        router: Router,
        *,
        queue: Queue | None,
        report_batch_item_failures: bool,
        timeout_margin: float,
        consumer_options: dict[str, Any],
    ) -> None:
        self._client = client
        self.router = router
        self.queue = queue
        self.report_batch_item_failures = report_batch_item_failures
        self.timeout_margin = timeout_margin
        self._options = consumer_options
        # Validated now: a mistake shows when the module is imported, not at the first invocation.
        Consumer(client, queue or Queue(client, "options-check"), router, **self._options)
        self._consumers: dict[str, Consumer] = {}
        self._starting: dict[str, asyncio.Task[Consumer]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

    # -- routing shortcuts -----------------------------------------------------------------

    def route(self, *args: Any, **kwargs: Any) -> Any:
        """Registers a route on this handler's router (see :meth:`fanferro.Router.route`)."""
        return self.router.route(*args, **kwargs)

    def default(self, handler: Handler) -> Handler:
        """Registers the fallback route (see :meth:`fanferro.Router.default`)."""
        return self.router.default(handler)

    @property
    def consumers(self) -> dict[str, Consumer]:
        """The consumers that processed records so far, by the ARN of their queue."""
        return dict(self._consumers)

    # -- invocation ------------------------------------------------------------------------

    def __call__(self, event: Mapping[str, Any], context: LambdaContext | Any = None) -> dict[str, Any]:
        """Processes one SQS event (the Lambda function's handler); see :meth:`handle`."""
        loop = self._loop
        if loop is None or loop.is_closed():
            loop = self._loop = asyncio.new_event_loop()
        return loop.run_until_complete(self.handle(event, context))

    async def handle(self, event: Mapping[str, Any], context: LambdaContext | Any = None) -> dict[str, Any]:
        """Processes the records of one SQS event and returns the response for Lambda.

        Args:
            event: The event Lambda invoked the function with (``{"Records": [...]}``).
            context: Lambda's context object: handlers still running ``timeout_margin`` seconds
                before the invocation times out are cancelled, and their records reported as
                failed. ``None`` sets no limit.

        Returns:
            ``{"batchItemFailures": [{"itemIdentifier": <SQS message id>}, ...]}``.

        Raises:
            BatchItemFailuresError: Records failed and the handler was created with
                ``report_batch_item_failures=False``.
            ConfigurationError: A record came from another queue than the handler's ``queue``, or
                names no queue while the handler has none.
            ProvisioningError: The queue could not be resolved (the first time records of it
                arrive). Lambda returns the whole batch to the queue when the function raises.
        """
        records = event.get("Records") or []
        timeout = self._timeout(context)
        client = self._client
        client._acquire_pump()
        try:
            by_queue: dict[str, list[Mapping[str, Any]]] = {}
            for record in records:
                by_queue.setdefault(str(record.get("eventSourceARN") or ""), []).append(record)
            failed: list[str] = []
            for arn, batch in by_queue.items():
                consumer = await self._consumer(arn)
                deleted = await consumer._process_records(batch, timeout)
                failed.extend(
                    str(record["messageId"]) for record in batch if record.get("receiptHandle") not in deleted
                )
        finally:
            await self._flush_events()
        if failed and not self.report_batch_item_failures:
            raise BatchItemFailuresError(
                f"{len(failed)} of {len(records)} records failed; the whole batch returns to the queue",
                failed,
            )
        return {"batchItemFailures": [{"itemIdentifier": message_id} for message_id in failed]}

    def _timeout(self, context: LambdaContext | Any) -> float | None:
        remaining = getattr(context, "get_remaining_time_in_millis", None)
        if remaining is None:
            return None
        return max(remaining() / 1000 - self.timeout_margin, 0.0)

    async def _consumer(self, arn: str) -> Consumer:
        consumer = self._consumers.get(arn)
        if consumer is not None:
            return consumer
        starting = self._starting.get(arn)
        if starting is None:
            starting = self._starting[arn] = asyncio.get_running_loop().create_task(self._start(arn))
        try:
            consumer = await starting
        finally:
            self._starting.pop(arn, None)
        self._consumers[arn] = consumer
        return consumer

    async def _start(self, arn: str) -> Consumer:
        consumer = Consumer(self._client, self._queue_of(arn), self.router, **self._options)
        await consumer._start_records()
        return consumer

    def _queue_of(self, arn: str) -> Queue:
        name = _queue_name(arn)
        if self.queue is not None:
            if name is not None and name != self.queue.name:
                raise ConfigurationError(
                    f"received records of queue {name!r} ({arn}), but this handler consumes {self.queue.name!r}; "
                    "create a handler per queue, or one without a queue"
                )
            return self.queue
        if name is None:
            raise ConfigurationError(
                f"an SQS event record names no queue (eventSourceARN {arn!r}); pass queue= to sqs_handler()"
            )
        declared = self._client.queues.get(name)
        if declared is not None:
            return declared
        # Not declared by the application: the queue exists (Lambda receives from it) and is used as is.
        return self._client.queue(
            name, prefixed=False, owner_account_id=arn.split(":")[4], provisioning=Provisioning.existing()
        )

    async def _flush_events(self) -> None:
        """Dispatches the events of the invocation and waits for running hooks (Lambda freezes after it)."""
        client = self._client
        client._release_pump()
        task = client._pump_task
        if client._pump_users == 0 and task is not None and not task.done():
            await asyncio.wait({task}, timeout=5.0)
        client._drain_core_events()
        await client.events.flush()

    def __repr__(self) -> str:
        return f"SqsEventHandler(queue={self.queue.name if self.queue else None!r}, routes={len(self.router)})"


def _queue_name(arn: str) -> str | None:
    parts = arn.split(":", 5)
    if len(parts) == 6 and parts[0] == "arn" and parts[2] == "sqs" and parts[5]:
        return parts[5]
    return None


def sqs_handler(
    client: Client,
    handler: Handler | Router | None = None,
    /,
    *,
    queue: Queue | str | None = None,
    report_batch_item_failures: bool = True,
    timeout_margin: Duration = 1.0,
    name: str | None = None,
    concurrency: int = 10,
    retry: RetryPolicy | None = None,
    dead_letter: DeadLetterPolicy | None = None,
    handler_timeout: Duration | None = None,
    ordering: Literal["auto", "fifo", "none"] = "auto",
    strict_ordering: bool = True,
    envelopes: Sequence[Envelope] | None = None,
    middleware: Sequence[ConsumeMiddleware] = (),
    auto_ack: bool = True,
    sns_unwrap: bool = True,
    auto_confirm_subscriptions: bool = False,
    delete_offloaded_payloads: bool = False,
) -> SqsEventHandler:
    """A Lambda function handler that processes SQS events with fanferro (see the module docs).

    The queue is resolved like a consumer's queue the first time records of it arrive (also its
    dead-letter queue, from ``dead_letter``, its declaration or its redrive policy), and its
    subscriptions are provisioned according to their policies.

    Args:
        client: The client (create it at module level, so that warm invocations reuse it).
        handler: A handler (receives every message), a :class:`~fanferro.Router`, or ``None`` to
            add routes later with ``@handler.route(...)``.
        queue: The queue the event source mapping receives from (a handle, or a name the client's
            ``queue_prefix`` is prepended to). Records of other queues are refused. Default: the
            queue each record names (``eventSourceARN``), a declared one when the client has it.
        report_batch_item_failures: Return failed records as ``batchItemFailures`` (the event
            source mapping must report them). ``False`` raises
            :class:`~fanferro.errors.BatchItemFailuresError` instead when records failed.
        timeout_margin: Seconds before the invocation times out at which handlers still running
            are cancelled and their records reported as failed (0-60).
        name: Name of the consumers in events and metrics (default: the queue name).
        concurrency: Handlers that run at once.
        retry: Retry policy (default :class:`~fanferro.RetryPolicy`).
        dead_letter: Dead-letter policy (default :class:`~fanferro.DeadLetterPolicy`).
        handler_timeout: Per-message handler timeout in seconds.
        ordering: ``"auto"`` (FIFO queues in order per message group), ``"fifo"`` or ``"none"``.
        strict_ordering: See :class:`~fanferro.Consumer`.
        envelopes: Envelopes tried when unwrapping (default: the client's).
        middleware: Consume middleware (after the client's).
        auto_ack: Ack messages whose handler returned without settling them; with ``False`` such
            records are reported as failed.
        sns_unwrap: Unwrap SNS notifications (subscriptions without raw delivery).
        auto_confirm_subscriptions: Confirm SNS subscription requests that arrive on the queue.
        delete_offloaded_payloads: Delete a message's offloaded payload once it was acked.

    Returns:
        The handler: assign it to the name the function's handler setting names.
    """
    margin = seconds(timeout_margin, "timeout_margin")
    if margin > 60:
        raise ValueError(f"timeout_margin must be between 0 and 60 seconds, got {margin}")
    if isinstance(handler, Router):
        router = handler
    else:
        router = Router()
        if handler is not None:
            router.add(handler, fallback=True)
    queue_handle = client.queue(queue) if isinstance(queue, str) else queue
    options: dict[str, Any] = {
        "name": name,
        "concurrency": concurrency,
        "retry": retry,
        "dead_letter": dead_letter,
        "handler_timeout": optional_seconds(handler_timeout, "handler_timeout"),
        "ordering": ordering,
        "strict_ordering": strict_ordering,
        "envelopes": envelopes,
        "middleware": middleware,
        "auto_ack": auto_ack,
        "sns_unwrap": sns_unwrap,
        "auto_confirm_subscriptions": auto_confirm_subscriptions,
        "delete_offloaded_payloads": delete_offloaded_payloads,
    }
    return SqsEventHandler(
        client,
        router,
        queue=queue_handle,
        report_batch_item_failures=report_batch_item_failures,
        timeout_margin=margin,
        consumer_options=options,
    )
