"""Exceptions handlers raise to decide what happens to the current message.

Example::

    @router.route(OrderCreated)
    async def handle(order: OrderCreated) -> None:
        if not await inventory.available():
            raise fanferro.Retry(delay=60)  # transient: try again in a minute
        if order.total < 0:
            raise fanferro.Reject("negative total")  # permanent: dead-letter now
        if order.test:
            raise fanferro.Discard("test order")  # irrelevant: delete without retries
"""

from __future__ import annotations

__all__ = ["Discard", "HandlerAction", "Reject", "Requeue", "Retry"]


class HandlerAction(Exception):  # noqa: N818 - these are control-flow signals, not errors
    """Base class of handler control-flow exceptions."""


class Retry(HandlerAction):
    """Retry the message (after ``delay`` seconds, or the retry policy's backoff).

    Counts as an attempt: once ``max_attempts`` is reached the message is dead-lettered.
    """

    def __init__(self, reason: str = "retry requested", *, delay: float | None = None) -> None:
        super().__init__(reason)
        self.delay = delay


class Requeue(HandlerAction):
    """Send a copy of the message back to the queue (attempt counter + 1) and delete this one."""

    def __init__(self, reason: str = "requeue requested", *, delay: float | None = None) -> None:
        super().__init__(reason)
        self.delay = delay


class Reject(HandlerAction):
    """The message can never be processed: forward it to the dead-letter queue immediately."""

    def __init__(self, reason: str = "rejected by handler") -> None:
        super().__init__(reason)


class Discard(HandlerAction):
    """The message is irrelevant: delete it without processing or retries."""

    def __init__(self, reason: str = "discarded by handler") -> None:
        super().__init__(reason)
