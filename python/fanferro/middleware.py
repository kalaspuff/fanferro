"""Middleware: wrap publishing and message handling with your own logic.

Publish middleware wraps the *preparation* of each outgoing message (codec, envelope, attributes)
for single publishes, batches and outbox preparation alike. It can modify the message before
calling ``call_next`` and inspect or adjust the prepared result::

    async def add_tenant(context: PublishContext, call_next: PrepareNext) -> PreparedMessage:
        context.message.attributes.setdefault("tenant", current_tenant())
        return await call_next(context)

Consume middleware wraps handler invocation (after routing and decoding)::

    async def timing(message: Message[Any], call_next: HandlerNext) -> None:
        started = time.perf_counter()
        try:
            await call_next(message)
        finally:
            metrics.observe(message.type, time.perf_counter() - started)

Middleware runs in registration order (the first registered is the outermost).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from fanferro.messages import Message, OutgoingMessage, PreparedMessage

__all__ = [
    "ConsumeMiddleware",
    "HandlerNext",
    "PrepareNext",
    "PublishContext",
    "PublishMiddleware",
    "chain_consume",
    "chain_publish",
]


@dataclass(slots=True)
class PublishContext:
    """What publish middleware sees."""

    message: OutgoingMessage
    """The message being published (mutable)."""
    destination: str
    """Queue or topic name."""
    kind: Literal["queue", "topic"]
    """Destination kind."""
    fifo: bool
    """Whether the destination is FIFO."""


type PrepareNext = Callable[[PublishContext], Awaitable[PreparedMessage]]
type PublishMiddleware = Callable[[PublishContext, PrepareNext], Awaitable[PreparedMessage]]
type HandlerNext = Callable[[Message[Any]], Awaitable[None]]
type ConsumeMiddleware = Callable[[Message[Any], HandlerNext], Awaitable[None]]


def chain_publish(middleware: Sequence[PublishMiddleware], final: PrepareNext) -> PrepareNext:
    """Composes publish middleware around ``final``."""
    call = final
    for item in reversed(middleware):
        call = _bind_publish(item, call)
    return call


def _bind_publish(item: PublishMiddleware, call_next: PrepareNext) -> PrepareNext:
    async def call(context: PublishContext) -> PreparedMessage:
        return await item(context, call_next)

    return call


def chain_consume(middleware: Sequence[ConsumeMiddleware], final: HandlerNext) -> HandlerNext:
    """Composes consume middleware around ``final``."""
    call = final
    for item in reversed(middleware):
        call = _bind_consume(item, call)
    return call


def _bind_consume(item: ConsumeMiddleware, call_next: HandlerNext) -> HandlerNext:
    async def call(message: Message[Any]) -> None:
        await item(message, call_next)

    return call
