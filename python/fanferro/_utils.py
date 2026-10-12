"""Internal helpers (not part of the public API)."""

from __future__ import annotations

import asyncio
import inspect
import weakref
from datetime import timedelta
from typing import TYPE_CHECKING, Any, overload

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

__all__ = ["LoopLocal", "maybe_await", "optional_seconds", "seconds"]

type Duration = float | int | timedelta


@overload
def seconds(value: Duration, name: str) -> float: ...
@overload
def seconds(value: None, name: str) -> None: ...
def seconds(value: Duration | None, name: str) -> float | None:
    """Converts a number of seconds or a ``timedelta`` to float seconds (validating it)."""
    if value is None:
        return None
    if isinstance(value, timedelta):
        value = value.total_seconds()
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"{name} must be a number of seconds or a timedelta, got {type(value).__name__}")
    result = float(value)
    if result < 0 or result != result:  # negative or NaN
        raise ValueError(f"{name} must be a non-negative number of seconds, got {value!r}")
    return result


def optional_seconds(value: Duration | None, name: str) -> float | None:
    """Like :func:`seconds` but keeps ``None``."""
    return seconds(value, name)


async def maybe_await[T](value: T | Awaitable[T]) -> T:
    """Awaits ``value`` if it is awaitable (supports sync and async callbacks)."""
    if inspect.isawaitable(value):
        return await value
    return value


def is_async_callable(function: Callable[..., Any]) -> bool:
    """Whether calling ``function`` returns a coroutine (functions, methods, callables, partials)."""
    while hasattr(function, "func"):  # functools.partial
        function = function.func
    if inspect.iscoroutinefunction(function):
        return True
    call = getattr(function, "__call__", None)  # noqa: B004 - callable objects with async __call__
    return call is not None and inspect.iscoroutinefunction(call)


class LoopLocal[T]:
    """Holds one value per running event loop (for loop-bound asyncio primitives).

    asyncio locks, events and queues bind to the loop that first uses them. Clients may be
    used from several loops (e.g. one per test), so loop-bound helpers are created per loop.
    """

    def __init__(self, factory: Callable[[], T]) -> None:
        self._factory = factory
        self._values: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, T] = weakref.WeakKeyDictionary()

    def get(self) -> T:
        """Returns the value for the running loop, creating it on first use."""
        loop = asyncio.get_running_loop()
        try:
            return self._values[loop]
        except KeyError:
            value = self._factory()
            self._values[loop] = value
            return value
