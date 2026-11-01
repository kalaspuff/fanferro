"""Routing messages to handlers.

A :class:`Router` holds routes; each route has a handler, an optional message type and an optional
matcher. For every message the first matching route wins (higher ``priority`` first, then
registration order). The handler's first parameter annotation decides what it receives:

* ``Message[OrderCreated]`` - the message, with ``message.body`` decoded as ``OrderCreated``;
* ``OrderCreated`` - only the decoded body;
* ``Message`` / no annotation - the message, body decoded from its content type (JSON -> Python
  data, text -> ``str``, otherwise ``bytes``).

Example::

    router = fanferro.Router()


    @router.route(OrderCreated)  # matches messages whose type is OrderCreated's type name
    async def created(order: OrderCreated) -> None: ...


    @router.route("orders.cancelled", when=m.header("version") >= 2)
    async def cancelled_v2(message: fanferro.Message[CancelledV2]) -> None: ...


    @router.default
    async def unknown(message: fanferro.Message) -> None: ...
"""

from __future__ import annotations

import functools
import inspect
import itertools
import typing
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, overload

from fanferro._utils import is_async_callable
from fanferro.errors import RoutingError
from fanferro.matchers import Matcher, MessagePredicate, as_matcher
from fanferro.messages import Message

if TYPE_CHECKING:
    from fanferro.message_types import MessageTypes

__all__ = ["Handler", "Route", "Router"]

type Handler = Callable[..., Awaitable[None]]
"""An async callable taking a :class:`Message` or a decoded body."""

_counter = itertools.count()


@dataclass(frozen=True, slots=True, repr=False)
class Route:
    """A handler and the conditions under which it receives messages."""

    handler: Handler
    body_type: Any
    """Type the body is decoded into (``Any`` = decide by content type)."""
    pass_message: bool
    """Call the handler with the :class:`Message` (``True``) or only the body."""
    name: str
    message_type: type | str | None = None
    """Message type condition (a class resolves to its registered type names)."""
    matcher: Matcher | None = None
    priority: int = 0
    fallback: bool = False
    order: int = field(default_factory=lambda: next(_counter))

    def type_names(self, types: MessageTypes) -> tuple[str, ...] | None:
        """Accepted message type names (``None`` = any type)."""
        if self.message_type is None:
            return None
        if isinstance(self.message_type, str):
            return (self.message_type,)
        return types.names_of(self.message_type)

    def matches(self, message: Message[Any], types: MessageTypes) -> bool:
        """Whether this route accepts ``message``."""
        names = self.type_names(types)
        if names is not None and message.type not in names:
            return False
        return self.matcher is None or self.matcher(message)

    def __repr__(self) -> str:
        parts = [repr(self.name)]
        if self.message_type is not None:
            parts.append(f"message_type={_type_repr(self.message_type)}")
        if self.matcher is not None:
            parts.append(f"when={self.matcher!r}")
        if self.priority:
            parts.append(f"priority={self.priority!r}")
        if self.fallback:
            parts.append("fallback=True")
        return f"Route({', '.join(parts)})"

    async def invoke(self, message: Message[Any]) -> None:
        """Calls the handler."""
        if self.pass_message:
            await self.handler(message)
        else:
            await self.handler(message.body)


def _type_repr(value: type | str) -> str:
    """A message type condition as written in a route: a class by its qualified name, a type name quoted."""
    return value.__qualname__ if isinstance(value, type) else repr(value)


def _unwrap_partial(handler: Callable[..., Any]) -> Callable[..., Any]:
    while isinstance(handler, functools.partial):
        handler = handler.func
    return handler


def _signature_target(handler: Callable[..., Any]) -> Callable[..., Any]:
    if inspect.isfunction(handler) or inspect.ismethod(handler) or isinstance(handler, functools.partial):
        # The signature of a partial leaves out the arguments it binds (its __call__ takes any).
        return handler
    call = getattr(handler, "__call__", None)  # noqa: B004 - callable objects
    return call if call is not None else handler


def _hints_target(handler: Callable[..., Any]) -> Callable[..., Any]:
    # A partial has no annotations of its own: the hints are those of the function it wraps.
    return _signature_target(_unwrap_partial(handler))


def _handler_name(handler: Callable[..., Any]) -> str:
    """The qualified name of a handler (of the function a partial wraps), for route names and errors."""
    return getattr(_unwrap_partial(handler), "__qualname__", None) or repr(handler)


def inspect_handler(handler: Handler, message_type: type | str | None = None) -> tuple[Any, bool]:
    """Determines ``(body_type, pass_message)`` from the handler signature.

    Raises:
        RoutingError: If the handler takes no parameters, is not async, or has unresolvable hints.
    """
    if not is_async_callable(handler):
        raise RoutingError(f"handler {handler!r} must be an async function")
    try:
        signature = inspect.signature(_signature_target(handler))
    except (TypeError, ValueError) as error:
        raise RoutingError(f"cannot inspect handler {handler!r}: {error}") from error
    parameters = [p for p in signature.parameters.values() if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    if not parameters:
        raise RoutingError(f"handler {_handler_name(handler)} must accept the message as its first argument")
    first = parameters[0]
    try:
        hints = typing.get_type_hints(_hints_target(handler), include_extras=True)
    except Exception as error:
        raise RoutingError(
            f"cannot resolve the type hints of {_handler_name(handler)}: {error}. "
            "Define the annotated types at module level or pass the type to route(...) explicitly."
        ) from error
    annotation = hints.get(first.name, inspect.Parameter.empty)
    explicit_class = message_type if isinstance(message_type, type) else None
    if annotation is inspect.Parameter.empty:
        return (explicit_class or Any), True
    if annotation is Message:
        return (explicit_class or Any), True
    if typing.get_origin(annotation) is Message:
        args = typing.get_args(annotation)
        return (args[0] if args else (explicit_class or Any)), True
    return annotation, False


class Router:
    """An ordered collection of routes. Routers can be nested with :meth:`include`."""

    def __init__(self, *, name: str | None = None) -> None:
        self.name = name
        self._routes: list[Route] = []
        self._sorted: list[Route] | None = None

    @property
    def routes(self) -> list[Route]:
        """Routes in evaluation order."""
        return list(self._ordered())

    def _ordered(self) -> list[Route]:
        if self._sorted is None:
            self._sorted = sorted(self._routes, key=lambda r: (r.fallback, -r.priority, r.order))
        return self._sorted

    def add(
        self,
        handler: Handler,
        message_type: type | str | None = None,
        *,
        when: Matcher | MessagePredicate | None = None,
        priority: int = 0,
        name: str | None = None,
        fallback: bool = False,
    ) -> Route:
        """Adds a route (the non-decorator form of :meth:`route`)."""
        body_type, pass_message = inspect_handler(handler, message_type)
        typed = isinstance(body_type, type) and not _is_generic_type(body_type)
        if message_type is None and not fallback and when is None and typed:
            # A typed handler without conditions routes by the body type's message type name.
            message_type = body_type
        route = Route(
            handler=handler,
            body_type=body_type,
            pass_message=pass_message,
            name=name or _handler_name(handler),
            message_type=message_type,
            matcher=as_matcher(when) if when is not None else None,
            priority=priority,
            fallback=fallback,
        )
        self._routes.append(route)
        self._sorted = None
        return route

    @overload
    def route[H: Handler](self, handler: H, /) -> H: ...
    @overload
    def route[H: Handler](
        self,
        message_type: type | str | None = None,
        /,
        *,
        when: Matcher | MessagePredicate | None = None,
        priority: int = 0,
        name: str | None = None,
    ) -> Callable[[H], H]: ...
    def route(
        self,
        message_type: Any = None,
        /,
        *,
        when: Matcher | MessagePredicate | None = None,
        priority: int = 0,
        name: str | None = None,
    ) -> Any:
        """Decorator registering a route.

        Args:
            message_type: A message class (matches its type name and decodes into it) or a type name.
                Omit it to route by the handler's annotation (typed handlers) or ``when`` only; a
                handler of ``Message``, ``Message[Any]`` or an untyped body then matches every message.
            when: Additional matcher (attributes, headers, body fields, topic...).
            priority: Higher priorities are evaluated first.
            name: Route name used in events (defaults to the handler's qualified name).
        """
        if callable(message_type) and not isinstance(message_type, type):
            self.add(message_type)
            return message_type

        def decorate[H: Handler](handler: H) -> H:
            self.add(handler, message_type, when=when, priority=priority, name=name)
            return handler

        return decorate

    def default[H: Handler](self, handler: H) -> H:
        """Decorator registering the fallback route (used when nothing else matches)."""
        self.add(handler, fallback=True)
        return handler

    def include(self, router: Router) -> None:
        """Adds all routes of another router (keeping their priorities and order)."""
        self._routes.extend(router._routes)
        self._sorted = None

    def resolve(self, message: Message[Any], types: MessageTypes) -> Route | None:
        """The first route that accepts ``message``."""
        for route in self._ordered():
            if route.matches(message, types):
                return route
        return None

    def __len__(self) -> int:
        return len(self._routes)

    def __repr__(self) -> str:
        name = f"name={self.name!r}, " if self.name is not None else ""
        return f"Router({name}routes={self._ordered()!r})"


def _is_generic_type(value: Any) -> bool:
    # Any is a class since Python 3.11 (isinstance(Any, type)), but names no message type.
    return (
        value is Any
        or value in (bytes, str, dict, list, int, float, bool, object)
        or typing.get_origin(value) is not None
    )
