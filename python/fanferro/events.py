"""Events, hooks and logging.

Everything notable that fanferro does is described by an :class:`Event` of a given
:class:`EventKind` (``"message.acked"``, ``"lease.extended"``, ``"resource.created"``, ...).
Events come from the Rust core (receiving, leases, settlement, provisioning actions) and from
the Python layer (publishing, handler outcomes, routing, provisioning failures). They are:

* logged through :mod:`logging` according to :class:`LoggingConfig` (per-kind levels, opt-outs),
* passed to hooks registered with :meth:`fanferro.Client.on` (sync or async callables).

Only kinds that are logged or hooked are produced at all, so unused events cost nothing.

Example::

    client = fanferro.Client(logging=fanferro.LoggingConfig(levels={"message.received": logging.INFO}))


    @client.on("message.dead_lettered")
    async def alert(event: fanferro.Event) -> None:
        await pager.notify(event.message, **event.fields)
"""

from __future__ import annotations

import asyncio
import contextlib
import fnmatch
import logging
from collections.abc import Awaitable, Callable, Collection, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from fanferro._utils import is_async_callable

__all__ = [
    "ErrorInfo",
    "Event",
    "EventDispatcher",
    "EventHook",
    "EventKind",
    "LoggingConfig",
]

_LOGGER = logging.getLogger("fanferro.events")


class EventKind(StrEnum):
    """Every kind of event. Values are stable dotted names (``category.action``).

    Keep in sync with ``EventKind`` in ``crates/fanferro-core/src/events.rs`` and
    ``docs/reference/events.md`` (checked by ``scripts/check_consistency.py``).
    """

    RESOURCE_FOUND = "resource.found"
    RESOURCE_CREATED = "resource.created"
    RESOURCE_UPDATED = "resource.updated"
    RESOURCE_DRIFT = "resource.drift"
    RESOURCE_MISSING = "resource.missing"
    RESOURCE_FAILED = "resource.failed"
    RESOURCE_FALLBACK = "resource.fallback"
    RESOURCE_POLICY_UPDATED = "resource.policy_updated"
    RESOURCE_DELETED = "resource.deleted"
    PUBLISH_SUCCEEDED = "publish.succeeded"
    PUBLISH_FAILED = "publish.failed"
    PUBLISH_RETRIED = "publish.retried"
    PAYLOAD_OFFLOADED = "payload.offloaded"
    PAYLOAD_FETCHED = "payload.fetched"
    PAYLOAD_DELETED = "payload.deleted"
    CONSUMER_STARTING = "consumer.starting"
    CONSUMER_STARTED = "consumer.started"
    CONSUMER_PAUSED = "consumer.paused"
    CONSUMER_RESUMED = "consumer.resumed"
    CONSUMER_STOPPING = "consumer.stopping"
    CONSUMER_DRAINING = "consumer.draining"
    CONSUMER_STOPPED = "consumer.stopped"
    CONSUMER_ERROR = "consumer.error"
    RECEIVE_SUCCEEDED = "receive.succeeded"
    RECEIVE_EMPTY = "receive.empty"
    RECEIVE_FAILED = "receive.failed"
    MESSAGE_RECEIVED = "message.received"
    MESSAGE_DECODE_FAILED = "message.decode_failed"
    MESSAGE_UNROUTABLE = "message.unroutable"
    MESSAGE_PROCESSED = "message.processed"
    MESSAGE_FAILED = "message.failed"
    MESSAGE_DUPLICATE = "message.duplicate"
    MESSAGE_ACKED = "message.acked"
    MESSAGE_ACK_FAILED = "message.ack_failed"
    MESSAGE_RETRY_SCHEDULED = "message.retry_scheduled"
    MESSAGE_REQUEUED = "message.requeued"
    MESSAGE_DEAD_LETTERED = "message.dead_lettered"
    MESSAGE_DEAD_LETTER_FAILED = "message.dead_letter_failed"
    MESSAGE_RELEASED = "message.released"
    MESSAGE_DROPPED = "message.dropped"
    LEASE_EXTENDED = "lease.extended"
    LEASE_EXTENSION_FAILED = "lease.extension_failed"
    LEASE_LOST = "lease.lost"
    LEASE_EXPIRED = "lease.expired"
    SUBSCRIPTION_CONFIRMED = "sns.subscription_confirmed"
    HOOK_FAILED = "hook.failed"
    CONFIG_WARNING = "config.warning"
    EVENTS_DROPPED = "events.dropped"

    @property
    def category(self) -> str:
        """The part before the first dot (``"message"`` for ``"message.acked"``)."""
        return self.value.split(".", 1)[0]

    @property
    def default_level(self) -> int:
        """Default :mod:`logging` level of this kind."""
        return _DEFAULT_LEVELS[self]


_LEVEL_NAMES = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING, "error": logging.ERROR}

_DEFAULT_LEVELS: dict[EventKind, int] = {
    EventKind.RESOURCE_FOUND: logging.DEBUG,
    EventKind.RESOURCE_CREATED: logging.INFO,
    EventKind.RESOURCE_UPDATED: logging.INFO,
    EventKind.RESOURCE_DRIFT: logging.WARNING,
    EventKind.RESOURCE_MISSING: logging.WARNING,
    EventKind.RESOURCE_FAILED: logging.ERROR,
    EventKind.RESOURCE_FALLBACK: logging.WARNING,
    EventKind.RESOURCE_POLICY_UPDATED: logging.INFO,
    EventKind.RESOURCE_DELETED: logging.INFO,
    EventKind.PUBLISH_SUCCEEDED: logging.DEBUG,
    EventKind.PUBLISH_FAILED: logging.ERROR,
    EventKind.PUBLISH_RETRIED: logging.WARNING,
    EventKind.PAYLOAD_OFFLOADED: logging.DEBUG,
    EventKind.PAYLOAD_FETCHED: logging.DEBUG,
    EventKind.PAYLOAD_DELETED: logging.DEBUG,
    EventKind.CONSUMER_STARTING: logging.INFO,
    EventKind.CONSUMER_STARTED: logging.INFO,
    EventKind.CONSUMER_PAUSED: logging.INFO,
    EventKind.CONSUMER_RESUMED: logging.INFO,
    EventKind.CONSUMER_STOPPING: logging.INFO,
    EventKind.CONSUMER_DRAINING: logging.INFO,
    EventKind.CONSUMER_STOPPED: logging.INFO,
    EventKind.CONSUMER_ERROR: logging.ERROR,
    EventKind.RECEIVE_SUCCEEDED: logging.DEBUG,
    EventKind.RECEIVE_EMPTY: logging.DEBUG,
    EventKind.RECEIVE_FAILED: logging.WARNING,
    EventKind.MESSAGE_RECEIVED: logging.DEBUG,
    EventKind.MESSAGE_DECODE_FAILED: logging.ERROR,
    EventKind.MESSAGE_UNROUTABLE: logging.WARNING,
    EventKind.MESSAGE_PROCESSED: logging.DEBUG,
    EventKind.MESSAGE_FAILED: logging.WARNING,
    EventKind.MESSAGE_DUPLICATE: logging.INFO,
    EventKind.MESSAGE_ACKED: logging.DEBUG,
    EventKind.MESSAGE_ACK_FAILED: logging.WARNING,
    EventKind.MESSAGE_RETRY_SCHEDULED: logging.INFO,
    EventKind.MESSAGE_REQUEUED: logging.INFO,
    EventKind.MESSAGE_DEAD_LETTERED: logging.WARNING,
    EventKind.MESSAGE_DEAD_LETTER_FAILED: logging.ERROR,
    EventKind.MESSAGE_RELEASED: logging.DEBUG,
    EventKind.MESSAGE_DROPPED: logging.WARNING,
    EventKind.LEASE_EXTENDED: logging.DEBUG,
    EventKind.LEASE_EXTENSION_FAILED: logging.WARNING,
    EventKind.LEASE_LOST: logging.WARNING,
    EventKind.LEASE_EXPIRED: logging.WARNING,
    EventKind.SUBSCRIPTION_CONFIRMED: logging.INFO,
    EventKind.HOOK_FAILED: logging.ERROR,
    EventKind.CONFIG_WARNING: logging.WARNING,
    EventKind.EVENTS_DROPPED: logging.WARNING,
}


@dataclass(frozen=True, slots=True)
class ErrorInfo:
    """Error details attached to failure events."""

    category: str
    message: str
    retryable: bool
    code: str | None = None
    request_id: str | None = None

    @classmethod
    def from_exception(cls, error: BaseException) -> ErrorInfo:
        """Describes a Python exception."""
        code = getattr(error, "code", None)
        request_id = getattr(error, "request_id", None)
        return cls(
            category=str(getattr(error, "category", type(error).__name__)),
            message=str(error) or type(error).__name__,
            retryable=bool(getattr(error, "retryable", False)),
            code=code if isinstance(code, str) else None,
            request_id=request_id if isinstance(request_id, str) else None,
        )


@dataclass(frozen=True, slots=True)
class Event:
    """Something that happened.

    Attributes:
        kind: What happened.
        level: :mod:`logging` level (after :class:`LoggingConfig` overrides are applied when logging).
        message: Human-readable summary.
        time: When it happened (UTC).
        fields: Structured context such as ``queue``, ``topic``, ``message_id``, ``duration``.
        error: Error details for failure events.
        exception: The Python exception, for failures raised in Python (handlers, hooks, codecs).
    """

    kind: EventKind
    level: int
    message: str
    time: datetime
    fields: Mapping[str, Any] = field(default_factory=dict)
    error: ErrorInfo | None = None
    exception: BaseException | None = None

    @classmethod
    def create(
        cls,
        kind: EventKind,
        message: str,
        /,
        *,
        level: int | None = None,
        exception: BaseException | None = None,
        **fields: Any,
    ) -> Event:
        """Creates an event timestamped now."""
        return cls(
            kind=kind,
            level=kind.default_level if level is None else level,
            message=message,
            time=datetime.now(UTC),
            fields={key: value for key, value in fields.items() if value is not None},
            error=ErrorInfo.from_exception(exception) if exception is not None else None,
            exception=exception,
        )

    @classmethod
    def from_core(cls, raw: Mapping[str, Any]) -> Event:
        """Converts an event emitted by the Rust core."""
        error = raw.get("error")
        return cls(
            kind=EventKind(raw["kind"]),
            level=_LEVEL_NAMES.get(raw["level"], logging.INFO),
            message=raw["message"],
            time=raw["time"],
            fields=raw.get("fields") or {},
            error=ErrorInfo(**error) if error else None,
        )


type EventHook = Callable[[Event], object]
"""A sync or async callable receiving events (return values are ignored; awaitables are awaited)."""

type KindSelector = EventKind | str | Iterable[EventKind | str]
"""An event kind, a glob pattern such as ``"message.*"`` or ``"*"``, or several of them."""


@dataclass(frozen=True, slots=True)
class LoggingConfig:
    """How events are logged.

    Attributes:
        enabled: Log events at all.
        logger: Base logger name (events of category ``message`` go to ``"<logger>.message"``) or a
            :class:`logging.Logger` instance used for every event.
        levels: Level overrides keyed by kind or glob pattern; a value of ``None`` disables logging of
            matching kinds. Later patterns win over earlier ones; exact kinds win over patterns.
        disabled: Kinds or glob patterns that are never logged.
        include_fields: Append structured fields to the log message text.
        include_tracebacks: Attach exception tracebacks to log records of failure events.
        formatter: Custom ``(event) -> str`` for the log message text.

    Every record carries ``record.fanferro = {"kind": ..., "fields": ..., "error": ...}`` for
    structured log processors.
    """

    enabled: bool = True
    logger: str | logging.Logger = "fanferro"
    levels: Mapping[EventKind | str, int | str | None] = field(default_factory=dict)
    disabled: Collection[EventKind | str] = ()
    include_fields: bool = True
    include_tracebacks: bool = True
    formatter: Callable[[Event], str] | None = None

    def level_for(self, kind: EventKind) -> int | None:
        """The level ``kind`` is logged at, or ``None`` when it is not logged."""
        if not self.enabled:
            return None
        if any(_matches(pattern, kind) for pattern in self.disabled):
            return None
        level: int | str | None = kind.default_level
        exact = None
        for pattern, override in self.levels.items():
            if str(pattern) == kind.value:
                exact = (override,)
            elif _matches(pattern, kind):
                level = override
        if exact is not None:
            level = exact[0]
        if level is None:
            return None
        if isinstance(level, str):
            return logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
        return level

    def logger_for(self, kind: EventKind) -> logging.Logger:
        """The logger used for ``kind``."""
        if isinstance(self.logger, logging.Logger):
            return self.logger
        return logging.getLogger(f"{self.logger}.{kind.category}")


def _matches(pattern: EventKind | str, kind: EventKind) -> bool:
    text = str(pattern)
    return text == kind.value or fnmatch.fnmatchcase(kind.value, text)


def _normalize(selector: KindSelector) -> list[str]:
    patterns = [str(selector)] if isinstance(selector, str) else [str(item) for item in selector]
    for pattern in patterns:
        if not any(fnmatch.fnmatchcase(kind.value, pattern) for kind in EventKind):
            raise ValueError(f"{pattern!r} does not match any event kind")
    return patterns


def _format_fields(fields: Mapping[str, Any]) -> str:
    parts = []
    for key, value in fields.items():
        text = str(value)
        if len(text) > 200:
            text = text[:197] + "..."
        parts.append(f"{key}={text}")
    return " ".join(parts)


class EventDispatcher:
    """Routes events to logging and hooks. Owned by :class:`fanferro.Client`."""

    def __init__(self, config: LoggingConfig | None = None) -> None:
        self._config = config or LoggingConfig()
        self._hooks: list[tuple[list[str], EventHook]] = []
        self._tasks: set[asyncio.Task[None]] = set()
        self._listeners: list[Callable[[], None]] = []
        # Per-kind lookups behind `is_enabled`, which runs for every message: the logger and level
        # a kind is logged at (the logger's own level is checked live) and whether a hook wants it.
        self._log_targets: dict[EventKind, tuple[logging.Logger, int] | None] = {}
        self._hooked: dict[EventKind, bool] = {}

    @property
    def config(self) -> LoggingConfig:
        """How events are logged."""
        return self._config

    @config.setter
    def config(self, config: LoggingConfig) -> None:
        self._config = config
        self._log_targets.clear()
        self._changed()

    def subscribe(self, selector: KindSelector, hook: EventHook) -> Callable[[], None]:
        """Calls ``hook`` for every event matching ``selector``; returns an unsubscribe function."""
        entry = (_normalize(selector), hook)
        self._hooks.append(entry)
        self._changed()

        def unsubscribe() -> None:
            with contextlib.suppress(ValueError):
                self._hooks.remove(entry)
            self._changed()

        return unsubscribe

    def on_change(self, listener: Callable[[], None]) -> None:
        """Registers a callback invoked when the set of wanted kinds may have changed."""
        self._listeners.append(listener)

    def _changed(self) -> None:
        self._hooked.clear()
        for listener in self._listeners:
            listener()

    def _log_target(self, kind: EventKind) -> tuple[logging.Logger, int] | None:
        try:
            return self._log_targets[kind]
        except KeyError:
            level = self._config.level_for(kind)
            target = None if level is None else (self._config.logger_for(kind), level)
            self._log_targets[kind] = target
            return target

    def is_enabled(self, kind: EventKind) -> bool:
        """Whether anything (logging or a hook) wants events of ``kind``."""
        target = self._log_target(kind)
        if target is not None and target[0].isEnabledFor(target[1]):
            return True
        hooked = self._hooked.get(kind)
        if hooked is None:
            hooked = any(any(fnmatch.fnmatchcase(kind.value, p) for p in patterns) for patterns, _ in self._hooks)
            self._hooked[kind] = hooked
        return hooked

    def enabled_kinds(self) -> list[EventKind]:
        """All kinds currently wanted."""
        return [kind for kind in EventKind if self.is_enabled(kind)]

    def emit(self, kind: EventKind, message: str, /, *, exception: BaseException | None = None, **fields: Any) -> None:
        """Creates and dispatches an event if anything wants it."""
        if self.is_enabled(kind):
            self.dispatch(Event.create(kind, message, exception=exception, **fields))

    def dispatch(self, event: Event) -> None:
        """Logs ``event`` and invokes matching hooks."""
        self._log(event)
        for patterns, hook in list(self._hooks):
            if any(fnmatch.fnmatchcase(event.kind.value, pattern) for pattern in patterns):
                self._call(hook, event)

    def _log(self, event: Event) -> None:
        target = self._log_target(event.kind)
        if target is None:
            return
        logger, level = target
        if not logger.isEnabledFor(level):
            return
        try:
            if self._config.formatter is not None:
                text = self._config.formatter(event)
            elif self._config.include_fields and event.fields:
                text = f"{event.message} ({_format_fields(event.fields)})"
            else:
                text = event.message
            if event.error is not None and event.exception is None:
                text = f"{text}: {event.error.message}"
        except Exception:  # a broken formatter must never break message processing  # noqa: BLE001 - deliberate catch-all; the error is reported, never swallowed silently
            text = event.message
        exc_info = event.exception if self._config.include_tracebacks and event.exception is not None else None
        logger.log(
            level,
            text,
            exc_info=exc_info,
            extra={"fanferro": {"kind": event.kind.value, "fields": dict(event.fields), "error": event.error}},
        )

    def _call(self, hook: EventHook, event: Event) -> None:
        if is_async_callable(hook):
            self._schedule(hook, event, None)
            return
        try:
            result = hook(event)
        except Exception as error:  # noqa: BLE001 - deliberate catch-all; the error is reported, never swallowed silently
            self._hook_failed(hook, event, error)
            return
        if isinstance(result, Awaitable):
            # A sync callable that returns an awaitable (a lambda calling an async function, an
            # async hook behind a sync decorator) is awaited like an async hook.
            self._schedule(hook, event, result)

    def _schedule(self, hook: EventHook, event: Event, awaitable: Awaitable[object] | None) -> None:
        """Runs an async hook (or awaits what a sync one returned) in a task of the running loop."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            _LOGGER.warning("async event hook %r skipped: no running event loop", hook)
            close = getattr(awaitable, "close", None)
            if callable(close):  # a coroutine: closing it spares the "never awaited" warning
                close()
            return
        task = loop.create_task(self._run_async(hook, event, awaitable))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run_async(self, hook: EventHook, event: Event, awaitable: Awaitable[object] | None) -> None:
        try:
            result = hook(event) if awaitable is None else awaitable
            if isinstance(result, Awaitable):
                await result
        except Exception as error:  # noqa: BLE001 - deliberate catch-all; the error is reported, never swallowed silently
            self._hook_failed(hook, event, error)

    def _hook_failed(self, hook: EventHook, event: Event, error: Exception) -> None:
        name = getattr(hook, "__qualname__", repr(hook))
        if event.kind is EventKind.HOOK_FAILED:
            _LOGGER.error("event hook %s failed while handling a hook failure", name, exc_info=error)
            return
        self.dispatch(
            Event.create(
                EventKind.HOOK_FAILED,
                f"event hook {name} failed for {event.kind.value}",
                exception=error,
                hook=name,
                event_kind=event.kind.value,
            )
        )

    async def flush(self) -> None:
        """Waits for running async hooks."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
