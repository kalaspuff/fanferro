from __future__ import annotations

import asyncio
import gc
import logging
import warnings
from collections.abc import Awaitable
from typing import Any

import pytest

import fanferro
from fanferro import _core
from fanferro.events import Event, EventDispatcher, EventKind, LoggingConfig


def test_kinds_match_the_core() -> None:
    core = {name: level for name, level, _ in _core.event_kinds()}
    assert set(core) == {kind.value for kind in EventKind}
    levels = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING, "error": logging.ERROR}
    for kind in EventKind:
        assert kind.default_level == levels[core[kind.value]], kind


def test_level_overrides() -> None:
    config = LoggingConfig(
        levels={"message.*": logging.INFO, "message.acked": None, "lease.extended": "warning"},
        disabled=["receive.*"],
    )
    assert config.level_for(EventKind.MESSAGE_RECEIVED) == logging.INFO
    assert config.level_for(EventKind.MESSAGE_ACKED) is None
    assert config.level_for(EventKind.LEASE_EXTENDED) == logging.WARNING
    assert config.level_for(EventKind.RECEIVE_FAILED) is None
    assert config.level_for(EventKind.RESOURCE_CREATED) == logging.INFO
    assert LoggingConfig(enabled=False).level_for(EventKind.RESOURCE_FAILED) is None
    custom = logging.getLogger("custom")
    assert LoggingConfig(logger=custom).logger_for(EventKind.MESSAGE_ACKED) is custom
    assert LoggingConfig().logger_for(EventKind.MESSAGE_ACKED).name == "fanferro.message"


def test_logging_and_structured_extra(caplog: pytest.LogCaptureFixture) -> None:
    dispatcher = EventDispatcher(LoggingConfig())
    with caplog.at_level(logging.DEBUG, logger="fanferro"):
        dispatcher.emit(EventKind.RESOURCE_CREATED, "queue created", name="q", resource="queue")
    record = caplog.records[-1]
    assert record.name == "fanferro.resource"
    assert record.levelno == logging.INFO
    assert "name=q" in record.getMessage()
    assert record.fanferro["kind"] == "resource.created"  # type: ignore[attr-defined]
    assert record.fanferro["fields"]["resource"] == "queue"  # type: ignore[attr-defined]


def test_custom_formatter_and_broken_formatter(caplog: pytest.LogCaptureFixture) -> None:
    dispatcher = EventDispatcher(LoggingConfig(formatter=lambda event: f"<<{event.kind}>>"))
    with caplog.at_level(logging.INFO, logger="fanferro"):
        dispatcher.emit(EventKind.RESOURCE_CREATED, "created")
    assert caplog.records[-1].getMessage() == "<<resource.created>>"

    def broken(event: Event) -> str:
        raise RuntimeError("boom")

    dispatcher = EventDispatcher(LoggingConfig(formatter=broken))
    with caplog.at_level(logging.INFO, logger="fanferro"):
        dispatcher.emit(EventKind.RESOURCE_CREATED, "fallback text")
    assert caplog.records[-1].getMessage() == "fallback text"


async def test_hooks_sync_async_and_failures() -> None:
    dispatcher = EventDispatcher(LoggingConfig(enabled=False))
    seen: list[str] = []
    failures: list[Event] = []

    def sync_hook(event: Event) -> None:
        seen.append(f"sync:{event.kind}")

    async def async_hook(event: Event) -> None:
        await asyncio.sleep(0)
        seen.append(f"async:{event.kind}")

    def broken(event: Event) -> None:
        raise ValueError("hook bug")

    unsubscribe = dispatcher.subscribe("message.*", sync_hook)
    dispatcher.subscribe([EventKind.MESSAGE_ACKED], async_hook)
    dispatcher.subscribe("message.acked", broken)
    dispatcher.subscribe(EventKind.HOOK_FAILED, failures.append)
    assert dispatcher.is_enabled(EventKind.MESSAGE_ACKED)
    assert not dispatcher.is_enabled(EventKind.RESOURCE_CREATED)
    dispatcher.emit(EventKind.MESSAGE_ACKED, "acked", queue="q")
    await dispatcher.flush()
    assert seen == ["sync:message.acked", "async:message.acked"]
    assert len(failures) == 1
    assert failures[0].fields["hook"].endswith("broken")
    unsubscribe()
    dispatcher.emit(EventKind.MESSAGE_RECEIVED, "received")
    assert seen == ["sync:message.acked", "async:message.acked"]


async def test_sync_hooks_returning_awaitables_are_awaited() -> None:
    # A lambda calling an async function, or an async hook behind a sync decorator, is not an async
    # function itself: what it returned was dropped (and Python warned it was never awaited).
    dispatcher = EventDispatcher(LoggingConfig(enabled=False))
    seen: list[str] = []
    failures: list[Event] = []

    async def notify(event: Event, by: str) -> None:
        await asyncio.sleep(0)
        seen.append(f"{by}:{event.kind}")

    async def broken(event: Event) -> None:
        raise ValueError("hook bug")

    def decorated(event: Event) -> Awaitable[None]:
        return notify(event, "decorated")

    dispatcher.subscribe("message.acked", lambda event: notify(event, "lambda"))
    dispatcher.subscribe("message.acked", decorated)
    dispatcher.subscribe("message.acked", lambda event: broken(event))
    dispatcher.subscribe(EventKind.HOOK_FAILED, failures.append)
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        dispatcher.emit(EventKind.MESSAGE_ACKED, "acked")
        await dispatcher.flush()
        gc.collect()
    assert sorted(seen) == ["decorated:message.acked", "lambda:message.acked"]
    assert [failure.fields["event_kind"] for failure in failures] == ["message.acked"]


def test_awaitables_of_sync_hooks_without_a_running_loop_are_skipped(caplog: pytest.LogCaptureFixture) -> None:
    dispatcher = EventDispatcher(LoggingConfig(enabled=False))
    seen: list[str] = []

    async def notify(event: Event) -> None:
        seen.append(event.kind)

    dispatcher.subscribe("message.acked", lambda event: notify(event))
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        dispatcher.emit(EventKind.MESSAGE_ACKED, "acked")
        gc.collect()
    assert seen == []
    assert "skipped: no running event loop" in caplog.text


def test_enablement_follows_hooks_and_logger_levels() -> None:
    dispatcher = EventDispatcher(LoggingConfig(logger="fanferro-test-enablement"))
    logger = logging.getLogger("fanferro-test-enablement.message")
    logger.setLevel(logging.INFO)
    assert not dispatcher.is_enabled(EventKind.MESSAGE_ACKED), "debug-level kind, logger at INFO"
    unsubscribe = dispatcher.subscribe("message.acked", lambda event: None)
    assert dispatcher.is_enabled(EventKind.MESSAGE_ACKED), "a new hook is seen right away"
    unsubscribe()
    assert not dispatcher.is_enabled(EventKind.MESSAGE_ACKED)
    logger.setLevel(logging.DEBUG)
    assert dispatcher.is_enabled(EventKind.MESSAGE_ACKED), "logger levels are checked live"
    dispatcher.config = LoggingConfig(logger="fanferro-test-enablement", disabled=["message.*"])
    assert not dispatcher.is_enabled(EventKind.MESSAGE_ACKED), "a new configuration applies at once"
    logger.setLevel(logging.NOTSET)


def test_invalid_selector() -> None:
    with pytest.raises(ValueError, match="does not match"):
        EventDispatcher().subscribe("nothing.matches", print)


def test_event_from_core_and_create() -> None:
    event = Event.from_core(
        {
            "kind": "message.acked",
            "level": "debug",
            "message": "acked",
            "time": Event.create(EventKind.MESSAGE_ACKED, "x").time,
            "fields": {"queue": "q"},
            "error": {"category": "aws", "message": "m", "retryable": True, "code": "C", "request_id": None},
        }
    )
    assert event.kind is EventKind.MESSAGE_ACKED
    assert event.level == logging.DEBUG
    assert event.error is not None
    assert event.error.code == "C"
    created = Event.create(
        EventKind.MESSAGE_FAILED, "failed", exception=fanferro.ValidationError("bad"), queue="q", skip=None
    )
    assert created.error is not None
    assert created.error.message == "bad"
    assert "skip" not in created.fields
    assert EventKind.MESSAGE_FAILED.category == "message"


async def test_a_hanging_async_hook_does_not_make_flush_wait_forever(caplog: pytest.LogCaptureFixture) -> None:
    dispatcher = EventDispatcher(LoggingConfig(enabled=False))
    dispatcher.flush_timeout = 0.2
    cancelled: list[bool] = []

    async def hang(event: Event) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    dispatcher.subscribe("message.*", hang)
    dispatcher.emit(EventKind.MESSAGE_ACKED, "acked")
    with caplog.at_level(logging.WARNING, logger="fanferro.events"):
        await asyncio.wait_for(dispatcher.flush(), 3)
    assert cancelled == [True]
    assert "cancelled 1 async event hooks" in caplog.text


async def test_a_hanging_async_hook_does_not_block_client_close(memory_client: fanferro.Client) -> None:
    # One hook that never returned (an HTTP alert without a timeout) made `async with client` hang
    # on exit until the orchestrator killed the process.
    memory_client.events.flush_timeout = 0.2

    async def hang(event: Event) -> None:
        await asyncio.Event().wait()

    memory_client.on("publish.*", hang)
    queue = memory_client.queue("hanging-hook")
    async with memory_client:
        await queue.send({"n": 1})
    await asyncio.wait_for(memory_client.close(), 5)


async def test_pending_async_hooks_are_capped() -> None:
    dispatcher = EventDispatcher(LoggingConfig(enabled=False))
    dispatcher.max_pending_hooks = 10
    release = asyncio.Event()
    dropped: list[Event] = []

    async def slow(event: Event) -> None:
        await release.wait()

    dispatcher.subscribe("publish.*", slow)
    dispatcher.subscribe(EventKind.EVENTS_DROPPED, dropped.append)
    for _ in range(50):
        dispatcher.emit(EventKind.PUBLISH_SUCCEEDED, "published")
    assert len(dispatcher._tasks) == 10
    assert dispatcher.hooks_skipped == 40
    release.set()
    await dispatcher.flush()
    assert [(event.fields["dropped"], event.fields["reason"]) for event in dropped] == [(40, "hooks")]


async def test_a_raising_logging_filter_never_breaks_an_operation(
    memory_client: fanferro.Client, capsys: pytest.CaptureFixture[str]
) -> None:
    # A filter that raised for some records made publish() raise after the message was sent, and
    # left a consumer stuck in "starting" (client.close() then waited for it forever).
    class Picky(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            raise KeyError("request_id")

    logger = logging.getLogger("fanferro")
    picky = Picky()
    level = logger.level
    logger.addFilter(picky)
    logger.setLevel(logging.DEBUG)
    for name in ("fanferro.publish", "fanferro.consumer", "fanferro.resource", "fanferro.message"):
        logging.getLogger(name).addFilter(picky)
    try:
        queue = memory_client.queue("picky-logging")
        seen: list[Any] = []
        consumer = memory_client.consumer(queue, wait_time=1)

        @consumer.route
        async def handle(message: fanferro.Message[Any]) -> None:
            seen.append(message.json())

        async with memory_client:
            await queue.resolve()
            await queue.send({"n": 1})
            await consumer.start()
            assert consumer.state == "running"
            for _ in range(100):
                if seen:
                    break
                await asyncio.sleep(0.05)
            await consumer.stop()
        await asyncio.wait_for(memory_client.close(), 5)
    finally:
        logger.removeFilter(picky)
        logger.setLevel(level)
        for name in ("fanferro.publish", "fanferro.consumer", "fanferro.resource", "fanferro.message"):
            logging.getLogger(name).removeFilter(picky)
    assert seen == [{"n": 1}]
    assert capsys.readouterr().err.count("raised KeyError") == 1


async def test_the_dispatchers_own_warnings_survive_a_raising_logging_filter(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # flush() and the hook cap log through fanferro.events: a raising filter there made flush()
    # raise before it cancelled the hooks, and the cap's warning raise into emit().
    class Picky(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            raise KeyError("request_id")

    logger = logging.getLogger("fanferro.events")
    picky = Picky()
    logger.addFilter(picky)
    try:
        dispatcher = EventDispatcher(LoggingConfig(enabled=False))
        dispatcher.max_pending_hooks = 1
        dispatcher.flush_timeout = 0.1
        cancelled: list[bool] = []

        async def hang(event: Event) -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.append(True)
                raise

        dispatcher.subscribe("message.*", hang)
        dispatcher.emit(EventKind.MESSAGE_ACKED, "acked")
        dispatcher.emit(EventKind.MESSAGE_ACKED, "acked")  # over the cap: warns
        assert dispatcher.hooks_skipped == 1
        await asyncio.wait_for(dispatcher.flush(), 3)
    finally:
        logger.removeFilter(picky)
    assert cancelled == [True]
    assert capsys.readouterr().err.count("raised KeyError") == 1
