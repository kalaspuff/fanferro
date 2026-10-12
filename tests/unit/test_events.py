from __future__ import annotations

import asyncio
import logging

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
