"""Typed statistics, metrics and queue counts (python/fanferro/stats.py)."""

from __future__ import annotations

import dataclasses
import json
import pickle
from decimal import Decimal
from typing import Any, assert_type

import pytest

import fanferro
from fanferro import testing
from fanferro.stats import EngineState
from tests.helpers import consume_until


async def test_queue_counts() -> None:
    client = testing.memory_client()
    queue = client.queue("counted")
    async with client:
        await queue.send({"n": 1})
        await queue.send({"n": 2}, delay=60)
        counts = await queue.approximate_count()
    assert_type(counts, fanferro.QueueCounts)
    assert counts == fanferro.QueueCounts(visible=1, in_flight=0, delayed=1)
    assert counts.total == 2
    assert counts.as_dict() == {"visible": 1, "in_flight": 0, "delayed": 1}
    with pytest.raises(dataclasses.FrozenInstanceError):
        counts.visible = 3  # type: ignore[misc]


async def test_consumer_stats_and_client_metrics() -> None:
    client = testing.memory_client()
    queue = client.queue("stats")
    consumer = client.consumer(queue, wait_time=1)
    handled: list[int] = []

    @consumer.default
    async def handle(message: fanferro.Message[Any]) -> None:
        handled.append(message.json()["n"])

    before = consumer.stats()
    assert before.engine.state == "created", "an engine that does not exist yet counts nothing"
    assert before.engine.messages_received == before.engine.pollers == 0
    async with client:
        await queue.send({"n": 1})
        await consume_until(consumer, lambda: consumer.stats().engine.messages_acked == 1)
        metrics = client.metrics()

    stats = consumer.stats()
    assert_type(stats, fanferro.ConsumerStats)
    assert_type(stats.engine, fanferro.EngineStats)
    assert_type(stats.engine.state, EngineState)
    assert_type(stats.engine.messages_acked, int)
    assert_type(stats.handler_seconds, float)
    assert (stats.name, stats.queue, stats.state) == (consumer.name, "stats", "stopped")
    assert (stats.processed, stats.failed, stats.active) == (1, 0, 0)
    assert stats.engine.state == "stopped"
    assert stats.engine.messages_received == stats.engine.messages_acked == 1
    assert stats.engine.receive_requests >= 1
    assert stats.engine.in_flight == stats.engine.leases == 0

    assert_type(metrics, fanferro.ClientMetrics)
    assert_type(metrics.client, fanferro.ClientCounters)
    assert metrics.client.messages_published == 1
    assert metrics.client.bytes_published > 0
    assert list(metrics.consumers) == [consumer.name]
    assert metrics.consumers[consumer.name].processed == 1

    plain = metrics.as_dict()
    assert plain["client"]["messages_published"] == 1
    assert plain["consumers"][consumer.name]["engine"]["messages_acked"] == 1
    json.dumps(plain)  # plain values only
    assert pickle.loads(pickle.dumps(metrics)) == metrics


def test_metrics_of_a_client_that_did_not_connect() -> None:
    metrics = testing.memory_client().metrics()
    assert metrics == fanferro.ClientMetrics(
        client=fanferro.ClientCounters(
            messages_published=0,
            publish_failures=0,
            bytes_published=0,
            publish_retries=0,
            payloads_offloaded=0,
            bytes_offloaded=0,
        ),
        consumers={},
    )


def test_message_attribute_types() -> None:
    message = testing.build_message({}, attributes={"tenant": "acme", "priority": 3, "price": Decimal("1.5")})
    assert_type(message.attribute("tenant"), str | int | float | Decimal | bytes | None)
    assert_type(message.attribute("tenant", "none"), str | int | float | Decimal | bytes)
    assert_type(message.attribute("tenant", 0.0), str | int | float | Decimal | bytes)
    assert message.attribute("priority") == 3
    assert message.attribute("price") == Decimal("1.5")
    assert message.attribute("missing") is None
    assert message.attribute("missing", "none") == "none"
    assert_type(message.dead_letter_info, fanferro.DeadLetterInfo | None)


def _health(**changes: Any) -> fanferro.ConsumerHealth:
    values: dict[str, Any] = {
        "name": "orders",
        "queue": "orders",
        "state": "running",
        "paused": False,
        "last_receive_at": None,
        "receive_gap": 0.0,
        "consecutive_receive_failures": 0,
        "last_error": None,
        "last_dispatch_at": None,
        "dispatch_gap": 0.1,
        "active": 0,
        "in_flight": 0,
    }
    return fanferro.ConsumerHealth(**{**values, **changes})


def test_consumer_health_verdicts() -> None:
    from datetime import UTC, datetime, timedelta

    assert _health().healthy()
    assert _health().ready()
    # Starting and stopping consumers are alive but take no work; created and stopped ones are not alive.
    for state in ("starting", "stopping"):
        assert _health(state=state, receive_gap=1e6, dispatch_gap=1e6).healthy()
        assert not _health(state=state).ready()
    for state in ("created", "stopped"):
        assert _health(state=state).problems() == (f"consumer orders is {state}",)
        assert not _health(state=state).ready()
    # Paused: the receive gap does not count (the engine leaves it out anyway), the dispatch gap does.
    assert _health(paused=True, receive_gap=1e6).healthy()
    assert not _health(paused=True).ready()
    assert not _health(paused=True, dispatch_gap=31).healthy()

    failure = fanferro.ReceiveFailure(
        error_type="fanferro.errors.AccessDeniedError",
        category="access_denied",
        code="AccessDenied",
        message="sqs:ReceiveMessage failed (access_denied)",
        retryable=False,
        at=datetime(2026, 11, 1, 12, 0, tzinfo=UTC),
    )
    failing = _health(receive_gap=61, consecutive_receive_failures=4, last_error=failure)
    assert failing.problems() == (
        "consumer orders has received nothing from queue orders for 61 s: 4 receives failed"
        " (last: AccessDeniedError AccessDenied)",
    )
    assert failing.healthy(max_receive_gap=timedelta(minutes=2))
    assert not failing.healthy(max_receive_gap=60)
    assert _health(receive_gap=61).problems() == ("consumer orders has received nothing from queue orders for 61 s",)
    assert _health(dispatch_gap=31).problems() == (
        "consumer orders has not dispatched messages for 31 s; is the event loop blocked?",
    )
    assert _health(dispatch_gap=31).healthy(max_dispatch_gap=60)
    assert _health(state="stopped", consecutive_receive_failures=3, last_error=failure).problems() == (
        "consumer orders is stopped after 3 failed receives (last: AccessDeniedError AccessDenied)",
    )
    with pytest.raises(ValueError, match="max_receive_gap"):
        _health().healthy(max_receive_gap=-1)

    as_dict = failing.as_dict()
    assert as_dict["last_error"]["at"] == "2026-11-01T12:00:00+00:00"
    assert json.loads(json.dumps(as_dict)) == as_dict


def test_client_health_aggregates_consumers() -> None:
    empty = fanferro.ClientHealth(consumers={})
    assert empty.healthy()
    assert empty.ready()
    health = fanferro.ClientHealth(consumers={"a": _health(name="a"), "b": _health(name="b", paused=True)})
    assert health.healthy()
    assert not health.ready()
    broken = fanferro.ClientHealth(consumers={"a": _health(name="a", state="stopped"), "b": _health(name="b")})
    assert broken.problems() == ("consumer a is stopped",)
    assert not broken.healthy()
    assert json.loads(json.dumps(broken.as_dict()))["consumers"]["a"]["state"] == "stopped"
