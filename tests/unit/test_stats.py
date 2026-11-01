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
