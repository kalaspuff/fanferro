"""Keyword options are typed (``fanferro.options``), and options given with an OutgoingMessage are refused."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import fanferro
from fanferro import OutgoingMessage
from fanferro.idempotency import InMemoryIdempotencyStore, idempotent
from fanferro.testing import memory_client
from tests.helpers import drain_queue

# Each line ending in `# error: <name>` must get that pyrefly error, and no other line any.
_PROBE = """
from datetime import timedelta
from typing import assert_type

import fanferro
from fanferro.testing import dispatch, memory_client


async def handler(message: fanferro.Message) -> None: ...


async def calls() -> None:
    client = memory_client(region="eu-north-1", queue_prefix="test-", broker=fanferro.MemoryBroker())
    topic = client.topic("orders")
    queue = client.queue("worker")

    published = await topic.publish({}, group_id="g", sns_subject="s", attributes={"tenant": "acme"})
    assert_type(published, fanferro.PublishResult)
    assert_type(await queue.send({}, delay=timedelta(seconds=5), headers={"tenant": "acme"}), fanferro.PublishResult)
    assert_type(await queue.publish_batch([{}], raise_on_failure=False, delay=5), fanferro.BatchPublishResult)
    assert_type(await queue.send_batch([{}], raise_on_failure=False, group_id="g"), fanferro.BatchPublishResult)
    assert_type(await topic.prepare({}, type="orders.created"), fanferro.PreparedMessage)
    assert_type(client.consumer(queue, handler, concurrency=20, wait_time=1), fanferro.Consumer)
    assert_type(queue.consumer(handler, retry=fanferro.RetryPolicy(), ordering="fifo"), fanferro.Consumer)
    assert_type(queue.subscribe_to(topic, filter_policy={"fanferro.type": ["x"]}), fanferro.Subscription)
    assert_type(topic.subscribe(queue, raw_message_delivery=True), fanferro.Subscription)
    assert_type(fanferro.Provisioning.ensure(on_drift="raise"), fanferro.Provisioning)
    shared: fanferro.ConsumerOptions = {"concurrency": 5, "handler_timeout": 30}
    client.consumer(queue, handler, **shared)
    await dispatch(handler, {}, retry=fanferro.RetryPolicy(max_attempts=3))

    memory_client(regoin="eu-north-1")  # error: unexpected-keyword
    await topic.publish({}, group_idd="g")  # error: unexpected-keyword
    await topic.publish({}, delay=5)  # error: unexpected-keyword
    await queue.send({}, sns_subject="s")  # error: unexpected-keyword
    await queue.send({}, delay="5")  # error: bad-argument-type
    await topic.prepare({}, dealy=5)  # error: unexpected-keyword
    await queue.send_batch([{}], dealy=5)  # error: unexpected-keyword
    client.consumer(queue, handler, concurency=20)  # error: unexpected-keyword
    queue.consumer(handler, concurrency="20")  # error: bad-argument-type
    queue.subscribe_to(topic, filter_polcy={})  # error: unexpected-keyword
    topic.subscribe(queue, raw_message_delivery="yes")  # error: bad-argument-type
    fanferro.Provisioning.ensure(on_drit="raise")  # error: unexpected-keyword
    fanferro.Provisioning.assume(on_error="explode")  # error: bad-argument-type
    await dispatch(handler, {}, retyr=None)  # error: unexpected-keyword
"""


def test_type_checkers_check_options(tmp_path: Path) -> None:
    pyrefly = shutil.which("pyrefly", path=str(Path(sys.executable).parent)) or shutil.which("pyrefly")
    if pyrefly is None:
        pytest.skip("pyrefly is not installed")
    probe = tmp_path / "probe.py"
    probe.write_text(textwrap.dedent(_PROBE), encoding="utf-8")
    result = subprocess.run(
        [
            pyrefly,
            "check",
            "--preset",
            "default",
            "--python-interpreter-path",
            sys.executable,
            "--output-format",
            "json",
            str(probe),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    reported = {(error["line"], error["name"]) for error in json.loads(result.stdout)["errors"]}
    expected = {
        (number, match.group(1))
        for number, line in enumerate(probe.read_text(encoding="utf-8").splitlines(), start=1)
        if (match := re.search(r"# error: ([a-z-]+)$", line))
    }
    assert reported == expected, result.stdout


@pytest.mark.parametrize("method", ["publish", "send", "prepare"])
async def test_options_with_an_outgoing_message_are_refused(method: str) -> None:
    client = memory_client()
    queue = client.queue("outgoing-with-options")
    async with client:
        with pytest.raises(TypeError, match=r"publish options \(delay, attributes\) cannot be given with an Outgoing"):
            await getattr(queue, method)(OutgoingMessage({"n": 1}), delay=600, attributes={"tenant": "acme"})
        assert (await queue.approximate_count()).total == 0


async def test_batch_defaults_apply_to_plain_bodies_only() -> None:
    client = memory_client()
    queue = client.queue("batch-defaults")
    async with client:
        result = await queue.send_batch(
            [{"n": 1}, OutgoingMessage({"n": 2}, attributes={"tenant": "globex"})], attributes={"tenant": "acme"}
        )
        assert len(result.successful) == 2
        messages = await drain_queue(client, queue)
    assert {m.json()["n"]: m.attribute("tenant") for m in messages} == {1: "acme", 2: "globex"}


def test_memory_client_takes_client_options() -> None:
    broker = fanferro.MemoryBroker()
    client = memory_client(broker=broker, queue_prefix="test-")
    assert client.broker is broker
    assert client.queue_prefix == "test-"
    assert client.region == "us-east-1"
    assert memory_client(broker=None).broker is not None
    with pytest.raises(TypeError, match="regoin"):
        memory_client(regoin="eu-north-1")  # type: ignore[call-arg]  # pyrefly: ignore[unexpected-keyword]


def test_idempotent_takes_the_middleware_options() -> None:
    store = InMemoryIdempotencyStore()
    middleware = idempotent(store, ttl=60, lock_ttl=30)
    assert middleware.store is store
    assert (middleware.ttl, middleware.lock_ttl) == (60.0, 30.0)
