"""Environment prefixes (``queue_prefix``/``topic_prefix``) and the validation of resource names."""

from __future__ import annotations

from typing import Any

import pytest

import fanferro
from fanferro.testing import memory_client


def staging_client() -> fanferro.Client:
    return memory_client(queue_prefix="staging-", topic_prefix="staging-")


async def _noop(message: fanferro.Message) -> None: ...


def test_no_prefix_by_default() -> None:
    client = memory_client()
    assert (client.queue_prefix, client.topic_prefix) == ("", "")
    queue = client.queue("orders-worker", dead_letter=fanferro.DeadLetterQueue("orders-worker-dlq"))
    assert queue.name == "orders-worker"
    assert queue.dead_letter_queue is not None
    assert queue.dead_letter_queue.name == "orders-worker-dlq"
    assert client.topic("orders").name == "orders"


def test_prefixes_apply_to_queues_topics_and_dead_letter_queues() -> None:
    client = memory_client(queue_prefix="dev-q-", topic_prefix="dev-t-")
    topic = client.topic("orders")
    queue = client.queue("orders-worker", dead_letter=fanferro.DeadLetterQueue("orders-worker-dlq"))
    assert topic.name == "dev-t-orders"
    assert queue.name == "dev-q-orders-worker"
    assert queue.dead_letter_queue is not None
    assert queue.dead_letter_queue.name == "dev-q-orders-worker-dlq"
    assert set(client.topics) == {"dev-t-orders"}
    assert set(client.queues) == {"dev-q-orders-worker", "dev-q-orders-worker-dlq"}
    # The core receives the final names.
    assert topic._spec()["name"] == "dev-t-orders"
    spec = queue._spec()
    assert spec["name"] == "dev-q-orders-worker"
    assert spec["dead_letter"]["queue"]["name"] == "dev-q-orders-worker-dlq"


def test_names_given_as_strings_are_prefixed() -> None:
    client = staging_client()
    orders = client.topic("orders")
    worker = client.queue("orders-worker")
    subscription = client.subscription("orders", "orders-worker", dead_letter_queue="orders-undelivered")
    assert subscription.topic is orders
    assert subscription.queue is worker
    assert subscription.dead_letter_queue is not None
    assert subscription.dead_letter_queue.name == "staging-orders-undelivered"
    assert subscription.name == "staging-orders -> staging-orders-worker"
    assert orders.subscribe("audit-worker").queue.name == "staging-audit-worker"
    assert worker.subscribe_to("payments").topic.name == "staging-payments"
    consumer = client.consumer("orders-worker", _noop)
    assert consumer.queue is worker
    assert consumer.name == "staging-orders-worker"
    assert set(client.queues) == {"staging-orders-worker", "staging-orders-undelivered", "staging-audit-worker"}
    assert set(client.topics) == {"staging-orders", "staging-payments"}


async def test_dead_letter_policy_queue_is_prefixed() -> None:
    client = staging_client()
    consumer = client.consumer("work", _noop, wait_time=1, dead_letter=fanferro.DeadLetterPolicy(queue="parking-lot"))
    async with client:
        await consumer.start()
        await consumer.stop()
    assert client.queues["staging-parking-lot"].resolved
    assert "parking-lot" not in client.queues


def test_redeclaring_a_name_returns_the_same_handle() -> None:
    client = staging_client()
    topic = client.topic("orders")
    queue = client.queue("orders-worker", dead_letter=fanferro.DeadLetterQueue("orders-worker-dlq"))
    assert client.topic("orders") is topic
    assert client.queue("orders-worker") is queue
    assert client.queue("orders-worker-dlq") is queue.dead_letter_queue
    assert client.queue("staging-orders-worker", prefixed=False) is queue, "the same AWS name is the same queue"
    with pytest.raises(fanferro.ConfigurationError, match="topic 'staging-orders' is already declared"):
        client.topic("orders", tags={"team": "orders"})
    with pytest.raises(fanferro.ConfigurationError, match="queue 'staging-orders-worker' is already declared"):
        client.queue("orders-worker", tags={"team": "orders"})


def test_redeclaring_with_the_same_settings_returns_the_handle() -> None:
    client = memory_client()
    attributes = fanferro.QueueAttributes(visibility_timeout=60)
    queue = client.queue("worker", attributes=attributes, tags={"team": "orders"})
    assert client.queue("worker", attributes=attributes, tags={"team": "orders"}) is queue
    assert client.queue("worker", attributes={"VisibilityTimeout": "60"}) is queue, "raw AWS names are equal too"
    with pytest.raises(fanferro.ConfigurationError, match=r"already declared with other settings \(attributes\)"):
        client.queue("worker", attributes=fanferro.QueueAttributes(visibility_timeout=30))
    topic = client.topic("orders", tags={"team": "orders"})
    assert client.topic("orders", tags={"team": "orders"}) is topic
    with pytest.raises(fanferro.ConfigurationError, match=r"\(tags\)"):
        client.topic("orders", tags={"team": "billing"})


def test_queues_can_share_a_dead_letter_queue() -> None:
    client = memory_client()
    first = client.queue("orders", dead_letter=fanferro.DeadLetterQueue("shared-dlq"))
    second = client.queue("billing", dead_letter=fanferro.DeadLetterQueue("shared-dlq", max_receive_count=3))
    assert first.dead_letter_queue is not None
    assert second.dead_letter_queue is first.dead_letter_queue
    assert first.dead_letter_queue.attributes == {"MessageRetentionPeriod": "1209600"}
    # Declared on its own first (to consume it): queues using it as their dead-letter queue keep it.
    parking = client.queue("parking-lot", attributes=fanferro.QueueAttributes(message_retention=3600))
    assert client.queue("returns", dead_letter=fanferro.DeadLetterQueue("parking-lot")).dead_letter_queue is parking
    assert parking.attributes == {"MessageRetentionPeriod": "3600"}
    # Settings a dead-letter queue sets explicitly must agree with its declaration.
    with pytest.raises(fanferro.ConfigurationError, match="'parking-lot' is already declared with other settings"):
        client.queue(
            "refunds",
            dead_letter=fanferro.DeadLetterQueue(
                "parking-lot", attributes=fanferro.QueueAttributes(message_retention=60)
            ),
        )


def test_redeclaring_a_subscription_with_other_settings_raises() -> None:
    client = memory_client()
    topic = client.topic("orders")
    queue = client.queue("worker")
    subscription = client.subscription(topic, queue, filter_policy={"kind": ["a"]})
    assert client.subscription(topic, queue) is subscription
    assert client.subscription(topic, queue, filter_policy={"kind": ["a"]}) is subscription
    assert topic.subscribe(queue, raw_message_delivery=True) is subscription
    with pytest.raises(fanferro.ConfigurationError, match=r"already declared with other settings \(filter_policy\)"):
        client.subscription(topic, queue, filter_policy={"kind": ["b"]})
    with pytest.raises(fanferro.ConfigurationError, match="raw_message_delivery"):
        queue.subscribe_to(topic, raw_message_delivery=False)


def test_prefixed_false_opts_out() -> None:
    client = staging_client()
    audit = client.topic("shared-audit", prefixed=False)
    archive = client.queue(
        "shared-archive", prefixed=False, dead_letter=fanferro.DeadLetterQueue("shared-archive-dlq", prefixed=False)
    )
    worker = client.queue("worker", dead_letter=fanferro.DeadLetterQueue("shared-dlq", prefixed=False))
    assert audit.name == "shared-audit"
    assert archive.name == "shared-archive"
    assert archive.dead_letter_queue is not None
    assert archive.dead_letter_queue.name == "shared-archive-dlq"
    assert worker.name == "staging-worker"
    assert worker.dead_letter_queue is not None
    assert worker.dead_letter_queue.name == "shared-dlq"
    # A name given as a string is always a logical name, so unprefixed resources are passed as handles.
    assert client.subscription(audit, "worker").topic is audit
    assert client.subscription("shared-audit", "worker").topic.name == "staging-shared-audit"


def test_urls_and_arns_are_never_prefixed() -> None:
    client = staging_client()
    base_url = "https://sqs.eu-west-1.amazonaws.com/123456789012"
    base_arn = "arn:aws:sns:eu-west-1:123456789012"
    assert client.queue(url=f"{base_url}/billing-events").name == "billing-events"
    assert client.queue("ledger", url=f"{base_url}/ledger").name == "ledger"
    assert client.topic(arn=f"{base_arn}:billing").name == "billing"
    assert client.topic("audit", arn=f"{base_arn}:audit").name == "audit"
    # A queue declared by the URL of its prefixed name is found by its logical name.
    orders = client.queue(url=f"{base_url}/staging-orders")
    assert client.queue("orders") is orders
    assert client.topic(arn=f"{base_arn}:staging-orders") is client.topic("orders")


def test_fifo_names_keep_their_suffix() -> None:
    client = staging_client()
    events = client.queue("events.fifo", dead_letter=fanferro.DeadLetterQueue("events-dlq.fifo"))
    ledger = client.topic("ledger.fifo")
    assert events.name == "staging-events.fifo"
    assert events.fifo
    assert events.dead_letter_queue is not None
    assert events.dead_letter_queue.name == "staging-events-dlq.fifo"
    assert events.dead_letter_queue.fifo
    assert ledger.name == "staging-ledger.fifo"
    assert ledger.fifo
    assert not client.queue("orders").fifo
    with pytest.raises(
        fanferro.ConfigurationError, match=r"dead-letter queue of 'staging-payments\.fifo' must be FIFO"
    ):
        client.queue("payments.fifo", dead_letter=fanferro.DeadLetterQueue("payments-dlq"))
    with pytest.raises(fanferro.ConfigurationError, match="dead-letter queue of 'staging-payments' must be standard"):
        client.queue("payments", dead_letter=fanferro.DeadLetterQueue("payments-dlq.fifo"))
    assert set(client.queues) == {"staging-events.fifo", "staging-events-dlq.fifo", "staging-orders"}


@pytest.mark.parametrize("argument", ["queue_prefix", "topic_prefix"])
def test_prefixes_are_validated(argument: str) -> None:
    def client_with(prefix: str) -> fanferro.Client:
        options: dict[str, Any] = {argument: prefix}
        return memory_client(**options)

    kind = argument.removesuffix("_prefix")
    for prefix in ("staging.", "dev env-", "ünï-", "team/", "arn:"):
        with pytest.raises(fanferro.ConfigurationError) as raised:
            client_with(prefix)
        assert str(raised.value) == (
            f"{argument} {prefix!r} may only contain letters, digits, '-' and '_', "
            f"like the {kind} names it is prepended to"
        )
    with pytest.raises(fanferro.ConfigurationError) as raised:
        client_with("dev.fifo-")
    assert str(raised.value) == f"{argument} 'dev.fifo-' must not contain '.fifo' (the suffix of FIFO {kind} names)"
    for prefix in ("", "staging-", "dev_2-", "PR-1234-"):
        assert getattr(client_with(prefix), argument) == prefix


def test_long_names_are_rejected_after_prefixing() -> None:
    client = staging_client()
    assert len(client.queue("q" * 72).name) == 80
    assert len(client.queue("f" * 67 + ".fifo").name) == 80
    assert len(client.topic("t" * 248).name) == 256
    assert len(client.topic("u" * 243 + ".fifo").name) == 256
    with pytest.raises(fanferro.ConfigurationError) as raised:
        client.queue("q" * 73)
    assert str(raised.value) == (
        f"queue name 'staging-{'q' * 73}' (prefix 'staging-' + '{'q' * 73}') is 81 characters; "
        "SQS allows 80 - shorten the prefix or the name"
    )
    with pytest.raises(fanferro.ConfigurationError, match="is 81 characters; SQS allows 80"):
        client.queue("f" * 68 + ".fifo")
    with pytest.raises(fanferro.ConfigurationError, match="is 257 characters; SNS allows 256"):
        client.topic("t" * 249)
    with pytest.raises(fanferro.ConfigurationError, match=r"queue name 'staging-d+' \(prefix 'staging-' \+ 'd+'\)"):
        client.queue("worker", dead_letter=fanferro.DeadLetterQueue("d" * 73))
    assert "staging-worker" not in client.queues
    unprefixed = "^queue name 'q{81}' is 81 characters; SQS allows 80 - shorten the name$"
    with pytest.raises(fanferro.ConfigurationError, match=unprefixed):
        memory_client().queue("q" * 81)


def test_names_must_follow_aws_naming_rules() -> None:
    client = staging_client()
    for bad in ("orders.v1", "has space", "ünïcode", "a/b", "a:b", "events.fifo.fifo", "events.FIFO"):
        with pytest.raises(fanferro.ConfigurationError) as raised:
            client.queue(bad)
        assert str(raised.value) == (
            f"queue name 'staging-{bad}' (prefix 'staging-' + {bad!r}) may only contain letters, digits, "
            "'-' and '_' (FIFO queues end in '.fifo')"
        )
        with pytest.raises(fanferro.ConfigurationError, match="may only contain letters, digits"):
            client.topic(bad)
    with pytest.raises(fanferro.ConfigurationError, match=r"^queue name 'orders\.v1' may only contain"):
        memory_client().queue("orders.v1")
    with pytest.raises(fanferro.ConfigurationError, match=r"^topic name '\.fifo' may only contain"):
        memory_client().topic(".fifo")
    for kind in ("queue", "topic"):
        with pytest.raises(fanferro.ConfigurationError, match=f"^{kind} name must not be empty"):
            getattr(client, kind)("")
    # Names taken from URLs and ARNs are checked as they are.
    with pytest.raises(fanferro.ConfigurationError, match=r"^queue name 'my\.queue' may only contain"):
        client.queue(url="http://localhost:4566/000000000000/my.queue")
    with pytest.raises(fanferro.ConfigurationError, match=r"^topic name 'my\.topic' may only contain"):
        client.topic(arn="arn:aws:sns:us-east-1:000000000000:my.topic")
