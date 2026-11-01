from __future__ import annotations

import asyncio
import datetime
import json
from decimal import Decimal
from typing import Any

import pytest

import fanferro
from fanferro.messages import attribute_python_value, to_attribute_value
from fanferro.testing import RecordingSettler, build_message


def test_attribute_conversions() -> None:
    assert to_attribute_value("x") == fanferro.AttributeValue.string("x")
    assert to_attribute_value(True) == fanferro.AttributeValue.string("true")
    assert to_attribute_value(3) == fanferro.AttributeValue.number("3")
    assert to_attribute_value(Decimal("1.5")) == fanferro.AttributeValue.number("1.5")
    assert to_attribute_value(b"\x01") == fanferro.AttributeValue.binary(b"\x01")
    with pytest.raises(TypeError):
        to_attribute_value(object())  # type: ignore[arg-type]
    assert attribute_python_value(fanferro.AttributeValue.number("7")) == 7
    assert attribute_python_value(fanferro.AttributeValue.number("7.25")) == Decimal("7.25")
    assert attribute_python_value(fanferro.AttributeValue("Number.float", "0.5")) == 0.5


async def test_message_api_and_settlement() -> None:
    settler = RecordingSettler()
    message = build_message({"a": 1}, attributes={"tenant": "acme"}, headers={"x": 1}, settler=settler)
    assert message.body == {"a": 1}
    assert message.json() == {"a": 1}
    assert message.attribute("tenant") == "acme"
    assert message.attribute("missing", "default") == "default"
    assert message.headers == {"x": 1}
    assert message.receive_count == 1
    assert message.attempt == 1
    assert message.sqs_message_id is None
    assert not message.settled
    await message.retry(delay=5)
    assert message.outcome == "retried"
    assert settler.calls == [("retry", {"delay": 5, "error": None})]
    with pytest.raises(RuntimeError, match="already settled"):
        await message.ack()
    with pytest.raises(RuntimeError):
        await message.extend(10)


@pytest.mark.parametrize("delay", [-1, float("nan"), "5"])
async def test_invalid_retry_delays_settle_nothing(delay: Any) -> None:
    # The delay was only checked by the native call, after the message had been marked settled.
    settler = RecordingSettler()
    message = build_message({"a": 1}, settler=settler)
    error = TypeError if isinstance(delay, str) else ValueError
    with pytest.raises(error, match="delay"):
        await message.retry(delay=delay)
    with pytest.raises(error, match="delay"):
        await message.requeue(delay=delay)
    with pytest.raises(error, match="delay"):
        fanferro.Retry(delay=delay)
    with pytest.raises(error, match="delay"):
        fanferro.Requeue(delay=delay)
    assert not message.settled
    assert settler.calls == []
    await message.retry(delay=2)
    assert settler.calls == [("retry", {"delay": 2.0, "error": None})]
    assert fanferro.Retry(delay=3).delay == fanferro.Requeue(delay=3).delay == 3.0


class _FailingSettler(RecordingSettler):
    def __init__(self) -> None:
        super().__init__()
        self.gate = asyncio.Event()
        self.gate.set()

    async def dead_letter(self, message: fanferro.Message[Any], reason: str, error: BaseException | None) -> None:
        await self.gate.wait()
        await asyncio.sleep(0)
        raise fanferro.LeaseError("lease lost")


async def test_settlement_methods_settle_when_called() -> None:
    # They were coroutine functions: without ``await`` nothing happened (#179).
    settler = RecordingSettler()
    message = build_message({"a": 1}, settler=settler)
    settlement = message.dead_letter("invalid")
    assert isinstance(settlement, fanferro.Settlement)
    assert settlement.operation == "dead_letter"
    assert message.settled
    assert message.outcome == "dead_lettered"
    assert settler.calls == [("dead_letter", {"reason": "invalid", "error": None})]
    await settlement
    await settlement  # awaiting again is harmless
    assert settlement.done()
    assert "Settlement('dead_letter'" in repr(settlement)


async def test_awaiting_a_settlement_raises_its_error() -> None:
    message = build_message({"a": 1}, settler=_FailingSettler())
    settlement = message.dead_letter()
    assert message.settled
    assert not settlement.done()
    with pytest.raises(fanferro.LeaseError, match="lease lost"):
        await settlement
    assert settlement.done()


def test_settlements_outside_an_event_loop() -> None:
    settler = RecordingSettler()
    message = build_message({"a": 1}, settler=settler)
    settlement = message.discard("test")
    assert settlement.done()
    assert settler.calls == [("discard", {"reason": "test"})]
    with pytest.raises(RuntimeError, match="needs a running event loop"):
        build_message({"a": 1}, settler=_FailingSettler()).dead_letter()


async def test_settlements_without_a_settler() -> None:
    message = build_message({"a": 1})
    message._settler = None
    settlement = message.release()
    assert settlement.done()
    await settlement
    assert message.outcome == "released"


async def test_a_cancelled_wait_does_not_stop_the_settlement() -> None:
    settler = _FailingSettler()
    settler.gate.clear()
    message = build_message({"a": 1}, settler=settler)
    settlement = message.dead_letter()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(settlement, timeout=0.01)
    assert not settlement.done()
    settler.gate.set()
    with pytest.raises(fanferro.LeaseError):
        await settlement


def test_internal_attributes_are_hidden() -> None:
    message = fanferro.Message(
        payload=fanferro.Payload(b"x"),
        attributes={"fanferro.type": "t", "tenant": "acme", "ExtendedPayloadSize": "3", "fanferro.attempt": "2"},
    )
    assert set(message.attributes) == {"tenant"}
    assert "fanferro.type" in message.raw_attributes
    assert message.attempt == 3


def test_body_before_routing() -> None:
    message = fanferro.Message(payload=fanferro.Payload(b"{}"))
    with pytest.raises(RuntimeError, match="decoded after routing"):
        _ = message.body
    bad = fanferro.Message(payload=fanferro.Payload(b"{"))
    with pytest.raises(fanferro.DecodeError):
        bad.json()


def dead_lettered(document: str) -> fanferro.Message[Any]:
    return fanferro.Message(payload=fanferro.Payload(b"x"), attributes={"fanferro.dead-letter": document})


def test_dead_letter_info() -> None:
    document = {
        "reason": "retries exhausted",
        "source": "arn:aws:sqs:eu-west-1:123456789012:orders",
        "sqs_message_id": "sqs-1",
        "receive_count": 3,
        "time": "2026-11-01T12:30:00.123456789Z",
        "queue": "orders",
        "attempt": "3",
        "message_type": "order.created.v1",
        "message_id": "order-1",
        "error_type": "builtins.ValueError",
        "error_message": "boom",
        "added_later": "kept",
    }
    info = dead_lettered(json.dumps(document)).dead_letter_info
    assert info is not None
    assert info == fanferro.DeadLetterInfo(
        reason="retries exhausted",
        source="arn:aws:sqs:eu-west-1:123456789012:orders",
        queue="orders",
        message_id="order-1",
        sqs_message_id="sqs-1",
        receive_count=3,
        attempt=3,
        message_type="order.created.v1",
        error_type="builtins.ValueError",
        error_message="boom",
        time=datetime.datetime(2026, 11, 1, 12, 30, 0, 123456, tzinfo=datetime.UTC),
    )
    assert info.document == document, "the whole document, keys the class does not describe included"
    assert "document" not in repr(info)


def test_dead_letter_info_of_a_minimal_or_foreign_document() -> None:
    info = dead_lettered(json.dumps({"reason": "boom", "receive_count": "x", "time": "yesterday"})).dead_letter_info
    assert info == fanferro.DeadLetterInfo(reason="boom")
    for document in ("nope", "[]", '{"receive_count": 3}', '{"reason": 3}'):
        assert dead_lettered(document).dead_letter_info is None, document
    assert fanferro.Message(payload=fanferro.Payload(b"x")).dead_letter_info is None


def test_prepared_message_roundtrip() -> None:
    wire = fanferro.WireMessage(b"\x00\x01binary", {"a": "x", "b": b"\x02", "n": 5})
    prepared = fanferro.PreparedMessage(
        destination={"kind": "queue", "url": "u", "fifo": True},
        destination_name="q.fifo",
        wire=wire,
        message_id="id-1",
        message_type="t",
        group_id="g",
        deduplication_id="d",
        delay=None,
        sns_subject=None,
        trace_header="Root=1-a-b",
    )
    restored = fanferro.PreparedMessage.from_json(prepared.to_json())
    assert restored == prepared
    text = fanferro.PreparedMessage(
        destination={}, destination_name="x", wire=fanferro.WireMessage("hello"), message_id="1"
    )
    assert text.to_dict()["body"] == {"text": "hello"}
    # Valid UTF-8 with characters SQS does not take as text (PostgreSQL's jsonb rejects the JSON
    # escape of U+0000): such bodies are stored as base64, like binary ones.
    for body in ("a\x00b", b"\x08\x00", "bell\x07"):
        control = fanferro.PreparedMessage(
            destination={}, destination_name="x", wire=fanferro.WireMessage(body), message_id="1"
        )
        assert "text" not in control.to_dict()["body"]
        assert "\\u00" not in control.to_json()
        assert fanferro.PreparedMessage.from_json(control.to_json()) == control
    with pytest.raises(ValueError, match="version"):
        fanferro.PreparedMessage.from_dict({"version": 2})


def test_batch_result() -> None:
    ok = fanferro.PublishResult("1", "t1", "q")
    failure = fanferro.BatchFailure(1, fanferro.OutgoingMessage(body={}), fanferro.ThrottledError("slow down"))
    result = fanferro.BatchPublishResult((ok, failure))
    assert result.successful == [ok]
    assert result.failed == [failure]
    assert not result.ok
    assert len(result) == 2
    with pytest.raises(fanferro.BatchPublishError) as raised:
        result.raise_for_failures()
    assert raised.value.result is result
    fanferro.BatchPublishResult((ok,)).raise_for_failures()


def test_queue_and_topic_attributes() -> None:
    attributes = fanferro.QueueAttributes(
        visibility_timeout=datetime.timedelta(minutes=2),
        message_retention=86400,
        delay=0,
        receive_wait_time=20,
        max_message_size=1048576,
        encryption=fanferro.Kms("alias/aws/sqs", data_key_reuse=300),
        content_based_deduplication=True,
        deduplication_scope="messageGroup",
        fifo_throughput_limit="perMessageGroupId",
        policy={"Version": "2012-10-17"},
        redrive_allow_policy={"redrivePermission": "allowAll"},
        extra={"Custom": "1"},
    ).to_aws()
    assert attributes["VisibilityTimeout"] == "120"
    assert attributes["KmsMasterKeyId"] == "alias/aws/sqs"
    assert attributes["KmsDataKeyReusePeriodSeconds"] == "300"
    assert attributes["ContentBasedDeduplication"] == "true"
    assert json.loads(attributes["Policy"]) == {"Version": "2012-10-17"}
    assert attributes["Custom"] == "1"
    assert fanferro.QueueAttributes(encryption=fanferro.SqsManagedEncryption(enabled=False)).to_aws() == {
        "SqsManagedSseEnabled": "false"
    }
    topic = fanferro.TopicAttributes(
        display_name="Orders",
        encryption=fanferro.Kms("alias/aws/sns"),
        tracing="Active",
        signature_version=2,
        max_message_size=1048576,
        content_based_deduplication=False,
        fifo_throughput_scope="MessageGroup",
        delivery_policy={"x": 1},
        archive_policy={"MessageRetentionPeriod": "30"},
    ).to_aws()
    assert topic["TracingConfig"] == "Active"
    assert topic["SignatureVersion"] == "2"
    assert topic["ContentBasedDeduplication"] == "false"
    with pytest.raises(ValueError, match="non-negative"):
        fanferro.QueueAttributes(visibility_timeout=-1).to_aws()


def test_provisioning_presets_and_reports() -> None:
    assert fanferro.Provisioning.existing() == fanferro.Provisioning()
    assert fanferro.Provisioning.create_missing().create
    ensure = fanferro.Provisioning.ensure(on_drift="raise")
    assert ensure.create
    assert ensure.update
    assert ensure.on_drift == "raise"
    assume = fanferro.Provisioning.assume()
    assert not assume.lookup
    assert assume.to_core() == {"lookup": False, "verify": False, "create": False, "update": False}
    report = fanferro.ProvisioningReport.from_core(
        {
            "kind": "queue",
            "name": "q",
            "status": "found",
            "resource": {"name": "q", "url": "u", "arn": "a", "fifo": False, "attributes": {"VisibilityTimeout": "30"}},
            "drift": [{"attribute": "DelaySeconds", "expected": "5", "actual": "0", "mutable": True}],
            "actions": [],
            "error": None,
            "dead_letter": None,
        }
    )
    assert isinstance(report.resource, fanferro.QueueInfo)
    assert report.resource.visibility_timeout == 30
    assert not report.ok
    with pytest.raises(ValueError, match="max_receive_count"):
        fanferro.DeadLetterQueue("x", max_receive_count=0)


def test_policies() -> None:
    backoff = fanferro.Backoff(initial=1, maximum=10, multiplier=2, jitter=False)
    assert [backoff.delay(n) for n in (1, 2, 3, 4, 5)] == [1, 2, 4, 8, 10]
    jittered = fanferro.Backoff.exponential(initial=4, maximum=4)
    assert all(2 <= jittered.delay(1) <= 4 for _ in range(50))
    assert fanferro.Backoff.constant(3).delay(9) == 3
    assert fanferro.Backoff.immediate().delay(3) == 0
    # Retrying forever (max_attempts=None) reaches attempt numbers whose power overflows a float.
    assert backoff.delay(5000) == 10
    assert fanferro.Backoff(initial=0, maximum=10, jitter=False).delay(5000) == 0
    assert 150 <= fanferro.Backoff().delay(10**6) <= 300
    assert fanferro.Backoff(initial=1.5, maximum=10, multiplier=2, jitter=False).delay(10**9) == 10
    with pytest.raises(ValueError, match="multiplier"):
        fanferro.Backoff(multiplier=0.5)

    policy = fanferro.RetryPolicy(max_attempts=3, dead_letter_on=(KeyError,))
    assert policy.decide(ValueError()) == "retry"
    assert policy.decide(KeyError()) == "dead_letter"
    assert policy.exhausted(3)
    assert not policy.exhausted(2)
    assert not fanferro.RetryPolicy(max_attempts=None).exhausted(1000)
    strict = fanferro.RetryPolicy(retry_on=(TimeoutError,), dead_letter_unknown=True)
    assert strict.decide(TimeoutError()) == "retry"
    assert strict.decide(ValueError()) == "dead_letter"
    classified = fanferro.RetryPolicy(classify=lambda error: "discard" if isinstance(error, LookupError) else None)
    assert classified.decide(IndexError()) == "discard"
    assert classified.decide(ValueError()) == "retry"
    with pytest.raises(ValueError, match="max_attempts"):
        fanferro.RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError, match="per_second"):
        fanferro.RateLimit(0)
    with pytest.raises(ValueError, match="drain_timeout"):
        fanferro.ShutdownPolicy(drain_timeout=-1)
    assert fanferro.Heartbeat(interval=5, extend_by=30, max_duration=3600).to_core() == {
        "enabled": True,
        "interval": 5.0,
        "extend_by": 30.0,
        "max_lease": 3600.0,
    }


async def _noop(message: fanferro.Message) -> None: ...


def test_consumer_options() -> None:
    client = fanferro.testing.memory_client()
    queue = client.queue("options")
    consumer = client.consumer(queue, _noop)
    assert consumer.prefetch == 10, "one receive batch per poller ahead of the handlers"
    assert consumer.min_receive_batch is None
    assert consumer.receive_batch_wait == 0.1
    assert consumer.settle_linger == 0.005
    assert client.consumer(queue, _noop, pollers=3).prefetch == 30
    assert client.consumer(queue, _noop, batch_size=4, pollers=2).prefetch == 8
    assert client.consumer(queue, _noop, concurrency=2, pollers=2).prefetch == 4, "at most what handlers take"
    assert client.consumer(queue, _noop, prefetch=0).prefetch == 0
    assert consumer.max_pollers is None
    assert consumer._extra_poller_slots == 10, "extra receive loops bring a batch of their own"
    assert client.consumer(queue, _noop, concurrency=4)._extra_poller_slots == 4
    assert client.consumer(queue, _noop, prefetch=10)._extra_poller_slots == 0, "an explicit prefetch is a bound"
    assert client.consumer(queue, _noop, pollers=2, max_pollers=2).max_pollers == 2
    invalid: list[tuple[fanferro.ConsumerOptions, str]] = [
        ({"min_receive_batch": 0}, "min_receive_batch"),
        ({"batch_size": 5, "min_receive_batch": 6}, "min_receive_batch"),
        ({"receive_batch_wait": -1}, "receive_batch_wait"),
        ({"settle_linger": 2}, "settle_linger"),
        ({"pollers": 0}, "pollers"),
        ({"pollers": 4, "max_pollers": 3}, "max_pollers"),
        ({"max_pollers": 65}, "max_pollers"),
        ({"prefetch": -1}, "prefetch"),
    ]
    for options, error in invalid:
        with pytest.raises(ValueError, match=error):
            client.consumer(queue, _noop, **options)


def test_large_payload_settings() -> None:
    settings = fanferro.LargePayloads("bucket", reference="both", encryption=fanferro.Kms("key"), presign_ttl=60)
    core = settings.to_core()
    assert core["encryption"] == {"mode": "kms", "key_id": "key", "bucket_key": True}
    assert core["presign_ttl"] == 60
    assert fanferro.LargePayloads("b").to_core()["presign_ttl"] == 24 * 3600, "presigned URLs last a day"
    assert fanferro.LargePayloads("b", encryption="s3").to_core()["encryption"] == {"mode": "s3"}
    with pytest.raises(ValueError, match="aws_extended"):
        fanferro.LargePayloads("b", format="aws_extended", reference="url")
    with pytest.raises(ValueError, match="bucket"):
        fanferro.LargePayloads("")
    assert fanferro.PayloadFetch(allowed_url_hosts=["*.example.com"]).to_core()["allowed_url_hosts"] == [
        "*.example.com"
    ]
    assert fanferro.PayloadFetch(allowed_buckets=("payloads",)).to_core()["allowed_buckets"] == ["payloads"]
    assert fanferro.PayloadFetch().to_core()["allowed_buckets"] == [], "any bucket by default"
    # A string is a sequence of strings to type checkers, and would allow its letters.
    with pytest.raises(TypeError, match="allowed_url_hosts must be a list"):
        fanferro.PayloadFetch(allowed_url_hosts="cdn.example.com")
    with pytest.raises(TypeError, match="allowed_buckets must be a list"):
        fanferro.PayloadFetch(allowed_buckets="payloads")


def test_aws_config_objects() -> None:
    assert "s3cr3t" not in repr(fanferro.Credentials("AKIA", "s3cr3t"))
    assert fanferro.AwsRetry(max_attempts=3, mode="adaptive").to_core()["mode"] == "adaptive"
    with pytest.raises(ValueError, match="max_attempts"):
        fanferro.AwsRetry(max_attempts=0).to_core()
    assert fanferro.Timeouts(read=30).to_core()["read"] == 30.0
    assert fanferro.Timeouts().to_core()["read"] == 10.0, "calls without a response time out"
    assert fanferro.Timeouts(read=None).to_core()["read"] is None


def test_message_data_follows_its_payload() -> None:
    message = build_message({"a": 1})
    assert message.data is message.data  # read once
    message.payload = message.payload.replace(data=b'{"b":2}')  # e.g. by a decrypting middleware
    assert message.data == b'{"b":2}'
