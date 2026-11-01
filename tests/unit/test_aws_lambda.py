"""fanferro.aws_lambda with SQS events in the layout Lambda hands a function (recorded records)."""

from __future__ import annotations

import asyncio
import base64
import json
from typing import Any

import pytest

import fanferro
from fanferro.testing import memory_client

ARN = "arn:aws:sqs:us-east-1:000000000000:orders"


def record(
    body: str,
    *,
    message_id: str,
    attributes: dict[str, dict[str, Any]] | None = None,
    system: dict[str, str] | None = None,
    arn: str = ARN,
) -> dict[str, Any]:
    """A record as an SQS event source mapping hands it to a function (receipt handles are fake)."""
    return {
        "messageId": message_id,
        "receiptHandle": f"AQEB{message_id}==",
        "body": body,
        "attributes": {
            "ApproximateReceiveCount": "1",
            "SentTimestamp": "1793500000000",
            "SenderId": "AIDAIENQZJOLO23YVJ4VO",
            "ApproximateFirstReceiveTimestamp": "1793500000005",
            **(system or {}),
        },
        "messageAttributes": {
            name: {"stringListValues": [], "binaryListValues": [], **value}
            for name, value in (attributes or {}).items()
        },
        "md5OfBody": "ignored",
        "md5OfMessageAttributes": None,
        "eventSource": "aws:sqs",
        "eventSourceARN": arn,
        "awsRegion": "us-east-1",
    }


RAW = record('{"order_id": 1}', message_id="raw-1")
"""A plain JSON body sent by another application (no fanferro attributes)."""

SNS = record(
    json.dumps(
        {
            "Type": "Notification",
            "MessageId": "22b80b92-fdea-4c2c-8f9d-bdfb0c7bf324",
            "TopicArn": "arn:aws:sns:us-east-1:000000000000:orders",
            "Message": json.dumps({"order_id": 2}),
            "Timestamp": "2026-11-01T10:00:00.000Z",
            "SignatureVersion": "1",
            "Signature": "EXAMPLE",
            "SigningCertURL": "https://sns.us-east-1.amazonaws.com/cert.pem",
            "UnsubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe",
            "MessageAttributes": {
                "fanferro.type": {"Type": "String", "Value": "orders.placed.v1"},
                "fanferro.id": {"Type": "String", "Value": "order-2"},
            },
        }
    ),
    message_id="sns-2",
)
"""An SNS notification (subscription without raw delivery) of a fanferro raw message."""

CLOUDEVENT = record(
    json.dumps(
        {
            "specversion": "1.0",
            "type": "orders.placed.v1",
            "source": "urn:service:shop",
            "id": "order-3",
            "time": "2026-11-01T10:00:00Z",
            "datacontenttype": "application/json",
            "data": {"order_id": 3},
        }
    ),
    message_id="ce-3",
    attributes={"content-type": {"dataType": "String", "stringValue": "application/cloudevents+json"}},
)
"""A structured CloudEvent."""

BASE64 = record(
    base64.b64encode(b"\x00\x01binary").decode(),
    message_id="b64-4",
    attributes={
        "fanferro.encoding": {"dataType": "String", "stringValue": "base64"},
        "fanferro.content-type": {"dataType": "String", "stringValue": "application/octet-stream"},
        "tag": {"dataType": "Binary", "binaryValue": base64.b64encode(b"\xff").decode()},
    },
)
"""A binary body fanferro sent in base64."""


@fanferro.message_type("orders.placed.v1")
class OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


async def test_recorded_records_are_unwrapped_and_routed() -> None:
    client = memory_client()
    client.queue("orders")
    seen: dict[str, Any] = {}

    async def handle(message: fanferro.Message[Any]) -> None:
        seen[str(message.sqs_message_id)] = (message.body, message.type, message.id, message.envelope)
        if message.sqs_message_id == "b64-4":
            assert message.attributes == {"tag": fanferro.AttributeValue.binary(b"\xff")}
        if message.sqs_message_id == "sns-2":
            assert message.topic_arn == "arn:aws:sns:us-east-1:000000000000:orders"

    handler = fanferro.aws_lambda.sqs_handler(client, handle)
    response = await handler.handle({"Records": [RAW, SNS, CLOUDEVENT, BASE64]})
    assert response == {"batchItemFailures": []}
    assert seen == {
        "raw-1": ({"order_id": 1}, None, "raw-1", "raw"),
        "sns-2": ({"order_id": 2}, "orders.placed.v1", "order-2", "raw"),
        "ce-3": ({"order_id": 3}, "orders.placed.v1", "order-3", "cloudevents"),
        "b64-4": (b"\x00\x01binary", None, "b64-4", "raw"),
    }
    await client.close()


async def test_retries_set_the_visibility_and_list_the_record_even_when_that_fails() -> None:
    client = memory_client()
    client.queue("orders")
    failures: list[fanferro.Event] = []
    client.on("consumer.error", failures.append)

    async def handle(order: dict[str, int]) -> None:
        raise ConnectionError("downstream unavailable")

    handler = fanferro.aws_lambda.sqs_handler(client, handle)
    # The fake receipt handle is refused by ChangeMessageVisibility: the record is listed anyway.
    response = await handler.handle({"Records": [RAW]})
    assert response == {"batchItemFailures": [{"itemIdentifier": "raw-1"}]}
    assert any("failed to settle message (retry)" in event.message for event in failures)
    await client.close()


async def test_fifo_records_of_a_group_after_a_failure_are_listed() -> None:
    client = memory_client()
    client.queue("orders.fifo")
    arn = ARN + ".fifo"
    records = [
        record(json.dumps({"n": n}), message_id=n, system={"MessageGroupId": n[0], "SequenceNumber": str(i)}, arn=arn)
        for i, n in enumerate(["a1", "a2", "a3", "b1"])
    ]
    handled: list[str] = []

    async def handle(body: dict[str, str]) -> None:
        handled.append(body["n"])
        if body["n"] == "a2":
            raise fanferro.Retry(delay=0)

    handler = fanferro.aws_lambda.sqs_handler(client, handle)
    response = await handler.handle({"Records": records})
    assert [item["itemIdentifier"] for item in response["batchItemFailures"]] == ["a2", "a3"]
    assert sorted(handled) == ["a1", "a2", "b1"]
    await client.close()


async def test_auto_ack_off_lists_records_the_handler_did_not_settle() -> None:
    client = memory_client()
    client.queue("orders")

    async def handle(message: fanferro.Message[Any]) -> None:
        if message.sqs_message_id == "ce-3":
            await message.ack()

    handler = fanferro.aws_lambda.sqs_handler(client, handle, auto_ack=False)
    response = await handler.handle({"Records": [RAW, CLOUDEVENT]})
    assert response == {"batchItemFailures": [{"itemIdentifier": "raw-1"}]}
    await client.close()


async def test_records_of_another_queue_are_refused() -> None:
    client = memory_client()
    queue = client.queue("payments")

    async def handle(body: Any) -> None:
        pass

    handler = fanferro.aws_lambda.sqs_handler(client, handle, queue=queue)
    with pytest.raises(fanferro.ConfigurationError, match="this handler consumes 'payments'"):
        await handler.handle({"Records": [RAW]})
    nameless = fanferro.aws_lambda.sqs_handler(client, handle)
    with pytest.raises(fanferro.ConfigurationError, match="names no queue"):
        await nameless.handle({"Records": [{**RAW, "eventSourceARN": ""}]})
    await client.close()


async def test_invalid_records_fail_the_invocation() -> None:
    client = memory_client()
    client.queue("orders")

    async def handle(body: Any) -> None:
        pass

    handler = fanferro.aws_lambda.sqs_handler(client, handle)
    broken = {**RAW, "messageAttributes": {"tag": {"dataType": "Binary", "binaryValue": "not base64!"}}}
    with pytest.raises(fanferro.ValidationError, match="base64"):
        await handler.handle({"Records": [broken]})
    assert await handler.handle({"Records": []}) == {"batchItemFailures": []}
    await client.close()


def test_options_are_validated_when_the_handler_is_created() -> None:
    client = memory_client()
    with pytest.raises(ValueError, match="concurrency"):
        fanferro.aws_lambda.sqs_handler(client, concurrency=0)
    with pytest.raises(ValueError, match="timeout_margin"):
        fanferro.aws_lambda.sqs_handler(client, timeout_margin=61)
    assert client.queues == {}, "validating declares no queue"


def test_the_handler_is_a_synchronous_function_that_keeps_its_loop() -> None:
    client = memory_client()
    client.queue("orders")
    loops: list[asyncio.AbstractEventLoop] = []

    async def handle(body: Any) -> None:
        loops.append(asyncio.get_running_loop())

    handler = fanferro.aws_lambda.sqs_handler(client, handle)
    assert handler({"Records": [RAW]}, None) == {"batchItemFailures": []}
    assert handler({"Records": [CLOUDEVENT]}, None) == {"batchItemFailures": []}
    assert len(loops) == 2
    assert loops[0] is loops[1]
    loop = handler._loop
    assert loop is not None
    loop.run_until_complete(client.close())
    loop.close()
