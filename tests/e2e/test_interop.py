"""Interoperability with other SQS/SNS clients, against a real AWS protocol implementation."""

from __future__ import annotations

import base64
import hashlib
import json
from typing import Any
from urllib.parse import urlsplit

import boto3
import pytest

import fanferro
from tests.conftest import emulator_client
from tests.helpers import OrderCreated, drain_queue

pytestmark = pytest.mark.e2e


@pytest.fixture
def aws(ministack_endpoint: str) -> dict[str, Any]:
    session = boto3.session.Session(aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1")
    return {
        "sqs": session.client("sqs", endpoint_url=ministack_endpoint),
        "sns": session.client("sns", endpoint_url=ministack_endpoint),
        "s3": session.client("s3", endpoint_url=ministack_endpoint),
    }


async def test_boto3_producer_fanferro_consumer(ministack_endpoint: str, aws: dict[str, Any], name: Any) -> None:
    client = emulator_client(ministack_endpoint)
    queue = client.queue(name("from-boto3"))
    async with client:
        info = await queue.resolve()
        aws["sqs"].send_message(
            QueueUrl=info.url,
            MessageBody=json.dumps({"order_id": 3, "total": 2.5}),
            MessageAttributes={"tenant": {"DataType": "String", "StringValue": "acme"}},
        )
        received: list[OrderCreated] = []
        consumer = client.consumer(queue, wait_time=1)

        @consumer.default
        async def handle(message: fanferro.Message[OrderCreated]) -> None:
            assert message.envelope == "raw"
            assert message.attribute("tenant") == "acme"
            assert message.id == message.sqs_message_id
            received.append(message.body)

        await consumer.run_until_idle(idle_for=1.0, timeout=20)
    assert received == [OrderCreated(order_id=3, total=2.5)]


async def test_fanferro_producer_boto3_consumer(ministack_endpoint: str, aws: dict[str, Any], name: Any) -> None:
    client = emulator_client(ministack_endpoint)
    queue = client.queue(name("to-boto3"))
    async with client:
        info = await queue.resolve()
        sent = await queue.send(OrderCreated(order_id=1), attributes={"tenant": "acme"})
        await queue.send(b"\x00\xffbinary", type="blob")
    response = aws["sqs"].receive_message(
        QueueUrl=info.url, MaxNumberOfMessages=10, MessageAttributeNames=["All"], WaitTimeSeconds=1
    )
    by_type = {m["MessageAttributes"]["fanferro.type"]["StringValue"]: m for m in response["Messages"]}
    order = by_type["test.order_created.v1"]
    assert json.loads(order["Body"]) == {"order_id": 1, "total": 0.0}, "the body is the plain JSON document"
    attributes = order["MessageAttributes"]
    assert attributes["fanferro.id"]["StringValue"] == sent.message_id
    assert attributes["tenant"]["StringValue"] == "acme"
    assert "content-type" not in attributes, "JSON is implied"
    blob = by_type["blob"]
    assert base64.b64decode(blob["Body"]) == b"\x00\xffbinary"
    assert blob["MessageAttributes"]["fanferro.encoding"]["StringValue"] == "base64"
    assert blob["MessageAttributes"]["content-type"]["StringValue"] == "application/octet-stream"


async def test_cloudevents_producer_boto3_consumer(ministack_endpoint: str, aws: dict[str, Any], name: Any) -> None:
    client = emulator_client(ministack_endpoint, envelope=fanferro.envelopes.CloudEvents())
    queue = client.queue(name("ce-to-boto3"))
    async with client:
        info = await queue.resolve()
        await queue.send(OrderCreated(order_id=1), attributes={"tenant": "acme"})
        await queue.send(b"\x00\xffbinary", type="blob")
    response = aws["sqs"].receive_message(
        QueueUrl=info.url, MaxNumberOfMessages=10, MessageAttributeNames=["All"], WaitTimeSeconds=1
    )
    by_type = {json.loads(m["Body"])["type"]: m for m in response["Messages"]}
    order = by_type["test.order_created.v1"]
    cloud_event = json.loads(order["Body"])
    assert cloud_event["data"] == {"order_id": 1, "total": 0.0}
    assert order["MessageAttributes"]["content-type"]["StringValue"] == "application/cloudevents+json"
    assert order["MessageAttributes"]["tenant"]["StringValue"] == "acme"
    binary_event = json.loads(by_type["blob"]["Body"])
    assert base64.b64decode(binary_event["data_base64"]) == b"\x00\xffbinary"


async def test_aws_extended_client_pointers(ministack_endpoint: str, aws: dict[str, Any], name: Any) -> None:
    bucket = name("extended")
    aws["s3"].create_bucket(Bucket=bucket)
    client = emulator_client(
        ministack_endpoint, large_payloads=fanferro.LargePayloads(bucket, threshold=1024, format="aws_extended")
    )
    queue = client.queue(name("extended-queue"))
    big = {"data": "y" * 5000}
    async with client:
        info = await queue.resolve()
        # A message written the way the AWS SQS Extended Client Library writes it.
        aws["s3"].put_object(Bucket=bucket, Key="java-written", Body=json.dumps(big).encode())
        aws["sqs"].send_message(
            QueueUrl=info.url,
            MessageBody=json.dumps(
                [
                    "software.amazon.payloadoffloading.PayloadS3Pointer",
                    {"s3BucketName": bucket, "s3Key": "java-written"},
                ]
            ),
            MessageAttributes={"ExtendedPayloadSize": {"DataType": "Number", "StringValue": str(len(json.dumps(big)))}},
        )
        received = await drain_queue(client, queue)
        assert [message.json() for message in received] == [big]
        # fanferro writes pointers in the same format.
        result = await queue.send(big)
        assert result.offloaded is not None
        assert result.offloaded.format == "aws_extended"
    response = aws["sqs"].receive_message(QueueUrl=info.url, MessageAttributeNames=["All"], WaitTimeSeconds=1)
    pointer = json.loads(response["Messages"][0]["Body"])
    assert pointer[0] == "software.amazon.payloadoffloading.PayloadS3Pointer"
    assert "ExtendedPayloadSize" in response["Messages"][0]["MessageAttributes"]
    stored = aws["s3"].get_object(Bucket=pointer[1]["s3BucketName"], Key=pointer[1]["s3Key"])["Body"].read()
    assert json.loads(stored) == big, "the stored object is the message body"


async def test_legacy_aws_extended_client_pointers(ministack_endpoint: str, aws: dict[str, Any], name: Any) -> None:
    # The legacy marker was read, but its pointer class refused: such messages were dead-lettered.
    bucket = name("legacy-extended")
    aws["s3"].create_bucket(Bucket=bucket)
    client = emulator_client(ministack_endpoint)
    queue = client.queue(name("legacy-extended-queue"))
    big = {"data": "z" * 5000}
    async with client:
        info = await queue.resolve()
        aws["s3"].put_object(Bucket=bucket, Key="legacy-written", Body=json.dumps(big).encode())
        aws["sqs"].send_message(
            QueueUrl=info.url,
            MessageBody=json.dumps(
                ["com.amazon.sqs.javamessaging.MessageS3Pointer", {"s3BucketName": bucket, "s3Key": "legacy-written"}]
            ),
            MessageAttributes={"SQSLargePayloadSize": {"DataType": "Number", "StringValue": str(len(json.dumps(big)))}},
        )
        received = await drain_queue(client, queue)
        assert [(message.json(), message.attributes) for message in received] == [(big, {})]
        with pytest.raises(fanferro.ValidationError, match="reserved for the transport layer"):
            await queue.send({"n": 1}, attributes={"SQSLargePayloadSize": 5})


async def test_presigned_url_pointers_are_allow_listed(ministack_endpoint: str, aws: dict[str, Any], name: Any) -> None:
    client = emulator_client(ministack_endpoint)
    queue = client.queue(name("ssrf"), dead_letter=fanferro.DeadLetterQueue(f"{name('ssrf')}-dlq"))
    bucket = name("ssrf-payloads")
    decode_failures: list[fanferro.Event] = []
    client.on(
        "message.decode_failed",
        lambda event: event.fields.get("consumer") == queue.name and decode_failures.append(event),
    )
    payload = json.dumps({"crafted": True}).encode()
    sha256 = hashlib.sha256(payload).hexdigest()
    endpoint = urlsplit(ministack_endpoint)
    async with client:
        info = await queue.resolve()
        aws["s3"].create_bucket(Bucket=bucket)
        aws["s3"].put_object(Bucket=bucket, Key="crafted", Body=payload)
        presigned = aws["s3"].generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": "crafted"})
        assert presigned.startswith(f"{ministack_endpoint}/{bucket}/crafted?")
        refused = [
            {"url": "http://169.254.169.254/latest/meta-data/"},
            # Hosts under amazonaws.com that are not S3: an internal load balancer, an EC2 instance.
            {"url": "https://internal-orders-1234567890.us-east-1.elb.amazonaws.com/", "sha256": sha256},
            {"url": "https://ec2-203-0-113-25.compute-1.amazonaws.com/", "sha256": sha256},
            # Another port on the endpoint's host.
            {"url": f"http://{endpoint.hostname}:{(endpoint.port or 80) + 1}/{bucket}/crafted", "sha256": sha256},
            # The endpoint itself, but without a SHA-256 to check what it answers against.
            {"url": presigned},
        ]
        for pointer in [*refused, {"url": presigned, "size": len(payload), "sha256": sha256}]:
            aws["sqs"].send_message(
                QueueUrl=info.url,
                MessageBody=json.dumps({"fanferro": "claim-check/v1", **pointer}),
                MessageAttributes={"fanferro.claim-check": {"DataType": "String", "StringValue": "v1"}},
            )
        received: list[Any] = []
        consumer = client.consumer(queue, wait_time=1)

        @consumer.default
        async def handle(message: fanferro.Message[Any]) -> None:
            received.append(message.json())

        await consumer.run_until_idle(idle_for=1.0, timeout=20)
        assert queue.dead_letter_queue is not None
        dead_counts = await queue.dead_letter_queue.approximate_count()
    assert received == [{"crafted": True}], "the pointer with a SHA-256 is fetched from the endpoint"
    messages = sorted(str(event.error.message) for event in decode_failures)  # type: ignore[union-attr]
    assert len(messages) == len(refused), messages
    assert all("refusing to fetch" in message for message in messages), messages
    assert sum("not an S3 endpoint" in message for message in messages) == 2, messages
    assert sum("over http:// from" in message for message in messages) == 2, messages
    assert sum("without a sha256" in message for message in messages) == 1, messages
    assert dead_counts["visible"] == len(refused)
