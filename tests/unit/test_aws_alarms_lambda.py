"""CloudWatch alarms and Lambda destination records and dead letters (fanferro.aws)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import fanferro
from fanferro import aws, testing
from fanferro.envelopes import AwsNotifications
from tests.aws_samples import (
    CLOUDWATCH_ALARM,
    CLOUDWATCH_ALARM_STATE_CHANGE,
    LAMBDA_FAILURE,
    S3_PUT,
    eventbridge,
)


def decode[T: aws.AwsDocument](cls: type[T], document: dict[str, Any], message_type: str) -> T:
    payload = AwsNotifications().unwrap(fanferro.WireMessage(json.dumps(document)))
    assert payload is not None
    assert payload.message_type == message_type
    assert message_type in fanferro.Client(region="us-east-1").types.names_of(cls)
    value = cls.from_json(json.loads(payload.data))
    assert cls.from_json(value.to_json()) == value, "round trip"
    return value


def test_cloudwatch_alarm_notifications() -> None:
    alarm = decode(aws.CloudWatchAlarm, CLOUDWATCH_ALARM, "aws.cloudwatch.alarm")
    assert (alarm.alarm_name, alarm.new_state_value, alarm.old_state_value) == ("ServerCpuTooHigh", "ALARM", "OK")
    assert (alarm.alarming, alarm.ok) == (True, False)
    assert alarm.state_change_time == datetime(2019, 10, 2, 17, 4, 40, 989000, tzinfo=UTC)
    assert alarm.alarm_actions == ("arn:aws:sns:us-east-1:123456789012:alarms",)
    assert alarm.trigger is not None
    assert (alarm.trigger.metric_name, alarm.trigger.namespace, alarm.trigger.threshold) == (
        "CPUUtilization",
        "AWS/EC2",
        50.0,
    )
    assert alarm.trigger.dimensions == (aws.CloudWatchDimension(name="InstanceId", value="i-12345678901234567"),)
    assert (alarm.trigger.period, alarm.trigger.evaluation_periods, alarm.trigger.unit) == (300, 1, None)


def test_cloudwatch_alarm_state_changes() -> None:
    change = decode(
        aws.CloudWatchAlarmStateChange,
        eventbridge("aws.cloudwatch", "CloudWatch Alarm State Change", CLOUDWATCH_ALARM_STATE_CHANGE),
        "aws.cloudwatch.alarm_state_change",
    )
    assert (change.alarm_name, change.alarming) == ("ServerCpuTooHigh", True)
    assert change.state.timestamp == datetime(2019, 10, 2, 17, 4, 40, 989000, tzinfo=UTC)
    assert change.state.reason_data_json()["recentDatapoints"] == [99.50160229693434]
    assert change.previous_state is not None
    assert change.previous_state.value == "OK"
    assert change.configuration["description"].startswith("Goes into alarm")
    assert change.alarm_tags == {"Environment": "prod", "Team": "cloudwatch"}


def test_lambda_invocation_records() -> None:
    record = decode(aws.LambdaInvocationRecord, LAMBDA_FAILURE, "aws.lambda.invocation_failure")
    assert not record.succeeded
    assert record.request_context == aws.LambdaRequestContext(
        request_id="e4b46cbf-b738-xmpl-8880-a18cdf61200e",
        function_arn="arn:aws:lambda:us-east-1:123456789012:function:my-function:$LATEST",
        condition="RetriesExhausted",
        approximate_invoke_count=3,
    )
    assert record.response_context == aws.LambdaResponseContext(
        status_code=200, executed_version="$LATEST", function_error="Unhandled"
    )
    assert record.request_payload == LAMBDA_FAILURE["requestPayload"]
    assert record.error_message is not None
    assert record.error_message.endswith("Process exited before completing request")
    assert record.timestamp == datetime(2019, 11, 14, 18, 16, 5, 568000, tzinfo=UTC)
    success = {
        **LAMBDA_FAILURE,
        "requestContext": {**LAMBDA_FAILURE["requestContext"], "condition": "Success"},
        "responsePayload": {"ok": True},
    }
    record = decode(aws.LambdaInvocationRecord, success, "aws.lambda.invocation_success")
    assert record.succeeded
    assert record.error_message is None
    through_eventbridge = eventbridge("lambda", "Lambda Function Invocation Result - Failure", LAMBDA_FAILURE)
    assert not decode(aws.LambdaInvocationRecord, through_eventbridge, "aws.lambda.invocation_failure").succeeded


async def test_lambda_dead_letters() -> None:
    received: list[tuple[aws.S3Event, aws.LambdaDeadLetter | None]] = []
    router = fanferro.Router()

    @router.route("aws.lambda.dead_letter")
    async def on_failed(message: fanferro.Message[aws.S3Event]) -> None:
        received.append((message.body, aws.LambdaDeadLetter.of(message)))

    @router.route
    async def on_upload(event: aws.S3Event) -> None:
        raise AssertionError("a failed event is not processed again by the S3 route")

    client = testing.memory_client()
    queue = client.queue("function-dlq")
    async with client:
        await queue.send(
            S3_PUT,
            envelope=AwsNotifications(),
            attributes={"RequestID": "e4b46cbf", "ErrorCode": 200, "ErrorMessage": "Process exited"},
        )
        await client.consumer(queue, router, wait_time=1).run_until_idle(idle_for=0.3, timeout=10)
    [(event, failure)] = received
    assert event.records[0].object.key == "HappyFace.jpg"
    assert failure == aws.LambdaDeadLetter(request_id="e4b46cbf", error_code=200, error_message="Process exited")
    assert aws.LambdaDeadLetter.of(testing.build_message({"a": 1})) is None
