"""Typed EventBridge events of AWS services and Auto Scaling's notifications (fanferro.aws)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest

import fanferro
from fanferro import aws, testing
from fanferro.codecs import AwsNotificationCodec
from fanferro.envelopes import AwsNotifications
from tests.aws_samples import (
    AUTO_SCALING_LAUNCHING,
    AUTO_SCALING_TEST,
    ECS_TASK_RUNNING,
    HEALTH_INSTANCE_RETIREMENT,
    STEP_FUNCTIONS_FAILED,
    eventbridge,
)


def received(document: dict[str, Any]) -> fanferro.Payload:
    """What a consumer makes of ``document`` arriving on a queue."""
    payload = AwsNotifications().unwrap(fanferro.WireMessage(json.dumps(document)))
    assert payload is not None
    return payload


def decode[T: aws.AwsDocument](cls: type[T], document: dict[str, Any], message_type: str) -> T:
    payload = received(document)
    assert payload.message_type == message_type
    assert message_type in fanferro.Client(region="us-east-1").types.names_of(cls)
    value = cls.from_json(json.loads(payload.data))
    assert cls.from_json(value.to_json()) == value, "round trip"
    return value


def test_scheduled_events() -> None:
    document = eventbridge(
        "aws.events", "Scheduled Event", {}, resources=["arn:aws:events:eu-north-1:111122223333:rule/nightly"]
    )
    assert decode(aws.ScheduledEvent, document, "aws.events.scheduled_event") == aws.ScheduledEvent()
    assert AwsNotificationCodec().type_name(aws.ScheduledEvent) == "aws.events.scheduled_event"


def test_ecs_task_state_change() -> None:
    task = decode(
        aws.EcsTaskStateChange,
        eventbridge("aws.ecs", "ECS Task State Change", ECS_TASK_RUNNING),
        "aws.ecs.task_state_change",
    )
    assert task.task_arn.endswith("task/example-cluster/a1173316d40a45dea9")
    assert (task.last_status, task.desired_status, task.stopped) == ("RUNNING", "RUNNING", False)
    assert (task.launch_type, task.group, task.version) == ("FARGATE", "family:webserver", 3)
    assert task.started_at == datetime(2025, 5, 6, 11, 2, 34, 325000, tzinfo=UTC)
    [container] = task.containers
    assert (container.name, container.last_status, container.exit_code) == ("web", "RUNNING", None)
    assert container.network_interfaces[0].private_ipv4_address == "10.0.1.163"
    assert task.attachments[0].details[0] == aws.EcsNameValue(name="subnetId", value="subnet-0d0eab1bb38d5ca64")

    stopped = {
        **ECS_TASK_RUNNING,
        "lastStatus": "STOPPED",
        "stopCode": "EssentialContainerExited",
        "stoppedReason": "Essential container in task exited",
        "stoppedAt": "2025-05-06T11:10:00.000Z",
        "containers": [{**ECS_TASK_RUNNING["containers"][0], "lastStatus": "STOPPED", "exitCode": 137}],
    }
    task = aws.EcsTaskStateChange.from_json(stopped)
    assert task.stopped
    assert (task.stop_code, task.containers[0].exit_code) == ("EssentialContainerExited", 137)


def test_step_functions_execution_status_change() -> None:
    execution = decode(
        aws.StepFunctionsExecutionStatusChange,
        eventbridge("aws.states", "Step Functions Execution Status Change", STEP_FUNCTIONS_FAILED),
        "aws.states.step_functions_execution_status_change",
    )
    assert (execution.status, execution.failed, execution.succeeded) == ("FAILED", True, False)
    assert (execution.error, execution.cause) == ("ErrorCode", "An error occurred.")
    assert execution.start_date == datetime(2019, 2, 26, 23, 52, 26, 847000, tzinfo=UTC)
    assert execution.to_json()["startDate"] == 1551225146847
    assert (execution.input_json(), execution.input_included) == ({}, True)
    assert execution.output_json() is None
    succeeded = aws.StepFunctionsExecutionStatusChange.from_json(
        {**STEP_FUNCTIONS_FAILED, "status": "SUCCEEDED", "output": '"Hello World!"', "error": None}
    )
    assert succeeded.succeeded
    assert succeeded.output_json() == "Hello World!"


def test_ec2_events() -> None:
    warning = decode(
        aws.Ec2SpotInstanceInterruptionWarning,
        eventbridge(
            "aws.ec2",
            "EC2 Spot Instance Interruption Warning",
            {"instance-id": "i-1234567890abcdef0", "instance-action": "terminate"},
        ),
        "aws.ec2.spot_instance_interruption_warning",
    )
    assert (warning.instance_id, warning.instance_action) == ("i-1234567890abcdef0", "terminate")
    rebalance = decode(
        aws.Ec2InstanceRebalanceRecommendation,
        eventbridge("aws.ec2", "EC2 Instance Rebalance Recommendation", {"instance-id": "i-1"}),
        "aws.ec2.instance_rebalance_recommendation",
    )
    assert rebalance.instance_id == "i-1"
    state = decode(
        aws.Ec2InstanceStateChange,
        eventbridge("aws.ec2", "EC2 Instance State-change Notification", {"instance-id": "i-1", "state": "stopping"}),
        "aws.ec2.instance_state_change_notification",
    )
    assert state.state == "stopping"


@pytest.mark.parametrize("delivery", ["eventbridge", "direct"])
def test_auto_scaling_lifecycle_actions(delivery: str) -> None:
    detail = {
        key: value
        for key, value in AUTO_SCALING_LAUNCHING.items()
        if key not in ("AccountId", "RequestId", "Service", "Time")
    }
    launching = (
        eventbridge("aws.autoscaling", "EC2 Instance-launch Lifecycle Action", detail)
        if delivery == "eventbridge"
        else AUTO_SCALING_LAUNCHING
    )
    action = decode(aws.AutoScalingLifecycleAction, launching, "aws.autoscaling.ec2_instance_launch_lifecycle_action")
    assert (action.launching, action.terminating) == (True, False)
    assert (action.auto_scaling_group_name, action.ec2_instance_id) == ("my-asg", "i-0598c7d356eba48d7")
    assert action.lifecycle_action_token == "71514b9d-6a40-4b26-8523-05e7ee35fa40"
    assert (action.origin, action.destination) == ("EC2", "AutoScalingGroup")
    assert (action.account_id is not None) == (delivery == "direct")
    terminating = json.loads(json.dumps(launching).replace("EC2_INSTANCE_LAUNCHING", "EC2_INSTANCE_TERMINATING"))
    if delivery == "eventbridge":
        terminating["detail-type"] = "EC2 Instance-terminate Lifecycle Action"
    action = decode(
        aws.AutoScalingLifecycleAction, terminating, "aws.autoscaling.ec2_instance_terminate_lifecycle_action"
    )
    assert action.terminating


def test_auto_scaling_test_notification() -> None:
    test = decode(aws.AutoScalingTestNotification, AUTO_SCALING_TEST, "aws.autoscaling.test_notification")
    assert test.auto_scaling_group_name == "my-asg"
    assert test.time == datetime(2021, 1, 19, 0, 35, 52, 359000, tzinfo=UTC)


def test_health_events() -> None:
    event = decode(
        aws.HealthEvent,
        eventbridge("aws.health", "AWS Health Event", HEALTH_INSTANCE_RETIREMENT),
        "aws.health.event",
    )
    assert (event.service, event.event_type_code) == ("EC2", "AWS_EC2_INSTANCE_RETIREMENT_SCHEDULED")
    assert (event.event_type_category, event.status_code, event.actionability) == (
        "scheduledChange",
        "open",
        "ACTION_REQUIRED",
    )
    assert event.start_time == datetime(2026, 8, 27, 13, 19, 3, tzinfo=UTC)
    assert event.to_json()["startTime"] == "Thu, 27 Aug 2026 13:19:03 GMT"
    assert (event.page, event.total_pages, event.backup_event) == (1, 1, False)
    assert event.description == "A description of the event will be provided here"
    assert event.affected_entities[0].status == "PENDING"
    assert event.event_metadata["truncated"] == "true"
    abuse = received(eventbridge("aws.health", "AWS Health Abuse Event", HEALTH_INSTANCE_RETIREMENT))
    assert abuse.message_type == "aws.health.abuse_event"


async def test_handlers_annotated_with_the_classes_receive_the_events() -> None:
    tasks: list[aws.EcsTaskStateChange] = []
    actions: list[aws.AutoScalingLifecycleAction] = []
    router = fanferro.Router()

    @router.route
    async def on_task(task: aws.EcsTaskStateChange) -> None:
        tasks.append(task)

    @router.route
    async def on_lifecycle(action: aws.AutoScalingLifecycleAction) -> None:
        actions.append(action)

    client = testing.memory_client()
    queue = client.queue("infrastructure")
    async with client:
        for document in (eventbridge("aws.ecs", "ECS Task State Change", ECS_TASK_RUNNING), AUTO_SCALING_LAUNCHING):
            await queue.send(json.dumps(document), envelope=AwsNotifications())
        await client.consumer(queue, router, wait_time=1).run_until_idle(idle_for=0.3, timeout=10)
    assert [task.last_status for task in tasks] == ["RUNNING"]
    assert [action.ec2_instance_id for action in actions] == ["i-0598c7d356eba48d7"]
