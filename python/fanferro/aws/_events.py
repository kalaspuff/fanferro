"""Typed ``detail`` of EventBridge events of AWS services that applications react to.

Scheduled rules, ECS task state changes, Step Functions executions, EC2 instance lifecycle (Spot
interruptions, rebalance recommendations, state changes), Auto Scaling lifecycle hooks (through
EventBridge or sent to SNS and SQS directly) and AWS Health events.
"""

from __future__ import annotations

import json
from collections.abc import Mapping  # noqa: TC003 - get_type_hints() resolves the annotations at runtime
from dataclasses import dataclass, field
from datetime import datetime  # noqa: TC003 - get_type_hints() resolves the annotations at runtime
from typing import Any, ClassVar

from fanferro.aws._document import AwsDocument, _at
from fanferro.message_types import message_type

# --- EventBridge Scheduler and scheduled rules -----------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduledEvent(AwsDocument):
    """A scheduled rule's event (message type ``aws.events.scheduled_event``).

    Its ``detail`` is empty: ``message.time`` is when the rule fired and
    ``aws.EventBridgeMetadata.of(message).resources`` has the rule's ARN.
    """

    aws_message_type: ClassVar[str | None] = "aws.events.scheduled_event"


# --- ECS ---------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class EcsNameValue(AwsDocument):
    """A name and value (attachment details, task attributes)."""

    name: str
    value: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class EcsAttachment(AwsDocument):
    """An attachment of a task (its elastic network interface)."""

    id: str | None = None
    type: str | None = None
    status: str | None = None
    details: tuple[EcsNameValue, ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class EcsNetworkInterface(AwsDocument):
    """A network interface of a container."""

    attachment_id: str | None = None
    private_ipv4_address: str | None = None
    ipv6_address: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class EcsContainer(AwsDocument):
    """A container of a task."""

    name: str
    last_status: str | None = None
    exit_code: int | None = None
    """The container's exit code, once it stopped."""
    reason: str | None = None
    container_arn: str | None = None
    task_arn: str | None = None
    image: str | None = None
    image_digest: str | None = None
    runtime_id: str | None = None
    cpu: str | None = None
    memory: str | None = None
    network_interfaces: tuple[EcsNetworkInterface, ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class EcsTaskStateChange(AwsDocument):
    """An ECS task's state changed (message type ``aws.ecs.task_state_change``)."""

    aws_message_type: ClassVar[str | None] = "aws.ecs.task_state_change"

    task_arn: str
    last_status: str
    """``PROVISIONING``, ``PENDING``, ``ACTIVATING``, ``RUNNING``, ``DEACTIVATING``, ``STOPPING``,
    ``DEPROVISIONING`` or ``STOPPED``."""
    desired_status: str | None = None
    cluster_arn: str | None = None
    task_definition_arn: str | None = None
    group: str | None = None
    """``family:<task definition>`` or ``service:<service name>``."""
    started_by: str | None = None
    launch_type: str | None = None
    capacity_provider_name: str | None = None
    platform_version: str | None = None
    availability_zone: str | None = None
    container_instance_arn: str | None = None
    cpu: str | None = None
    memory: str | None = None
    connectivity: str | None = None
    stop_code: str | None = None
    """Why the task stopped: ``EssentialContainerExited``, ``UserInitiated``, ``TaskFailedToStart``,
    ``ServiceSchedulerInitiated``, ``SpotInterruption``, ``TerminationNotice``..."""
    stopped_reason: str | None = None
    containers: tuple[EcsContainer, ...] = ()
    attachments: tuple[EcsAttachment, ...] = ()
    attributes: tuple[EcsNameValue, ...] = ()
    enable_execute_command: bool | None = None
    version: int | None = None
    """Increases with each change of the task: order events of a task by it."""
    created_at: datetime | None = None
    connectivity_at: datetime | None = None
    pull_started_at: datetime | None = None
    pull_stopped_at: datetime | None = None
    started_at: datetime | None = None
    stopping_at: datetime | None = None
    stopped_at: datetime | None = None
    execution_stopped_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def stopped(self) -> bool:
        """Whether the task stopped (``last_status`` is ``STOPPED``)."""
        return self.last_status == "STOPPED"


# --- Step Functions ------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class StepFunctionsExecutionStatusChange(AwsDocument):
    """A Step Functions execution's status changed (``aws.states.step_functions_execution_status_change``).

    Only standard workflows send these events.
    """

    aws_message_type: ClassVar[str | None] = "aws.states.step_functions_execution_status_change"

    execution_arn: str
    status: str
    """``RUNNING``, ``SUCCEEDED``, ``FAILED``, ``TIMED_OUT``, ``ABORTED`` or ``PENDING_REDRIVE``."""
    state_machine_arn: str | None = None
    name: str | None = None
    start_date: datetime | None = field(default=None, metadata=_at("startDate", time_format="millis"))
    stop_date: datetime | None = field(default=None, metadata=_at("stopDate", time_format="millis"))
    input: str | None = None
    """The execution's input, a JSON document (``None`` when it was too large for the event)."""
    input_included: bool | None = field(default=None, metadata=_at("inputDetails", "included"))
    output: str | None = None
    """The execution's output, a JSON document (``None`` when it was too large for the event)."""
    output_included: bool | None = field(default=None, metadata=_at("outputDetails", "included"))
    error: str | None = None
    cause: str | None = None
    state_machine_version_arn: str | None = None
    state_machine_alias_arn: str | None = None
    redrive_count: int | None = None
    redrive_date: datetime | None = None
    redrive_status: str | None = None
    redrive_status_reason: str | None = None

    @property
    def succeeded(self) -> bool:
        """Whether the execution succeeded."""
        return self.status == "SUCCEEDED"

    @property
    def failed(self) -> bool:
        """Whether the execution failed, timed out or was aborted."""
        return self.status in ("FAILED", "TIMED_OUT", "ABORTED")

    def input_json(self) -> Any:
        """The input, parsed (``None`` when the event does not include it)."""
        return json.loads(self.input) if self.input is not None else None

    def output_json(self) -> Any:
        """The output, parsed (``None`` when the event does not include it)."""
        return json.loads(self.output) if self.output is not None else None


# --- EC2 ---------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class Ec2SpotInstanceInterruptionWarning(AwsDocument):
    """EC2 interrupts a Spot instance in two minutes (``aws.ec2.spot_instance_interruption_warning``)."""

    aws_message_type: ClassVar[str | None] = "aws.ec2.spot_instance_interruption_warning"

    instance_id: str = field(metadata=_at("instance-id"))
    instance_action: str | None = field(default=None, metadata=_at("instance-action"))
    """``terminate``, ``stop`` or ``hibernate``."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Ec2InstanceRebalanceRecommendation(AwsDocument):
    """A Spot instance is at elevated risk of interruption (``aws.ec2.instance_rebalance_recommendation``)."""

    aws_message_type: ClassVar[str | None] = "aws.ec2.instance_rebalance_recommendation"

    instance_id: str = field(metadata=_at("instance-id"))


@dataclass(frozen=True, slots=True, kw_only=True)
class Ec2InstanceStateChange(AwsDocument):
    """An EC2 instance's state changed (``aws.ec2.instance_state_change_notification``)."""

    aws_message_type: ClassVar[str | None] = "aws.ec2.instance_state_change_notification"

    instance_id: str = field(metadata=_at("instance-id"))
    state: str
    """``pending``, ``running``, ``stopping``, ``stopped``, ``shutting-down`` or ``terminated``."""


# --- Auto Scaling -------------------------------------------------------------------------------------


@message_type(
    "aws.autoscaling.ec2_instance_launch_lifecycle_action", "aws.autoscaling.ec2_instance_terminate_lifecycle_action"
)
@dataclass(frozen=True, slots=True, kw_only=True)
class AutoScalingLifecycleAction(AwsDocument):
    """An instance waits in an Auto Scaling lifecycle hook until the action is completed.

    Message types ``aws.autoscaling.ec2_instance_launch_lifecycle_action`` and
    ``aws.autoscaling.ec2_instance_terminate_lifecycle_action``, whether the hook notifies
    EventBridge or an SNS topic or SQS queue directly (``account_id``, ``request_id`` and ``time``
    are only in direct notifications). Complete the action with ``CompleteLifecycleAction`` and
    :attr:`lifecycle_action_token`.
    """

    aws_message_type: ClassVar[str | None] = "aws.autoscaling.ec2_instance_launch_lifecycle_action"

    lifecycle_transition: str = field(metadata=_at("LifecycleTransition"))
    """``autoscaling:EC2_INSTANCE_LAUNCHING`` or ``autoscaling:EC2_INSTANCE_TERMINATING``."""
    auto_scaling_group_name: str = field(metadata=_at("AutoScalingGroupName"))
    ec2_instance_id: str = field(metadata=_at("EC2InstanceId"))
    lifecycle_hook_name: str | None = field(default=None, metadata=_at("LifecycleHookName"))
    lifecycle_action_token: str | None = field(default=None, metadata=_at("LifecycleActionToken"))
    notification_metadata: str | None = field(default=None, metadata=_at("NotificationMetadata"))
    origin: str | None = field(default=None, metadata=_at("Origin"))
    """``EC2``, ``AutoScalingGroup`` or ``WarmPool``."""
    destination: str | None = field(default=None, metadata=_at("Destination"))
    """``EC2``, ``AutoScalingGroup`` or ``WarmPool``."""
    account_id: str | None = field(default=None, metadata=_at("AccountId"))
    request_id: str | None = field(default=None, metadata=_at("RequestId"))
    time: datetime | None = field(default=None, metadata=_at("Time"))
    service: str | None = field(default=None, metadata=_at("Service"))

    @property
    def launching(self) -> bool:
        """Whether the instance is launching."""
        return self.lifecycle_transition == "autoscaling:EC2_INSTANCE_LAUNCHING"

    @property
    def terminating(self) -> bool:
        """Whether the instance is terminating."""
        return self.lifecycle_transition == "autoscaling:EC2_INSTANCE_TERMINATING"

    def __fanferro_message_type__(self) -> str:
        """The message type of this action: of a launching or a terminating instance."""
        if self.terminating:
            return "aws.autoscaling.ec2_instance_terminate_lifecycle_action"
        return "aws.autoscaling.ec2_instance_launch_lifecycle_action"


@dataclass(frozen=True, slots=True, kw_only=True)
class AutoScalingTestNotification(AwsDocument):
    """The message a lifecycle hook sends its SNS topic or SQS queue when it is added.

    Message type ``aws.autoscaling.test_notification``: give it a route (or a default route) that
    acknowledges it, or it is dead-lettered as a message no route matched.
    """

    aws_message_type: ClassVar[str | None] = "aws.autoscaling.test_notification"

    auto_scaling_group_name: str = field(metadata=_at("AutoScalingGroupName"))
    auto_scaling_group_arn: str | None = field(default=None, metadata=_at("AutoScalingGroupARN"))
    account_id: str | None = field(default=None, metadata=_at("AccountId"))
    request_id: str | None = field(default=None, metadata=_at("RequestId"))
    time: datetime | None = field(default=None, metadata=_at("Time"))
    event: str = field(default="autoscaling:TEST_NOTIFICATION", metadata=_at("Event"))
    service: str = field(default="AWS Auto Scaling", metadata=_at("Service"))


# --- AWS Health ----------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class HealthEventDescription(AwsDocument):
    """A description of an AWS Health event, in a language."""

    language: str | None = None
    latest_description: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class HealthAffectedEntity(AwsDocument):
    """A resource an AWS Health event affects."""

    entity_value: str
    """The resource's id or ARN."""
    status: str | None = None
    """``IMPAIRED``, ``UNIMPAIRED``, ``PENDING``, ``RESOLVED`` or ``UNKNOWN``."""
    last_updated_time: datetime | None = field(default=None, metadata=_at("lastUpdatedTime", time_format="rfc2822"))
    tags: Mapping[str, str] = field(default_factory=dict)


@message_type("aws.health.event", "aws.health.abuse_event")
@dataclass(frozen=True, slots=True, kw_only=True)
class HealthEvent(AwsDocument):
    """An AWS Health event: an issue, a scheduled change or an account notification.

    Message types ``aws.health.event`` and ``aws.health.abuse_event``. A long list of affected
    entities comes in pages, each its own event with the same :attr:`communication_id` prefix.
    """

    aws_message_type: ClassVar[str | None] = "aws.health.event"

    event_arn: str
    service: str
    """The affected service, e.g. ``EC2`` or ``LAMBDA``."""
    event_type_code: str
    """E.g. ``AWS_EC2_INSTANCE_RETIREMENT_SCHEDULED`` or ``AWS_LAMBDA_PLANNED_LIFECYCLE_EVENT``."""
    event_type_category: str | None = None
    """``issue``, ``accountNotification``, ``investigation`` or ``scheduledChange``."""
    event_scope_code: str | None = None
    """``ACCOUNT_SPECIFIC`` or ``PUBLIC``."""
    status_code: str | None = None
    """``open``, ``closed`` or ``upcoming``."""
    event_region: str | None = None
    """The affected region (the envelope's region is where the event was delivered)."""
    start_time: datetime | None = field(default=None, metadata=_at("startTime", time_format="rfc2822"))
    end_time: datetime | None = field(default=None, metadata=_at("endTime", time_format="rfc2822"))
    last_updated_time: datetime | None = field(default=None, metadata=_at("lastUpdatedTime", time_format="rfc2822"))
    communication_id: str | None = None
    event_description: tuple[HealthEventDescription, ...] = ()
    event_metadata: Mapping[str, str] = field(default_factory=dict)
    affected_entities: tuple[HealthAffectedEntity, ...] = ()
    affected_account: str | None = None
    actionability: str | None = None
    """``ACTION_REQUIRED``, ``ACTION_MAY_BE_REQUIRED`` or ``INFORMATIONAL``."""
    personas: tuple[str, ...] = ()
    page: int | None = None
    total_pages: int | None = None
    backup_event: bool | None = None

    @property
    def description(self) -> str | None:
        """The latest description (of the first language)."""
        return self.event_description[0].latest_description if self.event_description else None
