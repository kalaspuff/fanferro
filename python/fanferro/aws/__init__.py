"""Typed bodies of the notifications and events AWS services send to SNS and SQS.

Consumers recognise them by their body (the :class:`~fanferro.envelopes.AwsNotifications`
envelope, one of the defaults) and give them a message type, so a handler annotated with one of
these classes receives them decoded::

    @consumer.route
    async def on_upload(message: fanferro.Message[fanferro.aws.S3Event]) -> None:
        for record in message.body.records:
            print(record.event_name, record.bucket.name, record.object.key, record.object.size)


    @consumer.route
    async def on_bounce(message: fanferro.Message[fanferro.aws.SesBounceNotification]) -> None:
        if message.body.bounce.permanent:
            await suppress(recipient.email_address for recipient in message.body.bounce.bounced_recipients)

Notifications sent to SNS and SQS (message type, class):

* S3 event notification and test event: ``aws.s3.event``, :class:`S3Event`
* SES bounce, complaint, delivery: ``aws.ses.bounce``, ``aws.ses.complaint``, ``aws.ses.delivery``;
  :class:`SesBounceNotification`, :class:`SesComplaintNotification`, :class:`SesDeliveryNotification`
* other SES events (``Open``, ``Send``...): ``aws.ses.open``, ...; :class:`SesNotification`
* Auto Scaling lifecycle hook: ``aws.autoscaling.ec2_instance_launch_lifecycle_action``,
  ``..._terminate_lifecycle_action``; :class:`AutoScalingLifecycleAction` (and
  :class:`AutoScalingTestNotification`)
* CloudWatch alarm: ``aws.cloudwatch.alarm``, :class:`CloudWatchAlarm`
* Lambda destination record: ``aws.lambda.invocation_success``, ``aws.lambda.invocation_failure``;
  :class:`LambdaInvocationRecord`
* event in a Lambda function's dead-letter queue: ``aws.lambda.dead_letter``, the event's own
  class (:class:`LambdaDeadLetter` reads the error)

EventBridge events (their ``detail``):

* S3 events: ``aws.s3.object_created``, ``aws.s3.object_deleted``...; :class:`S3ObjectEvent`
* SES email events: as SES's notifications above
* scheduled rules: ``aws.events.scheduled_event``, :class:`ScheduledEvent`
* ECS task state changes: ``aws.ecs.task_state_change``, :class:`EcsTaskStateChange`
* Step Functions executions: ``aws.states.step_functions_execution_status_change``,
  :class:`StepFunctionsExecutionStatusChange`
* EC2: ``aws.ec2.spot_instance_interruption_warning``, ``aws.ec2.instance_rebalance_recommendation``,
  ``aws.ec2.instance_state_change_notification``; :class:`Ec2SpotInstanceInterruptionWarning`,
  :class:`Ec2InstanceRebalanceRecommendation`, :class:`Ec2InstanceStateChange`
* Auto Scaling lifecycle hooks: as above
* AWS Health: ``aws.health.event``, :class:`HealthEvent`
* CloudWatch alarm state changes: ``aws.cloudwatch.alarm_state_change``,
  :class:`CloudWatchAlarmStateChange`
* Lambda destination records: as above
* any other event: decoded into a class of your own (see below)

SES notifications come in two formats, feedback notifications of an identity (``notificationType``)
and event publishing of a configuration set (``eventType``); both decode into the same classes,
and so do SES's email events sent through EventBridge.

**EventBridge events** (from AWS services, SaaS partners or applications' ``PutEvents``) that a
rule sends to a queue or topic are unwrapped like an envelope: the body is the event's ``detail``,
decoded into the class a handler is annotated with (a pydantic model, a dataclass...), and
``message.id``, ``message.time`` and ``message.source`` are the event's. Their message type is
``aws.<service>.<event>`` for AWS services (``aws.ecs.task_state_change``) and the ``detail-type``
as it is for other sources (``InvoicePaid``); :class:`EventBridgeMetadata` has the account, region
and resources.

The classes are frozen dataclasses with the fields AWS documents, in snake case; fields AWS leaves
out are ``None`` (or empty), and fields it adds later are ignored (``message.json()`` has the whole
document). They encode back into AWS's documents, so tests can publish them (with the
``AwsNotifications`` envelope, as AWS sends them) or pass them to ``fanferro.testing.build_message()``.
"""

from __future__ import annotations

from fanferro.aws._cloudwatch import (
    CloudWatchAlarm,
    CloudWatchAlarmState,
    CloudWatchAlarmStateChange,
    CloudWatchAlarmTrigger,
    CloudWatchDimension,
)
from fanferro.aws._document import AwsDocument
from fanferro.aws._eventbridge import EventBridgeMetadata, S3EventBridgeBucket, S3EventBridgeObject, S3ObjectEvent
from fanferro.aws._events import (
    AutoScalingLifecycleAction,
    AutoScalingTestNotification,
    Ec2InstanceRebalanceRecommendation,
    Ec2InstanceStateChange,
    Ec2SpotInstanceInterruptionWarning,
    EcsAttachment,
    EcsContainer,
    EcsNameValue,
    EcsNetworkInterface,
    EcsTaskStateChange,
    HealthAffectedEntity,
    HealthEvent,
    HealthEventDescription,
    ScheduledEvent,
    StepFunctionsExecutionStatusChange,
)
from fanferro.aws._lambda import LambdaDeadLetter, LambdaInvocationRecord, LambdaRequestContext, LambdaResponseContext
from fanferro.aws._s3 import S3Bucket, S3Event, S3EventRecord, S3Object, S3RestoreEventData, S3TestEvent
from fanferro.aws._ses import (
    SesBounce,
    SesBouncedRecipient,
    SesBounceNotification,
    SesCommonHeaders,
    SesComplainedRecipient,
    SesComplaint,
    SesComplaintNotification,
    SesDelivery,
    SesDeliveryNotification,
    SesHeader,
    SesMail,
    SesNotification,
)

__all__ = [
    "AutoScalingLifecycleAction",
    "AutoScalingTestNotification",
    "AwsDocument",
    "CloudWatchAlarm",
    "CloudWatchAlarmState",
    "CloudWatchAlarmStateChange",
    "CloudWatchAlarmTrigger",
    "CloudWatchDimension",
    "Ec2InstanceRebalanceRecommendation",
    "Ec2InstanceStateChange",
    "Ec2SpotInstanceInterruptionWarning",
    "EcsAttachment",
    "EcsContainer",
    "EcsNameValue",
    "EcsNetworkInterface",
    "EcsTaskStateChange",
    "EventBridgeMetadata",
    "HealthAffectedEntity",
    "HealthEvent",
    "HealthEventDescription",
    "LambdaDeadLetter",
    "LambdaInvocationRecord",
    "LambdaRequestContext",
    "LambdaResponseContext",
    "S3Bucket",
    "S3Event",
    "S3EventBridgeBucket",
    "S3EventBridgeObject",
    "S3EventRecord",
    "S3Object",
    "S3ObjectEvent",
    "S3RestoreEventData",
    "S3TestEvent",
    "ScheduledEvent",
    "SesBounce",
    "SesBounceNotification",
    "SesBouncedRecipient",
    "SesCommonHeaders",
    "SesComplainedRecipient",
    "SesComplaint",
    "SesComplaintNotification",
    "SesDelivery",
    "SesDeliveryNotification",
    "SesHeader",
    "SesMail",
    "SesNotification",
    "StepFunctionsExecutionStatusChange",
]
