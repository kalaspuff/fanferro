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

========================================  ===============================  ==============================
Notification                              Message type                     Class
========================================  ===============================  ==============================
S3 event notification (and test event)    ``aws.s3.event``                 :class:`S3Event`
SES bounce                                ``aws.ses.bounce``               :class:`SesBounceNotification`
SES complaint                             ``aws.ses.complaint``            :class:`SesComplaintNotification`
SES delivery                              ``aws.ses.delivery``             :class:`SesDeliveryNotification`
other SES events (``Open``, ``Send``...)  ``aws.ses.open``, ...            :class:`SesNotification` (common part)
S3 event through EventBridge              ``aws.s3.object_created``, ...   :class:`S3ObjectEvent`
any other EventBridge event               see below                        a class of your own
========================================  ===============================  ==============================

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

from fanferro.aws._document import AwsDocument
from fanferro.aws._eventbridge import EventBridgeMetadata, S3EventBridgeBucket, S3EventBridgeObject, S3ObjectEvent
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
    "AwsDocument",
    "EventBridgeMetadata",
    "S3Bucket",
    "S3Event",
    "S3EventBridgeBucket",
    "S3EventBridgeObject",
    "S3EventRecord",
    "S3Object",
    "S3ObjectEvent",
    "S3RestoreEventData",
    "S3TestEvent",
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
]
