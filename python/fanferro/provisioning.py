"""Declarative resource specs and provisioning policies.

Every queue, topic and subscription handle carries a desired state (attributes, tags, dead-letter
queue, filter policy...) and a :class:`Provisioning` policy deciding what fanferro may do:

=================================  ======================================================
Policy                             Behaviour
=================================  ======================================================
``Provisioning.existing()``        look up, verify attributes, report drift (default)
``Provisioning.create_missing()``  look up; create when missing; report drift
``Provisioning.ensure()``          look up; create when missing; update drifted attributes
``Provisioning.assume()``          no API calls; derive URL/ARN from the name
=================================  ======================================================

and how failures are handled: ``on_missing``, ``on_drift`` and ``on_error`` each take a
:data:`FailureMode` - ``"raise"`` (fail loudly), ``"log"`` (emit an event and continue),
``"ignore"``, or :class:`Fallback` (a function that supplies a replacement resource).

Provisioning order for a queue: dead-letter queue first (so the redrive policy can reference it),
then lookup (``GetQueueUrl``), verification (``GetQueueAttributes``), update or creation.
"""

from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Literal

from fanferro._utils import Duration, seconds

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

__all__ = [
    "DeadLetterQueue",
    "Drift",
    "FailureMode",
    "Fallback",
    "Kms",
    "Provisioning",
    "ProvisioningFailure",
    "ProvisioningReport",
    "QueueAttributes",
    "QueueInfo",
    "SqsManagedEncryption",
    "SubscriptionInfo",
    "TopicAttributes",
    "TopicInfo",
]

type ResourceKind = Literal["queue", "topic", "subscription", "bucket"]
type ProvisioningStatus = Literal["found", "created", "updated", "assumed", "missing", "failed"]


@dataclass(frozen=True, slots=True)
class Drift:
    """An attribute (or tag, ``tag:<key>``) whose actual value differs from the spec."""

    attribute: str
    expected: str
    actual: str | None
    mutable: bool


@dataclass(frozen=True, slots=True)
class QueueInfo:
    """A resolved queue."""

    name: str
    url: str
    arn: str
    fifo: bool
    attributes: Mapping[str, str] = field(default_factory=dict)

    @property
    def visibility_timeout(self) -> int | None:
        """The queue's visibility timeout in seconds, when known."""
        value = self.attributes.get("VisibilityTimeout")
        return int(value) if value is not None and value.isdigit() else None

    @property
    def content_based_deduplication(self) -> bool:
        """Whether content-based deduplication is enabled (FIFO)."""
        return self.attributes.get("ContentBasedDeduplication", "").lower() == "true"

    @property
    def max_message_size(self) -> int | None:
        """``MaximumMessageSize`` when known."""
        value = self.attributes.get("MaximumMessageSize")
        return int(value) if value is not None and value.isdigit() else None

    @property
    def redrive_policy(self) -> dict[str, Any] | None:
        """The parsed ``RedrivePolicy`` (dead-letter target and max receive count)."""
        value = self.attributes.get("RedrivePolicy")
        if not value:
            return None
        try:
            policy = json.loads(value)
        except ValueError:
            return None
        return policy if isinstance(policy, dict) else None


@dataclass(frozen=True, slots=True)
class TopicInfo:
    """A resolved topic."""

    name: str
    arn: str
    fifo: bool
    attributes: Mapping[str, str] = field(default_factory=dict)

    @property
    def content_based_deduplication(self) -> bool:
        """Whether content-based deduplication is enabled (FIFO)."""
        return self.attributes.get("ContentBasedDeduplication", "").lower() == "true"

    @property
    def max_message_size(self) -> int | None:
        """``MaximumMessageSize`` when configured."""
        value = self.attributes.get("MaximumMessageSize")
        return int(value) if value is not None and value.isdigit() else None


@dataclass(frozen=True, slots=True)
class SubscriptionInfo:
    """A resolved subscription."""

    arn: str
    topic_arn: str
    queue_arn: str
    attributes: Mapping[str, str] = field(default_factory=dict)

    @property
    def raw_message_delivery(self) -> bool:
        """Whether raw message delivery is enabled."""
        return self.attributes.get("RawMessageDelivery", "").lower() == "true"


@dataclass(frozen=True, slots=True)
class ProvisioningReport:
    """What provisioning found or did for one resource."""

    kind: ResourceKind
    name: str
    status: ProvisioningStatus
    resource: QueueInfo | TopicInfo | SubscriptionInfo | str | None
    drift: tuple[Drift, ...] = ()
    actions: tuple[str, ...] = ()
    error: BaseException | None = None
    dead_letter: ProvisioningReport | None = None

    @classmethod
    def from_core(cls, data: Mapping[str, Any]) -> ProvisioningReport:
        """Converts the core report."""
        kind = data["kind"]
        raw = data.get("resource")
        resource: QueueInfo | TopicInfo | SubscriptionInfo | str | None
        if raw is None:
            resource = None
        elif kind == "queue":
            resource = QueueInfo(**raw)
        elif kind == "topic":
            resource = TopicInfo(**raw)
        elif kind == "subscription":
            resource = SubscriptionInfo(**raw)
        else:
            resource = str(raw)
        dead_letter = data.get("dead_letter")
        return cls(
            kind=kind,
            name=data["name"],
            status=data["status"],
            resource=resource,
            drift=tuple(Drift(**item) for item in data.get("drift", ())),
            actions=tuple(data.get("actions", ())),
            error=data.get("error"),
            dead_letter=cls.from_core(dead_letter) if dead_letter else None,
        )

    @property
    def ok(self) -> bool:
        """Whether the resource is usable and matches the spec."""
        return self.resource is not None and self.status != "failed" and not self.drift


@dataclass(frozen=True, slots=True)
class ProvisioningFailure:
    """Passed to :class:`Fallback` handlers."""

    reason: Literal["missing", "drift", "error"]
    """Which failure mode triggered."""
    report: ProvisioningReport
    """The provisioning outcome."""


type FallbackResult = QueueInfo | TopicInfo | SubscriptionInfo | None


@dataclass(frozen=True, slots=True)
class Fallback:
    """Failure mode that calls ``handler`` to supply a replacement resource.

    The handler may return a :class:`QueueInfo`/:class:`TopicInfo`/:class:`SubscriptionInfo` to
    use instead (e.g. a shared fallback queue), or ``None`` to continue without the resource.
    It may be sync or async, and may raise to fail loudly.
    """

    handler: Callable[[ProvisioningFailure], FallbackResult | Awaitable[FallbackResult]]


type FailureMode = Literal["raise", "log", "ignore"] | Fallback
"""How a provisioning failure is handled."""


@dataclass(frozen=True, slots=True)
class Provisioning:
    """What provisioning may do for a resource and how failures are handled.

    Attributes:
        lookup: Look the resource up. ``False`` derives the URL/ARN from the name without any API
            call (least-privilege deployments; requires the account id for topics and queues).
        verify: Read attributes and tags and report drift.
        create: Create the resource when it does not exist.
        update: Update drifted attributes/tags in place (immutable ones are reported).
        on_missing: What to do when the resource does not exist and ``create`` is off.
        on_drift: What to do when attributes differ and ``update`` is off.
        on_error: What to do when an AWS call fails (permissions, throttling, ...).
    """

    lookup: bool = True
    verify: bool = True
    create: bool = False
    update: bool = False
    on_missing: FailureMode = "raise"
    on_drift: FailureMode = "log"
    on_error: FailureMode = "raise"

    @classmethod
    def existing(cls, **overrides: Any) -> Provisioning:
        """The resource must exist; drift is logged (the default)."""
        return cls(**overrides)

    @classmethod
    def create_missing(cls, **overrides: Any) -> Provisioning:
        """Create the resource when missing; drift is logged."""
        return dataclasses.replace(cls(create=True), **overrides)

    @classmethod
    def ensure(cls, **overrides: Any) -> Provisioning:
        """Create when missing and update drifted attributes (infrastructure owned by the app)."""
        return dataclasses.replace(cls(create=True, update=True), **overrides)

    @classmethod
    def assume(cls, **overrides: Any) -> Provisioning:
        """No API calls: derive the URL/ARN from the name (requires an explicit or STS account id)."""
        return dataclasses.replace(cls(lookup=False, verify=False), **overrides)

    def to_core(self) -> dict[str, bool]:
        """The core representation."""
        return {"lookup": self.lookup, "verify": self.verify, "create": self.create, "update": self.update}


@dataclass(frozen=True, slots=True)
class Kms:
    """Server-side encryption with a KMS key (SQS, SNS and large payload objects).

    Attributes:
        key_id: Key id, ARN or alias (``alias/aws/sqs`` / ``alias/aws/sns`` for AWS managed keys).
        data_key_reuse: How long SQS reuses a data key (SQS only; 1 minute to 24 hours).
    """

    key_id: str
    data_key_reuse: Duration | None = None


@dataclass(frozen=True, slots=True)
class SqsManagedEncryption:
    """SQS-managed server-side encryption (SSE-SQS); ``enabled=False`` turns encryption off."""

    enabled: bool = True


def _bool(value: bool) -> str:
    return "true" if value else "false"


def _seconds_attribute(value: Duration, name: str) -> str:
    return str(round(seconds(value, name)))


@dataclass(frozen=True, slots=True)
class QueueAttributes:
    """Typed SQS queue attributes (``None`` means "not managed").

    Attributes:
        visibility_timeout: Default visibility timeout (0-12 hours).
        message_retention: How long messages are kept (1 minute - 14 days).
        delay: Default delivery delay (0-15 minutes).
        receive_wait_time: Default long polling wait (0-20 seconds).
        max_message_size: Maximum message size in bytes (1 KiB - 1 MiB).
        encryption: :class:`Kms` or :class:`SqsManagedEncryption`.
        content_based_deduplication: FIFO: deduplicate on a SHA-256 of the body.
        deduplication_scope: FIFO: ``"queue"`` or ``"messageGroup"``.
        fifo_throughput_limit: FIFO: ``"perQueue"`` or ``"perMessageGroupId"`` (high throughput).
        policy: Access policy document.
        redrive_allow_policy: Which source queues may use this queue as a dead-letter queue.
        extra: Raw attributes for anything not modelled here (passed through verbatim).
    """

    visibility_timeout: Duration | None = None
    message_retention: Duration | None = None
    delay: Duration | None = None
    receive_wait_time: Duration | None = None
    max_message_size: int | None = None
    encryption: Kms | SqsManagedEncryption | None = None
    content_based_deduplication: bool | None = None
    deduplication_scope: Literal["queue", "messageGroup"] | None = None
    fifo_throughput_limit: Literal["perQueue", "perMessageGroupId"] | None = None
    policy: Mapping[str, Any] | None = None
    redrive_allow_policy: Mapping[str, Any] | None = None
    extra: Mapping[str, str] = field(default_factory=dict)

    def to_aws(self) -> dict[str, str]:
        """AWS attribute names and values."""
        out: dict[str, str] = {}
        if self.visibility_timeout is not None:
            out["VisibilityTimeout"] = _seconds_attribute(self.visibility_timeout, "visibility_timeout")
        if self.message_retention is not None:
            out["MessageRetentionPeriod"] = _seconds_attribute(self.message_retention, "message_retention")
        if self.delay is not None:
            out["DelaySeconds"] = _seconds_attribute(self.delay, "delay")
        if self.receive_wait_time is not None:
            out["ReceiveMessageWaitTimeSeconds"] = _seconds_attribute(self.receive_wait_time, "receive_wait_time")
        if self.max_message_size is not None:
            out["MaximumMessageSize"] = str(self.max_message_size)
        if isinstance(self.encryption, Kms):
            out["KmsMasterKeyId"] = self.encryption.key_id
            if self.encryption.data_key_reuse is not None:
                out["KmsDataKeyReusePeriodSeconds"] = _seconds_attribute(
                    self.encryption.data_key_reuse, "data_key_reuse"
                )
        elif isinstance(self.encryption, SqsManagedEncryption):
            out["SqsManagedSseEnabled"] = _bool(self.encryption.enabled)
        if self.content_based_deduplication is not None:
            out["ContentBasedDeduplication"] = _bool(self.content_based_deduplication)
        if self.deduplication_scope is not None:
            out["DeduplicationScope"] = self.deduplication_scope
        if self.fifo_throughput_limit is not None:
            out["FifoThroughputLimit"] = self.fifo_throughput_limit
        if self.policy is not None:
            out["Policy"] = json.dumps(self.policy, separators=(",", ":"))
        if self.redrive_allow_policy is not None:
            out["RedriveAllowPolicy"] = json.dumps(self.redrive_allow_policy, separators=(",", ":"))
        out.update(self.extra)
        return out


@dataclass(frozen=True, slots=True)
class TopicAttributes:
    """Typed SNS topic attributes (``None`` means "not managed").

    Attributes:
        display_name: Display name (SMS/email sender name).
        encryption: :class:`Kms` (e.g. ``Kms("alias/aws/sns")``).
        content_based_deduplication: FIFO: deduplicate on a SHA-256 of the message.
        tracing: AWS X-Ray active tracing (``"Active"``) or ``"PassThrough"``.
        signature_version: ``1`` (SHA1) or ``2`` (SHA256) for notification signatures.
        max_message_size: Maximum message size in bytes (up to 1 MiB where supported).
        fifo_throughput_scope: FIFO: ``"Topic"`` or ``"MessageGroup"``.
        policy: Access policy document.
        delivery_policy: Delivery retry policy document.
        archive_policy: FIFO message archive policy.
        extra: Raw attributes passed through verbatim.
    """

    display_name: str | None = None
    encryption: Kms | None = None
    content_based_deduplication: bool | None = None
    tracing: Literal["Active", "PassThrough"] | None = None
    signature_version: Literal[1, 2] | None = None
    max_message_size: int | None = None
    fifo_throughput_scope: Literal["Topic", "MessageGroup"] | None = None
    policy: Mapping[str, Any] | None = None
    delivery_policy: Mapping[str, Any] | None = None
    archive_policy: Mapping[str, Any] | None = None
    extra: Mapping[str, str] = field(default_factory=dict)

    def to_aws(self) -> dict[str, str]:
        """AWS attribute names and values."""
        out: dict[str, str] = {}
        if self.display_name is not None:
            out["DisplayName"] = self.display_name
        if self.encryption is not None:
            out["KmsMasterKeyId"] = self.encryption.key_id
        if self.content_based_deduplication is not None:
            out["ContentBasedDeduplication"] = _bool(self.content_based_deduplication)
        if self.tracing is not None:
            out["TracingConfig"] = self.tracing
        if self.signature_version is not None:
            out["SignatureVersion"] = str(self.signature_version)
        if self.max_message_size is not None:
            out["MaximumMessageSize"] = str(self.max_message_size)
        if self.fifo_throughput_scope is not None:
            out["FifoThroughputScope"] = self.fifo_throughput_scope
        if self.policy is not None:
            out["Policy"] = json.dumps(self.policy, separators=(",", ":"))
        if self.delivery_policy is not None:
            out["DeliveryPolicy"] = json.dumps(self.delivery_policy, separators=(",", ":"))
        if self.archive_policy is not None:
            out["ArchivePolicy"] = json.dumps(self.archive_policy, separators=(",", ":"))
        out.update(self.extra)
        return out


@dataclass(frozen=True, slots=True)
class DeadLetterQueue:
    """The dead-letter queue of a queue (provisioned first and wired up with a ``RedrivePolicy``).

    Attributes:
        name: Dead-letter queue name (FIFO queues need a FIFO dead-letter queue).
        max_receive_count: Receives after which SQS moves a message to the dead-letter queue.
        attributes: Attributes of the dead-letter queue (e.g. a long ``message_retention``).
        tags: Tags of the dead-letter queue.
        provisioning: Policy for the dead-letter queue (defaults to the queue's policy).
    """

    name: str
    max_receive_count: int = 5
    attributes: QueueAttributes | Mapping[str, str] | None = field(
        default_factory=lambda: QueueAttributes(message_retention=timedelta(days=14))
    )
    tags: Mapping[str, str] = field(default_factory=dict)
    provisioning: Provisioning | None = None

    def __post_init__(self) -> None:
        if not 1 <= self.max_receive_count <= 1000:
            raise ValueError("max_receive_count must be between 1 and 1000")


def attributes_to_aws(attributes: QueueAttributes | TopicAttributes | Mapping[str, str] | None) -> dict[str, str]:
    """Normalizes typed or raw attributes to AWS attribute names."""
    if attributes is None:
        return {}
    if isinstance(attributes, QueueAttributes | TopicAttributes):
        return attributes.to_aws()
    return {str(key): str(value) for key, value in attributes.items()}
