"""What :meth:`Queue.peek <fanferro.Queue.peek>` and :meth:`Queue.redrive <fanferro.Queue.redrive>` return.

* :class:`PeekedMessage` - a message looked at without settling it (typically in a dead-letter
  queue), with why fanferro dead-lettered it.
* :class:`RedriveResult` - what a redrive moved, and the messages it left in the queue
  (:class:`RedriveFailure`).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from fanferro.messages import _HIDDEN_ATTRIBUTES, _INTERNAL_PREFIX

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

    from fanferro._core import AttributeValue
    from fanferro.errors import FanferroError

__all__ = ["PeekedMessage", "RedriveFailure", "RedriveResult"]


@dataclass(frozen=True, slots=True)
class PeekedMessage:
    """A message as :meth:`Queue.peek <fanferro.Queue.peek>` saw it (it stays in the queue).

    The body is the SQS body as received: the encoded message in its envelope, an SNS notification
    when the queue is subscribed without raw delivery, or the claim-check pointer of an offloaded
    payload (which is not fetched).
    """

    sqs_message_id: str
    """The SQS message id (of this copy: a dead-lettered copy has its own)."""
    body: str
    """The SQS body as received."""
    raw_attributes: dict[str, AttributeValue]
    """All message attributes, including fanferro's own (``fanferro.dead-letter``, ``fanferro.attempt``...)."""
    receive_count: int
    """``ApproximateReceiveCount`` (the peek's own receive included)."""
    sent_at: datetime | None
    """When SQS accepted this copy."""
    first_received_at: datetime | None
    """When this copy was first received."""
    group_id: str | None
    """FIFO message group id."""
    deduplication_id: str | None
    """FIFO deduplication id."""
    sequence_number: str | None
    """FIFO sequence number."""
    trace_header: str | None
    """The AWS X-Ray trace header (``AWSTraceHeader``), if any."""
    dead_letter_source_arn: str | None
    """The queue SQS's redrive policy moved the message from (``DeadLetterQueueSourceArn``)."""
    source: str | None
    """The queue a redrive moves the message back to: the ``source`` fanferro recorded when it
    dead-lettered the message (an ARN, or a URL when it did not know the ARN), else
    :attr:`dead_letter_source_arn`; ``None`` when neither is known."""

    @classmethod
    def from_core(cls, data: Mapping[str, Any]) -> PeekedMessage:
        """Converts the core representation."""
        return cls(**data)

    @property
    def attributes(self) -> dict[str, AttributeValue]:
        """Message attributes without fanferro's internal ones."""
        return {
            name: value
            for name, value in self.raw_attributes.items()
            if not name.startswith(_INTERNAL_PREFIX) and name not in _HIDDEN_ATTRIBUTES
        }

    @property
    def dead_letter_info(self) -> dict[str, Any] | None:
        """Why fanferro dead-lettered the message (``None`` when it did not).

        The keys include ``reason``, ``error_type``, ``error_message``, ``source``,
        ``receive_count`` and ``time``. Messages SQS's redrive policy moved have none.
        """
        value = self.raw_attributes.get("fanferro.dead-letter")
        if value is None:
            return None
        try:
            info = json.loads(str(value.value))
        except ValueError:
            return None
        return info if isinstance(info, dict) else None

    @property
    def requeued_attempts(self) -> int:
        """The attempts ``RetryPolicy(strategy="requeue")`` recorded in ``fanferro.attempt``.

        0 when the message was never requeued. A redrive removes the count unless told otherwise.
        """
        value = self.raw_attributes.get("fanferro.attempt")
        if value is None:
            return 0
        try:
            return int(str(value.value))
        except ValueError:
            return 0

    def json(self) -> Any:
        """The body parsed as JSON (raises ``ValueError`` when it is not JSON)."""
        return json.loads(self.body)


@dataclass(frozen=True, slots=True)
class RedriveFailure:
    """A message a redrive could not move or delete."""

    sqs_message_id: str
    """The SQS message id in the dead-letter queue."""
    destination: str | None
    """The queue URL it was to be moved to (``None`` when that is not known)."""
    error: FanferroError
    """Why: a :class:`~fanferro.ValidationError` when no source is known,
    :class:`~fanferro.ResourceNotFoundError` when the source queue does not exist, or the AWS error."""

    @classmethod
    def from_core(cls, data: Mapping[str, Any]) -> RedriveFailure:
        """Converts the core representation."""
        return cls(sqs_message_id=data["sqs_message_id"], destination=data["destination"], error=data["error"])


@dataclass(frozen=True, slots=True)
class RedriveResult:
    """What :meth:`Queue.redrive <fanferro.Queue.redrive>` did."""

    moved: int
    """How many messages were moved."""
    destinations: dict[str, int] = field(default_factory=dict)
    """How many messages were moved to each queue URL."""
    not_moved: list[RedriveFailure] = field(default_factory=list)
    """Messages left in the dead-letter queue (visible again), and why."""
    not_deleted: list[RedriveFailure] = field(default_factory=list)
    """Messages that were moved but could not be deleted from the dead-letter queue: they are in
    both queues now (and counted in :attr:`moved`)."""

    @classmethod
    def from_core(cls, data: Mapping[str, Any]) -> RedriveResult:
        """Converts the core representation."""
        return cls(
            moved=data["moved"],
            destinations=dict(data["destinations"]),
            not_moved=[RedriveFailure.from_core(entry) for entry in data["not_moved"]],
            not_deleted=[RedriveFailure.from_core(entry) for entry in data["not_deleted"]],
        )

    @property
    def ok(self) -> bool:
        """Whether every message received was moved and deleted."""
        return not self.not_moved and not self.not_deleted
