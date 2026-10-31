"""Message types: incoming, outgoing, publish results and prepared (outbox) messages.

* :class:`Message` - a received message with its decoded body.
* :class:`OutgoingMessage` - a message to publish with every option.
* :class:`PublishResult` / :class:`BatchPublishResult` - publish outcomes.
* :class:`PreparedMessage` - a fully encoded message, e.g. for a transactional outbox.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, Protocol

from fanferro._core import AttributeValue, Delivery, Payload, WireMessage, is_sqs_safe_text, new_message_id
from fanferro._utils import Duration, optional_seconds, seconds
from fanferro.codecs._base import loads_json
from fanferro.errors import DecodeError

if TYPE_CHECKING:
    from datetime import datetime

    from fanferro.codecs import Codec
    from fanferro.envelopes import Envelope

__all__ = [
    "AttributeInput",
    "BatchFailure",
    "BatchPublishResult",
    "HeaderValue",
    "Message",
    "Outcome",
    "OutgoingMessage",
    "PayloadPointer",
    "PreparedMessage",
    "PublishResult",
    "Settler",
    "attribute_python_value",
    "to_attribute_value",
]

type HeaderValue = str | int | bool
"""Envelope header (CloudEvents extension) value."""

type AttributeInput = AttributeValue | str | int | float | Decimal | bool | bytes
"""Accepted message attribute values: ``str`` -> String, numbers -> Number, ``bytes`` -> Binary."""

type Outcome = Literal["acked", "retried", "requeued", "dead_lettered", "released", "discarded"]
"""How a message was settled."""

_INTERNAL_PREFIX = "fanferro."
_HIDDEN_ATTRIBUTES = frozenset({"ExtendedPayloadSize", "SQSLargePayloadSize"})


def to_attribute_value(value: AttributeInput) -> AttributeValue:
    """Converts a Python value to an :class:`AttributeValue`."""
    if isinstance(value, AttributeValue):
        return value
    if isinstance(value, bool):
        return AttributeValue.string("true" if value else "false")
    if isinstance(value, str):
        return AttributeValue.string(value)
    if isinstance(value, int | float | Decimal):
        return AttributeValue.number(value)
    if isinstance(value, bytes | bytearray | memoryview):
        return AttributeValue.binary(bytes(value))
    raise TypeError(f"unsupported attribute value type {type(value).__name__}")


def attribute_python_value(value: AttributeValue) -> str | int | float | Decimal | bytes:
    """The natural Python value of an attribute (``Number`` -> ``int``/``Decimal``)."""
    raw = value.value
    if value.base_type == "Number" and isinstance(raw, str):
        try:
            return int(raw)
        except ValueError:
            number = Decimal(raw)
            return float(number) if "float" in value.data_type.lower() else number
    return raw


@dataclass(frozen=True, slots=True)
class PayloadPointer:
    """Where an offloaded (claim-check) payload is stored."""

    bucket: str | None
    key: str | None
    url: str | None
    size: int | None
    sha256: str | None
    format: Literal["fanferro", "aws_extended"]

    @classmethod
    def from_core(cls, data: Mapping[str, Any] | None) -> PayloadPointer | None:
        """Converts the core representation."""
        if not data:
            return None
        return cls(
            bucket=data.get("bucket"),
            key=data.get("key"),
            url=data.get("url"),
            size=data.get("size"),
            sha256=data.get("sha256"),
            format=data.get("format", "fanferro"),
        )


class Settler(Protocol):
    """Settles messages on behalf of :class:`Message` (implemented by the consumer and by test doubles)."""

    async def ack(self, message: Message[Any]) -> None:
        """Deletes the message (processing succeeded)."""
        ...

    async def retry(self, message: Message[Any], delay: float | None, error: BaseException | None) -> None:
        """Schedules a retry (respecting the retry policy unless ``delay`` is given)."""
        ...

    async def requeue(self, message: Message[Any], delay: float | None, error: BaseException | None) -> None:
        """Re-enqueues a copy with an incremented attempt counter and deletes the original."""
        ...

    async def dead_letter(self, message: Message[Any], reason: str, error: BaseException | None) -> None:
        """Forwards the message to the dead-letter queue."""
        ...

    async def release(self, message: Message[Any]) -> None:
        """Makes the message visible again immediately."""
        ...

    async def discard(self, message: Message[Any], reason: str) -> None:
        """Deletes the message without processing it."""
        ...

    async def extend(self, message: Message[Any], timeout: float) -> None:
        """Sets the visibility timeout to ``timeout`` seconds from now."""
        ...


_UNSET: Any = object()


class Message[T]:
    """A received message.

    ``message.body`` is the decoded application object (its type is chosen by the handler's
    annotation, e.g. ``Message[OrderCreated]``). Everything else describes the message: envelope
    metadata (``id``, ``type``, ``time``, ``headers``...), transport metadata (``receive_count``,
    ``group_id``, ``sent_at``...) and user attributes.

    Handlers normally just return (the message is acked) or raise (the retry policy applies).
    The settlement methods (:meth:`ack`, :meth:`retry`, :meth:`dead_letter`, ...) settle a
    message explicitly; once settled, the consumer does nothing further with it.
    """

    __slots__ = (
        "_body",
        "_data",
        "_handed_back",
        "_json",
        "_settled",
        "_settlement_failed",
        "_settler",
        "_slot",
        "attributes",
        "delivery",
        "envelope",
        "outcome",
        "payload",
        "queue",
        "raw_attributes",
    )

    def __init__(
        self,
        *,
        payload: Payload,
        attributes: Mapping[str, AttributeInput] | None = None,
        envelope: str = "raw",
        delivery: Delivery | None = None,
        queue: str | None = None,
        settler: Settler | None = None,
        body: T = _UNSET,
    ) -> None:
        raw = {name: to_attribute_value(value) for name, value in (attributes or {}).items()}
        self.payload: Payload = payload
        """The encoded payload and its metadata (as produced by the envelope)."""
        self.raw_attributes: dict[str, AttributeValue] = raw
        """All message attributes, including fanferro's own."""
        self.attributes: dict[str, AttributeValue] = {
            name: value
            for name, value in raw.items()
            if not name.startswith(_INTERNAL_PREFIX) and name not in _HIDDEN_ATTRIBUTES
        }
        """Message attributes without fanferro's internal ones."""
        self.envelope: str = envelope
        """Name of the envelope the message arrived in."""
        self.delivery: Delivery | None = delivery
        """The low-level delivery (``None`` for messages built in tests)."""
        self.queue: str | None = queue
        """Name of the queue the message was received from."""
        self.outcome: Outcome | None = None
        """How the message was settled (``None`` while unsettled)."""
        self._settler = settler
        self._settled = False
        self._settlement_failed = False  # a settlement call raised (see the consumer's _Settler)
        self._slot: Any = None  # the consumer's handler slot while the handler runs
        self._body: Any = body
        self._data: tuple[Payload, bytes] | None = None  # (payload, its data): read once per payload
        self._json: Any = _UNSET
        self._handed_back = 0  # receives that were hand-backs, not attempts (see the consumer)

    @classmethod
    def _received(
        cls,
        payload: Payload,
        raw_attributes: dict[str, AttributeValue],
        attributes: dict[str, AttributeValue],
        envelope: str,
        delivery: Delivery,
        queue: str,
        settler: Settler,
    ) -> Message[Any]:
        """A received message from parts the consumer prepared (no conversions, no copies)."""
        message = cls.__new__(cls)
        message.payload = payload
        message.raw_attributes = raw_attributes
        message.attributes = attributes
        message.envelope = envelope
        message.delivery = delivery
        message.queue = queue
        message.outcome = None
        message._settler = settler
        message._settled = False
        message._settlement_failed = False
        message._slot = None
        message._body = _UNSET
        message._data = None
        message._json = _UNSET
        message._handed_back = 0
        return message

    # -- payload ---------------------------------------------------------------------------

    @property
    def body(self) -> T:
        """The decoded body.

        Raises:
            RuntimeError: When accessed before routing decoded it (in router matchers use
                :meth:`json`, :attr:`data` or :attr:`attributes` instead).
        """
        if self._body is _UNSET:
            raise RuntimeError("message.body is decoded after routing; use message.json() or message.data in matchers")
        return self._body

    def _set_body(self, body: Any) -> None:
        self._body = body

    @property
    def data(self) -> bytes:
        """The encoded body bytes."""
        cached = self._data
        payload = self.payload
        if cached is None or cached[0] is not payload:
            cached = self._data = (payload, payload.data)
        return cached[1]

    def json(self) -> Any:
        """Parses the body as JSON (cached).

        Raises:
            DecodeError: If the body is not valid JSON.
        """
        if self._json is _UNSET:
            try:
                self._json = loads_json(self.data)
            except ValueError as error:
                raise DecodeError(f"message body is not valid JSON: {error}") from error
        return self._json

    @property
    def id(self) -> str:
        """The message id (envelope id, or the SQS message id for envelope-less messages)."""
        if self.payload.id:
            return self.payload.id
        if self.delivery is not None:
            return self.delivery.sqs_message_id
        return ""

    @property
    def type(self) -> str | None:
        """Logical message type (e.g. ``orders.created.v1``)."""
        return self.payload.message_type

    @property
    def content_type(self) -> str | None:
        """Media type of the body."""
        return self.payload.content_type

    @property
    def time(self) -> datetime | None:
        """When the message was created (falls back to the SQS sent timestamp)."""
        if self.payload.time is not None:
            return self.payload.time
        return self.delivery.sent_at if self.delivery is not None else None

    @property
    def source(self) -> str | None:
        """The producer (CloudEvents ``source``)."""
        return self.payload.source

    @property
    def subject(self) -> str | None:
        """The subject (CloudEvents ``subject``)."""
        return self.payload.subject

    @property
    def headers(self) -> dict[str, HeaderValue]:
        """Envelope headers (CloudEvents extension attributes)."""
        return self.payload.headers

    def attribute(self, name: str, default: Any = None) -> Any:
        """The Python value of attribute ``name`` (``str``, ``int``, ``Decimal`` or ``bytes``)."""
        value = self.raw_attributes.get(name)
        return default if value is None else attribute_python_value(value)

    # -- transport metadata ----------------------------------------------------------------

    @property
    def sqs_message_id(self) -> str | None:
        """The SQS message id."""
        return self.delivery.sqs_message_id if self.delivery is not None else None

    @property
    def receive_count(self) -> int:
        """How many times SQS delivered this copy of the message (``ApproximateReceiveCount``)."""
        return self.delivery.receive_count if self.delivery is not None else 1

    @property
    def attempt(self) -> int:
        """The processing attempt number across re-enqueues (requeues add to SQS' receive count).

        Receives of a FIFO message that the consumer handed back unprocessed, because an earlier
        message of its group was retried, are not attempts (they still count for SQS's
        ``maxReceiveCount``, and in another consumer process that receives the group next).
        """
        requeued = self.raw_attributes.get("fanferro.attempt")
        previous = 0
        if requeued is not None:
            try:
                previous = int(str(requeued.value))
            except ValueError:
                previous = 0
        return max(1, previous + self.receive_count - self._handed_back)

    @property
    def group_id(self) -> str | None:
        """FIFO message group id."""
        return self.delivery.group_id if self.delivery is not None else None

    @property
    def sequence_number(self) -> str | None:
        """FIFO sequence number."""
        return self.delivery.sequence_number if self.delivery is not None else None

    @property
    def sent_at(self) -> datetime | None:
        """When SQS accepted the message."""
        return self.delivery.sent_at if self.delivery is not None else None

    @property
    def received_at(self) -> datetime | None:
        """When this consumer received the message."""
        return self.delivery.received_at if self.delivery is not None else None

    @property
    def topic_arn(self) -> str | None:
        """The SNS topic the message was published to (when delivered through SNS)."""
        sns = self.delivery.sns if self.delivery is not None else None
        return sns["topic_arn"] if sns else None

    @property
    def trace_header(self) -> str | None:
        """The AWS X-Ray trace header (``AWSTraceHeader``), if any."""
        return self.delivery.aws_trace_header if self.delivery is not None else None

    @property
    def pointer(self) -> PayloadPointer | None:
        """Where the payload was stored, when it was offloaded (claim check)."""
        return PayloadPointer.from_core(self.delivery.pointer if self.delivery is not None else None)

    @property
    def dead_letter_info(self) -> dict[str, Any] | None:
        """Why fanferro dead-lettered this message (when consuming a dead-letter queue)."""
        value = self.raw_attributes.get("fanferro.dead-letter")
        if value is None:
            return None
        try:
            info = json.loads(str(value.value))
        except ValueError:
            return None
        return info if isinstance(info, dict) else None

    @property
    def remaining_visibility(self) -> float | None:
        """Seconds until the message becomes visible to other consumers (``None`` outside a consumer)."""
        return self.delivery.remaining_visibility if self.delivery is not None else None

    # -- settlement ------------------------------------------------------------------------

    @property
    def settled(self) -> bool:
        """Whether the message has been settled."""
        return self._settled

    def _begin(self, outcome: Outcome) -> Settler | None:
        if self._settled:
            raise RuntimeError(f"message {self.id} was already settled ({self.outcome})")
        self._settled = True
        self.outcome = outcome
        return self._settler

    async def ack(self) -> None:
        """Deletes the message: processing succeeded."""
        settler = self._begin("acked")
        if settler is not None:
            await settler.ack(self)

    async def retry(self, *, delay: Duration | None = None, error: BaseException | None = None) -> None:
        """Retries later (after ``delay`` seconds or ``timedelta``, or the retry policy's backoff).

        Raises:
            ValueError: If ``delay`` is negative or NaN (the message stays unsettled).
        """
        delay = optional_seconds(delay, "delay")  # before _begin: an invalid delay settles nothing
        settler = self._begin("retried")
        if settler is not None:
            await settler.retry(self, delay, error)

    async def requeue(self, *, delay: Duration | None = None, error: BaseException | None = None) -> None:
        """Re-enqueues a copy (attempt counter + 1) and deletes this one.

        Raises:
            ValueError: If ``delay`` is negative or NaN (the message stays unsettled).
        """
        delay = optional_seconds(delay, "delay")  # before _begin: an invalid delay settles nothing
        settler = self._begin("requeued")
        if settler is not None:
            await settler.requeue(self, delay, error)

    async def dead_letter(
        self, reason: str = "dead-lettered by handler", *, error: BaseException | None = None
    ) -> None:
        """Forwards the message to the dead-letter queue."""
        settler = self._begin("dead_lettered")
        if settler is not None:
            await settler.dead_letter(self, reason, error)

    async def release(self) -> None:
        """Makes the message visible again immediately (no processing happened)."""
        settler = self._begin("released")
        if settler is not None:
            await settler.release(self)

    async def discard(self, reason: str = "discarded by handler") -> None:
        """Deletes the message without processing it."""
        settler = self._begin("discarded")
        if settler is not None:
            await settler.discard(self, reason)

    async def extend(self, timeout: Duration) -> None:
        """Keeps the message invisible for ``timeout`` more seconds (heartbeats do this automatically).

        Raises:
            ValueError: If ``timeout`` is negative or NaN.
        """
        timeout = seconds(timeout, "timeout")
        if self._settled:
            raise RuntimeError(f"message {self.id} was already settled ({self.outcome})")
        if self._settler is not None:
            await self._settler.extend(self, timeout)

    def __repr__(self) -> str:
        return (
            f"Message(id={self.id!r}, type={self.type!r}, queue={self.queue!r}, "
            f"receive_count={self.receive_count}, outcome={self.outcome!r})"
        )


@dataclass(slots=True)
class OutgoingMessage:
    """A message to publish, with every option. Middleware receives and may modify it.

    The message holds its own copies of ``attributes`` and ``headers``: changing them never changes
    the dicts it was created with.

    Attributes:
        body: The application object (encoded by a codec).
        type: Message type name (defaults to the name registered for the body's class).
        attributes: User message attributes (at most 10 in total, including fanferro's own).
        headers: Envelope headers (CloudEvents extension attributes; lowercase alphanumeric names).
        id: Message id (stable across retries; used for deduplication).
        time: Creation time (defaults to now).
        subject: CloudEvents ``subject``.
        source: CloudEvents ``source`` (defaults to the client's ``source``).
        data_schema: CloudEvents ``dataschema``.
        content_type: Overrides the codec's content type (for ``bytes``/``str`` bodies).
        group_id: Message group id (required for FIFO destinations).
        deduplication_id: FIFO deduplication id (defaults to ``id``).
        delay: Delivery delay in seconds or as a ``timedelta`` (standard queues only, at most 900
            seconds; kept as float seconds).
        sns_subject: SNS ``Subject`` (topics only; shown in email notifications).
        codec: Codec (or codec name) to use instead of automatic selection.
        envelope: Envelope to use instead of the destination's.
        trace_header: AWS X-Ray trace header (set by telemetry).
    """

    body: Any
    type: str | None = None
    attributes: dict[str, AttributeInput] = field(default_factory=dict)
    headers: dict[str, HeaderValue] = field(default_factory=dict)
    id: str = field(default_factory=new_message_id)
    time: datetime | None = None
    subject: str | None = None
    source: str | None = None
    data_schema: str | None = None
    content_type: str | None = None
    group_id: str | None = None
    deduplication_id: str | None = None
    delay: Duration | None = None
    sns_subject: str | None = None
    codec: Codec | str | None = None
    envelope: Envelope | None = None
    trace_header: str | None = None

    def __post_init__(self) -> None:
        # Own copies: middleware and telemetry add entries (tenant, trace context...), and the dicts
        # passed in may be shared by several messages (constants, the defaults of publish_batch).
        # None (an optional argument passed through) means none.
        self.attributes = dict(self.attributes) if self.attributes is not None else {}
        self.headers = dict(self.headers) if self.headers is not None else {}
        self.delay = optional_seconds(self.delay, "delay")


@dataclass(frozen=True, slots=True)
class PublishResult:
    """The outcome of a successful publish/send."""

    message_id: str
    """fanferro message id."""
    transport_message_id: str
    """SQS/SNS message id."""
    destination: str
    """Queue or topic name."""
    sequence_number: str | None = None
    """FIFO sequence number."""
    size: int = 0
    """Encoded size in bytes (body plus attributes)."""
    offloaded: PayloadPointer | None = None
    """Where the payload was stored when it was offloaded (claim check)."""

    @classmethod
    def from_core(cls, data: Mapping[str, Any], destination: str) -> PublishResult:
        """Converts the core receipt."""
        return cls(
            message_id=data["message_id"],
            transport_message_id=data["transport_message_id"],
            destination=destination,
            sequence_number=data.get("sequence_number"),
            size=data.get("size", 0),
            offloaded=PayloadPointer.from_core(data.get("offloaded")),
        )


@dataclass(frozen=True, slots=True)
class BatchFailure:
    """A batch entry that could not be published."""

    index: int
    """Position in the input sequence."""
    message: OutgoingMessage | PreparedMessage
    """The message."""
    error: BaseException
    """Why it failed (``error.retryable`` tells whether retrying may help)."""


@dataclass(frozen=True, slots=True)
class BatchPublishResult:
    """Per-entry outcomes of a batch publish, in input order."""

    results: tuple[PublishResult | BatchFailure, ...]

    @property
    def successful(self) -> list[PublishResult]:
        """Entries that were published."""
        return [r for r in self.results if isinstance(r, PublishResult)]

    @property
    def failed(self) -> list[BatchFailure]:
        """Entries that failed."""
        return [r for r in self.results if isinstance(r, BatchFailure)]

    @property
    def ok(self) -> bool:
        """Whether every entry was published."""
        return not self.failed

    def __len__(self) -> int:
        return len(self.results)

    def raise_for_failures(self) -> None:
        """Raises :class:`fanferro.errors.BatchPublishError` if any entry failed."""
        if self.failed:
            from fanferro.errors import BatchPublishError

            first = self.failed[0].error
            raise BatchPublishError(
                f"{len(self.failed)} of {len(self.results)} messages failed to publish (first error: {first})", self
            )


def _encode_attribute(value: AttributeValue) -> dict[str, str]:
    raw = value.value
    if isinstance(raw, bytes):
        return {"data_type": value.data_type, "binary": base64.b64encode(raw).decode()}
    return {"data_type": value.data_type, "text": raw}


def _decode_attribute(data: Mapping[str, str]) -> AttributeValue:
    if "binary" in data:
        return AttributeValue(data["data_type"], base64.b64decode(data["binary"]))
    return AttributeValue(data["data_type"], data["text"])


@dataclass(frozen=True, slots=True)
class PreparedMessage:
    """A fully encoded message ready to be sent later, e.g. from a transactional outbox.

    Produced by ``topic.prepare(...)`` / ``queue.prepare(...)`` (codec, envelope, attributes and
    trace context applied, no network call) and sent with ``client.publish_prepared(...)``.
    :meth:`to_json`/:meth:`from_json` round-trip it through a database column. Large payloads
    are offloaded at send time, not at prepare time.
    """

    destination: Mapping[str, Any]
    destination_name: str
    wire: WireMessage
    message_id: str
    message_type: str | None = None
    group_id: str | None = None
    deduplication_id: str | None = None
    delay: float | None = None
    sns_subject: str | None = None
    trace_header: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """A JSON-compatible representation."""
        body = self.wire.body
        text = self.wire.body_text
        if text is not None and not is_sqs_safe_text(text):
            # Control characters such as NUL are valid UTF-8 but not text that every store keeps
            # (PostgreSQL rejects the JSON escape of U+0000 in jsonb): such bodies go as base64.
            text = None
        return {
            "version": 1,
            "destination": dict(self.destination),
            "destination_name": self.destination_name,
            "body": {"text": text} if text is not None else {"base64": base64.b64encode(body).decode()},
            "attributes": {name: _encode_attribute(value) for name, value in self.wire.attributes.items()},
            "message_id": self.message_id,
            "message_type": self.message_type,
            "group_id": self.group_id,
            "deduplication_id": self.deduplication_id,
            "delay": self.delay,
            "sns_subject": self.sns_subject,
            "trace_header": self.trace_header,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PreparedMessage:
        """Reverses :meth:`to_dict`."""
        if data.get("version") != 1:
            raise ValueError(f"unsupported prepared message version {data.get('version')!r}")
        body_data = data["body"]
        body: bytes | str = body_data["text"] if "text" in body_data else base64.b64decode(body_data["base64"])
        attributes = {name: _decode_attribute(value) for name, value in data.get("attributes", {}).items()}
        return cls(
            destination=dict(data["destination"]),
            destination_name=data["destination_name"],
            wire=WireMessage(body, attributes),
            message_id=data["message_id"],
            message_type=data.get("message_type"),
            group_id=data.get("group_id"),
            deduplication_id=data.get("deduplication_id"),
            delay=data.get("delay"),
            sns_subject=data.get("sns_subject"),
            trace_header=data.get("trace_header"),
        )

    def to_json(self) -> str:
        """Serializes to a JSON string."""
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_json(cls, text: str | bytes) -> PreparedMessage:
        """Parses :meth:`to_json` output."""
        return cls.from_dict(json.loads(text))

    def core_entry(self) -> dict[str, Any]:
        """The batch entry passed to the core."""
        return {
            "wire": self.wire,
            "message_id": self.message_id,
            "group_id": self.group_id,
            "deduplication_id": self.deduplication_id,
            "delay": self.delay,
            "subject": self.sns_subject,
            "trace_header": self.trace_header,
        }


def ensure_sequence[T](value: T | Sequence[T]) -> list[T]:
    """Wraps a single value in a list (strings count as single values)."""
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return list(value)
    return [value]  # type: ignore[list-item]
