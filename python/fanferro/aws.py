"""Typed bodies of the notifications AWS services send to SNS and SQS: S3 events and SES email.

Consumers recognise these notifications by their body (the
:class:`~fanferro.envelopes.AwsNotifications` envelope, one of the defaults) and give them a
message type, so a handler annotated with one of these classes receives them decoded::

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
========================================  ===============================  ==============================

SES notifications come in two formats, feedback notifications of an identity (``notificationType``)
and event publishing of a configuration set (``eventType``); both decode into the same classes.
The classes are frozen dataclasses with the fields AWS documents, in snake case; fields AWS leaves
out are ``None`` (or empty), and fields it adds later are ignored (``message.json()`` has the whole
document). They encode back into AWS's documents, so tests can publish them (with the
``AwsNotifications`` envelope, as AWS sends them) or pass them to ``fanferro.testing.build_message()``.
"""

from __future__ import annotations

import dataclasses
import functools
import re
import types
import typing
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar, Self
from urllib.parse import quote_plus, unquote_plus

from fanferro.errors import DecodeError

__all__ = [
    "S3Bucket",
    "S3Event",
    "S3EventRecord",
    "S3Object",
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

_METADATA = "fanferro.aws"

_INTEGER = re.compile(r"-?[0-9]+")
"""An integer as a string (ASCII digits only: ``str.isdigit()`` accepts ``²``, which ``int()`` rejects)."""


@dataclass(frozen=True, slots=True)
class _Key:
    path: tuple[str, ...]
    url_encoded: bool = False


def _at(*path: str, url_encoded: bool = False) -> dict[str, Any]:
    """Field metadata: where the field is in AWS's document (default: its name in camel case)."""
    return {_METADATA: _Key(path, url_encoded)}


_SKIP: dict[str, Any] = {_METADATA: None}
"""Field metadata of fields that are not in the document."""


class AwsDocument:
    """Base of the classes of AWS notification documents (decoded and encoded by fanferro's codec)."""

    __slots__ = ()

    aws_message_type: ClassVar[str | None] = None
    """The message type fanferro gives the notification (``None``: not a notification of its own)."""

    @classmethod
    def from_json(cls, document: Mapping[str, Any]) -> Self:
        """Parses AWS's JSON document (as ``json.loads`` returns it).

        Raises:
            DecodeError: When the document lacks a required field or has a value of the wrong type.
        """
        return _parse(cls, document, ())

    def to_json(self) -> dict[str, Any]:
        """The JSON document AWS sends for this notification (fields AWS leaves out omitted)."""
        return _dump(self)


def _camel(name: str) -> str:
    head, *rest = name.rstrip("_").split("_")
    return head + "".join(part[:1].upper() + part[1:] for part in rest)


@functools.cache
def _fields(cls: type) -> tuple[tuple[str, _Key, Any, bool], ...]:
    """Each document field of ``cls``: name, key, type and whether it is required."""
    hints = typing.get_type_hints(cls)
    result = []
    for f in dataclasses.fields(cls):
        key = f.metadata.get(_METADATA, _Key((_camel(f.name),)))
        if key is None:
            continue
        required = f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING
        result.append((f.name, key, hints[f.name], required))
    return tuple(result)


def _render(path: tuple[str, ...]) -> str:
    return ".".join(path) or "document"


def _parse[T](cls: type[T], document: Any, path: tuple[str, ...]) -> T:
    custom = getattr(cls, "_from_document", None)
    return custom(document, path) if custom is not None else _parse_fields(cls, document, path)


def _parse_fields[T](cls: type[T], document: Any, path: tuple[str, ...], **values: Any) -> T:
    if not isinstance(document, Mapping):
        raise DecodeError(f"{_render(path)}: expected an object for {cls.__name__}, got {type(document).__name__}")
    for name, key, hint, required in _fields(cls):
        if name in values:
            continue
        value: Any = document
        for part in key.path:
            value = value.get(part) if isinstance(value, Mapping) else None
        if value is None:
            if required:
                raise DecodeError(f"{_render((*path, *key.path))}: missing required field for {cls.__name__}")
            continue
        if key.url_encoded and isinstance(value, str):
            value = unquote_plus(value)
        values[name] = _convert(value, hint, (*path, *key.path))
    return cls(**values)


def _convert(value: Any, hint: Any, path: tuple[str, ...]) -> Any:
    origin = typing.get_origin(hint)
    if origin in (types.UnionType, typing.Union):
        options = [arg for arg in typing.get_args(hint) if arg is not type(None)]
        return _convert(value, options[0], path)
    if hint is Any:
        return value
    if isinstance(hint, type) and issubclass(hint, AwsDocument):
        return _parse(hint, value, path)
    if origin is tuple:
        if not isinstance(value, list):
            raise DecodeError(f"{_render(path)}: expected a list, got {type(value).__name__}")
        (item,) = typing.get_args(hint)[:1]
        return tuple(_convert(entry, item, (*path, str(index))) for index, entry in enumerate(value))
    if origin is Mapping:
        if not isinstance(value, Mapping):
            raise DecodeError(f"{_render(path)}: expected an object, got {type(value).__name__}")
        item = typing.get_args(hint)[1]
        return {str(name): _convert(entry, item, (*path, str(name))) for name, entry in value.items()}
    if hint is datetime:
        try:
            parsed = datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise DecodeError(f"{_render(path)}: expected an ISO 8601 time, got {value!r}") from None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    if hint is bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.lower() in ("true", "false"):  # SES documents it as a string too
            return value.lower() == "true"
    elif hint is int:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str) and _INTEGER.fullmatch(value.strip()):
            return int(value)
    elif hint is str:
        if isinstance(value, str):
            return value
        if isinstance(value, int | float) and not isinstance(value, bool):
            return str(value)
    raise DecodeError(f"{_render(path)}: expected {getattr(hint, '__name__', hint)}, got {value!r}")


def _dump(document: AwsDocument) -> dict[str, Any]:
    custom = getattr(document, "_to_document", None)
    return custom() if custom is not None else _dump_fields(document)


def _dump_fields(document: AwsDocument) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, key, _, _ in _fields(type(document)):
        value = getattr(document, name)
        if value is None:
            continue
        if key.url_encoded and isinstance(value, str):
            value = quote_plus(value, safe="/")
        target = result
        for part in key.path[:-1]:
            target = target.setdefault(part, {})
        target[key.path[-1]] = _to_jsonable(value)
    return result


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, AwsDocument):
        return _dump(value)
    if isinstance(value, tuple):
        return [_to_jsonable(entry) for entry in value]
    if isinstance(value, Mapping):
        return {name: _to_jsonable(entry) for name, entry in value.items()}
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return value


# --- S3 ------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class S3Bucket(AwsDocument):
    """The bucket of an S3 event."""

    name: str
    arn: str | None = None
    owner_principal_id: str | None = field(default=None, metadata=_at("ownerIdentity", "principalId"))


@dataclass(frozen=True, slots=True, kw_only=True)
class S3Object(AwsDocument):
    """The object of an S3 event."""

    key: str = field(metadata=_at("key", url_encoded=True))
    """The object key, URL-decoded (S3 sends ``red+flower.jpg`` for ``red flower.jpg``)."""
    size: int | None = None
    """Size in bytes (not in events of deleted objects)."""
    etag: str | None = field(default=None, metadata=_at("eTag"))
    version_id: str | None = None
    """The version, in versioned buckets."""
    sequencer: str | None = None
    """Orders the events of one key (PUT and DELETE events); compare with :meth:`S3EventRecord.sequence`."""


@dataclass(frozen=True, slots=True, kw_only=True)
class S3RestoreEventData(AwsDocument):
    """``glacierEventData.restoreEventData`` of ``ObjectRestore:Completed`` events."""

    lifecycle_restoration_expiry_time: datetime | None = None
    lifecycle_restore_storage_class: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class S3EventRecord(AwsDocument):
    """One event of an S3 event notification (an entry of ``Records``)."""

    event_name: str
    """The event type without the ``s3:`` prefix, e.g. ``ObjectCreated:Put`` or ``ObjectRemoved:Delete``."""
    event_time: datetime
    bucket: S3Bucket = field(metadata=_at("s3", "bucket"))
    object: S3Object = field(metadata=_at("s3", "object"))
    event_version: str | None = None
    event_source: str = "aws:s3"
    aws_region: str | None = None
    principal_id: str | None = field(default=None, metadata=_at("userIdentity", "principalId"))
    """The IAM principal that caused the event."""
    source_ip_address: str | None = field(default=None, metadata=_at("requestParameters", "sourceIPAddress"))
    request_id: str | None = field(default=None, metadata=_at("responseElements", "x-amz-request-id"))
    host_id: str | None = field(default=None, metadata=_at("responseElements", "x-amz-id-2"))
    configuration_id: str | None = field(default=None, metadata=_at("s3", "configurationId"))
    """The id of the bucket's notification configuration that sent the event."""
    s3_schema_version: str | None = field(default=None, metadata=_at("s3", "s3SchemaVersion"))
    restore: S3RestoreEventData | None = field(default=None, metadata=_at("glacierEventData", "restoreEventData"))

    @property
    def category(self) -> str:
        """The event's category: ``ObjectCreated``, ``ObjectRemoved``, ``ObjectRestore``..."""
        return self.event_name.partition(":")[0]

    @property
    def created(self) -> bool:
        """Whether an object was created (``ObjectCreated:*``)."""
        return self.category == "ObjectCreated"

    @property
    def removed(self) -> bool:
        """Whether an object was removed (``ObjectRemoved:*``)."""
        return self.category == "ObjectRemoved"

    @property
    def sequence(self) -> int | None:
        """The sequencer as a number: of two events of the same key, the greater happened later."""
        return int(self.object.sequencer, 16) if self.object.sequencer else None


@dataclass(frozen=True, slots=True, kw_only=True)
class S3TestEvent(AwsDocument):
    """The message S3 sends when a bucket's notification configuration is set up."""

    bucket: str = field(metadata=_at("Bucket"))
    time: datetime | None = field(default=None, metadata=_at("Time"))
    request_id: str | None = field(default=None, metadata=_at("RequestId"))
    host_id: str | None = field(default=None, metadata=_at("HostId"))


@dataclass(frozen=True, slots=True, kw_only=True)
class S3Event(AwsDocument):
    """An S3 event notification (message type ``aws.s3.event``).

    S3's test event (sent when notifications are set up) is an ``S3Event`` too, without records and
    with :attr:`test_event`, so handlers that go through :attr:`records` skip it.
    """

    aws_message_type: ClassVar[str | None] = "aws.s3.event"

    records: tuple[S3EventRecord, ...] = field(default=(), metadata=_at("Records"))
    test_event: S3TestEvent | None = field(default=None, metadata=_SKIP)

    @classmethod
    def _from_document(cls, document: Any, path: tuple[str, ...]) -> S3Event:
        if isinstance(document, Mapping) and "Records" not in document and document.get("Event") == "s3:TestEvent":
            return cls(test_event=_parse(S3TestEvent, document, path))
        return _parse_fields(cls, document, path)

    def _to_document(self) -> dict[str, Any]:
        if self.test_event is not None and not self.records:
            return {"Service": "Amazon S3", "Event": "s3:TestEvent", **_dump(self.test_event)}
        return {"Records": [_dump(record) for record in self.records]}


# --- SES -----------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class SesHeader(AwsDocument):
    """A header of the original email."""

    name: str
    value: str


@dataclass(frozen=True, slots=True, kw_only=True)
class SesCommonHeaders(AwsDocument):
    """Common headers of the original email (``mail.commonHeaders``)."""

    from_: tuple[str, ...] = field(default=(), metadata=_at("from"))
    to: tuple[str, ...] = ()
    cc: tuple[str, ...] = ()
    bcc: tuple[str, ...] = ()
    reply_to: tuple[str, ...] = ()
    sender: tuple[str, ...] = ()
    return_path: str | None = None
    message_id: str | None = None
    date: str | None = None
    """As in the email (RFC 5322, e.g. ``Mon, 08 Oct 2018 14:05:45 +0000``)."""
    subject: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SesMail(AwsDocument):
    """The email a notification is about (``mail``)."""

    message_id: str
    """The id SES gave the email (what ``SendEmail`` returned)."""
    timestamp: datetime | None = None
    """When the email was sent."""
    source: str | None = None
    """The envelope ``MAIL FROM`` address."""
    source_arn: str | None = None
    source_ip: str | None = None
    sending_account_id: str | None = None
    caller_identity: str | None = None
    destination: tuple[str, ...] = ()
    headers_truncated: bool | None = None
    headers: tuple[SesHeader, ...] = ()
    """The original headers (when the notifications include them)."""
    common_headers: SesCommonHeaders | None = None
    tags: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    """Tags of the email (event publishing only)."""


@dataclass(frozen=True, slots=True, kw_only=True)
class SesBouncedRecipient(AwsDocument):
    """A recipient whose address bounced."""

    email_address: str
    action: str | None = None
    """The DSN's ``Action`` (``failed``, ``delayed``...)."""
    status: str | None = None
    """The DSN's ``Status`` (e.g. ``5.1.1``)."""
    diagnostic_code: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SesBounce(AwsDocument):
    """A bounce (``bounce``)."""

    bounce_type: str
    """``Permanent`` (hard), ``Transient`` (soft) or ``Undetermined``."""
    bounce_sub_type: str | None = None
    """E.g. ``General``, ``NoEmail``, ``Suppressed``, ``OnAccountSuppressionList``, ``MailboxFull``."""
    bounced_recipients: tuple[SesBouncedRecipient, ...] = ()
    timestamp: datetime | None = None
    feedback_id: str | None = None
    remote_mta_ip: str | None = None
    reporting_mta: str | None = field(default=None, metadata=_at("reportingMTA"))

    @property
    def permanent(self) -> bool:
        """Whether the bounce is hard: remove these addresses from mailing lists."""
        return self.bounce_type == "Permanent"

    @property
    def transient(self) -> bool:
        """Whether the bounce is soft: sending to these addresses may work later."""
        return self.bounce_type == "Transient"


@dataclass(frozen=True, slots=True, kw_only=True)
class SesComplainedRecipient(AwsDocument):
    """A recipient who may have complained."""

    email_address: str


@dataclass(frozen=True, slots=True, kw_only=True)
class SesComplaint(AwsDocument):
    """A complaint (``complaint``)."""

    complained_recipients: tuple[SesComplainedRecipient, ...] = ()
    timestamp: datetime | None = None
    feedback_id: str | None = None
    complaint_sub_type: str | None = None
    """``None``, ``OnAccountSuppressionList`` or ``OnTenantSuppressionList``."""
    complaint_feedback_type: str | None = None
    """The feedback report's type: ``abuse``, ``auth-failure``, ``fraud``, ``not-spam``, ``other``, ``virus``."""
    user_agent: str | None = None
    arrival_date: datetime | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SesDelivery(AwsDocument):
    """A delivery to the recipients' mail server (``delivery``)."""

    timestamp: datetime | None = None
    processing_time_millis: int | None = None
    recipients: tuple[str, ...] = ()
    smtp_response: str | None = None
    reporting_mta: str | None = field(default=None, metadata=_at("reportingMTA"))
    remote_mta_ip: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SesNotification(AwsDocument):
    """What every SES notification has: its type and the email. Decodes any SES notification."""

    notification_type: str
    """``Bounce``, ``Complaint``, ``Delivery``, ``Send``, ``Open``, ``Rendering Failure``..."""
    mail: SesMail
    event_publishing: bool = field(default=False, metadata=_SKIP)
    """Whether a configuration set's event publishing sent it (``eventType``), not an identity's
    feedback notifications (``notificationType``)."""

    @classmethod
    def _from_document(cls, document: Any, path: tuple[str, ...]) -> Self:
        if isinstance(document, Mapping) and "eventType" in document:
            return _parse_fields(
                cls, {**document, "notificationType": document["eventType"]}, path, event_publishing=True
            )
        return _parse_fields(cls, document, path)

    def _to_document(self) -> dict[str, Any]:
        document = _dump_fields(self)
        if self.event_publishing:
            document = {"eventType": document.pop("notificationType"), **document}
        return document


@dataclass(frozen=True, slots=True, kw_only=True)
class SesBounceNotification(SesNotification):
    """An SES bounce notification (message type ``aws.ses.bounce``)."""

    aws_message_type: ClassVar[str | None] = "aws.ses.bounce"

    bounce: SesBounce


@dataclass(frozen=True, slots=True, kw_only=True)
class SesComplaintNotification(SesNotification):
    """An SES complaint notification (message type ``aws.ses.complaint``)."""

    aws_message_type: ClassVar[str | None] = "aws.ses.complaint"

    complaint: SesComplaint


@dataclass(frozen=True, slots=True, kw_only=True)
class SesDeliveryNotification(SesNotification):
    """An SES delivery notification (message type ``aws.ses.delivery``)."""

    aws_message_type: ClassVar[str | None] = "aws.ses.delivery"

    delivery: SesDelivery
