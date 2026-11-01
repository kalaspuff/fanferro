"""S3 event notifications."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime  # noqa: TC003 - get_type_hints() resolves the annotations at runtime
from typing import Any, ClassVar

from fanferro.aws._document import _SKIP, AwsDocument, _at, _dump, _parse, _parse_fields


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
