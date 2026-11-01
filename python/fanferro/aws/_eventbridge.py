"""EventBridge events: the wrapper's metadata, and S3's events."""

from __future__ import annotations

from collections.abc import Mapping  # noqa: TC003 - get_type_hints() resolves the annotations at runtime
from dataclasses import dataclass, field
from datetime import datetime  # noqa: TC003 - get_type_hints() resolves the annotations at runtime
from typing import TYPE_CHECKING, Any, ClassVar

from fanferro.aws._document import AwsDocument, _at
from fanferro.message_types import message_type

if TYPE_CHECKING:
    from fanferro.messages import Message


@dataclass(frozen=True, slots=True, kw_only=True)
class EventBridgeMetadata:
    """The wrapper of an EventBridge event a consumer received (its ``detail`` is the body).

    ``message.id``, ``message.time``, ``message.source`` and ``message.type`` have the event's id,
    time, source and type too; this adds the rest::

        metadata = aws.EventBridgeMetadata.of(message)
        if metadata is not None and metadata.account != OUR_ACCOUNT:
            ...
    """

    id: str
    detail_type: str
    source: str
    time: datetime | None = None
    account: str | None = None
    region: str | None = None
    resources: tuple[str, ...] = ()
    """ARNs of the resources the event is about (the bucket, the rule, the task...)."""

    @classmethod
    def of(cls, message: Message[Any]) -> EventBridgeMetadata | None:
        """The metadata of ``message`` (``None`` if it is not an EventBridge event)."""
        headers = message.headers
        detail_type = headers.get("detail-type")
        if message.envelope != "aws" or not isinstance(detail_type, str):
            return None
        account, region, resources = headers.get("account"), headers.get("region"), headers.get("resources")
        return cls(
            id=message.id,
            detail_type=detail_type,
            source=message.source or "",
            time=message.payload.time,
            account=account if isinstance(account, str) else None,
            region=region if isinstance(region, str) else None,
            resources=tuple(resources.split(",")) if isinstance(resources, str) and resources else (),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class S3EventBridgeBucket(AwsDocument):
    """The bucket of an S3 event sent through EventBridge."""

    name: str
    aws_generated_tags: Mapping[str, str] = field(default_factory=dict, metadata=_at("aws-generated-tags"))


@dataclass(frozen=True, slots=True, kw_only=True)
class S3EventBridgeObject(AwsDocument):
    """The object of an S3 event sent through EventBridge."""

    key: str
    """The object key as it is (EventBridge does not URL-encode it, unlike S3's notifications)."""
    size: int | None = None
    etag: str | None = None
    version_id: str | None = field(default=None, metadata=_at("version-id"))
    sequencer: str | None = None
    has_object_annotation: bool | None = field(default=None, metadata=_at("has-object-annotation"))


_S3_EVENTS = (
    "object_created",
    "object_deleted",
    "object_restore_initiated",
    "object_restore_completed",
    "object_restore_expired",
    "object_tags_added",
    "object_tags_deleted",
    "object_acl_updated",
    "object_storage_class_changed",
    "object_access_tier_changed",
    "object_annotation_created",
    "object_annotation_removed",
    "object_retention_updated",
)


@message_type(*(f"aws.s3.{event}" for event in _S3_EVENTS))
@dataclass(frozen=True, slots=True, kw_only=True)
class S3ObjectEvent(AwsDocument):
    """An S3 event sent through EventBridge (its ``detail``): ``aws.s3.object_created``, ...

    Routes for this class match every S3 event type (``aws.s3.object_created``,
    ``aws.s3.object_deleted``, ``aws.s3.object_restore_completed``...); route one of them with
    ``@consumer.route("aws.s3.object_created")``, or read ``message.type``.
    """

    aws_message_type: ClassVar[str | None] = "aws.s3.object_created"

    bucket: S3EventBridgeBucket
    object: S3EventBridgeObject
    version: str | None = None
    event_version: str | None = field(default=None, metadata=_at("event-version"))
    request_id: str | None = field(default=None, metadata=_at("request-id"))
    requester: str | None = None
    """The account id or AWS service principal that made the request."""
    source_ip_address: str | None = field(default=None, metadata=_at("source-ip-address"))
    reason: str | None = None
    """``PutObject``, ``POST Object``, ``CopyObject``, ``CompleteMultipartUpload`` (created);
    ``DeleteObject``, ``Lifecycle Expiration`` (deleted)."""
    deletion_type: str | None = field(default=None, metadata=_at("deletion-type"))
    """``Permanently Deleted`` or ``Delete Marker Created``."""
    restore_expiry_time: datetime | None = field(default=None, metadata=_at("restore-expiry-time"))
    source_storage_class: str | None = field(default=None, metadata=_at("source-storage-class"))
    destination_storage_class: str | None = field(default=None, metadata=_at("destination-storage-class"))
    destination_access_tier: str | None = field(default=None, metadata=_at("destination-access-tier"))

    @property
    def sequence(self) -> int | None:
        """The sequencer as a number: of two events of the same key, the greater happened later."""
        return int(self.object.sequencer, 16) if self.object.sequencer else None
