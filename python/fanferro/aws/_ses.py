"""SES email notifications."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime  # noqa: TC003 - get_type_hints() resolves the annotations at runtime
from typing import Any, ClassVar, Self

from fanferro.aws._document import _SKIP, AwsDocument, _at, _dump_fields, _parse_fields


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
