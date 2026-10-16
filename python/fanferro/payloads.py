"""Large payloads (claim checks): store big messages in S3 and send a pointer.

SQS accepts messages of up to 1 MiB, SNS of up to 256 KiB by default. With :class:`LargePayloads`,
messages whose encoded size exceeds ``threshold`` (256 KiB by default) are uploaded to S3 and
replaced by a small pointer; consumers download them transparently (the handler never notices).

The pointer can contain:

* the bucket and key (``reference="s3"``): receivers need ``s3:GetObject``;
* a presigned URL (``reference="url"``): receivers need no AWS permissions at all;
* both (``reference="both"``): receivers try their preferred method first.

``format="aws_extended"`` writes pointers compatible with the AWS SQS/SNS Extended Client
libraries (Java, Python), so fanferro can interoperate with services that use them. Consumers
accept both formats regardless of this setting.

Lifecycle: payload objects are not deleted by default (with SNS fan-out several subscribers read
the same object). Configure an S3 lifecycle rule on the prefix, or pass
``delete_offloaded_payloads=True`` to consumers of point-to-point queues.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Literal

from fanferro._utils import Duration, seconds
from fanferro.provisioning import Kms

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["LargePayloads", "PayloadFetch"]


@dataclass(frozen=True, slots=True)
class LargePayloads:
    """Offload large message bodies to S3.

    Attributes:
        bucket: Bucket for payload objects.
        prefix: Key prefix; the object key is ``{prefix}{message_id}`` (idempotent across retries).
        threshold: Offload when the encoded message (body plus attributes) exceeds this many bytes.
        always: Offload every message regardless of size.
        reference: What the pointer contains: ``"s3"``, ``"url"`` (presigned) or ``"both"``.
        presign_ttl: Lifetime of presigned URLs (24 hours by default, at most 7 days). A URL signed
            with temporary credentials (an IAM role) stops working when they expire, which can be
            sooner: consumers that can read the bucket should get ``reference="both"``.
        format: ``"fanferro"`` or ``"aws_extended"`` (AWS extended client compatible; S3 only).
        encryption: ``"s3"`` (SSE-S3), a :class:`fanferro.Kms` key (SSE-KMS) or ``None`` (bucket default).
        bucket_key: Use S3 bucket keys with SSE-KMS (fewer KMS requests).
        storage_class: S3 storage class, e.g. ``"INTELLIGENT_TIERING"``.
        create_bucket: Create the bucket during provisioning if it does not exist.
    """

    bucket: str
    prefix: str = "fanferro/"
    threshold: int = 256 * 1024
    always: bool = False
    reference: Literal["s3", "url", "both"] = "s3"
    presign_ttl: Duration = timedelta(hours=24)
    format: Literal["fanferro", "aws_extended"] = "fanferro"
    encryption: Literal["s3"] | Kms | None = None
    bucket_key: bool = True
    storage_class: str | None = None
    create_bucket: bool = False

    def __post_init__(self) -> None:
        if not self.bucket:
            raise ValueError("bucket must not be empty")
        if self.threshold <= 0:
            raise ValueError("threshold must be positive")
        if self.format == "aws_extended" and self.reference != "s3":
            raise ValueError("the aws_extended pointer format only supports reference='s3'")

    def to_core(self) -> dict[str, Any]:
        """The core representation."""
        encryption: dict[str, Any]
        if self.encryption is None:
            encryption = {"mode": "bucket_default"}
        elif isinstance(self.encryption, Kms):
            encryption = {"mode": "kms", "key_id": self.encryption.key_id, "bucket_key": self.bucket_key}
        else:
            encryption = {"mode": "s3"}
        return {
            "bucket": self.bucket,
            "key_prefix": self.prefix,
            "threshold": self.threshold,
            "always": self.always,
            "reference": self.reference,
            "presign_ttl": seconds(self.presign_ttl, "presign_ttl"),
            "format": self.format,
            "encryption": encryption,
            "storage_class": self.storage_class,
        }


@dataclass(frozen=True, slots=True)
class PayloadFetch:
    """How consumers download offloaded payloads.

    Attributes:
        max_bytes: Refuse payloads larger than this (protects memory).
        prefer: Try ``"s3"`` (``GetObject``) or ``"url"`` (presigned URL) first.
        allowed_url_hosts: Hosts presigned URLs may point to (``*.example.com`` wildcards allowed).
            Empty means AWS hosts plus the configured ``endpoint_url`` - this blocks server-side
            request forgery through crafted messages.
        allow_http: Allow plain ``http://`` URLs (always allowed for the ``endpoint_url`` host).
        verify_checksum: Verify the recorded size and SHA-256.
    """

    max_bytes: int = 64 * 1024 * 1024
    prefer: Literal["s3", "url"] = "s3"
    allowed_url_hosts: Sequence[str] = field(default_factory=tuple)
    allow_http: bool = False
    verify_checksum: bool = True

    def to_core(self) -> dict[str, Any]:
        """The core representation."""
        return {
            "max_bytes": self.max_bytes,
            "prefer": self.prefer,
            "allowed_url_hosts": list(self.allowed_url_hosts),
            "allow_http": self.allow_http,
            "verify_checksum": self.verify_checksum,
        }
