"""Codec of the notifications AWS services send (``fanferro.aws``): S3 events, SES notifications."""

from __future__ import annotations

import json
from typing import Any

from fanferro.aws import AwsDocument
from fanferro.codecs._base import JSON_CONTENT_TYPE, EncodedBody, SettingsEquality, is_json_content_type, loads_json
from fanferro.errors import DecodeError

__all__ = ["AwsNotificationCodec"]

_ENCODER = json.JSONEncoder(separators=(",", ":"), ensure_ascii=False)


class AwsNotificationCodec(SettingsEquality):
    """Decodes AWS's JSON documents into the classes of :mod:`fanferro.aws` and encodes them back.

    Their message type names are those the :class:`~fanferro.envelopes.AwsNotifications` envelope
    gives the notifications it recognises (``aws.s3.event``, ``aws.ses.bounce``...).
    """

    name = "aws"

    def can_encode(self, value: object) -> bool:
        return isinstance(value, AwsDocument)

    def encode(self, value: Any) -> EncodedBody:
        return EncodedBody(_ENCODER.encode(value.to_json()).encode(), JSON_CONTENT_TYPE)

    def can_decode(self, target: Any, content_type: str | None) -> bool:
        return (
            isinstance(target, type)
            and issubclass(target, AwsDocument)
            and (content_type is None or is_json_content_type(content_type))
        )

    def decode(self, data: bytes, target: Any, content_type: str | None) -> Any:
        try:
            document = loads_json(data)
        except ValueError as error:
            raise DecodeError(f"message body is not valid JSON: {error}") from error
        return target.from_json(document)

    def type_name(self, target: Any) -> str | None:
        if isinstance(target, type) and issubclass(target, AwsDocument):
            return target.aws_message_type
        return None
