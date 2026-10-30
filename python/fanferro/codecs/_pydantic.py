"""Pydantic v2 codec (models, and generic types such as ``list[Model]`` or discriminated unions)."""

from __future__ import annotations

import functools
import sys
import typing
from typing import Any

from fanferro.codecs._base import JSON_CONTENT_TYPE, EncodedBody, is_json_content_type, media_type
from fanferro.errors import DecodeError, EncodeError

__all__ = ["PydanticCodec"]


def _pydantic() -> Any:
    return sys.modules.get("pydantic")


@functools.lru_cache(maxsize=512)
def _adapter(target: Any) -> Any:
    import pydantic

    return pydantic.TypeAdapter(target)


@functools.cache
def _names_and_aliases() -> dict[str, Any]:
    """Validation options that accept field names as well as aliases (pydantic 2.11 or later).

    Bodies carry field names unless the codec or the model serializes by alias, while validation
    accepts only aliases by default: without both, a model with aliases could not read its own
    messages.
    """
    import pydantic

    major, minor = (int(part) for part in pydantic.VERSION.split(".")[:2])
    return {"by_alias": True, "by_name": True} if (major, minor) >= (2, 11) else {}


class PydanticCodec:
    """Encodes pydantic models with ``model_dump_json`` and validates JSON into models.

    Besides ``BaseModel`` subclasses it decodes typing constructs (``list[Model]``,
    ``Annotated[A | B, Field(discriminator="kind")]``, ``TypedDict``...) through ``TypeAdapter``.

    Decoding accepts field names and aliases (with pydantic 2.11 or later; before, only aliases
    unless the model sets ``populate_by_name``), so models with aliases read messages written
    either way.

    Args:
        by_alias: Serialize using field aliases (``None``: as the model's ``serialize_by_alias``
            configures, field names by default).
        exclude_none: Omit fields whose value is ``None``.
        strict: Validate in strict mode.
    """

    name = "pydantic"

    def __init__(self, *, by_alias: bool | None = None, exclude_none: bool = False, strict: bool | None = None) -> None:
        self.by_alias = by_alias
        self.exclude_none = exclude_none
        self.strict = strict
        self.content_type = JSON_CONTENT_TYPE

    def can_encode(self, value: object) -> bool:
        pydantic = _pydantic()
        return pydantic is not None and isinstance(value, pydantic.BaseModel)

    def encode(self, value: Any) -> EncodedBody:
        options = {} if self.by_alias is None else {"by_alias": self.by_alias}
        try:
            data = value.model_dump_json(exclude_none=self.exclude_none, **options)
        except Exception as error:
            raise EncodeError(f"cannot encode {type(value).__name__}: {error}") from error
        return EncodedBody(data.encode(), self.content_type)

    def can_decode(self, target: Any, content_type: str | None) -> bool:
        pydantic = _pydantic()
        if pydantic is None:
            return False
        if (
            content_type is not None
            and not is_json_content_type(content_type)
            and media_type(content_type) != "text/plain"
        ):
            return False
        if isinstance(target, type):
            if issubclass(target, pydantic.BaseModel):
                return True
            return typing.is_typeddict(target)
        return typing.get_origin(target) is not None

    def decode(self, data: bytes, target: Any, content_type: str | None) -> Any:
        import pydantic

        options = _names_and_aliases()
        try:
            if isinstance(target, type) and issubclass(target, pydantic.BaseModel):
                return target.model_validate_json(data, strict=self.strict, **options)
            return _adapter(target).validate_json(data, strict=self.strict, **options)
        except pydantic.ValidationError as error:
            raise DecodeError(f"validation failed for {getattr(target, '__name__', target)}: {error}") from error

    def type_name(self, target: Any) -> str | None:
        return None
