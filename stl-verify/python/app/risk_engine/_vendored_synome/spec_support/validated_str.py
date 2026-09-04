import os
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from pydantic import GetCoreSchemaHandler

_VALIDATE = os.getenv("VALIDATION_DISABLED") != "1"


class ValidatedStr(str):
    """String subclass with optional validation.

    Subclass this and override ``_validate`` to enforce invariants:

        class EvmAddress(ValidatedStr):
            @classmethod
            def _validate(cls, value: str) -> None:
                validate_eip55(value)

    Set ``VALIDATION_DISABLED=1`` to skip validation, matching the behaviour of
    ``validated_dataclass``.
    """

    @classmethod
    def _validate(cls, value: str) -> None:
        """Raise ``ValueError`` if *value* does not satisfy the invariant."""

    def __new__(cls, value: str) -> Self:
        if _VALIDATE:
            cls._validate(value)
        return super().__new__(cls, value)

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: "GetCoreSchemaHandler") -> Any:
        from pydantic_core import core_schema

        return core_schema.no_info_plain_validator_function(
            cls,
            serialization=core_schema.plain_serializer_function_ser_schema(str),
        )
