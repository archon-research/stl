import os
from dataclasses import dataclass
from typing import dataclass_transform

_VALIDATE = os.getenv("VALIDATION_DISABLED") != "1"


@dataclass_transform()
def validated_dataclass(cls=None, *, validators=(), **kwargs):
    """Dataclass decorator with pydantic validation enabled by default.

    ``validators`` is an iterable of callables that take the constructed
    instance and may raise ``ValueError`` for invalid combinations of
    fields. They run after Pydantic's per-field type validation and after
    the dataclass-generated ``__init__``. This is the AXS-compatible
    substitute for ``__post_init__``: cross-field validation lives in
    ``spec_support`` (which is exempt from the spec subset's
    no-methods-on-dataclasses rule) and is wired into the class via the
    decorator rather than as a method.

    The ``@dataclass_transform()`` decorator (PEP 681) tells type checkers
    this function produces a dataclass, so callers can use
    ``@validated_dataclass`` or ``@validated_dataclass(validators=[...])``
    and still get dataclass-style ``__init__`` inference.

    Set ``VALIDATION_DISABLED=1`` to skip validation (e.g. in
    performance-critical paths)."""

    def wrap(cls):
        cls = dataclass(cls, **kwargs)

        if _VALIDATE:
            from pydantic import TypeAdapter

            adapter = TypeAdapter(cls)
            original_init = cls.__init__

            def _validated_init(self, **data):
                adapter.validate_python(data)
                original_init(self, **data)
                for validator in validators:
                    validator(self)

            cls.__init__ = _validated_init

        return cls

    if cls is None:
        return wrap
    return wrap(cls)
