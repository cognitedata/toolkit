from abc import abstractmethod
from typing import Any, TypeVar

from pydantic import BaseModel, TypeAdapter, ValidationInfo
from pydantic.alias_generators import to_camel
from pydantic.config import ExtraValues

from cognite_toolkit._cdf_tk.client._resource_base import Identifier

_EXTRA_FIELDS_CONTEXT_KEY = "toolkit_extra_fields"


class BaseModelResource(BaseModel, alias_generator=to_camel, extra="forbid"): ...


class ToolkitResource(BaseModelResource):
    @abstractmethod
    def as_id(self) -> Identifier:
        """Return an identifier for this resource."""
        raise NotImplementedError()


T_Resource = TypeVar("T_Resource", bound=ToolkitResource)
T_BaseModelResource = TypeVar("T_BaseModelResource", bound=BaseModelResource)


def validate_as(target: type[T_BaseModelResource], data: Any, info: ValidationInfo) -> T_BaseModelResource:
    """Validate ``data`` as ``target``, keeping the extra-field behavior of the ongoing validation.

    Resource classes that select a class based on the content of the file (``GroupYAML``, ``Capability``,
    ``Authentication``, ...) cannot use the wrap validator handler for this, as the handler is bound to the class
    the validation started on. They have to call ``model_validate`` on the selected class instead, which starts a
    fresh validation that does not inherit the ``extra`` argument given to the outer call. Going through this
    function carries that argument over, so a caller that asked for unrecognized fields to be ignored gets that
    behavior for the whole file and not just its top level.
    """
    return target.model_validate(data, extra=_extra_from_context(info.context), context=info.context)


def validate_ignoring_unknown_fields(model: type[T_BaseModelResource], data: dict[str, Any]) -> T_BaseModelResource:
    """Validate ``data`` as ``model``, accepting and discarding fields the model does not recognize."""
    return model.model_validate(data, extra="ignore", context={_EXTRA_FIELDS_CONTEXT_KEY: "ignore"})


def validate_list_ignoring_unknown_fields(
    adapter: TypeAdapter[list[T_BaseModelResource]], data: list[Any]
) -> list[T_BaseModelResource]:
    """List equivalent of :func:`validate_ignoring_unknown_fields`."""
    return adapter.validate_python(data, extra="ignore", context={_EXTRA_FIELDS_CONTEXT_KEY: "ignore"})


def _extra_from_context(context: Any) -> ExtraValues | None:
    if isinstance(context, dict):
        return context.get(_EXTRA_FIELDS_CONTEXT_KEY)
    return None
