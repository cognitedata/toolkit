import sys
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class BaseModelObject(BaseModel):
    """Base class for all object. This includes resources and nested objects."""

    # We allow extra fields to support forward compatibility.
    model_config = ConfigDict(alias_generator=to_camel, extra="allow")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the resource to a dictionary.

        This is the default serialization method for request resources.
        """
        return self.model_dump(mode="json", by_alias=camel_case)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> "Self":
        """Load method to match CogniteResource signature."""
        return cls.model_validate(resource)


class RequestResource(BaseModelObject): ...


T_RequestResource = TypeVar("T_RequestResource", bound=RequestResource)


class ResponseResource(BaseModelObject, Generic[T_RequestResource], ABC):
    @abstractmethod
    def as_request_resource(self) -> T_RequestResource:
        """Convert the response resource to a request resource."""
        ...


class Identifier(BaseModel):
    """Base class for all identifier classes."""

    model_config = ConfigDict(alias_generator=to_camel, extra="ignore", populate_by_name=True, frozen=True)

    def dump(self, include_type: bool = True) -> dict[str, Any]:
        """Dump the identifier to a dictionary.

        Args:
            include_type (bool): Whether to include the type of the identifier in the output.

        Returns:
            dict[str, Any]: The dumped identifier.
        """
        return self.model_dump(mode="json", by_alias=True, exclude_defaults=not include_type)

    def as_id(self) -> Self:
        return self
