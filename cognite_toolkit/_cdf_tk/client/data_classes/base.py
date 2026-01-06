import sys
from abc import ABC, abstractmethod
from collections import UserList
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

if TYPE_CHECKING:
    from cognite.client import CogniteClient

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
        return self.model_dump(mode="json", by_alias=camel_case, exclude_unset=True)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        """Load method to match CogniteResource signature."""
        return cls.model_validate(resource)


class Identifier(BaseModelObject):
    """Base class for all identifier objects typically
    {"externalId": "..."}, {"id": ...}, {"space": "...", "externalId: }."""

    model_config = ConfigDict(alias_generator=to_camel, extra="ignore", populate_by_name=True, frozen=True)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the resource to a dictionary.

        This is the default serialization method for request resources.
        """
        return self.model_dump(mode="json", by_alias=camel_case, exclude_unset=True)


T_Identifier = TypeVar("T_Identifier", bound=Identifier)


class RequestResource(BaseModelObject, ABC):
    @abstractmethod
    def as_id(self) -> Identifier:
        raise NotImplementedError()

    def __str__(self) -> str:
        raise NotImplementedError()


T_RequestResource = TypeVar("T_RequestResource", bound=RequestResource)


class ResponseResource(BaseModelObject, Generic[T_RequestResource], ABC):
    @abstractmethod
    def as_request_resource(self) -> T_RequestResource:
        """Convert the response resource to a request resource."""
        raise NotImplementedError()

    def as_write(self) -> T_RequestResource:
        """Alias for as_request_resource to match protocol signature."""
        return self.as_request_resource()


T_ResponseResource = TypeVar("T_ResponseResource", bound=ResponseResource)

T_Resource = TypeVar("T_Resource", bound=RequestResource | ResponseResource)


# Todo: Delete this class and use list[T_Resource] directly
class BaseResourceList(UserList[T_Resource]):
    """Base class for resource lists."""

    _RESOURCE: type[T_Resource]

    def __init__(self, initlist: list[T_Resource] | None = None, **_: Any) -> None:
        super().__init__(initlist or [])

    def dump(self, camel_case: bool = True) -> list[dict[str, Any]]:
        return [item.dump(camel_case) for item in self.data]

    @classmethod
    def load(cls, data: list[dict[str, Any]], cognite_client: "CogniteClient | None" = None) -> Self:
        items = [cls._RESOURCE.model_validate(item) for item in data]
        return cls(items)  # type: ignore[arg-type]
