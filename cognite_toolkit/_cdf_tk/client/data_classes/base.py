import sys
from abc import ABC, abstractmethod
from collections import UserList
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.utils.http_client._data_classes2 import BaseModelObject, RequestResource

if TYPE_CHECKING:
    from cognite.client import CogniteClient

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


T_RequestResource = TypeVar("T_RequestResource", bound=RequestResource)


class ResponseResource(BaseModelObject, Generic[T_RequestResource], ABC):
    @abstractmethod
    def as_request_resource(self) -> T_RequestResource:
        """Convert the response resource to a request resource."""
        ...

    def as_write(self) -> T_RequestResource:
        """Alias for as_request_resource to match protocol signature."""
        return self.as_request_resource()


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


T_Resource = TypeVar("T_Resource", bound=RequestResource | ResponseResource)


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
