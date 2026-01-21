"""Base classes for Cognite Client resources and identifiers."""

import sys
import types
from abc import ABC, abstractmethod
from collections import UserList
from typing import Any, ClassVar, Generic, Literal, TypeVar, Union, get_args, get_origin

from cognite.client import CogniteClient
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class BaseModelObject(BaseModel):
    """Base class for all object. This includes resources and nested objects."""

    # We allow extra fields to support forward compatibility.
    model_config = ConfigDict(alias_generator=to_camel, extra="allow", populate_by_name=True)

    def dump(self, camel_case: bool = True, exclude_extra: bool = False) -> dict[str, Any]:
        """Dump the resource to a dictionary.

        Args:
            camel_case (bool): Whether to use camelCase for the keys. Default is True.
            exclude_extra (bool): Whether to exclude extra fields not defined in the model. Default is False.

        """
        if exclude_extra:
            return self.model_dump(
                mode="json",
                by_alias=camel_case,
                exclude_unset=True,
                exclude=set(self.__pydantic_extra__) if self.__pydantic_extra__ else None,
            )
        return self.model_dump(mode="json", by_alias=camel_case, exclude_unset=True)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        """Load method to match CogniteResource signature."""
        return cls.model_validate(resource, by_alias=True)


class RequestItem(BaseModelObject, ABC):
    """A request item is any object that can be sent to the CDF API as part of a request."""

    def __str__(self) -> str:
        """All request items must implement a string representation.

        This is used to identify the item in error messages and logs.
        """
        raise NotImplementedError()


T_RequestItem = TypeVar("T_RequestItem", bound=RequestItem)


class Identifier(RequestItem):
    """Base class for all identifier objects typically
    {"externalId": "..."}, {"id": ...}, {"space": "...", "externalId: "..."}."""

    model_config = ConfigDict(alias_generator=to_camel, extra="ignore", frozen=True)

    def dump(self, camel_case: bool = True, exclude_extra: bool = False) -> dict[str, Any]:
        """Dump the resource to a dictionary.

        This is the default serialization method for request resources.
        """
        return self.model_dump(mode="json", by_alias=camel_case, exclude_unset=True)


T_Identifier = TypeVar("T_Identifier", bound=Identifier)


class RequestResource(RequestItem, ABC):
    @abstractmethod
    def as_id(self) -> Identifier:
        raise NotImplementedError()

    def __str__(self) -> str:
        return str(self.as_id())


T_RequestResource = TypeVar("T_RequestResource", bound=RequestResource)


class UpdatableRequestResource(RequestResource, ABC):
    container_fields: ClassVar[frozenset[str]] = frozenset()
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset()

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        """Convert the request resource to an update item."""
        update_item = self.as_id().dump(camel_case=True)
        update: dict[str, Any] = {}
        field_by_name = {info.alias or field_id: (field_id, info) for field_id, info in type(self).model_fields.items()}
        # When mode is "patch", we only include fields that are set
        exclude_unset = mode == "patch"
        for key, value in self.model_dump(mode="json", by_alias=True, exclude_unset=exclude_unset).items():
            if key in update_item:
                # Skip identifier fields
                continue
            if key not in field_by_name:
                # Skip unknown fields
                continue
            field_id, info = field_by_name[key]
            if field_id in self.container_fields:
                if mode == "patch":
                    update[key] = {"add": value}
                elif mode == "replace":
                    if value is None:
                        origin = _get_annotation_origin(info.annotation)
                        if origin is list:
                            update[key] = {"set": []}
                        elif origin is dict:
                            update[key] = {"set": {}}
                        else:
                            raise NotImplementedError(
                                f'Cannot replace container field "{key}" with None when its type is unknown.'
                            )
                    else:
                        update[key] = {"set": value}
                else:
                    raise NotImplementedError(f'Update mode "{mode}" is not supported for container fields.')
            elif value is None:
                if field_id not in self.non_nullable_fields:
                    update[key] = {"setNull": True}
            else:
                update[key] = {"set": value}
        update_item["update"] = update
        return update_item


def _get_annotation_origin(field_type: Any) -> Any:
    origin = get_origin(field_type)
    args = get_args(field_type)

    # Check for Union type (both typing.Union and | syntax from Python 3.10+)
    is_union = origin is Union or isinstance(field_type, getattr(types, "UnionType", ()))

    if is_union:
        # Handle Optional[T] by filtering out NoneType
        none_types = (type(None), types.NoneType)
        non_none_args = [arg for arg in args if arg not in none_types]
        if len(non_none_args) == 1:
            field_type = non_none_args[0]
            origin = get_origin(field_type) or field_type
    return origin


class ResponseResource(BaseModelObject, Generic[T_RequestResource], ABC):
    @abstractmethod
    def as_request_resource(self) -> T_RequestResource:
        """Convert the response resource to a request resource."""
        raise NotImplementedError()

    # Todo remove when CogniteClient data classes are completely removed from the codebase
    # and we only use the pydantic resource classes instead.from
    def as_write(self) -> T_RequestResource:
        """Alias for as_request_resource to match protocol signature."""
        return self.as_request_resource()


T_ResponseResource = TypeVar("T_ResponseResource", bound=ResponseResource)

# Todo: Delete this class and use list[T_Resource] directly
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
