import sys
from abc import ABC, abstractmethod
from collections.abc import Iterable, Set
from typing import Any, ClassVar, Literal, TypeAlias, TypeVar

from pydantic import model_validator

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    Identifier,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

InstanceType: TypeAlias = Literal["node", "edge"]


class TypedInstanceIdentifier(Identifier):
    """Identifier for an Instance instance."""

    instance_type: InstanceType
    space: str
    external_id: str

    def __str__(self) -> str:
        return f"Instance({self.instance_type}, {self.space}, {self.external_id})"

    def dump(self, camel_case: bool = True, include_type: bool = True) -> dict[str, Any]:
        """Dump the resource to a dictionary.

        This is the default serialization method for request resources.
        """
        return self.model_dump(mode="json", by_alias=camel_case, exclude_unset=not include_type)


class TypedNodeIdentifier(TypedInstanceIdentifier):
    instance_type: Literal["node"] = "node"

    def __str__(self) -> str:
        return f"Node({self.space}, {self.external_id})"

    @classmethod
    def from_external_id(cls, item: ExternalId, space: str) -> Self:
        return cls(instance_type="node", space=space, external_id=item.external_id)

    @classmethod
    def from_external_ids(cls, items: Iterable[ExternalId], space: str) -> list[Self]:
        return [cls.from_external_id(item, space) for item in items]

    @classmethod
    def from_str_ids(cls, items: Iterable[str], space: str) -> list[Self]:
        return [cls(instance_type="node", space=space, external_id=item) for item in items]


class TypedEdgeIdentifier(TypedInstanceIdentifier):
    instance_type: Literal["edge"] = "edge"

    def __str__(self) -> str:
        return f"Edge({self.space}, {self.external_id})"


T_TypedInstanceIdentifier = TypeVar("T_TypedInstanceIdentifier", bound=TypedInstanceIdentifier)


class InstanceIdentifier(Identifier):
    space: str
    external_id: str

    def __str__(self) -> str:
        return f"Instance({self.space}, {self.external_id})"


class InstanceResult(BaseModelObject):
    instance_type: InstanceType
    version: int
    was_modified: bool
    space: str
    external_id: str
    created_time: int
    last_updated_time: int

    def as_id(self) -> TypedInstanceIdentifier:
        return TypedInstanceIdentifier(
            instance_type=self.instance_type,
            space=self.space,
            external_id=self.external_id,
        )


class TypedViewReference(Identifier):
    type: Literal["view"] = "view"
    space: str
    external_id: str
    version: str

    def __str__(self) -> str:
        return f"View({self.space}, {self.external_id}, v{self.version})"

    def dump(self, camel_case: bool = True, include_type: bool = True) -> dict[str, Any]:
        """Dump the resource to a dictionary.

        This is the default serialization method for request resources.
        """
        return self.model_dump(mode="json", by_alias=camel_case, exclude_unset=not include_type)

    def as_property_reference(self, property_name: str) -> list[str]:
        return [self.space, f"{self.external_id}/{self.version}", property_name]


######################################################
# The classes below are helper classes for making instances request/responses.
# By using these, we can avoid having to include the instances specific classes in the DTO classes
# that are instance. Instead, these classes can now only have the properties they need to define.
#######################################################


class WrappedInstanceRequest(RequestResource, ABC):
    """This is a base class for resources that are Instances.
    It is used to define resources that are
    """

    VIEW_ID: ClassVar[TypedViewReference]
    instance_type: InstanceType
    space: str
    external_id: str
    existing_version: int | None = None

    def dump(
        self,
        camel_case: bool = True,
        exclude_extra: bool = False,
        context: Literal["api", "toolkit"] = "api",
        exclude: Set[str] | None = None,
    ) -> dict[str, Any]:
        """Dump the resource to a dictionary.

        Args:
            camel_case (bool): Whether to use camelCase for the keys. Default is True.
            exclude_extra (bool): Whether to exclude extra fields not defined in the model. Default is False.
            context (Literal["api", "toolkit"]): The context in which the dump is used. Default is "api".

        """
        exclude_set = set(exclude or set())
        if exclude_extra:
            exclude_set |= set(self.__pydantic_extra__) if self.__pydantic_extra__ else set()
        if context == "api":
            exclude_set.update({"existing_version", "instance_type", "space", "external_id"})
        dumped = self.model_dump(mode="json", by_alias=camel_case, exclude_unset=True, exclude=exclude_set)
        if context == "toolkit":
            return dumped
        return {
            "instanceType": self.instance_type,
            "space": self.space,
            "externalId": self.external_id,
            "sources": [
                {
                    "source": self.VIEW_ID.dump(camel_case=camel_case, include_type=True),
                    "properties": dumped,
                }
            ],
        }


T_WrappedInstanceRequest = TypeVar("T_WrappedInstanceRequest", bound=WrappedInstanceRequest)


class WrappedInstanceResponse(ResponseResource[T_WrappedInstanceRequest], ABC):
    VIEW_ID: ClassVar[TypedViewReference]
    instance_type: InstanceType
    space: str
    external_id: str

    version: int
    created_time: int
    last_updated_time: int
    deleted_time: int | None = None

    @model_validator(mode="before")
    def move_properties(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Move properties from sources to the top level."""
        return move_properties(values, cls.VIEW_ID)

    def dump(
        self, camel_case: bool = True, exclude_extra: bool = False, context: Literal["api", "toolkit"] = "api"
    ) -> dict[str, Any]:
        """Dump the resource to a dictionary.

        Args:
            camel_case (bool): Whether to use camelCase for the keys. Default is True.
            exclude_extra (bool): Whether to exclude extra fields not defined in the model. Default is False.

        """
        output = super().dump(camel_case=camel_case, exclude_extra=exclude_extra)
        if context == "toolkit":
            return output
        properties: dict[str, Any] = {}
        for key in list(output.keys()):
            if key in {
                "instanceType",
                "space",
                "externalId",
                "version",
                "createdTime",
                "lastUpdatedTime",
                "deletedTime",
                "instance_type",
                "external_id",
                "created_time",
                "last_updated_time",
                "deleted_time",
            }:
                continue
            properties[key] = output.pop(key)
        output["properties"] = {
            self.VIEW_ID.space: {
                f"{self.VIEW_ID.external_id}/{self.VIEW_ID.version}": properties,
            }
        }
        return output


def move_properties(values: dict[str, Any], view_id: TypedViewReference) -> dict[str, Any]:
    """Help function to move properties from properties.space.externalId/version to the top level.

    It is used in WrappedInstanceResponse to move properties from the response to the top level.
    """
    if "properties" not in values:
        return values
    properties = values["properties"]
    if not isinstance(properties, dict) or view_id.space not in properties:
        return values
    view_properties = properties[view_id.space]
    identifier = f"{view_id.external_id}/{view_id.version}"
    if not isinstance(view_properties, dict) or identifier not in view_properties:
        return values
    source_properties = view_properties[identifier]
    return {**{key: value for key, value in values.items() if key != "properties"}, **source_properties}


T_WrappedInstanceResponse = TypeVar("T_WrappedInstanceResponse", bound=WrappedInstanceResponse)


class WrappedInstanceListRequest(RequestResource, ABC):
    VIEW_ID: ClassVar[TypedViewReference]
    instance_type: Literal["node"] = "node"
    space: str
    external_id: str

    @abstractmethod
    def dump_instances(self) -> list[dict[str, Any]]:
        """Dumps the object to a list of instance request dictionaries."""
        raise NotImplementedError()

    def as_id(self) -> TypedNodeIdentifier:
        return TypedNodeIdentifier(
            instance_type=self.instance_type,
            space=self.space,
            external_id=self.external_id,
        )

    @abstractmethod
    def as_ids(self) -> list[TypedInstanceIdentifier]:
        """Convert the response to a list of typed instance identifiers."""
        raise NotImplementedError()


T_InstancesListRequest = TypeVar("T_InstancesListRequest", bound=WrappedInstanceListRequest)


class WrappedInstanceListResponse(ResponseResource[T_InstancesListRequest], ABC):
    VIEW_ID: ClassVar[TypedViewReference]
    instance_type: Literal["node"] = "node"
    space: str
    external_id: str

    @model_validator(mode="before")
    @classmethod
    def move_properties(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Move properties from sources to the top level."""
        return move_properties(values, cls.VIEW_ID)

    @abstractmethod
    def as_ids(self) -> list[TypedInstanceIdentifier]:
        """Convert the response to a list of typed instance identifiers."""
        raise NotImplementedError()


T_InstancesListResponse = TypeVar("T_InstancesListResponse", bound=WrappedInstanceListResponse)
