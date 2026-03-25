import sys
from abc import ABC
from collections.abc import Iterable
from typing import Annotated, Any, Literal, TypeVar

from pydantic import PlainSerializer, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers._identifiers import ExternalId

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class SpaceId(Identifier):
    space: str

    def __str__(self) -> str:
        return self.space

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"space-{self.space}"
        return self.space


class DataModelingId(Identifier, ABC):
    type: str

    def dump(self, camel_case: bool = True, exclude_extra: bool = False, include_type: bool = False) -> dict[str, Any]:
        """Dumps the identifier to a dictionary.

        Args:
            camel_case: Whether to use camelCase for the keys. Defaults to True.
            exclude_extra: Whether to exclude extra fields that are not part of the API payload. Defaults to False.
            include_type: Whether to include the 'type' field in the output. Defaults to True.

        Returns:
            A dictionary representation of the identifier.
        """
        exclude: set[str] | None = None
        if not include_type:
            exclude = {"type"}
        return self.model_dump(mode="json", by_alias=camel_case, exclude=exclude)


class ContainerId(DataModelingId):
    type: Literal["container"] = "container"
    space: str
    external_id: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"space-{self.space}.externalId-{self.external_id}"
        return f"{self.space}.{self.external_id}"

    def as_tuple(self) -> tuple[str, str]:
        return self.space, self.external_id


def _dump_container_reference_untyped(instance: ContainerId) -> dict[str, Any]:
    return instance.dump(include_type=False)


ContainerReferenceUntyped = Annotated[
    ContainerId, PlainSerializer(_dump_container_reference_untyped, when_used="always")
]


class ViewNoVersionId(DataModelingId):
    type: Literal["view"] = "view"
    space: str
    external_id: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"space-{self.space}-externalId-{self.external_id}"
        return f"{self.space}.{self.external_id}"


class ViewId(ViewNoVersionId):
    version: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}(version={self.version})"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"space-{self.space}-externalId.{self.external_id}version-{self.version}"
        return f"{self.space}.{self.external_id}.{self.version}"

    def as_property_reference(self, property_id: str) -> list[str]:
        return [self.space, f"{self.external_id}/{self.version}", property_id]


def _dump_view_reference_untyped(instance: ViewNoVersionId) -> dict[str, Any]:
    return instance.dump(include_type=False)


ViewNoVersionUntypedId = Annotated[ViewNoVersionId, PlainSerializer(_dump_view_reference_untyped, when_used="always")]
ViewUntypedId = Annotated[ViewId, PlainSerializer(_dump_view_reference_untyped, when_used="always")]


class DataModelNoVersionId(Identifier):
    space: str
    external_id: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"space-{self.space}.externalId-{self.external_id}"
        return f"{self.space}.{self.external_id}"


class DataModelId(DataModelNoVersionId):
    version: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}(version={self.version})"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"space-{self.space}.externalId-{self.external_id}.version-{self.version}"
        return f"{self.space}.{self.external_id}.{self.version}"


class InstanceDefinitionId(Identifier):
    instance_type: str
    space: str
    external_id: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"space-{self.space}.externalId-{self.external_id}"
        return f"{self.space}.{self.external_id}"

    def dump(
        self, camel_case: bool = True, exclude_extra: bool = False, include_instance_type: bool = True
    ) -> dict[str, Any]:
        """Dumps the identifier to a dictionary.

        Args:
            camel_case: Whether to use camelCase for the keys. Defaults to True.
            exclude_extra: Whether to exclude extra fields that are not part of the API payload. Defaults to False.
            include_instance_type: Whether to include the 'instance_type' field in the output. Defaults to True.

        Returns:
            A dictionary representation of the identifier.
        """
        exclude: set[str] | None = None
        if not include_instance_type:
            exclude = {"instance_type"}
        return self.model_dump(mode="json", by_alias=camel_case, exclude=exclude)


class NodeId(InstanceDefinitionId):
    instance_type: Literal["node"] = "node"

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}"

    @classmethod
    def from_external_id(cls, item: ExternalId, space: str) -> Self:
        return cls(space=space, external_id=item.external_id)

    @classmethod
    def from_external_ids(cls, items: Iterable[ExternalId], space: str) -> list[Self]:
        return [cls.from_external_id(item, space) for item in items]

    @classmethod
    def from_str_ids(cls, str_ids: Iterable[str], space: str) -> list[Self]:
        return [cls(space=space, external_id=str_id) for str_id in str_ids]


class EdgeId(InstanceDefinitionId):
    instance_type: Literal["edge"] = "edge"

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}"


def _dump_no_type(instance: NodeId | EdgeId) -> dict[str, Any]:
    return instance.dump(include_instance_type=False)


NodeUntypedId = Annotated[NodeId, PlainSerializer(_dump_no_type, when_used="always")]


EdgeUntypedId = Annotated[EdgeId, PlainSerializer(_dump_no_type, when_used="always")]

T_InstanceId = TypeVar("T_InstanceId", bound=InstanceDefinitionId)


class EdgeTypeId(Identifier):
    """This is used to identify edges used in a schema.

    For example, if you have a view with a 'multi_edge_connection' property, then this identifier can be used
    to find all edges that are connected to the nodes in that view.
    """

    type: NodeUntypedId
    direction: Literal["outwards", "inwards"]

    def __str__(self) -> str:
        return f"{self.type!s}(direction={self.direction})"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"type-{self.type._as_filename(False)}.direction-{self.direction}"
        return f"{self.type._as_filename(False)}.{self.direction}"

    @model_validator(mode="before")
    def parse_str(cls, value: Any) -> Any:
        if not isinstance(value, str):
            return value
        # Expected format: "space:externalId(direction=outwards)"
        try:
            main_part, direction_part = value.split("(direction=")
            direction = direction_part.rstrip(")")
            space, external_id = main_part.split(":")
            return {"type": {"space": space, "external_id": external_id}, "direction": direction}
        except ValueError as e:
            raise ValueError(
                f"Invalid format for EdgeTypeId: {value}. Expected space:externalId(direction=outwards)"
            ) from e


class ContainerDirectId(Identifier):
    source: ContainerId
    identifier: str

    def __str__(self) -> str:
        return f"{self.source!s}.{self.identifier}"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"source-{self.source._as_filename(False)}.identifier-{self.identifier}"
        return f"{self.source._as_filename(False)}.{self.identifier}"


class ViewDirectId(Identifier):
    source: ViewId
    identifier: str

    def __str__(self) -> str:
        return f"{self.source!s}.{self.identifier}"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"source-{self.source._as_filename(False)}.identifier-{self.identifier}"
        return f"{self.source._as_filename(False)}.{self.identifier}"


class ContainerIndexId(ContainerId):
    identifier: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}(index={self.identifier})"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"{super()._as_filename(include_type)}.identifier-{self.identifier}"
        return f"{super()._as_filename(include_type)}.{self.identifier}"


class ContainerConstraintId(ContainerId):
    identifier: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}(constraint={self.identifier})"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"{super()._as_filename(include_type)}.identifier-{self.identifier}"
        return f"{super()._as_filename(include_type)}.{self.identifier}"


class DatapointSubscriptionTimeSeriesId(Identifier):
    external_id: str | None = None
    id: int | None = None
    instance_id: NodeUntypedId | None = None

    def __str__(self) -> str:
        if self.external_id is not None:
            return f"externalId='{self.external_id}'"
        elif self.id is not None:
            return f"id={self.id}"
        elif self.instance_id is not None:
            return f"instanceId={self.instance_id!s}"
        else:
            return "undefined"

    def _as_filename(self, include_type: bool = False) -> str:
        if self.external_id is not None:
            if include_type:
                return f"externalId-{self.external_id}"
            return self.external_id
        elif self.id is not None:
            if include_type:
                return f"id-{self.id}"
            return str(self.id)
        elif self.instance_id is not None:
            if include_type:
                return f"instanceId-{self.instance_id._as_filename(False)}"
            return f"{self.instance_id._as_filename(False)}"
        else:
            return "undefined"


class InstanceId(Identifier):
    """This is an instance identifier. It is used in the classic timeseries/files API
    to reference a CogniteTimeSeries/CogniteFile by its instanceId.
    """

    instance_id: NodeUntypedId

    def __str__(self) -> str:
        return f"instanceId='{self.instance_id}'"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"instanceId-{self.instance_id._as_filename(False)}"
        return f"{self.instance_id._as_filename(False)}"
