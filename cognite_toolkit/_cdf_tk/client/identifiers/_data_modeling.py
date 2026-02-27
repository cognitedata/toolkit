from abc import ABC
from typing import Annotated, Any, Literal

from pydantic import PlainSerializer

from cognite_toolkit._cdf_tk.client._resource_base import Identifier


class SpaceReference(Identifier):
    space: str

    def __str__(self) -> str:
        return self.space


class DataModelingIdentifier(Identifier, ABC):
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


class ContainerReference(DataModelingIdentifier):
    type: Literal["container"] = "container"
    space: str
    external_id: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}"

    def as_tuple(self) -> tuple[str, str]:
        return self.space, self.external_id


class ViewReferenceNoVersion(DataModelingIdentifier):
    type: Literal["view"] = "view"
    space: str
    external_id: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}"


class ViewReference(ViewReferenceNoVersion):
    version: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}(version={self.version})"


class DataModelReferenceNoVersion(Identifier):
    space: str
    external_id: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}"


class DataModelReference(DataModelReferenceNoVersion):
    version: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}(version={self.version})"


class InstanceIdDefinition(Identifier):
    instance_type: str
    space: str
    external_id: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}"

    def dump(self, camel_case: bool = True, exclude_extra: bool = False, include_instance_type: bool = True) -> dict:
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


class NodeReference(InstanceIdDefinition):
    instance_type: Literal["node"] = "node"

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}"


def _dump_no_type(instance: NodeReference) -> dict[str, Any]:
    return instance.dump(include_instance_type=False)


# We use an annotated type here to ensure that NodeReference(...) == NodeReferenceUntyped(...) but that
# the serialization is different.
NodeReferenceUntyped = Annotated[NodeReference, PlainSerializer(_dump_no_type, when_used="always")]


class EdgeReference(InstanceIdDefinition):
    instance_type: Literal["edge"] = "edge"

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}"


class ContainerDirectReference(Identifier):
    source: ContainerReference
    identifier: str

    def __str__(self) -> str:
        return f"{self.source!s}.{self.identifier}"


class ViewDirectReference(Identifier):
    source: ViewReference
    identifier: str

    def __str__(self) -> str:
        return f"{self.source!s}.{self.identifier}"


class ContainerIndexReference(ContainerReference):
    identifier: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}(index={self.identifier})"


class ContainerConstraintReference(ContainerReference):
    identifier: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}(constraint={self.identifier})"


class DatapointSubscriptionTimeSeriesId(Identifier):
    external_id: str | None = None
    id: int | None = None
    instance_id: NodeReference | None = None

    def __str__(self) -> str:
        if self.external_id is not None:
            return f"externalId='{self.external_id}'"
        elif self.id is not None:
            return f"id={self.id}"
        elif self.instance_id is not None:
            return f"instanceId={self.instance_id!s}"
        else:
            return "undefined"


class InstanceId(Identifier):
    instance_id: NodeReference

    def __str__(self) -> str:
        return f"instanceId='{self.instance_id}'"

    def dump(self, camel_case: bool = True, exclude_extra: bool = False) -> dict[str, Any]:
        return {
            "instanceId" if camel_case else "instance_id": self.instance_id.dump(
                camel_case=camel_case, exclude_extra=exclude_extra, include_instance_type=False
            )
        }
