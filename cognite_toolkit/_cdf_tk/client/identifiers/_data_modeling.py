from typing import Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import Identifier


class SpaceReference(Identifier):
    space: str

    def __str__(self) -> str:
        return self.space


class ContainerReference(Identifier):
    type: Literal["container"] = Field("container", exclude=True)
    space: str
    external_id: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}"

    def as_tuple(self) -> tuple[str, str]:
        return self.space, self.external_id


class ViewReferenceNoVersion(Identifier):
    type: Literal["view"] = Field("view", exclude=True)
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


class NodeReference(Identifier):
    space: str
    external_id: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}"


class EdgeReference(Identifier):
    space: str
    external_id: str

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


# Todo: Temporary put here to avoid circular imports.


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
