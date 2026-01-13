from typing import Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import Identifier


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


class ViewReference(Identifier):
    type: Literal["view"] = Field("view", exclude=True)
    space: str
    external_id: str
    version: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}(version={self.version})"


class DataModelReference(Identifier):
    space: str
    external_id: str
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
