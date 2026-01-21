from abc import ABC
from typing import Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource

from ._constraints import Constraint
from ._data_types import DataType
from ._indexes import Index
from ._references import ContainerReference


class ContainerPropertyDefinition(BaseModelObject):
    immutable: bool | None = None
    nullable: bool | None = None
    auto_increment: bool | None = None
    default_value: str | int | float | bool | dict[str, JsonValue] | None = None
    description: str | None = None
    name: str | None = None
    type: DataType


class Container(BaseModelObject, ABC):
    space: str
    external_id: str
    name: str | None = None
    description: str | None = None
    used_for: Literal["node", "edge", "all"] | None = None
    properties: dict[str, ContainerPropertyDefinition]
    constraints: dict[str, Constraint] | None = None
    indexes: dict[str, Index] | None = None

    def as_id(self) -> ContainerReference:
        return ContainerReference(
            space=self.space,
            external_id=self.external_id,
        )


class ContainerRequest(Container, RequestResource): ...


class ContainerResponse(Container, ResponseResource[ContainerRequest]):
    created_time: int
    last_updated_time: int
    is_global: bool

    def as_request_resource(self) -> "ContainerRequest":
        return ContainerRequest.model_validate(self.model_dump(by_alias=True))
