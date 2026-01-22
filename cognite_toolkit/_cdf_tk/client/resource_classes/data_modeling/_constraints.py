from abc import ABC
from typing import Annotated, Any, Literal

from pydantic import Field, TypeAdapter, field_serializer
from pydantic_core.core_schema import FieldSerializationInfo

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

from ._references import ContainerReference


class ConstraintDefinition(BaseModelObject, ABC):
    constraint_type: str


class UniquenessConstraintDefinition(ConstraintDefinition):
    constraint_type: Literal["uniqueness"] = "uniqueness"
    properties: list[str]
    by_space: bool | None = None


class RequiresConstraintDefinition(ConstraintDefinition):
    constraint_type: Literal["requires"] = "requires"
    require: ContainerReference

    @field_serializer("require", mode="plain")
    @classmethod
    def serialize_require(cls, require: ContainerReference, info: FieldSerializationInfo) -> dict[str, Any]:
        return {**require.model_dump(**vars(info)), "type": "container"}


Constraint = Annotated[
    UniquenessConstraintDefinition | RequiresConstraintDefinition,
    Field(discriminator="constraint_type"),
]

ConstraintAdapter: TypeAdapter[Constraint] = TypeAdapter(Constraint)
