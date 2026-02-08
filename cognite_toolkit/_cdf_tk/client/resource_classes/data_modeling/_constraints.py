from abc import ABC
from typing import Annotated, Any, Literal

from pydantic import Field, TypeAdapter, field_serializer, model_serializer
from pydantic_core.core_schema import FieldSerializationInfo, SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

from ._references import ContainerReference


class ConstraintDefinition(BaseModelObject, ABC):
    constraint_type: str

    @model_serializer(mode="wrap")  # type: ignore[type-var]
    def serialize(self, handler: SerializerFunctionWrapHandler, info: FieldSerializationInfo) -> dict[str, Any]:
        # Always serialize constraint_type, even if model_dump(exclude_unset=True)
        serialized = handler(self)
        return {"constraintType" if info.by_alias else "constraint_type": self.constraint_type, **serialized}


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
