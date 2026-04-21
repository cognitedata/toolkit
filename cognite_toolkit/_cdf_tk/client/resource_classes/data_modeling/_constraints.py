from abc import ABC
from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, ConfigDict, TypeAdapter, field_serializer
from pydantic_core.core_schema import FieldSerializationInfo

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId
from cognite_toolkit._cdf_tk.utils._auxiliary import registry_from_subclasses_with_type_field


class ConstraintDefinition(BaseModelObject, ABC):
    constraint_type: str


class UniquenessConstraintDefinition(ConstraintDefinition):
    constraint_type: Literal["uniqueness"] = "uniqueness"
    properties: list[str]
    by_space: bool | None = None


class RequiresConstraintDefinition(ConstraintDefinition):
    constraint_type: Literal["requires"] = "requires"
    require: ContainerId

    @field_serializer("require", mode="plain")
    @classmethod
    def serialize_require(cls, require: ContainerId, info: FieldSerializationInfo) -> dict[str, Any]:
        return {**require.model_dump(**vars(info)), "type": "container"}


class UnknownConstraintDefinition(ConstraintDefinition):
    constraint_type: str


def _handle_unknown_constraint(value: Any) -> Any:
    if isinstance(value, dict):
        constraint_type = value.get("constraintType")
        if constraint_type not in _CONSTRAINT_BY_TYPE:
            return UnknownConstraintDefinition.model_validate(value)
        return _CONSTRAINT_BY_TYPE[constraint_type].model_validate(value)
    return value


_CONSTRAINT_BY_TYPE = registry_from_subclasses_with_type_field(
    ConstraintDefinition,
    type_field="constraint_type",
    exclude=(UnknownConstraintDefinition,),
)


Constraint = Annotated[
    UniquenessConstraintDefinition | RequiresConstraintDefinition | UnknownConstraintDefinition,
    BeforeValidator(_handle_unknown_constraint),
]

ConstraintAdapter: TypeAdapter[Constraint] = TypeAdapter(Constraint)
