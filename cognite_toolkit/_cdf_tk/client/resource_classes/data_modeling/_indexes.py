from abc import ABC
from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, ConfigDict, TypeAdapter

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.utils._auxiliary import dict_discriminator_value, registry_from_subclasses_with_type_field


class IndexDefinition(BaseModelObject, ABC):
    index_type: str
    properties: list[str]


class BtreeIndex(IndexDefinition):
    index_type: Literal["btree"] = "btree"
    by_space: bool | None = None
    cursorable: bool | None = None


class InvertedIndex(IndexDefinition):
    index_type: Literal["inverted"] = "inverted"


class UnknownIndexDefinition(IndexDefinition):
    model_config = ConfigDict(extra="allow")
    index_type: str


def _handle_unknown_index(value: Any) -> Any:
    if isinstance(value, dict):
        index_type = dict_discriminator_value(value, "index_type")
        if index_type not in _INDEX_BY_TYPE:
            return UnknownIndexDefinition.model_validate(value)
        return _INDEX_BY_TYPE[index_type].model_validate(value)
    return value


_INDEX_BY_TYPE = registry_from_subclasses_with_type_field(
    IndexDefinition,
    type_field="index_type",
    exclude=(UnknownIndexDefinition,),
)


Index = Annotated[BtreeIndex | InvertedIndex | UnknownIndexDefinition, BeforeValidator(_handle_unknown_index)]

IndexAdapter: TypeAdapter[Index] = TypeAdapter(Index)
