from abc import ABC
from typing import Annotated, Any, Literal

from pydantic import Field, TypeAdapter, model_serializer
from pydantic_core.core_schema import FieldSerializationInfo, SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject


class IndexDefinition(BaseModelObject, ABC):
    index_type: str
    properties: list[str]

    @model_serializer(mode="wrap")  # type: ignore[type-var]
    def serialize(self, handler: SerializerFunctionWrapHandler, info: FieldSerializationInfo) -> dict[str, Any]:
        # Always serialize as {"indexType": self.index_type}, even if model_dump(exclude_unset=True)
        serialized = handler(self)
        return {"indexType" if info.by_alias else "index_type": self.index_type, **serialized}


class BtreeIndex(IndexDefinition):
    index_type: Literal["btree"] = "btree"
    by_space: bool | None = None
    cursorable: bool | None = None


class InvertedIndex(IndexDefinition):
    index_type: Literal["inverted"] = "inverted"


Index = Annotated[BtreeIndex | InvertedIndex, Field(discriminator="index_type")]

IndexAdapter: TypeAdapter[Index] = TypeAdapter(Index)
