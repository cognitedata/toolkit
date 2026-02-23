from abc import ABC
from typing import Annotated, Literal

from pydantic import Field, TypeAdapter

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject


class IndexDefinition(BaseModelObject, ABC):
    index_type: str
    properties: list[str]


class BtreeIndex(IndexDefinition):
    index_type: Literal["btree"] = "btree"
    by_space: bool | None = None
    cursorable: bool | None = None


class InvertedIndex(IndexDefinition):
    index_type: Literal["inverted"] = "inverted"


Index = Annotated[BtreeIndex | InvertedIndex, Field(discriminator="index_type")]

IndexAdapter: TypeAdapter[Index] = TypeAdapter(Index)
