from typing import Annotated

from pydantic import Field, TypeAdapter

from ._base import DataSelector
from ._instances import InstanceSelector, InstanceViewSelector
from ._raw import RawTableSelector

Selector = Annotated[
    RawTableSelector | InstanceViewSelector,
    Field(discriminator="type"),
]

SelectorAdapter: TypeAdapter[Selector] = TypeAdapter(Selector)


__all__ = [
    "DataSelector",
    "InstanceSelector",
    "InstanceViewSelector",
    "RawTableSelector",
    "Selector",
    "SelectorAdapter",
]
