from typing import Annotated

from pydantic import Field, TypeAdapter

from ._asset_centric import AssetCentricSelector, AssetSubtreeSelector, DataSetSelector
from ._base import DataSelector
from ._charts import AllChartsSelector, ChartOwnerSelector
from ._instances import InstanceFileSelector, InstanceSelector, InstanceViewSelector
from ._raw import RawTableSelector

Selector = Annotated[
    RawTableSelector
    | InstanceViewSelector
    | InstanceFileSelector
    | AllChartsSelector
    | ChartOwnerSelector
    | AssetSubtreeSelector
    | DataSetSelector,
    Field(discriminator="type"),
]

SelectorAdapter: TypeAdapter[Selector] = TypeAdapter(Selector)


__all__ = [
    "AllChartsSelector",
    "AssetCentricSelector",
    "ChartOwnerSelector",
    "DataSelector",
    "DataSetSelector",
    "InstanceFileSelector",
    "InstanceSelector",
    "InstanceViewSelector",
    "RawTableSelector",
    "Selector",
    "SelectorAdapter",
]
