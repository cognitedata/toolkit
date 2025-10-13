from typing import Annotated

from pydantic import Field, TypeAdapter

from ._asset_centric import AssetCentricFileSelector, AssetCentricSelector, AssetSubtreeSelector, DataSetSelector
from ._base import DataSelector
from ._canvas import CanvasSelector
from ._charts import AllChartsSelector, ChartOwnerSelector, ChartSelector
from ._instances import InstanceFileSelector, InstanceSelector, InstanceViewSelector
from ._raw import RawTableSelector

Selector = Annotated[
    RawTableSelector
    | InstanceViewSelector
    | InstanceFileSelector
    | AllChartsSelector
    | ChartOwnerSelector
    | AssetSubtreeSelector
    | AssetCentricFileSelector
    | DataSetSelector,
    Field(discriminator="type"),
]

SelectorAdapter: TypeAdapter[Selector] = TypeAdapter(Selector)


__all__ = [
    "AllChartsSelector",
    "AssetCentricFileSelector",
    "AssetCentricSelector",
    "AssetSubtreeSelector",
    "CanvasSelector",
    "ChartOwnerSelector",
    "ChartSelector",
    "DataSelector",
    "DataSetSelector",
    "InstanceFileSelector",
    "InstanceSelector",
    "InstanceViewSelector",
    "RawTableSelector",
    "Selector",
    "SelectorAdapter",
]
