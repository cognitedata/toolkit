from typing import Annotated

from pydantic import Field, TypeAdapter

from ._asset_centric import AssetCentricFileSelector, AssetCentricSelector, AssetSubtreeSelector, DataSetSelector
from ._base import DataSelector
from ._canvas import CanvasSelector
from ._charts import AllChartsSelector, ChartOwnerSelector, ChartSelector
from ._instances import (
    InstanceFileSelector,
    InstanceSelector,
    InstanceSpaceSelector,
    InstanceViewSelector,
    SelectedView,
)
from ._raw import RawTableSelector, SelectedTable

Selector = Annotated[
    RawTableSelector
    | InstanceViewSelector
    | InstanceFileSelector
    | AllChartsSelector
    | ChartOwnerSelector
    | AssetSubtreeSelector
    | AssetCentricFileSelector
    | DataSetSelector
    | InstanceSpaceSelector,
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
    "InstanceSpaceSelector",
    "InstanceViewSelector",
    "RawTableSelector",
    "SelectedTable",
    "SelectedView",
    "Selector",
    "SelectorAdapter",
]
