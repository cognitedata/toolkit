from typing import Annotated

from pydantic import Field, TypeAdapter

from ._asset_centric import AssetCentricFileSelector, AssetCentricSelector, AssetSubtreeSelector, DataSetSelector
from ._base import DataSelector
from ._canvas import CanvasSelector
from ._charts import AllChartsSelector, ChartExternalIdSelector, ChartOwnerSelector, ChartSelector
from ._datapoints import (
    DataPointsFileSelector,
    ExternalIdColumn,
    InstanceColumn,
    InternalIdColumn,
    TimeSeriesColumn,
)
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
    | InstanceSpaceSelector
    | AllChartsSelector
    | ChartOwnerSelector
    | AssetSubtreeSelector
    | AssetCentricFileSelector
    | DataSetSelector
    | DataPointsFileSelector
    | ChartExternalIdSelector,
    Field(discriminator="type"),
]

SelectorAdapter: TypeAdapter[Selector] = TypeAdapter(Selector)


__all__ = [
    "AllChartsSelector",
    "AssetCentricFileSelector",
    "AssetCentricSelector",
    "AssetSubtreeSelector",
    "CanvasSelector",
    "ChartExternalIdSelector",
    "ChartOwnerSelector",
    "ChartSelector",
    "DataPointsFileSelector",
    "DataSelector",
    "DataSetSelector",
    "ExternalIdColumn",
    "InstanceColumn",
    "InstanceFileSelector",
    "InstanceSelector",
    "InstanceSpaceSelector",
    "InstanceViewSelector",
    "InternalIdColumn",
    "RawTableSelector",
    "SelectedTable",
    "SelectedView",
    "Selector",
    "SelectorAdapter",
    "TimeSeriesColumn",
]
