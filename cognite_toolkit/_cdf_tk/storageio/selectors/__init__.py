from typing import Annotated

from pydantic import Field, TypeAdapter

from ._asset_centric import AssetCentricFileSelector, AssetCentricSelector, AssetSubtreeSelector, DataSetSelector
from ._base import DataSelector
from ._canvas import CanvasExternalIdSelector, CanvasSelector
from ._charts import AllChartsSelector, ChartExternalIdSelector, ChartOwnerSelector, ChartSelector
from ._datapoints import (
    DataPointsDataSetSelector,
    DataPointsFileSelector,
    DataPointsSelector,
    ExternalIdColumn,
    InstanceColumn,
    InternalIdColumn,
    TimeSeriesColumn,
)
from ._file_content import (
    FileContentSelector,
    FileDataModelingTemplate,
    FileDataModelingTemplateSelector,
    FileIdentifierSelector,
    FileMetadataTemplate,
    FileMetadataTemplateSelector,
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
    | DataPointsDataSetSelector
    | DataPointsFileSelector
    | ChartExternalIdSelector
    | CanvasExternalIdSelector
    | FileMetadataTemplateSelector
    | FileDataModelingTemplateSelector
    | FileIdentifierSelector,
    Field(discriminator="type"),
]

SelectorAdapter: TypeAdapter[Selector] = TypeAdapter(Selector)


__all__ = [
    "AllChartsSelector",
    "AssetCentricFileSelector",
    "AssetCentricSelector",
    "AssetSubtreeSelector",
    "CanvasExternalIdSelector",
    "CanvasSelector",
    "ChartExternalIdSelector",
    "ChartOwnerSelector",
    "ChartSelector",
    "DataPointsDataSetSelector",
    "DataPointsFileSelector",
    "DataPointsSelector",
    "DataSelector",
    "DataSetSelector",
    "ExternalIdColumn",
    "FileContentSelector",
    "FileDataModelingTemplate",
    "FileDataModelingTemplateSelector",
    "FileIdentifierSelector",
    "FileMetadataTemplate",
    "FileMetadataTemplateSelector",
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
