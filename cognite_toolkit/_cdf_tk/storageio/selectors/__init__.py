from pathlib import Path
from typing import Annotated

from pydantic import Field, TypeAdapter, ValidationError

from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning, ToolkitWarning
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file
from cognite_toolkit._cdf_tk.validation import humanize_validation_error

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
from ._three_d import ThreeDModelFilteredSelector, ThreeDModelIdSelector, ThreeDSelector

Selector = Annotated[
    RawTableSelector
    | ThreeDModelIdSelector
    | ThreeDModelFilteredSelector
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

ALPHA_SELECTORS = {FileIdentifierSelector}
INTERNAL = {ThreeDModelIdSelector, ThreeDModelFilteredSelector}
SelectorAdapter: TypeAdapter[Selector] = TypeAdapter(Selector)


def load_selector(manifest_file: Path) -> Selector | ToolkitWarning:
    """Loads a selector from a manifest file.

    Args:
        manifest_file: Path to the manifest file.

    Returns:
        A selector object or a toolkit warning if loading fails or the selector is an alpha feature that is not enabled.
    """
    selector_dict = read_yaml_file(manifest_file, expected_output="dict")
    try:
        selector = SelectorAdapter.validate_python(selector_dict)
    except ValidationError as e:
        errors = humanize_validation_error(e)
        return ResourceFormatWarning(manifest_file, tuple(errors), text="Invalid selector in metadata file, skipping.")
    if not Flags.EXTEND_UPLOAD.is_enabled() and type(selector) in ALPHA_SELECTORS:
        return MediumSeverityWarning(
            f"Selector type '{type(selector).__name__}' in file '{manifest_file}' is in alpha. To enable it set the alpha flag 'extend-upload = true' in your CDF.toml file."
        )
    elif type(selector) in INTERNAL:
        return MediumSeverityWarning(
            f"Selector type '{type(selector).__name__}' in file '{manifest_file}' is for internal use only and cannot be used."
        )
    return selector


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
    "ThreeDModelIdSelector",
    "ThreeDSelector",
    "TimeSeriesColumn",
    "load_selector",
]
