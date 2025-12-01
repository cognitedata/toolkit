from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses

from ._annotations import AnnotationIO
from ._applications import CanvasIO, ChartIO
from ._asset_centric import (
    AssetCentricIO,
    AssetIO,
    EventIO,
    FileMetadataIO,
    HierarchyIO,
    TimeSeriesIO,
)
from ._base import (
    ConfigurableStorageIO,
    Page,
    StorageIO,
    StorageIOConfig,
    T_Selector,
    TableStorageIO,
    UploadableStorageIO,
    UploadItem,
)
from ._data_classes import InstanceIdCSVList, InstanceIdRow, ModelList
from ._datapoints import DatapointsIO
from ._file_content import FileContentIO
from ._instances import InstanceIO
from ._raw import RawIO
from .selectors._base import DataSelector

# MyPy fails to recognize that get_concrete_subclasses always returns a list of concrete subclasses.
STORAGE_IO_CLASSES = get_concrete_subclasses(StorageIO)  # type: ignore[type-abstract]
UPLOAD_IO_CLASSES = get_concrete_subclasses(UploadableStorageIO)  # type: ignore[type-abstract]


def get_upload_io(selector: DataSelector) -> type[UploadableStorageIO]:
    """Get the appropriate UploadableStorageIO class based on the type of the provided selector."""
    for cls in UPLOAD_IO_CLASSES:
        if isinstance(selector, cls.BASE_SELECTOR) and selector.kind == cls.KIND:
            return cls
    raise ValueError(f"No UploadableStorageIO found for selector of type {type(selector).__name__}")


__all__ = [
    "AnnotationIO",
    "AssetCentricIO",
    "AssetIO",
    "CanvasIO",
    "ChartIO",
    "ConfigurableStorageIO",
    "DatapointsIO",
    "EventIO",
    "FileContentIO",
    "FileMetadataIO",
    "HierarchyIO",
    "InstanceIO",
    "InstanceIdCSVList",
    "InstanceIdRow",
    "ModelList",
    "Page",
    "RawIO",
    "StorageIO",
    "StorageIOConfig",
    "T_Selector",
    "TableStorageIO",
    "TimeSeriesIO",
    "UploadItem",
    "UploadableStorageIO",
    "get_upload_io",
]
