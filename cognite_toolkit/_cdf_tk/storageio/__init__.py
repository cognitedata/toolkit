from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses

from ._applications import CanvasIO, ChartIO
from ._asset_centric import AssetIO, BaseAssetCentricIO, EventIO, FileMetadataIO, TimeSeriesIO
from ._base import ConfigurableStorageIO, StorageIO, StorageIOConfig, T_Selector, TableStorageIO
from ._data_classes import InstanceIdCSVList, InstanceIdRow, ModelList
from ._instances import InstanceIO
from ._raw import RawIO
from .selectors._base import DataSelector

# MyPy fails to recognize that get_concrete_subclasses always returns a list of concrete subclasses.
STORAGE_IO_CLASSES = get_concrete_subclasses(StorageIO)  # type: ignore[type-abstract]


def get_storage_io(selector: DataSelector, kind: str) -> type[StorageIO]:
    """Get the appropriate StorageIO class based on the type of the provided selector."""
    for cls in STORAGE_IO_CLASSES:
        if issubclass(type(selector), cls.BASE_SELECTOR) and cls.KIND == kind:
            return cls
    raise ValueError(f"No StorageIO found for selector of type {type(selector)}")


__all__ = [
    "AssetIO",
    "BaseAssetCentricIO",
    "CanvasIO",
    "ChartIO",
    "ConfigurableStorageIO",
    "EventIO",
    "FileMetadataIO",
    "InstanceIO",
    "InstanceIdCSVList",
    "InstanceIdRow",
    "ModelList",
    "RawIO",
    "StorageIO",
    "StorageIOConfig",
    "T_Selector",
    "TableStorageIO",
    "TimeSeriesIO",
    "get_storage_io",
]
