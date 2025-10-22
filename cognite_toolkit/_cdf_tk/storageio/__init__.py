from pathlib import Path

from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.utils.fileio import COMPRESSION_BY_SUFFIX

from ._applications import CanvasIO, ChartIO
from ._asset_centric import AssetIO, BaseAssetCentricIO, EventIO, FileMetadataIO, TimeSeriesIO
from ._base import ConfigurableStorageIO, StorageIO, StorageIOConfig, T_Selector, TableStorageIO, UploadableStorageIO
from ._data_classes import InstanceIdCSVList, InstanceIdRow, ModelList
from ._instances import InstanceIO
from ._raw import RawIO
from .selectors._base import DataSelector

# MyPy fails to recognize that get_concrete_subclasses always returns a list of concrete subclasses.
STORAGE_IO_CLASSES = get_concrete_subclasses(StorageIO)  # type: ignore[type-abstract]
UPLOAD_IO_CLASSES = get_concrete_subclasses(UploadableStorageIO)  # type: ignore[type-abstract]


def get_upload_io(selector_cls: type[DataSelector], kind: str | Path) -> type[UploadableStorageIO]:
    """Get the appropriate UploadableStorageIO class based on the type of the provided selector."""
    for cls in UPLOAD_IO_CLASSES:
        if issubclass(selector_cls, cls.BASE_SELECTOR) and are_same_kind(cls.KIND, kind):
            return cls
    raise ValueError(f"No UploadableStorageIO found for selector of type {selector_cls.__name__}")


def are_same_kind(kind: str, kind_or_path: str | Path, /) -> bool:
    """Check if two kinds are the same, ignoring case and compression suffixes."""
    if not isinstance(kind_or_path, Path):
        return kind.casefold() == kind_or_path.casefold()
    stem = kind_or_path.stem
    if kind_or_path.suffix in COMPRESSION_BY_SUFFIX:
        stem = Path(stem).stem
    return stem.lower().endswith(kind.casefold())


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
    "UploadableStorageIO",
    "are_same_kind",
    "get_upload_io",
]
