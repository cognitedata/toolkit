"""Temporary here to avoid circular imports while refactoring the codebase."""

from typing import TypeAlias, TypeVar

from cognite.client.data_classes import Annotation, Event, FileMetadata, TimeSeries

from cognite_toolkit._cdf_tk.client.data_classes.asset import AssetResponse

AssetCentricResource: TypeAlias = AssetResponse | FileMetadata | Event | TimeSeries
AssetCentricResourceExtended: TypeAlias = AssetResponse | FileMetadata | Event | TimeSeries | Annotation
T_AssetCentricResource = TypeVar("T_AssetCentricResource", bound=AssetCentricResource)
T_AssetCentricResourceExtended = TypeVar("T_AssetCentricResourceExtended", bound=AssetCentricResourceExtended)
