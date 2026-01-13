"""Temporary here to avoid circular imports while refactoring the codebase."""

from typing import TypeAlias, TypeVar

from cognite.client.data_classes import Annotation, FileMetadata

from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventResponse
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesResponse

AssetCentricResource: TypeAlias = AssetResponse | FileMetadata | EventResponse | TimeSeriesResponse
AssetCentricResourceExtended: TypeAlias = AssetResponse | FileMetadata | EventResponse | TimeSeriesResponse | Annotation
T_AssetCentricResource = TypeVar("T_AssetCentricResource", bound=AssetCentricResource)
T_AssetCentricResourceExtended = TypeVar("T_AssetCentricResourceExtended", bound=AssetCentricResourceExtended)
