from collections.abc import Hashable
from datetime import date, datetime
from typing import Any, Literal, TypeAlias, TypeVar, get_args

from cognite.client.data_classes import Annotation, Asset, Event, FileMetadata, TimeSeries
from cognite.client.data_classes._base import CogniteObject, WriteableCogniteResourceList

JsonVal: TypeAlias = None | str | int | float | bool | dict[str, "JsonVal"] | list["JsonVal"]

AssetCentricDestinationType: TypeAlias = Literal["assets", "files", "events", "timeseries", "sequences"]
AssetCentricType: TypeAlias = Literal["asset", "file", "event", "timeseries", "sequence", "fileAnnotation"]
AssetCentricResource: TypeAlias = Asset | FileMetadata | Event | TimeSeries
AssetCentricResourceExtended: TypeAlias = Asset | FileMetadata | Event | TimeSeries | Annotation
AssetCentricKind: TypeAlias = Literal["Assets", "Events", "TimeSeries", "FileMetadata", "FileAnnotations"]

DataType: TypeAlias = Literal["string", "integer", "float", "boolean", "json", "date", "timestamp", "epoch"]
PythonTypes: TypeAlias = str | int | float | bool | datetime | date | dict[str, Any] | list[Any]

AVAILABLE_DATA_TYPES: set[DataType] = set(get_args(DataType))

T_ID = TypeVar("T_ID", bound=Hashable)
T_WritableCogniteResourceList = TypeVar("T_WritableCogniteResourceList", bound=WriteableCogniteResourceList)
T_Value = TypeVar("T_Value")
PrimitiveType: TypeAlias = str | int | float | bool

T_WriteCogniteResource = TypeVar("T_WriteCogniteResource", bound=CogniteObject)
T_AssetCentricResource = TypeVar("T_AssetCentricResource", bound=AssetCentricResource)
