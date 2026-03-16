from collections.abc import Hashable
from datetime import date, datetime
from typing import Any, Literal, TypeAlias, TypeVar, get_args

JsonVal: TypeAlias = None | str | int | float | bool | dict[str, "JsonVal"] | list["JsonVal"]

AssetCentricDestinationType: TypeAlias = Literal["assets", "files", "events", "timeseries", "sequences"]
AssetCentricType: TypeAlias = Literal["asset", "file", "event", "timeseries", "sequence"]
AssetCentricTypeExtended: TypeAlias = Literal["asset", "file", "event", "timeseries", "sequence", "annotation"]
AssetCentricKind: TypeAlias = Literal["Assets", "Events", "TimeSeries", "FileMetadata"]
AssetCentricKindExtended: TypeAlias = Literal["Assets", "Events", "TimeSeries", "FileMetadata", "Annotations"]

DataType: TypeAlias = Literal["string", "integer", "float", "boolean", "json", "date", "timestamp", "epoch"]
PythonTypes: TypeAlias = str | int | float | bool | datetime | date | dict[str, Any] | list[Any]

AVAILABLE_DATA_TYPES: set[DataType] = set(get_args(DataType))

T_ID = TypeVar("T_ID", bound=Hashable)
T_Value = TypeVar("T_Value")
PrimitiveType: TypeAlias = str | int | float | bool
