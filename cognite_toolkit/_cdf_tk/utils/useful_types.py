from collections.abc import Hashable
from datetime import date, datetime
from typing import Any, Literal, TypeAlias, TypeVar, get_args

from cognite.client.data_classes import Asset, Event, FileMetadata, Sequence, TimeSeries
from cognite.client.data_classes._base import WriteableCogniteResourceList

JsonVal: TypeAlias = None | str | int | float | bool | dict[str, "JsonVal"] | list["JsonVal"]

AssetCentricDestinationType: TypeAlias = Literal["assets", "files", "events", "timeseries", "sequences"]
AssetCentricType: TypeAlias = Literal["asset", "file", "event", "timeseries", "sequence"]
AssetCentricResource: TypeAlias = Asset | FileMetadata | Event | TimeSeries | Sequence
DataType: TypeAlias = Literal["string", "integer", "float", "boolean", "json", "date", "timestamp"]
PythonTypes: TypeAlias = str | int | float | bool | datetime | date | dict[str, Any] | list[Any]

AVAILABLE_DATA_TYPES: set[DataType] = set(get_args(DataType))

T_ID = TypeVar("T_ID", bound=Hashable)
T_WritableCogniteResourceList = TypeVar("T_WritableCogniteResourceList", bound=WriteableCogniteResourceList)
T_Value = TypeVar("T_Value")
PrimitiveType: TypeAlias = str | int | float | bool
