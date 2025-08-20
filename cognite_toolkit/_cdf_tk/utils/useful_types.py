from datetime import date, datetime
from typing import Any, Literal, TypeAlias, get_args

JsonVal: TypeAlias = None | str | int | float | bool | dict[str, "JsonVal"] | list["JsonVal"]

AssetCentricDestinationType: TypeAlias = Literal["assets", "files", "events", "timeseries", "sequences"]
AssetCentric: TypeAlias = Literal["asset", "file", "event", "timeseries", "sequence"]
DataType: TypeAlias = Literal["string", "integer", "float", "boolean", "json", "date", "timestamp"]
PythonTypes: TypeAlias = str | int | float | bool | datetime | date | dict[str, Any] | list[Any]

AVAILABLE_DATA_TYPES: set[DataType] = set(get_args(DataType))
