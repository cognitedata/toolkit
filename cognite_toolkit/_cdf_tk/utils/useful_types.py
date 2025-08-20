from typing import Literal, TypeAlias

JsonVal: TypeAlias = None | str | int | float | bool | dict[str, "JsonVal"] | list["JsonVal"]

AssetCentricDestinationType: TypeAlias = Literal["assets", "files", "events", "timeseries", "sequences"]
AssetCentric: TypeAlias = Literal["asset", "file", "event", "timeseries", "sequence"]
DataType: TypeAlias = Literal["string", "integer", "float", "boolean", "json", "date", "timestamp"]
