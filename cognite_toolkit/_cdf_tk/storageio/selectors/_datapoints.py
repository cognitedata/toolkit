from abc import ABC, abstractmethod
from functools import cached_property
from typing import Annotated, Any, Literal

from cognite.client._proto.data_points_pb2 import (
    InstanceId,
)
from pydantic import Field

from ._base import DataSelector, SelectorObject


class Column(SelectorObject, ABC):
    column_type: str
    column: str
    dtype: Literal["numeric", "string"]

    @abstractmethod
    def as_wrapped_id(self) -> dict[str, Any]: ...


class InstanceColumn(Column):
    column_type: Literal["instance"] = "instance"
    space: str
    external_id: str

    def as_wrapped_id(self) -> dict[str, Any]:
        return {"instanceId": InstanceId(space=self.space, externalId=self.external_id)}


class ExternalIdColumn(Column):
    column_type: Literal["externalId"] = "externalId"
    external_id: str

    def as_wrapped_id(self) -> dict[str, Any]:
        return {"externalId": self.external_id}


class InternalIdColumn(Column):
    column_type: Literal["internalId"] = "internalId"
    internal_id: int

    def as_wrapped_id(self) -> dict[str, Any]:
        return {"id": self.internal_id}


TimeSeriesColumn = Annotated[
    InstanceColumn | ExternalIdColumn | InternalIdColumn,
    Field(discriminator="column_type"),
]


class DataPointsFileSelector(DataSelector):
    type: Literal["datapointsFile"] = "datapointsFile"
    kind: Literal["datapoints"] = "datapoints"

    timestamp_column: str
    columns: tuple[TimeSeriesColumn, ...]

    @property
    def group(self) -> str:
        return "Datapoints"

    def __str__(self) -> str:
        return "datapoints_file"

    @cached_property
    def id_by_column(self) -> dict[str, Column]:
        return {col.column: col for col in self.columns}
