from abc import ABC
from functools import cached_property
from typing import Annotated, Literal

from pydantic import Field

from ._base import DataSelector, SelectorObject


class Column(SelectorObject, ABC):
    column_type: str
    column: str


class InstanceColumn(Column):
    column_type: Literal["instance"] = "instance"
    space: str
    external_id: str


class ExternalIdColumn(Column):
    column_type: Literal["externalId"] = "externalId"
    external_id: str


class InternalIdColumn(Column):
    column_type: Literal["internalId"] = "internalId"
    internal_id: int


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
    def id_by_column(self) -> dict[str, TimeSeriesColumn]:
        return {col.column: col for col in self.columns}
