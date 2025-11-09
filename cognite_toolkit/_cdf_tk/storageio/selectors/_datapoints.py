from abc import ABC
from pathlib import Path
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

    path: Path
    columns: list[TimeSeriesColumn]

    @property
    def group(self) -> str:
        return f"Datapoints_{self.path.stem}"

    def __str__(self) -> str:
        return f"file_{self.path.name}"
