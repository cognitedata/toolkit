from abc import ABC
from typing import Literal, TypeAlias

from cognite.client.data_classes._base import (
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

Visibility: TypeAlias = Literal["public", "private"]


class ChartCore(WriteableCogniteResource["ChartWrite"], ABC):
    def __init__(self, external_id: str, visibility: Visibility, data: object) -> None:
        self.external_id = external_id
        self.visibility = visibility
        self.data = data


class ChartWrite(ChartCore):
    def as_write(self) -> "ChartWrite":
        return self


class Chart(ChartCore):
    def __init__(
        self,
        external_id: str,
        created_time: int,
        last_updated_time: int,
        visibility: Visibility,
        data: object,
    ) -> None:
        super().__init__(external_id, visibility, data)
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    def as_write(self) -> ChartWrite:
        return ChartWrite(external_id=self.external_id, visibility=self.visibility, data=self.data)


class ChartWriteList(CogniteResourceList):
    _RESOURCE = ChartWrite


class ChartList(WriteableCogniteResourceList[ChartWrite, Chart]):
    _RESOURCE = Chart

    def as_write(self) -> ChartWriteList:
        return ChartWriteList([item.as_write() for item in self])
