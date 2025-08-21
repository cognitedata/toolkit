import sys
from abc import ABC
from typing import Any, Literal, TypeAlias

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

Visibility: TypeAlias = Literal["PUBLIC", "PRIVATE"]


class ChartCore(WriteableCogniteResource["ChartWrite"], ABC):
    """Base class for the Chart data model.

    Args:
        external_id (str): Unique identifier for the chart.
        visibility (Visibility): Visibility of the chart, either 'public' or 'private'.
        data (object): The data associated with the chars.
    """

    def __init__(self, external_id: str, visibility: Visibility, data: object) -> None:
        self.external_id = external_id
        self.visibility = visibility
        self.data = data


class ChartWrite(ChartCore):
    """A chart that can be written to the CDF.

    Args:
        external_id (str): Unique identifier for the chart.
        visibility (Visibility): Visibility of the chart, either 'public' or 'private'.
        data (object): The data associated with the chart.

    """

    def as_write(self) -> "ChartWrite":
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            visibility=resource["visibility"],
            data=resource["data"],
        )


class Chart(ChartCore):
    """A chart that can be read from the CDF.

    Args:
        external_id (str): Unique identifier for the chart.
        created_time (int): Timestamp when the chart was created.
        last_updated_time (int): Timestamp when the chart was last updated.
        visibility (Visibility): Visibility of the chart, either 'public' or 'private'.
        data (object): The data associated with the chart.
    """

    def __init__(
        self,
        external_id: str,
        created_time: int,
        last_updated_time: int,
        visibility: Visibility,
        data: object,
        owner_id: str,
    ) -> None:
        super().__init__(external_id, visibility, data)
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.owner_id = owner_id

    def as_write(self) -> ChartWrite:
        return ChartWrite(external_id=self.external_id, visibility=self.visibility, data=self.data)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            visibility=resource["visibility"],
            data=resource["data"],
            owner_id=resource["ownerId"],
        )


class ChartWriteList(CogniteResourceList):
    _RESOURCE = ChartWrite


class ChartList(WriteableCogniteResourceList[ChartWrite, Chart]):
    _RESOURCE = Chart

    def as_write(self) -> ChartWriteList:
        return ChartWriteList([item.as_write() for item in self])
