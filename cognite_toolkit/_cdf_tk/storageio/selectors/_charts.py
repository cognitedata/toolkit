from abc import ABC
from typing import Literal

from ._base import DataSelector


class ChartSelector(DataSelector, ABC): ...


class ChartOwnerSelector(ChartSelector):
    type: Literal["chartOwner"] = "chartOwner"
    owner_id: str

    @property
    def group(self) -> str:
        return "Charts"

    def __str__(self) -> str:
        return self.owner_id


class AllChartsSelector(ChartSelector):
    type: Literal["allCharts"] = "allCharts"

    @property
    def group(self) -> str:
        return "Charts"

    def __str__(self) -> str:
        return "all"
