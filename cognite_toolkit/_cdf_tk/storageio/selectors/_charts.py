import hashlib
from abc import ABC
from typing import Literal

from ._base import DataSelector


class ChartSelector(DataSelector, ABC):
    kind: Literal["Charts"] = "Charts"


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


class ChartExternalIdSelector(ChartSelector):
    type: Literal["chartExternalId"] = "chartExternalId"
    external_ids: tuple[str, ...]

    @property
    def group(self) -> str:
        return "Charts"

    def __str__(self) -> str:
        hash_ = hashlib.md5(",".join(sorted(self.external_ids)).encode()).hexdigest()[:8]
        return f"chart_count_{len(self.external_ids)}_hash_{hash_}"
