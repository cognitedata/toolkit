import sys
from dataclasses import dataclass
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import NodeId, ViewId

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass
class ChartObject: ...


@dataclass
class UserInfo(ChartObject):
    id: str
    email: str | None = None
    display_name: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        """Load a UserInfo object from a resource dictionary."""
        return cls(id=resource["id"], email=resource.get("email"), display_name=resource.get("displayName"))


@dataclass
class ChartSettings(ChartObject):
    show_y_axis: bool = True
    show_x_axis: bool = True
    show_gridlines: bool = True
    merge_units: bool = False

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        """Load a ChartSettings object from a resource dictionary."""
        return cls(
            show_y_axis=resource.get("showYAxis", True),
            show_x_axis=resource.get("showXAxis", True),
            show_gridlines=resource.get("showGridlines", True),
            merge_units=resource.get("mergeUnits", False),
        )


@dataclass
class ThresholdFilter(ChartObject):
    min_unit: str
    max_unit: str


@dataclass
class ChartCall(ChartObject):
    id: str
    hash: int
    call_id: str
    call_date: int
    status: str


@dataclass
class SubSetting(ChartObject):
    auto_align: bool


@dataclass
class FlowElement(ChartObject):
    id: str
    type: str
    position: tuple[float, float]
    data: dict[str, object]


@dataclass
class Flow(ChartObject):
    zoom: float
    elements: list[FlowElement]
    position: tuple[float, float]


@dataclass
class ChartSource(ChartObject):
    type: str
    id: str


@dataclass
class ChartCoreTimeseries(ChartObject):
    type: str
    id: str
    color: str
    node_reference: NodeId
    view_reference: ViewId
    name: str
    line_weight: int
    line_style: str
    interpolation: str
    display_mode: str
    enabled: bool
    created_at: int
    preferred_unit: str = ""
    range: tuple[float | None, float | None] = (None, None)


@dataclass
class ChartTimeseries(ChartObject):
    type: str
    id: str
    name: str
    color: str
    tsId: int
    tsExternalId: str
    line_weight: int
    line_style: str
    interpolation: str
    display_mode: str
    enabled: bool
    unit: str
    original_unit: str
    preferred_unit: str
    description: str
    created_at: int
    range: tuple[float | None, float | None] = (None, None)


@dataclass
class ChartWorkflow(ChartObject):
    type: str
    version: str
    id: str
    name: str
    color: str
    enabled: bool
    settings: SubSetting
    line_weight: int
    line_style: str
    interpolation: str
    unit: str
    preferred_unit: str
    created_at: int
    flow: Flow
    calls: list[ChartCall]


@dataclass
class ChartThreshold(ChartObject):
    id: str
    name: str
    visible: bool
    source_id: str
    upper_limit: float
    type: str
    filter: ThresholdFilter
    calls: list[ChartCall]


@dataclass
class ChartScheduledCalculation(ChartObject):
    version: str
    type: str
    id: str
    name: str
    color: str
    settings: SubSetting
    enabled: bool
    description: str
    line_weight: int
    line_style: str
    interpolation: str
    unit: str
    preferred_unit: str
    created_at: int
    flow: Flow
    range: tuple[float | None, float | None] = (None, None)


@dataclass
class ChartData(ChartObject):
    version: int
    name: str
    date_from: str
    date_to: str
    user_info: UserInfo
    live_model: bool
    time_series_collection: list[ChartTimeseries]
    core_timeseries_collection: list[ChartCoreTimeseries]
    workflow_collection: list[ChartWorkflow]
    source_collection: list[ChartSource]
    threshold_collection: list[ChartThreshold]
    scheduled_calculation_collection: list[ChartScheduledCalculation]
    settings: ChartSettings
