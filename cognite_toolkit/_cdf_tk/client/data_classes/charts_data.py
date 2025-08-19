import sys
from dataclasses import dataclass

from cognite.client.data_classes.data_modeling import NodeId, ViewId

if sys.version_info >= (3, 11):
    pass
else:
    pass


@dataclass
class ChartObject: ...


@dataclass
class UserInfo(ChartObject):
    id: str | None = None
    email: str | None = None
    display_name: str | None = None


@dataclass
class ChartSettings(ChartObject):
    show_y_axis: bool = True
    show_x_axis: bool = True
    show_gridlines: bool = True
    merge_units: bool = False


@dataclass
class ThresholdFilter(ChartObject):
    min_unit: str | None = None
    max_unit: str | None = None


@dataclass
class ChartCall(ChartObject):
    id: str | None = None
    hash: int | None = None
    call_id: str | None = None
    call_date: int | None = None
    status: str | None = None


@dataclass
class SubSetting(ChartObject):
    auto_align: bool | None = None


@dataclass
class FlowElement(ChartObject):
    id: str | None = None
    type: str | None = None
    position: tuple[float | None, float | None] | None = None
    data: dict[str, object] | None = None


@dataclass
class Flow(ChartObject):
    zoom: float | None = None
    elements: list[FlowElement] | None = None
    position: tuple[float | None, float | None] | None = None


@dataclass
class ChartSource(ChartObject):
    type: str | None = None
    id: str | None = None


@dataclass
class BaseChartElement(ChartObject):
    type: str | None = None
    id: str | None = None
    name: str | None = None
    color: str | None = None
    enabled: bool | None = None
    line_weight: int | None = None
    line_style: str | None = None
    interpolation: str | None = None
    unit: str | None = None
    preferred_unit: str | None = None
    created_at: int | None = None
    range: tuple[float | None, float | None] | None = None
    description: str | None = None


@dataclass
class ChartCoreTimeseries(BaseChartElement):
    node_reference: NodeId | None = None
    view_reference: ViewId | None = None
    display_mode: str | None = None
    enabled: bool | None = None


@dataclass
class ChartTimeseries(BaseChartElement):
    tsId: int | None = None
    tsExternalId: str | None = None
    display_mode: str | None = None
    original_unit: str | None = None


@dataclass
class ChartWorkflow(BaseChartElement):
    version: str | None = None
    settings: "SubSetting | None" = None
    flow: "Flow | None" = None
    calls: list[ChartCall] | None = None


@dataclass
class ChartThreshold(BaseChartElement):
    visible: bool | None = None
    source_id: str | None = None
    upper_limit: float | None = None
    filter: "ThresholdFilter | None" = None
    calls: list["ChartCall"] | None = None


@dataclass
class ChartScheduledCalculation(BaseChartElement):
    version: str | None = None
    settings: "SubSetting | None" = None
    flow: Flow | None = None
    enabled: bool | None = None


@dataclass
class ChartData(ChartObject):
    version: int | None = None
    name: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    user_info: UserInfo | None = None
    live_model: bool | None = None
    time_series_collection: list[ChartTimeseries] | None = None
    core_timeseries_collection: list[ChartCoreTimeseries] | None = None
    workflow_collection: list[ChartWorkflow] | None = None
    source_collection: list[ChartSource] | None = None
    threshold_collection: list[ChartThreshold] | None = None
    scheduled_calculation_collection: list[ChartScheduledCalculation] | None = None
    settings: ChartSettings | None = None
