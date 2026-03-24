from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import NodeUntypedId, ViewUntypedId


class ChartObject(BaseModelObject, extra="allow"): ...


class ChartElement(ChartObject):
    id: str | None = None
    type: str | None = None


class UserInfo(ChartObject):
    id: str | None = None
    email: str | None = None
    display_name: str | None = None


class ChartSettings(ChartObject):
    show_y_axis: bool = True
    show_min_max: bool = True
    show_gridlines: bool = True
    merge_units: bool = False


class ThresholdFilter(ChartObject):
    min_unit: str | None = None
    max_unit: str | None = None


class ChartCall(ChartObject):
    id: str | None = None
    hash: int | None = None
    call_id: str | None = None
    call_date: int | None = None
    status: str | None = None


class SubSetting(ChartObject):
    auto_align: bool | None = None


class ChartPosition(ChartObject):
    x: float | None = None
    y: float | None = None


class FlowData(ChartObject):
    type: str | None = None
    selected_source_id: str | None = None


class FlowElement(ChartElement):
    position: ChartPosition | None = None
    data: FlowData | None = None
    source: str | None = None
    target: str | None = None
    source_handle: str | None = None
    target_handle: str | None = None


class Flow(ChartObject):
    zoom: float | None = None
    elements: list[FlowElement] | None = None
    position: tuple[float | None, float | None] | None = None


class ChartSource(ChartElement): ...


class ChartCoreTimeseries(ChartElement):
    node_reference: NodeUntypedId | None = None
    view_reference: ViewUntypedId | None = None
    display_mode: str | None = None
    color: str | None = None
    created_at: int | None = None
    enabled: bool | None = None
    interpolation: str | None = None
    line_style: str | None = None
    line_weight: float | None = None
    name: str | None = None
    preferred_unit: str | None = None
    range: list[float | None] | None = None


class ChartTimeseries(ChartElement):
    color: str | None = None
    created_at: int | None = None
    enabled: bool | None = None
    interpolation: str | None = None
    line_style: str | None = None
    line_weight: float | None = None
    name: str | None = None
    preferred_unit: str | None = None
    range: list[float | None] | None = None
    unit: str | None = None
    ts_id: int | None = None
    ts_external_id: str | None = None
    display_mode: str | None = None
    original_unit: str | None = None
    description: str | None = None


class ChartWorkflow(ChartElement):
    version: str | None = None
    name: str | None = None
    color: str | None = None
    enabled: bool | None = None
    line_weight: float | None = None
    line_style: str | None = None
    interpolation: str | None = None
    unit: str | None = None
    preferred_unit: str | None = None
    range: list[float | None] | None = None
    created_at: int | None = None
    settings: SubSetting | None = None
    flow: Flow | None = None
    calls: list[ChartCall] | None = None


class ChartThreshold(ChartElement):
    visible: bool | None = None
    name: str | None = None
    source_id: str | None = None
    lower_limit: float | None = None
    upper_limit: float | None = None
    filter: ThresholdFilter | None = None
    calls: list[ChartCall] | None = None


class ChartScheduledCalculation(ChartElement):
    color: str | None = None
    created_at: int | None = None
    description: str | None = None
    enabled: bool | None = None
    interpolation: str | None = None
    line_style: str | None = None
    line_weight: float | None = None
    name: str | None = None
    preferred_unit: str | None = None
    range: list[float | None] | None = None
    unit: str | None = None
    version: str | None = None
    settings: SubSetting | None = None
    flow: Flow | None = None


class MonitoringJob(ChartObject):
    id: int | None = None
    source_id: str | None = None
    source_type: str | None = None


class EventFilter(ChartObject):
    id: str | None = None
    name: str | None = None
    visible: bool | None = None
    color: str | None = None
    filter: dict[str, JsonValue] | None = None


class ChartActivity(ChartObject):
    is_highlighted: bool | None = None
    is_pinned: bool | None = None
    node_reference: NodeUntypedId | None = None
    view_reference: ViewUntypedId | None = None


class ChartData(ChartObject):
    version: int
    name: str
    date_from: str
    date_to: str
    user_info: UserInfo | None = None
    live_mode: bool | None = None
    time_series_collection: list[ChartTimeseries] | None = None
    core_timeseries_collection: list[ChartCoreTimeseries] | None = None
    workflow_collection: list[ChartWorkflow] | None = None
    source_collection: list[ChartSource] | None = None
    threshold_collection: list[ChartThreshold] | None = None
    scheduled_calculation_collection: list[ChartScheduledCalculation] | None = None
    settings: ChartSettings | None = None
    monitoring_jobs: list[MonitoringJob] | None = None
    event_filters: list[EventFilter] | None = None
    activities_collection: list[ChartActivity] | None = None
