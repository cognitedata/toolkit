from typing import Any

from cognite.client.data_classes.data_modeling import NodeId, ViewId
from pydantic import JsonValue, field_serializer, field_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject


class UserInfo(BaseModelObject):
    id: str | None = None
    email: str | None = None
    display_name: str | None = None


class ChartSettings(BaseModelObject):
    show_y_axis: bool = True
    show_min_max: bool = True
    show_gridlines: bool = True
    merge_units: bool = False


class ThresholdFilter(BaseModelObject):
    min_unit: str | None = None
    max_unit: str | None = None


class ChartCall(BaseModelObject):
    id: str | None = None
    hash: int | None = None
    call_id: str | None = None
    call_date: int | None = None
    status: str | None = None


class SubSetting(BaseModelObject):
    auto_align: bool | None = None


class ChartPosition(BaseModelObject):
    x: float | None = None
    y: float | None = None


class FlowElement(BaseModelObject):
    id: str | None = None
    type: str | None = None
    position: ChartPosition | None = None
    data: JsonValue | None = None
    source: str | None = None
    target: str | None = None
    source_handle: str | None = None
    target_handle: str | None = None


class Flow(BaseModelObject):
    zoom: float | None = None
    elements: list[FlowElement] | None = None
    position: tuple[float | None, float | None] | None = None


class ChartElement(BaseModelObject):
    id: str | None = None
    type: str | None = None


class ChartSource(ChartElement): ...


class ChartCoreTimeseries(ChartElement):
    node_reference: NodeId | None = None
    view_reference: ViewId | None = None
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

    @field_serializer("node_reference", when_used="always")
    def serialize_node_reference(self, node_reference: NodeId | None) -> dict[str, Any] | None:
        if node_reference:
            return node_reference.dump(include_instance_type=False)
        return None

    @field_serializer("view_reference", when_used="always")
    def serialize_view_reference(self, view_reference: ViewId | None) -> dict[str, Any] | None:
        if view_reference:
            return view_reference.dump(include_type=False)
        return None

    @field_validator("node_reference", mode="before")
    @classmethod
    def validate_node_reference(cls, value: Any) -> NodeId | None:
        if value is None or isinstance(value, NodeId):
            return value
        return NodeId.load(value)

    @field_validator("view_reference", mode="before")
    @classmethod
    def validate_view_reference(cls, value: Any) -> ViewId | None:
        if value is None or isinstance(value, ViewId):
            return value
        return ViewId.load(value)


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


class ChartData(BaseModelObject):
    version: int | None = None
    name: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    user_info: UserInfo | None = None
    live_mode: bool | None = None
    time_series_collection: list[ChartTimeseries] | None = None
    core_timeseries_collection: list[ChartCoreTimeseries] | None = None
    workflow_collection: list[ChartWorkflow] | None = None
    source_collection: list[ChartSource] | None = None
    threshold_collection: list[ChartThreshold] | None = None
    scheduled_calculation_collection: list[ChartScheduledCalculation] | None = None
    settings: ChartSettings | None = None
