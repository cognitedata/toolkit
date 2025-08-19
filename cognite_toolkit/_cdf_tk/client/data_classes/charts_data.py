import sys
from dataclasses import dataclass
from typing import Any

from cognite.client.data_classes._base import CogniteObject
from cognite.client.data_classes.data_modeling import NodeId, ViewId

if sys.version_info >= (3, 11):
    pass
else:
    pass


@dataclass
class ChartObject(CogniteObject): ...


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

    def dump(self, camel_case: bool = True) -> dict:
        data = super().dump(camel_case=camel_case)
        if self.elements:
            data["elements"] = [el.dump(camel_case=camel_case) for el in self.elements]
        return data


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
    settings: SubSetting | None = None
    flow: Flow | None = None
    calls: list[ChartCall] | None = None

    def dump(self, camel_case: bool = True) -> dict:
        data = super().dump(camel_case=camel_case)
        if self.settings:
            data["settings"] = self.settings.dump(camel_case=camel_case)
        if self.flow:
            data["flow"] = self.flow.dump(camel_case=camel_case)
        if self.calls:
            data["calls"] = [c.dump(camel_case=camel_case) for c in self.calls]
        return data


@dataclass
class ChartThreshold(BaseChartElement):
    visible: bool | None = None
    source_id: str | None = None
    upper_limit: float | None = None
    filter: ThresholdFilter | None = None
    calls: list["ChartCall"] | None = None

    def dump(self, camel_case: bool = True) -> dict:
        data = super().dump(camel_case=camel_case)
        if self.filter:
            data["filter"] = self.filter.dump(camel_case=camel_case)
        if self.calls:
            data["calls"] = [c.dump(camel_case=camel_case) for c in self.calls]
        return data


@dataclass
class ChartScheduledCalculation(BaseChartElement):
    version: str | None = None
    settings: SubSetting | None = None
    flow: Flow | None = None
    enabled: bool | None = None

    def dump(self, camel_case: bool = True) -> dict:
        data = super().dump(camel_case=camel_case)
        if self.settings:
            data["settings"] = self.settings.dump(camel_case=camel_case)
        if self.flow:
            data["flow"] = self.flow.dump(camel_case=camel_case)
        return data


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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the ChartData object to a dictionary."""
        data = super().dump(camel_case=camel_case)
        if self.time_series_collection:
            data["timeSeriesCollection" if camel_case else "time_series_collection"] = [
                ts.dump(camel_case=camel_case) for ts in self.time_series_collection
            ]
        if self.core_timeseries_collection:
            data["coreTimeseriesCollection" if camel_case else "core_timeseries_collection"] = [
                cts.dump(camel_case=camel_case) for cts in self.core_timeseries_collection
            ]
        if self.workflow_collection:
            data["workflowCollection" if camel_case else "workflow_collection"] = [
                wf.dump(camel_case=camel_case) for wf in self.workflow_collection
            ]
        if self.source_collection:
            data["sourceCollection" if camel_case else "source_collection"] = [
                src.dump(camel_case=camel_case) for src in self.source_collection
            ]
        if self.threshold_collection:
            data["thresholdCollection" if camel_case else "threshold_collection"] = [
                th.dump(camel_case=camel_case) for th in self.threshold_collection
            ]
        if self.scheduled_calculation_collection:
            data["scheduledCalculationCollection" if camel_case else "scheduled_calculation_collection"] = [
                sc.dump(camel_case=camel_case) for sc in self.scheduled_calculation_collection
            ]
        if self.user_info:
            data["userInfo" if camel_case else "user_info"] = self.user_info.dump(camel_case=camel_case)
        if self.settings:
            data["settings"] = self.settings.dump(camel_case=camel_case)
        return data
