import sys
from dataclasses import dataclass, field, fields
from functools import lru_cache
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes._base import CogniteObject
from cognite.client.data_classes.data_modeling import NodeId, ViewId
from cognite.client.utils._auxiliary import to_camel_case

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass
class ChartObject(CogniteObject):
    # ChartObjects are used in the frontend and the backend does not do any validation of these fields.
    # Therefore, to ensure that we do not lose any data, we store unknown fields in a separate dictionary.
    # This allows unknown fields to be preserved when loading and dumping ChartObjects
    # (serialization and deserialization).
    _unknown_fields: dict[str, object] | None = field(default=None, init=False, repr=False)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        """Load a ChartObject from a dictionary."""
        instance = super()._load(resource, cognite_client=cognite_client)
        instance._unknown_fields = {k: v for k, v in resource.items() if k not in cls._known_camel_case_props()}
        return instance

    @classmethod
    @lru_cache(maxsize=1)
    def _known_camel_case_props(cls) -> set[str]:
        return {to_camel_case(f.name) for f in fields(cls)}

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the ChartObject to a dictionary."""
        data = super().dump(camel_case=camel_case)
        if self._unknown_fields:
            data.update(self._unknown_fields)
        return data


@dataclass
class UserInfo(ChartObject):
    id: str | None = None
    email: str | None = None
    display_name: str | None = None


@dataclass
class ChartSettings(ChartObject):
    show_y_axis: bool = True
    show_min_max: bool = True
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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        data = super().dump(camel_case=camel_case)
        if self.elements:
            data["elements"] = [el.dump(camel_case=camel_case) for el in self.elements]
        return data

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        """Load a Flow object from a dictionary."""
        instance = super()._load(resource, cognite_client=cognite_client)
        if "elements" in resource:
            instance.elements = [FlowElement._load(el, cognite_client=cognite_client) for el in resource["elements"]]
        return instance


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
    line_weight: float | None = None
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


@dataclass
class ChartTimeseries(BaseChartElement):
    ts_id: int | None = None
    ts_external_id: str | None = None
    display_mode: str | None = None
    original_unit: str | None = None


@dataclass
class ChartWorkflow(BaseChartElement):
    version: str | None = None
    settings: SubSetting | None = None
    flow: Flow | None = None
    calls: list[ChartCall] | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        data = super().dump(camel_case=camel_case)
        if self.settings:
            data["settings"] = self.settings.dump(camel_case=camel_case)
        if self.flow:
            data["flow"] = self.flow.dump(camel_case=camel_case)
        if self.calls:
            data["calls"] = [c.dump(camel_case=camel_case) for c in self.calls]
        return data

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        """Load a ChartWorkflow object from a dictionary."""
        instance = super()._load(resource, cognite_client=cognite_client)
        if "settings" in resource:
            instance.settings = SubSetting._load(resource["settings"], cognite_client=cognite_client)
        if "flow" in resource:
            instance.flow = Flow._load(resource["flow"], cognite_client=cognite_client)
        if "calls" in resource:
            instance.calls = [ChartCall._load(call, cognite_client=cognite_client) for call in resource["calls"]]
        return instance


@dataclass
class ChartThreshold(BaseChartElement):
    visible: bool | None = None
    source_id: str | None = None
    upper_limit: float | None = None
    filter: ThresholdFilter | None = None
    calls: list[ChartCall] | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        data = super().dump(camel_case=camel_case)
        if self.filter:
            data["filter"] = self.filter.dump(camel_case=camel_case)
        if self.calls:
            data["calls"] = [c.dump(camel_case=camel_case) for c in self.calls]
        return data

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        """Load a ChartThreshold object from a dictionary."""
        instance = super()._load(resource, cognite_client=cognite_client)
        if "filter" in resource:
            instance.filter = ThresholdFilter._load(resource["filter"], cognite_client=cognite_client)
        if "calls" in resource:
            instance.calls = [ChartCall._load(call, cognite_client=cognite_client) for call in resource["calls"]]
        return instance


@dataclass
class ChartScheduledCalculation(BaseChartElement):
    version: str | None = None
    settings: SubSetting | None = None
    flow: Flow | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        data = super().dump(camel_case=camel_case)
        if self.settings:
            data["settings"] = self.settings.dump(camel_case=camel_case)
        if self.flow:
            data["flow"] = self.flow.dump(camel_case=camel_case)
        return data

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        """Load a ChartScheduledCalculation object from a dictionary."""
        instance = super()._load(resource, cognite_client=cognite_client)
        if "settings" in resource:
            instance.settings = SubSetting._load(resource["settings"], cognite_client=cognite_client)
        if "flow" in resource:
            instance.flow = Flow._load(resource["flow"], cognite_client=cognite_client)
        return instance


@dataclass
class ChartData(ChartObject):
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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the ChartData object to a dictionary."""
        data = super().dump(camel_case=camel_case)
        list_attrs = [
            "time_series_collection",
            "core_timeseries_collection",
            "workflow_collection",
            "source_collection",
            "threshold_collection",
            "scheduled_calculation_collection",
        ]
        for attr_name in list_attrs:
            if collection := getattr(self, attr_name):
                key = to_camel_case(attr_name) if camel_case else attr_name
                data[key] = [item.dump(camel_case=camel_case) for item in collection]

        single_attrs_map = {
            "user_info": "userInfo",
            "settings": "settings",
        }
        for attr_name, camel_key in single_attrs_map.items():
            if item := getattr(self, attr_name):
                key = camel_key if camel_case else attr_name
                data[key] = item.dump(camel_case=camel_case)
        return data

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        """Load a ChartData object from a dictionary."""
        instance = super()._load(resource, cognite_client=cognite_client)
        collections_map = [
            ("timeSeriesCollection", "time_series_collection", ChartTimeseries),
            ("coreTimeseriesCollection", "core_timeseries_collection", ChartCoreTimeseries),
            ("workflowCollection", "workflow_collection", ChartWorkflow),
            ("sourceCollection", "source_collection", ChartSource),
            ("thresholdCollection", "threshold_collection", ChartThreshold),
            ("scheduledCalculationCollection", "scheduled_calculation_collection", ChartScheduledCalculation),
        ]
        for resource_key, attr_name, subclass in collections_map:
            if resource_key in resource:
                setattr(
                    instance,
                    attr_name,
                    [subclass._load(item, cognite_client=cognite_client) for item in resource[resource_key]],  # type: ignore[attr-defined]
                )
        attribute_map = [
            ("userInfo", "user_info", UserInfo),
            ("settings", "settings", ChartSettings),
        ]
        for resource_key, attr_name, subclass in attribute_map:
            if resource_key in resource:
                setattr(
                    instance,
                    attr_name,
                    subclass._load(resource[resource_key], cognite_client=cognite_client),  # type: ignore[attr-defined]
                )

        return instance
