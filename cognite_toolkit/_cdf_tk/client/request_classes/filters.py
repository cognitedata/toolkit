from typing import Any, Literal, Self

from pydantic import ConfigDict, Field, JsonValue, field_validator
from pydantic_core.core_schema import ValidationInfo

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes import streamlit_
from cognite_toolkit._cdf_tk.client.resource_classes.annotation import AnnotationStatus, AnnotationType
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.extraction_pipeline import Contact, RawTable
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionStatus
from cognite_toolkit._cdf_tk.client.resource_classes.function_call import FunctionCallStatus
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import SchemaType
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_execution import WorkflowExecutionStatus

from .base import BaseModelRequest


class Filter(BaseModelRequest): ...


class EpochTimestampRange(BaseModelRequest):
    model_config = ConfigDict(protected_namespaces=tuple())
    min_: int | None = Field(default=None, alias="min")
    max_: int | None = Field(default=None, alias="max")


class LabelFilter(Filter):
    """Match resources whose labels contain any or all of the given external IDs."""

    contains_any: list[str] | None = None
    contains_all: list[str] | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        contains_any_key = "containsAny" if camel_case else "contains_any"
        contains_all_key = "containsAll" if camel_case else "contains_all"
        body: dict[str, Any] = {}
        if self.contains_any is not None:
            body[contains_any_key] = [{"externalId": external_id} for external_id in self.contains_any]
        if self.contains_all is not None:
            body[contains_all_key] = [{"externalId": external_id} for external_id in self.contains_all]
        return body


class GeoLocationFilter(Filter):
    """Geographic relation against a GeoJSON geometry in ``shape``."""

    relation: Literal["INTERSECTS", "DISJOINT", "WITHIN"]
    shape: dict[str, JsonValue]


class ClassicFilter(Filter):
    asset_subtree_ids: list[ExternalId | InternalId] | None = None
    data_set_ids: list[ExternalId | InternalId] | None = None

    @classmethod
    def from_asset_subtree_and_data_sets(
        cls,
        asset_subtree_id: str | int | list[str | int] | None = None,
        data_set_id: str | int | list[str | int] | None = None,
    ) -> Self:
        # Only set the given IDs, as dump() sends every set field and the API spec does not allow null for these.
        ids = {
            "asset_subtree_ids": cls._as_internal_or_external_id_list(asset_subtree_id),
            "data_set_ids": cls._as_internal_or_external_id_list(data_set_id),
        }
        return cls.model_validate({key: value for key, value in ids.items() if value is not None})

    @classmethod
    def _as_internal_or_external_id_list(
        cls, id: str | int | list[str | int] | None
    ) -> list[ExternalId | InternalId] | None:
        if id is None:
            return None
        ids = id if isinstance(id, list) else [id]
        return [ExternalId(external_id=item) if isinstance(item, str) else InternalId(id=item) for item in ids]


class StreamlitFilter(ClassicFilter):
    creator: str | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        body = self.model_dump(mode="json", by_alias=camel_case, exclude_unset=True, exclude={"creator"})
        if self.creator is not None:
            body["metadata"] = {"creator": self.creator}
        body["directoryPrefix"] = streamlit_.STREAMLIT_DIRECTORY
        return body


RelationshipResourceType = Literal["asset", "timeSeries", "file", "event", "sequence"]


class ConfidenceRange(BaseModelRequest):
    min_: float | None = Field(default=None, alias="min")
    max_: float | None = Field(default=None, alias="max")


class RelationshipResourceReference(BaseModelRequest):
    type: RelationshipResourceType
    external_id: str


class RelationshipFilter(Filter):
    """Filter for listing relationships.

    ``active_at_time`` matches relationships that are active at some point in the range, and ``sources_or_targets``
    matches relationships where the resource is either the source or the target.
    """

    source_external_ids: list[str] | None = Field(default=None, min_length=1, max_length=1000)
    source_types: list[RelationshipResourceType] | None = Field(default=None, min_length=1, max_length=1000)
    target_external_ids: list[str] | None = Field(default=None, min_length=1, max_length=1000)
    target_types: list[RelationshipResourceType] | None = Field(default=None, min_length=1, max_length=1000)
    data_set_ids: list[ExternalId | InternalId] | None = Field(default=None, max_length=1000)
    start_time: EpochTimestampRange | None = None
    end_time: EpochTimestampRange | None = None
    confidence: ConfidenceRange | None = None
    last_updated_time: EpochTimestampRange | None = None
    created_time: EpochTimestampRange | None = None
    active_at_time: EpochTimestampRange | None = None
    labels: LabelFilter | None = None
    sources_or_targets: list[RelationshipResourceReference] | None = Field(default=None, min_length=1, max_length=1)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        body = self.model_dump(mode="json", by_alias=camel_case, exclude_unset=True, exclude={"labels"})
        if self.labels is not None:
            body["labels"] = self.labels.dump(camel_case)
        return body


class ExtractionPipelineFilter(Filter):
    """Filter for listing extraction pipelines.

    ``schedule`` is either "On trigger", "Continuous", or a cron expression.
    """

    external_id_prefix: str | None = None
    name: str | None = None
    data_set_ids: list[ExternalId | InternalId] | None = Field(default=None, min_length=1, max_length=100)
    schedule: str | None = None
    contacts: list[Contact] | None = None
    raw_tables: list[RawTable] | None = None
    metadata: dict[str, str] | None = None
    source: str | None = None
    documentation: str | None = None
    created_by: str | None = None
    created_time: EpochTimestampRange | None = None
    last_updated_time: EpochTimestampRange | None = None


class TransformationTagFilter(Filter):
    contains_any: list[str]


class TransformationFilter(Filter):
    data_set_ids: list[ExternalId | InternalId] | None = None
    is_public: bool | None = None
    name_regex: str | None = None
    query_regex: str | None = None
    destination_type: SchemaType | str | None = None
    conflict_mode: Literal["abort", "delete", "update", "upsert"] | None = None
    has_blocked_error: bool | None = None
    cdf_project_name: str | None = None
    created_time: EpochTimestampRange | None = None
    last_updated_time: EpochTimestampRange | None = None
    tags: TransformationTagFilter | None = None


class TransformationNotificationFilter(Filter):
    transformation_id: int | None = None
    transformation_external_id: str | None = None
    destination: str | None = None


class DataModelingFilter(Filter):
    space: str | None = None
    include_global: bool | None = None


class ContainerFilter(DataModelingFilter):
    used_for: Literal["node", "edge", "record", "all"] | None = None


class ViewFilter(DataModelingFilter):
    include_inherited_properties: bool | None = None
    all_versions: bool | None = None
    used_for: Literal["node", "edge", "record", "all"] | None = None


class DataModelFilter(DataModelingFilter):
    all_versions: bool | None = None


class InstanceFilter(Filter):
    instance_type: Literal["node", "edge"] | None = None
    source: ViewId | None = None
    space: list[str] | None = None
    filter: dict[str, JsonValue] | None = None

    @field_validator("filter")
    def only_filter_or_space(cls, v: dict, info: ValidationInfo) -> dict:
        if v is not None and info.data.get("space") is not None:
            raise ValueError("Cannot specify both filter and space in InstanceFilter")
        return v

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if self.instance_type is not None:
            body["instanceType"] = self.instance_type
        if self.source is not None:
            body["sources"] = [{"source": self.source.dump(include_type=True)}]
        if filter := self.dump_filter():
            body["filter"] = filter
        return body

    def dump_filter(self, include_has_data: bool = False) -> dict[str, Any] | None:
        leaf_filters: list[dict[str, Any]] = []
        if self.space is not None:
            instance_type = self.instance_type or "node"
            leaf_filters.append(
                {
                    "in": {
                        "property": [instance_type, "space"],
                        "values": self.space,
                    }
                }
            )
        elif self.filter is not None:
            leaf_filters.append(self.filter)
        if include_has_data and self.source is not None:
            leaf_filters.append({"hasData": [self.source.dump(include_type=True)]})
        if len(leaf_filters) == 0:
            return None
        elif len(leaf_filters) == 1:
            return leaf_filters[0]
        else:
            return {"and": leaf_filters}


class AnnotationFilter(Filter):
    annotated_resource_type: Literal["file", "threedmodel"]
    annotated_resource_ids: list[ExternalId | InternalId]
    annotation_type: AnnotationType | None = None
    creating_app: str | None = None
    creating_app_version: str | None = None
    creating_user: str | None = None
    status: AnnotationStatus | None = None
    data: dict[str, JsonValue] | None = None


class ThreeDAssetMappingFilter(Filter): ...


class ThreeDAssetMappingAssetIdFilter(ThreeDAssetMappingFilter):
    asset_ids: list[int] = Field(max_length=100)


class ThreeDAssetMappingAssetInstanceIdFilter(ThreeDAssetMappingFilter):
    asset_instance_ids: list[NodeId] = Field(max_length=100)


class ThreeDAssetMapping3DNodeFilter(ThreeDAssetMappingFilter):
    node_ids: list[int] = Field(max_length=100)


class ThreeDAssetMappingTreeIndexFilter(ThreeDAssetMappingFilter):
    tree_indexes: list[int] = Field(max_length=100)


class ThreeDNodeNameFilter(Filter):
    """Filter 3D nodes by exact name.

    ``names`` is limited to 1000 entries.
    """

    names: list[str] = Field(min_length=1, max_length=1000)


class ThreeDNodePropertyFilter(Filter):
    """Filter 3D nodes by property values.

    ``properties`` maps a category to property names, and each property to the values that
    satisfy it. Values for one property are combined with OR. Properties and categories are
    combined with AND. At most 1000 property values are allowed in total.
    """

    properties: dict[str, dict[str, list[str]]]


class SequenceRowFilter(Filter):
    external_id: str
    start: int | None = None
    end: int | None = None
    columns: list[str] | None = None


class SimulatorFilter(Filter):
    simulator_external_ids: list[str] | None = None


class SimulatorModelFilter(SimulatorFilter):
    external_id_prefix: str | None = None
    data_set_ids: list[int] | None = None


class SimulatorModelRevisionFilter(SimulatorFilter):
    model_external_ids: list[str] | None = None
    all_versions: bool | None = None
    created_time: EpochTimestampRange | None = None
    last_updated_time: EpochTimestampRange | None = None


class SimulatorModelRoutineFilter(SimulatorFilter):
    model_external_ids: list[str] | None = None
    simulator_integration_external_ids: list[str] | None = None


class SimulatorModelRoutineRevisionFilter(SimulatorFilter):
    routine_external_ids: list[str] | None = None
    all_versions: bool | None = None
    model_external_ids: list[str] | None = None
    simulator_integration_external_ids: list[str] | None = None
    created_time: EpochTimestampRange | None = None


class ChartMonitorModelFilter(Filter):
    timeseries_id: list[int] | None = None


class FunctionFilter(Filter):
    """Filter for listing functions. None means no filter on that field, so None values are not sent."""

    name: str | None = None
    owner: str | None = None
    file_id: int | None = None
    status: FunctionStatus | str | None = None
    external_id_prefix: str | None = None
    created_time: EpochTimestampRange | None = None
    metadata: dict[str, str] | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return self.model_dump(mode="json", by_alias=camel_case, exclude_none=True)


class FunctionCallFilter(Filter):
    """Filter for listing calls of a single function."""

    schedule_id: int | None = None
    status: FunctionCallStatus | str | None = None
    start_time: EpochTimestampRange | None = None
    end_time: EpochTimestampRange | None = None


class ChartMonitorJobFilter(Filter):
    external_ids: list[str] | None = Field(default=None, max_length=100)
    ids: list[int] | None = Field(None, max_length=100)
    channel_ids: list[int] | None = None
    source: str | None = None
    model: ChartMonitorModelFilter | None = None


class WorkflowFilterItem(BaseModelRequest):
    """Match one workflow, or only one version of it when ``version`` is set."""

    external_id: str
    version: str | None = None


class WorkflowFilter(Filter):
    workflow_filters: list[WorkflowFilterItem] | None = Field(default=None, max_length=100)


class WorkflowExecutionFilter(WorkflowFilter):
    workflow_filters: list[WorkflowFilterItem] | None = Field(default=None, min_length=1, max_length=100)
    created_time_start: int | None = None
    created_time_end: int | None = None
    status: list[WorkflowExecutionStatus | str] | None = None
