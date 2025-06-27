from enum import Enum
from typing import Any, Literal, cast, Optional

import yaml
from cognite.client.data_classes.contextualization import (
    DiagramDetectConfig,
    ConnectionFlags,
    CustomizeFuzziness,
    DirectionWeights,
)
from cognite.client.data_classes.filters import Filter
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteAPIError
from pydantic import BaseModel, Field
from pydantic.alias_generators import to_camel
from utils.DataStructures import AnnotationStatus, FilterOperator


# Configuration Classes
class ViewPropertyConfig(BaseModel, alias_generator=to_camel):
    schema_space: str
    instance_space: Optional[str] = None
    external_id: str
    version: str
    annotation_type: Optional[Literal["diagrams.FileLink", "diagrams.AssetLink"]] = None

    def as_view_id(self) -> dm.ViewId:
        return dm.ViewId(
            space=self.schema_space, external_id=self.external_id, version=self.version
        )

    def as_property_ref(self, property) -> list[str]:
        return [self.schema_space, f"{self.external_id}/{self.version}", property]


class FilterConfig(BaseModel, alias_generator=to_camel):
    values: Optional[list[AnnotationStatus | str] | AnnotationStatus | str] = None
    negate: bool = False
    operator: FilterOperator
    target_property: str

    def as_filter(self, view_properties: ViewPropertyConfig) -> Filter:
        property_reference = view_properties.as_property_ref(self.target_property)

        # Converts enum value into string -> i.e.) in the case of AnnotationStatus
        if isinstance(self.values, list):
            find_values = [v.value if isinstance(v, Enum) else v for v in self.values]
        elif isinstance(self.values, Enum):
            find_values = self.values.value
        else:
            find_values = self.values

        filter: Filter
        if find_values is None:
            if self.operator == FilterOperator.EXISTS:
                filter = dm.filters.Exists(property=property_reference)
            else:
                raise ValueError(f"Operator {self.operator} requires a value")
        elif self.operator == FilterOperator.IN:
            if not isinstance(find_values, list):
                raise ValueError(
                    f"Operator 'IN' requires a list of values for property {self.target_property}"
                )
            filter = dm.filters.In(property=property_reference, values=find_values)
        elif self.operator == FilterOperator.EQUALS:
            filter = dm.filters.Equals(property=property_reference, value=find_values)
        elif self.operator == FilterOperator.CONTAINSALL:
            filter = dm.filters.ContainsAll(
                property=property_reference, values=find_values
            )
        elif self.operator == FilterOperator.SEARCH:
            filter = dm.filters.Search(property=property_reference, value=find_values)
        else:
            raise NotImplementedError(f"Operator {self.operator} is not implemented.")

        if self.negate:
            return dm.filters.Not(filter)
        else:
            return filter


class QueryConfig(BaseModel, alias_generator=to_camel):
    target_view: ViewPropertyConfig
    filters: list[FilterConfig]
    limit: Optional[int] = -1

    def build_filter(self) -> Filter:
        list_filters: list[Filter] = [
            f.as_filter(self.target_view) for f in self.filters
        ]

        if len(list_filters) == 1:
            return list_filters[0]
        else:
            return dm.filters.And(
                *list_filters
            )  # NOTE: '*' Unpacks each filter in the list


class ConnectionFlagsConfig(BaseModel, alias_generator=to_camel):
    no_text_inbetween: Optional[bool] = None
    natural_reading_order: Optional[bool] = None

    def as_connection_flag(self) -> ConnectionFlags:
        params = {
            key: value for key, value in self.model_dump().items() if value is not None
        }
        return ConnectionFlags(**params)


class CustomizeFuzzinessConfig(BaseModel, alias_generator=to_camel):
    fuzzy_score: Optional[float] = None
    max_boxes: Optional[int] = None
    min_chars: Optional[int] = None

    def as_customize_fuzziness(self) -> CustomizeFuzziness:
        params = {
            key: value for key, value in self.model_dump().items() if value is not None
        }
        return CustomizeFuzziness(**params)


class DirectionWeightsConfig(BaseModel, alias_generator=to_camel):
    left: Optional[float] = None
    right: Optional[float] = None
    up: Optional[float] = None
    down: Optional[float] = None

    def as_direction_weights(self) -> DirectionWeights:
        params = {
            key: value for key, value in self.model_dump().items() if value is not None
        }
        return DirectionWeights(**params)


class DiagramDetectConfigModel(BaseModel, alias_generator=to_camel):
    # NOTE: configs come from V7 of the cognite python sdk cognite SDK
    annotation_extract: Optional[bool] = None
    case_sensitive: Optional[bool] = None
    connection_flags: Optional[ConnectionFlagsConfig] = None
    customize_fuzziness: Optional[CustomizeFuzzinessConfig] = None
    direction_delta: Optional[float] = None
    direction_weights: Optional[DirectionWeightsConfig] = None
    min_fuzzy_score: Optional[float] = None
    read_embedded_text: Optional[bool] = None
    remove_leading_zeros: Optional[bool] = None
    substitutions: Optional[dict[str, list[str]]] = None

    def as_config(self) -> DiagramDetectConfig:
        params = {}
        if self.annotation_extract is not None:
            params["annotation_extract"] = self.annotation_extract
        if self.case_sensitive is not None:
            params["case_sensitive"] = self.case_sensitive
        if self.connection_flags is not None:
            params["connection_flags"] = self.connection_flags.as_connection_flag()
        if self.customize_fuzziness is not None:
            params["customize_fuzziness"] = (
                self.customize_fuzziness.as_customize_fuzziness()
            )
        if self.direction_delta is not None:
            params["direction_delta"] = self.direction_delta
        if self.direction_weights is not None:
            params["direction_weights"] = self.direction_weights.as_direction_weights()
        if self.min_fuzzy_score is not None:
            params["min_fuzzy_score"] = self.min_fuzzy_score
        if self.read_embedded_text is not None:
            params["read_embedded_text"] = self.read_embedded_text
        if self.remove_leading_zeros is not None:
            params["remove_leading_zeros"] = self.remove_leading_zeros
        if self.substitutions is not None:
            params["substitutions"] = self.substitutions

        return DiagramDetectConfig(**params)


# Launch Related Configs
class DataModelServiceConfig(BaseModel, alias_generator=to_camel):
    get_files_to_process_query: QueryConfig | list[QueryConfig]
    get_target_entities_query: QueryConfig | list[QueryConfig]
    get_file_entities_query: QueryConfig | list[QueryConfig]


class CacheServiceConfig(BaseModel, alias_generator=to_camel):
    cache_time_limit: int
    raw_db: str
    raw_table_cache: str


class AnnotationServiceConfig(BaseModel, alias_generator=to_camel):
    page_range: int = Field(gt=0, le=50)
    partial_match: bool = True
    min_tokens: int = 1
    diagram_detect_config: Optional[DiagramDetectConfigModel] = None


class PrepareFunction(BaseModel, alias_generator=to_camel):
    get_files_for_annotation_reset_query: Optional[QueryConfig | list[QueryConfig]] = (
        None
    )
    get_files_to_annotate_query: QueryConfig | list[QueryConfig]


class LaunchFunction(BaseModel, alias_generator=to_camel):
    batch_size: int = Field(gt=0, le=50)
    primary_scope_property: str
    secondary_scope_property: Optional[str] = None
    file_search_property: str = "aliases"
    target_entities_search_property: str = "aliases"
    data_model_service: DataModelServiceConfig
    cache_service: CacheServiceConfig
    annotation_service: AnnotationServiceConfig


# Finalize Related Configs
class RetrieveServiceConfig(BaseModel, alias_generator=to_camel):
    get_job_id_query: QueryConfig | list[QueryConfig]


class ApplyServiceConfig(BaseModel, alias_generator=to_camel):
    auto_approval_threshold: float = Field(gt=0.0, le=1.0)
    auto_suggest_threshold: float = Field(gt=0.0, le=1.0)


class ReportServiceConfig(BaseModel, alias_generator=to_camel):
    raw_db: str
    raw_table_doc_tag: str
    raw_table_doc_doc: str
    raw_batch_size: int


class FinalizeFunction(BaseModel, alias_generator=to_camel):
    clean_old_annotations: bool
    max_retry_attempts: int
    retrieve_service: RetrieveServiceConfig
    apply_service: ApplyServiceConfig
    report_service: ReportServiceConfig


class DataModelViews(BaseModel, alias_generator=to_camel):
    core_annotation_view: ViewPropertyConfig
    annotation_state_view: ViewPropertyConfig
    file_view: ViewPropertyConfig
    target_entities_view: ViewPropertyConfig


class Config(BaseModel, alias_generator=to_camel):
    data_model_views: DataModelViews
    prepare_function: PrepareFunction
    launch_function: LaunchFunction
    finalize_function: FinalizeFunction

    @classmethod
    def parse_direct_relation(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return dm.DirectRelationReference.load(value)
        return value


# Functions to construct queries
def get_limit_from_query(query: QueryConfig | list[QueryConfig]) -> int:
    """
    Determines the retrieval limit from a query configuration.
    Handles 'None' by treating it as the default -1 (unlimited).
    """
    default_limit = -1
    if isinstance(query, list):
        if not query:
            return default_limit
        limits = [q.limit if q.limit is not None else default_limit for q in query]
        return max(limits)
    else:
        return query.limit if query.limit is not None else default_limit


def build_filter_from_query(query: QueryConfig | list[QueryConfig]) -> Filter:
    """
    Builds a Cognite Filter from a query configuration.

    If the query is a list, it builds a filter for each item and combines them with a logical OR.
    If the query is a single object, it builds the filter directly from it.
    """
    if isinstance(query, list):
        list_filters: list[Filter] = [q.build_filter() for q in query]
        if not list_filters:
            raise ValueError("Query list cannot be empty.")
        return (
            dm.filters.Or(*list_filters) if len(list_filters) > 1 else list_filters[0]
        )
    else:
        return query.build_filter()


def load_config_parameters(
    client: CogniteClient,
    function_data: dict[str, Any],
) -> Config:
    """
    Retrieves the configuration parameters from the function data and loads the configuration from CDF.
    """
    if "ExtractionPipelineExtId" not in function_data:
        raise ValueError(
            "Missing key 'ExtractionPipelineExtId' in input data to the function"
        )

    pipeline_ext_id = function_data["ExtractionPipelineExtId"]
    try:
        raw_config = client.extraction_pipelines.config.retrieve(pipeline_ext_id)
        if raw_config.config is None:
            raise ValueError(
                f"No config found for extraction pipeline: {pipeline_ext_id!r}"
            )
    except CogniteAPIError:
        raise RuntimeError(
            f"Not able to retrieve pipeline config for extraction pipeline: {pipeline_ext_id!r}"
        )

    loaded_yaml_data = yaml.safe_load(raw_config.config)

    if isinstance(loaded_yaml_data, dict):
        return Config.model_validate(loaded_yaml_data)
    else:
        raise ValueError(
            "Invalid configuration structure from CDF: \nExpected a YAML dictionary with a top-level 'config' key."
        )
