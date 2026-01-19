from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property
from typing import Any

import pytest

from cognite_toolkit._cdf_tk.client.api.agents import AgentsAPI
from cognite_toolkit._cdf_tk.client.api.assets import AssetsAPI
from cognite_toolkit._cdf_tk.client.api.containers import ContainersAPI
from cognite_toolkit._cdf_tk.client.api.data_models import DataModelsAPI
from cognite_toolkit._cdf_tk.client.api.datasets import DataSetsAPI
from cognite_toolkit._cdf_tk.client.api.events import EventsAPI
from cognite_toolkit._cdf_tk.client.api.extraction_pipelines import ExtractionPipelinesAPI
from cognite_toolkit._cdf_tk.client.api.functions import FunctionsAPI
from cognite_toolkit._cdf_tk.client.api.groups import GroupsAPI
from cognite_toolkit._cdf_tk.client.api.hosted_extractor_destinations import HostedExtractorDestinationsAPI
from cognite_toolkit._cdf_tk.client.api.hosted_extractor_jobs import HostedExtractorJobsAPI
from cognite_toolkit._cdf_tk.client.api.hosted_extractor_mappings import HostedExtractorMappingsAPI
from cognite_toolkit._cdf_tk.client.api.hosted_extractor_sources import HostedExtractorSourcesAPI
from cognite_toolkit._cdf_tk.client.api.instances import InstancesAPI
from cognite_toolkit._cdf_tk.client.api.labels import LabelsAPI
from cognite_toolkit._cdf_tk.client.api.raw import RawDatabasesAPI
from cognite_toolkit._cdf_tk.client.api.relationships import RelationshipsAPI
from cognite_toolkit._cdf_tk.client.api.security_categories import SecurityCategoriesAPI
from cognite_toolkit._cdf_tk.client.api.sequences import SequencesAPI
from cognite_toolkit._cdf_tk.client.api.simulator_models import SimulatorModelsAPI
from cognite_toolkit._cdf_tk.client.api.spaces import SpacesAPI
from cognite_toolkit._cdf_tk.client.api.three_d import ThreeDClassicModelsAPI
from cognite_toolkit._cdf_tk.client.api.timeseries import TimeSeriesAPI
from cognite_toolkit._cdf_tk.client.api.transformations import TransformationsAPI
from cognite_toolkit._cdf_tk.client.api.views import ViewsAPI
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI
from cognite_toolkit._cdf_tk.client.resource_classes.agent import AgentRequest, AgentResponse
from cognite_toolkit._cdf_tk.client.resource_classes.annotation import AnnotationRequest, AnnotationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest, AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.base import Identifier, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerRequest,
    ContainerResponse,
    DataModelRequest,
    DataModelResponse,
    EdgeRequest,
    EdgeResponse,
    NodeRequest,
    NodeResponse,
    SpaceRequest,
    SpaceResponse,
    ViewRequest,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest, DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventRequest, EventResponse
from cognite_toolkit._cdf_tk.client.resource_classes.extraction_pipeline import (
    ExtractionPipelineRequest,
    ExtractionPipelineResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionRequest, FunctionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import (
    FunctionScheduleRequest,
    FunctionScheduleResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.graphql_data_model import (
    GraphQLDataModelRequest,
    GraphQLDataModelResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.group import GroupRequest, GroupResponse
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_destination import (
    HostedExtractorDestinationRequest,
    HostedExtractorDestinationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import (
    HostedExtractorJobRequest,
    HostedExtractorJobResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_mapping import (
    HostedExtractorMappingRequest,
    HostedExtractorMappingResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_source import (
    KafkaSourceRequest,
    KafkaSourceResponse,
    MQTTSourceRequest,
    MQTTSourceResponse,
    RESTSourceRequest,
    RESTSourceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.label import LabelRequest, LabelResponse
from cognite_toolkit._cdf_tk.client.resource_classes.location_filter import (
    LocationFilterRequest,
    LocationFilterResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.raw import RAWDatabase, RAWTable
from cognite_toolkit._cdf_tk.client.resource_classes.relationship import RelationshipRequest, RelationshipResponse
from cognite_toolkit._cdf_tk.client.resource_classes.robotics import (
    RobotCapabilityRequest,
    RobotCapabilityResponse,
    RobotDataPostProcessingRequest,
    RobotDataPostProcessingResponse,
    RobotFrameRequest,
    RobotFrameResponse,
    RobotLocationRequest,
    RobotLocationResponse,
    RobotMapRequest,
    RobotMapResponse,
    RobotRequest,
    RobotResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.search_config_resource import (
    SearchConfigRequest,
    SearchConfigResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.securitycategory import (
    SecurityCategoryRequest,
    SecurityCategoryResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.sequence import SequenceRequest, SequenceResponse
from cognite_toolkit._cdf_tk.client.resource_classes.sequence_rows import SequenceRowsRequest, SequenceRowsResponse
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_model import (
    SimulatorModelRequest,
    SimulatorModelResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.streamlit_ import StreamlitRequest, StreamlitResponse
from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamRequest, StreamResponse
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import ThreeDModelClassicRequest, ThreeDModelResponse
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationRequest, TransformationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow import WorkflowRequest, WorkflowResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import (
    WorkflowTriggerRequest,
    WorkflowTriggerResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import (
    WorkflowVersionRequest,
    WorkflowVersionResponse,
)


@dataclass
class CDFResource:
    response_cls: type[ResponseResource]
    request_cls: type[RequestResource]
    example_data: dict[str, Any]
    api_class: type[CDFResourceAPI] | None = None
    is_dump_equal_to_example: bool = True
    is_as_request_possible: bool = True

    @cached_property
    def response_instance(self) -> ResponseResource:
        return self.response_cls.model_validate(self.example_data)

    @cached_property
    def request_instance(self) -> RequestResource:
        return self.response_instance.as_request_resource()

    @cached_property
    def resource_id(self) -> Identifier:
        return self.request_instance.as_id()


def get_example_minimum_responses(resource_cls: type[ResponseResource]) -> dict[str, Any]:
    """Return an example with the only required and identifier fields for the given resource class."""
    responses: dict[type[ResponseResource], dict[str, Any]] = {
        AssetResponse: {
            "id": 123,
            "externalId": "asset_001",
            "name": "Example Asset",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
            "rootId": 1,
        },
        TimeSeriesResponse: {
            "id": 456,
            "externalId": "ts_001",
            "isString": False,
            "isStep": False,
            "type": "numeric",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        EventResponse: {
            "id": 789,
            "externalId": "event_001",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        FileMetadataResponse: {
            "id": 101,
            "externalId": "file_001",
            "name": "example.pdf",
            "uploaded": True,
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        AgentResponse: {
            "externalId": "agent_001",
            "name": "Example Agent",
            "ownerId": "user@example.com",
            "runtimeVersion": "v1",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        AnnotationResponse: {
            "id": 4096,
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
            "annotatedResourceType": "file",
            "annotatedResourceId": 1337,
            "annotationType": "images.Classification",
            "creatingApp": "cognite-toolkit",
            "creatingAppVersion": "1.0.0",
            "creatingUser": "user@example.com",
            "data": {"label": "pump"},
            "status": "approved",
        },
        RAWDatabase: {
            "name": "example_db",
        },
        RAWTable: {
            "dbName": "example_db",
            "name": "example_table",
        },
        SimulatorModelResponse: {
            "id": 111,
            "externalId": "simulator_model_001",
            "simulatorExternalId": "simulator_001",
            "name": "Example Simulator Model",
            "dataSetId": 123456,
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
            # 'type' is not required in the response, but is required in the request. Likely a bug in the CDF API docs.
            "type": "default",
        },
        SpaceResponse: {
            "space": "my_space",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
            "isGlobal": False,
        },
        ContainerResponse: {
            "space": "my_space",
            "externalId": "my_container",
            "properties": {
                "name": {
                    "type": {"type": "text", "list": False, "collation": "ucs_basic"},
                    "nullable": True,
                },
            },
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
            "isGlobal": False,
        },
        DataModelResponse: {
            "space": "my_space",
            "externalId": "my_data_model",
            "version": "1",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
            "isGlobal": False,
        },
        ViewResponse: {
            "space": "my_space",
            "externalId": "my_view",
            "version": "1",
            "filter": None,
            "properties": {},
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
            "writable": True,
            "queryable": True,
            "usedFor": "node",
            "isGlobal": False,
            "mappedContainers": [],
        },
        NodeResponse: {
            "space": "my_space",
            "externalId": "my_node",
            "instanceType": "node",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
            "version": 1,
            "properties": {
                "my_space": {
                    "MyView/v1": {
                        "propertyA": "valueA",
                    }
                }
            },
        },
        EdgeResponse: {
            "space": "my_space",
            "externalId": "my_edge",
            "instanceType": "edge",
            "type": {
                "space": "my_space",
                "externalId": "my_node_type",
            },
            "startNode": {
                "space": "my_space",
                "externalId": "start_node",
            },
            "endNode": {
                "space": "my_space",
                "externalId": "end_node",
            },
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
            "version": 1,
            "properties": {
                "my_space": {
                    "MyView/v1": {
                        "propertyB": "valueB",
                    }
                }
            },
        },
        SecurityCategoryResponse: {
            "id": 201,
            "name": "security_category_001",
        },
        SequenceResponse: {
            "id": 202,
            "externalId": "sequence_001",
            "columns": [
                {"externalId": "col_1", "valueType": "STRING"},
            ],
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        DataSetResponse: {
            "id": 203,
            "externalId": "dataset_001",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        LabelResponse: {
            "externalId": "label_001",
            "name": "Example Label",
            "createdTime": 1622547800000,
        },
        ExtractionPipelineResponse: {
            "id": 204,
            "externalId": "extraction_pipeline_001",
            "name": "Example Extraction Pipeline",
            "dataSetId": 123456,
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        KafkaSourceResponse: {
            "type": "kafka",
            "externalId": "kafka_source_001",
            "bootstrapBrokers": [{"host": "localhost", "port": 9092}],
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        MQTTSourceResponse: {
            "type": "mqtt5",
            "externalId": "mqtt_source_001",
            "host": "localhost",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        RESTSourceResponse: {
            "type": "rest",
            "externalId": "rest_source_001",
            "host": "api.example.com",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        HostedExtractorMappingResponse: {
            "externalId": "mapping_001",
            "mapping": {"expression": "SELECT * FROM source"},
            "published": True,
            "input": {"type": "json"},
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        HostedExtractorJobResponse: {
            "externalId": "job_001",
            "destinationId": "destination_001",
            "sourceId": "source_001",
            "format": {"type": "cognite"},
            "config": {"topicFilter": "my/topic"},
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        HostedExtractorDestinationResponse: {
            "externalId": "destination_001",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        TransformationResponse: {
            "id": 205,
            "externalId": "transformation_001",
            "name": "Example Transformation",
            "ignoreNullFields": True,
            "query": "SELECT * FROM source",
            "isPublic": True,
            "conflictMode": "upsert",
            "destination": {"type": "assets"},
            "owner": "user@example.com",
            "ownerIsCurrentUser": True,
            "hasSourceOidcCredentials": False,
            "hasDestinationOidcCredentials": False,
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        WorkflowResponse: {
            "externalId": "workflow_001",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        WorkflowVersionResponse: {
            "workflowExternalId": "workflow_001",
            "version": "1",
            "workflowDefinition": {
                "tasks": [
                    {
                        "externalId": "task_001",
                        "type": "function",
                        "parameters": {
                            "function": {"externalId": "my_function"},
                        },
                    }
                ]
            },
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        WorkflowTriggerResponse: {
            "externalId": "trigger_001",
            "triggerRule": {
                "triggerType": "schedule",
                "cronExpression": "0 0 * * *",
            },
            "workflowExternalId": "workflow_001",
            "workflowVersion": "1",
            "isPaused": False,
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        GroupResponse: {
            "id": 202,
            "externalId": "group_001",
            "name": "Example Group",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        SequenceRowsResponse: {
            "id": 123,
            "externalId": "sequence_001",
            "columns": [{"externalId": "col1", "valueType": "LONG"}, {"externalId": "col2", "valueType": "DOUBLE"}],
            "rows": [
                {"rowNumber": 0, "values": ["value1", 123]},
                {"rowNumber": 1, "values": ["value2", 456]},
            ],
        },
        SearchConfigResponse: {
            "id": 789,
            "view": {
                "space": "my_space",
                "externalId": "my_view",
            },
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        GraphQLDataModelResponse: {
            "space": "my_space",
            "externalId": "my_graphql_model",
            "version": "1",
            "isGlobal": False,
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        FunctionResponse: {
            "id": 456,
            "externalId": "function_001",
            "name": "My Function",
            "fileId": 789,
            "createdTime": 1622547800000,
            "status": "Ready",
        },
        FunctionScheduleResponse: {
            "id": 321,
            "name": "My Schedule",
            "cronExpression": "0 0 * * *",
            "createdTime": 1622547800000,
            "when": "0 0 * * *",
            "functionId": 456,
            "functionExternalId": "function_001",
        },
        StreamlitResponse: {
            "externalId": "streamlit_001",
            "name": "My Streamlit App",
            "creator": "user@example.com",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        LocationFilterResponse: {
            "id": 654,
            "externalId": "location_001",
            "name": "My Location",
            "createdTime": 1622547800000,
            "updatedTime": 1622547800000,
        },
        RelationshipResponse: {
            "externalId": "relationship_001",
            "sourceExternalId": "asset_001",
            "sourceType": "asset",
            "targetExternalId": "timeseries_001",
            "targetType": "timeSeries",
            "createdTime": 1622547800000,
            "lastUpdatedTime": 1622547800000,
        },
        RobotFrameResponse: {
            "externalId": "frame_001",
            "name": "Example Frame",
            "createdTime": 1622547800000,
            "updatedTime": 1622547800000,
        },
        RobotCapabilityResponse: {
            "externalId": "capability_001",
            "name": "PTZ Camera",
            "method": "capture_ptz",
            "inputSchema": {"type": "object"},
            "dataHandlingSchema": {"type": "object"},
            "createdTime": 1622547800000,
            "updatedTime": 1622547800000,
        },
        RobotLocationResponse: {
            "externalId": "location_001",
            "name": "Factory Floor",
            "createdTime": 1622547800000,
            "updatedTime": 1622547800000,
        },
        RobotResponse: {
            "name": "Spot-001",
            "capabilities": ["capability_001"],
            "robotType": "SPOT",
            "dataSetId": 123456,
            "createdTime": 1622547800000,
            "updatedTime": 1622547800000,
        },
        RobotDataPostProcessingResponse: {
            "externalId": "postprocessing_001",
            "name": "Gauge Reader",
            "method": "read_gauge",
            "inputSchema": {"type": "object"},
            "createdTime": 1622547800000,
            "updatedTime": 1622547800000,
        },
        RobotMapResponse: {
            "externalId": "map_001",
            "name": "Factory Map",
            "mapType": "THREEDMODEL",
            "createdTime": 1622547800000,
            "updatedTime": 1622547800000,
        },
        StreamResponse: {
            "externalId": "stream_001",
            "createdTime": 1622547800000,
            "createdFromTemplate": "ImmutableTestStream",
            "type": "Immutable",
        },
        ThreeDModelResponse: {
            "id": 123,
            "name": "Example 3D Model",
            "createdTime": 1622547800000,
        },
    }
    try:
        return responses[resource_cls]
    except KeyError:
        raise ValueError(f"No example response defined for {resource_cls}")


def iterate_cdf_resources() -> Iterable[tuple]:
    yield pytest.param(
        CDFResource(
            response_cls=AssetResponse,
            request_cls=AssetRequest,
            example_data=get_example_minimum_responses(AssetResponse),
            api_class=AssetsAPI,
        ),
        id="Asset",
    )
    yield pytest.param(
        CDFResource(
            response_cls=TimeSeriesResponse,
            request_cls=TimeSeriesRequest,
            example_data=get_example_minimum_responses(TimeSeriesResponse),
            api_class=TimeSeriesAPI,
        ),
        id="TimeSeries",
    )
    yield pytest.param(
        CDFResource(
            response_cls=EventResponse,
            request_cls=EventRequest,
            example_data=get_example_minimum_responses(EventResponse),
            api_class=EventsAPI,
        ),
        id="Event",
    )
    yield pytest.param(
        CDFResource(
            response_cls=FileMetadataResponse,
            request_cls=FileMetadataRequest,
            example_data=get_example_minimum_responses(FileMetadataResponse),
            # FileMetadata API requires custom mocking.
        ),
        id="FileMetadata",
    )
    yield pytest.param(
        CDFResource(
            response_cls=AgentResponse,
            request_cls=AgentRequest,
            example_data=get_example_minimum_responses(AgentResponse),
            api_class=AgentsAPI,
        ),
        id="Agent",
    )
    yield pytest.param(
        CDFResource(
            response_cls=AnnotationResponse,
            request_cls=AnnotationRequest,
            example_data=get_example_minimum_responses(AnnotationResponse),
            # We cannot use the generic tests AnnotationsAPI for the Annotation resource, as it
            # requires an Annotation filter in the list/iterate/paginate methods.
        ),
        id="Annotation",
    )
    yield pytest.param(
        CDFResource(
            response_cls=RAWDatabase,
            request_cls=RAWDatabase,
            example_data=get_example_minimum_responses(RAWDatabase),
            api_class=RawDatabasesAPI,
        ),
        id="RAWDatabase",
    )
    yield pytest.param(
        CDFResource(
            response_cls=RAWTable,
            request_cls=RAWTable,
            example_data=get_example_minimum_responses(RAWTable),
            is_dump_equal_to_example=False,
            # We cannot use the generic tests RAWTableAPI for the RAWTable resource, as it requires db_name as
            # a path parameter and thus custom endpoint mocking.
        ),
        id="RAWTable",
    )
    yield pytest.param(
        CDFResource(
            response_cls=SimulatorModelResponse,
            request_cls=SimulatorModelRequest,
            example_data=get_example_minimum_responses(SimulatorModelResponse),
            api_class=SimulatorModelsAPI,
        ),
        id="SimulatorModel",
    )
    yield pytest.param(
        CDFResource(
            response_cls=SpaceResponse,
            request_cls=SpaceRequest,
            example_data=get_example_minimum_responses(SpaceResponse),
            api_class=SpacesAPI,
        ),
        id="Space",
    )
    yield pytest.param(
        CDFResource(
            response_cls=ContainerResponse,
            request_cls=ContainerRequest,
            example_data=get_example_minimum_responses(ContainerResponse),
            api_class=ContainersAPI,
        ),
        id="Container",
    )
    yield pytest.param(
        CDFResource(
            response_cls=DataModelResponse,
            request_cls=DataModelRequest,
            example_data=get_example_minimum_responses(DataModelResponse),
            api_class=DataModelsAPI,
        ),
        id="DataModel",
    )
    yield pytest.param(
        CDFResource(
            response_cls=ViewResponse,
            request_cls=ViewRequest,
            example_data=get_example_minimum_responses(ViewResponse),
            api_class=ViewsAPI,
        ),
        id="View",
    )
    yield pytest.param(
        CDFResource(
            response_cls=NodeResponse,
            request_cls=NodeRequest,
            example_data=get_example_minimum_responses(NodeResponse),
            api_class=InstancesAPI,
        ),
        id="Node",
    )
    yield pytest.param(
        CDFResource(
            response_cls=EdgeResponse,
            request_cls=EdgeRequest,
            example_data=get_example_minimum_responses(EdgeResponse),
            api_class=InstancesAPI,
        ),
        id="Edge",
    )
    yield pytest.param(
        CDFResource(
            response_cls=SecurityCategoryResponse,
            request_cls=SecurityCategoryRequest,
            example_data=get_example_minimum_responses(SecurityCategoryResponse),
            api_class=SecurityCategoriesAPI,
        ),
        id="SecurityCategory",
    )
    yield pytest.param(
        CDFResource(
            response_cls=SequenceResponse,
            request_cls=SequenceRequest,
            example_data=get_example_minimum_responses(SequenceResponse),
            api_class=SequencesAPI,
        ),
        id="Sequence",
    )
    yield pytest.param(
        CDFResource(
            response_cls=DataSetResponse,
            request_cls=DataSetRequest,
            example_data=get_example_minimum_responses(DataSetResponse),
            api_class=DataSetsAPI,
        ),
        id="DataSet",
    )
    yield pytest.param(
        CDFResource(
            response_cls=LabelResponse,
            request_cls=LabelRequest,
            example_data=get_example_minimum_responses(LabelResponse),
            api_class=LabelsAPI,
        ),
        id="Label",
    )
    yield pytest.param(
        CDFResource(
            response_cls=ExtractionPipelineResponse,
            request_cls=ExtractionPipelineRequest,
            example_data=get_example_minimum_responses(ExtractionPipelineResponse),
            api_class=ExtractionPipelinesAPI,
        ),
        id="ExtractionPipeline",
    )
    yield pytest.param(
        CDFResource(
            response_cls=KafkaSourceResponse,
            request_cls=KafkaSourceRequest,
            example_data=get_example_minimum_responses(KafkaSourceResponse),
            api_class=HostedExtractorSourcesAPI,
        ),
        id="HostedExtractorKafkaSource",
    )
    yield pytest.param(
        CDFResource(
            response_cls=MQTTSourceResponse,
            request_cls=MQTTSourceRequest,
            example_data=get_example_minimum_responses(MQTTSourceResponse),
            api_class=HostedExtractorSourcesAPI,
        ),
        id="HostedExtractorMQTTSource",
    )
    yield pytest.param(
        CDFResource(
            response_cls=RESTSourceResponse,
            request_cls=RESTSourceRequest,
            example_data=get_example_minimum_responses(RESTSourceResponse),
            api_class=HostedExtractorSourcesAPI,
        ),
        id="HostedExtractorRESTSource",
    )
    yield pytest.param(
        CDFResource(
            response_cls=HostedExtractorMappingResponse,
            request_cls=HostedExtractorMappingRequest,
            example_data=get_example_minimum_responses(HostedExtractorMappingResponse),
            api_class=HostedExtractorMappingsAPI,
        ),
        id="HostedExtractorMapping",
    )
    yield pytest.param(
        CDFResource(
            response_cls=HostedExtractorJobResponse,
            request_cls=HostedExtractorJobRequest,
            example_data=get_example_minimum_responses(HostedExtractorJobResponse),
            api_class=HostedExtractorJobsAPI,
        ),
        id="HostedExtractorJob",
    )
    yield pytest.param(
        CDFResource(
            response_cls=HostedExtractorDestinationResponse,
            request_cls=HostedExtractorDestinationRequest,
            example_data=get_example_minimum_responses(HostedExtractorDestinationResponse),
            api_class=HostedExtractorDestinationsAPI,
        ),
        id="HostedExtractorDestination",
    )
    yield pytest.param(
        CDFResource(
            response_cls=TransformationResponse,
            request_cls=TransformationRequest,
            example_data=get_example_minimum_responses(TransformationResponse),
            api_class=TransformationsAPI,
        ),
        id="Transformation",
    )

    yield pytest.param(
        CDFResource(
            response_cls=WorkflowResponse,
            request_cls=WorkflowRequest,
            example_data=get_example_minimum_responses(WorkflowResponse),
            # Workflows cannot be tested in generic API tests due to
            # custom endpoint mocking.
        ),
        id="Workflow",
    )
    yield pytest.param(
        CDFResource(
            response_cls=WorkflowVersionResponse,
            request_cls=WorkflowVersionRequest,
            example_data=get_example_minimum_responses(WorkflowVersionResponse),
            # WorkflowVersion cannot be tested in generic API test due to custom
            # endpoint mocking.
        ),
        id="WorkflowVersion",
    )
    yield pytest.param(
        CDFResource(
            response_cls=WorkflowTriggerResponse,
            request_cls=WorkflowTriggerRequest,
            example_data=get_example_minimum_responses(WorkflowTriggerResponse),
            is_dump_equal_to_example=True,
            is_as_request_possible=False,
            # WorkflowTrigger cannot be tested in generic API due to
            # it cannot create as_request from response.
        ),
        id="WorkflowTrigger",
    )
    yield pytest.param(
        CDFResource(
            response_cls=GroupResponse,
            request_cls=GroupRequest,
            example_data=get_example_minimum_responses(GroupResponse),
            api_class=GroupsAPI,
        ),
        id="Group",
    )
    yield pytest.param(
        CDFResource(
            response_cls=SequenceRowsResponse,
            request_cls=SequenceRowsRequest,
            example_data=get_example_minimum_responses(SequenceRowsResponse),
        ),
        id="SequenceRows",
    )
    yield pytest.param(
        CDFResource(
            response_cls=SearchConfigResponse,
            request_cls=SearchConfigRequest,
            example_data=get_example_minimum_responses(SearchConfigResponse),
        ),
        id="SearchConfig",
    )
    yield pytest.param(
        CDFResource(
            response_cls=GraphQLDataModelResponse,
            request_cls=GraphQLDataModelRequest,
            example_data=get_example_minimum_responses(GraphQLDataModelResponse),
            # GraphQLDataModel cannot be tested in generic API tests due to
            # custom endpoint mocking.
        ),
        id="GraphQLDataModel",
    )
    yield pytest.param(
        CDFResource(
            response_cls=FunctionResponse,
            request_cls=FunctionRequest,
            example_data=get_example_minimum_responses(FunctionResponse),
            api_class=FunctionsAPI,
        ),
        id="Function",
    )
    yield pytest.param(
        CDFResource(
            response_cls=FunctionScheduleResponse,
            request_cls=FunctionScheduleRequest,
            example_data=get_example_minimum_responses(FunctionScheduleResponse),
            is_dump_equal_to_example=False,
            is_as_request_possible=False,
            # FunctionSchedule cannot be tested in generic API due to
            # it cannot create as_request from response.
        ),
        id="FunctionSchedule",
    )
    yield pytest.param(
        CDFResource(
            response_cls=StreamlitResponse,
            request_cls=StreamlitRequest,
            example_data=get_example_minimum_responses(StreamlitResponse),
        ),
        id="Streamlit",
    )
    yield pytest.param(
        CDFResource(
            response_cls=LocationFilterResponse,
            request_cls=LocationFilterRequest,
            example_data=get_example_minimum_responses(LocationFilterResponse),
        ),
        id="LocationFilter",
    )
    yield pytest.param(
        CDFResource(
            response_cls=RelationshipResponse,
            request_cls=RelationshipRequest,
            example_data=get_example_minimum_responses(RelationshipResponse),
            api_class=RelationshipsAPI,
        ),
        id="Relationship",
    )
    yield pytest.param(
        CDFResource(
            response_cls=RobotFrameResponse,
            request_cls=RobotFrameRequest,
            example_data=get_example_minimum_responses(RobotFrameResponse),
        ),
        id="RobotFrame",
    )
    yield pytest.param(
        CDFResource(
            response_cls=RobotCapabilityResponse,
            request_cls=RobotCapabilityRequest,
            example_data=get_example_minimum_responses(RobotCapabilityResponse),
        ),
        id="RobotCapability",
    )
    yield pytest.param(
        CDFResource(
            response_cls=RobotLocationResponse,
            request_cls=RobotLocationRequest,
            example_data=get_example_minimum_responses(RobotLocationResponse),
        ),
        id="RobotLocation",
    )
    yield pytest.param(
        CDFResource(
            response_cls=RobotResponse,
            request_cls=RobotRequest,
            example_data=get_example_minimum_responses(RobotResponse),
        ),
        id="Robot",
    )
    yield pytest.param(
        CDFResource(
            response_cls=RobotDataPostProcessingResponse,
            request_cls=RobotDataPostProcessingRequest,
            example_data=get_example_minimum_responses(RobotDataPostProcessingResponse),
        ),
        id="RobotDataPostProcessing",
    )
    yield pytest.param(
        CDFResource(
            response_cls=RobotMapResponse,
            request_cls=RobotMapRequest,
            example_data=get_example_minimum_responses(RobotMapResponse),
        ),
        id="RobotMap",
    )
    yield pytest.param(
        CDFResource(
            response_cls=StreamResponse,
            request_cls=StreamRequest,
            example_data=get_example_minimum_responses(StreamResponse),
            # StreamsAPI uses path parameters for retrieve/delete, so generic API tests
            # do not work directly without custom mocking.
        ),
        id="Stream",
    )
    yield pytest.param(
        CDFResource(
            response_cls=ThreeDModelResponse,
            request_cls=ThreeDModelClassicRequest,
            example_data=get_example_minimum_responses(ThreeDModelResponse),
            api_class=ThreeDClassicModelsAPI,
        ),
        id="ThreeDClassicModel",
    )
