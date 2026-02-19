import io
import types
import zipfile
from collections.abc import Callable, Hashable, Iterable, Set
from typing import Annotated, Any, cast, get_args, get_origin

import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import RequestResource, T_ResponseResource
from cognite_toolkit._cdf_tk.client.api.cognite_files import CogniteFilesAPI
from cognite_toolkit._cdf_tk.client.api.data_product_versions import DataProductVersionsAPI
from cognite_toolkit._cdf_tk.client.api.data_products import DataProductsAPI
from cognite_toolkit._cdf_tk.client.api.datasets import DataSetsAPI
from cognite_toolkit._cdf_tk.client.api.extraction_pipeline_config import ExtractionPipelineConfigsAPI
from cognite_toolkit._cdf_tk.client.api.function_schedules import FunctionSchedulesAPI
from cognite_toolkit._cdf_tk.client.api.functions import FunctionsAPI
from cognite_toolkit._cdf_tk.client.api.hosted_extractor_jobs import HostedExtractorJobsAPI
from cognite_toolkit._cdf_tk.client.api.infield import APMConfigAPI, InFieldCDMConfigAPI
from cognite_toolkit._cdf_tk.client.api.instances import InstancesAPI, WrappedInstancesAPI
from cognite_toolkit._cdf_tk.client.api.location_filters import LocationFiltersAPI
from cognite_toolkit._cdf_tk.client.api.migration import ResourceViewMappingsAPI
from cognite_toolkit._cdf_tk.client.api.raw import RawDatabasesAPI, RawTablesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_capabilities import CapabilitiesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_data_postprocessing import DataPostProcessingAPI
from cognite_toolkit._cdf_tk.client.api.robotics_frames import FramesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_locations import LocationsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_maps import MapsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_robots import RobotsAPI
from cognite_toolkit._cdf_tk.client.api.search_config import SearchConfigurationsAPI
from cognite_toolkit._cdf_tk.client.api.security_categories import SecurityCategoriesAPI
from cognite_toolkit._cdf_tk.client.api.sequence_rows import SequenceRowsAPI
from cognite_toolkit._cdf_tk.client.api.simulator_model_revisions import SimulatorModelRevisionsAPI
from cognite_toolkit._cdf_tk.client.api.simulator_models import SimulatorModelsAPI
from cognite_toolkit._cdf_tk.client.api.simulator_routine_revisions import SimulatorRoutineRevisionsAPI
from cognite_toolkit._cdf_tk.client.api.simulator_routines import SimulatorRoutinesAPI
from cognite_toolkit._cdf_tk.client.api.streams import StreamsAPI
from cognite_toolkit._cdf_tk.client.api.three_d import (
    ThreeDClassicAssetMappingAPI,
    ThreeDClassicModelsAPI,
    ThreeDClassicRevisionsAPI,
    ThreeDDMAssetMappingAPI,
)
from cognite_toolkit._cdf_tk.client.api.transformation_notifications import TransformationNotificationsAPI
from cognite_toolkit._cdf_tk.client.api.transformation_schedules import TransformationSchedulesAPI
from cognite_toolkit._cdf_tk.client.api.transformations import TransformationsAPI
from cognite_toolkit._cdf_tk.client.api.workflow_triggers import WorkflowTriggersAPI
from cognite_toolkit._cdf_tk.client.api.workflow_versions import WorkflowVersionsAPI
from cognite_toolkit._cdf_tk.client.cdf_client.api import CDFResourceAPI, Endpoint
from cognite_toolkit._cdf_tk.client.http_client import RequestMessage, SuccessResponse, ToolkitAPIError
from cognite_toolkit._cdf_tk.client.request_classes.filters import SequenceRowFilter
from cognite_toolkit._cdf_tk.client.resource_classes.agent import AgentRequest
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import APMConfigRequest
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest, AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.cognite_file import CogniteFileRequest
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerRequest,
    DataModelRequest,
    EdgeRequest,
    NodeRequest,
    SpaceRequest,
    ViewRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest, DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventRequest, EventResponse
from cognite_toolkit._cdf_tk.client.resource_classes.extraction_pipeline import (
    ExtractionPipelineRequest,
    ExtractionPipelineResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.extraction_pipeline_config import ExtractionPipelineConfigRequest
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionRequest
from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import FunctionScheduleRequest
from cognite_toolkit._cdf_tk.client.resource_classes.graphql_data_model import GraphQLDataModelRequest
from cognite_toolkit._cdf_tk.client.resource_classes.group import GroupRequest
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_destination import (
    HostedExtractorDestinationRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import HostedExtractorJobRequest
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_mapping import HostedExtractorMappingRequest
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_source import (
    EventHubSourceRequest,
    KafkaSourceRequest,
    MQTTSourceRequest,
    RESTSourceRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import (
    ExtractionPipelineConfigId,
    InternalId,
    InternalIdUnwrapped,
    ThreeDModelRevisionId,
)
from cognite_toolkit._cdf_tk.client.resource_classes.infield import InFieldCDMLocationConfigRequest
from cognite_toolkit._cdf_tk.client.resource_classes.label import LabelRequest
from cognite_toolkit._cdf_tk.client.resource_classes.location_filter import LocationFilterRequest
from cognite_toolkit._cdf_tk.client.resource_classes.raw import (
    RAWDatabaseRequest,
    RAWTableRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.relationship import RelationshipRequest
from cognite_toolkit._cdf_tk.client.resource_classes.resource_view_mapping import ResourceViewMappingRequest
from cognite_toolkit._cdf_tk.client.resource_classes.search_config import SearchConfigRequest
from cognite_toolkit._cdf_tk.client.resource_classes.securitycategory import SecurityCategoryRequest
from cognite_toolkit._cdf_tk.client.resource_classes.sequence import (
    SequenceColumnRequest,
    SequenceRequest,
    SequenceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.sequence_rows import SequenceRowsRequest
from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamRequest
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicRequest,
    AssetMappingDMRequest,
    ThreeDModelClassicRequest,
    ThreeDModelDMSRequest,
    ThreeDRevisionClassicRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import (
    NonceCredentials as TransformationNonceCredentials,
)
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationRequest
from cognite_toolkit._cdf_tk.client.resource_classes.transformation_notification import (
    TransformationNotificationRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.transformation_schedule import TransformationScheduleRequest
from cognite_toolkit._cdf_tk.client.resource_classes.workflow import WorkflowRequest
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import NonceCredentials, WorkflowTriggerRequest
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import WorkflowVersionRequest
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from tests_smoke.constants import (
    ASSET_EXTERNAL_ID,
    EVENT_EXTERNAL_ID,
    EXTRACTION_PIPELINE_CONFIG,
    SEQUENCE_COLUMN_ID,
    SEQUENCE_EXTERNAL_ID,
    SMOKE_SPACE,
    SMOKE_TEST_CONTAINER_EXTERNAL_ID,
    SMOKE_TEST_VIEW_EXTERNAL_ID,
)
from tests_smoke.exceptions import EndpointAssertionError

NOT_GENERIC_TESTED: Set[type[CDFResourceAPI]] = frozenset(
    {
        # Robotics API already have its own smoke tests.
        # Todo: Use generic tests for sub-APIs if possible.
        RobotsAPI,
        MapsAPI,
        FramesAPI,
        LocationsAPI,
        CapabilitiesAPI,
        DataPostProcessingAPI,
        # Needs special handling as it needs an existing simulator to create models.
        SimulatorModelsAPI,
        SimulatorRoutinesAPI,
        SimulatorModelRevisionsAPI,
        SimulatorRoutineRevisionsAPI,
        # Do not support delete.
        DataSetsAPI,
        # RAW tables depend on existing RAW databases, so they are tested together.
        RawDatabasesAPI,
        RawTablesAPI,
        # Job depends on source and destination, so tested together.
        HostedExtractorJobsAPI,
        # Edge depend on nodes
        InstancesAPI,
        # WorkflowTrigger and WorkflowVersion depend on existing workflows
        WorkflowVersionsAPI,
        WorkflowTriggersAPI,
        # 3D Models are expensive to create, and AssetMappings depend on existing models and assets/nodes.
        ThreeDClassicModelsAPI,
        ThreeDDMAssetMappingAPI,
        ThreeDClassicAssetMappingAPI,
        ThreeDClassicRevisionsAPI,
        # Requires special handling of list call,
        ExtractionPipelineConfigsAPI,
        # Cannot be deleted and recreated frequently.
        StreamsAPI,
        # SecurityCategories can easily hit a bad state.
        SecurityCategoriesAPI,
        # Created response cannot be made into a request.
        InFieldCDMConfigAPI,
        APMConfigAPI,
        ResourceViewMappingsAPI,
        CogniteFilesAPI,
        # Update and list have to be specially handled due to the way the API works.
        LocationFiltersAPI,
        # Dependency between Functions and FunctionSchedules makes it hard to test them in a generic way.
        FunctionsAPI,
        FunctionSchedulesAPI,
        # Does not support delete
        SearchConfigurationsAPI,
        # Dependencies between APIs
        TransformationsAPI,
        TransformationSchedulesAPI,
        TransformationNotificationsAPI,
        # Requires special handling of requests.
        # GraphQLDataModelsAPI,
        # List method requires an argument,
        SequenceRowsAPI,
        # The dataproduct API is not yet supported in CDF.
        DataProductsAPI,
        DataProductVersionsAPI,
    }
)


def crud_cdf_resource_apis() -> Iterable[tuple]:
    subclasses = get_concrete_subclasses(CDFResourceAPI)  # type: ignore[type-abstract]
    for api_cls in subclasses:
        if api_cls in NOT_GENERIC_TESTED:
            continue
        base_cls = next(
            (
                base
                for base in api_cls.__orig_bases__  # type: ignore[attr-defined]
                if get_origin(base) in (CDFResourceAPI, WrappedInstancesAPI)
            ),
            None,
        )
        assert base_cls is not None, f"Could not find generic base class for {api_cls.__name__}"
        _, request_cls, __ = get_args(base_cls)
        if get_origin(request_cls) is Annotated:
            union_type = get_args(request_cls)[0]
            if get_origin(union_type) is not types.UnionType:
                raise NotImplementedError("Only Union Annotated types are supported in tests.")
            request_classes: tuple[type[RequestResource], ...] = get_args(union_type)
        else:
            request_classes = (request_cls,)
        for request_cls in request_classes:
            examples = get_examples_minimum_requests(request_cls)
            id_str = f"{api_cls.__name__}"
            if len(request_classes) > 1:
                id_str += f" {request_cls.__name__}"
            if len(examples) == 1:
                yield pytest.param(examples[0], request_cls, api_cls, id=id_str)
            else:
                for no, example in enumerate(examples, start=1):
                    yield pytest.param(example, request_cls, api_cls, id=f"{id_str} example {no}")


GRAPHQL_MODEL = """"The smoke tests for GraphQL"
type SmokeTest {
  name: String! @limits(maxTextSize: 255)
  data: JSONObject!
}
"""


def get_examples_minimum_requests(request_cls: type[RequestResource]) -> list[dict[str, Any]]:
    """Return an example with the only required and identifier fields for the given resource class."""
    requests: dict[type[RequestResource], list[dict[str, Any]]] = {
        AgentRequest: [{"externalId": "smoke-test-agent", "name": "Smoke Test Agent"}],
        APMConfigRequest: [
            {
                "externalId": "smoke-test-apm-config",
            }
        ],
        AssetRequest: [{"name": "smoke-test-asset", "externalId": "smoke-test-asset"}],
        CogniteFileRequest: [{"externalId": "smoke-test-file", "space": SMOKE_SPACE}],
        DataSetRequest: [{"externalId": "smoke-tests-crudl-dataset"}],
        EventRequest: [{"externalId": "smoke-test-event"}],
        FileMetadataRequest: [{"name": "smoke-test-file", "externalId": "smoke-test-file"}],
        FunctionRequest: [{"externalId": "smoke-test-function", "name": "smoke-test-function", "fileId": -1}],
        FunctionScheduleRequest: [
            {
                "name": "smoke-test-function-schedule",
                "cronExpression": "0 0 * * *",
            }
        ],
        ExtractionPipelineRequest: [
            {
                "name": "smoke-test-pipeline",
                "externalId": "smoke-test-pipeline",
                "dataSetId": -1,
            }
        ],
        ExtractionPipelineConfigRequest: [
            {
                "externalId": EXTRACTION_PIPELINE_CONFIG,
            }
        ],
        KafkaSourceRequest: [
            {
                "type": "kafka",
                "externalId": "smoke-test-kafka-source",
                "bootstrapBrokers": [
                    {"host": "host1.kafka.local", "port": 9092},
                ],
            }
        ],
        RESTSourceRequest: [
            {
                "type": "rest",
                "externalId": "smoke-test-rest-source",
                "host": "smoke-test-endpoint",
            }
        ],
        MQTTSourceRequest: [
            {
                "type": "mqtt5",
                "externalId": "smoke-test-mqtt-source",
                "host": "smoke-test-mqtt-broker",
            }
        ],
        EventHubSourceRequest: [
            {
                "type": "eventhub",
                "externalId": "smoke-test-eventhub-source",
                "host": "myHost",
                "eventHubName": "smoke-test-hub",
                "authentication": {
                    "type": "basic",
                    "username": "smoke-test-username",
                },
            }
        ],
        HostedExtractorJobRequest: [
            {
                "externalId": "smoke-test-extractor-job",
                "sourceId": "smoke-test-kafka-source",
                "destinationId": "smoke-test-extractor-destination",
                "format": {"type": "cognite"},
                "config": {"topic": "smoke-test-topic"},
            }
        ],
        HostedExtractorMappingRequest: [
            {
                "externalId": "smoke-test-extractor-mapping",
                "published": True,
                "mapping": {"expression": "2 * 3"},
            }
        ],
        HostedExtractorDestinationRequest: [{"externalId": "smoke-test-extractor-destination"}],
        GraphQLDataModelRequest: [
            {
                "space": SMOKE_SPACE,
                "externalId": "smoke_test_graphql_data_model",
                "version": "v1",
                "graphQlDml": GRAPHQL_MODEL,
            }
        ],
        InFieldCDMLocationConfigRequest: [
            {
                "space": SMOKE_SPACE,
                "externalId": "smoke-test-infield-cdm-location-config",
            }
        ],
        GroupRequest: [{"name": "smoke-test-group"}],
        NodeRequest: [{"externalId": "smoke-test-node", "space": SMOKE_SPACE, "instanceType": "node"}],
        EdgeRequest: [
            {
                "externalId": "smoke-test-edge",
                "space": SMOKE_SPACE,
                "instanceType": "edge",
                "startNode": {
                    "space": SMOKE_SPACE,
                    "externalId": "smoke-test-node",
                },
                "endNode": {
                    "space": SMOKE_SPACE,
                    "externalId": "smoke-test-node",
                },
                "type": {
                    "space": SMOKE_SPACE,
                    "externalId": "smoke-test-node",
                },
            }
        ],
        LabelRequest: [{"name": "smoke-test-label", "externalId": "smoke-test-label"}],
        LocationFilterRequest: [{"externalId": "smoke-test-location-filter", "name": "smoke-test-location-filter"}],
        RAWDatabaseRequest: [{"name": "smoke-test-raw-database"}],
        RAWTableRequest: [{"name": "smoke-test-raw-table", "dbName": "smoke-test-raw-database"}],
        SearchConfigRequest: [{"view": {"space": "cdf_cdm", "externalId": "CogniteAsset"}}],
        SecurityCategoryRequest: [{"name": "smoke-test-security-category"}],
        SequenceRequest: [
            {"externalId": "smoke-test-sequence", "columns": [{"externalId": "smoke-test-sequence-column"}]}
        ],
        SequenceRowsRequest: [
            {
                "externalId": SEQUENCE_EXTERNAL_ID,
                "columns": [SEQUENCE_COLUMN_ID],
                "rows": [{"rowNumber": 1, "values": [37]}],
            }
        ],
        StreamRequest: [
            {"externalId": "smoke-test-stream3", "settings": {"template": {"name": "ImmutableTestStream"}}}
        ],
        ThreeDModelClassicRequest: [{"name": "smoke-test-3d-model-classic"}],
        ThreeDModelDMSRequest: [{"name": "smoke-test-3d-model-dms", "space": SMOKE_SPACE, "type": "CAD"}],
        ThreeDRevisionClassicRequest: [{"fileId": -1, "modelId": -1}],
        AssetMappingClassicRequest: [{"externalId": "smoke-test-asset-mapping-classic", "model3dId": 1, "assetId": 1}],
        AssetMappingDMRequest: [
            {
                "externalId": "smoke-test-asset-mapping-dm",
                "model3dId": 1,
                "nodeId": "smoke-test-node",
            }
        ],
        RelationshipRequest: [
            {
                "externalId": "smoke-test-relationship",
                "sourceExternalId": ASSET_EXTERNAL_ID,
                "sourceType": "asset",
                "targetExternalId": EVENT_EXTERNAL_ID,
                "targetType": "event",
            }
        ],
        ResourceViewMappingRequest: [
            {
                "externalId": "my_mapping",
                "resourceType": "asset",
                "viewId": {
                    "space": "cdf_cdm",
                    "externalId": "CogniteAsset",
                    "type": "view",
                    "version": "v1",
                },
                "propertyMapping": {"name": "name"},
            }
        ],
        TimeSeriesRequest: [{"externalId": "smoke-test-timeseries"}],
        TransformationRequest: [
            {
                "name": "smoke-test-transformation",
                "externalId": "smoke-test-transformation",
                "ignoreNullFields": True,
                "query": "SELECT 1",
                "destination": {"type": "assets"},
            }
        ],
        TransformationScheduleRequest: [{"externalId": "smoke-test-transformation", "interval": "0 0 * * *"}],
        TransformationNotificationRequest: [
            {"transformationExternalId": "smoke-test-transformation", "destination": "example@email.com"}
        ],
        WorkflowRequest: [{"externalId": "smoke-test-workflow"}],
        WorkflowTriggerRequest: [
            {
                "externalId": "smoke-test-workflow-trigger",
                "workflowExternalId": "smoke-test-workflow",
                "workflowVersion": "v1",
                "triggerRule": {
                    "triggerType": "schedule",
                    "cronExpression": "0 0 * * *",
                },
                "authentication": {"nonce": "smoke-test-nonce"},
            }
        ],
        WorkflowVersionRequest: [
            {
                "workflowExternalId": "smoke-test-workflow",
                "version": "v1",
                "workflowDefinition": {
                    "tasks": [
                        {
                            "externalId": "task1",
                            "type": "cdf",
                            "parameters": {
                                "cdfRequest": {
                                    "resourcePath": "/timeseries/list",
                                    "method": "GET",
                                }
                            },
                        }
                    ]
                },
            }
        ],
        SpaceRequest: [{"space": "smoke_test_space"}],
        ContainerRequest: [
            {
                "externalId": "smoke_test_container",
                "space": SMOKE_SPACE,
                "properties": {
                    "name": {"type": {"type": "text"}},
                },
                "constraints": {
                    "nameUnique": {
                        "constraintType": "uniqueness",
                        "properties": ["name"],
                        "bySpace": True,
                    }
                },
                "indexes": {
                    "nameIndex": {
                        "indexType": "btree",
                        "properties": ["name"],
                        "bySpace": True,
                        "cursorable": True,
                    }
                },
            }
        ],
        ViewRequest: [
            {
                "externalId": "smoke_test_view",
                "space": SMOKE_SPACE,
                "version": "v1",
                "properties": {
                    "name": {
                        "container": {
                            "space": SMOKE_SPACE,
                            "externalId": SMOKE_TEST_CONTAINER_EXTERNAL_ID,
                        },
                        "containerPropertyIdentifier": "name",
                    },
                },
            }
        ],
        DataModelRequest: [
            {
                "externalId": "smoke_test_data_model",
                "space": SMOKE_SPACE,
                "version": "v1",
                "views": [
                    {
                        "space": SMOKE_SPACE,
                        "externalId": SMOKE_TEST_VIEW_EXTERNAL_ID,
                        "version": "v1",
                    }
                ],
            }
        ],
    }
    try:
        return requests[request_cls]
    except KeyError:
        raise NotImplementedError(f"No example request defined for {request_cls.__name__}")


@pytest.fixture(scope="session")
def smoke_extraction_pipeline(
    toolkit_client: ToolkitClient, smoke_dataset: DataSetResponse
) -> Iterable[ExtractionPipelineResponse]:
    pipeline = ExtractionPipelineRequest(
        external_id=EXTRACTION_PIPELINE_CONFIG,
        name="Pipeline for smoke tests of configs",
        data_set_id=smoke_dataset.id,
    )

    retrieved = toolkit_client.tool.extraction_pipelines.retrieve([pipeline.as_id()], ignore_unknown_ids=True)
    if len(retrieved) == 0:
        yield toolkit_client.tool.extraction_pipelines.create([pipeline])[0]
    else:
        yield retrieved[0]

    toolkit_client.tool.extraction_pipelines.delete([pipeline.as_id()], ignore_unknown_ids=True)


@pytest.fixture(scope="module")
def function_code(toolkit_client: ToolkitClient) -> FileMetadataResponse:
    metadata = FileMetadataRequest(
        name="Smoke test function code",
        external_id="smoke-test-function-code",
        mime_type="application/zip",
    )

    file_response = toolkit_client.tool.filemetadata.retrieve([metadata.as_id()], ignore_unknown_ids=True)
    if file_response:
        if not file_response[0].uploaded:
            raise EndpointAssertionError(
                "/filemetadata",
                "A file with the same external ID already exists but is not uploaded. Please delete or change the external ID of the existing file.",
            )
        return file_response[0]
    code = """from cognite.client import CogniteClient


    def handle(client: CogniteClient, data: dict, function_call_info: dict) -> str:
        print("Print statements will be shown in the logs.")
        print("Running with the following configuration:\n")
        return {
            "data": data,
            "functionInfo": function_call_info,
        }
    """
    # Create zip file in memory with handler.py
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("handler.py", code)
    zip_content = zip_buffer.getvalue()

    created = toolkit_client.tool.filemetadata.create([metadata])
    if len(created) != 1:
        raise EndpointAssertionError("/filemetadata", f"Expected 1 created file metadata, got {len(created)}")
    file = created[0]
    if file.upload_url is None:
        raise EndpointAssertionError("/filemetadata", "No upload URL returned for the created file metadata.")
    response = toolkit_client.http_client.request_single_retries(
        RequestMessage(
            endpoint_url=file.upload_url,
            method="PUT",
            content_type="application/zip",
            data_content=zip_content,
        )
    )
    if not isinstance(response, SuccessResponse):
        raise EndpointAssertionError(
            "/filemetadata",
            f"Failed to upload function code to the provided upload URL. {response!s}",
        )
    return file


@pytest.fixture(scope="session")
def smoke_asset(toolkit_client: ToolkitClient) -> AssetResponse:
    asset_request = AssetRequest(name="smoke-test-asset", external_id=ASSET_EXTERNAL_ID)
    retrieved = toolkit_client.tool.assets.retrieve([asset_request.as_id()], ignore_unknown_ids=True)
    if len(retrieved) == 0:
        return toolkit_client.tool.assets.create([asset_request])[0]
    return retrieved[0]


@pytest.fixture(scope="session")
def smoke_event(toolkit_client: ToolkitClient) -> EventResponse:
    event_request = EventRequest(external_id=EVENT_EXTERNAL_ID, source="smoke-test")
    retrieved = toolkit_client.tool.events.retrieve([event_request.as_id()], ignore_unknown_ids=True)
    if len(retrieved) == 0:
        return toolkit_client.tool.events.create([event_request])[0]
    return retrieved[0]


@pytest.fixture(scope="session")
def smoke_sequence(toolkit_client: ToolkitClient) -> SequenceResponse:
    sequence_request = SequenceRequest(
        external_id=SEQUENCE_EXTERNAL_ID, columns=[SequenceColumnRequest(external_id=SEQUENCE_COLUMN_ID)]
    )
    retrieved = toolkit_client.tool.sequences.retrieve([sequence_request.as_id()], ignore_unknown_ids=True)
    if len(retrieved) == 0:
        return toolkit_client.tool.sequences.create([sequence_request])[0]
    return retrieved[0]


@pytest.mark.usefixtures("smoke_space", "smoke_asset", "smoke_event", "smoke_container", "smoke_view")
class TestCDFResourceAPI:
    def assert_endpoint_method(
        self, method: Callable[[], list[T_ResponseResource]], name: str, endpoint: Endpoint, id: Hashable | None = None
    ) -> Hashable:
        try:
            response_list = method()
        except ToolkitAPIError as e:
            raise EndpointAssertionError(endpoint.path, f"{name} method failed with error: {e!s}") from e

        if len(response_list) != 1:
            raise EndpointAssertionError(endpoint.path, f"Expected 1 {name} item, got {len(response_list)}")
        response = response_list[0]
        response_id = response.as_request_resource().as_id()
        if id is None:
            return response_id
        if response_id != id:
            raise EndpointAssertionError(endpoint.path, f"{name.title()} item's ID does not match the requested ID.")
        return response_id

    @pytest.mark.parametrize("example_data, request_cls, api_cls", crud_cdf_resource_apis())
    def test_crudl(
        self,
        example_data: dict[str, Any],
        request_cls: type[RequestResource],
        api_cls: type[CDFResourceAPI],
        toolkit_client: ToolkitClient,
        smoke_dataset: DataSetResponse,
    ) -> None:
        """Generic test for CRUDL (create, retrieve, update, delete, list) operations of CDFResourceAPI subclasses.

        API endpoints that do not follow the standard CRUDL pattern or require special setup/teardown

        Args:
            example_data: Example data for creating the resource.
            request_cls: The RequestResource subclass for the resource.
            api_cls: The CDFResourceAPI subclass to test.
            toolkit_client: The ToolkitClient fixture.
            smoke_dataset: The smoke test dataset fixture.
        Raises:
            EndpointAssertionError: If any of the CRUDL operations do not behave as expected.

        """

        # Set up
        if "dataSetId" in example_data:
            example_data["dataSetId"] = smoke_dataset.id

        request = request_cls.model_validate(example_data)
        try:
            id: Hashable | None = request.as_id()
        except ValueError as _:
            # If the request does not have enough info to create an identifier yet, we set id to None
            id = None

        # We now that all subclasses only need http_client as argument, even though
        # CDFResourceAPI also require endpoint map (and disable gzip).
        api = api_cls(toolkit_client.http_client)  # type: ignore[call-arg]
        methods = api._method_endpoint_map

        try:
            if hasattr(api, "create"):
                create_endpoint = methods["create"] if "create" in methods else methods["upsert"]
                id = self.assert_endpoint_method(lambda: api.create([request]), "create", create_endpoint, id)
            if hasattr(api, "retrieve"):
                retrieve_endpoint = methods["retrieve"]
                self.assert_endpoint_method(lambda: api.retrieve([id]), "retrieve", retrieve_endpoint, id)
            if hasattr(api, "update"):
                updated_endpoint = methods["update"] if "update" in methods else methods["upsert"]
                self.assert_endpoint_method(lambda: api.update([request]), "update", updated_endpoint, id)
            if hasattr(api, "list"):
                list_endpoint = methods["list"]
                try:
                    listed_items = api.list(limit=1)
                except TypeError:
                    listed_items = api.list()
                if len(listed_items) == 0:
                    raise EndpointAssertionError(list_endpoint.path, "Expected at least 1 listed item, got 0")
        finally:
            if hasattr(api, "delete") and id is not None:
                api.delete([id])

    def test_all_cdf_resource_apis_registered(self) -> None:
        """Test that all CDFResourceAPI subclasses are registered in ToolkitClient."""
        existing_api = set(get_concrete_subclasses(CDFResourceAPI))  # type: ignore[type-abstract]
        generic_tested: set[type[CDFResourceAPI]] = {param[0][2] for param in crud_cdf_resource_apis()}

        missing_tests = existing_api - (generic_tested | NOT_GENERIC_TESTED)
        if missing_tests:
            missing_names = [cls.__name__ for cls in missing_tests]
            raise AssertionError(
                f"CDFResourceAPI subclasses missing {humanize_collection(missing_names)} tests in TestCDFResourceAPI.test_crud_list"
            )

    def test_classic_3D_model_and_revision_crudl(
        self, toolkit_client: ToolkitClient, three_d_file: FileMetadataResponse
    ) -> None:
        model_example = get_examples_minimum_requests(ThreeDModelClassicRequest)[0]
        model_request = ThreeDModelClassicRequest.model_validate(model_example)

        revision = get_examples_minimum_requests(ThreeDRevisionClassicRequest)[0]
        revision_request = ThreeDRevisionClassicRequest.model_validate(revision)
        revision_request.file_id = three_d_file.id

        model_id: InternalId | None = None
        revision_id: ThreeDModelRevisionId | None = None
        try:
            ### 3D Model Classic CRUDL ###
            model_id = cast(
                InternalId,
                self.assert_endpoint_method(
                    lambda: toolkit_client.tool.three_d.models_classic.create([model_request]),
                    "create",
                    toolkit_client.tool.three_d.models_classic._method_endpoint_map["create"],
                ),
            )

            self.assert_endpoint_method(
                lambda: toolkit_client.tool.three_d.models_classic.retrieve([model_id]),
                "retrieve",
                toolkit_client.tool.three_d.models_classic._method_endpoint_map["retrieve"],
                model_id,
            )
            # Need to set ID for update.
            model_request.id = model_id.id
            self.assert_endpoint_method(
                lambda: toolkit_client.tool.three_d.models_classic.update([model_request]),
                "update",
                toolkit_client.tool.three_d.models_classic._method_endpoint_map["update"],
                model_id,
            )

            list_endpoint = toolkit_client.tool.three_d.models_classic._method_endpoint_map["list"]
            try:
                listed_models = list(toolkit_client.tool.three_d.models_classic.list(limit=1))
            except ToolkitAPIError as e:
                raise EndpointAssertionError(list_endpoint.path, f"List method failed with error: {e!s}") from e
            if len(listed_models) == 0:
                raise EndpointAssertionError(list_endpoint.path, "Expected at least 1 listed model, got 0")

            ### 3D Revision Classic CRUDL ###
            revision_request.model_id = model_id.id
            revision_id = cast(
                ThreeDModelRevisionId,
                self.assert_endpoint_method(
                    lambda: toolkit_client.tool.three_d.revisions_classic.create([revision_request]),
                    "create",
                    toolkit_client.tool.three_d.revisions_classic._method_endpoint_map["create"],
                ),
            )

            # Need to set ID for update
            revision_request.id = revision_id.id
            self.assert_endpoint_method(
                lambda: toolkit_client.tool.three_d.revisions_classic.update([revision_request]),
                "update",
                toolkit_client.tool.three_d.revisions_classic._method_endpoint_map["update"],
                revision_id,
            )

            try:
                listed_revisions = toolkit_client.tool.three_d.revisions_classic.list(model_id=model_id.id, limit=1)
            except ToolkitAPIError as e:
                raise EndpointAssertionError(
                    toolkit_client.tool.three_d.revisions_classic._method_endpoint_map["list"].path,
                    f"List method failed with error: {e!s}",
                ) from e
            if len(list(listed_revisions)) == 0:
                raise EndpointAssertionError(
                    toolkit_client.tool.three_d.revisions_classic._method_endpoint_map["list"].path,
                    "Expected at least 1 listed revision, got 0",
                )
        finally:
            if revision_id is not None:
                toolkit_client.tool.three_d.revisions_classic.delete([revision_id])
            if model_id is not None:
                toolkit_client.tool.three_d.models_classic.delete([model_id])

    def test_raw_tables_and_databases_crudl(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client

        database_example = get_examples_minimum_requests(RAWDatabaseRequest)[0]
        table_example = get_examples_minimum_requests(RAWTableRequest)[0]
        db = RAWDatabaseRequest.model_validate(database_example)
        table = RAWTableRequest.model_validate(table_example)

        try:
            # Create database
            db_endpoints = client.tool.raw.databases._method_endpoint_map
            db_create = db_endpoints["create"]
            created_db = client.tool.raw.databases.create([db])
            if len(created_db) != 1:
                raise EndpointAssertionError(db_create.path, f"Expected 1 created database, got {len(created_db)}")
            if created_db[0].name != db.name:
                raise EndpointAssertionError(db_create.path, "Created database name does not match requested name.")

            # List databases
            db_list = db_endpoints["list"]
            listed_dbs = list(client.tool.raw.databases.list(limit=1))
            if len(listed_dbs) == 0:
                raise EndpointAssertionError(db_list.path, "Expected at least 1 listed database, got 0")

            # Create table
            table_endpoints = client.tool.raw.tables._method_endpoint_map
            table_create = table_endpoints["create"]
            created_table = client.tool.raw.tables.create([table])
            if len(created_table) != 1:
                raise EndpointAssertionError(table_create.path, f"Expected 1 created table, got {len(created_table)}")
            if created_table[0].name != table.name:
                raise EndpointAssertionError(table_create.path, "Created table name does not match requested name.")

            # List tables
            table_list = table_endpoints["list"]
            listed_tables = list(client.tool.raw.tables.list(limit=1, db_name=db.name))
            if len(listed_tables) == 0:
                raise EndpointAssertionError(table_list.path, "Expected at least 1 listed table, got 0")
            if listed_tables[0].db_name != db.name:
                raise EndpointAssertionError(
                    table_list.path, "Listed table database name does not match the requested database name."
                )
        finally:
            # Clean up
            try:
                client.tool.raw.tables.delete([table.as_id()])
            except ToolkitAPIError:
                pass
            try:
                client.tool.raw.databases.delete([db.as_id()])
            except ToolkitAPIError:
                pass

    def test_datasets_crudl(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client
        dataset_example = get_examples_minimum_requests(DataSetRequest)[0]
        dataset_request = DataSetRequest.model_validate(dataset_example)
        identifier = dataset_request.as_id()
        # Retrieve
        retrieved = client.tool.datasets.retrieve([identifier], ignore_unknown_ids=True)
        if len(retrieved) == 0:
            create_endpoint = client.tool.datasets._method_endpoint_map["create"]
            self.assert_endpoint_method(
                lambda: client.tool.datasets.create([dataset_request]), "create", create_endpoint, identifier
            )
            retrieved = client.tool.datasets.retrieve([identifier])

        retrieve_endpoint = client.tool.datasets._method_endpoint_map["retrieve"]
        if len(retrieved) != 1:
            raise EndpointAssertionError(retrieve_endpoint.path, f"Expected 1 retrieved dataset, got {len(retrieved)}")
        if retrieved[0].external_id != dataset_request.external_id:
            raise EndpointAssertionError(
                retrieve_endpoint.path, "Retrieved dataset external ID does not match requested external ID."
            )
        update_endpoint = client.tool.datasets._method_endpoint_map["update"]
        self.assert_endpoint_method(
            lambda: client.tool.datasets.update([dataset_request]), "update", update_endpoint, identifier
        )

        # List datasets
        ds_list = client.tool.datasets._method_endpoint_map["list"]
        listed_ds = list(client.tool.datasets.list(limit=1))
        if len(listed_ds) == 0:
            raise EndpointAssertionError(ds_list.path, "Expected at least 1 listed dataset, got 0")

        # DataSets cannot be deleted, so we do not test delete here.

    def test_stream_crudl(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client

        stream_example = get_examples_minimum_requests(StreamRequest)[0]
        stream_request = StreamRequest.model_validate(stream_example)
        stream_id = stream_request.as_id()

        # Retrieve
        retrieved = client.streams.retrieve([stream_id], ignore_unknown_ids=True)
        if len(retrieved) == 0:
            create_endpoint = client.streams._method_endpoint_map["create"]
            self.assert_endpoint_method(
                lambda: client.streams.create([stream_request]), "create", create_endpoint, stream_id
            )
            retrieved = client.streams.retrieve([stream_id])
        retrieve_endpoint = client.streams._method_endpoint_map["retrieve"]
        if len(retrieved) != 1:
            raise EndpointAssertionError(retrieve_endpoint.path, f"Expected 1 retrieved stream, got {len(retrieved)}")
        if retrieved[0].external_id != stream_request.external_id:
            raise EndpointAssertionError(
                retrieve_endpoint.path, "Retrieved stream external ID does not match requested external ID."
            )

        # List streams
        stream_list_endpoint = client.streams._method_endpoint_map["list"]
        listed_streams = list(client.streams.list())
        if len(listed_streams) == 0:
            raise EndpointAssertionError(stream_list_endpoint.path, "Expected at least 1 listed stream, got 0")

        # We do not delete the stream as there are limits to delete/recreate of it.

    def test_hosted_extractors_crudl(self, toolkit_client: ToolkitClient, smoke_dataset: DataSetResponse) -> None:
        client = toolkit_client

        source_example = get_examples_minimum_requests(KafkaSourceRequest)[0]
        source_request = KafkaSourceRequest.model_validate(source_example)
        source_id = source_request.as_id()

        dest_example = get_examples_minimum_requests(HostedExtractorDestinationRequest)[0]
        dest_request = HostedExtractorDestinationRequest.model_validate(dest_example)
        dest_id = dest_request.as_id()

        job_example = get_examples_minimum_requests(HostedExtractorJobRequest)[0]
        job_request = HostedExtractorJobRequest.model_validate(job_example)
        job_id = job_request.as_id()

        try:
            # Create source
            source_create_endpoint = client.tool.hosted_extractors.sources._method_endpoint_map["create"]
            self.assert_endpoint_method(
                lambda: client.tool.hosted_extractors.sources.create([source_request]),
                "create",
                source_create_endpoint,
                source_id,
            )

            # Create destination
            destination_create_endpoint = client.tool.hosted_extractors.destinations._method_endpoint_map["create"]
            self.assert_endpoint_method(
                lambda: client.tool.hosted_extractors.destinations.create([dest_request]),
                "create",
                destination_create_endpoint,
                dest_id,
            )

            # Create job (dependent on source and destination)
            job_create_endpoint = client.tool.hosted_extractors.jobs._method_endpoint_map["create"]
            self.assert_endpoint_method(
                lambda: client.tool.hosted_extractors.jobs.create([job_request]), "create", job_create_endpoint, job_id
            )

            # Retrieve job
            job_retrieve_endpoint = client.tool.hosted_extractors.jobs._method_endpoint_map["retrieve"]
            self.assert_endpoint_method(
                lambda: client.tool.hosted_extractors.jobs.retrieve([job_id]), "retrieve", job_retrieve_endpoint, job_id
            )

            # List jobs
            job_list_endpoint = client.tool.hosted_extractors.jobs._method_endpoint_map["list"]
            listed_jobs = list(client.tool.hosted_extractors.jobs.list(limit=1))
            if len(listed_jobs) == 0:
                raise EndpointAssertionError(job_list_endpoint.path, "Expected at least 1 listed job, got 0")

            # Update job
            job_update_endpoint = client.tool.hosted_extractors.jobs._method_endpoint_map["update"]
            self.assert_endpoint_method(
                lambda: client.tool.hosted_extractors.jobs.update([job_request]), "update", job_update_endpoint, job_id
            )

        finally:
            # Clean up
            client.tool.hosted_extractors.jobs.delete([job_id], ignore_unknown_ids=True)
            client.tool.hosted_extractors.destinations.delete([dest_id], ignore_unknown_ids=True)
            client.tool.hosted_extractors.sources.delete([source_id], ignore_unknown_ids=True, force=True)

    def test_instances_crudl(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client

        node_example = get_examples_minimum_requests(NodeRequest)[0]
        node_request = NodeRequest.model_validate(node_example)
        node_id = node_request.as_id()

        edge_example = get_examples_minimum_requests(EdgeRequest)[0]
        edge_request = EdgeRequest.model_validate(edge_example)
        edge_id = edge_request.as_id()

        try:
            # Create node
            create_endpoint = client.tool.instances._method_endpoint_map["upsert"]
            try:
                created_node = client.tool.instances.create([node_request])
            except ToolkitAPIError:
                raise EndpointAssertionError(create_endpoint.path, "Creating node instance failed.")
            if len(created_node) != 1:
                raise EndpointAssertionError(create_endpoint.path, f"Expected 1 created node, got {len(created_node)}")
            if created_node[0].as_id() != node_id:
                raise EndpointAssertionError(create_endpoint.path, "Created node ID does not match requested node ID.")

            # Create edge (dependent on node)
            try:
                created_edge = client.tool.instances.create([edge_request])
            except ToolkitAPIError:
                raise EndpointAssertionError(create_endpoint.path, "Creating edge instance failed.")
            if len(created_edge) != 1:
                raise EndpointAssertionError(create_endpoint.path, f"Expected 1 created edge, got {len(created_edge)}")
            if created_edge[0].as_id() != edge_id:
                raise EndpointAssertionError(create_endpoint.path, "Created edge ID does not match requested edge ID.")

            # Retrieve edge
            retrieve_endpoint = client.tool.instances._method_endpoint_map["retrieve"]
            self.assert_endpoint_method(
                lambda: client.tool.instances.retrieve([edge_id]), "retrieve", retrieve_endpoint, edge_id
            )

            # Retrieve node
            self.assert_endpoint_method(
                lambda: client.tool.instances.retrieve([node_id]), "retrieve", retrieve_endpoint, node_id
            )

            # List instances
            list_endpoint = client.tool.instances._method_endpoint_map["list"]
            listed = list(client.tool.instances.list(limit=2))
            if len(listed) <= 1:
                raise EndpointAssertionError(list_endpoint.path, "Expected at least 2 listed instances, got 0")

        finally:
            # Clean up
            client.tool.instances.delete([edge_id])
            client.tool.instances.delete([node_id])

    def test_extraction_pipeline_config_crudl(
        self, toolkit_client: ToolkitClient, smoke_extraction_pipeline: ExtractionPipelineResponse
    ) -> None:
        client = toolkit_client

        example = get_examples_minimum_requests(ExtractionPipelineConfigRequest)[0]
        request = ExtractionPipelineConfigRequest.model_validate(example)

        method_map = client.tool.extraction_pipelines.configs._method_endpoint_map
        config_id = cast(
            ExtractionPipelineConfigId,
            self.assert_endpoint_method(
                lambda: client.tool.extraction_pipelines.configs.create([request]),
                "create",
                method_map["create"],
            ),
        )

        self.assert_endpoint_method(
            lambda: client.tool.extraction_pipelines.configs.retrieve([config_id]),
            "retrieve",
            method_map["retrieve"],
            config_id,
        )

        # List configs
        try:
            listed_configs = client.tool.extraction_pipelines.configs.list(
                external_id=smoke_extraction_pipeline.external_id, limit=1
            )
        except ToolkitAPIError as e:
            raise EndpointAssertionError(
                method_map["list"].path, f"Listing extraction pipeline configs failed with error: {e!s}"
            ) from e
        if len(listed_configs) != 1:
            raise EndpointAssertionError(
                method_map["list"].path, "Expected at 1 listed extraction pipeline config, got 0"
            )

    def test_workflow_crudl(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client

        workflow_example = get_examples_minimum_requests(WorkflowRequest)[0]
        workflow_request = WorkflowRequest.model_validate(workflow_example)
        workflow_id = workflow_request.as_id()

        workflow_version = get_examples_minimum_requests(WorkflowVersionRequest)[0]
        workflow_version_request = WorkflowVersionRequest.model_validate(workflow_version)
        workflow_version_id = workflow_version_request.as_id()

        workflow_trigger = get_examples_minimum_requests(WorkflowTriggerRequest)[0]
        workflow_trigger_request = WorkflowTriggerRequest.model_validate(workflow_trigger)
        workflow_trigger_id = workflow_trigger_request.as_id()
        workflow_trigger_request.authentication = NonceCredentials(
            nonce=toolkit_client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE").nonce
        )

        try:
            # Create workflow
            create_endpoint = client.tool.workflows._method_endpoint_map["upsert"]
            self.assert_endpoint_method(
                lambda: client.tool.workflows.create([workflow_request]),
                "create",
                create_endpoint,
                workflow_id,
            )

            # Create workflow version
            version_create_endpoint = client.tool.workflows.versions._method_endpoint_map["upsert"]
            self.assert_endpoint_method(
                lambda: client.tool.workflows.versions.create([workflow_version_request]),
                "create",
                version_create_endpoint,
                workflow_version_id,
            )
            # Create workflow trigger
            trigger_create_endpoint = client.tool.workflows.triggers._method_endpoint_map["upsert"]
            created_triggers = client.tool.workflows.triggers.create([workflow_trigger_request])
            if len(created_triggers) != 1:
                raise EndpointAssertionError(
                    trigger_create_endpoint.path, f"Expected 1 created workflow trigger, got {len(created_triggers)}"
                )
            if created_triggers[0].as_id() != workflow_trigger_id:
                raise EndpointAssertionError(
                    trigger_create_endpoint.path, "Created workflow trigger ID does not match requested ID."
                )

            # Retrieve workflow version
            version_retrieve_endpoint = client.tool.workflows.versions._method_endpoint_map["retrieve"]
            self.assert_endpoint_method(
                lambda: client.tool.workflows.versions.retrieve([workflow_version_id]),
                "retrieve",
                version_retrieve_endpoint,
                workflow_version_id,
            )

            # List workflow versions
            version_list_endpoint = client.tool.workflows.versions._method_endpoint_map["list"]
            listed_versions = list(
                client.tool.workflows.versions.list(workflow_external_id=workflow_request.external_id, limit=1)
            )
            if len(listed_versions) == 0:
                raise EndpointAssertionError(
                    version_list_endpoint.path, "Expected at least 1 listed workflow version, got 0"
                )
            # List workflow triggers
            trigger_list_endpoint = client.tool.workflows.triggers._method_endpoint_map["list"]
            listed_triggers = list(client.tool.workflows.triggers.list(limit=1))
            if len(listed_triggers) == 0:
                raise EndpointAssertionError(
                    trigger_list_endpoint.path, "Expected at least 1 listed workflow trigger, got 0"
                )

        finally:
            # Clean up
            client.tool.workflows.triggers.delete([workflow_trigger_id])
            client.tool.workflows.versions.delete([workflow_version_id])
            client.tool.workflows.delete([workflow_id])

    def test_security_categories_crudl(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client

        security_category_example = get_examples_minimum_requests(SecurityCategoryRequest)[0]
        security_category_request = SecurityCategoryRequest.model_validate(security_category_example)

        # Cleanup any existing security categories with the same name
        existing_list = client.tool.security_categories.list(limit=None)
        to_delete: list[InternalIdUnwrapped] = []
        for existing in existing_list:
            if existing.name == security_category_request.name:
                to_delete.append(existing.as_id())
        if to_delete:
            client.tool.security_categories.delete(to_delete)

        try:
            # Create security category
            create_endpoint = client.tool.security_categories._method_endpoint_map["create"]
            created_id = self.assert_endpoint_method(
                lambda: client.tool.security_categories.create([security_category_request]),
                "create",
                create_endpoint,
            )

            # List security categories
            list_endpoint = client.tool.security_categories._method_endpoint_map["list"]
            listed = list(client.tool.security_categories.list(limit=1))
            if len(listed) == 0:
                raise EndpointAssertionError(list_endpoint.path, "Expected at least 1 listed security category, got 0")
        finally:
            # Clean up
            client.tool.security_categories.delete([created_id])  # type: ignore[list-item]

    def test_infield_cdm_location_config_crudl(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client

        location_config_example = get_examples_minimum_requests(InFieldCDMLocationConfigRequest)[0]
        location_config_request = InFieldCDMLocationConfigRequest.model_validate(location_config_example)
        location_config_id = location_config_request.as_id()

        try:
            # Create location config
            create_endpoint = client.infield.cdm_config._method_endpoint_map["upsert"]
            try:
                created = client.infield.cdm_config.create([location_config_request])
            except ToolkitAPIError:
                raise EndpointAssertionError(create_endpoint.path, "Creating location config instance failed.")
            if len(created) != 1:
                raise EndpointAssertionError(create_endpoint.path, f"Expected 1 created node, got {len(created)}")
            if created[0].as_id() != location_config_id:
                raise EndpointAssertionError(create_endpoint.path, "Created node ID does not match requested node ID.")

            # Retrieve location config
            retrieve_endpoint = client.infield.cdm_config._method_endpoint_map["retrieve"]
            self.assert_endpoint_method(
                lambda: client.infield.cdm_config.retrieve([location_config_id]),
                "retrieve",
                retrieve_endpoint,
                location_config_id,
            )

        finally:
            # Clean up
            client.infield.cdm_config.delete([location_config_id])

    def test_apm_config_crudls(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client

        apm_config_example = get_examples_minimum_requests(APMConfigRequest)[0]
        apm_config_request = APMConfigRequest.model_validate(apm_config_example)
        apm_config_id = apm_config_request.as_id()

        try:
            # Create APM config
            create_endpoint = client.infield.apm_config._method_endpoint_map["upsert"]
            try:
                created = client.infield.apm_config.create([apm_config_request])
            except ToolkitAPIError:
                raise EndpointAssertionError(create_endpoint.path, "Creating APM config instance failed.")
            if len(created) != 1:
                raise EndpointAssertionError(create_endpoint.path, f"Expected 1 created APM config, got {len(created)}")
            if created[0].as_id() != apm_config_id:
                raise EndpointAssertionError(create_endpoint.path, "Created APM config ID does not match requested ID.")

            # Retrieve APM config
            retrieve_endpoint = client.infield.apm_config._method_endpoint_map["retrieve"]
            self.assert_endpoint_method(
                lambda: client.infield.apm_config.retrieve([apm_config_id]),
                "retrieve",
                retrieve_endpoint,
                apm_config_id,
            )

        finally:
            # Clean up
            client.infield.apm_config.delete([apm_config_id])

    def test_resource_view_mapping_crudls(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client

        mapping_example = get_examples_minimum_requests(ResourceViewMappingRequest)[0]
        mapping_request = ResourceViewMappingRequest.model_validate(mapping_example)
        mapping_id = mapping_request.as_id()

        try:
            # Create resource view mapping
            create_endpoint = client.migration.resource_view_mapping._method_endpoint_map["upsert"]
            try:
                created = client.migration.resource_view_mapping.create([mapping_request])
            except ToolkitAPIError:
                raise EndpointAssertionError(create_endpoint.path, "Creating resource view mapping instance failed.")
            if len(created) != 1:
                raise EndpointAssertionError(
                    create_endpoint.path, f"Expected 1 created resource view mapping, got {len(created)}"
                )
            if created[0].as_id() != mapping_id:
                raise EndpointAssertionError(
                    create_endpoint.path, "Created resource view mapping ID does not match requested ID."
                )

            # Retrieve resource view mapping
            retrieve_endpoint = client.migration.resource_view_mapping._method_endpoint_map["retrieve"]
            self.assert_endpoint_method(
                lambda: client.migration.resource_view_mapping.retrieve([mapping_id]),
                "retrieve",
                retrieve_endpoint,
                mapping_id,
            )

            # List resource view mappings
            list_endpoint = client.migration.resource_view_mapping._method_endpoint_map["list"]
            try:
                listed_mappings = list(
                    client.migration.resource_view_mapping.list(resource_type=mapping_request.resource_type, limit=1)
                )
            except ToolkitAPIError:
                raise EndpointAssertionError(list_endpoint.path, "Listing resource view mappings failed.")
            if len(listed_mappings) == 0:
                raise EndpointAssertionError(
                    list_endpoint.path, "Expected at least 1 listed resource view mapping, got 0"
                )

        finally:
            # Clean up
            client.migration.resource_view_mapping.delete([mapping_id])

    def test_cognite_file_crudl(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client

        file_example = get_examples_minimum_requests(CogniteFileRequest)[0]
        file_request = CogniteFileRequest.model_validate(file_example)
        file_id = file_request.as_id()

        try:
            # Create file
            create_endpoint = client.tool.cognite_files._method_endpoint_map["upsert"]
            try:
                created = client.tool.cognite_files.create([file_request])
            except ToolkitAPIError:
                raise EndpointAssertionError(create_endpoint.path, "Creating file instance failed.")
            if len(created) != 1:
                raise EndpointAssertionError(create_endpoint.path, f"Expected 1 created file, got {len(created)}")
            if created[0].as_id() != file_id:
                raise EndpointAssertionError(create_endpoint.path, "Created file ID does not match requested ID.")

            # Retrieve file
            retrieve_endpoint = client.tool.cognite_files._method_endpoint_map["retrieve"]
            self.assert_endpoint_method(
                lambda: client.tool.cognite_files.retrieve([file_id]), "retrieve", retrieve_endpoint, file_id
            )

            # List files
            list_endpoint = client.tool.cognite_files._method_endpoint_map["list"]
            listed_files = list(client.tool.cognite_files.list(limit=1))
            if len(listed_files) == 0:
                raise EndpointAssertionError(list_endpoint.path, "Expected at least 1 listed file, got 0")
        finally:
            # Clean up
            client.tool.cognite_files.delete([file_id])

    def test_location_filter_crudls(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client

        example = get_examples_minimum_requests(LocationFilterRequest)[0]
        request = LocationFilterRequest.model_validate(example)
        location_filter_id: InternalId | None = None
        try:
            # Create location filter
            create_endpoint = client.tool.location_filters._method_endpoint_map["create"]
            try:
                created = client.tool.location_filters.create([request])
            except ToolkitAPIError:
                raise EndpointAssertionError(create_endpoint.path, "Creating location filter instance failed.")
            if len(created) != 1:
                raise EndpointAssertionError(
                    create_endpoint.path, f"Expected 1 created location filter, got {len(created)}"
                )

            # Get location filter ID
            request = created[0].as_request_resource()
            location_filter_id = request.as_id()
            # Retrieve location filter
            retrieve_endpoint = client.tool.location_filters._method_endpoint_map["retrieve"]
            self.assert_endpoint_method(
                lambda: client.tool.location_filters.retrieve([location_filter_id]),
                "retrieve",
                retrieve_endpoint,
                id=location_filter_id,
            )

            # Update location filter
            update_endpoint = client.tool.location_filters._method_endpoint_map["update"]
            self.assert_endpoint_method(
                lambda: client.tool.location_filters.update([request]),
                "update",
                update_endpoint,
                id=location_filter_id,
            )

            # List location filters
            list_endpoint = client.tool.location_filters._method_endpoint_map["list"]
            listed = client.tool.location_filters.list()
            if len(listed) == 0:
                raise EndpointAssertionError(list_endpoint.path, "Expected at least 1 listed location filter, got 0")
        finally:
            # Clean up
            if location_filter_id is not None:
                client.tool.location_filters.delete([location_filter_id])

    def test_function_crudls(self, toolkit_client: ToolkitClient, function_code: FileMetadataResponse) -> None:
        client = toolkit_client

        function_example = get_examples_minimum_requests(FunctionRequest)[0]
        function_example["fileId"] = function_code.id
        function_request = FunctionRequest.model_validate(function_example)
        function_id = function_request.as_id()

        schedule_example = get_examples_minimum_requests(FunctionScheduleRequest)[0]
        function_schedule_request = FunctionScheduleRequest.model_validate(schedule_example)

        schedule_id: InternalId | None = None

        try:
            # Create function
            create_endpoint = client.tool.functions._method_endpoint_map["create"]
            try:
                created_list = client.tool.functions.create([function_request])
            except ToolkitAPIError:
                raise EndpointAssertionError(create_endpoint.path, "Creating function instance failed.")
            if len(created_list) != 1:
                raise EndpointAssertionError(
                    create_endpoint.path, f"Expected 1 created function, got {len(created_list)}"
                )
            if created_list[0].as_request_resource().as_id() != function_id:
                raise EndpointAssertionError(create_endpoint.path, "Created function ID does not match requested ID.")
            created = created_list[0]

            # Retrieve function
            retrieve_endpoint = client.tool.functions._method_endpoint_map["retrieve"]
            self.assert_endpoint_method(
                lambda: client.tool.functions.retrieve([function_id]),
                "retrieve",
                retrieve_endpoint,
                function_id,
            )
            # List functions
            list_endpoint = client.tool.functions._method_endpoint_map["list"]
            listed = list(client.tool.functions.list(limit=1))
            if len(listed) == 0:
                raise EndpointAssertionError(list_endpoint.path, "Expected at least 1 listed function, got 0")

            # Create function schedule (dependent on function)
            function_schedule_request.function_id = created.id
            function_schedule_request.nonce = toolkit_client.iam.sessions.create(
                session_type="ONESHOT_TOKEN_EXCHANGE"
            ).nonce
            schedule_create_endpoint = client.tool.functions.schedules._method_endpoint_map["create"]
            try:
                created_schedule_list = client.tool.functions.schedules.create([function_schedule_request])
            except ToolkitAPIError:
                raise EndpointAssertionError(
                    schedule_create_endpoint.path, "Creating function schedule instance failed."
                )
            if len(created_schedule_list) != 1:
                raise EndpointAssertionError(
                    schedule_create_endpoint.path,
                    f"Expected 1 created function schedule, got {len(created_schedule_list)}",
                )
            created_schedule = created_schedule_list[0]
            schedule_id = created_schedule.as_request_resource().as_id()
            self.assert_endpoint_method(
                lambda: client.tool.functions.schedules.retrieve([schedule_id]),
                "retrieve",
                client.tool.functions.schedules._method_endpoint_map["retrieve"],
                schedule_id,
            )

            # List function schedules
            schedule_list_endpoint = client.tool.functions.schedules._method_endpoint_map["list"]
            try:
                listed_schedules = client.tool.functions.schedules.list(function_id=created.id, limit=1)
            except ToolkitAPIError:
                raise EndpointAssertionError(schedule_list_endpoint.path, "Listing function schedules failed.")
            if len(listed_schedules) == 0:
                raise EndpointAssertionError(
                    schedule_list_endpoint.path, "Expected at least 1 listed function schedule, got 0"
                )
        finally:
            # Clean up
            if schedule_id is not None:
                client.tool.functions.schedules.delete([schedule_id])
            client.tool.functions.delete([function_id], ignore_unknown_ids=True)

    def test_search_config_crudls(self, toolkit_client: ToolkitClient) -> None:
        # Search Config does not support delete.
        client = toolkit_client
        search_config_example = get_examples_minimum_requests(SearchConfigRequest)[0]
        search_config_request = SearchConfigRequest.model_validate(search_config_example)
        search_config_id = search_config_request.as_id()

        # List existing search config
        list_endpoint = client.tool.search_configurations._method_endpoint_map["list"]
        try:
            listed_configs = list(client.tool.search_configurations.list())
        except ToolkitAPIError:
            raise EndpointAssertionError(list_endpoint.path, "Listing search configs failed.")
        for config in listed_configs:
            if config.as_id() == search_config_id:
                search_config_request.id = config.id

        # Update existing or create new search config
        create_endpoint = client.tool.search_configurations._method_endpoint_map["upsert"]
        try:
            created = client.tool.search_configurations.create([search_config_request])
        except ToolkitAPIError:
            raise EndpointAssertionError(create_endpoint.path, "Creating search config instance failed.")
        if len(created) != 1:
            raise EndpointAssertionError(create_endpoint.path, f"Expected 1 created search config, got {len(created)}")
        if created[0].as_id() != search_config_id:
            raise EndpointAssertionError(create_endpoint.path, "Created search config ID does not match requested ID.")

    @pytest.mark.usefixtures("smoke_sequence")
    def test_sequence_rows_crudl(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client

        sequence_rows_example = get_examples_minimum_requests(SequenceRowsRequest)[0]
        sequence_rows_request = SequenceRowsRequest.model_validate(sequence_rows_example)
        sequence_id = sequence_rows_request.as_id()

        try:
            # Create sequence rows
            create_endpoint = client.tool.sequences.rows._method_endpoint_map["create"]
            try:
                client.tool.sequences.rows.create([sequence_rows_request])
            except ToolkitAPIError:
                raise EndpointAssertionError(create_endpoint.path, "Creating sequence rows instance failed.")

            # Retrieve latest
            latest_endpoint = client.tool.sequences.rows._latest_endpoint
            try:
                latest = client.tool.sequences.rows.latest(external_id=sequence_id.external_id)
            except ToolkitAPIError:
                raise EndpointAssertionError(latest_endpoint.path, "Retrieving latest sequence rows failed.")
            if latest.external_id != sequence_id.external_id:
                raise EndpointAssertionError(
                    latest_endpoint.path, "Retrieved latest sequence rows external ID does not match requested ID."
                )

            # List sequence rows
            list_endpoint = client.tool.sequences.rows._method_endpoint_map["list"]
            try:
                listed_rows = client.tool.sequences.rows.list(
                    SequenceRowFilter(external_id=sequence_id.external_id), limit=1
                )
            except ToolkitAPIError:
                raise EndpointAssertionError(list_endpoint.path, "Listing sequence rows failed.")
            if len(listed_rows) == 0:
                raise EndpointAssertionError(list_endpoint.path, "Expected at least 1 listed sequence row, got 0")
        finally:
            # Clean up
            client.tool.sequences.rows.delete([sequence_id])

    def test_transformation_crudls(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client

        transformation_example = get_examples_minimum_requests(TransformationRequest)[0]
        transformation_request = TransformationRequest.model_validate(transformation_example)
        transformation_id = transformation_request.as_id()
        schedule_example = get_examples_minimum_requests(TransformationScheduleRequest)[0]
        schedule_request = TransformationScheduleRequest.model_validate(schedule_example)
        notification = get_examples_minimum_requests(TransformationNotificationRequest)[0]
        notification_request = TransformationNotificationRequest.model_validate(notification)
        schedule_id: InternalId | None = None
        notification_id: InternalId | None = None

        session = toolkit_client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE")
        credentials = TransformationNonceCredentials(
            session_id=session.id,
            nonce=session.nonce,
            cdf_project_name=toolkit_client.config.project,
        )
        transformation_request.source_nonce = credentials
        transformation_request.destination_nonce = credentials

        # Clean up any existing transformation with the same ID
        try:
            client.tool.transformations.delete([transformation_id], ignore_unknown_ids=True)
        except ToolkitAPIError:
            pass

        try:
            # Create transformation
            created_endpoint = client.tool.transformations._method_endpoint_map["create"]
            self.assert_endpoint_method(
                lambda: client.tool.transformations.create([transformation_request]),
                "create",
                created_endpoint,
                transformation_id,
            )

            # Retrieve transformation
            retrieve_endpoint = client.tool.transformations._method_endpoint_map["retrieve"]
            self.assert_endpoint_method(
                lambda: client.tool.transformations.retrieve([transformation_id]),
                "retrieve",
                retrieve_endpoint,
                transformation_id,
            )

            # List transformations
            list_endpoint = client.tool.transformations._method_endpoint_map["list"]
            try:
                listed = list(client.tool.transformations.list(limit=1))
            except ToolkitAPIError:
                raise EndpointAssertionError(list_endpoint.path, "Listing transformations failed.")
            if len(listed) == 0:
                raise EndpointAssertionError(list_endpoint.path, "Expected at least 1 listed transformation, got 0")

            ### Schedules ###
            # Create transformation schedule (dependent on transformation)
            schedule_create_endpoint = client.tool.transformations.schedules._method_endpoint_map["create"]
            self.assert_endpoint_method(
                lambda: client.tool.transformations.schedules.create([schedule_request]),
                "create",
                schedule_create_endpoint,
                transformation_id,
            )
            # Retrieve transformation schedule
            schedule_retrieve_endpoint = client.tool.transformations.schedules._method_endpoint_map["retrieve"]
            schedule_id = cast(
                InternalId,
                self.assert_endpoint_method(
                    lambda: client.tool.transformations.schedules.retrieve([schedule_request.as_id()]),
                    "retrieve",
                    schedule_retrieve_endpoint,
                    schedule_request.as_id(),
                ),
            )

            # List transformation schedules
            schedule_list_endpoint = client.tool.transformations.schedules._method_endpoint_map["list"]
            try:
                listed_schedules = client.tool.transformations.schedules.list(limit=1)
            except ToolkitAPIError:
                raise EndpointAssertionError(schedule_list_endpoint.path, "Listing transformation schedules failed.")
            if len(listed_schedules) == 0:
                raise EndpointAssertionError(
                    schedule_list_endpoint.path, "Expected at least 1 listed transformation schedule, got 0"
                )

            ### Notifications ###

            # Create transformation notification (dependent on transformation)
            notification_create_endpoint = client.tool.transformations.notifications._method_endpoint_map["create"]
            notification_id = cast(
                InternalId,
                self.assert_endpoint_method(
                    lambda: client.tool.transformations.notifications.create([notification_request]),
                    "create",
                    notification_create_endpoint,
                ),
            )

            # List transformation notifications
            notification_list_endpoint = client.tool.transformations.notifications._method_endpoint_map["list"]
            try:
                listed_notifications = client.tool.transformations.notifications.list(limit=1)
            except ToolkitAPIError:
                raise EndpointAssertionError(
                    notification_list_endpoint.path, "Listing transformation notifications failed."
                )
            if len(listed_notifications) == 0:
                raise EndpointAssertionError(
                    notification_list_endpoint.path, "Expected at least 1 listed transformation notification, got 0"
                )
        finally:
            # Clean up
            if notification_id is not None:
                client.tool.transformations.notifications.delete([notification_id])
            if schedule_id is not None:
                client.tool.transformations.schedules.delete([schedule_id], ignore_unknown_ids=True)
            client.tool.transformations.delete([transformation_id], ignore_unknown_ids=True)
