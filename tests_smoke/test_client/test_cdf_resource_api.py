from collections.abc import Iterable, Set
from typing import Any, get_args, get_origin

import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import RequestResource
from cognite_toolkit._cdf_tk.client.api.datasets import DataSetsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_capabilities import CapabilitiesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_data_postprocessing import DataPostProcessingAPI
from cognite_toolkit._cdf_tk.client.api.robotics_frames import FramesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_locations import LocationsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_maps import MapsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_robots import RobotsAPI
from cognite_toolkit._cdf_tk.client.api.simulator_models import SimulatorModelsAPI
from cognite_toolkit._cdf_tk.client.cdf_client.api import CDFResourceAPI
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeRequest
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventRequest
from cognite_toolkit._cdf_tk.client.resource_classes.extraction_pipeline import ExtractionPipelineRequest
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest
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
from cognite_toolkit._cdf_tk.client.resource_classes.label import LabelRequest
from cognite_toolkit._cdf_tk.client.resource_classes.raw import RAWDatabase, RAWTable
from cognite_toolkit._cdf_tk.client.resource_classes.securitycategory import SecurityCategoryRequest
from cognite_toolkit._cdf_tk.client.resource_classes.sequence import SequenceRequest
from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamRequest
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicRequest,
    AssetMappingDMRequest,
    ThreeDModelClassicRequest,
    ThreeDModelDMSRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationRequest
from cognite_toolkit._cdf_tk.client.resource_classes.workflow import WorkflowRequest
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import WorkflowTriggerRequest
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import WorkflowVersionRequest
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
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
        # Do not support delete.
        # Todo: create manual tests for datasets.
        DataSetsAPI,
    }
)


def crud_cdf_resource_apis() -> Iterable[tuple]:
    subclasses = get_concrete_subclasses(CDFResourceAPI)  # type: ignore[type-abstract]
    for subclass in subclasses:
        if not (hasattr(subclass, "create") and hasattr(subclass, "delete")):
            # Need to be manually tested.
            continue
        if subclass in NOT_GENERIC_TESTED:
            continue
        yield pytest.param(subclass, id=subclass.__name__)


def get_example_minimum_requests(request_cls: type[RequestResource]) -> dict[str, Any]:
    """Return an example with the only required and identifier fields for the given resource class."""
    requests: dict[type[RequestResource], dict[str, Any]] = {
        AssetRequest: {"name": "smoke-test-asset", "externalId": "smoke-test-asset"},
        EventRequest: {"externalId": "smoke-test-event"},
        FileMetadataRequest: {"name": "smoke-test-file", "externalId": "smoke-test-file"},
        ExtractionPipelineRequest: {
            "name": "smoke-test-pipeline",
            "externalId": "smoke-test-pipeline",
            "dataSetId": -1,
        },
        KafkaSourceRequest: {
            "name": "smoke-test-kafka-source",
            "externalId": "smoke-test-kafka-source",
            "bootstrapServers": "smoke-test:9092",
            "topic": "smoke-test-topic",
        },
        RESTSourceRequest: {
            "name": "smoke-test-rest-source",
            "externalId": "smoke-test-rest-source",
            "endpoint": "https://smoke-test-endpoint",
        },
        MQTTSourceRequest: {
            "name": "smoke-test-mqtt-source",
            "externalId": "smoke-test-mqtt-source",
            "broker": "smoke-test-broker",
        },
        EventHubSourceRequest: {
            "name": "smoke-test-eventhub-source",
            "externalId": "smoke-test-eventhub-source",
            "connectionString": "Endpoint=sb://smoke-test.servicebus.windows.net/;SharedAccessKeyName=smoke-test;SharedAccessKey = smoke-test-key",
            "eventHubName": "smoke-test-hub",
        },
        HostedExtractorJobRequest: {
            "name": "smoke-test-extractor-job",
            "externalId": "smoke-test-extractor-job",
            "pipelineId": 1,
            "sourceId": 1,
            "destinationId": 1,
        },
        HostedExtractorMappingRequest: {
            "name": "smoke-test-extractor-mapping",
            "externalId": "smoke-test-extractor-mapping",
            "jobId": 1,
            "mapping": {},
        },
        HostedExtractorDestinationRequest: {
            "name": "smoke-test-extractor-destination",
            "externalId": "smoke-test-extractor-destination",
            "type": "CDF",
        },
        NodeRequest: {"externalId": "smoke-test-node", "name": "smoke-test-node"},
        LabelRequest: {"name": "smoke-test-label", "externalId": "smoke-test-label", "color": "blue"},
        RAWDatabase: {"name": "smoke-test-raw-database", "externalId": "smoke-test-raw-database"},
        RAWTable: {"name": "smoke-test-raw-table", "externalId": "smoke-test-raw-table", "databaseId": 1},
        SecurityCategoryRequest: {"name": "smoke-test-security-category", "externalId": "smoke-test-security-category"},
        SequenceRequest: {"name": "smoke-test-sequence", "externalId": "smoke-test-sequence"},
        StreamRequest: {"name": "smoke-test-stream", "externalId": "smoke-test-stream"},
        ThreeDModelClassicRequest: {"name": "smoke-test-3d-model-classic", "externalId": "smoke-test-3d-model-classic"},
        ThreeDModelDMSRequest: {"name": "smoke-test-3d-model-dms", "externalId": "smoke-test-3d-model-dms"},
        AssetMappingClassicRequest: {"externalId": "smoke-test-asset-mapping-classic", "model3dId": 1, "assetId": 1},
        AssetMappingDMRequest: {
            "externalId": "smoke-test-asset-mapping-dm",
            "model3dId": 1,
            "nodeId": "smoke-test-node",
        },
        TimeSeriesRequest: {"name": "smoke-test-timeseries", "externalId": "smoke-test-timeseries"},
        TransformationRequest: {
            "name": "smoke-test-transformation",
            "externalId": "smoke-test-transformation",
            "script": "return input;",
        },
        WorkflowRequest: {"name": "smoke-test-workflow", "externalId": "smoke-test-workflow"},
        WorkflowTriggerRequest: {
            "name": "smoke-test-workflow-trigger",
            "externalId": "smoke-test-workflow-trigger",
            "workflowExternalId": "smoke-test-workflow",
            "type": "ON_DEMAND",
        },
        WorkflowVersionRequest: {"workflowExternalId": "smoke-test-workflow", "version": "v1", "definition": {}},
    }
    try:
        return requests[request_cls]
    except KeyError:
        raise NotImplementedError(f"No example request defined for {request_cls.__name__}")


class TestCDFResourceAPI:
    @pytest.mark.parametrize("api_cls", crud_cdf_resource_apis())
    def test_crud_list(
        self, api_cls: type[CDFResourceAPI], toolkit_client: ToolkitClient, smoke_dataset: DataSetResponse
    ) -> None:
        # Set up
        cdf_resource_base = next((base for base in api_cls.__orig_bases__ if get_origin(base) is CDFResourceAPI), None)  # type: ignore[attr-defined]
        assert cdf_resource_base is not None, "Error in test. Could not find CDFResourceAPI in __orig_bases__"
        request_cls: type[RequestResource]
        _, request_cls, __ = get_args(cdf_resource_base)

        example_data = get_example_minimum_requests(request_cls)
        if "dataSetId" in example_data:
            example_data["dataSetId"] = smoke_dataset.id

        request = request_cls.model_validate(example_data)
        id = request.as_id()

        # We now that all subclasses only need http_client as argument, even though
        # CDFResourceAPI also require endpoint map (and disable gzip).
        api = api_cls(toolkit_client.http_client)  # type: ignore[call-arg]
        assert hasattr(api, "create") and hasattr(api, "delete"), (
            "API does not support create and delete methods. Cannot run CRUD test."
        )
        methods = api._method_endpoint_map
        try:
            create_endpoint = methods["create"]
            created = api.create([request])
            if len(created) != 1:
                raise EndpointAssertionError(create_endpoint.path, f"Expected 1 created item, got {len(created)}")
            created_item = created[0]
            if created_item.as_request_resource().as_id() != id:
                raise EndpointAssertionError(create_endpoint.path, "Created item's ID does not match the requested ID.")
            if hasattr(api, "update"):
                updated_endpoint = methods["update"]
                updated = api.update([request])
                if len(updated) != 1:
                    raise EndpointAssertionError(updated_endpoint.path, f"Expected 1 updated item, got {len(updated)}")
            if hasattr(api, "retrieve"):
                retrieve_endpoint = methods["retrieve"]
                retrieved = api.retrieve([id])
                if len(retrieved) != 1:
                    raise EndpointAssertionError(
                        retrieve_endpoint.path, f"Expected 1 retrieved item, got {len(retrieved)}"
                    )
                retrieved_item = retrieved[0]
                if retrieved_item.as_request_resource().as_id() != id:
                    raise EndpointAssertionError(
                        retrieve_endpoint.path, "Retrieved item's ID does not match the requested ID."
                    )
            if hasattr(api, "list"):
                list_endpoint = methods["list"]
                listed_items = list(api.list(limit=1))
                if len(listed_items) == 0:
                    raise EndpointAssertionError(list_endpoint.path, "Expected at least 1 listed item, got 0")
        finally:
            api.delete([id])

    def test_all_cdf_resource_apis_registered(self) -> None:
        """Test that all CDFResourceAPI subclasses are registered in ToolkitClient."""
        existing_api = set(get_concrete_subclasses(CDFResourceAPI))  # type: ignore[type-abstract]
        generic_tested: set[type[CDFResourceAPI]] = {param[0][0] for param in crud_cdf_resource_apis()}

        missing_tests = existing_api - (generic_tested | NOT_GENERIC_TESTED)
        if missing_tests:
            missing_names = [cls.__name__ for cls in missing_tests]
            raise AssertionError(
                f"CDFResourceAPI subclasses missing {humanize_collection(missing_names)} tests in TestCDFResourceAPI.test_crud_list"
            )
