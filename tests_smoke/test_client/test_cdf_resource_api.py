import types
from collections.abc import Callable, Hashable, Iterable, Set
from typing import Annotated, Any, get_args, get_origin

import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import RequestResource, T_ResponseResource
from cognite_toolkit._cdf_tk.client.api.datasets import DataSetsAPI
from cognite_toolkit._cdf_tk.client.api.hosted_extractor_jobs import HostedExtractorJobsAPI
from cognite_toolkit._cdf_tk.client.api.instances import InstancesAPI
from cognite_toolkit._cdf_tk.client.api.raw import RawDatabasesAPI, RawTablesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_capabilities import CapabilitiesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_data_postprocessing import DataPostProcessingAPI
from cognite_toolkit._cdf_tk.client.api.robotics_frames import FramesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_locations import LocationsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_maps import MapsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_robots import RobotsAPI
from cognite_toolkit._cdf_tk.client.api.simulator_models import SimulatorModelsAPI
from cognite_toolkit._cdf_tk.client.cdf_client.api import CDFResourceAPI, Endpoint
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import EdgeRequest, NodeRequest
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest, DataSetResponse
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
from tests_smoke.constants import SMOKE_SPACE
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
        DataSetsAPI,
        # RAW tables depend on existing RAW databases, so they are tested together.
        RawDatabasesAPI,
        RawTablesAPI,
        # Job depends on source and destination, so tested together.
        HostedExtractorJobsAPI,
        # Edge depend on nodes
        InstancesAPI,
    }
)


def crud_cdf_resource_apis() -> Iterable[tuple]:
    subclasses = get_concrete_subclasses(CDFResourceAPI)  # type: ignore[type-abstract]
    for api_cls in subclasses:
        if api_cls in NOT_GENERIC_TESTED:
            continue
        cdf_resource_base = next((base for base in api_cls.__orig_bases__ if get_origin(base) is CDFResourceAPI), None)  # type: ignore[attr-defined]
        assert cdf_resource_base is not None, "Error in test. Could not find CDFResourceAPI in __orig_bases__"
        _, request_cls, __ = get_args(cdf_resource_base)
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


def get_examples_minimum_requests(request_cls: type[RequestResource]) -> list[dict[str, Any]]:
    """Return an example with the only required and identifier fields for the given resource class."""
    requests: dict[type[RequestResource], list[dict[str, Any]]] = {
        AssetRequest: [{"name": "smoke-test-asset", "externalId": "smoke-test-asset"}],
        DataSetRequest: [{"externalId": "smoke-tests-crudl-dataset"}],
        EventRequest: [{"externalId": "smoke-test-event"}],
        FileMetadataRequest: [{"name": "smoke-test-file", "externalId": "smoke-test-file"}],
        ExtractionPipelineRequest: [
            {
                "name": "smoke-test-pipeline",
                "externalId": "smoke-test-pipeline",
                "dataSetId": -1,
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
                "name": "smoke-test-rest-source",
                "externalId": "smoke-test-rest-source",
                "endpoint": "https://smoke-test-endpoint",
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
                "type": "eventHub",
                "externalId": "smoke-test-eventhub-source",
                "host": "sb://smoke-test.servicebus.windows.net/",
                "eventHubName": "smoke-test-hub",
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
        RAWDatabase: [{"name": "smoke-test-raw-database"}],
        RAWTable: [{"name": "smoke-test-raw-table", "dbName": "smoke-test-raw-database"}],
        SecurityCategoryRequest: [
            {"name": "smoke-test-security-category", "externalId": "smoke-test-security-category"}
        ],
        SequenceRequest: [
            {"externalId": "smoke-test-sequence", "columns": [{"externalId": "smoke-test-sequence-column"}]}
        ],
        StreamRequest: [{"externalId": "smoke-test-stream", "settings": {"template": {"name": "ImmutableTestStream"}}}],
        ThreeDModelClassicRequest: [{"name": "smoke-test-3d-model-classic"}],
        ThreeDModelDMSRequest: [{"name": "smoke-test-3d-model-dms", "space": SMOKE_SPACE, "type": "CAD"}],
        AssetMappingClassicRequest: [{"externalId": "smoke-test-asset-mapping-classic", "model3dId": 1, "assetId": 1}],
        AssetMappingDMRequest: [
            {
                "externalId": "smoke-test-asset-mapping-dm",
                "model3dId": 1,
                "nodeId": "smoke-test-node",
            }
        ],
        TimeSeriesRequest: [{"externalId": "smoke-test-timeseries"}],
        TransformationRequest: [
            {
                "name": "smoke-test-transformation",
                "externalId": "smoke-test-transformation",
                "ignoreNullFields": True,
            }
        ],
        WorkflowRequest: [{"externalId": "smoke-test-workflow"}],
        WorkflowTriggerRequest: [
            {
                "name": "smoke-test-workflow-trigger",
                "externalId": "smoke-test-workflow-trigger",
                "workflowExternalId": "smoke-test-workflow",
                "type": "ON_DEMAND",
            }
        ],
        WorkflowVersionRequest: [{"workflowExternalId": "smoke-test-workflow", "version": "v1", "definition": {}}],
    }
    try:
        return requests[request_cls]
    except KeyError:
        raise NotImplementedError(f"No example request defined for {request_cls.__name__}")


@pytest.mark.usefixtures("smoke_space")
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
                listed_items = list(api.list(limit=1))
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

    def test_raw_tables_and_databases_crudl(self, toolkit_client: ToolkitClient) -> None:
        client = toolkit_client

        database_example = get_examples_minimum_requests(RAWDatabase)[0]
        table_example = get_examples_minimum_requests(RAWTable)[0]
        db = RAWDatabase.model_validate(database_example)
        table = RAWTable.model_validate(table_example)

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
