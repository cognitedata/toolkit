from typing import Any

import httpx
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.annotations import AnnotationsAPI
from cognite_toolkit._cdf_tk.client.api.filemetadata import FileMetadataAPI
from cognite_toolkit._cdf_tk.client.api.function_schedules import FunctionSchedulesAPI
from cognite_toolkit._cdf_tk.client.api.graphql_data_models import GraphQLDataModelsAPI
from cognite_toolkit._cdf_tk.client.api.location_filters import LocationFiltersAPI
from cognite_toolkit._cdf_tk.client.api.raw import RawTablesAPI
from cognite_toolkit._cdf_tk.client.api.search_config import SearchConfigurationsAPI
from cognite_toolkit._cdf_tk.client.api.streams import StreamsAPI
from cognite_toolkit._cdf_tk.client.api.workflow_triggers import WorkflowTriggersAPI
from cognite_toolkit._cdf_tk.client.api.workflow_versions import WorkflowVersionsAPI
from cognite_toolkit._cdf_tk.client.api.workflows import WorkflowsAPI
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import APIMethod
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.request_classes.filters import AnnotationFilter
from cognite_toolkit._cdf_tk.client.resource_classes.annotation import AnnotationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import (
    FunctionScheduleRequest,
    FunctionScheduleResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.graphql_data_model import (
    GraphQLDataModelResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.location_filter import LocationFilterResponse
from cognite_toolkit._cdf_tk.client.resource_classes.raw import RAWTableResponse
from cognite_toolkit._cdf_tk.client.resource_classes.search_config import SearchConfigResponse
from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow import WorkflowResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import (
    WorkflowTriggerRequest,
    WorkflowTriggerResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import WorkflowVersionResponse
from tests.test_unit.test_cdf_tk.test_client.data import (
    CDFResource,
    get_example_minimum_responses,
    iterate_cdf_resources,
)


class TestCDFResourceAPI:
    @pytest.mark.parametrize("resource", [param for param in iterate_cdf_resources() if param[0][0].api_class])
    def test_create_retrieve_delete_iterate_list(
        self, resource: CDFResource, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        """Test create, retrieve, delete, and list methods of CDFResourceAPI.

        Note that not all resources implement all methods; the test will skip methods that are not implemented.
        Furthermore, it is typically bad practice to test multiple functionalities in a single test case,
        but the setup is quite heavy, so we combine them here for efficiency.
        """
        if resource.api_class is None:
            pytest.skip("No API class defined for this resource")

        client = HTTPClient(toolkit_config)
        api = resource.api_class(client)
        if hasattr(api, "create"):
            self._mock_endpoint(api, "create", {"items": [resource.example_data]}, respx_mock)
            to_create = resource.request_instance
            created = api.create([to_create])
            assert len(created) == 1
            assert created[0].dump() == resource.example_data
        if hasattr(api, "retrieve"):
            self._mock_endpoint(api, "retrieve", {"items": [resource.example_data]}, respx_mock)
            to_retrieve = resource.resource_id

            retrieved = api.retrieve([to_retrieve])
            assert len(retrieved) == 1
            assert retrieved[0].dump() == resource.example_data
        if hasattr(api, "update"):
            self._mock_endpoint(api, "update", {"items": [resource.example_data]}, respx_mock)
            to_update = resource.request_instance

            updated = api.update([to_update])
            assert len(updated) == 1
            assert updated[0].dump() == resource.example_data
        if hasattr(api, "delete"):
            self._mock_endpoint(api, "delete", {"items": [resource.resource_id.dump()]}, respx_mock)
            to_delete = resource.resource_id

            _ = api.delete([to_delete])
            assert len(respx_mock.calls) >= 1  # At least one call should have been made
        if hasattr(api, "list"):
            self._mock_endpoint(api, "list", {"items": [resource.example_data]}, respx_mock)

            listed = api.list(limit=10)
            assert len(listed) >= 1
            assert listed[0].dump() == resource.example_data
        if hasattr(api, "paginate"):
            self._mock_endpoint(api, "list", {"items": [resource.example_data]}, respx_mock)

            page = api.paginate()
            assert isinstance(page, PagedResponse)
            assert len(page.items) == 1
            assert page.items[0].dump() == resource.example_data
        if hasattr(api, "iterate"):
            self._mock_endpoint(api, "list", {"items": [resource.example_data]}, respx_mock)

            batches = list(api.iterate())
            assert len(batches) >= 1
            items = [item for batch in batches for item in batch]
            assert items[0].dump() == resource.example_data

    @classmethod
    def _mock_endpoint(
        cls,
        api: CDFResourceAPI,
        api_method: APIMethod,
        json: dict[str, Any] | None,
        respx_mock: respx.MockRouter,
    ) -> None:
        if api_method in ("create", "update") and api_method not in api._method_endpoint_map:
            api_method = "upsert"
        endpoint = api._method_endpoint_map[api_method]
        url = api._make_url(endpoint.path)

        respx_mock.request(endpoint.method, url).mock(return_value=httpx.Response(status_code=200, json=json))

    #### These are tests for APIs that cannot use the generic test above
    # This is typically due to custom endpoints or request object cannot be made from response object
    ####

    def test_raw_table_api_crud(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        """Test RawTablesAPI create, delete, list, and iterate methods.

        This cannot be tested using the generic test above as it requires custom mocking of the
        api endpoints as database is part of the URL.
        """
        resource = get_example_minimum_responses(RAWTableResponse)
        instance = RAWTableResponse.model_validate(resource)
        request = instance.as_request_resource()
        config = toolkit_config
        api = RawTablesAPI(HTTPClient(config))

        # Test create
        respx_mock.post(config.create_api_url(f"/raw/dbs/{instance.db_name}/tables")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        created = api.create([request])
        assert len(created) == 1
        assert created[0] == instance

        # Test delete
        respx_mock.post(config.create_api_url(f"/raw/dbs/{instance.db_name}/tables/delete")).mock(
            return_value=httpx.Response(status_code=200)
        )
        api.delete([request.as_id()])

        # Test retrieve list
        respx_mock.get(config.create_api_url(f"/raw/dbs/{instance.db_name}/tables")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        listed = api.list(db_name=instance.db_name, limit=10)
        assert len(listed) == 1
        assert listed[0] == instance

        # Test iterate
        respx_mock.get(config.create_api_url(f"/raw/dbs/{instance.db_name}/tables")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        page = api.paginate(db_name=instance.db_name, limit=10)
        assert len(page.items) == 1
        assert page.items[0] == instance

    def test_metadataapi_crud_iterate(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        resource = get_example_minimum_responses(FileMetadataResponse)
        instance = FileMetadataResponse.model_validate(resource)
        config = toolkit_config
        api = FileMetadataAPI(HTTPClient(config))
        request_item = instance.as_request_resource()

        # Test create
        respx_mock.post(config.create_api_url("/files")).mock(
            return_value=httpx.Response(status_code=200, json=resource)
        )
        created = api.create([request_item], overwrite=False)
        assert len(created) == 1
        assert created[0].dump() == resource

        # Test retrieve
        respx_mock.post(config.create_api_url("/files/byids")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        retrieved = api.retrieve([instance.as_id()])
        assert len(retrieved) == 1
        assert retrieved[0].dump() == resource

        # Test update
        respx_mock.post(config.create_api_url("/files/update")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        updated = api.update([request_item])
        assert len(updated) == 1
        assert updated[0].dump() == resource

        # Test delete
        respx_mock.post(config.create_api_url("/files/delete")).mock(return_value=httpx.Response(status_code=200))
        api.delete([instance.as_id()])
        assert len(respx_mock.calls) >= 1  # At least one call should have

        # Test iterate/list/paginate
        respx_mock.post(config.create_api_url("/files/list")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )

        listed = api.list(limit=10)
        assert len(listed) == 1
        assert listed[0].dump() == resource

        page = api.paginate(limit=10)
        assert len(page.items) == 1
        assert page.items[0].dump() == resource

        iterated = list(api.iterate(limit=10))
        assert len(iterated) >= 1
        items = [item for batch in iterated for item in batch]
        assert items[0].dump() == resource

    def test_workflow_api_crud_list_methods(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        resource = get_example_minimum_responses(WorkflowResponse)
        instance = WorkflowResponse.model_validate(resource)
        config = toolkit_config
        api = WorkflowsAPI(HTTPClient(config))
        request_item = instance.as_request_resource()

        # Test create/update (same endpoint)
        respx_mock.post(config.create_api_url("/workflows")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        created = api.create([request_item])
        assert len(created) == 1
        assert created[0].dump() == resource

        updated = api.update([request_item])
        assert len(updated) == 1
        assert updated[0].dump() == resource

        # Test retrieve
        respx_mock.get(config.create_api_url(f"/workflows/{instance.external_id}")).mock(
            return_value=httpx.Response(status_code=200, json=resource)
        )
        retrieved = api.retrieve([instance.as_id()])
        assert len(retrieved) == 1
        assert retrieved[0].dump() == resource

        # Test delete
        respx_mock.post(config.create_api_url("/workflows/delete")).mock(return_value=httpx.Response(status_code=200))
        api.delete([instance.as_id()])
        assert len(respx_mock.calls) >= 1  # At least one call should have been made

        # Test iterate/list
        respx_mock.get(config.create_api_url("/workflows")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        listed = api.list(limit=10)
        assert len(listed) == 1
        assert listed[0].dump() == resource

        page = api.paginate(limit=10)
        assert len(page.items) == 1
        assert page.items[0].dump() == resource

    def test_workflow_version_api_crud_list_methods(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        resource = get_example_minimum_responses(WorkflowVersionResponse)
        instance = WorkflowVersionResponse.model_validate(resource)
        config = toolkit_config
        api = WorkflowVersionsAPI(HTTPClient(config))
        request_item = instance.as_request_resource()

        # Test create/update (same endpoint)
        respx_mock.post(config.create_api_url("/workflows/versions")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        created = api.create([request_item])
        assert len(created) == 1
        assert created[0].dump() == resource

        updated = api.update([request_item])
        assert len(updated) == 1
        assert updated[0].dump() == resource

        # Test retrieve
        respx_mock.get(
            config.create_api_url(f"/workflows/{instance.workflow_external_id}/versions/{instance.version}")
        ).mock(return_value=httpx.Response(status_code=200, json=resource))
        retrieved = api.retrieve([instance.as_id()])
        assert len(retrieved) == 1
        assert retrieved[0].dump() == resource

        # Test delete
        respx_mock.post(config.create_api_url("/workflows/versions/delete")).mock(
            return_value=httpx.Response(status_code=200)
        )
        api.delete([instance.as_id()])
        assert len(respx_mock.calls) >= 1  # At least one call should have been made

        # Test iterate/list
        respx_mock.post(config.create_api_url("/workflows/versions/list")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        listed = api.list(limit=10)
        assert len(listed) == 1
        assert listed[0].dump() == resource

        page = api.paginate(limit=10)
        assert len(page.items) == 1
        assert page.items[0].dump() == resource

    def test_workflow_trigger_api_crud_list_methods(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        resource = get_example_minimum_responses(WorkflowTriggerResponse)
        instance = WorkflowTriggerResponse.model_validate(resource)
        config = toolkit_config
        api = WorkflowTriggersAPI(HTTPClient(config))
        request_item = WorkflowTriggerRequest.model_validate(
            {**resource, "authentication": {"nonce": "test-nonce"}}, extra="ignore"
        )

        # Test create/update (same endpoint)
        respx_mock.post(config.create_api_url("/workflows/triggers")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        created = api.create([request_item])
        assert len(created) == 1
        assert created[0].dump() == resource

        updated = api.update([request_item])
        assert len(updated) == 1
        assert updated[0].dump() == resource

        # Test delete
        respx_mock.post(config.create_api_url("/workflows/triggers/delete")).mock(
            return_value=httpx.Response(status_code=200)
        )
        api.delete([instance.as_id()])
        assert len(respx_mock.calls) >= 1  # At least one call should have been made

        # Test iterate/list
        respx_mock.get(config.create_api_url("/workflows/triggers")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        listed = api.list(limit=10)
        assert len(listed) == 1
        assert listed[0].dump() == resource

        page = api.paginate(limit=10)
        assert len(page.items) == 1
        assert page.items[0].dump() == resource

    def test_function_schedule_api_crud_list_methods(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        resource = get_example_minimum_responses(FunctionScheduleResponse)

        config = toolkit_config
        api = FunctionSchedulesAPI(HTTPClient(config))
        request_item = FunctionScheduleRequest.model_validate(
            {**resource, "nonce": "test-nonce", "id": 1}, extra="ignore"
        )
        # Test create
        respx_mock.post(config.create_api_url("/functions/schedules")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        created = api.create([request_item])
        assert len(created) == 1
        assert created[0].dump() == resource

        # Test retrieve
        respx_mock.post(config.create_api_url("/functions/schedules/byids")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        retrieved = api.retrieve([request_item.as_id()])
        assert len(retrieved) == 1
        assert retrieved[0].dump() == resource

        # Test delete
        respx_mock.post(config.create_api_url("/functions/schedules/delete")).mock(
            return_value=httpx.Response(status_code=200)
        )
        api.delete([request_item.as_id()])
        assert len(respx_mock.calls) >= 1  # At least one call should have been made

        # Test iterate/list/paginate
        respx_mock.post(config.create_api_url("/functions/schedules/list")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        listed = api.list(limit=10)
        assert len(listed) == 1
        assert listed[0].dump() == resource

        page = api.paginate(limit=10)
        assert len(page.items) == 1
        assert page.items[0].dump() == resource

        iterated = list(api.iterate(limit=10))
        assert len(iterated) == 1
        assert iterated[0][0].dump() == resource

    def test_annotation_api_crud_list_methods(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        resource = get_example_minimum_responses(AnnotationResponse)
        instance = AnnotationResponse.model_validate(resource)
        config = toolkit_config
        request_item = instance.as_request_resource()
        api = AnnotationsAPI(HTTPClient(config))
        filter = AnnotationFilter(
            annotated_resource_type="file", annotated_resource_ids=[ExternalId(external_id="my_file")]
        )

        # Test create
        respx_mock.post(config.create_api_url("/annotations")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        created = api.create([request_item])
        assert len(created) == 1
        assert created[0].dump() == resource

        # Test retrieve
        respx_mock.post(config.create_api_url("/annotations/byids")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        retrieved = api.retrieve([instance.as_id()])
        assert len(retrieved) == 1
        assert retrieved[0].dump() == resource

        # Test update
        respx_mock.post(config.create_api_url("/annotations/update")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        updated = api.update([request_item], mode="replace")
        assert len(updated) == 1
        assert updated[0].dump() == resource

        # Test delete
        respx_mock.post(config.create_api_url("/annotations/delete")).mock(return_value=httpx.Response(status_code=200))
        api.delete([instance.as_id()])
        assert len(respx_mock.calls) >= 1  # At least one call should have been made

        # Test iterate/list/paginate
        respx_mock.post(config.create_api_url("/annotations/list")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        listed = api.list(limit=10, filter=filter)
        assert len(listed) == 1
        assert listed[0].dump() == resource

        page = api.paginate(limit=10, filter=filter)
        assert len(page.items) == 1
        assert page.items[0].dump() == resource

        iterated = list(api.iterate(limit=10, filter=filter))
        assert len(iterated) == 1
        assert iterated[0][0].dump() == resource

    def test_stream_api_crud_list_methods(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        resource = get_example_minimum_responses(StreamResponse)
        instance = StreamResponse.model_validate(resource)
        config = toolkit_config
        api = StreamsAPI(HTTPClient(config))
        request_item = instance.as_request_resource()

        # Test create
        respx_mock.post(config.create_api_url("/streams")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        created = api.create([request_item])
        assert len(created) == 1
        assert created[0].dump() == resource

        # Test retrieve
        respx_mock.get(config.create_api_url(f"/streams/{instance.external_id}")).mock(
            return_value=httpx.Response(status_code=200, json=resource)
        )
        retrieved = api.retrieve([request_item.as_id()])
        assert len(retrieved) == 1
        assert retrieved[0].dump() == resource

        # Test delete
        respx_mock.post(config.create_api_url("/streams/delete")).mock(return_value=httpx.Response(status_code=200))
        api.delete([request_item.as_id()])
        assert len(respx_mock.calls) >= 1  # At least one call should have been made

        # Test list
        respx_mock.get(config.create_api_url("/streams")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        listed = api.list()
        assert len(listed) == 1
        assert listed[0].dump() == resource

    def test_graphql_data_models_api_custom_methods(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        resource = get_example_minimum_responses(GraphQLDataModelResponse)
        instance = GraphQLDataModelResponse.model_validate(resource)
        config = toolkit_config
        api = GraphQLDataModelsAPI(HTTPClient(config))
        request_item = instance.as_request_resource()

        # Test create/update (same endpoint)
        respx_mock.post(config.create_api_url("/dml/graphql")).mock(
            return_value=httpx.Response(status_code=200, json={"upsertGraphQlDmlVersion": {"data": resource}})
        )
        created = api.create([request_item])
        assert len(created) == 1
        assert created[0].dump() == resource

        updated = api.update([request_item])
        assert len(updated) == 1
        assert updated[0].dump() == resource

        # Test retrieve
        respx_mock.post(config.create_api_url("/models/datamodels/byids")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        retrieved = api.retrieve([request_item.as_id()], inline_views=False)
        assert len(retrieved) == 1
        assert retrieved[0].dump() == resource

        # Test delete
        respx_mock.post(config.create_api_url("/models/datamodels/delete")).mock(
            return_value=httpx.Response(status_code=200)
        )
        api.delete([request_item.as_id()])
        assert len(respx_mock.calls) >= 1  # At least one call should have been made

        # Test iterate/list/paginate
        respx_mock.get(config.create_api_url("/models/datamodels")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        listed = api.list(limit=10)
        assert len(listed) == 1
        assert listed[0].dump() == resource

        page = api.paginate(limit=10)
        assert len(page.items) == 1
        assert page.items[0].dump() == resource

        iterated = list(api.iterate(limit=10))
        assert len(iterated) == 1
        assert iterated[0][0].dump() == resource

    def test_search_config_crud_list_methods(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        resource = get_example_minimum_responses(SearchConfigResponse)
        instance = SearchConfigResponse.model_validate(resource)
        config = toolkit_config
        api = SearchConfigurationsAPI(HTTPClient(config))
        request_item = instance.as_request_resource()

        # Test create/update
        respx_mock.post(config.create_app_url("/storage/config/apps/search/views/upsert")).mock(
            return_value=httpx.Response(status_code=200, json=resource)
        )
        created = api.create([request_item])
        assert len(created) == 1
        assert created[0].dump() == resource

        updated = api.update([request_item])
        assert len(updated) == 1
        assert updated[0].dump() == resource

        # Test iterate/list/paginate
        respx_mock.post(config.create_app_url("/storage/config/apps/search/views/list")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        listed = api.list()
        assert len(listed) == 1
        assert listed[0].dump() == resource

        page = api.paginate()
        assert len(page.items) == 1
        assert page.items[0].dump() == resource

        iterated = list(api.iterate())
        assert len(iterated) == 1
        assert iterated[0][0].dump() == resource

    def test_location_filter_crud_list_methods(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        resource = get_example_minimum_responses(LocationFilterResponse)
        instance = LocationFilterResponse.model_validate(resource)
        config = toolkit_config
        api = LocationFiltersAPI(HTTPClient(config))
        request_item = instance.as_request_resource()

        # Test create
        respx_mock.post(config.create_app_url("/storage/config/locationfilters")).mock(
            return_value=httpx.Response(status_code=200, json=resource)
        )
        created = api.create([request_item])
        assert len(created) == 1
        assert created[0].dump() == resource

        # Test retrieve
        respx_mock.post(config.create_app_url("/storage/config/locationfilters/byids")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        retrieved = api.retrieve([request_item.as_id()])
        assert len(retrieved) == 1
        assert retrieved[0].dump() == resource

        # Test update
        respx_mock.put(config.create_app_url(f"/storage/config/locationfilters/{instance.id}")).mock(
            return_value=httpx.Response(status_code=200, json=resource)
        )
        updated = api.update([request_item])
        assert len(updated) == 1
        assert updated[0].dump() == resource

        # Test delete
        respx_mock.delete(config.create_app_url(f"/storage/config/locationfilters/{instance.id}")).mock(
            return_value=httpx.Response(status_code=200, json=resource)
        )
        deleted = api.delete([request_item.as_id()])
        assert len(respx_mock.calls) >= 1  # At least one call should have been made
        assert len(deleted) == 1
        assert deleted[0].dump() == resource

        # Test iterate/list/paginate
        respx_mock.post(config.create_app_url("/storage/config/locationfilters/list")).mock(
            return_value=httpx.Response(status_code=200, json={"items": [resource]})
        )
        listed = api.list()
        assert len(listed) == 1
        assert listed[0].dump() == resource

        page = api.paginate()
        assert len(page.items) == 1
        assert page.items[0].dump() == resource

        iterated = list(api.iterate())
        assert len(iterated) == 1
        assert iterated[0][0].dump() == resource
