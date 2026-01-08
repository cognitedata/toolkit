from typing import Any

import httpx
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import APIMethod
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from tests.test_unit.test_cdf_tk.test_client.data import CDFResource, iterate_cdf_resources


class TestCDFResourceAPI:
    @pytest.mark.parametrize("resource", list(iterate_cdf_resources()))
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
            self._mock_endpoint(api, "delete", None, respx_mock)
            to_delete = resource.resource_id

            _ = api.delete([to_delete])
            assert len(respx_mock.calls) >= 1  # At least one call should have been made
        if hasattr(api, "list"):
            self._mock_endpoint(api, "list", {"items": [resource.example_data]}, respx_mock)

            listed = api.list(limit=10)
            assert len(listed) >= 1
            assert listed[0].dump() == resource.example_data
        if hasattr(api, "iterate"):
            self._mock_endpoint(api, "list", {"items": [resource.example_data]}, respx_mock)

            page = api.iterate()
            assert isinstance(page, PagedResponse)
            assert len(page.items) == 1
            assert page.items[0].dump() == resource.example_data

    @classmethod
    def _mock_endpoint(
        cls,
        api: CDFResourceAPI,
        api_method: APIMethod,
        json: dict[str, Any] | None,
        respx_mock: respx.MockRouter,
    ) -> None:
        endpoint = api._method_endpoint_map[api_method]
        url = api._make_url(endpoint.path)

        respx_mock.request(endpoint.method, url).mock(return_value=httpx.Response(status_code=200, json=json))
