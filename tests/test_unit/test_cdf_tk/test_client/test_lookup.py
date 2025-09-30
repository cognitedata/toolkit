import json

import pytest
import requests
import responses
from cognite.client.data_classes import Event, FileMetadata
from cognite.client.data_classes._base import CogniteResource

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from tests.test_unit.utils import FakeCogniteResourceGenerator


class TestLookup:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    @pytest.mark.parametrize(
        "endpoint,resource_cls",
        [
            pytest.param("events", Event, id="events"),
            pytest.param("files", FileMetadata, id="files"),
        ],
    )
    def test_lookup_id(
        self,
        endpoint: str,
        resource_cls: type[CogniteResource],
        toolkit_config: ToolkitClientConfig,
        rsps: responses.RequestsMock,
    ) -> None:
        resources = self._create_resources(resource_cls, N=5)
        client = self._create_client_mock(endpoint, resources, rsps, toolkit_config)
        lookup_api = getattr(client.lookup, endpoint)

        multiple_external_ids = lookup_api.external_id(id=[resource.id for resource in resources])
        assert multiple_external_ids == [resource.external_id for resource in resources]

        single_external_di = lookup_api.external_id(id=resources[0].id)
        assert single_external_di == resources[0].external_id

        multiple_ids = lookup_api.id(external_id=[resource.external_id for resource in resources])
        assert multiple_ids == [resource.id for resource in resources]

        single_id = lookup_api.id(external_id=resources[0].external_id)
        assert single_id == resources[0].id

    @staticmethod
    def _create_resources(resource_cls: type[CogniteResource], N: int) -> list[CogniteResource]:
        generator = FakeCogniteResourceGenerator(seed=42)
        resources = [generator.create_instance(resource_cls) for _ in range(N)]
        for i, resource in enumerate(resources):
            resource.id = i + 1000
            resource.external_id = f"ext-{i + 1000}"
        return resources

    @staticmethod
    def _create_client_mock(
        endpoint: str, resources: list[CogniteResource], rsps: responses.RequestsMock, config: ToolkitClientConfig
    ) -> ToolkitClient:
        resource_by_id = {resource.id: resource for resource in resources}
        resource_by_external_id = {resource.external_id: resource for resource in resources}

        def retrieve_multiple_callback(request: requests.PreparedRequest) -> tuple[int, dict[str, str], str]:
            items = json.loads(request.body)["items"]
            response_items = []
            for item in items:
                if "id" in item and (resource := resource_by_id.get(item["id"])) is not None:
                    response_items.append(resource.dump(camel_case=True))
                elif "externalId" in item and (resource := resource_by_external_id.get(item["externalId"])) is not None:
                    response_items.append(resource.dump(camel_case=True))
            # Return format: (status code, headers, body)
            return 200, {}, json.dumps({"items": response_items})

        rsps.add_callback(
            responses.POST, config.create_api_url(f"{endpoint}/byids"), callback=retrieve_multiple_callback
        )
        client = ToolkitClient(config)
        return client
