import json
from collections.abc import Iterator, Sequence

import httpx
import pytest
import respx
from _pytest.mark import ParameterSet
from cognite.client.data_classes import Event, FileMetadata
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.capabilities import Capability, EventsAcl, FilesAcl

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.exceptions import AuthorizationError
from tests.test_unit.utils import FakeCogniteResourceGenerator


@pytest.fixture
def rsps() -> Iterator[respx.MockRouter]:
    with respx.mock() as rsps:
        yield rsps


@pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
class TestLookup:
    CASES: Sequence[ParameterSet] = [
        pytest.param("events", Event, EventsAcl, id="events"),
        pytest.param("files", FileMetadata, FilesAcl, id="files"),
    ]

    @pytest.mark.parametrize("endpoint,resource_cls,cap_cls", CASES)
    def test_lookup_id(
        self,
        endpoint: str,
        resource_cls: type[CogniteResource],
        cap_cls: type[Capability],
        toolkit_config: ToolkitClientConfig,
        rsps: respx.MockRouter,
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

    @pytest.mark.parametrize("endpoint,resource_cls,cap_cls", CASES)
    def test_lookup_external_id(
        self,
        endpoint: str,
        resource_cls: type[CogniteResource],
        cap_cls: type[Capability],
        toolkit_config: ToolkitClientConfig,
        rsps: respx.MockRouter,
    ) -> None:
        resources = self._create_resources(resource_cls, N=5)
        client = self._create_client_mock(endpoint, resources, rsps, toolkit_config)
        lookup_api = getattr(client.lookup, endpoint)

        # We are testing both orders as the lookup caches the results.
        single_id = lookup_api.id(external_id=resources[0].external_id)
        assert single_id == resources[0].id

        multiple_ids = lookup_api.id(external_id=[resource.external_id for resource in resources])
        assert multiple_ids == [resource.id for resource in resources]

        single_external_di = lookup_api.external_id(id=resources[0].id)
        assert single_external_di == resources[0].external_id

        multiple_external_ids = lookup_api.external_id(id=[resource.id for resource in resources])
        assert multiple_external_ids == [resource.external_id for resource in resources]

    @pytest.mark.parametrize("endpoint,resource_cls,cap_cls", CASES)
    def test_missing_access(
        self,
        endpoint: str,
        resource_cls: type[CogniteResource],
        cap_cls: type[Capability],
        toolkit_config: ToolkitClientConfig,
        rsps: respx.MockRouter,
    ) -> None:
        rsps.post(toolkit_config.create_api_url(f"{endpoint}/byids")).respond(status_code=403)
        rsps.get(f"{toolkit_config.base_url}/api/v1/token/inspect").respond(
            json={
                "subject": "123",
                "projects": [],
                "capabilities": [],
            }
        )
        client = ToolkitClient(config=toolkit_config)
        lookup_api = getattr(client.lookup, endpoint)

        with pytest.raises(AuthorizationError) as exc_info:
            lookup_api.id(external_id="non-existing")

        assert cap_cls.__name__ in str(exc_info.value)

    def test_lookup_external_id_missing(
        self,
        toolkit_config: ToolkitClientConfig,
        rsps: respx.MockRouter,
    ) -> None:
        config = toolkit_config
        rsps.post(config.create_api_url("assets/byids")).respond(
            status_code=200,
            json={
                "items": [
                    {
                        "id": 1,
                        "externalId": "ext-1",
                        "name": "Asset 1",
                        "rootId": 1,
                        "createdTime": 1000000000000,
                        "lastUpdatedTime": 1000000000000,
                    }
                ]
            },
        )
        client = ToolkitClient(config=config)
        result = client.lookup.assets.external_id([1, 2, 3])
        assert result == ["ext-1"]
        result2 = client.lookup.assets.external_id([1, 2, 3])
        assert result2 == ["ext-1"]

        assert len(rsps.calls) == 1  # Cached result used for second call

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
        endpoint: str, resources: list[CogniteResource], rsps: respx.MockRouter, config: ToolkitClientConfig
    ) -> ToolkitClient:
        resource_by_id = {resource.id: resource for resource in resources}
        resource_by_external_id = {resource.external_id: resource for resource in resources}

        def retrieve_multiple_callback(request: httpx.Request) -> httpx.Response:
            items = json.loads(request.content)["items"]
            response_items = []
            for item in items:
                if "id" in item and (resource := resource_by_id.get(item["id"])) is not None:
                    response_items.append(resource.dump(camel_case=True))
                elif "externalId" in item and (resource := resource_by_external_id.get(item["externalId"])) is not None:
                    response_items.append(resource.dump(camel_case=True))
            return httpx.Response(200, json={"items": response_items})

        rsps.post(config.create_api_url(f"{endpoint}/byids")).mock(side_effect=retrieve_multiple_callback)
        client = ToolkitClient(config)
        return client
