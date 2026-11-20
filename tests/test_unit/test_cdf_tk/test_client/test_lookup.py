import json
from collections.abc import Iterator, Sequence
from typing import Any, Protocol

import httpx2
import pytest
import respx
from _pytest.mark import ParameterSet
from cognite.client.data_classes import Event, FileMetadata
from cognite.client.data_classes.capabilities import Capability, EventsAcl, FilesAcl

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.group import UserProfilesAcl
from cognite_toolkit._cdf_tk.exceptions import AuthorizationError
from tests.test_unit.utils import FakeCogniteResourceGenerator


class _IdResource(Protocol):
    id: int
    external_id: str

    def dump(self, camel_case: bool = True) -> dict[str, Any]: ...


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
        resource_cls: type[_IdResource],
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
        resource_cls: type[_IdResource],
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
        resource_cls: type[_IdResource],
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
    def _create_resources(resource_cls: type[_IdResource], N: int) -> list[_IdResource]:
        generator = FakeCogniteResourceGenerator(seed=42)
        resources = [generator.create_instance(resource_cls) for _ in range(N)]
        for i, resource in enumerate(resources):
            resource.id = i + 1000
            resource.external_id = f"ext-{i + 1000}"
        return resources

    @staticmethod
    def _create_client_mock(
        endpoint: str, resources: list[_IdResource], rsps: respx.MockRouter, config: ToolkitClientConfig
    ) -> ToolkitClient:
        resource_by_id = {resource.id: resource for resource in resources}
        resource_by_external_id = {resource.external_id: resource for resource in resources}

        def retrieve_multiple_callback(request: httpx2.Request) -> httpx2.Response:
            items = json.loads(request.content)["items"]
            response_items = []
            for item in items:
                if "id" in item and (resource := resource_by_id.get(item["id"])) is not None:
                    response_items.append(resource.dump(camel_case=True))
                elif "externalId" in item and (resource := resource_by_external_id.get(item["externalId"])) is not None:
                    response_items.append(resource.dump(camel_case=True))
            return httpx2.Response(200, json={"items": response_items})

        rsps.post(config.create_api_url(f"{endpoint}/byids")).mock(side_effect=retrieve_multiple_callback)
        client = ToolkitClient(config)
        return client


@pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
class TestUserLookup:
    ORG_ID = "test-org"

    @classmethod
    def _create_client(cls, config: ToolkitClientConfig) -> ToolkitClient:
        client = ToolkitClient(config=config)
        # Avoid the organization lookup request.
        client.project.get_organization_id = lambda: cls.ORG_ID  # ty: ignore[invalid-assignment]
        return client

    @classmethod
    def _principals_url(cls, config: ToolkitClientConfig) -> str:
        return config.create_auth_url(f"/orgs/{cls.ORG_ID}/principals")

    @staticmethod
    def _user_principal(id_: str, email: str | None) -> dict[str, Any]:
        return {
            "id": id_,
            "type": "USER",
            "name": f"User {id_}",
            "pictureUrl": "https://example.com/picture.png",
            "email": email,
        }

    @staticmethod
    def _user_profile(user_identifier: str, email: str | None) -> dict[str, Any]:
        return {
            "userIdentifier": user_identifier,
            "email": email,
            "identityType": "USER",
            "lastUpdatedTime": 1000000000000,
        }

    def test_lookup_email_and_user_id(self, toolkit_config: ToolkitClientConfig, rsps: respx.MockRouter) -> None:
        rsps.get(self._principals_url(toolkit_config)).respond(
            status_code=200,
            json={
                "items": [
                    self._user_principal("user-1", "user1@example.com"),
                    self._user_principal("user-2", "user2@example.com"),
                    self._user_principal("user-3", None),
                ]
            },
        )
        client = self._create_client(toolkit_config)
        users = client.lookup.users

        assert users.email("user-1") == "user1@example.com"
        assert users.email(["user-1", "user-2"]) == ["user1@example.com", "user2@example.com"]
        assert users.user_id("user2@example.com") == "user-2"
        assert users.user_id(["user1@example.com", "user2@example.com"]) == ["user-1", "user-2"]

        assert len(rsps.calls) == 1  # Users are only listed once.

    def test_lookup_missing_users(self, toolkit_config: ToolkitClientConfig, rsps: respx.MockRouter) -> None:
        rsps.get(self._principals_url(toolkit_config)).respond(
            status_code=200,
            json={"items": [self._user_principal("user-1", "user1@example.com"), self._user_principal("user-2", None)]},
        )
        client = self._create_client(toolkit_config)
        users = client.lookup.users

        assert users.email("user-2") is None
        assert users.email("non-existing") is None
        assert users.email(["user-1", "non-existing"]) == ["user1@example.com"]
        assert users.user_id("missing@example.com") is None
        assert users.user_id(("user1@example.com", "missing@example.com")) == ["user-1"]

    def test_fallback_to_user_profiles(self, toolkit_config: ToolkitClientConfig, rsps: respx.MockRouter) -> None:
        rsps.get(self._principals_url(toolkit_config)).respond(status_code=401)
        rsps.get(toolkit_config.create_api_url("/profiles")).respond(
            status_code=200,
            json={
                "items": [
                    self._user_profile("user-1", "user1@example.com"),
                    self._user_profile("user-2", "user2@example.com"),
                ]
            },
        )
        client = self._create_client(toolkit_config)
        users = client.lookup.users

        assert users.email("user-1") == "user1@example.com"
        assert users.user_id(["user2@example.com"]) == ["user-2"]

    def test_principals_error_is_raised(self, toolkit_config: ToolkitClientConfig, rsps: respx.MockRouter) -> None:
        rsps.get(self._principals_url(toolkit_config)).respond(status_code=500)
        client = self._create_client(toolkit_config)

        with pytest.raises(ToolkitAPIError):
            client.lookup.users.email("user-1")

    def test_missing_access(self, toolkit_config: ToolkitClientConfig, rsps: respx.MockRouter) -> None:
        rsps.get(self._principals_url(toolkit_config)).respond(status_code=401)
        rsps.get(toolkit_config.create_api_url("/profiles")).respond(status_code=403)
        rsps.get(f"{toolkit_config.base_url}/api/v1/token/inspect").respond(
            json={
                "subject": "123",
                "projects": [],
                "capabilities": [],
            }
        )
        client = self._create_client(toolkit_config)

        with pytest.raises(AuthorizationError) as exc_info:
            client.lookup.users.email("user-1")

        assert UserProfilesAcl.__name__ in str(exc_info.value)
