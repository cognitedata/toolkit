from unittest.mock import MagicMock

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.organization_users import OrganizationUsersAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.resource_classes.organization_user import OrganizationUser
from tests.test_unit.test_cdf_tk.test_client.data import get_example_minimum_responses

ORG_ID = "my-org"
USERS_URL = f"https://auth.cognite.com/api/v0/orgs/{ORG_ID}/users"


@pytest.fixture
def organization_users_api(toolkit_config: ToolkitClientConfig) -> OrganizationUsersAPI:
    project_api = MagicMock()
    project_api.get_organization_id.return_value = ORG_ID
    return OrganizationUsersAPI(HTTPClient(toolkit_config), project_api)


def _user(user_id: str) -> dict[str, str]:
    return {**get_example_minimum_responses(OrganizationUser), "id": user_id}


@pytest.mark.usefixtures("disable_gzip")
class TestOrganizationUsersAPI:
    def test_list_follows_cursor(
        self, organization_users_api: OrganizationUsersAPI, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.get(USERS_URL).mock(
            side_effect=[
                httpx2.Response(200, json={"items": [_user("user-1")], "nextCursor": "next"}),
                httpx2.Response(200, json={"items": [_user("user-2")]}),
            ]
        )

        users = organization_users_api.list()

        assert {
            "ids": [user.id for user in users],
            "params": [dict(call.request.url.params) for call in route.calls],
            "cdf-version": {call.request.headers["cdf-version"] for call in route.calls},
        } == {"ids": ["user-1", "user-2"], "params": [{}, {"cursor": "next"}], "cdf-version": {"beta"}}

    def test_list_truncates_to_limit_and_uses_given_org(
        self, organization_users_api: OrganizationUsersAPI, respx_mock: respx.MockRouter
    ) -> None:
        respx_mock.get("https://auth.cognite.com/api/v0/orgs/other-org/users").mock(
            return_value=httpx2.Response(200, json={"items": [_user("user-1"), _user("user-2")], "nextCursor": "next"})
        )

        users = organization_users_api.list(org="other-org", limit=1)

        assert [user.id for user in users] == ["user-1"]
