import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.postgres_gateway_users import PostgresGatewayUsersAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import PostgresGatewayUserId
from cognite_toolkit._cdf_tk.client.resource_classes.postgres_gateway_user import (
    PostgresGatewayUserRequest,
    SessionCredentials,
)


class TestPostgresGatewayUsersAPI:
    @pytest.mark.usefixtures("disable_gzip")
    def test_crudl(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        api = PostgresGatewayUsersAPI(HTTPClient(toolkit_config))
        user = {"username": "pg_user_001", "createdTime": 1730204346000, "sessionId": 123}
        created_user = {**user, "host": "fdw.bluefield.cogniteapp.com", "password": "generated-password"}
        respx_mock.post(api._make_url("/postgresgateway")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [created_user]})
        )
        respx_mock.post(api._make_url("/postgresgateway/byids")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [user]})
        )
        respx_mock.post(api._make_url("/postgresgateway/update")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [user]})
        )
        respx_mock.post(api._make_url("/postgresgateway/delete")).mock(
            return_value=httpx2.Response(status_code=200, json={})
        )
        respx_mock.get(api._make_url("/postgresgateway")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [user]})
        )
        user_id = PostgresGatewayUserId(username="pg_user_001")

        created = api.create(
            [PostgresGatewayUserRequest(username="ignored", credentials=SessionCredentials(nonce="nonce-1"))]
        )
        retrieved = api.retrieve([user_id], ignore_unknown_ids=True)
        updated = api.update(
            [PostgresGatewayUserRequest(username="pg_user_001", credentials=SessionCredentials(nonce="nonce-2"))]
        )
        api.delete([user_id], ignore_unknown_ids=True)
        listed = api.list(limit=1)

        assert {
            "created": [item.dump() for item in created],
            "retrieved": [item.dump() for item in retrieved],
            "updated": [item.dump() for item in updated],
            "listed": [item.dump() for item in listed],
            "requests": [
                json.loads(call.request.content) for call in respx_mock.calls if call.request.method == "POST"
            ],
        } == {
            "created": [created_user],
            "retrieved": [user],
            "updated": [user],
            "listed": [user],
            "requests": [
                {"items": [{"credentials": {"nonce": "nonce-1"}}]},
                {"items": [{"username": "pg_user_001"}], "ignoreUnknownIds": True},
                {
                    "items": [
                        {"username": "pg_user_001", "update": {"credentials": {"set": {"nonce": "nonce-2"}}}},
                    ]
                },
                {"items": [{"username": "pg_user_001"}], "ignoreUnknownIds": True},
            ],
        }
