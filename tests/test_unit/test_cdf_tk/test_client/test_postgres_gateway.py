import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.postgres_gateway_tables import PostgresGatewayTablesAPI
from cognite_toolkit._cdf_tk.client.api.postgres_gateway_users import PostgresGatewayUsersAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import PostgresGatewayTableId, PostgresGatewayUserId
from cognite_toolkit._cdf_tk.client.resource_classes.postgres_gateway_table import (
    BuiltInTableResponse,
    PostgresGatewayColumn,
    PostgresGatewayTableRequestAdapter,
    PostgresGatewayTableResponseAdapter,
    RawRowsTableRequest,
    RawRowsTableResponse,
    RawTableOptions,
    UnknownTableRequest,
    UnknownTableResponse,
    ViewTableOptions,
    ViewTableRequest,
    ViewTableResponse,
)
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


class TestPostgresGatewayTablesAPI:
    @pytest.mark.usefixtures("disable_gzip")
    def test_crdl(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        api = PostgresGatewayTablesAPI(HTTPClient(toolkit_config))
        raw_table = {
            "tablename": "raw_table",
            "type": "raw_rows",
            "columns": {"key": {"type": "TEXT"}},
            "options": {"database": "my_db", "table": "my_table"},
        }
        view_table = {"tablename": "view_table", "type": "nodes", "columns": {"name": {"type": "TEXT"}}, "options": {}}
        base = "/postgresgateway/tables/pg_user_001"
        respx_mock.post(api._make_url(base)).mock(
            return_value=httpx2.Response(
                status_code=200, json={"items": [{**raw_table, "createdTime": 1}, {**view_table, "createdTime": 1}]}
            )
        )
        respx_mock.post(api._make_url(f"{base}/byids")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [raw_table]})
        )
        respx_mock.post(api._make_url(f"{base}/delete")).mock(return_value=httpx2.Response(status_code=200, json={}))
        list_route = respx_mock.get(api._make_url(base)).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [raw_table, view_table]})
        )
        table_ids = [
            PostgresGatewayTableId(username="pg_user_001", tablename="raw_table"),
            PostgresGatewayTableId(username="pg_user_001", tablename="view_table"),
        ]

        created = api.create(
            [
                RawRowsTableRequest(
                    username="pg_user_001",
                    tablename="raw_table",
                    options=RawTableOptions(database="my_db", table="my_table"),
                    columns={"key": PostgresGatewayColumn(type="TEXT")},
                ),
                ViewTableRequest(
                    username="pg_user_001",
                    tablename="view_table",
                    options=ViewTableOptions(space="my_space", external_id="MyView", version="v1"),
                ),
            ]
        )
        retrieved = api.retrieve(table_ids[:1], ignore_unknown_ids=True)
        api.delete(table_ids, ignore_unknown_ids=True)
        listed = api.list("pg_user_001", include_built_ins=True, limit=10)

        assert {
            "created": [item.as_id() for item in created],
            "retrieved": [item.as_request_resource().dump() for item in retrieved],
            "listed": [(item.as_id(), type(item)) for item in listed],
            "list_params": dict(list_route.calls.last.request.url.params),
            "requests": [
                json.loads(call.request.content) for call in respx_mock.calls if call.request.method == "POST"
            ],
        } == {
            "created": table_ids,
            "retrieved": [raw_table],
            "listed": [(table_ids[0], RawRowsTableResponse), (table_ids[1], ViewTableResponse)],
            "list_params": {"includeBuiltIns": "true", "limit": "10"},
            "requests": [
                {
                    "items": [
                        raw_table,
                        {
                            "tablename": "view_table",
                            "type": "view",
                            "options": {"space": "my_space", "externalId": "MyView", "version": "v1"},
                        },
                    ]
                },
                {"items": [{"tablename": "raw_table"}], "ignoreUnknownIds": True},
                {"items": [{"tablename": "raw_table"}, {"tablename": "view_table"}], "ignoreUnknownIds": True},
            ],
        }

    def test_load_table_response_by_type(self) -> None:
        loaded = [
            PostgresGatewayTableResponseAdapter.validate_python({"tablename": "t", "type": table_type, "columns": {}})
            for table_type in ["built_in", "edges", "new_type"]
        ]

        assert [type(item) for item in loaded] == [BuiltInTableResponse, ViewTableResponse, UnknownTableResponse]

    def test_unknown_table_request_round_trip(self) -> None:
        data = {"tablename": "t", "type": "new_type", "options": {"foo": "bar"}}

        request = PostgresGatewayTableRequestAdapter.validate_python({**data, "username": "u"})
        response = PostgresGatewayTableResponseAdapter.validate_python(data)
        response.username = "u"

        assert isinstance(request, UnknownTableRequest)
        assert request.dump() == data
        assert response.as_request_resource() == request
