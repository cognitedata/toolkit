from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import PostgresGatewayUserId, RawDatabaseId, RawTableId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.postgres_gateway_table import PostgresGatewayTableResponse
from cognite_toolkit._cdf_tk.client.resource_classes.postgres_gateway_user import (
    PostgresGatewayUserRequest,
    PostgresGatewayUserResponse,
    SessionCredentials,
)
from cognite_toolkit._cdf_tk.client.resource_classes.session import (
    ClientCredentialsSessionRequest,
    SessionCreateResponse,
    SessionResponse,
)
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.exceptions import ResourceCreationError
from cognite_toolkit._cdf_tk.resource_ios import (
    PostgresGatewayTableIO,
    PostgresGatewayUserIO,
    RawDatabaseIO,
    RawTableIO,
    ViewIO,
)

RAW_TABLE = {
    "username": "pg_user_001",
    "tablename": "my_raw_rows",
    "type": "raw_rows",
    "options": {"database": "my_db", "table": "my_table", "primaryKey": "key"},
    "columns": {"key": {"type": "TEXT"}, "value": {"type": "BIGINT"}},
}
VIEW_TABLE = {
    "username": "pg_user_001",
    "tablename": "my_assets",
    "type": "view",
    "options": {"space": "cdf_cdm", "externalId": "CogniteAsset", "version": "v1"},
}

LOCAL_USER = """username: pg_user_001
credentials:
  clientId: my-client-id
  clientSecret: my-client-secret
"""

CDF_USER = PostgresGatewayUserResponse(username="pg_user_001", created_time=0, session_id=42)


def _load_local(io: PostgresGatewayUserIO, content: str = LOCAL_USER) -> dict[str, Any]:
    filepath = MagicMock(spec=Path)
    filepath.read_text.return_value = content
    return io.load_resource_file(filepath, {})[0]


class TestPostgresGatewayUserIO:
    @pytest.mark.parametrize(
        "sessions, expected_credentials",
        [
            pytest.param(
                [SessionResponse(id=42, client_id="my-client-id", status="ACTIVE")],
                {"clientId": "my-client-id", "clientSecret": "my-client-secret"},
                id="Same client is unchanged",
            ),
            pytest.param(
                [SessionResponse(id=42, client_id="other-client-id", status="ACTIVE")],
                {"clientId": "other-client-id", "clientSecret": "my-client-secret"},
                id="Different client is changed",
            ),
            pytest.param(
                [SessionResponse(id=42, client_id="my-client-id", status="REVOKED")],
                None,
                id="Revoked session is changed",
            ),
            pytest.param(ToolkitAPIError("Missing sessionsAcl:LIST"), None, id="Unknown session is changed"),
        ],
    )
    def test_dump_resource_compares_session_client(
        self, sessions: list[SessionResponse] | Exception, expected_credentials: dict[str, str] | None
    ) -> None:
        client = ToolkitClientMock()
        if isinstance(sessions, Exception):
            client.sessions.retrieve.side_effect = sessions
        else:
            client.sessions.retrieve.return_value = sessions
        io = PostgresGatewayUserIO(client)
        local = _load_local(io)

        dumped = io.dump_resource(CDF_USER, local)

        expected: dict[str, Any] = {"username": "pg_user_001"}
        if expected_credentials is not None:
            expected["credentials"] = expected_credentials
        assert dumped == expected

    def test_update_sets_session_nonce(self) -> None:
        client = ToolkitClientMock()
        client.sessions.create.return_value = [
            SessionCreateResponse(id=43, client_id="my-client-id", status="READY", nonce="nonce-1")
        ]
        client.postgres_gateway.users.update.return_value = [CDF_USER]
        io = PostgresGatewayUserIO(client)
        local = _load_local(io)

        io.update([io.load_resource(local)])

        assert {
            "session": client.sessions.create.call_args.args[0],
            "update": client.postgres_gateway.users.update.call_args.args[0],
        } == {
            "session": [ClientCredentialsSessionRequest(client_id="my-client-id", client_secret="my-client-secret")],
            "update": [
                PostgresGatewayUserRequest(username="pg_user_001", credentials=SessionCredentials(nonce="nonce-1"))
            ],
        }

    def test_create_raises_as_username_is_generated_by_cdf(self) -> None:
        io = PostgresGatewayUserIO(ToolkitClientMock())

        with pytest.raises(ResourceCreationError, match="Create the user outside Toolkit"):
            io.create([PostgresGatewayUserRequest(username="pg_user_001")])


class TestPostgresGatewayTableIO:
    @pytest.mark.parametrize(
        "local, cdf_response",
        [
            pytest.param(RAW_TABLE, {k: v for k, v in RAW_TABLE.items() if k != "username"}, id="RAW table"),
            pytest.param(
                VIEW_TABLE,
                # The API returns the view columns and type, but not the view itself.
                {"tablename": "my_assets", "type": "nodes", "columns": {"name": {"type": "TEXT"}}, "options": {}},
                id="View table",
            ),
        ],
    )
    def test_dump_resource_is_unchanged(self, local: dict[str, Any], cdf_response: dict[str, Any]) -> None:
        cdf_table = PostgresGatewayTableResponse.model_validate(cdf_response)
        cdf_table.username = "pg_user_001"

        assert PostgresGatewayTableIO(ToolkitClientMock()).dump_resource(cdf_table, local) == local

    def test_retrieve_tables_of_missing_user_returns_empty(self) -> None:
        client = ToolkitClientMock()
        client.postgres_gateway.tables.retrieve.side_effect = ToolkitAPIError(
            "User with given username does not exist", code=404
        )
        io = PostgresGatewayTableIO(client)

        assert io.retrieve([io.get_id(RAW_TABLE)]) == []

    def test_get_dependencies(self) -> None:
        raw_table = PostgresGatewayTableIO.validate_object(RAW_TABLE)
        view_table = PostgresGatewayTableIO.validate_object(VIEW_TABLE)
        user = (PostgresGatewayUserIO, PostgresGatewayUserId(username="pg_user_001"))

        assert {
            "raw": list(PostgresGatewayTableIO.get_dependencies(raw_table)),
            "view": list(PostgresGatewayTableIO.get_dependencies(view_table)),
        } == {
            "raw": [
                user,
                (RawDatabaseIO, RawDatabaseId(name="my_db")),
                (RawTableIO, RawTableId(db_name="my_db", name="my_table")),
            ],
            "view": [user, (ViewIO, ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1"))],
        }

    @pytest.mark.parametrize(
        "data",
        [
            pytest.param({k: v for k, v in RAW_TABLE.items() if k != "columns"}, id="RAW table without columns"),
            pytest.param({**RAW_TABLE, "options": VIEW_TABLE["options"]}, id="RAW table with view options"),
            pytest.param({**VIEW_TABLE, "columns": RAW_TABLE["columns"]}, id="View table with columns"),
        ],
    )
    def test_yaml_rejects_options_not_matching_type(self, data: dict[str, Any]) -> None:
        with pytest.raises(ValidationError):
            PostgresGatewayTableIO.validate_object(data)
