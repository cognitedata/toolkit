from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
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
from cognite_toolkit._cdf_tk.resource_ios import PostgresGatewayUserIO

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
