from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, Literal, final

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import InternalId, PostgresGatewayUserId
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    PostgresGatewayAcl,
    PostgresGatewayUsersScope,
    ScopeDefinition,
)
from cognite_toolkit._cdf_tk.client.resource_classes.postgres_gateway_user import (
    PostgresGatewayUserRequest,
    PostgresGatewayUserResponse,
    SessionCredentials,
)
from cognite_toolkit._cdf_tk.client.resource_classes.session import ClientCredentialsSessionRequest
from cognite_toolkit._cdf_tk.exceptions import ResourceCreationError, ResourceUpdateError, ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ResourceIO
from cognite_toolkit._cdf_tk.utils.cdf import read_auth, try_find_error
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import sanitize_filename
from cognite_toolkit._cdf_tk.yaml_classes import PostgresGatewayUserYAML

from ._auth import GroupAllScopedIO
from ._group_scoped import GroupResourceScopedIO

_INACTIVE_SESSION_STATUSES = frozenset({"CANCELLED", "EXPIRED", "REVOKED", "ACCESS_LOST"})


@final
class PostgresGatewayUserIO(
    ResourceIO[PostgresGatewayUserId, PostgresGatewayUserRequest, PostgresGatewayUserResponse, PostgresGatewayUserYAML]
):
    folder_name = "postgres_gateway"
    resource_cls = PostgresGatewayUserResponse
    resource_write_cls = PostgresGatewayUserRequest
    kind = "PostgresGatewayUser"
    yaml_cls = PostgresGatewayUserYAML
    dependencies = frozenset({GroupAllScopedIO, GroupResourceScopedIO})
    # CDF generates the username on create, so Toolkit cannot recreate a dropped user.
    support_drop = False
    _doc_url = "Postgres-Gateway-Users/operation/update_users"

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._credentials_by_username: dict[str, ClientCredentialsSessionRequest] = {}
        self._session_client_id_by_id: dict[int, str | None] = {}

    @property
    def display_name(self) -> str:
        return "Postgres gateway users"

    @classmethod
    def get_id(
        cls, item: PostgresGatewayUserRequest | PostgresGatewayUserResponse | PostgresGatewayUserId | dict[str, Any]
    ) -> PostgresGatewayUserId:
        if isinstance(item, PostgresGatewayUserId):
            return item
        if isinstance(item, dict):
            username = item.get("username")
            if not isinstance(username, str) or not username:
                raise ToolkitRequiredValueError("Postgres gateway user must have username set.")
            return PostgresGatewayUserId(username=username)
        return item.as_id()

    @classmethod
    def dump_id(cls, id: PostgresGatewayUserId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_filename(cls, id: PostgresGatewayUserId) -> str:
        return sanitize_filename(id.username)

    @classmethod
    def get_minimum_scope(cls, items: Sequence[PostgresGatewayUserRequest]) -> ScopeDefinition:
        return PostgresGatewayUsersScope(usernames=sorted({cls.get_id(item).username for item in items}))

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | PostgresGatewayUsersScope):
            yield PostgresGatewayAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def get_dependencies(cls, resource: PostgresGatewayUserYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        return []

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        resources = super().load_resource_file(filepath, environment_variables)
        for resource in resources:
            username = self.get_id(resource).username
            if "credentials" not in resource:
                self._credentials_by_username.pop(username, None)
                continue
            self._credentials_by_username[username] = read_auth(
                resource["credentials"],
                self.client.config,
                username,
                "Postgres gateway user",
                console=self.console,
            )
        return resources

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> PostgresGatewayUserRequest:
        # The client credentials are kept in _credentials_by_username and exchanged for a session nonce on update.
        resource.pop("credentials", None)
        return PostgresGatewayUserRequest._load(resource)

    def dump_resource(
        self, resource: PostgresGatewayUserResponse, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        dumped: dict[str, Any] = {"username": resource.username}
        local_credentials = (local or {}).get("credentials")
        if not isinstance(local_credentials, dict) or resource.session_id is None:
            return dumped
        client_id = self._lookup_session_client_id(resource.session_id)
        if client_id is not None:
            # CDF does not return the client secret, so a changed secret alone is only deployed with --force-update.
            dumped["credentials"] = {**local_credentials, "clientId": client_id}
        return dumped

    def _lookup_session_client_id(self, session_id: int) -> str | None:
        if session_id in self._session_client_id_by_id:
            return self._session_client_id_by_id[session_id]
        client_id: str | None = None
        try:
            sessions = self.client.sessions.retrieve([InternalId(id=session_id)])
        except ToolkitAPIError:
            # Missing sessionsAcl:LIST or an unknown session. The credentials are then redeployed.
            sessions = []
        if sessions and sessions[0].status not in _INACTIVE_SESSION_STATUSES:
            client_id = sessions[0].client_id
        self._session_client_id_by_id[session_id] = client_id
        return client_id

    def create(self, items: Sequence[PostgresGatewayUserRequest]) -> list[PostgresGatewayUserResponse]:
        if not items:
            return []
        usernames = humanize_collection([self.get_id(item).username for item in items])
        raise ResourceCreationError(
            f"Postgres gateway user(s) {usernames} do not exist in CDF. Toolkit cannot create Postgres gateway "
            "users, as CDF generates the username. Create the user outside Toolkit and use the generated "
            "username in the YAML file."
        )

    def retrieve(self, ids: Sequence[PostgresGatewayUserId]) -> list[PostgresGatewayUserResponse]:
        if not ids:
            return []
        return self.client.postgres_gateway.users.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[PostgresGatewayUserRequest]) -> list[PostgresGatewayUserResponse]:
        updated: list[PostgresGatewayUserResponse] = []
        for item in items:
            username = self.get_id(item).username
            credentials = self._credentials_by_username.get(username)
            if credentials is None:
                continue
            try:
                nonce = self.client.sessions.create_single(credentials).nonce
                request = PostgresGatewayUserRequest(username=username, credentials=SessionCredentials(nonce=nonce))
                updated.extend(self.client.postgres_gateway.users.update([request]))
            except ToolkitAPIError as e:
                if hint := try_find_error(credentials):
                    raise ResourceUpdateError(f"Failed to update Postgres gateway user {username!r}: {hint}") from e
                raise
        return updated

    def delete(self, ids: Sequence[PostgresGatewayUserId]) -> list[PostgresGatewayUserId]:
        id_list = list(ids)
        if not id_list:
            return []
        self.client.postgres_gateway.users.delete(id_list, ignore_unknown_ids=True)
        return id_list

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[PostgresGatewayUserResponse]:
        if data_set_external_id or space or parent_ids:
            return iter(())
        return (user for page in self.client.postgres_gateway.users.iterate(limit=None) for user in page)

    def sensitive_strings(self, item: PostgresGatewayUserRequest) -> Iterable[str]:
        if credentials := self._credentials_by_username.get(self.get_id(item).username):
            yield credentials.client_secret
