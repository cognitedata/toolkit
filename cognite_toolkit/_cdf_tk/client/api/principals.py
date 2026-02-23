"""Principals API for managing CDF principals.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Principals
"""

from collections.abc import Iterable, Sequence

from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.client.api.project import ProjectAPI
from cognite_toolkit._cdf_tk.client.cdf_client.api import CDFResourceAPI, Endpoint
from cognite_toolkit._cdf_tk.client.cdf_client.responses import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import (
    ExternalId,
    PrincipalId,
    PrincipalLoginId,
)
from cognite_toolkit._cdf_tk.client.resource_classes.principal import LoginSession, Principal, PrincipalType
from cognite_toolkit._cdf_tk.utils.useful_types import PrimitiveType


class PrincipalLoginSessionsAPI(CDFResourceAPI[LoginSession]):
    """API for the Cognite Principal Login Sessions endpoints.

    This API is used to manage login sessions for principals. It is not project-scoped.
    """

    def __init__(self, http_client: HTTPClient, project_api: ProjectAPI) -> None:
        self._revoke = Endpoint(method="POST", path="/orgs/{org}/principals/{principal}/sessions/revoke", item_limit=10)
        super().__init__(
            http_client,
            method_endpoint_map={
                "list": Endpoint(method="GET", path="/orgs/{org}/principals/{principal}/sessions", item_limit=100),
                # Misusing retrieve here to use the generic _request_item_response method.
                "retrieve": self._revoke,
            },
        )
        self._project_api = project_api

    def _make_url(self, path: str = "") -> str:
        """Create the full URL for this resource endpoint."""
        return self._http_client.config.create_auth_url(path)

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[LoginSession]:
        return PagedResponse[LoginSession].model_validate_json(response.body)

    def revoke(self, items: list[PrincipalLoginId]) -> None:
        """Revoke login sessions for a principal.

        Args:
            items: A list of PrincipalLoginId objects representing the sessions to revoke.
        """
        for principal, principal_items in self._group_items_by_text_field(items, "principal").items():
            path = self._revoke.path.format(org=self._project_api.get_organization_id(), principal=principal)
            self._request_no_response(items, "retrieve", endpoint=path)

    def paginate(self, principal_id: str, limit: int = 10, cursor: str | None = None) -> PagedResponse[LoginSession]:
        """Paginate through all login sessions for a principal.

        Args:
            principal_id: The ID of the principal to list sessions for.
            limit: The maximum number of items to return per page. Default is 10.
        Returns:
            A sequence of LoginSession objects.
        """
        path = self._method_endpoint_map["list"].path.format(
            org=self._project_api.get_organization_id(), principal=principal_id
        )
        result = self._paginate(limit, cursor, endpoint_path=path)
        for item in result.items:
            item.principal = principal_id
        return result

    def iterate(
        self, principal_id: str, limit: int | None = 10, cursor: str | None = None
    ) -> Iterable[list[LoginSession]]:
        """Iterate through all login sessions for a principal."""
        endpoint_path = self._method_endpoint_map["list"].path.format(
            org=self._project_api.get_organization_id(), principal=principal_id
        )
        for items in self._iterate(endpoint_path=endpoint_path, limit=limit, cursor=cursor):
            for item in items:
                item.principal = principal_id
            yield items

    def list(self, principal_id: str, limit: int | None = 10) -> list[LoginSession]:
        """List login sessions for a principal.

        Args:
            principal_id: The ID of the principal to list sessions for.
            limit: The maximum number of items to return. Default is 10.
        Returns:
            A list of LoginSession objects.
        """
        endpoint_path = self._method_endpoint_map["list"].path.format(
            org=self._project_api.get_organization_id(), principal=principal_id
        )
        results = self._list(endpoint_path=endpoint_path, limit=limit)
        for item in results:
            item.principal = principal_id
        return results


class PrincipalsAPI(CDFResourceAPI[Principal]):
    """API for the Cognite Principals endpoints.

    Unlike most CDF APIs, the Principals API is not project-scoped.
    The `me` endpoint uses `/api/v1/principals/me`, and all other
    endpoints use `/api/v1/orgs/{org}/principals/...`.
    """

    def __init__(self, http_client: HTTPClient, project_api: ProjectAPI) -> None:
        super().__init__(
            http_client,
            method_endpoint_map={
                "list": Endpoint(method="GET", path="/orgs/{org}/principals", item_limit=100),
                "retrieve": Endpoint(method="POST", path="/orgs/{org}/principals/byids", item_limit=100),
            },
        )
        self._me_endpoint = Endpoint(method="GET", path="/principals/me", item_limit=1)
        self._project_api = project_api
        self.login_sessions = PrincipalLoginSessionsAPI(http_client, project_api)

    def _make_url(self, path: str = "") -> str:
        """Create the full URL for this resource endpoint."""
        return self._http_client.config.create_auth_url(path)

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[Principal]:
        return PagedResponse[Principal].model_validate_json(response.body)

    def me(self) -> Principal:
        """Get the current caller's principal information.

        Returns:
            The principal (ServiceAccountPrincipal or UserPrincipal) that issued the request.
        """
        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._http_client.config.create_auth_url(self._me_endpoint.path),
                method=self._me_endpoint.method,
            )
        ).get_success_or_raise()
        return TypeAdapter(Principal).validate_json(response.body)

    def retrieve(self, items: Sequence[PrincipalId | ExternalId], ignore_unknown_ids: bool = False) -> list[Principal]:
        """Retrieve principals by their IDs or external IDs.

        Args:
            items: A sequence of Principal or ExternalId objects to retrieve.
            ignore_unknown_ids: If True, unknown IDs will be ignored and not cause an error. Default is False.
        """
        path = self._method_endpoint_map["retrieve"].path.format(org=self._project_api.get_organization_id())
        return self._request_item_response(
            items, "retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}, endpoint=path
        )

    def _create_list_parameters(self, types: list[PrincipalType] | None) -> dict[str, PrimitiveType] | None:
        """Create the query parameters for the list endpoint."""
        if types is None:
            return None
        return {"types": ",".join(types)}

    def paginate(
        self, types: list[PrincipalType] | None = None, limit: int = 10, cursor: str | None = None
    ) -> PagedResponse[Principal]:
        """Paginate through all principals in the organization.

        Args:
            limit: The maximum number of items to return per page. Default is 10.

        Returns:
            A sequence of Principal objects.
        """
        path = self._method_endpoint_map["list"].path.format(org=self._project_api.get_organization_id())
        return self._paginate(limit, cursor, params=self._create_list_parameters(types), endpoint_path=path)

    def iterate(
        self, types: list[PrincipalType] | None = None, limit: int | None = 10, cursor: str | None = None
    ) -> Iterable[list[Principal]]:
        """Iterate through all principals in the organization."""
        endpoint_path = self._method_endpoint_map["list"].path.format(org=self._project_api.get_organization_id())
        return self._iterate(
            params=self._create_list_parameters(types), endpoint_path=endpoint_path, limit=limit, cursor=cursor
        )

    def list(self, types: list[PrincipalType] | None = None, limit: int = 10) -> list[Principal]:
        """List principals in the organization.

        Args:
            limit: The maximum number of items to return. Default is 10.

        Returns:
            A list of Principal objects.
        """
        endpoint_path = self._method_endpoint_map["list"].path.format(org=self._project_api.get_organization_id())
        return self._list(params=self._create_list_parameters(types), endpoint_path=endpoint_path, limit=limit)
