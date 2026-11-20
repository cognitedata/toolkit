"""Applications API for managing applications registered for an organization.

Based on the alpha API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Applications
"""

import builtins
from collections.abc import Iterable, Sequence
from functools import partial
from typing import Literal

from cognite_toolkit._cdf_tk.client.api.project import ProjectAPI
from cognite_toolkit._cdf_tk.client.cdf_client.api import CDFResourceAPI, Endpoint
from cognite_toolkit._cdf_tk.client.cdf_client.responses import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
    ToolkitAPIError,
)
from cognite_toolkit._cdf_tk.client.identifiers import ApplicationId
from cognite_toolkit._cdf_tk.client.resource_classes.application import ApplicationRequest, ApplicationResponse

_API_VERSION = "alpha"
_WRITE_LIMIT = 10
_LIST_LIMIT = 50


class ApplicationsAPI(CDFResourceAPI[ApplicationResponse]):
    """API for the Cognite Applications endpoints.

    Applications are registered for an organization, not a project, and are served from the auth host.
    """

    def __init__(self, http_client: HTTPClient, project_api: ProjectAPI) -> None:
        super().__init__(
            http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/orgs/{org}/applications", item_limit=_WRITE_LIMIT),
                "retrieve": Endpoint(method="GET", path="/orgs/{org}/applications/{client_id}", item_limit=1),
                "update": Endpoint(method="POST", path="/orgs/{org}/applications/update", item_limit=_WRITE_LIMIT),
                "delete": Endpoint(method="POST", path="/orgs/{org}/applications/delete", item_limit=_WRITE_LIMIT),
                "list": Endpoint(method="GET", path="/orgs/{org}/applications", item_limit=_LIST_LIMIT),
            },
            api_version=_API_VERSION,
        )
        self._project_api = project_api

    def _make_url(self, path: str = "") -> str:
        """Create the full URL for this resource endpoint."""
        return self._http_client.config.create_auth_url(path)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ApplicationResponse]:
        return PagedResponse[ApplicationResponse].model_validate_json(response.body)

    def _org_path(self, method: Literal["create", "update", "delete", "list"]) -> str:
        return self._method_endpoint_map[method].path.format(org=self._project_api.get_organization_id())

    def create(self, items: Sequence[ApplicationRequest]) -> builtins.list[ApplicationResponse]:
        """Create applications.

        Args:
            items: Applications to create. At most 10 per request.
        Returns:
            The created applications, including their generated client IDs.
        """
        return self._request_item_response(items, "create", endpoint=self._org_path("create"))

    def retrieve(
        self, items: Sequence[ApplicationId], ignore_unknown_ids: bool = False
    ) -> builtins.list[ApplicationResponse]:
        """Retrieve applications by client ID.

        The API has no batch retrieve endpoint, so each application is fetched with its own request.

        Args:
            items: Client IDs of the applications to retrieve.
            ignore_unknown_ids: Skip applications that are not found.
        Returns:
            The retrieved applications.
        """
        endpoint = self._method_endpoint_map["retrieve"]
        org = self._project_api.get_organization_id()
        results: builtins.list[ApplicationResponse] = []
        for item in items:
            request = RequestMessage(
                endpoint_url=self._make_url(endpoint.path.format(org=org, client_id=item.id)),
                method=endpoint.method,
                api_version=self._api_version,
            )
            response = self._http_client.request_single_retries(request)
            if isinstance(response, SuccessResponse):
                results.append(ApplicationResponse.model_validate_json(response.body))
                continue
            try:
                response.get_success_or_raise(request)
            except ToolkitAPIError as e:
                if ignore_unknown_ids and e.code == 404:
                    continue
                raise
        return results

    def update(
        self, items: Sequence[ApplicationRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> builtins.list[ApplicationResponse]:
        """Update applications.

        Args:
            items: Applications to update. Each must have its client ID set. At most 10 per request.
            mode: ``replace`` sets every field, clearing unset ones. ``patch`` sets only fields that were assigned.
        Returns:
            The updated applications.
        """
        responses = self._chunk_requests(
            items,
            "update",
            serialization=partial(self._serialize_updates, mode=mode),
            endpoint_path=self._org_path("update"),
        )
        return [item for response in responses for item in self._validate_page_response(response).items]

    def delete(self, items: Sequence[ApplicationId]) -> None:
        """Delete applications by client ID.

        Args:
            items: Client IDs of the applications to delete. At most 10 per request.
        """
        self._request_no_response(items, "delete", endpoint=self._org_path("delete"))

    def paginate(self, limit: int = _LIST_LIMIT, cursor: str | None = None) -> PagedResponse[ApplicationResponse]:
        """Fetch one page of applications in the organization.

        Args:
            limit: Maximum number of applications to return. The server caps this at 50.
            cursor: Cursor for the next page.
        Returns:
            One page of applications.
        """
        return self._paginate(limit, cursor, endpoint_path=self._org_path("list"))

    def iterate(self, limit: int | None = None) -> Iterable[builtins.list[ApplicationResponse]]:
        """Iterate over applications in the organization.

        Args:
            limit: Maximum number of applications to return in total. None returns all applications.
        Returns:
            Pages of applications.
        """
        return self._iterate(limit=limit, endpoint_path=self._org_path("list"))

    def list(self, limit: int | None = None) -> builtins.list[ApplicationResponse]:
        """List applications in the organization.

        Args:
            limit: Maximum number of applications to return. None returns all applications.
        Returns:
            Applications.
        """
        return self._list(limit=limit, endpoint_path=self._org_path("list"))
