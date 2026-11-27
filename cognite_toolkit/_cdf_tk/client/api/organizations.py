"""Organizations API for managing CDF organizations and their projects.

The endpoints are served from https://auth.cognite.com and are not project-scoped.
The `/api/v1/...` endpoints are GA, while the `/api/admin/...` endpoints are beta (alpha for the updates).

Based on the API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Organizations
"""

import builtins
from typing import Any, Literal

from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.client.api.project import ProjectAPI
from cognite_toolkit._cdf_tk.client.cdf_client.api import CDFResourceAPI, Endpoint
from cognite_toolkit._cdf_tk.client.cdf_client.responses import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.organization import (
    IdentityProvider,
    OrgProjectCreateResponse,
    OrgProjectRequest,
    OrgProjectResponse,
    OrgRequest,
    OrgResponse,
    OrgWithContactPersonsResponse,
)


class _OrgScopedAPI:
    """Shared helpers for APIs where the organization defaults to the organization of the current project."""

    _http_client: HTTPClient
    _project_api: ProjectAPI
    _api_version: str | None

    def _org(self, org: str | None) -> str:
        return org if org is not None else self._project_api.get_organization_id()

    def _make_url(self, path: str = "") -> str:
        return self._http_client.config.create_auth_url(path)

    def _send(self, request: RequestMessage) -> SuccessResponse:
        return self._http_client.request_single_retries(request).get_success_or_raise(request)


class _OrgAdminScopedAPI(_OrgScopedAPI):
    def _make_url(self, path: str = "") -> str:
        return self._http_client.config.create_auth_admin_url(path)


class OrganizationProjectsAPI(_OrgScopedAPI, CDFResourceAPI[OrgProjectResponse]):
    """Projects in an organization (GA, `/api/v1/orgs/{org}/projects`)."""

    def __init__(self, http_client: HTTPClient, project_api: ProjectAPI) -> None:
        super().__init__(
            http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/orgs/{org}/projects", item_limit=1),
                "list": Endpoint(method="GET", path="/orgs/{org}/projects", item_limit=1000),
            },
        )
        self._project_api = project_api

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[OrgProjectResponse]:
        return PagedResponse[OrgProjectResponse].model_validate_json(response.body)

    def create(self, item: OrgProjectRequest, org: str | None = None) -> OrgProjectCreateResponse:
        """Create a project in an organization.

        Args:
            item: The project to create.
            org: The organization to create the project in. Defaults to the organization of the current project.

        Returns:
            The created project.
        """
        path = self._method_endpoint_map["create"].path.format(org=self._org(org))
        (response,) = self._chunk_requests([item], "create", self._serialize_items, endpoint_path=path)
        return PagedResponse[OrgProjectCreateResponse].model_validate_json(response.body).items[0]

    def list(
        self, org: str | None = None, include_admin_properties: bool = False, limit: int | None = None
    ) -> builtins.list[OrgProjectResponse]:
        """List the projects in an organization.

        Args:
            org: The organization to list projects for. Defaults to the organization of the current project.
            include_admin_properties: Whether to include the cluster name, state and deletion time of the projects.
            limit: The maximum number of projects to return. None returns all.

        Returns:
            The projects in the organization.
        """
        path = self._method_endpoint_map["list"].path.format(org=self._org(org))
        params = {"includeAdminProperties": True} if include_admin_properties else None
        return self._collect_pages(
            PagedResponse[OrgProjectResponse], path, "GET", limit=limit, page_size=None, params=params
        )


class OrganizationAdminProjectsAPI(_OrgAdminScopedAPI, CDFResourceAPI[OrgProjectResponse]):
    """Projects in an organization (beta, `/api/admin/orgs/{org}/projects`)."""

    def __init__(self, http_client: HTTPClient, project_api: ProjectAPI) -> None:
        super().__init__(
            http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/orgs/{org}/projects", item_limit=1),
                "list": Endpoint(method="GET", path="/orgs/{org}/projects", item_limit=1000),
            },
            api_version="beta",
        )
        self._project_api = project_api

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[OrgProjectResponse]:
        return PagedResponse[OrgProjectResponse].model_validate_json(response.body)

    def create(self, item: OrgProjectRequest, org: str | None = None) -> OrgProjectCreateResponse:
        """Create a project in an organization.

        Args:
            item: The project to create.
            org: The organization to create the project in. Defaults to the organization of the current project.

        Returns:
            The created project.
        """
        endpoint = self._method_endpoint_map["create"]
        request = RequestMessage(
            endpoint_url=self._make_url(endpoint.path.format(org=self._org(org))),
            method=endpoint.method,
            body_content=item.dump(),
            api_version=self._api_version,
        )
        return OrgProjectCreateResponse.model_validate_json(self._send(request).body)

    def list(self, org: str | None = None, limit: int | None = None) -> builtins.list[OrgProjectResponse]:
        """List the projects in an organization.

        Args:
            org: The organization to list projects for. Defaults to the organization of the current project.
            limit: The maximum number of projects to return. None returns all.

        Returns:
            The projects in the organization.
        """
        path = self._method_endpoint_map["list"].path.format(org=self._org(org))
        return self._collect_pages(PagedResponse[OrgProjectResponse], path, "GET", limit=limit, page_size=None)


class OrganizationAdminAPI(_OrgAdminScopedAPI, CDFResourceAPI[OrgResponse]):
    """Organizations (beta, `/api/admin/orgs/...`). The update methods are alpha."""

    def __init__(self, http_client: HTTPClient, project_api: ProjectAPI) -> None:
        super().__init__(
            http_client,
            method_endpoint_map={
                "retrieve": Endpoint(method="GET", path="/orgs/{org}", item_limit=1),
                "delete": Endpoint(method="DELETE", path="/orgs/{org}", item_limit=1),
                "create": Endpoint(method="POST", path="/orgs/{org}/orgs", item_limit=1),
                "list": Endpoint(method="GET", path="/orgs/{org}/orgs", item_limit=1000),
            },
            api_version="beta",
        )
        self._project_api = project_api
        self.projects = OrganizationAdminProjectsAPI(http_client, project_api)

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[OrgResponse]:
        return PagedResponse[OrgResponse].model_validate_json(response.body)

    def _request(
        self,
        method: Literal["GET", "POST", "PUT", "DELETE"],
        org: str,
        suffix: str = "",
        body: dict[str, Any] | None = None,
        data: bytes | None = None,
        content_type: str = "application/json",
        api_version: str | None = None,
    ) -> SuccessResponse:
        request = RequestMessage(
            endpoint_url=self._make_url(f"/orgs/{org}{suffix}"),
            method=method,
            body_content=body,
            data_content=data,
            content_type=content_type,
            api_version=api_version or self._api_version,
        )
        return self._send(request)

    def retrieve(self, org: str | None = None) -> OrgWithContactPersonsResponse:
        """Retrieve an organization.

        Args:
            org: The organization to retrieve. Defaults to the organization of the current project.
        """
        response = self._request("GET", self._org(org))
        return OrgWithContactPersonsResponse.model_validate_json(response.body)

    def delete(self, org: str) -> None:
        """Delete an organization.

        Args:
            org: The organization to delete.
        """
        self._request("DELETE", org)

    def list_children(self, org: str | None = None, limit: int | None = None) -> builtins.list[OrgResponse]:
        """List the child organizations of an organization.

        Args:
            org: The parent organization. Defaults to the organization of the current project.
            limit: The maximum number of organizations to return. None returns all.
        """
        response = self._request("GET", self._org(org), "/orgs")
        children = TypeAdapter(builtins.list[OrgResponse]).validate_json(response.body)
        return children if limit is None else children[:limit]

    def create_child(self, item: OrgRequest, org: str | None = None) -> OrgResponse:
        """Create a child organization.

        Args:
            item: The organization to create.
            org: The parent organization. Defaults to the organization of the current project.
        """
        response = self._request("POST", self._org(org), "/orgs", body=item.dump())
        return OrgResponse.model_validate_json(response.body)

    def update_admin_group_id(self, admin_group_id: str, org: str | None = None) -> None:
        """Update the externally managed group that contains the organization's admins (alpha).

        If the caller is not a member of the new group, they lose admin access to the organization immediately.

        Args:
            admin_group_id: The ID of the group in the external identity provider.
            org: The organization to update. Defaults to the organization of the current project.
        """
        self._request(
            "PUT",
            self._org(org),
            "/adminGroupId",
            data=admin_group_id.encode("utf-8"),
            content_type="text/plain",
            api_version="alpha",
        )

    def update_admins_can_create_orgs_in_subtree(self, value: bool, org: str | None = None) -> None:
        """Allow or forbid admins of an organization to create sub-organizations (alpha).

        Args:
            value: Whether admins can create organizations in the subtree of the organization.
            org: The organization to update. Defaults to the organization of the current project.
        """
        self._put_bool("/adminsCanCreateOrgsInSubtree", value, org)

    def update_admins_can_create_projects_in_subtree(self, value: bool, org: str | None = None) -> None:
        """Allow or forbid admins of an organization to create projects (alpha).

        Args:
            value: Whether admins can create projects in the subtree of the organization.
            org: The organization to update. Defaults to the organization of the current project.
        """
        self._put_bool("/adminsCanCreateProjectsInSubtree", value, org)

    def _put_bool(self, suffix: str, value: bool, org: str | None) -> None:
        self._request("PUT", self._org(org), suffix, data=b"true" if value else b"false", api_version="alpha")

    def update_allowed_clusters(self, clusters: builtins.list[str], org: str | None = None) -> builtins.list[str]:
        """Update the clusters on which admins of the organization can create projects (alpha).

        Args:
            clusters: The allowed cluster names.
            org: The organization to update. Defaults to the organization of the current project.

        Returns:
            The allowed clusters after the update.
        """
        response = self._request(
            "PUT", self._org(org), "/allowedClusters", body={"clusters": clusters}, api_version="alpha"
        )
        return TypeAdapter(builtins.list[str]).validate_python(response.body_json["clusters"])

    def update_idp(self, idp: IdentityProvider, org: str) -> None:
        """Update the IdP configuration of an organization (alpha).

        All authenticated principals of the organization are logged out immediately. The organization
        must be given explicitly, as updating the IdP of your own organization will typically lock you out.

        Args:
            idp: The new IdP configuration.
            org: The organization to update.
        """
        self._request("PUT", org, "/idp", body=idp.dump(), api_version="alpha")


class OrganizationsAPI(_OrgScopedAPI, CDFResourceAPI[OrgResponse]):
    """Organizations (GA, `/api/v1/orgs/...`).

    Unlike most CDF APIs, the Organizations API is not project-scoped. The organization arguments default to
    the organization of the current project, except for delete, which requires an explicit organization.
    """

    def __init__(self, http_client: HTTPClient, project_api: ProjectAPI) -> None:
        super().__init__(
            http_client,
            method_endpoint_map={
                "retrieve": Endpoint(method="GET", path="/orgs/{org}", item_limit=1),
                "delete": Endpoint(method="POST", path="/orgs/{org}/delete", item_limit=1),
                "create": Endpoint(method="POST", path="/orgs/{org}/orgs", item_limit=1),
                "list": Endpoint(method="GET", path="/orgs/{org}/orgs", item_limit=1000),
            },
        )
        self._project_api = project_api
        self.projects = OrganizationProjectsAPI(http_client, project_api)
        self.admin = OrganizationAdminAPI(http_client, project_api)

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[OrgResponse]:
        return PagedResponse[OrgResponse].model_validate_json(response.body)

    def retrieve(self, org: str | None = None) -> OrgWithContactPersonsResponse:
        """Retrieve an organization.

        Args:
            org: The organization to retrieve. Defaults to the organization of the current project.
        """
        endpoint = self._method_endpoint_map["retrieve"]
        request = RequestMessage(
            endpoint_url=self._make_url(endpoint.path.format(org=self._org(org))), method=endpoint.method
        )
        return OrgWithContactPersonsResponse.model_validate_json(self._send(request).body)

    def delete(self, org: str) -> None:
        """Delete an organization.

        Args:
            org: The organization to delete.
        """
        endpoint = self._method_endpoint_map["delete"]
        request = RequestMessage(
            endpoint_url=self._make_url(endpoint.path.format(org=org)), method=endpoint.method, body_content={}
        )
        self._send(request)

    def list_children(self, org: str | None = None, limit: int | None = None) -> list[OrgResponse]:
        """List the child organizations of an organization.

        Args:
            org: The parent organization. Defaults to the organization of the current project.
            limit: The maximum number of organizations to return. None returns all.
        """
        path = self._method_endpoint_map["list"].path.format(org=self._org(org))
        return self._collect_pages(PagedResponse[OrgResponse], path, "GET", limit=limit, page_size=None)

    def create_child(self, item: OrgRequest, org: str | None = None) -> OrgResponse:
        """Create a child organization.

        Args:
            item: The organization to create.
            org: The parent organization. Defaults to the organization of the current project.
        """
        path = self._method_endpoint_map["create"].path.format(org=self._org(org))
        return self._request_item_response([item], "create", endpoint=path)[0]
