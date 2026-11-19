import json
from collections.abc import Callable
from typing import Any

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.resource_classes.organization import (
    AzureAdIdp,
    OrgProjectRequest,
    OrgRequest,
)

AUTH = "https://auth.cognite.com"
_IDP = {"idpVendor": "AZURE_AD", "issuer": "https://login.microsoftonline.com/tenant/v2.0"}
_ORG = {
    "id": "child",
    "parentId": "my-org",
    "idp": _IDP,
    "adminsCanCreateOrgsInSubtree": False,
    "adminsCanCreateProjectsInSubtree": True,
    "allowedClusters": ["westeurope-1"],
}
_CREATED_PROJECT = {"name": "my-project", "clusterName": "westeurope-1", "projectAdminGroupId": "group"}


@pytest.fixture
def client(toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> ToolkitClient:
    # A fresh client, so the cached organization id is resolved against this mock.
    respx_mock.get(toolkit_config.base_api_url).mock(
        return_value=httpx2.Response(
            200,
            json={
                "name": "Project",
                "urlName": toolkit_config.project,
                "organization": "my-org",
                "userProfilesConfiguration": {"enabled": True},
            },
        )
    )
    return ToolkitClient(toolkit_config)


def _request_summary(route: respx.Route) -> dict[str, Any]:
    request = route.calls[0].request
    content = request.content.decode("utf-8")
    return {
        "url": str(request.url),
        "method": request.method,
        "cdf-version": request.headers.get("cdf-version"),
        "content-type": request.headers.get("content-type"),
        "body": (json.loads(content) if request.headers.get("content-type") == "application/json" else content)
        if content
        else None,
    }


@pytest.mark.usefixtures("disable_gzip")
class TestOrganizationsAPI:
    @pytest.mark.parametrize(
        "method, url, response, call, expected",
        [
            pytest.param(
                "GET",
                f"{AUTH}/api/v1/orgs/my-org",
                {**_ORG, "id": "my-org", "parentId": "root", "contactPersons": [{"id": 1, "email": "a@b.com"}]},
                lambda c: c.organizations.retrieve().dump(),
                {
                    "result": {
                        **_ORG,
                        "id": "my-org",
                        "parentId": "root",
                        "isDeleted": False,
                        "profilesEnabled": True,
                        "contactPersons": [{"id": 1, "email": "a@b.com"}],
                    },
                    "request": {
                        "url": f"{AUTH}/api/v1/orgs/my-org",
                        "method": "GET",
                        "cdf-version": "20230101",
                        "content-type": "application/json",
                        "body": None,
                    },
                },
                id="retrieve defaults to current org",
            ),
            pytest.param(
                "POST",
                f"{AUTH}/api/v1/orgs/my-org/orgs",
                {"items": [_ORG]},
                lambda c: (
                    c.organizations.create_child(OrgRequest(id="child", idp=AzureAdIdp(issuer=_IDP["issuer"])))
                    .as_id()
                    .id
                ),
                {
                    "result": "child",
                    "request": {
                        "url": f"{AUTH}/api/v1/orgs/my-org/orgs",
                        "method": "POST",
                        "cdf-version": "20230101",
                        "content-type": "application/json",
                        "body": {
                            "items": [
                                {
                                    "id": "child",
                                    "idp": _IDP,
                                    "adminsCanCreateOrgsInSubtree": False,
                                    "adminsCanCreateProjectsInSubtree": False,
                                }
                            ]
                        },
                    },
                },
                id="create child org",
            ),
            pytest.param(
                "POST",
                f"{AUTH}/api/v1/orgs/other/delete",
                {},
                lambda c: c.organizations.delete("other"),
                {
                    "result": None,
                    "request": {
                        "url": f"{AUTH}/api/v1/orgs/other/delete",
                        "method": "POST",
                        "cdf-version": "20230101",
                        "content-type": "application/json",
                        "body": {},
                    },
                },
                id="delete org",
            ),
            pytest.param(
                "GET",
                f"{AUTH}/api/v1/orgs/my-org/projects",
                {
                    "items": [
                        {
                            "name": "p1",
                            "apiUrl": "https://westeurope-1.cognitedata.com",
                            "clusterName": "westeurope-1",
                            "state": "ACTIVE",
                        }
                    ]
                },
                lambda c: [p.dump() for p in c.organizations.projects.list(include_admin_properties=True)],
                {
                    "result": [
                        {
                            "name": "p1",
                            "apiUrl": "https://westeurope-1.cognitedata.com",
                            "clusterName": "westeurope-1",
                            "state": "ACTIVE",
                        }
                    ],
                    "request": {
                        "url": f"{AUTH}/api/v1/orgs/my-org/projects?includeAdminProperties=true",
                        "method": "GET",
                        "cdf-version": "20230101",
                        "content-type": "application/json",
                        "body": None,
                    },
                },
                id="list projects with admin properties",
            ),
            pytest.param(
                "POST",
                f"{AUTH}/api/v1/orgs/my-org/projects",
                {"items": [_CREATED_PROJECT]},
                lambda c: c.organizations.projects.create(
                    OrgProjectRequest(name="my-project", cluster_name="westeurope-1")
                ).dump(),
                {
                    "result": _CREATED_PROJECT,
                    "request": {
                        "url": f"{AUTH}/api/v1/orgs/my-org/projects",
                        "method": "POST",
                        "cdf-version": "20230101",
                        "content-type": "application/json",
                        "body": {"items": [{"name": "my-project", "clusterName": "westeurope-1"}]},
                    },
                },
                id="create project",
            ),
            pytest.param(
                "GET",
                f"{AUTH}/api/admin/orgs/my-org/orgs",
                [_ORG],
                lambda c: [o.id for o in c.organizations.admin.list_children()],
                {
                    "result": ["child"],
                    "request": {
                        "url": f"{AUTH}/api/admin/orgs/my-org/orgs",
                        "method": "GET",
                        "cdf-version": "beta",
                        "content-type": "application/json",
                        "body": None,
                    },
                },
                id="admin list child orgs (bare array)",
            ),
            pytest.param(
                "POST",
                f"{AUTH}/api/admin/orgs/my-org/projects",
                _CREATED_PROJECT,
                lambda c: c.organizations.admin.projects.create(
                    OrgProjectRequest(name="my-project", cluster_name="westeurope-1")
                ).dump(),
                {
                    "result": _CREATED_PROJECT,
                    "request": {
                        "url": f"{AUTH}/api/admin/orgs/my-org/projects",
                        "method": "POST",
                        "cdf-version": "beta",
                        "content-type": "application/json",
                        "body": {"name": "my-project", "clusterName": "westeurope-1"},
                    },
                },
                id="admin create project (bare body)",
            ),
            pytest.param(
                "DELETE",
                f"{AUTH}/api/admin/orgs/other",
                None,
                lambda c: c.organizations.admin.delete("other"),
                {
                    "result": None,
                    "request": {
                        "url": f"{AUTH}/api/admin/orgs/other",
                        "method": "DELETE",
                        "cdf-version": "beta",
                        "content-type": "application/json",
                        "body": None,
                    },
                },
                id="admin delete org",
            ),
            pytest.param(
                "PUT",
                f"{AUTH}/api/admin/orgs/my-org/adminGroupId",
                None,
                lambda c: c.organizations.admin.update_admin_group_id("new-group"),
                {
                    "result": None,
                    "request": {
                        "url": f"{AUTH}/api/admin/orgs/my-org/adminGroupId",
                        "method": "PUT",
                        "cdf-version": "alpha",
                        "content-type": "text/plain",
                        "body": "new-group",
                    },
                },
                id="admin update admin group id (text/plain)",
            ),
            pytest.param(
                "PUT",
                f"{AUTH}/api/admin/orgs/my-org/adminsCanCreateOrgsInSubtree",
                None,
                lambda c: c.organizations.admin.update_admins_can_create_orgs_in_subtree(True),
                {
                    "result": None,
                    "request": {
                        "url": f"{AUTH}/api/admin/orgs/my-org/adminsCanCreateOrgsInSubtree",
                        "method": "PUT",
                        "cdf-version": "alpha",
                        "content-type": "application/json",
                        "body": True,
                    },
                },
                id="admin update admins can create orgs (bare boolean)",
            ),
            pytest.param(
                "PUT",
                f"{AUTH}/api/admin/orgs/my-org/allowedClusters",
                {"clusters": ["westeurope-1", "az-eastus-1"]},
                lambda c: c.organizations.admin.update_allowed_clusters(["westeurope-1", "az-eastus-1"]),
                {
                    "result": ["westeurope-1", "az-eastus-1"],
                    "request": {
                        "url": f"{AUTH}/api/admin/orgs/my-org/allowedClusters",
                        "method": "PUT",
                        "cdf-version": "alpha",
                        "content-type": "application/json",
                        "body": {"clusters": ["westeurope-1", "az-eastus-1"]},
                    },
                },
                id="admin update allowed clusters",
            ),
            pytest.param(
                "PUT",
                f"{AUTH}/api/admin/orgs/other/idp",
                None,
                lambda c: c.organizations.admin.update_idp(AzureAdIdp(issuer=_IDP["issuer"]), org="other"),
                {
                    "result": None,
                    "request": {
                        "url": f"{AUTH}/api/admin/orgs/other/idp",
                        "method": "PUT",
                        "cdf-version": "alpha",
                        "content-type": "application/json",
                        "body": _IDP,
                    },
                },
                id="admin update idp",
            ),
        ],
    )
    def test_request_and_response(
        self,
        method: str,
        url: str,
        response: Any,
        call: Callable[[ToolkitClient], Any],
        expected: dict[str, Any],
        client: ToolkitClient,
        respx_mock: respx.MockRouter,
    ) -> None:
        mocked = (
            httpx2.Response(200, json=response)
            if response is not None
            else httpx2.Response(204 if method == "DELETE" else 200)
        )
        route = respx_mock.route(method=method, url__startswith=url).mock(return_value=mocked)

        result = call(client)

        assert {"result": result, "request": _request_summary(route)} == expected

    def test_list_children_follows_cursor(self, client: ToolkitClient, respx_mock: respx.MockRouter) -> None:
        respx_mock.get(f"{AUTH}/api/v1/orgs/my-org/orgs").mock(
            side_effect=[
                httpx2.Response(200, json={"items": [_ORG], "nextCursor": "abc"}),
                httpx2.Response(200, json={"items": [{**_ORG, "id": "child-2"}]}),
            ]
        )

        result = client.organizations.list_children()

        assert [org.id for org in result] == ["child", "child-2"]
