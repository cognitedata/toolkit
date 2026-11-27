import json
from unittest.mock import MagicMock

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.applications import ApplicationsAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import ApplicationId
from cognite_toolkit._cdf_tk.client.resource_classes.application import ApplicationRequest

_ORG = "my-org"
_CLIENT_ID = "f4a3e291-7682-4148-89c0-be3a8f17eb23"
_APPLICATION = {
    "id": _CLIENT_ID,
    "name": "My Data Portal",
    "allowedOrigins": ["https://*.example.com"],
    "projectNames": ["my-project"],
    "createdTime": 1730204346000,
    "lastUpdatedTime": 1730204346000,
}


@pytest.fixture
def api(toolkit_config: ToolkitClientConfig) -> ApplicationsAPI:
    project_api = MagicMock()
    project_api.get_organization_id.return_value = _ORG
    return ApplicationsAPI(HTTPClient(toolkit_config), project_api)


class TestApplicationsAPI:
    @pytest.mark.usefixtures("disable_gzip")
    def test_create_update_list_and_delete(
        self, api: ApplicationsAPI, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        base_url = toolkit_config.create_auth_url(f"/orgs/{_ORG}/applications")
        respx_mock.post(base_url).mock(return_value=httpx2.Response(status_code=201, json={"items": [_APPLICATION]}))
        respx_mock.post(f"{base_url}/update").mock(
            return_value=httpx2.Response(status_code=200, json={"items": [_APPLICATION]})
        )
        respx_mock.get(base_url).mock(return_value=httpx2.Response(status_code=200, json={"items": [_APPLICATION]}))
        respx_mock.post(f"{base_url}/delete").mock(return_value=httpx2.Response(status_code=200, json={}))

        request = ApplicationRequest(name="My Data Portal", project_names=["my-project"])
        created = api.create([request])
        updated = api.update([created[0].as_request_resource()])
        patched = api.update([ApplicationRequest(id=_CLIENT_ID, name="Renamed", project_names=[])], mode="patch")
        replaced = api.update([ApplicationRequest(id=_CLIENT_ID, name="Renamed", project_names=[])])
        listed = api.list(limit=1)
        api.delete([ApplicationId(id=_CLIENT_ID)])

        assert {
            "urls": [str(call.request.url) for call in respx_mock.calls],
            "versions": {call.request.headers["cdf-version"] for call in respx_mock.calls},
            "created": [item.dump() for item in created],
            "create_body": json.loads(respx_mock.calls[0].request.content),
            "updated": [item.id for item in updated + patched + replaced],
            "update_bodies": [json.loads(call.request.content) for call in respx_mock.calls[1:4]],
            "listed": [item.id for item in listed],
            "delete_body": json.loads(respx_mock.calls[5].request.content),
        } == {
            "urls": [
                base_url,
                f"{base_url}/update",
                f"{base_url}/update",
                f"{base_url}/update",
                f"{base_url}?limit=1",
                f"{base_url}/delete",
            ],
            "versions": {"alpha"},
            "created": [_APPLICATION],
            "create_body": {"items": [{"name": "My Data Portal", "projectNames": ["my-project"]}]},
            "updated": [_CLIENT_ID] * 3,
            "update_bodies": [
                {
                    "items": [
                        {
                            "id": _CLIENT_ID,
                            "update": {
                                "name": {"set": "My Data Portal"},
                                "description": {"setNull": True},
                                "projectNames": {"set": ["my-project"]},
                                "allowedOrigins": {"set": ["https://*.example.com"]},
                            },
                        }
                    ]
                },
                {"items": [{"id": _CLIENT_ID, "update": {"name": {"set": "Renamed"}, "projectNames": {"set": []}}}]},
                {
                    "items": [
                        {
                            "id": _CLIENT_ID,
                            "update": {
                                "name": {"set": "Renamed"},
                                "description": {"setNull": True},
                                "projectNames": {"set": []},
                                "allowedOrigins": {"set": []},
                            },
                        }
                    ]
                },
            ],
            "listed": [_CLIENT_ID],
            "delete_body": {"items": [{"id": _CLIENT_ID}]},
        }

    def test_retrieve_ignore_unknown_ids(
        self, api: ApplicationsAPI, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        base_url = toolkit_config.create_auth_url(f"/orgs/{_ORG}/applications")
        respx_mock.get(f"{base_url}/{_CLIENT_ID}").mock(
            return_value=httpx2.Response(status_code=200, json=_APPLICATION)
        )
        respx_mock.get(f"{base_url}/missing").mock(
            return_value=httpx2.Response(status_code=404, json={"error": {"code": 404, "message": "Not found"}})
        )

        retrieved = api.retrieve([ApplicationId(id=_CLIENT_ID), ApplicationId(id="missing")], ignore_unknown_ids=True)

        assert [item.dump() for item in retrieved] == [_APPLICATION]
