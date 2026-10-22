import gzip
import json

import httpx2
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.transformation_externaldata import TransformationExternalDataSourcesAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.externaldata import (
    ExternalDataSourceUsabilityResponse,
    OneLakeCredentialsWrite,
    OneLakeExternalDataSourceRequest,
    OneLakeLocationDescription,
    OneLakeSettingsWrite,
    RotateKeysRequest,
    SnowflakeExternalDataSourceResponse,
)

_EXAMPLE_RESPONSE = {
    "externalId": "fabric-lakehouse-prod",
    "name": "Production lakehouse",
    "format": "one_lake",
    "dataSetId": 123,
    "createdTime": 1,
    "lastUpdatedTime": 2,
    "settings": {
        "credentials": {"clientId": "azure-client-id", "tenantId": "azure-tenant-id"},
        "locationDescription": {"workspaceId": "workspace-guid", "containerId": "lakehouse-guid"},
    },
}

_SNOWFLAKE_RESPONSE = {
    "externalId": "snowflake-prod",
    "name": "Production Snowflake",
    "format": "snowflake",
    "expiryTime": 2_000_000_000_000,
    "createdTime": 1,
    "lastUpdatedTime": 2,
    "settings": {
        "credentials": {
            "accountIdentifier": "myorg-myaccount",
            "userName": "cdf_user",
            "roleName": "CDF_ROLE",
            "publicKey": "public-key",
        },
        "locationDescription": {"warehouseName": "CDF_WH"},
    },
}


def _request_json(request: httpx2.Request) -> dict:
    content = request.content
    if request.headers.get("content-encoding") == "gzip":
        content = gzip.decompress(content)
    return json.loads(content)


def _make_request() -> OneLakeExternalDataSourceRequest:
    return OneLakeExternalDataSourceRequest(
        external_id="fabric-lakehouse-prod",
        name="Production lakehouse",
        data_set_id=123,
        settings=OneLakeSettingsWrite(
            credentials=OneLakeCredentialsWrite(
                client_id="azure-client-id",
                tenant_id="azure-tenant-id",
                client_secret="secret",
            ),
            location_description=OneLakeLocationDescription(
                workspace_id="workspace-guid",
                container_id="lakehouse-guid",
            ),
        ),
    )


class TestTransformationExternalDataSourcesAPI:
    def test_create_list_delete(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        client = HTTPClient(toolkit_config)
        api = TransformationExternalDataSourcesAPI(client)
        request = _make_request()

        create_url = toolkit_config.create_api_url("/transformations/externaldata")
        respx_mock.post(create_url).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [_EXAMPLE_RESPONSE]})
        )
        created = api.create([request])
        assert len(created) == 1
        assert created[0].external_id == "fabric-lakehouse-prod"
        assert respx_mock.calls[-1].request.headers["cdf-version"] == "alpha"

        list_url = toolkit_config.create_api_url("/transformations/externaldata")
        respx_mock.get(list_url).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [_EXAMPLE_RESPONSE]})
        )
        listed = api.list(limit=10)
        assert len(listed) == 1
        batches = list(api.iterate(limit=10))
        assert batches[0][0].external_id == "fabric-lakehouse-prod"

        delete_url = toolkit_config.create_api_url("/transformations/externaldata/delete")
        respx_mock.post(delete_url).mock(return_value=httpx2.Response(status_code=200, json={}))
        api.delete([ExternalId(external_id="fabric-lakehouse-prod")])
        assert respx_mock.calls[-1].request.url.path.endswith("/transformations/externaldata/delete")

    def test_list_filter_retrieve_and_rotate_keys(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        client = HTTPClient(toolkit_config)
        api = TransformationExternalDataSourcesAPI(client)

        list_url = toolkit_config.create_api_url("/transformations/externaldata")
        respx_mock.get(list_url).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [_SNOWFLAKE_RESPONSE]})
        )
        listed = api.list(limit=10, format="snowflake")
        assert isinstance(listed[0], SnowflakeExternalDataSourceResponse)
        assert listed[0].settings.credentials.public_key == "public-key"
        assert respx_mock.calls[-1].request.url.params["format"] == "snowflake"

        retrieve_url = toolkit_config.create_api_url("/transformations/externaldata/byids")
        respx_mock.post(retrieve_url).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [_EXAMPLE_RESPONSE]})
        )
        retrieved = api.retrieve([ExternalId(external_id="fabric-lakehouse-prod")], ignore_unknown_ids=True)
        assert len(retrieved) == 1
        retrieve_body = _request_json(respx_mock.calls[-1].request)
        assert retrieve_body == {
            "items": [{"externalId": "fabric-lakehouse-prod"}],
            "ignoreUnknownIds": True,
        }

        rotate_url = toolkit_config.create_api_url("/transformations/externaldata/rotatekeys")
        respx_mock.post(rotate_url).mock(
            return_value=httpx2.Response(
                status_code=200,
                json={
                    "items": [
                        {
                            "externalId": "snowflake-prod",
                            "publicKey": "new-public-key",
                            "expiryTime": 2_000_000_000_000,
                        }
                    ]
                },
            )
        )
        rotated = api.rotate_keys([RotateKeysRequest(external_id="snowflake-prod", expiry_time=2_000_000_000_000)])
        assert rotated[0].public_key == "new-public-key"
        assert _request_json(respx_mock.calls[-1].request) == {
            "items": [{"externalId": "snowflake-prod", "expiryTime": 2_000_000_000_000}]
        }
        assert respx_mock.calls[-1].request.headers["cdf-version"] == "alpha"

    def test_verify_usability(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        client = HTTPClient(toolkit_config)
        api = TransformationExternalDataSourcesAPI(client)
        usability_url = toolkit_config.create_api_url("/transformations/externaldata/usability")
        respx_mock.post(usability_url).mock(
            return_value=httpx2.Response(
                status_code=200,
                json={"externalId": "fabric-lakehouse-prod", "usableVersion": "00000000-0000-0000-0000-000000000001"},
            )
        )
        usability = api.verify_usability("fabric-lakehouse-prod")
        assert isinstance(usability, ExternalDataSourceUsabilityResponse)
        assert usability.external_id == "fabric-lakehouse-prod"
        assert usability.usable_version == "00000000-0000-0000-0000-000000000001"
        assert respx_mock.calls[-1].request.headers["cdf-version"] == "alpha"
