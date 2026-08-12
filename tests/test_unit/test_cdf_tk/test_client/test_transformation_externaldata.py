import httpx
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.transformation_externaldata import (
    ExternalDataSourceUsability,
    TransformationExternalDataSourcesAPI,
)
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.externaldata import (
    ExternalDataSourceRequest,
    OneLakeCredentialsWrite,
    OneLakeLocationDescription,
    OneLakeSettingsWrite,
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


def _make_request() -> ExternalDataSourceRequest:
    return ExternalDataSourceRequest(
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
    def test_upsert_create_update_list_delete(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        client = HTTPClient(toolkit_config)
        api = TransformationExternalDataSourcesAPI(client)
        request = _make_request()

        upsert_url = toolkit_config.create_api_url("/transformations/externaldata")
        respx_mock.post(upsert_url).mock(
            return_value=httpx.Response(status_code=200, json={"items": [_EXAMPLE_RESPONSE]})
        )
        created = api.upsert([request])
        assert len(created) == 1
        assert created[0].external_id == "fabric-lakehouse-prod"

        updated = api.create([request])
        assert len(updated) == 1
        assert api.update([request]) == updated

        list_url = toolkit_config.create_api_url("/transformations/externaldata")
        respx_mock.get(list_url).mock(
            return_value=httpx.Response(status_code=200, json={"items": [_EXAMPLE_RESPONSE]})
        )
        listed = api.list(limit=10)
        assert len(listed) == 1
        batches = list(api.iterate(limit=10))
        assert batches[0][0].external_id == "fabric-lakehouse-prod"

        delete_url = toolkit_config.create_api_url("/transformations/externaldata/delete")
        respx_mock.post(delete_url).mock(return_value=httpx.Response(status_code=200, json={}))
        api.delete([ExternalId(external_id="fabric-lakehouse-prod")])
        assert respx_mock.calls[-1].request.url.path.endswith("/transformations/externaldata/delete")

    def test_verify_usability(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        client = HTTPClient(toolkit_config)
        api = TransformationExternalDataSourcesAPI(client)
        usability_url = toolkit_config.create_api_url("/transformations/externaldata/usability")
        respx_mock.post(usability_url).mock(
            return_value=httpx.Response(
                status_code=200,
                json={"externalId": "fabric-lakehouse-prod", "usableVersion": "00000000-0000-0000-0000-000000000001"},
            )
        )
        usability = api.verify_usability("fabric-lakehouse-prod")
        assert isinstance(usability, ExternalDataSourceUsability)
        assert usability.external_id == "fabric-lakehouse-prod"
        assert usability.usable_version == "00000000-0000-0000-0000-000000000001"
