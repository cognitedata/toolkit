from pathlib import Path
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.external_data_source import (
    ExternalDataSourceRequest,
    ExternalDataSourceResponse,
    OneLakeCredentialsRead,
    OneLakeCredentialsWrite,
    OneLakeLocationDescription,
    OneLakeSettingsRead,
    OneLakeSettingsWrite,
)
from cognite_toolkit._cdf_tk.resource_ios import DataSetsIO, ExternalDataSourceIO, ResourceWorker
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.test_unit.approval_client import ApprovalToolkitClient

_YAML = """
externalId: fabric-lakehouse-prod
name: Production lakehouse
settings:
  credentials:
    clientId: azure-client-id
    tenantId: azure-tenant-id
    clientSecret: azure-client-secret
  locationDescription:
    workspaceName: workspace-guid
    containerName: lakehouse-guid
"""


class TestExternalDataSourceIO:
    def test_sensitive_strings(self) -> None:
        item = ExternalDataSourceRequest(
            external_id="fabric-lakehouse-prod",
            settings=OneLakeSettingsWrite(
                credentials=OneLakeCredentialsWrite(
                    client_id="id",
                    tenant_id="tenant",
                    client_secret="secret-value",
                ),
                location_description=OneLakeLocationDescription(
                    workspace_name="workspace",
                    container_name="lakehouse",
                ),
            ),
        )
        loader = ExternalDataSourceIO(MagicMock(), None, None)
        assert list(loader.sensitive_strings(item)) == ["secret-value"]

    def test_dump_resource_preserves_local_client_secret(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        loader = ExternalDataSourceIO.create_loader(toolkit_client_approval.mock_client)
        response = ExternalDataSourceResponse(
            external_id="fabric-lakehouse-prod",
            name="Production lakehouse",
            format="one_lake",
            created_time=1,
            last_updated_time=1,
            settings=OneLakeSettingsRead(
                credentials=OneLakeCredentialsRead(client_id="azure-client-id", tenant_id="azure-tenant-id"),
                location_description=OneLakeLocationDescription(
                    workspace_name="workspace-guid",
                    container_name="lakehouse-guid",
                ),
            ),
        )
        local = {
            "settings": {
                "credentials": {
                    "clientSecret": "azure-client-secret",
                }
            }
        }
        dumped = loader.dump_resource(response, local)
        assert dumped["settings"]["credentials"]["clientSecret"] == "azure-client-secret"

    def test_prepare_resources_create(
        self, toolkit_client_approval: ApprovalToolkitClient, env_vars_with_client: EnvironmentVariables
    ) -> None:
        local_file = MagicMock(spec=Path)
        local_file.read_text.return_value = _YAML
        loader = ExternalDataSourceIO.create_loader(toolkit_client_approval.mock_client)
        worker = ResourceWorker(loader, "deploy")
        resources = worker.prepare_resources([local_file])
        assert {
            "create": len(resources.to_create),
            "changed": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 1, "changed": 0, "delete": 0, "unchanged": 0}

    def test_get_dependent_items_dataset(self) -> None:
        deps = list(
            ExternalDataSourceIO.get_dependent_items(
                {"externalId": "fabric-lakehouse-prod", "dataSetExternalId": "my_dataset"}
            )
        )
        assert deps == [(DataSetsIO, ExternalId(external_id="my_dataset"))]

    def test_request_dump_includes_format(self) -> None:
        item = ExternalDataSourceRequest(
            external_id="fabric-lakehouse-prod",
            settings=OneLakeSettingsWrite(
                credentials=OneLakeCredentialsWrite(
                    client_id="id",
                    tenant_id="tenant",
                    client_secret="secret",
                ),
                location_description=OneLakeLocationDescription(
                    workspace_name="workspace",
                    container_name="lakehouse",
                ),
            ),
        )
        dumped = item.dump()
        assert dumped["format"] == "one_lake"
