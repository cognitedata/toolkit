from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.externaldata import (
    ExternalDataSourceRequest,
    ExternalDataSourceResponse,
    OneLakeCredentialsRead,
    OneLakeCredentialsWrite,
    OneLakeLocationDescription,
    OneLakeSettingsRead,
    OneLakeSettingsWrite,
)
from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, DataSetScope
from cognite_toolkit._cdf_tk.client.resource_classes.group.acls import TransformationsExternalDataSourcesAcl
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.resource_ios import DataSetsIO, ExternalDataSourceIO, ResourceWorker
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.yaml_classes import ExternalDataSourceYAML
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
    workspaceId: workspace-guid
    containerId: lakehouse-guid
"""


def _make_request(**kwargs: object) -> ExternalDataSourceRequest:
    defaults = {
        "external_id": "fabric-lakehouse-prod",
        "settings": OneLakeSettingsWrite(
            credentials=OneLakeCredentialsWrite(
                client_id="id",
                tenant_id="tenant",
                client_secret="secret",
            ),
            location_description=OneLakeLocationDescription(
                workspace_id="workspace-guid",
                container_id="lakehouse-guid",
            ),
        ),
    }
    defaults.update(kwargs)
    return ExternalDataSourceRequest(**defaults)  # type: ignore[arg-type]


def _make_response(**kwargs: object) -> ExternalDataSourceResponse:
    defaults = {
        "external_id": "fabric-lakehouse-prod",
        "format": "one_lake",
        "created_time": 1,
        "last_updated_time": 1,
        "settings": OneLakeSettingsRead(
            credentials=OneLakeCredentialsRead(client_id="azure-client-id", tenant_id="azure-tenant-id"),
            location_description=OneLakeLocationDescription(
                workspace_id="workspace-guid",
                container_id="lakehouse-guid",
            ),
        ),
    }
    defaults.update(kwargs)
    return ExternalDataSourceResponse(**defaults)  # type: ignore[arg-type]


class TestExternalDataSourceIO:
    def test_sensitive_strings(self) -> None:
        item = _make_request()
        loader = ExternalDataSourceIO(MagicMock(), None, None)
        assert list(loader.sensitive_strings(item)) == ["secret"]

    def test_dump_resource_without_local_omits_client_secret(
        self, toolkit_client_approval: ApprovalToolkitClient
    ) -> None:
        loader = ExternalDataSourceIO.create_loader(toolkit_client_approval.mock_client)
        dumped = loader.dump_resource(_make_response())
        credentials = dumped.get("settings", {}).get("credentials", {})
        assert "clientSecret" not in credentials

    def test_dump_resource_with_local_returns_identifier(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        loader = ExternalDataSourceIO.create_loader(toolkit_client_approval.mock_client)
        local = {
            "externalId": "fabric-lakehouse-prod",
            "settings": {
                "credentials": {
                    "clientId": "azure-client-id",
                    "tenantId": "azure-tenant-id",
                    "clientSecret": "azure-client-secret",
                }
            },
        }
        dumped = loader.dump_resource(_make_response(), local)
        assert dumped == {"externalId": "fabric-lakehouse-prod"}

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

    def test_prepare_resources_existing_recreates(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        toolkit_client_approval.append(ExternalDataSourceResponse, _make_response())
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
        } == {"create": 1, "changed": 0, "delete": 1, "unchanged": 0}

    def test_get_dependent_items_dataset(self) -> None:
        deps = list(
            ExternalDataSourceIO.get_dependent_items(
                {"externalId": "fabric-lakehouse-prod", "dataSetExternalId": "my_dataset"}
            )
        )
        assert deps == [(DataSetsIO, ExternalId(external_id="my_dataset"))]

    def test_get_dependencies_dataset(self) -> None:
        resource = ExternalDataSourceYAML.model_validate(
            {
                "externalId": "fabric-lakehouse-prod",
                "dataSetExternalId": "my_dataset",
                "settings": {
                    "credentials": {"clientId": "id", "tenantId": "tenant", "clientSecret": "secret"},
                    "locationDescription": {"workspaceId": "ws", "containerId": "lh"},
                },
            }
        )
        deps = list(ExternalDataSourceIO.get_dependencies(resource))
        assert deps == [(DataSetsIO, ExternalId(external_id="my_dataset"))]

    def test_request_dump_includes_format(self) -> None:
        dumped = _make_request().dump()
        assert dumped["format"] == "one_lake"

    def test_get_id_from_dict(self) -> None:
        assert ExternalDataSourceIO.get_id({"externalId": "fabric-prod"}) == ExternalId(external_id="fabric-prod")

    def test_get_id_from_dict_snake_case(self) -> None:
        assert ExternalDataSourceIO.get_id({"external_id": "fabric-prod"}) == ExternalId(external_id="fabric-prod")

    def test_get_id_from_dict_missing_raises(self) -> None:
        with pytest.raises(ToolkitRequiredValueError, match="externalId"):
            ExternalDataSourceIO.get_id({})

    def test_get_id_from_request(self) -> None:
        assert ExternalDataSourceIO.get_id(_make_request()) == ExternalId(external_id="fabric-lakehouse-prod")

    def test_get_id_from_request_missing_raises(self) -> None:
        with pytest.raises(ToolkitRequiredValueError, match="external_id"):
            ExternalDataSourceIO.get_id(_make_request(external_id=""))

    def test_dump_id(self) -> None:
        assert ExternalDataSourceIO.dump_id(ExternalId(external_id="fabric-prod")) == {"externalId": "fabric-prod"}

    def test_as_str(self) -> None:
        assert ExternalDataSourceIO.as_str(ExternalId(external_id="fabric/prod")) == "fabric_prod"

    def test_get_minimum_scope(self) -> None:
        scope = ExternalDataSourceIO.get_minimum_scope([_make_request(data_set_id=123)])
        assert isinstance(scope, DataSetScope)

    def test_create_acl_all_scope(self) -> None:
        acls = list(ExternalDataSourceIO.create_acl({"READ", "WRITE"}, AllScope()))
        assert len(acls) == 1
        assert isinstance(acls[0], TransformationsExternalDataSourcesAcl)

    def test_load_resource_with_dataset(self) -> None:
        with monkeypatch_toolkit_client() as client:
            loader = ExternalDataSourceIO.create_loader(client)
            client.lookup.data_sets.id.return_value = 42
            loaded = loader.load_resource(
                {
                    "externalId": "fabric-lakehouse-prod",
                    "dataSetExternalId": "my_dataset",
                    "settings": {
                        "credentials": {"clientId": "id", "tenantId": "tenant", "clientSecret": "secret"},
                        "locationDescription": {"workspaceId": "ws", "containerId": "lh"},
                    },
                }
            )
        assert loaded.data_set_id == 42

    def test_create_retrieve_delete(self) -> None:
        with monkeypatch_toolkit_client() as client:
            loader = ExternalDataSourceIO.create_loader(client)
            item = _make_request()
            response = _make_response()
            api = client.tool.transformations.external_data_sources
            api.create.return_value = [response]
            api.list.return_value = [response]

            assert loader.create([item]) == [response]
            assert loader.retrieve([ExternalId(external_id="fabric-lakehouse-prod")]) == [response]
            assert loader.retrieve([]) == []
            assert loader.delete([ExternalId(external_id="fabric-lakehouse-prod")]) == 1
            assert loader.delete([]) == 0

    def test_iterate_all(self) -> None:
        with monkeypatch_toolkit_client() as client:
            loader = ExternalDataSourceIO.create_loader(client)
            response = _make_response()
            client.tool.transformations.external_data_sources.list.return_value = [response]
            assert list(loader._iterate()) == [response]

    def test_iterate_with_space_returns_nothing(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        loader = ExternalDataSourceIO.create_loader(toolkit_client_approval.mock_client)
        assert list(loader._iterate(space="sp")) == []

    def test_iterate_with_parent_ids_returns_nothing(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        loader = ExternalDataSourceIO.create_loader(toolkit_client_approval.mock_client)
        assert list(loader._iterate(parent_ids=[ExternalId(external_id="parent")])) == []

    def test_iterate_filters_by_dataset(self) -> None:
        with monkeypatch_toolkit_client() as client:
            loader = ExternalDataSourceIO.create_loader(client)
            in_dataset = _make_response(external_id="in-dataset", data_set_id=42)
            other = _make_response(external_id="other", data_set_id=99)
            client.lookup.data_sets.id.return_value = 42
            client.tool.transformations.external_data_sources.list.return_value = [in_dataset, other]
            assert list(loader._iterate(data_set_external_id="my_dataset")) == [in_dataset]

    def test_iterate_missing_dataset_returns_empty(self) -> None:
        with monkeypatch_toolkit_client() as client:
            loader = ExternalDataSourceIO.create_loader(client)
            client.lookup.data_sets.id.return_value = None
            assert list(loader._iterate(data_set_external_id="my_dataset")) == []
            client.tool.transformations.external_data_sources.list.assert_not_called()
