import os
from collections.abc import Hashable
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from _pytest.monkeypatch import MonkeyPatch
from cognite.client.data_classes import ExtractionPipelineConfig, ExtractionPipelineConfigWrite
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawDatabase, RawTable
from cognite_toolkit._cdf_tk.commands import CleanCommand
from cognite_toolkit._cdf_tk.cruds import (
    DataSetsCRUD,
    ExtractionPipelineConfigCRUD,
    ExtractionPipelineCRUD,
    RawDatabaseCRUD,
    RawTableCRUD,
    ResourceCRUD,
    ResourceWorker,
)
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestExtractionPipelineDependencies:
    _yaml = """
        externalId: 'ep_src_asset_hamburg_sap'
        name: 'Hamburg SAP'
        dataSetId: 12345
    """

    config_yaml = """
        externalId: 'ep_src_asset'
        description: 'DB extractor config reading data from Springfield SAP'
    """

    def test_load_extraction_pipeline_upsert_update_one(
        self, toolkit_client_approval: ApprovalToolkitClient, monkeypatch: MonkeyPatch
    ) -> None:
        toolkit_client_approval.append(
            ExtractionPipelineConfig,
            ExtractionPipelineConfig(
                external_id="ep_src_asset",
                description="DB extractor config reading data from Springfield SAP",
                config="\n    logger: \n        {level: WARN}",
            ),
        )

        local_file = MagicMock(spec=Path)
        local_file.read_text.return_value = self.config_yaml

        loader = ExtractionPipelineConfigCRUD.create_loader(toolkit_client_approval.mock_client)
        worker = ResourceWorker(loader, "deploy")
        resources = worker.prepare_resources([local_file])
        assert {
            "create": len(resources.to_create),
            "changed": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 0, "changed": 1, "delete": 0, "unchanged": 0}

    def test_load_extraction_pipeline_delete_one(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        env_vars_with_client: EnvironmentVariables,
        monkeypatch: MonkeyPatch,
    ) -> None:
        toolkit_client_approval.append(
            ExtractionPipelineConfig,
            ExtractionPipelineConfig(
                external_id="ep_src_asset",
                description="DB extractor config reading data from Springfield SAP",
                config="\n    logger: \n        {level: WARN}",
            ),
        )

        local_file = MagicMock(spec=Path)
        local_file.read_text.return_value = self.config_yaml
        local_file.stem = "ep_src_asset"

        cmd = CleanCommand(print_warning=False)
        loader = ExtractionPipelineConfigCRUD.create_loader(env_vars_with_client.get_client())
        with patch.object(ExtractionPipelineConfigCRUD, "find_files", return_value=[local_file]):
            res = cmd.clean_resources(loader, env_vars_with_client, [], dry_run=True, drop=True)
            assert res is not None
            assert res.deleted == 1


class TestExtractionPipelineLoader:
    @pytest.mark.parametrize(
        "item, expected",
        [
            pytest.param(
                {
                    "dataSetExternalId": "ds_my_dataset",
                    "rawTables": [
                        {"dbName": "my_db", "tableName": "my_table"},
                        {"dbName": "my_db", "tableName": "my_table2"},
                    ],
                },
                [
                    (DataSetsCRUD, "ds_my_dataset"),
                    (RawDatabaseCRUD, RawDatabase("my_db")),
                    (RawTableCRUD, RawTable("my_db", "my_table")),
                    (RawTableCRUD, RawTable("my_db", "my_table2")),
                ],
                id="Extraction pipeline to Table",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceCRUD], Hashable]]) -> None:
        actual = ExtractionPipelineCRUD.get_dependent_items(item)

        assert list(actual) == expected

    @patch.dict(
        os.environ,
        {
            "INGESTION_CLIENT_ID": "this-is-the-ingestion-client-id",
            "INGESTION_CLIENT_SECRET": "this-is-the-ingestion-client-secret",
            "NON-SECRET": "this-is-not-a-secret",
        },
    )
    def test_omit_environment_variables(
        self, env_vars_with_client: EnvironmentVariables, monkeypatch: MonkeyPatch
    ) -> None:
        local_file = MagicMock(spec=Path)
        local_file.read_text.return_value = """
            - externalId: 'ep_src_asset'
              name: 'Hamburg SAP'
              config: 'secret: ${INGESTION_CLIENT_SECRET}'
            - externalId: 'ep_src_asset_2'
              name: '${NON-SECRET}'
              config: 'secret: ${INGESTION_CLIENT_SECRET}'
        """
        local_file.stem = "ep_src_asset"

        loader = ExtractionPipelineConfigCRUD.create_loader(env_vars_with_client.get_client())
        res = loader.load_resource_file(filepath=local_file, environment_variables=env_vars_with_client.dump())
        # Assert that env vars are skipped for this loader
        assert res[0]["config"] == "secret: ${INGESTION_CLIENT_SECRET}"
        assert res[1]["name"] == "this-is-not-a-secret"


class TestExtractionPipelineConfigCRUD:
    def test_load_resource_no_warning_on_keyvault(self) -> None:
        resource = {
            "externalId": "ep_src_asset",
            "config": """azure-keyvault:
  authentication-method: client-secret
  keyvault-name: CogniteKeyVault
  tenant-id: ${AZ_ENTRA_TENANT_ID}
  client-id: ${AZ_SERVICE_PRINCIPLE_APPLICATION_ID}
  secret: ${AZ_SERVICE_PRINCIPLE_CLIENT_SECRET}
databases:
-   connection-string:  !keyvault value-secret-name
    name: my_db
    type: odbc""",
        }
        console = MagicMock(spec=Console)
        print_mock = MagicMock()
        console.print = print_mock
        crud = ExtractionPipelineConfigCRUD(MagicMock(spec=ToolkitClient), None, console=console)

        loaded = crud.load_resource(resource)

        assert isinstance(loaded, ExtractionPipelineConfigWrite)
        # No warning should be printed
        print_mock.assert_not_called()

    def test_load_resource_invalid_yaml_warning(self) -> None:
        resource = {
            "externalId": "ep_src_asset",
            "config": "invalid-yaml: [unclosed_list",
        }
        console = MagicMock(spec=Console)
        print_mock = MagicMock()
        console.print = print_mock
        crud = ExtractionPipelineConfigCRUD(MagicMock(spec=ToolkitClient), None, console=console)
        loaded = crud.load_resource(resource)

        assert isinstance(loaded, ExtractionPipelineConfigWrite)
        print_mock.assert_called_once()
        args, _ = print_mock.call_args
        _, message = args
        assert "ep_src_asset" in message

    def test_load_resource_yaml_array(self) -> None:
        resource = {
            "externalId": "ep_src_asset",
            "config": "- item1: value1",
        }
        console = MagicMock(spec=Console)
        print_mock = MagicMock()
        console.print = print_mock
        crud = ExtractionPipelineConfigCRUD(MagicMock(spec=ToolkitClient), None, console=console)
        loaded = crud.load_resource(resource)

        assert isinstance(loaded, ExtractionPipelineConfigWrite)
        print_mock.assert_called_once()
        args, _ = print_mock.call_args
        _, message = args
        assert "ep_src_asset" in message
        assert "a valid YAML mapping" in message
