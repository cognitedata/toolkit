import os
from collections.abc import Hashable
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from _pytest.monkeypatch import MonkeyPatch
from cognite.client.data_classes import ExtractionPipelineConfig

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawDatabase, RawTable
from cognite_toolkit._cdf_tk.commands import CleanCommand
from cognite_toolkit._cdf_tk.loaders import (
    DataSetsLoader,
    ExtractionPipelineConfigLoader,
    ExtractionPipelineLoader,
    RawDatabaseLoader,
    RawTableLoader,
    ResourceLoader,
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

        loader = ExtractionPipelineConfigLoader.create_loader(toolkit_client_approval.mock_client)
        worker = ResourceWorker(loader)
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
        loader = ExtractionPipelineConfigLoader.create_loader(env_vars_with_client.get_client())
        with patch.object(ExtractionPipelineConfigLoader, "find_files", return_value=[local_file]):
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
                    (DataSetsLoader, "ds_my_dataset"),
                    (RawDatabaseLoader, RawDatabase("my_db")),
                    (RawTableLoader, RawTable("my_db", "my_table")),
                    (RawTableLoader, RawTable("my_db", "my_table2")),
                ],
                id="Extraction pipeline to Table",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceLoader], Hashable]]) -> None:
        actual = ExtractionPipelineLoader.get_dependent_items(item)

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

        loader = ExtractionPipelineConfigLoader.create_loader(env_vars_with_client.get_client())
        res = loader.load_resource_file(filepath=local_file, environment_variables=env_vars_with_client.dump())
        # Assert that env vars are skipped for this loader
        assert res[0]["config"] == "secret: ${INGESTION_CLIENT_SECRET}"
        assert res[1]["name"] == "this-is-not-a-secret"
