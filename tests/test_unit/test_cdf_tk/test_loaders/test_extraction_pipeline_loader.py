from collections.abc import Hashable
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from _pytest.monkeypatch import MonkeyPatch
from cognite.client.data_classes import ExtractionPipelineConfig

from cognite_toolkit._cdf_tk.commands import CleanCommand, DeployCommand
from cognite_toolkit._cdf_tk.loaders import (
    DataSetsLoader,
    ExtractionPipelineConfigLoader,
    ExtractionPipelineLoader,
    RawDatabaseLoader,
    RawTableLoader,
    ResourceLoader,
)
from cognite_toolkit._cdf_tk.loaders.data_classes import RawDatabaseTable
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
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

    def test_load_extraction_pipeline_upsert_create_one(
        self, toolkit_client_approval: ApprovalToolkitClient, monkeypatch: MonkeyPatch
    ):
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_authorization.return_value = toolkit_client_approval.mock_client
        cdf_tool.client = toolkit_client_approval.mock_client

        toolkit_client_approval.append(
            ExtractionPipelineConfig,
            ExtractionPipelineConfig(
                external_id="ep_src_asset",
                description="DB extractor config reading data from Springfield SAP",
            ),
        )

    def test_load_extraction_pipeline_upsert_update_one(
        self, toolkit_client_approval: ApprovalToolkitClient, monkeypatch: MonkeyPatch
    ) -> None:
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_authorization.return_value = toolkit_client_approval.mock_client
        cdf_tool.client = toolkit_client_approval.mock_client
        cdf_tool.toolkit_client = toolkit_client_approval.mock_client

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

        cmd = DeployCommand(print_warning=False)
        loader = ExtractionPipelineConfigLoader.create_loader(cdf_tool, None)
        resources = loader.load_resource(local_file, cdf_tool, skip_validation=False)
        to_create, changed, unchanged = cmd.to_create_changed_unchanged_triple([resources], loader)
        assert len(to_create) == 0
        assert len(changed) == 1
        assert len(unchanged) == 0

    def test_load_extraction_pipeline_delete_one(
        self, toolkit_client_approval: ApprovalToolkitClient, monkeypatch: MonkeyPatch
    ) -> None:
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_authorization.return_value = toolkit_client_approval.mock_client
        cdf_tool.client = toolkit_client_approval.mock_client
        cdf_tool.toolkit_client = toolkit_client_approval.mock_client

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

        cmd = CleanCommand(print_warning=False)
        loader = ExtractionPipelineConfigLoader.create_loader(cdf_tool, None)
        with patch.object(ExtractionPipelineConfigLoader, "find_files", return_value=[local_file]):
            res = cmd.clean_resources(loader, cdf_tool, dry_run=True, drop=True)
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
                    (RawDatabaseLoader, RawDatabaseTable("my_db")),
                    (RawTableLoader, RawDatabaseTable("my_db", "my_table")),
                    (RawTableLoader, RawDatabaseTable("my_db", "my_table2")),
                ],
                id="Extraction pipeline to Table",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceLoader], Hashable]]) -> None:
        actual = ExtractionPipelineLoader.get_dependent_items(item)

        assert list(actual) == expected
