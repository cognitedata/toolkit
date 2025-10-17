from pathlib import Path
from typing import Any

import pytest
from cognite.client.data_classes import DataSet, DataSetList
from cognite.client.data_classes.aggregations import UniqueResult, UniqueResultList
from cognite.client.data_classes.data_modeling import DataModel, View

from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.creators import InstanceSpaceCreator, SourceSystemCreator
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL, VIEWS
from cognite_toolkit._cdf_tk.data_classes import ResourceDeployResult
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestCreator:
    def test_create_instance_spaces(self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path) -> None:
        toolkit_client_approval.append(DataModel, COGNITE_MIGRATION_MODEL)
        toolkit_client_approval.append(View, VIEWS)

        data_sets = DataSetList(
            [
                DataSet(
                    external_id=f"dataset_{letter}",
                    name=f"Dataset {letter}",
                    description=f"This is dataset {letter}",
                )
                for letter in "ABC"
            ]
        )

        results = MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=InstanceSpaceCreator(toolkit_client_approval.client, data_sets),
            dry_run=False,
            verbose=False,
            output_dir=tmp_path,
        )
        assert "spaces" in results
        result = results["spaces"]
        assert isinstance(result, ResourceDeployResult)
        assert result.created == 3

    @pytest.mark.parametrize(
        "arguments",
        [
            pytest.param({"data_set_external_id": "my_data_set"}, id="with_data_set"),
            pytest.param({"hierarchy": "my_root_asset"}, id="with_hierarchy"),
        ],
    )
    def test_create_source_systems(
        self, arguments: dict[str, Any], toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        toolkit_client_approval.append(DataModel, COGNITE_MIGRATION_MODEL)
        toolkit_client_approval.append(View, VIEWS)
        asset_sources = UniqueResultList([UniqueResult(100, ["aveva"]), UniqueResult(50, ["custom"])])
        event_sources = UniqueResultList([UniqueResult(400, ["sap"]), UniqueResult(200, ["internal"])])
        file_sources = UniqueResultList([UniqueResult(1000, ["sharepoint"])])
        client = toolkit_client_approval.mock_client
        client.assets.aggregate_unique_values.return_value = asset_sources
        client.events.aggregate_unique_values.return_value = event_sources
        client.documents.aggregate_unique_values.return_value = file_sources

        results = MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=SourceSystemCreator(toolkit_client_approval.client, "my_source_space", **arguments),
            dry_run=False,
            verbose=False,
            output_dir=tmp_path,
        )
        assert "nodes" in results
        result = results["nodes"]
        assert isinstance(result, ResourceDeployResult)
        assert result.created == 5
