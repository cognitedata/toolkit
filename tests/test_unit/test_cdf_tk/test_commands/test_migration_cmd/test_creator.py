from pathlib import Path
from typing import Any

import pytest
from cognite.client.data_classes import DataSet, DataSetList
from cognite.client.data_classes.aggregations import UniqueResult, UniqueResultList
from cognite.client.data_classes.data_modeling import DataModel, NodeApply, SpaceApply, View

from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.creators import InstanceSpaceCreator, SourceSystemCreator
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL, VIEWS
from cognite_toolkit._cdf_tk.data_classes import ResourceDeployResult
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestCreator:
    @pytest.mark.parametrize("dry_run", [pytest.param(True, id="dry_run"), pytest.param(False, id="not_dry_run")])
    def test_create_instance_spaces(
        self, dry_run: bool, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
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
        toolkit_client_approval.append(DataSet, data_sets)

        results = MigrationCommand(silent=True).create(
            client=toolkit_client_approval.client,
            creator=InstanceSpaceCreator(
                toolkit_client_approval.client, data_set_external_ids=[ds.external_id for ds in data_sets]
            ),
            dry_run=dry_run,
            verbose=False,
            output_dir=tmp_path,
        )
        assert "spaces" in results
        result = results["spaces"]
        assert isinstance(result, ResourceDeployResult)
        assert result.created == 3
        configurations = list(tmp_path.rglob("*Space.yaml"))
        assert len(configurations) == 3
        created_spaces = toolkit_client_approval.created_resources["Space"] if not dry_run else []
        assert all(isinstance(space, SpaceApply) for space in created_spaces)
        expected_created = {ds.external_id for ds in data_sets} if not dry_run else set()
        assert {space.space for space in created_spaces} == expected_created

    def test_create_instance_spaces_missing_external_id(
        self, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        toolkit_client_approval.append(DataModel, COGNITE_MIGRATION_MODEL)
        toolkit_client_approval.append(View, VIEWS)
        data_sets = DataSetList(
            [
                DataSet(
                    id=i,
                    name=f"Dataset {i}",
                    description=f"This is dataset {i}",
                )
                for i in range(3)
            ]
        )

        with pytest.raises(
            ToolkitRequiredValueError,
            match="Cannot create instance spaces for datasets with missing external IDs: 0, 1 and 2",
        ):
            MigrationCommand(silent=True).create(
                client=toolkit_client_approval.client,
                creator=InstanceSpaceCreator(toolkit_client_approval.client, datasets=data_sets),
                dry_run=False,
                verbose=False,
                output_dir=tmp_path,
            )

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
        configurations = list(tmp_path.rglob("*Node.yaml"))
        assert len(configurations) == 5
        expected_external_ids = {"aveva", "custom", "sap", "internal", "sharepoint"}
        created_nodes = toolkit_client_approval.created_resources["Node"]
        assert all(isinstance(node, NodeApply) for node in created_nodes)
        assert {node.external_id for node in created_nodes} == expected_external_ids
