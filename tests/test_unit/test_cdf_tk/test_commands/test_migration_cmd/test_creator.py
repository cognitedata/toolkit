from pathlib import Path

from cognite.client.data_classes import DataSet, DataSetList
from cognite.client.data_classes.data_modeling import DataModel, View

from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.creators import InstanceSpaceCreator
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
