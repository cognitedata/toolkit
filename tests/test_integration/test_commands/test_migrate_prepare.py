from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import MigrationPrepareCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_model import CONTAINERS, VIEWS
from cognite_toolkit._cdf_tk.commands._migrate.default_mappings import create_default_mappings


class TestMigrateTimeSeriesCommand:
    def test_migration_prepare_command(
        self,
        toolkit_client: ToolkitClient,
    ) -> None:
        cmd = MigrationPrepareCommand(silent=True, skip_tracking=True)
        expected_resources = {
            "spaces": 1,
            "containers": len(CONTAINERS),
            "views": len(VIEWS),
            "data models": 1,
            "resource view mapping": len(create_default_mappings()),
        }

        dry_run_result = cmd.deploy_cognite_migration(toolkit_client, True, verbose=False)
        actual_dry_run = {result.resource_name: result.total_count for result in dry_run_result}
        assert actual_dry_run == expected_resources, "Dry run result does not match expected resources"

        actual_result = cmd.deploy_cognite_migration(
            toolkit_client,
            False,
            verbose=False,
        )
        actual = {result.resource_name: result.total_count for result in actual_result}

        assert actual == expected_resources, "Actual result does not match expected resources"
