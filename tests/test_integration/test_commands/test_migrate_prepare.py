from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import MigrationPrepareCommand


class TestMigrateTimeSeriesCommand:
    def test_migration_prepare_command(
        self,
        toolkit_client: ToolkitClient,
    ) -> None:
        cmd = MigrationPrepareCommand(silent=True, skip_tracking=True)

        dry_run_result = cmd.deploy_cognite_migration(toolkit_client, True, verbose=False)

        actual_result = cmd.deploy_cognite_migration(toolkit_client, False, verbose=False)

        assert dry_run_result.has_counts, "Dry run should have counts"
        assert actual_result.has_counts, "Actual should have counts"
