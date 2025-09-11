from pathlib import Path

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.commands._migrate.adapter import AssetCentricMigrationIOAdapter, MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricMapper
from cognite_toolkit._cdf_tk.storageio import AssetIO, InstanceIO


class TestMigrationCommand:
    def test_migrate(self, toolkit_client_config: ToolkitClientConfig, tmp_path: Path) -> None:
        config = toolkit_client_config
        csv_file = tmp_path / "migration.csv"
        client = ToolkitClient(config)
        command = MigrationCommand(silent=True)

        io = AssetCentricMigrationIOAdapter(client, AssetIO(client), InstanceIO(client))
        command.migrate(
            selected=MigrationCSVFileSelector(csv_file, resource_type="asset"),
            data=io,
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path / "logs",
            dry_run=False,
            verbose=False,
        )
