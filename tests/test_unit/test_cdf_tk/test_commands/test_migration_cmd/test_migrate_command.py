from collections.abc import Iterator
from pathlib import Path

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig, ToolkitClient
from cognite_toolkit._cdf_tk.commands._migrate.cmd import MigrationCommand
import responses
import pytest
from cognite_toolkit._cdf_tk.storageio import AssetCentricFileSelector, AssetIO, InstanceIO, InstanceSelector
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetMapper

@pytest.fixture()
def rsps() -> Iterator[responses.RequestsMock]:
    with responses.RequestsMock() as rsps:
        yield rsps


class TestMigrationCommand:
    def test_migrate_assets(self, toolkit_config: ToolkitClientConfig, rsps: responses.RequestsMock, tmp_path: Path) -> None:
        config = toolkit_config
        client = ToolkitClient(config, enable_set_pending_ids=True)
        cmd = MigrationCommand(silent=True)
        mapping_file = tmp_path / "mapping.csv"
        mapping_content = """id,space,externalId,ingestionView"""

        rsps.post(
            config.create_api_url("assets/byids"),
            json={"items": [{"id": 1, "name": "Asset 1"}, {"id": 2, "name": "Asset 2"}]},
            status=200,
        )
        cmd.migrate(
            AssetCentricFileSelector(mapping_file),
            MappingWrapper(AssetIO(client), ),
            AssetMapper(client),
            InstanceFileSelector(mapping_file),
            InstanceIO(client),
            log_dir=tmp_path / "logs",
            dry_run=False,
            verbose=True,
        )




