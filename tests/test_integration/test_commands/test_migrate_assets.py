from collections.abc import Iterator
from pathlib import Path

import pytest
from cognite.client.data_classes import AssetList, AssetWrite, AssetWriteList
from cognite.client.data_classes.data_modeling import NodeId, Space
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteAsset
from cognite.client.exceptions import CogniteAPIError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._migrate.adapter import AssetCentricMigrationIOAdapter, MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricMapper
from cognite_toolkit._cdf_tk.storageio import AssetIO, InstanceIO
from tests.test_integration.constants import RUN_UNIQUE_ID


@pytest.fixture()
def three_assets(toolkit_client: ToolkitClient, toolkit_space: Space) -> Iterator[AssetList]:
    client = toolkit_client
    space = toolkit_space.space
    assets = AssetWriteList([])
    for i in range(3):
        asset = AssetWrite(
            external_id=f"toolkit_asset_test_migration_{i}_{RUN_UNIQUE_ID}",
            name=f"toolkit_asset_test_migration_{i}_{RUN_UNIQUE_ID}",
            parent_external_id=f"toolkit_asset_test_migration_{0}_{RUN_UNIQUE_ID}" if i > 0 else None,
        )
        assets.append(asset)
    output = client.assets.retrieve_multiple(external_ids=assets.as_external_ids(), ignore_unknown_ids=True)
    if output:
        try:
            client.assets.delete(external_id=output.as_external_ids(), ignore_unknown_ids=True)
        except CogniteAPIError:
            client.data_modeling.instances.delete([NodeId(space, ts.external_id) for ts in output])
    created = client.assets.create(assets)

    yield created

    # Cleanup after test
    deleted = client.data_modeling.instances.delete([NodeId(space, ts.external_id) for ts in created])
    if deleted.nodes:
        return
    client.assets.delete(external_id=created.as_external_ids())


@pytest.mark.skip(
    "We will change the migration model and then we cannot have an integration tests deploying it while we are changing it."
)
class TestMigrateAssetsCommand:
    def test_migrate_assets(
        self,
        toolkit_client: ToolkitClient,
        three_assets: AssetList,
        toolkit_space: Space,
        tmp_path: Path,
    ) -> None:
        client = toolkit_client
        space = toolkit_space.space

        input_file = tmp_path / "timeseries_migration.csv"
        with input_file.open("w", encoding="utf-8") as f:
            f.write(
                "id,dataSetId,space,externalId\n"
                + "\n".join(
                    f"{a.id},{a.data_set_id if a.data_set_id else ''},{space},{a.external_id}" for a in three_assets
                )
                + "\n"
            )

        cmd = MigrationCommand(skip_tracking=True, silent=True)
        cmd.migrate(
            selected=MigrationCSVFileSelector(input_file, resource_type="asset"),
            data=AssetCentricMigrationIOAdapter(client, AssetIO(client), InstanceIO(client)),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path / "logs",
            dry_run=False,
            verbose=False,
        )
        node_ids = [NodeId(space, a.external_id) for a in three_assets]
        migrated_assets = client.data_modeling.instances.retrieve_nodes(node_ids, CogniteAsset)
        assert len(migrated_assets) == len(three_assets), "Not all assets were migrated successfully."
