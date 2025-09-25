from pathlib import Path

import responses

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.commands._migrate.adapter import AssetCentricMigrationIOAdapter, MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.storageio import AssetIO, InstanceIO


class TestAssetCentricMigrationIOAdapter:
    def test_download(self, toolkit_config: ToolkitClientConfig, rsps: responses.RequestsMock, tmp_path: Path) -> None:
        config = toolkit_config
        client = ToolkitClient(config=config)
        N = 1500
        items = [{"id": i, "externalId": f"asset_{i}", "space": "mySpace"} for i in range(N)]
        rsps.post(config.create_api_url("/assets/byids"), json={"items": items[: AssetIO.CHUNK_SIZE]})
        rsps.post(config.create_api_url("/assets/byids"), json={"items": items[AssetIO.CHUNK_SIZE :]})

        csv_file = tmp_path / "files.csv"
        csv_file.write_text("id,space,externalId\n" + "\n".join(f"{i},mySpace,asset_{i}" for i in range(N)))
        selector = MigrationCSVFileSelector(datafile=csv_file, resource_type="asset")
        adapter = AssetCentricMigrationIOAdapter(
            client,
            AssetIO(client),
            InstanceIO(client),
        )
        downloaded = list(adapter.stream_data(selector))
        assert len(downloaded) == 2
        assert sum(len(chunk) for chunk in downloaded) == N
        unexpected_space = [
            item for chunk in downloaded for item in chunk if item.mapping.instance_id.space != "mySpace"
        ]
        assert not unexpected_space, f"Found items with unexpected space: {unexpected_space}"
        first_item = downloaded[0][0]
        assert first_item.dump() == {
            "mapping": {"id": 0, "instanceId": {"space": "mySpace", "externalId": "asset_0"}, "resourceType": "asset"},
            "resource": {"id": 0, "externalId": "asset_0"},
        }
