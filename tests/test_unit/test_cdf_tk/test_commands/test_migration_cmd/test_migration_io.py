from pathlib import Path

import responses
from cognite.client.data_classes import Annotation

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.commands._migrate.migration_io import (
    AnnotationMigrationIO,
    AssetCentricMigrationIO,
)
from cognite_toolkit._cdf_tk.commands._migrate.selectors import MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.storageio import AssetIO


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
        selector = MigrationCSVFileSelector(datafile=csv_file, kind="Assets")
        adapter = AssetCentricMigrationIO(client)
        downloaded = list(adapter.stream_data(selector))
        assert len(downloaded) == 2
        assert sum(len(chunk) for chunk in downloaded) == N
        unexpected_space = [
            item for chunk in downloaded for item in chunk.items if item.mapping.instance_id.space != "mySpace"
        ]
        assert not unexpected_space, f"Found items with unexpected space: {unexpected_space}"
        first_item = downloaded[0].items[0]
        assert first_item.dump() == {
            "mapping": {"id": 0, "instanceId": {"space": "mySpace", "externalId": "asset_0"}, "resourceType": "asset"},
            "resource": {"id": 0, "externalId": "asset_0"},
        }


class TestAnnotationMigrationIO:
    def test_download_annotations(
        self, toolkit_config: ToolkitClientConfig, rsps: responses.RequestsMock, tmp_path: Path
    ) -> None:
        config = toolkit_config
        client = ToolkitClient(config=config)
        N = 1500
        annotation_items = [
            Annotation(
                annotation_type="diagrams.AssetLink",
                data={},
                status="accepted",
                creating_user="doctrino",
                creating_app="unit_test",
                creating_app_version="1.0.0",
                annotated_resource_id=123,
                annotated_resource_type="file",
                id=i,
                created_time=1,
                last_updated_time=1,
            ).dump()
            for i in range(N)
        ]
        rsps.post(
            config.create_api_url("/annotations/byids"),
            json={"items": annotation_items[: AssetCentricMigrationIO.CHUNK_SIZE]},
        )
        rsps.post(
            config.create_api_url("/annotations/byids"),
            json={"items": annotation_items[AssetCentricMigrationIO.CHUNK_SIZE :]},
        )

        csv_file = tmp_path / "annotations.csv"
        csv_file.write_text("id,space,externalId\n" + "\n".join(f"{i},mySpace,annotation_{i}" for i in range(N)))
        selector = MigrationCSVFileSelector(datafile=csv_file, kind="Annotations")

        migration_io = AnnotationMigrationIO(client)

        downloaded = list(migration_io.stream_data(selector))

        assert len(downloaded) == 2
        assert sum(len(chunk) for chunk in downloaded) == N
        first_item = downloaded[0].items[0]
        assert first_item.dump() == {
            "mapping": {
                "id": 0,
                "instanceId": {"space": "mySpace", "externalId": "annotation_0"},
                "resourceType": "annotation",
            },
            "resource": annotation_items[0],
        }
