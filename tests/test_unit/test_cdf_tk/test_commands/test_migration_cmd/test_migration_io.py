from pathlib import Path
from unittest.mock import MagicMock

import pytest
import responses
import respx
from cognite.client.data_classes import Annotation
from httpx import Response

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.commands._migrate.migration_io import (
    AnnotationMigrationIO,
    AssetCentricMigrationIO,
    ThreeDAssetMappingMigrationIO,
)
from cognite_toolkit._cdf_tk.commands._migrate.selectors import MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.storageio import AssetIO, UploadItem
from cognite_toolkit._cdf_tk.storageio.selectors import ThreeDModelIdSelector


@pytest.fixture(scope="module")
def toolkit_client(toolkit_config: ToolkitClientConfig) -> ToolkitClient:
    return ToolkitClient(config=toolkit_config)


class TestAssetCentricMigrationIOAdapter:
    def test_download(self, toolkit_client: ToolkitClient, rsps: responses.RequestsMock, tmp_path: Path) -> None:
        client = toolkit_client
        config = toolkit_client.config
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
        self, toolkit_client: ToolkitClient, rsps: responses.RequestsMock, tmp_path: Path
    ) -> None:
        client = toolkit_client
        config = toolkit_client.config
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
        ] + [
            # This should be filtered out
            Annotation(
                annotation_type="images.AssetLink",
                data={},
                status="accepted",
                creating_user="doctrino",
                creating_app="unit_test",
                creating_app_version="1.0.0",
                annotated_resource_id=456,
                annotated_resource_type="file",
                id=N,
                created_time=1,
                last_updated_time=1,
            ).dump()
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
        csv_file.write_text("id,space,externalId\n" + "\n".join(f"{i},mySpace,annotation_{i}" for i in range(N + 1)))
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
                "ingestionView": "cdf_asset_annotations_mapping",
            },
            "resource": annotation_items[0],
        }


class TestThreeDAssetMappingMigrationIO:
    def test_download_3d_asset_mappings(
        self, toolkit_client: ToolkitClient, respx_mock: respx.MockRouter, tmp_path: Path
    ) -> None:
        client = toolkit_client
        config = toolkit_client.config
        N = 150
        model_id = 37
        revision_id = 101
        respx_mock.get(
            config.create_api_url("/3d/models"),
        ).mock(
            return_value=Response(
                status_code=200,
                json={
                    "items": [
                        {
                            "name": "model_37",
                            "id": model_id,
                            "createdTime": 1,
                            "lastRevisionInfo": {"revisionId": revision_id},
                            "space": "mySpace",
                        }
                    ],
                    "nextCursor": None,
                },
            )
        )

        model_endpoint = f"/3d/models/{model_id}/revisions/{revision_id}/mappings"
        items = [{"nodeId": i, "assetId": i} for i in range(N)]
        respx_mock.post(
            config.create_api_url(f"{model_endpoint}/list"),
        ).mock(
            side_effect=[
                Response(
                    status_code=200,
                    json={
                        "items": items[: ThreeDAssetMappingMigrationIO.CHUNK_SIZE],
                        "nextCursor": "cursor_1",
                    },
                ),
                Response(
                    status_code=200,
                    json={
                        "items": items[ThreeDAssetMappingMigrationIO.CHUNK_SIZE :],
                        "nextCursor": None,
                    },
                ),
            ]
        )
        respx_mock.post(
            config.create_api_url(f"{model_endpoint}"),
        ).mock(
            return_value=Response(
                status_code=200,
                json={},
            )
        )

        selector = ThreeDModelIdSelector(ids=(37,))
        io = ThreeDAssetMappingMigrationIO(client, object_3D_space="mySpace", cad_node_space="mySpace")
        pages = list(io.stream_data(selector))
        assert len(pages) == 2
        items = [item for chunk in pages for item in chunk.items]
        assert len(items) == N

        _ = io.as_id(items[0])

        assert io.count(selector) is None, "3D Asset mapping count should be None"

        with HTTPClient(config) as http_client:
            io.upload_items([UploadItem(f"{no:,}", item) for no, item in enumerate(items)], http_client=http_client)

        assert len(respx_mock.calls) == 4  # 1 model list, 2 mapping list, 1 uploads (since we pass in all at once)

    def test_invalid_methods(self, toolkit_client: ToolkitClient) -> None:
        """Migration IO is not expected to serialize/deserialize."""
        client = toolkit_client
        io = ThreeDAssetMappingMigrationIO(client, object_3D_space="mySpace", cad_node_space="mySpace")

        with pytest.raises(NotImplementedError):
            io.json_to_resource(MagicMock())

        with pytest.raises(NotImplementedError):
            io.data_to_json_chunk(MagicMock())
