from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest
import respx
from httpx import Response

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.http_client._data_classes import ErrorDetails
from cognite_toolkit._cdf_tk.client.http_client._item_classes import (
    ItemsFailedResponse,
    ItemsResultList,
    ItemsSuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId
from cognite_toolkit._cdf_tk.client.resource_classes.annotation import AnnotationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    InstanceSource,
    NodeOrEdgeRequest,
    NodeRequest,
    SpaceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import ThreeDModelClassicResponse, ThreeDModelDMSRequest
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.migration_io import (
    AnnotationMigrationIO,
    AssetCentricMigrationIO,
    Image360CollectionInstanceIO,
    ThreeDAssetMappingMigrationIO,
)
from cognite_toolkit._cdf_tk.commands._migrate.selectors import MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.dataio import AssetDataIO, DataItem, Page
from cognite_toolkit._cdf_tk.dataio.selectors import ThreeDModelIdSelector


@pytest.fixture(scope="module")
def toolkit_client(toolkit_config: ToolkitClientConfig) -> ToolkitClient:
    return ToolkitClient(config=toolkit_config)


class TestAssetCentricMigrationIOAdapter:
    def test_download(self, toolkit_client: ToolkitClient, respx_mock: respx.MockRouter, tmp_path: Path) -> None:
        client = toolkit_client
        config = toolkit_client.config
        N = 1500
        items = [
            {
                "id": i,
                "externalId": f"asset_{i}",
                "name": f"Asset {i}",
                "createdTime": 0,
                "lastUpdatedTime": 1,
                "rootId": 0,
            }
            for i in range(N)
        ]
        respx.post(
            config.create_api_url("/models/spaces/byids"),
        ).mock(
            return_value=httpx.Response(
                status_code=200,
                json={
                    "items": [
                        SpaceResponse(space="mySpace", created_time=1, last_updated_time=1, is_global=False).dump()
                    ]
                },
            )
        )

        respx_mock.post(config.create_api_url("/assets/byids")).mock(
            side_effect=[
                Response(status_code=200, json={"items": items[: AssetDataIO.CHUNK_SIZE]}),
                Response(status_code=200, json={"items": items[AssetDataIO.CHUNK_SIZE :]}),
            ]
        )

        csv_file = tmp_path / "files.csv"
        csv_file.write_text("id,space,externalId\n" + "\n".join(f"{i},mySpace,asset_{i}" for i in range(N)))
        selector = MigrationCSVFileSelector(datafile=csv_file, kind="Assets")
        adapter = AssetCentricMigrationIO(client)
        downloaded = list(adapter.stream_data(selector))
        assert len(downloaded) == 2
        assert sum(len(chunk) for chunk in downloaded) == N
        unexpected_space = [
            di.item for chunk in downloaded for di in chunk.items if di.item.mapping.instance_id.space != "mySpace"
        ]
        assert not unexpected_space, f"Found items with unexpected space: {unexpected_space}"
        first_item = downloaded[0].items[0].item
        assert first_item.dump() == {
            "mapping": {"id": 0, "instanceId": {"space": "mySpace", "externalId": "asset_0"}, "resourceType": "asset"},
            "resource": {
                "id": 0,
                "externalId": "asset_0",
                "name": "Asset 0",
                "createdTime": 0,
                "lastUpdatedTime": 1,
                "rootId": 0,
            },
        }


class TestAnnotationMigrationIO:
    def test_download_annotations(
        self, toolkit_client: ToolkitClient, respx_mock: respx.MockRouter, tmp_path: Path
    ) -> None:
        client = toolkit_client
        config = toolkit_client.config
        N = 1500
        annotation_items = [
            AnnotationResponse(
                annotation_type="diagrams.AssetLink",
                data={},
                status="approved",
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
            AnnotationResponse(
                annotation_type="images.AssetLink",
                data={},
                status="approved",
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
        respx_mock.post(config.create_api_url("/annotations/byids")).mock(
            side_effect=[
                Response(status_code=200, json={"items": annotation_items[: AssetCentricMigrationIO.CHUNK_SIZE]}),
                Response(status_code=200, json={"items": annotation_items[AssetCentricMigrationIO.CHUNK_SIZE :]}),
            ]
        )

        csv_file = tmp_path / "annotations.csv"
        csv_file.write_text("id,space,externalId\n" + "\n".join(f"{i},mySpace,annotation_{i}" for i in range(N + 1)))
        selector = MigrationCSVFileSelector(datafile=csv_file, kind="Annotations")

        migration_io = AnnotationMigrationIO(client)

        downloaded = list(migration_io.stream_data(selector))

        assert len(downloaded) == 2
        assert sum(len(chunk) for chunk in downloaded) == N
        first_item = downloaded[0].items[0].item
        assert first_item.dump() == {
            "mapping": {
                "id": 0,
                "instanceId": {"space": "mySpace", "externalId": "annotation_0"},
                "resourceType": "annotation",
                "ingestionMapping": "cdf_asset_annotations_mapping",
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
        # Create items with some duplicates: same nodeId and assetId appear twice
        duplicate_count = 50
        first_batch_items = [{"nodeId": i, "assetId": i} for i in range(ThreeDAssetMappingMigrationIO.CHUNK_SIZE)]
        # Second batch has actual duplicates (same nodeId and assetId) plus new ones
        second_batch_items = [{"nodeId": i, "assetId": i} for i in range(duplicate_count)] + [
            {"nodeId": i, "assetId": i} for i in range(ThreeDAssetMappingMigrationIO.CHUNK_SIZE, N)
        ]
        respx_mock.post(
            config.create_api_url(f"{model_endpoint}/list"),
        ).mock(
            side_effect=[
                Response(
                    status_code=200,
                    json={
                        "items": first_batch_items,
                        "nextCursor": "cursor_1",
                    },
                ),
                Response(
                    status_code=200,
                    json={
                        "items": second_batch_items,
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

        # Set up a mock logger to capture logged entries
        mock_logger = MagicMock()
        io.logger = mock_logger

        pages = list(io.stream_data(selector))
        assert len(pages) == 2
        data_items = [di for chunk in pages for di in chunk.items]
        # We should get N unique items (duplicates are skipped)
        assert len(data_items) == N

        # Verify that duplicates were logged as skipped
        mock_logger.log.assert_called_once()
        logged_entries = mock_logger.log.call_args[0][0]
        assert len(logged_entries) == duplicate_count
        for entry in logged_entries:
            assert entry.label == "Skipped"
            assert entry.message == "Duplicate asset mapping found."

        assert io.count(selector) is None, "3D Asset mapping count should be None"

        with HTTPClient(config) as http_client:
            io.upload_items(
                Page(
                    worker_id="main",
                    items=data_items,
                ),
                http_client=http_client,
            )

        assert len(respx_mock.calls) == 4  # 1 model list, 2 mapping list, 1 uploads (since we pass in all at once)

    def test_invalid_methods(self, toolkit_client: ToolkitClient) -> None:
        """Migration IO is not expected to serialize/deserialize."""
        client = toolkit_client
        io = ThreeDAssetMappingMigrationIO(client, object_3D_space="mySpace", cad_node_space="mySpace")

        with pytest.raises(NotImplementedError):
            io.json_to_resource(MagicMock())

        with pytest.raises(NotImplementedError):
            io.data_to_json_chunk(MagicMock())


class TestImage360CollectionInstanceIO:
    @staticmethod
    def _revision_node(model3d: dict | None = None) -> NodeRequest:
        revision_properties: dict = {"status": "Done", "published": True, "type": "Image360"}
        if model3d is not None:
            revision_properties["model3D"] = model3d
        return NodeRequest(
            space="mySpace",
            external_id="collection1_cdm",
            sources=[
                InstanceSource(
                    source=ContainerId(space="cdf_cdm_3d", external_id="Cognite3DRevision"),
                    properties=revision_properties,
                ),
                InstanceSource(
                    source=ContainerId(space="cdf_cdm", external_id="CogniteDescribable"),
                    properties={"name": "My collection"},
                ),
            ],
        )

    def test_creates_model_and_patches_reference_after_successful_upload(self) -> None:
        page = Page[NodeOrEdgeRequest](
            worker_id="main", items=[DataItem(tracking_id="mySpace:collection1", item=self._revision_node())]
        )
        created_model = ThreeDModelClassicResponse(id=42, name="My collection", created_time=0)

        with monkeypatch_toolkit_client() as client:
            client.tool.three_d.models_classic.create.return_value = [created_model]
            io = Image360CollectionInstanceIO(client)
            http_client = MagicMock()
            http_client.config.create_api_url.return_value = "https://example.com/models/instances"
            http_client.request_items_retries.side_effect = [
                ItemsResultList(
                    [ItemsSuccessResponse(ids=["mySpace:collection1"], status_code=200, body="{}", content=b"{}")]
                ),
                ItemsResultList(
                    [ItemsSuccessResponse(ids=["mySpace:collection1"], status_code=200, body="{}", content=b"{}")]
                ),
            ]
            results = io.upload_items(page, http_client)

        assert len(results) == 2
        assert http_client.request_items_retries.call_count == 2
        client.tool.three_d.models_classic.create.assert_called_once_with(
            [ThreeDModelDMSRequest(name="My collection", space="mySpace", type="Image360")]
        )
        patched_node = http_client.request_items_retries.call_args_list[1].kwargs["message"].items[0].item
        model_source = next(s for s in patched_node.sources if s.source.external_id == "Cognite3DRevision")
        assert model_source.properties["model3D"] == {"space": "mySpace", "externalId": "cog_3d_model_42"}

    def test_skips_model_creation_when_initial_upload_fails(self) -> None:
        """If the revision node upsert fails, the 3D model must never be created, avoiding a dangling model."""
        page = Page[NodeOrEdgeRequest](
            worker_id="main", items=[DataItem(tracking_id="mySpace:collection1", item=self._revision_node())]
        )

        with monkeypatch_toolkit_client() as client:
            io = Image360CollectionInstanceIO(client)
            http_client = MagicMock()
            http_client.config.create_api_url.return_value = "https://example.com/models/instances"
            http_client.request_items_retries.return_value = ItemsResultList(
                [
                    ItemsFailedResponse(
                        ids=["mySpace:collection1"],
                        status_code=400,
                        body="{}",
                        error=ErrorDetails(code=400, message="Error"),
                    )
                ]
            )
            results = io.upload_items(page, http_client)

        assert http_client.request_items_retries.call_count == 1
        client.tool.three_d.models_classic.create.assert_not_called()
        assert len(results) == 1

    def test_reuses_existing_model3d_without_creating(self) -> None:
        """When the revision node already has a model3D reference (reused from a previous migration),
        no new model should be created and the node is only uploaded once."""
        existing_model3d = {"space": "mySpace", "externalId": "cog_3d_model_99"}
        page = Page[NodeOrEdgeRequest](
            worker_id="main",
            items=[DataItem(tracking_id="mySpace:collection1", item=self._revision_node(existing_model3d))],
        )

        with monkeypatch_toolkit_client() as client:
            io = Image360CollectionInstanceIO(client)
            http_client = MagicMock()
            http_client.config.create_api_url.return_value = "https://example.com/models/instances"
            http_client.request_items_retries.return_value = ItemsResultList(
                [ItemsSuccessResponse(ids=["mySpace:collection1"], status_code=200, body="{}", content=b"{}")]
            )
            results = io.upload_items(page, http_client)

        assert http_client.request_items_retries.call_count == 1
        client.tool.three_d.models_classic.create.assert_not_called()
        assert len(results) == 1
