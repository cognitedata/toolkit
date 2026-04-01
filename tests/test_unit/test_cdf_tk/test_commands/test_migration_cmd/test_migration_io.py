import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx
from httpx import Response

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId
from cognite_toolkit._cdf_tk.client.resource_classes.annotation import AnnotationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceResponse
from cognite_toolkit._cdf_tk.client.resource_classes.records import RecordRequest, RecordSource
from cognite_toolkit._cdf_tk.commands._migrate.migration_io import (
    AnnotationMigrationIO,
    AssetCentricMigrationIO,
    RecordsMigrationIO,
    ThreeDAssetMappingMigrationIO,
)
from cognite_toolkit._cdf_tk.commands._migrate.selectors import MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.storageio import AssetIO, DataItem, Page
from cognite_toolkit._cdf_tk.storageio.selectors import ThreeDModelIdSelector


def _record_request_for_test(space: str, external_id: str) -> RecordRequest:
    return RecordRequest(
        space=space,
        external_id=external_id,
        sources=[
            RecordSource(
                source=ContainerId(space="cspace", external_id="EventContainer"),
                properties={"x": "y"},
            )
        ],
    )


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
                Response(status_code=200, json={"items": items[: AssetIO.CHUNK_SIZE]}),
                Response(status_code=200, json={"items": items[AssetIO.CHUNK_SIZE :]}),
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
        data_items = [di for chunk in pages for di in chunk.items]
        assert len(data_items) == N

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


@pytest.mark.usefixtures("disable_gzip")
class TestRecordsMigrationIO:
    def test_upload_items_posts_to_stream_records(
        self, toolkit_client: ToolkitClient, respx_mock: respx.MockRouter
    ) -> None:
        client = toolkit_client
        config = client.config
        stream_external_id = "unit_test_stream"
        upload_route = respx_mock.post(config.create_api_url(f"/streams/{stream_external_id}/records")).mock(
            return_value=Response(status_code=200, json={})
        )
        io = RecordsMigrationIO(client, stream_external_id, skip_existing=False)
        items = [DataItem(tracking_id="t1", item=_record_request_for_test("sp", "e1"))]
        with HTTPClient(config) as http_client:
            io.upload_items(Page(worker_id="main", items=items), http_client)
        assert upload_route.called

    def test_upload_items_skip_existing_filters_then_uploads_new_only(
        self, toolkit_client: ToolkitClient, respx_mock: respx.MockRouter
    ) -> None:
        client = toolkit_client
        config = client.config
        stream_external_id = "unit_test_stream_skip"
        filter_url = config.create_api_url(f"/streams/{stream_external_id}/records/filter")
        upload_url = config.create_api_url(f"/streams/{stream_external_id}/records")
        respx_mock.post(filter_url).mock(
            return_value=Response(
                status_code=200,
                json={"items": [{"space": "sp", "externalId": "existing"}]},
            )
        )
        upload_route = respx_mock.post(upload_url).mock(return_value=Response(status_code=200, json={}))
        mock_loader = MagicMock()
        mock_loader.last_updated_time_windows.return_value = [None]
        with patch(
            "cognite_toolkit._cdf_tk.commands._migrate.migration_io.StreamCRUD.create_loader",
            return_value=mock_loader,
        ):
            io = RecordsMigrationIO(client, stream_external_id, skip_existing=True)
        io.logger = MagicMock()
        items = [
            DataItem(tracking_id="track_existing", item=_record_request_for_test("sp", "existing")),
            DataItem(tracking_id="track_new", item=_record_request_for_test("sp", "new_one")),
        ]
        with HTTPClient(config) as http_client:
            io.upload_items(Page(worker_id="main", items=items), http_client)
        io.logger.tracker.finalize_item.assert_called_once_with("track_existing", "skipped")
        assert upload_route.called
        upload_body = json.loads(upload_route.calls[0].request.content)
        assert len(upload_body["items"]) == 1
        assert upload_body["items"][0]["externalId"] == "new_one"

    def test_remove_existing_multiple_spaces(
        self, toolkit_client: ToolkitClient, respx_mock: respx.MockRouter
    ) -> None:
        client = toolkit_client
        config = client.config
        stream_external_id = "unit_test_stream_spaces"
        filter_url = config.create_api_url(f"/streams/{stream_external_id}/records/filter")
        upload_url = config.create_api_url(f"/streams/{stream_external_id}/records")
        # Filter is called once per space; return one existing item for "alpha", none for "beta"
        filter_route = respx_mock.post(filter_url).mock(
            side_effect=[
                Response(status_code=200, json={"items": [{"space": "alpha", "externalId": "alpha_1"}]}),
                Response(status_code=200, json={"items": []}),
            ]
        )
        upload_route = respx_mock.post(upload_url).mock(return_value=Response(status_code=200, json={}))
        mock_loader = MagicMock()
        mock_loader.last_updated_time_windows.return_value = [None]
        with patch(
            "cognite_toolkit._cdf_tk.commands._migrate.migration_io.StreamCRUD.create_loader",
            return_value=mock_loader,
        ):
            io = RecordsMigrationIO(client, stream_external_id, skip_existing=True)
        io.logger = MagicMock()
        items = [
            DataItem(tracking_id="t_alpha1", item=_record_request_for_test("alpha", "alpha_1")),
            DataItem(tracking_id="t_alpha2", item=_record_request_for_test("alpha", "alpha_2")),
            DataItem(tracking_id="t_beta1", item=_record_request_for_test("beta", "beta_1")),
        ]
        with HTTPClient(config) as http_client:
            io.upload_items(Page(worker_id="main", items=items), http_client)
        assert filter_route.call_count == 2
        io.logger.tracker.finalize_item.assert_called_once_with("t_alpha1", "skipped")
        upload_body = json.loads(upload_route.calls[0].request.content)
        uploaded_ids = {item["externalId"] for item in upload_body["items"]}
        assert uploaded_ids == {"alpha_2", "beta_1"}

    def test_remove_existing_batches_over_filter_limit(
        self, toolkit_client: ToolkitClient, respx_mock: respx.MockRouter
    ) -> None:
        client = toolkit_client
        config = client.config
        stream_external_id = "unit_test_stream_batch"
        filter_url = config.create_api_url(f"/streams/{stream_external_id}/records/filter")
        upload_url = config.create_api_url(f"/streams/{stream_external_id}/records")
        filter_route = respx_mock.post(filter_url).mock(
            return_value=Response(status_code=200, json={"items": []})
        )
        respx_mock.post(upload_url).mock(return_value=Response(status_code=200, json={}))
        mock_loader = MagicMock()
        mock_loader.last_updated_time_windows.return_value = [None]
        with patch(
            "cognite_toolkit._cdf_tk.commands._migrate.migration_io.StreamCRUD.create_loader",
            return_value=mock_loader,
        ):
            io = RecordsMigrationIO(client, stream_external_id, skip_existing=True)
        io.logger = MagicMock()
        # 150 items > FILTER_IN_MAX_VALUES (100) → requires 2 filter batches
        items = [
            DataItem(tracking_id=f"t{i}", item=_record_request_for_test("sp", f"evt_{i}"))
            for i in range(150)
        ]
        with HTTPClient(config) as http_client:
            io.upload_items(Page(worker_id="main", items=items), http_client)
        assert filter_route.call_count == 2

    def test_remove_existing_empty_chunk(self, toolkit_client: ToolkitClient, respx_mock: respx.MockRouter) -> None:
        client = toolkit_client
        config = client.config
        stream_external_id = "unit_test_stream_empty"
        filter_route = respx_mock.post(
            config.create_api_url(f"/streams/{stream_external_id}/records/filter")
        ).mock(return_value=Response(status_code=200, json={}))
        mock_loader = MagicMock()
        mock_loader.last_updated_time_windows.return_value = [None]
        with patch(
            "cognite_toolkit._cdf_tk.commands._migrate.migration_io.StreamCRUD.create_loader",
            return_value=mock_loader,
        ):
            io = RecordsMigrationIO(client, stream_external_id, skip_existing=True)
        with HTTPClient(config) as http_client:
            io.upload_items(Page(worker_id="main", items=[]), http_client)
        assert not filter_route.called
