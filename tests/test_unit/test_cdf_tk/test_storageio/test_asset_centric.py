import json
from collections.abc import Iterable
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest
import respx
from cognite.client.data_classes import (
    LabelDefinition,
    LabelDefinitionList,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.cdf_client import PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import DownloadCommand, UploadCommand
from cognite_toolkit._cdf_tk.dataio import (
    AssetCentricIO,
    AssetDataIO,
    EventDataIO,
    FileMetadataDataIO,
    TimeSeriesDataIO,
)
from cognite_toolkit._cdf_tk.dataio._base import Page, TableUploadableDataIO
from cognite_toolkit._cdf_tk.dataio.selectors import AssetCentricSelector, AssetSubtreeSelector, DataSetSelector
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.fileio import FileReader

RESOURCE_COUNT = 50
DATA_SET_ID = 1234
DATA_SET_EXTERNAL_ID = "test_data_set"
ASSET_ID = 123
ASSET_EXTERNAL_ID = "test_hierarchy"
CHUNK_SIZE = 10


@pytest.fixture(scope="module")
def some_asset_data() -> list[AssetResponse]:
    """Fixture to provide a sample assets for testing."""
    return [
        AssetResponse(
            id=1000 + i,
            externalId=f"asset_{i}",
            name=f"Asset {i}",
            description=f"Description for asset {i}",
            rootId=ASSET_ID,
            source="test_source",
            labels=[{"externalId": "my_label"}],
            dataSetId=DATA_SET_ID,
            createdTime=1,
            lastUpdatedTime=1,
            **({"metadata": {"key": f"value_{i}"}} if i % 2 == 0 else {}),
        )
        for i in range(RESOURCE_COUNT)
    ]


@pytest.fixture(scope="module")
def some_filemetadata_data() -> list[FileMetadataResponse]:
    """Fixture to provide a sample FileMetadataList for testing."""
    return [
        FileMetadataResponse(
            id=1000 + i,
            external_id=f"file_{i}",
            name=f"File {i}",
            directory="/test/dir",
            mime_type="text/plain",
            data_set_id=DATA_SET_ID,
            asset_ids=[ASSET_ID],
            source="test_source",
            created_time=1,
            last_updated_time=1,
            uploaded=True,
            **({"metadata": {"key": f"value_{i}"}} if i % 2 == 0 else {}),
        )
        for i in range(RESOURCE_COUNT)
    ]


@pytest.fixture(scope="module")
def some_timeseries_data() -> list[TimeSeriesResponse]:
    """Fixture to provide a sample list of TimeSeriesResponse for testing."""
    return [
        TimeSeriesResponse(
            id=2000 + i,
            externalId=f"ts_{i}",
            name=f"Time Series {i}",
            description=f"Description for time series {i}",
            assetId=ASSET_ID,
            dataSetId=DATA_SET_ID,
            unit="unit",
            isString=False,
            isStep=False,
            type="numeric",
            createdTime=1,
            lastUpdatedTime=1,
            **({"metadata": {"key": f"value_{i}"}} if i % 2 == 0 else {}),
        )
        for i in range(RESOURCE_COUNT)
    ]


@pytest.fixture(scope="module")
def some_event_data() -> list[EventResponse]:
    """Fixture to provide a sample list of EventResponse for testing."""
    return [
        EventResponse(
            id=3000 + i,
            externalId=f"event_{i}",
            description=f"Description for event {i}",
            assetIds=[ASSET_ID],
            dataSetId=DATA_SET_ID,
            source="test_source",
            startTime=1000000000000 + i * 1000,
            endTime=1000000001000 + i * 1000,
            createdTime=1,
            lastUpdatedTime=1,
            **({"metadata": {"key": f"value_{i}"}} if i % 2 == 0 else {}),
        )
        for i in range(RESOURCE_COUNT)
    ]


@pytest.fixture()
def asset_centric_client(
    toolkit_config: ToolkitClientConfig,
    some_asset_data: list[AssetResponse],
    some_filemetadata_data: list[FileMetadataResponse],
    some_timeseries_data: list[TimeSeriesResponse],
    some_event_data: list[EventResponse],
) -> Iterable[ToolkitClient]:
    with monkeypatch_toolkit_client() as client:
        asset_chunks = list(chunker(some_asset_data, CHUNK_SIZE))
        asset_chunks.reverse()

        def iterate_assets(*_, **__) -> PagedResponse[AssetResponse]:
            nonlocal asset_chunks
            asset_items = asset_chunks.pop()
            return PagedResponse(items=asset_items, nextCursor="cursor" if asset_chunks else None)

        timeseries_chunks = list(chunker(some_timeseries_data, CHUNK_SIZE))
        timeseries_chunks.reverse()

        def iterate_timeseries(*_, **__) -> PagedResponse[TimeSeriesResponse]:
            nonlocal timeseries_chunks
            timeseries_items = timeseries_chunks.pop()
            return PagedResponse(items=timeseries_items, nextCursor="cursor" if timeseries_chunks else None)

        event_chunks = list(chunker(some_event_data, CHUNK_SIZE))
        event_chunks.reverse()

        def iterate_events(*_, **__) -> PagedResponse[EventResponse]:
            nonlocal event_chunks
            event_items = event_chunks.pop()
            return PagedResponse(items=event_items, nextCursor="cursor" if event_chunks else None)

        file_chunks = list(chunker(some_filemetadata_data, CHUNK_SIZE))
        file_chunks.reverse()

        def iterate_files(*_, **__) -> PagedResponse[FileMetadataResponse]:
            nonlocal file_chunks
            file_items = file_chunks.pop()
            return PagedResponse(items=file_items, nextCursor="cursor" if file_chunks else None)

        client.tool.assets.paginate.side_effect = iterate_assets
        client.tool.timeseries.paginate.side_effect = iterate_timeseries
        client.tool.events.paginate.side_effect = iterate_events
        client.tool.filemetadata.paginate.side_effect = iterate_files

        client.assets.aggregate_count.return_value = RESOURCE_COUNT
        client.files.aggregate_count.return_value = RESOURCE_COUNT
        client.events.aggregate_count.return_value = RESOURCE_COUNT
        client.time_series.aggregate_count.return_value = RESOURCE_COUNT

        client.lookup.data_sets.external_id.return_value = DATA_SET_EXTERNAL_ID
        client.lookup.data_sets.id.return_value = DATA_SET_ID

        def lookup_asset_external_id(ids: int | list[int], *_, **__) -> list[str] | str:
            if isinstance(ids, int):
                return ASSET_EXTERNAL_ID
            return [ASSET_EXTERNAL_ID for _ in ids]

        def lookup_asset_id(external_ids: str | list[str], *_, **__) -> list[int] | int:
            if isinstance(external_ids, str):
                return ASSET_ID
            return [ASSET_ID]

        client.lookup.assets.external_id.side_effect = lookup_asset_external_id
        client.lookup.assets.id.side_effect = lookup_asset_id

        client.tool.token.verify_acls.return_value = []
        client.labels.retrieve.return_value = LabelDefinitionList(
            [LabelDefinition(external_id="my_label", name="my_label", created_time=0)]
        )
        client.config = toolkit_config
        yield client


@pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
class TestAssetCentricIO:
    @pytest.mark.parametrize(
        "io_class,selector,create_endpoint",
        [
            pytest.param(
                AssetDataIO, AssetSubtreeSelector(hierarchy=ASSET_EXTERNAL_ID, kind="Assets"), "/assets", id="AssetIO"
            ),
            pytest.param(
                FileMetadataDataIO,
                DataSetSelector(data_set_external_id=DATA_SET_EXTERNAL_ID, kind="FileMetadata"),
                None,
                id="FileMetadataIO",
            ),
            pytest.param(
                TimeSeriesDataIO,
                DataSetSelector(data_set_external_id=DATA_SET_EXTERNAL_ID, kind="TimeSeries"),
                "/timeseries",
                id="TimeSeriesIO",
            ),
            pytest.param(
                EventDataIO,
                DataSetSelector(data_set_external_id=DATA_SET_EXTERNAL_ID, kind="Events"),
                "/events",
                id="EventIO",
            ),
        ],
    )
    def test_download_upload(
        self,
        io_class: type[AssetCentricIO],
        selector: AssetCentricSelector,
        create_endpoint: str | None,
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
        asset_centric_client: ToolkitClient,
        some_asset_data: list[AssetResponse],
        some_filemetadata_data: list[FileMetadataResponse],
        some_timeseries_data: list[TimeSeriesResponse],
        some_event_data: list[EventResponse],
    ) -> None:
        io = io_class(asset_centric_client)

        assert io.count(selector) == RESOURCE_COUNT

        source = io.stream_data(selector)

        row_chunks: list[Page] = []
        for page in source:
            rows_page = io.data_to_row(page, selector)
            assert len(rows_page) == CHUNK_SIZE
            for data_item in rows_page.items:
                row = data_item.item
                assert isinstance(row, dict)
                assert "dataSetExternalId" in row
                assert row["dataSetExternalId"] == DATA_SET_EXTERNAL_ID
            row_chunks.append(rows_page)

        if create_endpoint is None:
            return  # No upload test for this IO class

        assert isinstance(io, TableUploadableDataIO)
        config = toolkit_config
        resources = {
            AssetDataIO: some_asset_data,
            FileMetadataDataIO: some_filemetadata_data,
            TimeSeriesDataIO: some_timeseries_data,
            EventDataIO: some_event_data,
        }[io_class]

        resource_by_external_id = {
            resource.external_id: resource for resource in resources if resource.external_id is not None
        }

        def create_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert "items" in payload
            items = payload["items"]
            assert isinstance(items, list)
            return httpx.Response(
                status_code=200,
                json={
                    "items": [
                        resource_by_external_id[item["externalId"]].dump() for item in items if "externalId" in item
                    ]
                },
            )

        respx_mock.post(config.create_api_url(create_endpoint)).mock(side_effect=create_callback)

        with HTTPClient(config) as upload_client:
            for rows_page in row_chunks:
                upload_items = io.rows_to_data(rows_page, selector)
                io.upload_items(upload_items, upload_client, selector)

        assert respx_mock.calls.call_count == RESOURCE_COUNT // CHUNK_SIZE
        uploaded_resources = []
        for call in respx_mock.calls:
            uploaded_resources.extend(json.loads(call.request.content)["items"])

        assert uploaded_resources == [resource.as_write().dump() for resource in resources]


class TestAssetIO:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_download_upload_command(
        self,
        some_asset_data: list[AssetResponse],
        tmp_path: Path,
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
        monkeypatch: pytest.MonkeyPatch,
        asset_centric_client: ToolkitClient,
    ) -> None:
        config = toolkit_config
        monkeypatch.setenv("CDF_CLUSTER", config.cdf_cluster)
        monkeypatch.setenv("CDF_PROJECT", config.project)

        def asset_create_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert "items" in payload
            items = payload["items"]
            assert isinstance(items, list)
            assert len(items) == len(some_asset_data)
            assert {item["externalId"] for item in items} == {asset.external_id for asset in some_asset_data}
            return httpx.Response(status_code=200, json={"items": [asset.dump() for asset in some_asset_data]})

        respx_mock.post(config.create_api_url("/assets")).mock(side_effect=asset_create_callback)

        selector = AssetSubtreeSelector(hierarchy="test_hierarchy", kind="Assets", download_dir_name="assets")

        io = AssetDataIO(asset_centric_client)

        download_command = DownloadCommand(silent=True, skip_tracking=True)
        upload_command = UploadCommand(silent=True, skip_tracking=True)

        download_command.download(
            selectors=[selector],
            io=io,
            output_dir=tmp_path,
            verbose=False,
            file_format=".csv",
            compression="none",
            limit=100,
        )

        csv_files = list((tmp_path / selector.download_dir_name).glob("*.csv"))
        assert len(csv_files) == 1

        upload_command.upload(
            input_dir=tmp_path / selector.download_dir_name,
            client=asset_centric_client,
            deploy_resources=True,
            dry_run=False,
            verbose=False,
            kind=io.KIND,
        )

        assert len(respx_mock.calls) == 1

    def test_read_assets_chunks(self) -> None:
        assets = [
            {"id": 1, "depth": 3},
            {"id": 2, "depth": 2},
            {"id": 3, "depth": 1},
            {"id": 4},
            {"id": 5, "depth": "not_an_int"},
        ]
        assets_with_line_numbers = list(enumerate(assets, start=1))
        other_reader = MagicMock(spec=FileReader)
        other_reader.read_chunks_with_line_numbers.return_value = assets_with_line_numbers
        other_reader.input_file = Path("mocked_file.csv")
        output = list(
            AssetDataIO.read_chunks(other_reader, AssetSubtreeSelector(hierarchy="does not matter", kind="Assets"))
        )

        result = [[(di.tracking_id, di.item) for di in page.items] for page in output]
        assert result == [
            [("line 4", {"id": 4}), ("line 5", {"id": 5, "depth": "not_an_int"})],
            [("line 3", {"id": 3, "depth": 1})],
            [("line 2", {"id": 2, "depth": 2})],
            [("line 1", {"id": 1, "depth": 3})],
        ]
