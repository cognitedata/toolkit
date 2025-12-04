import json
from collections.abc import Iterable
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest
import respx
from cognite.client.data_classes import (
    Asset,
    AssetList,
    CountAggregate,
    DataSet,
    DataSetList,
    Event,
    EventList,
    FileMetadata,
    FileMetadataList,
    LabelDefinition,
    LabelDefinitionList,
    TimeSeries,
    TimeSeriesList,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import DownloadCommand, UploadCommand
from cognite_toolkit._cdf_tk.storageio import AssetIO, EventIO, FileMetadataIO, TimeSeriesIO
from cognite_toolkit._cdf_tk.storageio.selectors import AssetSubtreeSelector, DataSetSelector
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.fileio import FileReader
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

RESOURCE_COUNT = 50
DATA_SET_ID = 1234
DATA_SET_EXTERNAL_ID = "test_data_set"
ASSET_ID = 123
ASSET_EXTERNAL_ID = "test_hierarchy"
CHUNK_SIZE = 10


@pytest.fixture(scope="module")
def some_asset_data() -> AssetList:
    """Fixture to provide a sample AssetList for testing."""
    return AssetList(
        [
            Asset(
                id=1000 + i,
                external_id=f"asset_{i}",
                name=f"Asset {i}",
                description=f"Description for asset {i}",
                root_id=ASSET_ID,
                source="test_source",
                labels=["my_label"],
                data_set_id=DATA_SET_ID,
            )
            for i in range(RESOURCE_COUNT)
        ]
    )


@pytest.fixture(scope="module")
def some_filemetadata_data() -> FileMetadataList:
    """Fixture to provide a sample FileMetadataList for testing."""
    return FileMetadataList(
        [
            FileMetadata(
                external_id=f"file_{i}",
                name=f"File {i}",
                directory="/test/dir",
                mime_type="text/plain",
                data_set_id=DATA_SET_ID,
                asset_ids=[ASSET_ID],
                source="test_source",
            )
            for i in range(RESOURCE_COUNT)
        ]
    )


@pytest.fixture(scope="module")
def some_timeseries_data() -> TimeSeriesList:
    """Fixture to provide a sample TimeSeriesList for testing."""
    return TimeSeriesList(
        [
            TimeSeries(
                external_id=f"ts_{i}",
                name=f"Time Series {i}",
                description=f"Description for time series {i}",
                asset_id=ASSET_ID,
                data_set_id=DATA_SET_ID,
                unit="unit",
                is_string=False,
                is_step=False,
            )
            for i in range(RESOURCE_COUNT)
        ]
    )


@pytest.fixture(scope="module")
def some_event_data() -> EventList:
    """Fixture to provide a sample EventList for testing."""
    return EventList(
        [
            Event(
                external_id=f"event_{i}",
                description=f"Description for event {i}",
                asset_ids=[ASSET_ID],
                data_set_id=DATA_SET_ID,
                source="test_source",
                start_time=1000000000000 + i * 1000,
                end_time=1000000001000 + i * 1000,
            )
            for i in range(RESOURCE_COUNT)
        ]
    )


@pytest.fixture()
def asset_centric_client(
    some_asset_data: AssetList,
    some_filemetadata_data: FileMetadataList,
    some_timeseries_data: TimeSeriesList,
    some_event_data: EventList,
) -> Iterable[ToolkitClient]:
    with monkeypatch_toolkit_client() as client:
        client.assets.return_value = chunker(some_asset_data, CHUNK_SIZE)
        client.files.return_value = chunker(some_filemetadata_data, CHUNK_SIZE)
        client.time_series.return_value = chunker(some_timeseries_data, CHUNK_SIZE)
        client.events.return_value = chunker(some_event_data, CHUNK_SIZE)

        client.files.aggregate.return_value = [CountAggregate(RESOURCE_COUNT)]

        client.lookup.data_sets.external_id.return_value = DATA_SET_EXTERNAL_ID
        client.lookup.data_sets.id.return_value = DATA_SET_ID
        client.time_series.aggregate_count.return_value = len(some_timeseries_data)

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

        yield client


@pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
class TestAssetCentricIO:
    def test_download(self): ...


class TestAssetIO:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_download_upload(
        self, toolkit_config: ToolkitClientConfig, some_asset_data: AssetList, respx_mock: respx.MockRouter
    ) -> None:
        config = toolkit_config
        asset_by_external_id = {asset.external_id: asset for asset in some_asset_data if asset.external_id is not None}
        selector = AssetSubtreeSelector(hierarchy="test_hierarchy", kind="Assets")

        def create_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert "items" in payload
            items = payload["items"]
            assert isinstance(items, list)
            return httpx.Response(
                status_code=200,
                json={
                    "items": [asset_by_external_id[item["externalId"]].dump() for item in items if "externalId" in item]
                },
            )

        respx_mock.post(config.create_api_url("/assets")).mock(side_effect=create_callback)
        with monkeypatch_toolkit_client() as client:
            client.config = config
            client.assets.return_value = chunker(some_asset_data, 10)
            client.assets.aggregate_count.return_value = RESOURCE_COUNT
            client.lookup.data_sets.external_id.return_value = "test_data_set"
            client.lookup.data_sets.id.return_value = DATA_SET_ID

            io = AssetIO(client)

            assert io.count(selector) == RESOURCE_COUNT

            source = io.stream_data(selector)
            json_chunks: list[list[dict[str, JsonVal]]] = []
            for page in source:
                # New interface: stream_data returns Page objects
                json_chunk = io.data_to_json_chunk(page.items)
                assert isinstance(json_chunk, list)
                assert len(json_chunk) == 10
                for item in json_chunk:
                    assert isinstance(item, dict)
                    assert "dataSetExternalId" in item
                    assert item["dataSetExternalId"] == "test_data_set"
                json_chunks.append(json_chunk)

            with HTTPClient(config) as upload_client:
                from cognite_toolkit._cdf_tk.storageio._base import UploadItem

                # New interface: convert individual items and create UploadItems
                for chunk in json_chunks:
                    write_items = [io.json_to_resource(item) for item in chunk]
                    upload_items = [UploadItem(source_id=io.as_id(item), item=item) for item in write_items]
                    io.upload_items(upload_items, upload_client, selector)

            assert respx_mock.calls.call_count == 5  # 50 rows in chunks of 10
            uploaded_assets = []
            for call in respx_mock.calls:
                uploaded_assets.extend(json.loads(call.request.content)["items"])

            assert uploaded_assets == some_asset_data.as_write().dump()

    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_download_upload_command(
        self,
        some_asset_data: AssetList,
        tmp_path: Path,
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        config = toolkit_config
        monkeypatch.setenv("CDF_CLUSTER", config.cdf_cluster)
        monkeypatch.setenv("CDF_PROJECT", config.project)

        def asset_create_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert "items" in payload
            items = payload["items"]
            assert isinstance(items, list)
            assert items == [asset.as_write().dump() for asset in some_asset_data]
            return httpx.Response(status_code=200, json={"items": some_asset_data.dump()})

        respx_mock.post(config.create_api_url("/assets")).mock(side_effect=asset_create_callback)

        selector = AssetSubtreeSelector(hierarchy="test_hierarchy", kind="Assets")
        with monkeypatch_toolkit_client() as client:
            client.config = config
            client.verify.authorization.return_value = []
            client.assets.return_value = [some_asset_data]
            client.assets.aggregate_count.return_value = 100
            client.lookup.data_sets.external_id.return_value = "test_data_set"
            client.lookup.data_sets.id.return_value = 1234
            client.lookup.assets.external_id.return_value = "test_hierarchy"
            client.lookup.assets.id.return_value = 123
            client.data_sets.retrieve_multiple.return_value = DataSetList(
                [DataSet(id=1234, external_id="test_data_set")]
            )
            client.labels.retrieve.return_value = LabelDefinitionList(
                [LabelDefinition(external_id="my_label", name="my_label")]
            )

            io = AssetIO(client)

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

            upload_command.upload(
                input_dir=tmp_path / selector.group,
                client=client,
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
            AssetIO.read_chunks(other_reader, AssetSubtreeSelector(hierarchy="does not matter", kind="Assets"))
        )

        assert output == [
            [("line 4", {"id": 4}), ("line 5", {"id": 5, "depth": "not_an_int"})],
            [("line 3", {"id": 3, "depth": 1})],
            [("line 2", {"id": 2, "depth": 2})],
            [("line 1", {"id": 1, "depth": 3})],
        ]


class TestFileMetadataIO:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_download(self, asset_centric_client: ToolkitClient) -> None:
        selector = DataSetSelector(data_set_external_id="DataSetSelector", kind="FileMetadata")

        io = FileMetadataIO(asset_centric_client)

        assert io.count(selector) == RESOURCE_COUNT

        source = io.stream_data(selector)
        for page in source:
            json_chunk = io.data_to_json_chunk(page.items)
            assert isinstance(json_chunk, list)
            assert len(json_chunk) == CHUNK_SIZE
            for item in json_chunk:
                assert isinstance(item, dict)
                assert "dataSetExternalId" in item
                assert item["dataSetExternalId"] == DATA_SET_EXTERNAL_ID


class TestTimeSeriesIO:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_download_upload(
        self,
        toolkit_config: ToolkitClientConfig,
        some_timeseries_data: TimeSeriesList,
        respx_mock: respx.MockRouter,
        asset_centric_client: ToolkitClient,
    ) -> None:
        config = toolkit_config
        ts_by_external_id = {ts.external_id: ts for ts in some_timeseries_data if ts.external_id is not None}
        selector = DataSetSelector(data_set_external_id="DataSetSelector", kind="TimeSeries")

        def create_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert "items" in payload
            items = payload["items"]
            assert isinstance(items, list)
            return httpx.Response(
                status_code=200,
                json={
                    "items": [ts_by_external_id[item["externalId"]].dump() for item in items if "externalId" in item]
                },
            )

        respx_mock.post(config.create_api_url("/timeseries")).mock(side_effect=create_callback)

        io = TimeSeriesIO(asset_centric_client)

        assert io.count(selector) == RESOURCE_COUNT

        source = io.stream_data(selector)
        json_chunks: list[list[dict[str, JsonVal]]] = []
        for page in source:
            # New interface: stream_data returns Page objects
            json_chunk = io.data_to_json_chunk(page.items)
            assert isinstance(json_chunk, list)
            assert len(json_chunk) == 10
            for item in json_chunk:
                assert isinstance(item, dict)
                assert "dataSetExternalId" in item
                assert item["dataSetExternalId"] == DATA_SET_EXTERNAL_ID
            json_chunks.append(json_chunk)

        with HTTPClient(config) as upload_client:
            from cognite_toolkit._cdf_tk.storageio._base import UploadItem

            # New interface: convert individual items and create UploadItems
            for chunk in json_chunks:
                write_items = [io.json_to_resource(item) for item in chunk]
                upload_items = [UploadItem(source_id=io.as_id(item), item=item) for item in write_items]
                io.upload_items(upload_items, upload_client, selector)

        assert respx_mock.calls.call_count == 5  # 50 rows in chunks of 10
        uploaded_ts = []
        for call in respx_mock.calls:
            uploaded_ts.extend(json.loads(call.request.content)["items"])

        assert uploaded_ts == some_timeseries_data.as_write().dump()


class TestEventIO:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_download_upload(
        self,
        toolkit_config: ToolkitClientConfig,
        some_event_data: EventList,
        respx_mock: respx.MockRouter,
    ) -> None:
        config = toolkit_config
        event_by_external_id = {event.external_id: event for event in some_event_data if event.external_id is not None}
        selector = DataSetSelector(data_set_external_id="DataSetSelector", kind="Events")

        def create_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert "items" in payload
            items = payload["items"]
            assert isinstance(items, list)
            return httpx.Response(
                status_code=200,
                json={
                    "items": [event_by_external_id[item["externalId"]].dump() for item in items if "externalId" in item]
                },
            )

        respx_mock.post(config.create_api_url("/events")).mock(side_effect=create_callback)
        with monkeypatch_toolkit_client() as client:
            client.config = config
            client.events.return_value = chunker(some_event_data, 10)
            client.events.aggregate_count.return_value = len(some_event_data)
            client.lookup.data_sets.external_id.return_value = "test_data_set"
            client.lookup.data_sets.id.return_value = 1234
            client.lookup.assets.external_id.return_value = ["test_hierarchy"]
            client.lookup.assets.id.return_value = [123]

            io = EventIO(client)

            assert io.count(selector) == 50

            source = io.stream_data(selector)
            json_chunks: list[list[dict[str, JsonVal]]] = []
            for page in source:
                json_chunk = io.data_to_json_chunk(page.items)
                assert isinstance(json_chunk, list)
                assert len(json_chunk) == 10
                for item in json_chunk:
                    assert isinstance(item, dict)
                    assert "dataSetExternalId" in item
                    assert item["dataSetExternalId"] == "test_data_set"
                json_chunks.append(json_chunk)

            with HTTPClient(config) as upload_client:
                from cognite_toolkit._cdf_tk.storageio._base import UploadItem

                # New interface: convert individual items and create UploadItems
                for chunk in json_chunks:
                    write_items = [io.json_to_resource(item) for item in chunk]
                    upload_items = [UploadItem(source_id=io.as_id(item), item=item) for item in write_items]
                    io.upload_items(upload_items, upload_client, selector)

            assert respx_mock.calls.call_count == 5  # 50 rows in chunks of 10
            uploaded_events = []
            for call in respx_mock.calls:
                uploaded_events.extend(json.loads(call.request.content)["items"])

            assert uploaded_events == some_event_data.as_write().dump()
