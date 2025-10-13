import json
from pathlib import Path

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

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import DownloadCommand, UploadCommand
from cognite_toolkit._cdf_tk.storageio import AssetIO, EventIO, FileMetadataIO, TimeSeriesIO
from cognite_toolkit._cdf_tk.storageio.selectors import AssetSubtreeSelector, DataSetSelector
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


@pytest.fixture()
def some_asset_data() -> AssetList:
    """Fixture to provide a sample AssetList for testing."""
    return AssetList(
        [
            Asset(
                id=1000 + i,
                external_id=f"asset_{i}",
                name=f"Asset {i}",
                description=f"Description for asset {i}",
                root_id=123,
                source="test_source",
                labels=["my_label"],
                data_set_id=1234,
            )
            for i in range(100)
        ]
    )


class TestAssetIO:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_download_upload(
        self, toolkit_config: ToolkitClientConfig, some_asset_data: AssetList, respx_mock: respx.MockRouter
    ) -> None:
        config = toolkit_config
        asset_by_external_id = {asset.external_id: asset for asset in some_asset_data if asset.external_id is not None}
        selector = AssetSubtreeSelector(hierarchy="test_hierarchy", resource_type="asset")

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
            client.assets.aggregate_count.return_value = 100
            client.lookup.data_sets.external_id.return_value = "test_data_set"
            client.lookup.data_sets.id.return_value = 1234

            io = AssetIO(client)

            assert io.count(selector) == 100

            source = io.stream_data(selector)
            json_chunks: list[list[dict[str, JsonVal]]] = []
            for chunk in source:
                json_chunk = io.data_to_json_chunk(chunk)
                assert isinstance(json_chunk, list)
                assert len(json_chunk) == 10
                for item in json_chunk:
                    assert isinstance(item, dict)
                    assert "dataSetExternalId" in item
                    assert item["dataSetExternalId"] == "test_data_set"
                json_chunks.append(json_chunk)

            with HTTPClient(config) as upload_client:
                data_chunks = (io.json_chunk_to_data(chunk) for chunk in json_chunks)
                for data_chunk in data_chunks:
                    io.upload_items(data_chunk, upload_client, selector)

            assert respx_mock.calls.call_count == 10  # 100 rows in chunks of 10
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
    ) -> None:
        config = toolkit_config

        def asset_create_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert "items" in payload
            items = payload["items"]
            assert isinstance(items, list)
            assert items == [asset.as_write().dump() for asset in some_asset_data]
            return httpx.Response(status_code=200, json={"items": some_asset_data.dump()})

        respx_mock.post(config.create_api_url("/assets")).mock(side_effect=asset_create_callback)

        selector = AssetSubtreeSelector(hierarchy="test_hierarchy", resource_type="asset")
        with monkeypatch_toolkit_client() as client:
            client.config = config
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
                io=io,
                input_dir=tmp_path / io.FOLDER_NAME,
                ensure_configurations=True,
                dry_run=False,
                verbose=False,
            )

            assert len(respx_mock.calls) == 1


@pytest.fixture()
def some_filemetadata_data() -> FileMetadataList:
    """Fixture to provide a sample FileMetadataList for testing."""
    return FileMetadataList(
        [
            FileMetadata(
                external_id=f"file_{i}",
                name=f"File {i}",
                directory="/test/dir",
                mime_type="text/plain",
                data_set_id=1234,
                asset_ids=[123],
                source="test_source",
            )
            for i in range(50)
        ]
    )


class TestFileMetadataIO:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_download_upload(
        self,
        toolkit_config: ToolkitClientConfig,
        some_filemetadata_data: FileMetadataList,
        respx_mock: respx.MockRouter,
    ) -> None:
        config = toolkit_config
        file_by_external_id = {
            file.external_id: file for file in some_filemetadata_data if file.external_id is not None
        }

        def create_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert "externalId" in payload
            return httpx.Response(
                status_code=200,
                json={"items": [file_by_external_id[payload["externalId"]].dump()]},
            )

        respx_mock.post(config.create_api_url("/files")).mock(side_effect=create_callback)
        selector = DataSetSelector(data_set_external_id="DataSetSelector", resource_type="file")

        with monkeypatch_toolkit_client() as client:
            client.config = config
            client.files.return_value = chunker(some_filemetadata_data, 10)
            client.files.aggregate.return_value = [CountAggregate(50)]
            client.lookup.data_sets.external_id.return_value = "test_data_set"
            client.lookup.data_sets.id.return_value = 1234
            client.lookup.assets.external_id.return_value = ["test_hierarchy"]
            client.lookup.assets.id.return_value = [123]

            io = FileMetadataIO(client)

            assert io.count(selector) == 50

            source = io.stream_data(selector)
            json_chunks: list[list[dict[str, JsonVal]]] = []
            for chunk in source:
                json_chunk = io.data_to_json_chunk(chunk)
                assert isinstance(json_chunk, list)
                assert len(json_chunk) == 10
                for item in json_chunk:
                    assert isinstance(item, dict)
                    assert "dataSetExternalId" in item
                    assert item["dataSetExternalId"] == "test_data_set"
                json_chunks.append(json_chunk)

            with HTTPClient(config) as upload_client:
                data_chunks = (io.json_chunk_to_data(chunk) for chunk in json_chunks)
                for data_chunk in data_chunks:
                    io.upload_items(data_chunk, upload_client, selector)

            # /files only support creating one at a time.
            assert respx_mock.calls.call_count == len(some_filemetadata_data)
            uploaded_files = []
            for call in respx_mock.calls:
                uploaded_files.append(json.loads(call.request.content))

            assert uploaded_files == some_filemetadata_data.as_write().dump()


@pytest.fixture()
def some_timeseries_data() -> TimeSeriesList:
    """Fixture to provide a sample TimeSeriesList for testing."""
    return TimeSeriesList(
        [
            TimeSeries(
                external_id=f"ts_{i}",
                name=f"Time Series {i}",
                description=f"Description for time series {i}",
                asset_id=123,
                data_set_id=1234,
                unit="unit",
                is_string=False,
                is_step=False,
            )
            for i in range(50)
        ]
    )


class TestTimeSeriesIO:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_download_upload(
        self,
        toolkit_config: ToolkitClientConfig,
        some_timeseries_data: TimeSeriesList,
        respx_mock: respx.MockRouter,
    ) -> None:
        config = toolkit_config
        ts_by_external_id = {ts.external_id: ts for ts in some_timeseries_data if ts.external_id is not None}
        selector = DataSetSelector(data_set_external_id="DataSetSelector", resource_type="timeseries")

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
        with monkeypatch_toolkit_client() as client:
            client.config = config
            client.time_series.return_value = chunker(some_timeseries_data, 10)
            client.time_series.aggregate_count.return_value = len(some_timeseries_data)
            client.lookup.data_sets.external_id.return_value = "test_data_set"
            client.lookup.data_sets.id.return_value = 1234
            client.lookup.assets.external_id.return_value = "test_hierarchy"
            client.lookup.assets.id.return_value = 123

            io = TimeSeriesIO(client)

            assert io.count(selector) == 50

            source = io.stream_data(selector)
            json_chunks: list[list[dict[str, JsonVal]]] = []
            for chunk in source:
                json_chunk = io.data_to_json_chunk(chunk)
                assert isinstance(json_chunk, list)
                assert len(json_chunk) == 10
                for item in json_chunk:
                    assert isinstance(item, dict)
                    assert "dataSetExternalId" in item
                    assert item["dataSetExternalId"] == "test_data_set"
                json_chunks.append(json_chunk)

            with HTTPClient(config) as upload_client:
                data_chunks = (io.json_chunk_to_data(chunk) for chunk in json_chunks)
                for data_chunk in data_chunks:
                    io.upload_items(data_chunk, upload_client, selector)

            assert respx_mock.calls.call_count == 5  # 50 rows in chunks of 10
            uploaded_ts = []
            for call in respx_mock.calls:
                uploaded_ts.extend(json.loads(call.request.content)["items"])

            assert uploaded_ts == some_timeseries_data.as_write().dump()


@pytest.fixture()
def some_event_data() -> EventList:
    """Fixture to provide a sample EventList for testing."""
    return EventList(
        [
            Event(
                external_id=f"event_{i}",
                description=f"Description for event {i}",
                asset_ids=[123],
                data_set_id=1234,
                source="test_source",
                start_time=1000000000000 + i * 1000,
                end_time=1000000001000 + i * 1000,
            )
            for i in range(50)
        ]
    )


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
        selector = DataSetSelector(data_set_external_id="DataSetSelector", resource_type="event")

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
            for chunk in source:
                json_chunk = io.data_to_json_chunk(chunk)
                assert isinstance(json_chunk, list)
                assert len(json_chunk) == 10
                for item in json_chunk:
                    assert isinstance(item, dict)
                    assert "dataSetExternalId" in item
                    assert item["dataSetExternalId"] == "test_data_set"
                json_chunks.append(json_chunk)

            with HTTPClient(config) as upload_client:
                data_chunks = (io.json_chunk_to_data(chunk) for chunk in json_chunks)
                for data_chunk in data_chunks:
                    io.upload_items(data_chunk, upload_client, selector)

            assert respx_mock.calls.call_count == 5  # 50 rows in chunks of 10
            uploaded_events = []
            for call in respx_mock.calls:
                uploaded_events.extend(json.loads(call.request.content)["items"])

            assert uploaded_events == some_event_data.as_write().dump()
