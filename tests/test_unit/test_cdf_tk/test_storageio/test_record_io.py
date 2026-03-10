import json
from pathlib import Path

import httpx
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.records import RecordRequest
from cognite_toolkit._cdf_tk.commands import DownloadCommand, UploadCommand
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.storageio import RecordIO, UploadItem
from cognite_toolkit._cdf_tk.storageio.selectors import RecordContainerSelector
from cognite_toolkit._cdf_tk.storageio.selectors._records import SelectedContainer, SelectedStream


def _make_selector(initialize_cursor: str = "365d-ago") -> RecordContainerSelector:
    return RecordContainerSelector(
        stream=SelectedStream(external_id="my_stream"),
        container=SelectedContainer(space="my_space", external_id="my_container"),
        initialize_cursor=initialize_cursor,
    )


def _make_sync_response(
    record_count: int, has_next: bool = False, next_cursor: str = "cursor_abc", start_index: int = 0
) -> dict:
    return {
        "items": [
            {
                "space": "my_space",
                "externalId": f"record_{start_index + i}",
                "properties": {
                    "my_space": {
                        "my_container": {"name": f"Record {start_index + i}"},
                    }
                },
            }
            for i in range(record_count)
        ],
        "nextCursor": next_cursor,
        "hasNext": has_next,
    }


class TestRecordIO:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_upload_items(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        selector = _make_selector()
        record_count = 5
        records = [
            RecordRequest(
                space="my_space",
                external_id=f"record_{i}",
                sources=[
                    {
                        "source": {"type": "container", "space": "my_space", "externalId": "my_container"},
                        "properties": {"name": f"Record {i}"},
                    }
                ],
            )
            for i in range(record_count)
        ]
        upload_items = [UploadItem(source_id=record.external_id, item=record) for record in records]

        expected_url = toolkit_config.create_api_url("/streams/my_stream/records")

        def record_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert "items" in payload
            items = payload["items"]
            assert len(items) == record_count
            return httpx.Response(status_code=200, json={"items": items})

        with HTTPClient(toolkit_config) as http_client:
            with respx.mock() as mock_router:
                mock_router.post(expected_url).mock(side_effect=record_callback)
                io = RecordIO(client)
                results = io.upload_items(upload_items, http_client, selector=selector)

        success_ids = [item_id for res in results if isinstance(res, ItemsSuccessResponse) for item_id in res.ids]
        assert len(success_ids) == record_count

    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_stream_data_multiple_pages(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        selector = _make_selector(initialize_cursor="30d-ago")
        sync_url = toolkit_config.create_api_url("/streams/my_stream/records/sync")

        with respx.mock() as mock_router:
            route = mock_router.post(sync_url)
            route.side_effect = [
                httpx.Response(
                    status_code=200,
                    json=_make_sync_response(3, has_next=True, next_cursor="cursor_page2", start_index=0),
                ),
                httpx.Response(
                    status_code=200,
                    json=_make_sync_response(2, has_next=False, next_cursor="cursor_page3", start_index=3),
                ),
            ]
            io = RecordIO(client)
            pages = list(io.stream_data(selector, limit=100))

        assert len(pages) == 2  # Both pages should be yielded, even though they contain fewer items than requested
        assert len(pages[0].items) == 3
        assert len(pages[1].items) == 2
        all_ids = [item.external_id for page in pages for item in page.items]
        assert all_ids == ["record_0", "record_1", "record_2", "record_3", "record_4"]

        first_request = json.loads(route.calls[0].request.content)
        assert first_request["initializeCursor"] == "30d-ago", "initializeCursor should be set to the selector's value"
        assert "cursor" not in first_request

        second_request = json.loads(route.calls[1].request.content)
        assert "cursor" in second_request
        assert second_request["cursor"] == "cursor_page2"
        assert (
            "initializeCursor" not in second_request
        )  # Note: initializeCursor is silently ignored by the API if cursor is present

    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_stream_data_empty_response(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        selector = _make_selector()
        sync_url = toolkit_config.create_api_url("/streams/my_stream/records/sync")

        with respx.mock() as mock_router:
            mock_router.post(sync_url).mock(
                return_value=httpx.Response(status_code=200, json=_make_sync_response(0, has_next=False))
            )
            io = RecordIO(client)
            pages = list(io.stream_data(selector, limit=100))

        assert len(pages) == 0

    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_stream_data_with_instance_space_filter(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        selector = RecordContainerSelector(
            stream=SelectedStream(external_id="my_stream"),
            container=SelectedContainer(space="my_space", external_id="my_container"),
            instance_spaces=("filtered_space",),
        )
        sync_url = toolkit_config.create_api_url("/streams/my_stream/records/sync")

        def sync_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            record_filter = payload["filter"]
            assert "and" in record_filter
            filter_parts = record_filter["and"]
            assert any("hasData" in part for part in filter_parts), "hasData filter should always be present"
            assert any(
                "in" in part and part["in"]["property"] == ["space"] and part["in"]["values"] == ["filtered_space"]
                for part in filter_parts
            )
            return httpx.Response(status_code=200, json=_make_sync_response(1, has_next=False))

        with respx.mock() as mock_router:
            mock_router.post(sync_url).mock(side_effect=sync_callback)
            io = RecordIO(client)
            pages = list(io.stream_data(selector, limit=100))

        assert len(pages) == 1

    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_stream_data_enforces_max_total_records(
        self, toolkit_config: ToolkitClientConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = ToolkitClient(config=toolkit_config)
        selector = _make_selector()
        sync_url = toolkit_config.create_api_url("/streams/my_stream/records/sync")
        io = RecordIO(client)
        monkeypatch.setattr(RecordIO, "MAX_TOTAL_RECORDS", 5)

        with respx.mock() as mock_router:
            route = mock_router.post(sync_url)
            route.side_effect = [
                httpx.Response(status_code=200, json=_make_sync_response(5, has_next=True)),
            ]
            pages = list(io.stream_data(selector, limit=None))

        total_records = sum(len(page.items) for page in pages)
        assert total_records == 5

        request_body = json.loads(route.calls[0].request.content)
        assert request_body["limit"] == 5

    @pytest.mark.skipif(not Flags.EXTEND_UPLOAD.is_enabled(), reason="Alpha feature is not enabled")
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_download_upload_round_trip(
        self,
        tmp_path: Path,
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        config = toolkit_config
        client = ToolkitClient(config)
        monkeypatch.setenv("CDF_CLUSTER", config.cdf_cluster)
        monkeypatch.setenv("CDF_PROJECT", config.project)

        record_count = 10
        sync_response_data = _make_sync_response(record_count, has_next=False)

        def record_upload_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert "items" in payload
            items = payload["items"]
            assert isinstance(items, list)
            assert len(items) == record_count
            assert {item["externalId"] for item in items} == {f"record_{i}" for i in range(record_count)}
            return httpx.Response(status_code=200, json={"items": items})

        stream_url = config.create_api_url("/streams/my_stream")
        aggregate_url = config.create_api_url(RecordIO.AGGREGATE_ENDPOINT.format(streamId="my_stream"))
        sync_url = config.create_api_url(RecordIO.SYNC_ENDPOINT.format(streamId="my_stream"))
        upload_url = config.create_api_url(RecordIO.UPLOAD_ENDPOINT.format(streamId="my_stream"))

        respx_mock.get(stream_url).respond(
            json={
                "externalId": "my_stream",
                "createdTime": 1730204346000,
                "createdFromTemplate": "BasicLiveData",
                "type": "Mutable",
                "settings": {
                    "lifecycle": {"retainedAfterSoftDelete": "P42D"},
                    "limits": {
                        "maxRecordsTotal": {"provisioned": 5000000},
                        "maxGigaBytesTotal": {"provisioned": 15},
                    },
                },
            },
            status_code=200,
        )
        respx_mock.post(aggregate_url).respond(json={"aggregates": {"total": {"count": record_count}}}, status_code=200)
        respx_mock.post(sync_url).respond(json=sync_response_data, status_code=200)
        respx_mock.post(upload_url).mock(side_effect=record_upload_callback)

        selector = RecordContainerSelector(
            stream=SelectedStream(external_id="my_stream"),
            container=SelectedContainer(space="my_space", external_id="my_container"),
            initialize_cursor="365d-ago",
            download_dir_name="my_stream",
        )
        download_command = DownloadCommand(silent=True, skip_tracking=True)
        upload_command = UploadCommand(silent=True, skip_tracking=True)

        download_command.download(
            selectors=[selector],
            io=RecordIO(client),
            output_dir=tmp_path,
            verbose=False,
            file_format=".ndjson",
            compression="none",
            limit=100,
        )

        upload_command.upload(
            input_dir=tmp_path / selector.download_dir_name,
            client=client,
            deploy_resources=False,
            dry_run=False,
            verbose=False,
            kind=RecordIO.KIND,
        )

        assert len(respx_mock.calls) == 4
