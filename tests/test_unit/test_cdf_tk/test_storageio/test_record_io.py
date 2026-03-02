import json

import httpx
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.records import RecordRequest
from cognite_toolkit._cdf_tk.storageio import RecordIO, UploadItem
from cognite_toolkit._cdf_tk.storageio._records import RecordSyncResponse
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


class TestRecordIODownload:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_stream_data_single_page(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        selector = _make_selector()
        sync_url = toolkit_config.create_api_url("/streams/my_stream/records/sync")

        with respx.mock() as mock_router:
            mock_router.post(sync_url).mock(
                return_value=httpx.Response(status_code=200, json=_make_sync_response(3, has_next=False))
            )
            io = RecordIO(client)
            pages = list(io.stream_data(selector, limit=100))

        assert len(pages) == 1
        assert len(pages[0].items) == 3
        assert pages[0].items[0].external_id == "record_0"

    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_stream_data_multiple_pages(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        selector = _make_selector()
        sync_url = toolkit_config.create_api_url("/streams/my_stream/records/sync")

        call_count = 0

        def sync_callback(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            payload = json.loads(request.content)
            if call_count == 0:
                assert "initializeCursor" in payload
                assert "cursor" not in payload
                call_count += 1
                return httpx.Response(
                    status_code=200,
                    json=_make_sync_response(3, has_next=True, next_cursor="cursor_page2", start_index=0),
                )
            else:
                assert "cursor" in payload
                assert payload["cursor"] == "cursor_page2"
                assert "initializeCursor" not in payload
                return httpx.Response(
                    status_code=200,
                    json=_make_sync_response(2, has_next=False, next_cursor="cursor_page3", start_index=3),
                )

        with respx.mock() as mock_router:
            mock_router.post(sync_url).mock(side_effect=sync_callback)
            io = RecordIO(client)
            pages = list(io.stream_data(selector, limit=100))

        assert len(pages) == 2
        assert len(pages[0].items) == 3
        assert len(pages[1].items) == 2
        all_ids = [item.external_id for page in pages for item in page.items]
        assert all_ids == ["record_0", "record_1", "record_2", "record_3", "record_4"]

    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_stream_data_respects_limit(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        selector = _make_selector()
        sync_url = toolkit_config.create_api_url("/streams/my_stream/records/sync")

        def sync_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            requested_limit = payload["limit"]
            return httpx.Response(
                status_code=200,
                json=_make_sync_response(requested_limit, has_next=True, next_cursor="cursor_next"),
            )

        with respx.mock() as mock_router:
            mock_router.post(sync_url).mock(side_effect=sync_callback)
            io = RecordIO(client)
            pages = list(io.stream_data(selector, limit=5))

        total_records = sum(len(page.items) for page in pages)
        assert total_records == 5

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
    def test_stream_data_with_record_space_filter(self, toolkit_config: ToolkitClientConfig) -> None:
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
            assert any("hasData" in part for part in filter_parts)
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
    def test_stream_data_since_passed_as_initialize_cursor(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        selector = _make_selector(initialize_cursor="30d-ago")
        sync_url = toolkit_config.create_api_url("/streams/my_stream/records/sync")

        def sync_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert payload["initializeCursor"] == "30d-ago"
            return httpx.Response(status_code=200, json=_make_sync_response(1, has_next=False))

        with respx.mock() as mock_router:
            mock_router.post(sync_url).mock(side_effect=sync_callback)
            io = RecordIO(client)
            pages = list(io.stream_data(selector, limit=100))

        assert len(pages) == 1

    def test_data_to_json_chunk(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        io = RecordIO(client)
        sync_data = _make_sync_response(2)
        response = RecordSyncResponse.model_validate(sync_data)

        result = io.data_to_json_chunk(response.items)

        assert len(result) == 2
        assert result[0]["space"] == "my_space"
        assert result[0]["externalId"] == "record_0"
        assert "sources" in result[0]

    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_stream_data_enforces_max_total_records(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        selector = _make_selector()
        sync_url = toolkit_config.create_api_url("/streams/my_stream/records/sync")
        io = RecordIO(client)

        original_max = RecordIO.MAX_TOTAL_RECORDS
        RecordIO.MAX_TOTAL_RECORDS = 5
        try:

            def sync_callback(request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    status_code=200,
                    json=_make_sync_response(3, has_next=True, next_cursor="cursor_next"),
                )

            with respx.mock() as mock_router:
                mock_router.post(sync_url).mock(side_effect=sync_callback)
                pages = list(io.stream_data(selector, limit=None))

            total_records = sum(len(page.items) for page in pages)
            assert total_records <= 5
        finally:
            RecordIO.MAX_TOTAL_RECORDS = original_max
