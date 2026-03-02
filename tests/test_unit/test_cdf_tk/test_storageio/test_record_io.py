import json

import httpx
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.records import RecordRequest
from cognite_toolkit._cdf_tk.storageio import RecordIO, UploadItem
from cognite_toolkit._cdf_tk.storageio.selectors import RecordContainerSelector
from cognite_toolkit._cdf_tk.storageio.selectors._records import SelectedContainer, SelectedStream


class TestRecordIO:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_upload_items(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        selector = RecordContainerSelector(
            stream=SelectedStream(external_id="my_stream"),
            container=SelectedContainer(space="my_space", external_id="my_container"),
        )
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
