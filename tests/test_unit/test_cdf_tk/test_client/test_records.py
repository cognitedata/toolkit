import json
from collections.abc import Callable, Sequence

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.records import RecordsAPI
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.records import RecordId, RecordRequest, RecordSource

RECORD = RecordRequest(
    space="my_space",
    external_id="rec_1",
    sources=[
        RecordSource(source=ContainerId(space="my_space", external_id="MyContainer"), properties={"name": "a"}),
        RecordSource(source=ViewId(space="my_space", external_id="MyView", version="v1"), properties={"count": 1}),
    ],
)
RECORD_BODY = {
    "space": "my_space",
    "externalId": "rec_1",
    "sources": [
        {
            "source": {"space": "my_space", "externalId": "MyContainer", "type": "container"},
            "properties": {"name": "a"},
        },
        {
            "source": {"space": "my_space", "externalId": "MyView", "version": "v1", "type": "view"},
            "properties": {"count": 1},
        },
    ],
}


@pytest.mark.usefixtures("disable_gzip")
class TestRecordsAPI:
    @pytest.mark.parametrize(
        "method, path, items, expected_items",
        [
            pytest.param(RecordsAPI.ingest, "/streams/my_stream/records", [RECORD], [RECORD_BODY], id="ingest"),
            pytest.param(RecordsAPI.upsert, "/streams/my_stream/records/upsert", [RECORD], [RECORD_BODY], id="upsert"),
            pytest.param(
                RecordsAPI.delete,
                "/streams/my_stream/records/delete",
                [RecordId(space="my_space", external_id="rec_1")],
                [{"space": "my_space", "externalId": "rec_1"}],
                id="delete",
            ),
        ],
    )
    def test_write_endpoints(
        self,
        toolkit_client: ToolkitClient,
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
        method: Callable[[RecordsAPI, str, Sequence], None],
        path: str,
        items: Sequence,
        expected_items: list[dict],
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url(path)).mock(return_value=httpx2.Response(202, json={}))

        method(toolkit_client.records, "my_stream", items)

        assert json.loads(route.calls[0].request.content) == {"items": expected_items}

    def test_write_splits_into_chunks(
        self,
        toolkit_client: ToolkitClient,
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(RecordsAPI, "_WRITE_ITEM_LIMIT", 2)
        route = respx_mock.post(toolkit_config.create_api_url("/streams/my_stream/records/delete")).mock(
            return_value=httpx2.Response(200, json={})
        )

        toolkit_client.records.delete("my_stream", [RecordId(space="s", external_id=f"r{i}") for i in range(3)])

        assert [len(json.loads(call.request.content)["items"]) for call in route.calls] == [2, 1]

    def test_aggregate(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/streams/my_stream/records/aggregate")).mock(
            return_value=httpx2.Response(200, json={"aggregates": {"total": {"count": 5}}})
        )

        toolkit_client.records.aggregate("my_stream", {"total": {"count": {}}}, last_updated_time={"gte": "2d-ago"})

        assert json.loads(route.calls[0].request.content) == {
            "aggregates": {"total": {"count": {}}},
            "lastUpdatedTime": {"gte": "2d-ago"},
        }
