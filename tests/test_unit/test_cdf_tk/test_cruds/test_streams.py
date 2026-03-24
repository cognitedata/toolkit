import pytest
import respx
from httpx import Response

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.cruds._resource_cruds.streams import StreamCRUD


@pytest.fixture(scope="module")
def toolkit_client(toolkit_config: ToolkitClientConfig) -> ToolkitClient:
    return ToolkitClient(config=toolkit_config)


class TestStreamCRUDIterLastUpdatedTimeWindows:
    def test_immutable_stream_with_filtering_interval(
        self, toolkit_client: ToolkitClient, respx_mock: respx.MockRouter, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = toolkit_client.config
        hour_ms = 60 * 60 * 1000
        created_time_ms = 0
        now_ms = 3 * hour_ms
        monkeypatch.setattr("cognite_toolkit._cdf_tk.cruds._resource_cruds.streams.time.time", lambda: now_ms / 1000)
        respx_mock.get(config.create_api_url("/streams/my-immutable-stream")).mock(
            return_value=Response(
                status_code=200,
                json={
                    "externalId": "my-immutable-stream",
                    "createdTime": created_time_ms,
                    "createdFromTemplate": "ImmutableTestStream",
                    "type": "Immutable",
                    "settings": {
                        "lifecycle": {"retainedAfterSoftDelete": "P30D"},
                        "limits": {
                            "maxRecordsTotal": {"provisioned": 1_000_000},
                            "maxGigaBytesTotal": {"provisioned": 10},
                            "maxFilteringInterval": "PT1H",
                        },
                    },
                },
            )
        )
        crud = StreamCRUD.create_loader(toolkit_client)

        windows = list(crud.iter_last_updated_time_windows("my-immutable-stream"))

        assert windows == [
            {"gte": 0, "lt": hour_ms},
            {"gte": hour_ms, "lt": 2 * hour_ms},
            {"gte": 2 * hour_ms, "lt": now_ms},
        ]

    def test_mutable_stream_yields_single_empty_window(
        self, toolkit_client: ToolkitClient, respx_mock: respx.MockRouter
    ) -> None:
        config = toolkit_client.config
        respx_mock.get(config.create_api_url("/streams/my-mutable-stream")).mock(
            return_value=Response(
                status_code=200,
                json={
                    "externalId": "my-mutable-stream",
                    "createdTime": 1_000,
                    "createdFromTemplate": "BasicLiveData",
                    "type": "Mutable",
                },
            )
        )
        crud = StreamCRUD.create_loader(toolkit_client)

        windows = list(crud.iter_last_updated_time_windows("my-mutable-stream"))

        assert windows == [None]

    def test_unknown_stream_yields_no_windows(
        self, toolkit_client: ToolkitClient, respx_mock: respx.MockRouter
    ) -> None:
        config = toolkit_client.config
        respx_mock.get(config.create_api_url("/streams/unknown-stream")).mock(
            return_value=Response(status_code=404, json={"error": {"code": 404, "message": "Not found"}})
        )
        crud = StreamCRUD.create_loader(toolkit_client)

        windows = list(crud.iter_last_updated_time_windows("unknown-stream"))

        assert windows == []
